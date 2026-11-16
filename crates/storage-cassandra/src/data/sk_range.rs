// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The sort key range a Cassandra Query reads, with the start key folded in.

use std::cmp::Ordering;

use extenddb_core::expression::{CompareOp, ExpressionMaps, SortKeyCondition};
use extenddb_core::types::ScalarAttributeType;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk};

use super::query::{binary_upper_bound, string_upper_bound};
use super::resolve_expr_to_av;

/// One side of a sort key range: the CQL operator and the bound value.
pub(super) type SkBound = (&'static str, SortKeyValue);

/// The sort key range a Query reads: at most one lower and one upper bound.
///
/// Cassandra takes one restriction per side of a clustering column ("More
/// than one restriction was found for the start bound on sk_s"), so the
/// start key cannot be appended as a second `>` or `<` next to the key
/// condition's own bound; [`SkRange::advance_past`] folds them into one.
#[derive(Default)]
pub(super) struct SkRange {
    pub(super) lower: Option<SkBound>,
    pub(super) upper: Option<SkBound>,
}

impl SkRange {
    /// The range a key condition's sort key clause asks for.
    pub(super) fn from_condition(
        sk_cond: &SortKeyCondition,
        expression_maps: &ExpressionMaps,
        sk_type: ScalarAttributeType,
    ) -> Result<Self, StorageError> {
        let mut range = Self::default();
        match sk_cond {
            SortKeyCondition::Compare { op, value, .. } => {
                let sk_av = resolve_expr_to_av(value, expression_maps)?;
                let sk_val = parse_sk(&sk_av, sk_type)?;
                match op {
                    CompareOp::Eq => {
                        let other = parse_sk(&sk_av, sk_type)?;
                        range.lower = Some((">=", sk_val));
                        range.upper = Some(("<=", other));
                    }
                    CompareOp::Lt => range.upper = Some(("<", sk_val)),
                    CompareOp::Le => range.upper = Some(("<=", sk_val)),
                    CompareOp::Gt => range.lower = Some((">", sk_val)),
                    CompareOp::Ge => range.lower = Some((">=", sk_val)),
                    CompareOp::Ne => {
                        return Err(StorageError::Validation(
                            "Query key condition does not support <> on the sort key".to_owned(),
                        ));
                    }
                }
            }
            SortKeyCondition::Between { low, high, .. } => {
                let low_av = resolve_expr_to_av(low, expression_maps)?;
                let high_av = resolve_expr_to_av(high, expression_maps)?;
                range.lower = Some((">=", parse_sk(&low_av, sk_type)?));
                range.upper = Some(("<=", parse_sk(&high_av, sk_type)?));
            }
            SortKeyCondition::BeginsWith { prefix, .. } => {
                let prefix_av = resolve_expr_to_av(prefix, expression_maps)?;
                let prefix_sk = parse_sk(&prefix_av, sk_type)?;
                // Exclusive upper bound by type; None when the prefix is empty
                // or all 0xFF (no finite upper bound).
                let upper_sk_opt = match &prefix_sk {
                    SortKeyValue::S(s) => Some(SortKeyValue::S(string_upper_bound(s))),
                    SortKeyValue::B(b) => binary_upper_bound(b).map(SortKeyValue::B),
                    SortKeyValue::N(_) => {
                        return Err(StorageError::Validation(
                            "BeginsWith is not supported for numeric sort keys".to_owned(),
                        ));
                    }
                };
                range.lower = Some((">=", prefix_sk));
                range.upper = upper_sk_opt.map(|upper_sk| ("<", upper_sk));
            }
        }
        Ok(range)
    }

    /// Whether `value` lies inside the range.
    pub(super) fn contains(&self, value: &SortKeyValue) -> bool {
        let below = self.lower.as_ref().is_some_and(|(op, low)| {
            matches!(cmp_sk(value, low), Ordering::Less)
                || (*op == ">" && cmp_sk(value, low) == Ordering::Equal)
        });
        let above = self.upper.as_ref().is_some_and(|(op, high)| {
            matches!(cmp_sk(value, high), Ordering::Greater)
                || (*op == "<" && cmp_sk(value, high) == Ordering::Equal)
        });
        !below && !above
    }

    /// Resume after `start`: the bound on the near side of the scan direction
    /// becomes a strict bound at `start` when `start` is at or past the
    /// condition's bound. A `start` short of that bound is left out: the
    /// condition's bound already excludes everything up to it.
    pub(super) fn advance_past(&mut self, start: SortKeyValue, forward: bool) {
        if forward {
            if self
                .lower
                .as_ref()
                .is_none_or(|(_, low)| cmp_sk(&start, low) != Ordering::Less)
            {
                self.lower = Some((">", start));
            }
        } else if self
            .upper
            .as_ref()
            .is_none_or(|(_, high)| cmp_sk(&start, high) != Ordering::Greater)
        {
            self.upper = Some(("<", start));
        }
    }

    /// The range as CQL `AND` clauses on `sk_col`, with the binds in order.
    /// Equal inclusive bounds render as one `=` clause.
    pub(super) fn render(self, sk_col: &str) -> (String, Vec<SortKeyValue>) {
        let (lower, upper) = match (self.lower, self.upper) {
            (Some((">=", low)), Some(("<=", high))) if cmp_sk(&low, &high) == Ordering::Equal => {
                (Some(("=", low)), None)
            }
            bounds => bounds,
        };
        let mut clause = String::new();
        let mut binds = Vec::new();
        for (op, value) in [lower, upper].into_iter().flatten() {
            clause.push_str(&format!(" AND {sk_col} {op} ?"));
            binds.push(value);
        }
        (clause, binds)
    }
}

/// Order two sort key values as Cassandra orders the clustering column: text
/// and blob by unsigned bytes, decimal numerically. Both values are parsed
/// with the table's sort key type, so mixed variants do not occur.
fn cmp_sk(a: &SortKeyValue, b: &SortKeyValue) -> Ordering {
    match (a, b) {
        (SortKeyValue::S(a), SortKeyValue::S(b)) => a.as_bytes().cmp(b.as_bytes()),
        (SortKeyValue::N(a), SortKeyValue::N(b)) => a.cmp(b),
        (SortKeyValue::B(a), SortKeyValue::B(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use extenddb_core::expression::{parse_key_condition, tokenize};
    use extenddb_core::types::AttributeValue;

    use super::*;

    fn s(v: &str) -> SortKeyValue {
        SortKeyValue::S(v.to_owned())
    }

    fn ge_le(low: &str, high: &str) -> SkRange {
        SkRange {
            lower: Some((">=", s(low))),
            upper: Some(("<=", s(high))),
        }
    }

    fn rendered(range: SkRange) -> (String, Vec<String>) {
        let (clause, binds) = range.render("sk_s");
        let binds = binds
            .into_iter()
            .map(|b| match b {
                SortKeyValue::S(v) => v,
                _ => unreachable!(),
            })
            .collect();
        (clause, binds)
    }

    #[test]
    fn start_key_inside_a_lower_bound_replaces_it() {
        // `sk >= a` with start key `m`, forward: one lower bound, strict at m.
        let mut range = SkRange {
            lower: Some((">=", s("a"))),
            upper: None,
        };
        range.advance_past(s("m"), true);
        assert_eq!(
            rendered(range),
            (" AND sk_s > ?".to_owned(), vec!["m".to_owned()])
        );
    }

    #[test]
    fn start_key_at_the_bound_replaces_it() {
        let mut range = SkRange {
            lower: Some((">=", s("m"))),
            upper: None,
        };
        range.advance_past(s("m"), true);
        assert_eq!(
            rendered(range),
            (" AND sk_s > ?".to_owned(), vec!["m".to_owned()])
        );
    }

    #[test]
    fn start_key_short_of_the_bound_leaves_the_condition_bound() {
        let mut range = SkRange {
            lower: Some((">=", s("m"))),
            upper: None,
        };
        range.advance_past(s("a"), true);
        assert_eq!(
            rendered(range),
            (" AND sk_s >= ?".to_owned(), vec!["m".to_owned()])
        );
    }

    #[test]
    fn between_keeps_the_far_bound_in_either_direction() {
        let mut forward = ge_le("aa", "m");
        forward.advance_past(s("ab"), true);
        assert_eq!(
            rendered(forward),
            (
                " AND sk_s > ? AND sk_s <= ?".to_owned(),
                vec!["ab".to_owned(), "m".to_owned()]
            )
        );
        let mut reverse = ge_le("aa", "m");
        reverse.advance_past(s("ab"), false);
        assert_eq!(
            rendered(reverse),
            (
                " AND sk_s >= ? AND sk_s < ?".to_owned(),
                vec!["aa".to_owned(), "ab".to_owned()]
            )
        );
    }

    #[test]
    fn equal_bounds_render_as_equality_until_advanced() {
        assert_eq!(
            rendered(ge_le("m", "m")),
            (" AND sk_s = ?".to_owned(), vec!["m".to_owned()])
        );
        let mut range = ge_le("m", "m");
        range.advance_past(s("m"), true);
        assert_eq!(
            rendered(range),
            (
                " AND sk_s > ? AND sk_s <= ?".to_owned(),
                vec!["m".to_owned(), "m".to_owned()]
            )
        );
    }

    #[test]
    fn begins_with_in_reverse_replaces_the_exclusive_upper_bound() {
        let mut range = SkRange {
            lower: Some((">=", s("ab"))),
            upper: Some(("<", SortKeyValue::S(string_upper_bound("ab")))),
        };
        range.advance_past(s("abz"), false);
        assert_eq!(
            rendered(range),
            (
                " AND sk_s >= ? AND sk_s < ?".to_owned(),
                vec!["ab".to_owned(), "abz".to_owned()]
            )
        );
    }

    #[test]
    fn contains_honours_strict_and_inclusive_bounds() {
        let strict = SkRange {
            lower: Some((">", s("a"))),
            upper: Some(("<", s("m"))),
        };
        assert!(!strict.contains(&s("a")));
        assert!(strict.contains(&s("b")));
        assert!(!strict.contains(&s("m")));
        let inclusive = ge_le("a", "m");
        assert!(inclusive.contains(&s("a")));
        assert!(inclusive.contains(&s("m")));
        assert!(!inclusive.contains(&s("z")));
        assert!(SkRange::default().contains(&s("anything")));
    }

    #[test]
    fn numeric_bounds_compare_numerically() {
        let n = |v: &str| SortKeyValue::N(v.parse().unwrap());
        let mut range = SkRange {
            lower: Some((">=", n("5"))),
            upper: None,
        };
        // "10" sorts before "5" as text; as a number it is past the bound.
        range.advance_past(n("10"), true);
        match range.lower {
            Some((">", SortKeyValue::N(v))) => assert_eq!(v.to_string(), "10"),
            other => panic!("unexpected lower bound: {:?}", other.map(|(op, _)| op)),
        }
    }

    /// Parse a key condition's sort key clause the way the Query path does.
    fn condition(
        expr: &str,
        values: &[(&str, AttributeValue)],
    ) -> (SortKeyCondition, ExpressionMaps) {
        let tokens = tokenize(expr).unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        kc.resolve_pk_sk("pk", &HashMap::new()).unwrap();
        let values = values
            .iter()
            .map(|(k, v)| (k.trim_start_matches(':').to_owned(), v.clone()))
            .collect();
        (
            kc.sk_condition.expect("a sort key clause"),
            ExpressionMaps::new(HashMap::new(), values),
        )
    }

    /// Render a range as its clause and binds printed by type, for any sort key type.
    fn shown(range: SkRange) -> (String, Vec<String>) {
        let (clause, binds) = range.render("sk");
        let binds = binds
            .into_iter()
            .map(|b| match b {
                SortKeyValue::S(v) => format!("S:{v}"),
                SortKeyValue::B(v) => format!(
                    "B:{}",
                    v.iter().map(|x| format!("{x:02x}")).collect::<String>()
                ),
                SortKeyValue::N(v) => format!("N:{v}"),
            })
            .collect();
        (clause, binds)
    }

    #[test]
    fn from_condition_pins_each_operator_and_the_begins_with_upper_bound() {
        let s_av = |v: &str| AttributeValue::S(v.to_owned());
        let range = |expr: &str, values: &[(&str, AttributeValue)], ty: ScalarAttributeType| {
            let (cond, maps) = condition(expr, values);
            shown(SkRange::from_condition(&cond, &maps, ty).unwrap())
        };
        let strs = |v: &[&str]| v.iter().map(|x| format!("S:{x}")).collect::<Vec<_>>();

        // `=` is one inclusive bound on each side, rendered as a single `= ?`.
        assert_eq!(
            range(
                "pk = :p AND sk = :v",
                &[(":v", s_av("m"))],
                ScalarAttributeType::S
            ),
            (" AND sk = ?".to_owned(), strs(&["m"]))
        );
        // BETWEEN is inclusive on both sides.
        assert_eq!(
            range(
                "pk = :p AND sk BETWEEN :lo AND :hi",
                &[(":lo", s_av("aa")), (":hi", s_av("m"))],
                ScalarAttributeType::S
            ),
            (" AND sk >= ? AND sk <= ?".to_owned(), strs(&["aa", "m"]))
        );
        // begins_with on a string: inclusive prefix, exclusive prefix plus U+10FFFF.
        assert_eq!(
            range(
                "pk = :p AND begins_with(sk, :v)",
                &[(":v", s_av("ab"))],
                ScalarAttributeType::S
            ),
            (
                " AND sk >= ? AND sk < ?".to_owned(),
                strs(&["ab", "ab\u{10FFFF}"])
            )
        );
        // begins_with on binary: the last byte below 0xFF is incremented and the
        // rest dropped; an all-0xFF prefix has no finite upper bound.
        assert_eq!(
            range(
                "pk = :p AND begins_with(sk, :v)",
                &[(":v", AttributeValue::B(vec![0x61, 0xFF]))],
                ScalarAttributeType::B
            ),
            (
                " AND sk >= ? AND sk < ?".to_owned(),
                vec!["B:61ff".to_owned(), "B:62".to_owned()]
            )
        );
        assert_eq!(
            range(
                "pk = :p AND begins_with(sk, :v)",
                &[(":v", AttributeValue::B(vec![0xFF, 0xFF]))],
                ScalarAttributeType::B
            ),
            (" AND sk >= ?".to_owned(), vec!["B:ffff".to_owned()])
        );
        // The strict and inclusive comparisons land on the expected side.
        for (expr, clause) in [
            ("pk = :p AND sk < :v", " AND sk < ?"),
            ("pk = :p AND sk <= :v", " AND sk <= ?"),
            ("pk = :p AND sk > :v", " AND sk > ?"),
            ("pk = :p AND sk >= :v", " AND sk >= ?"),
        ] {
            assert_eq!(
                range(expr, &[(":v", s_av("m"))], ScalarAttributeType::S),
                (clause.to_owned(), strs(&["m"])),
                "{expr}"
            );
        }
    }
}
