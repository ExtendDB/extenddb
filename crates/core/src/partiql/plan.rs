// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SELECT planning.
//!
//! Decides, from a parsed `SELECT` and the target's key attribute names,
//! whether the statement is a point read, a Query, or a Scan, and separates
//! the key condition from the residual filter. The rules are the ones measured
//! on the service (see the behavioural spec): a partition-key equality with an
//! optional sort-key key-condition is a Query; a full-key equality is a point
//! read; anything else is a Scan.

use super::ast::{CompareOp, Operand, Path, SelectStatement, WhereExpr};

/// How a `SELECT` reaches its rows.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectPlan {
    /// Full primary key pinned by equality: at most one item.
    PointRead,
    /// Partition key pinned by equality, with an optional sort-key key condition.
    Query { has_sort_condition: bool },
    /// Everything else: a scan with the whole WHERE as a filter.
    Scan,
}

/// The key attribute names of the target (table or index).
#[derive(Debug, Clone, Copy)]
pub struct KeyNames<'a> {
    pub partition: &'a str,
    pub sort: Option<&'a str>,
}

/// Classify a `SELECT` against the target's key names.
#[must_use]
pub fn classify(stmt: &SelectStatement, keys: KeyNames<'_>) -> SelectPlan {
    let Some(where_clause) = &stmt.where_clause else {
        return SelectPlan::Scan;
    };
    // The top level must be a conjunction of simple predicates for a Query/point
    // read; any OR/NOT, or a non-equality on the partition key, is a Scan.
    let mut terms = Vec::new();
    if !flatten_and(where_clause, &mut terms) {
        return SelectPlan::Scan;
    }

    let mut pk_equality = false;
    let mut sort_condition = false;
    let mut sort_equality = false;
    for term in &terms {
        match term {
            WhereExpr::Compare(left, op, _) => {
                if let Some(name) = simple_path(left) {
                    if name == keys.partition {
                        if *op == CompareOp::Eq {
                            pk_equality = true;
                        } else {
                            // Any non-equality on the partition key forces a Scan.
                            return SelectPlan::Scan;
                        }
                    } else if Some(name) == keys.sort {
                        match op {
                            CompareOp::Eq => {
                                sort_condition = true;
                                sort_equality = true;
                            }
                            CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge => {
                                sort_condition = true;
                            }
                            // `<>` on the sort key is not a key condition -> Scan.
                            CompareOp::Ne => return SelectPlan::Scan,
                        }
                    }
                }
            }
            WhereExpr::Between(left, _, _) | WhereExpr::BeginsWith(left, _) => {
                if let Some(name) = simple_path(left)
                    && Some(name) == keys.sort
                {
                    sort_condition = true;
                }
            }
            WhereExpr::In(left, _) => {
                // `pk = x AND sk IN [..]` is a set of point reads; treated as a
                // key-qualified access, not a Scan, when the left is the sort key.
                if let Some(name) = simple_path(left)
                    && Some(name) == keys.sort
                {
                    sort_condition = true;
                }
            }
            _ => {}
        }
    }

    if !pk_equality {
        return SelectPlan::Scan;
    }
    if sort_equality && keys.sort.is_some() {
        return SelectPlan::PointRead;
    }
    SelectPlan::Query {
        has_sort_condition: sort_condition,
    }
}

/// Flatten a top-level `AND` chain into its conjuncts. Returns false if the
/// expression contains an `OR` or `NOT` at the top level (which forces a Scan).
fn flatten_and<'a>(expr: &'a WhereExpr, out: &mut Vec<&'a WhereExpr>) -> bool {
    match expr {
        WhereExpr::And(a, b) => flatten_and(a, out) && flatten_and(b, out),
        WhereExpr::Or(_, _) | WhereExpr::Not(_) => false,
        other => {
            out.push(other);
            true
        }
    }
}

/// The attribute name when the operand is a bare attribute path, else `None`.
fn simple_path(op: &Operand) -> Option<&str> {
    match op {
        Operand::Path(Path { root, steps }) if steps.is_empty() => Some(root.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partiql::ast::Statement;
    use crate::partiql::parse;

    fn select(sql: &str) -> SelectStatement {
        match parse(sql).unwrap() {
            Statement::Select(s) => s,
            _ => panic!("not a select"),
        }
    }

    const KEYS: KeyNames<'static> = KeyNames {
        partition: "pk",
        sort: Some("sk"),
    };

    #[test]
    fn full_key_equality_is_a_point_read() {
        let s = select("SELECT * FROM \"t\" WHERE pk = 'a' AND sk = 'b'");
        assert_eq!(classify(&s, KEYS), SelectPlan::PointRead);
    }

    #[test]
    fn partition_only_is_a_query() {
        let s = select("SELECT * FROM \"t\" WHERE pk = 'a'");
        assert_eq!(
            classify(&s, KEYS),
            SelectPlan::Query {
                has_sort_condition: false
            }
        );
    }

    #[test]
    fn partition_plus_sort_range_is_a_query() {
        let s = select("SELECT * FROM \"t\" WHERE pk = 'a' AND sk > 'b'");
        assert_eq!(
            classify(&s, KEYS),
            SelectPlan::Query {
                has_sort_condition: true
            }
        );
    }

    #[test]
    fn no_where_is_a_scan() {
        let s = select("SELECT * FROM \"t\"");
        assert_eq!(classify(&s, KEYS), SelectPlan::Scan);
    }

    #[test]
    fn sort_not_equal_degrades_to_scan() {
        let s = select("SELECT * FROM \"t\" WHERE pk = 'a' AND sk <> 'b'");
        assert_eq!(classify(&s, KEYS), SelectPlan::Scan);
    }

    #[test]
    fn or_across_keys_is_a_scan() {
        let s = select("SELECT * FROM \"t\" WHERE pk = 'a' OR sk = 'b'");
        assert_eq!(classify(&s, KEYS), SelectPlan::Scan);
    }

    #[test]
    fn non_key_filter_under_pk_equality_is_a_query() {
        let s = select("SELECT * FROM \"t\" WHERE pk = 'a' AND name = 'x'");
        assert_eq!(
            classify(&s, KEYS),
            SelectPlan::Query {
                has_sort_condition: false
            }
        );
    }
}
