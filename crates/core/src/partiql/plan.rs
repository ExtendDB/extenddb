// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How a WHERE clause reads its target (a table or an index).
//!
//! The service reads a WHERE clause as an OR of disjuncts. It splits at every
//! OR with a condition on a key attribute on either side, and at every IN on a
//! key attribute (one disjunct per value), and AND distributes over the split.
//! It then takes the disjuncts in order (measured 2026-11-02):
//!
//! * A disjunct reads by key when it holds one distinct equality of the
//!   partition key with a literal or parameter and no other predicate on the
//!   partition key, and on the sort key either nothing or one distinct key
//!   condition (`=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with` with
//!   values) and nothing else. A value on the left is the mirrored
//!   comparison: `1 < sk` is `sk > 1`.
//! * Any other predicate on a key attribute (`<>`, `NOT` over one condition,
//!   a function of it, a second value, an IN with a path in its list) makes
//!   the disjunct a scan, and the first disjunct that is a scan makes the
//!   whole statement one: nothing after it is examined.
//! * A key attribute compared with another path (`sk = a`), a path into a key
//!   attribute (`sk.x`), and a NOT over an AND or an OR that names a key
//!   attribute, are not predicates on the key: they stay in the disjunct's
//!   filter, which a keyed read refuses.
//! * A key condition (an equality of the partition key with a value, a key
//!   condition on the sort key, or an AND of them) compared with a boolean
//!   literal is read by that key condition, and the literal alone decides:
//!   compared with true (`= true` or `<> true`) the disjunct returns every
//!   item it reads, compared with false (`= false` or `<> false`) none
//!   (measured 2026-11-03: `pk = 'a' <> true` returns partition a, `pk = 'a'
//!   AND sk > 'x' <> true` the items after x, `(pk = 'a' AND sk = 'x') <>
//!   true` a/x, `pk = 'a' <> false` and `pk = 'a' AND sk = 'x' = false`
//!   nothing, and `pk = 'a' = false OR pk = 'b'` partition b). Any other
//!   condition compared with a boolean is the condition, or its negation for
//!   `= false` and `<> true` (measured 2026-11-03: `pk IN ['a'] <> true` and
//!   `pk = 'a' AND n = 1 = false` read as NOTs).
//!
//! [`WhereAnalysis`] holds that reading. The SELECT executor, the index rules,
//! the point reads of `BatchExecuteStatement` and `ExecuteTransaction` and the
//! overlap refusal all work from it; [`key_equality`] gives a write's WHERE
//! the same reading of a single condition.

use std::cmp::Ordering;
use std::collections::HashMap;

use super::ast::{BooleanComparison, CompareOp, Operand, WhereExpr};
use crate::types::AttributeValue;

mod ranges;
mod roles;
mod split;

use ranges::{Bound, Interval, interval};
use roles::{Role, is_key_condition, role};
use split::{Split, key_read, split};

/// The key attribute names of the target (table or index).
#[derive(Debug, Clone, Copy)]
pub struct KeyNames<'a> {
    pub partition: &'a str,
    pub sort: Option<&'a str>,
}

impl KeyNames<'_> {
    fn is_key(&self, name: &str) -> bool {
        name == self.partition || Some(name) == self.sort
    }
}

/// A key condition on the sort key, written with the key on the left.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortCondition<'a> {
    /// `sk <op> value` for `=`, `<`, `<=`, `>` and `>=`.
    Compare(CompareOp, &'a Operand),
    /// `sk BETWEEN low AND high`.
    Between(&'a Operand, &'a Operand),
    /// `begins_with(sk, prefix)`.
    BeginsWith(&'a Operand),
}

impl<'a> SortCondition<'a> {
    /// The values the condition compares the sort key with, in statement order.
    #[must_use]
    pub fn values(&self) -> Vec<&'a Operand> {
        match *self {
            Self::Compare(_, value) | Self::BeginsWith(value) => vec![value],
            Self::Between(low, high) => vec![low, high],
        }
    }
}

/// A disjunct the service reads by key.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyRead<'a> {
    /// The value the partition key equals.
    pub partition: &'a Operand,
    /// The key condition on the sort key, when the disjunct has one.
    pub sort: Option<SortCondition<'a>>,
    /// The disjunct's other conditions, in statement order.
    pub filter: Vec<&'a WhereExpr>,
    /// The comparisons with a boolean its key conditions are under, in
    /// statement order (see the module documentation).
    pub comparisons: Vec<BooleanComparison>,
}

impl KeyRead<'_> {
    /// Whether the disjunct returns no item of those it reads: one of its
    /// key conditions is compared with false.
    #[must_use]
    pub fn returns_nothing(&self) -> bool {
        self.comparisons
            .iter()
            .any(|comparison| !comparison.literal)
    }
}

/// The disjuncts of a WHERE clause, read as the service reads them.
#[derive(Debug, Clone)]
pub struct WhereAnalysis<'a> {
    /// The WHERE clause read.
    condition: &'a WhereExpr,
    /// The disjuncts read by key, in statement order, up to the first one
    /// that is a scan.
    reads: Vec<KeyRead<'a>>,
    /// Whether every disjunct reads by key.
    keyed: bool,
    /// How many disjuncts the WHERE splits into, at most `u64::MAX`.
    disjuncts: u64,
}

impl<'a> WhereAnalysis<'a> {
    /// Read `where_clause` against the target's key names. `value_of`
    /// resolves a literal or a bound parameter to its value and returns
    /// `None` for anything else; two equalities on one key are the same
    /// condition when their values are equal.
    ///
    /// Past [`MAX_BRANCHES`] disjuncts only the first [`MAX_BRANCHES`] are
    /// read; the statement reads by key when every disjunct does, those past
    /// the first [`MAX_BRANCHES`] too, and its disjuncts are counted to the
    /// last.
    #[must_use]
    pub fn new(
        where_clause: &'a WhereExpr,
        keys: KeyNames<'_>,
        value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
    ) -> Self {
        let Split {
            branches,
            complete,
            tally,
        } = split(where_clause, keys, value_of);
        let mut reads = Vec::with_capacity(branches.len());
        for branch in &branches {
            match key_read(&branch.conjuncts, value_of) {
                Some(read) => reads.push(read),
                None => {
                    return Self {
                        condition: where_clause,
                        reads,
                        keyed: false,
                        disjuncts: tally.count,
                    };
                }
            }
        }
        Self {
            condition: where_clause,
            keyed: (complete || tally.reads_by_key()) && !reads.is_empty(),
            reads,
            disjuncts: tally.count,
        }
    }

    /// How many disjuncts the WHERE splits into, those past the first
    /// [`MAX_BRANCHES`] too (at most `u64::MAX`): one for each value of an
    /// IN on a key attribute and for each side of an OR that splits,
    /// multiplied across an AND.
    #[must_use]
    pub fn disjunct_count(&self) -> u64 {
        self.disjuncts
    }

    /// The refusal of a WHERE of a table read that reads a path into a key
    /// attribute: in the filter of a disjunct read by key, naming the first
    /// such disjunct (see [`scalar_key_refusal`]), else in any condition of
    /// the WHERE, naming the first in statement order. Measured 2026-11-02:
    /// `sk.x = 1` alone and `x = 1 OR (pk = 'a' AND sk.x = 1)` are refused,
    /// as `pk = 'a' AND sk.x = 1` is. A NOT over an AND or an OR is one
    /// condition here, as it is to the key reading: a path into a key under
    /// it gets the filter refusal of a disjunct read by key instead.
    #[must_use]
    pub fn path_into_key_refusal(&self, keys: KeyNames<'_>) -> Option<String> {
        self.reads
            .iter()
            .find_map(|read| scalar_key_refusal(&read.filter, keys))
            .or_else(|| first_path_into_key(self.condition, keys).map(scalar_message))
    }

    /// The disjuncts read by key before the first one that is a scan: every
    /// disjunct when the statement reads by key. The service checks each of
    /// these in order.
    #[must_use]
    pub fn reads(&self) -> &[KeyRead<'a>] {
        &self.reads
    }

    /// Whether every disjunct reads by key, so the statement is not a scan.
    #[must_use]
    pub fn is_keyed(&self) -> bool {
        self.keyed
    }

    /// The one key read, when the statement reads by key with one disjunct.
    #[must_use]
    pub fn single(&self) -> Option<&KeyRead<'a>> {
        match self.reads.as_slice() {
            [read] if self.keyed => Some(read),
            _ => None,
        }
    }

    /// The filter conditions every disjunct shares, in statement order, when
    /// the statement reads by key: the conditions beside the key conditions
    /// at the top of the WHERE. A condition inside one OR branch is not among
    /// them (measured 2026-11-02 through an index: an unprojected attribute
    /// beside an IN on the index key is refused, inside one OR branch it is
    /// not).
    #[must_use]
    pub fn shared_filter(&self) -> Option<Vec<&'a WhereExpr>> {
        if !self.keyed {
            return None;
        }
        let mut seen: HashMap<*const WhereExpr, usize> = HashMap::new();
        for read in &self.reads {
            let mut once: Vec<*const WhereExpr> = Vec::new();
            for term in &read.filter {
                let id: *const WhereExpr = *term;
                if !once.contains(&id) {
                    once.push(id);
                    *seen.entry(id).or_default() += 1;
                }
            }
        }
        let first = self.reads.first()?;
        Some(
            first
                .filter
                .iter()
                .copied()
                .filter(|term| {
                    let id: *const WhereExpr = *term;
                    seen.get(&id) == Some(&self.reads.len())
                })
                .collect(),
        )
    }

    /// Refuse a statement whose disjuncts read overlapping parts of one
    /// partition.
    ///
    /// When every disjunct reads by key, two on the same partition whose
    /// sort-key ranges overlap are refused; a disjunct with no sort-key
    /// condition covers the whole partition. A statement that is a scan
    /// refuses nothing. Measured 2026-11-02: a disjunct with a non-key filter
    /// beside a sort-key equality disjunct on the same partition is refused,
    /// and a disjunct whose sort-key condition is negated or written with
    /// `<>` makes the statement run.
    ///
    /// # Errors
    ///
    /// Returns [`OVERLAP_MESSAGE`] for overlapping disjuncts.
    pub fn check_overlap(
        &self,
        value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
    ) -> Result<(), &'static str> {
        if !self.keyed {
            return Ok(());
        }
        let mut reads: Vec<(Bound, Interval)> = Vec::new();
        for read in &self.reads {
            let Some(partition) = value_of(read.partition)
                .as_ref()
                .and_then(Bound::from_value)
            else {
                return Ok(());
            };
            let interval = match &read.sort {
                Some(condition) => match interval(condition, value_of) {
                    Some(interval) => interval,
                    None => return Ok(()),
                },
                None => Interval::UNBOUNDED,
            };
            reads.push((partition, interval));
        }
        let mut partitions: Vec<&Bound> = Vec::new();
        for (partition, _) in &reads {
            if !partitions.iter().any(|p| p.same(partition)) {
                partitions.push(partition);
            }
        }
        for partition in partitions {
            let mut intervals: Vec<&Interval> = reads
                .iter()
                .filter(|(p, _)| p.same(partition))
                .map(|(_, i)| i)
                .collect();
            if intervals.len() < 2 {
                continue;
            }
            // Every bound must be comparable with every other for the order
            // below to mean anything; a key value of the wrong type is
            // reported before this check.
            let comparable = intervals.iter().all(|a| {
                intervals
                    .iter()
                    .all(|b| Interval::cmp_lower(a, b).is_some() && a.overlaps(b).is_some())
            });
            if !comparable {
                return Ok(());
            }
            intervals.sort_by(|a, b| Interval::cmp_lower(a, b).unwrap_or(Ordering::Equal));
            if intervals
                .windows(2)
                .any(|pair| pair[0].overlaps(pair[1]) == Some(true))
            {
                return Err(OVERLAP_MESSAGE);
            }
        }
        Ok(())
    }
}

/// The service's refusal of a `SELECT` whose OR branches read overlapping
/// sort-key ranges of one partition (measured 2026-11-02).
pub const OVERLAP_MESSAGE: &str =
    "Overlapping conditions with range keys are not supported in where clause";

/// The refusal of a keyed read whose filter compares a key attribute with
/// another path, naming the key of the first such condition (measured
/// 2026-11-02: `pk = 'a' AND sk = a`, `a = sk`, `sk = pk` and `pk = a` name
/// the key compared).
#[must_use]
pub fn filter_key_refusal(filter: &[&WhereExpr], keys: KeyNames<'_>) -> Option<String> {
    filter.iter().find_map(|term| match role(term, keys) {
        Role::KeyAndPath(key) => Some(format!(
            "Filter Expression can only contain non-primary key attributes: Primary key \
             attribute: {key}"
        )),
        _ => None,
    })
}

/// The refusal of a keyed read whose filter reads a path into a key
/// attribute, naming the key of the first such condition (measured
/// 2026-11-02: `sk.x = 1` and `sk[0] = 1` beside `pk = 'a'`).
#[must_use]
pub fn scalar_key_refusal(filter: &[&WhereExpr], keys: KeyNames<'_>) -> Option<String> {
    filter.iter().find_map(|term| match role(term, keys) {
        Role::IntoKey(key) => Some(scalar_message(key)),
        _ => None,
    })
}

/// The refusal of a path into a key attribute, before the key's name.
pub const SCALAR_KEY_REFUSAL: &str = "Key attributes must be scalars; list random access '[]' and map lookup '.' are not \
     allowed: Key ";

/// The refusal of a path into a key attribute among the conditions a write
/// checks against its item, before the key's name (see
/// [`condition_key_path`]).
pub const CONDITION_KEY_REFUSAL: &str = "Key attributes must be scalars; list random access '[]' and map lookup '.' are \
     not allowed: Key: ";

/// The key attribute of a condition of a write's WHERE that tests a path
/// into it against values alone: a comparison with a value on either side,
/// BETWEEN with values, IN with a list of values, begins_with, contains or
/// attribute_type with a value as the second argument, an IS test of a
/// type, or one of these compared with a boolean (measured 2026-11-05 on
/// UPDATE and DELETE: `pk.c = 1`, `1 = pk.c`, `sk[0] = 'z'`, `pk.c BETWEEN 1
/// AND 2`, `pk.c IN [1]`, `begins_with(pk.c, 'x')`, `pk.c IS NULL` and `pk.c
/// = 1 = false` are refused with [`SCALAR_KEY_REFUSAL`]). `None` for any
/// other condition.
#[must_use]
pub fn path_tested_against_values<'a>(expr: &'a WhereExpr, keys: KeyNames<'_>) -> Option<&'a str> {
    roles::value_test_of_path_into_key(expr, keys)
}

/// The key attribute the refusal of a path into a key attribute among a
/// write's other conditions names: the sort key when any of `conditions`
/// reads a path into it, else the partition key when one reads a path into
/// that (measured 2026-11-05: `NOT pk.c = 1 AND NOT sk.d = 1`, either way
/// round, and `pk.c = sk.d` name sk; `pk.c = 1 OR sk = 'b'` names pk).
/// These are the conditions [`path_tested_against_values`] does not take:
/// OR, NOT, a function of the path, IS MISSING, IS NOT, and the path
/// compared with another path.
#[must_use]
pub fn condition_key_path<'a>(conditions: &[&'a WhereExpr], keys: KeyNames<'_>) -> Option<&'a str> {
    let mut named = Vec::new();
    for condition in conditions {
        roles::keys_with_paths_into_them(condition, keys, &mut named);
    }
    named
        .iter()
        .find(|name| keys.sort == Some(**name))
        .or_else(|| named.first())
        .copied()
}

/// The refusal of a path into the key attribute `key`.
fn scalar_message(key: &str) -> String {
    format!("{SCALAR_KEY_REFUSAL}{key}")
}

/// The key attribute of the first condition of a WHERE, in statement order,
/// that reads a path into a key attribute. The walk goes through AND and OR
/// only, with its own stack, and asks each other condition its role.
fn first_path_into_key<'a>(expr: &'a WhereExpr, keys: KeyNames<'_>) -> Option<&'a str> {
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            WhereExpr::And(a, b) | WhereExpr::Or(a, b) => {
                pending.push(b);
                pending.push(a);
            }
            condition => {
                if let Role::IntoKey(key) = role(condition, keys) {
                    return Some(key);
                }
            }
        }
    }
    None
}

/// The order in which two disjuncts read by key are read: by partition
/// value, then by where their sort-key ranges start (a disjunct with no
/// sort-key condition, or one with no lower bound, first; on an equal start
/// an inclusive one first). Measured 2026-11-02: `(pk = 'b' AND sk = 'x') OR
/// (pk = 'a' AND sk > 'a')` reads a/x and a/y before b/x. Values that do not
/// resolve, or that have two types, compare equal.
#[must_use]
pub fn read_order(
    a: &KeyRead<'_>,
    b: &KeyRead<'_>,
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> Ordering {
    read_order_in(a, b, Direction::default(), value_of)
}

/// The directions of a read under an ORDER BY: of its partitions, and of
/// the items of one partition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Direction {
    pub partitions_descending: bool,
    pub items_descending: bool,
}

/// [`read_order`] with the partitions, and the sort-key ranges of one
/// partition, in the directions `direction` gives.
#[must_use]
pub fn read_order_in(
    a: &KeyRead<'_>,
    b: &KeyRead<'_>,
    direction: Direction,
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> Ordering {
    let directed = |order: Ordering, descending: bool| {
        if descending { order.reverse() } else { order }
    };
    let bound = |op: &Operand| value_of(op).as_ref().and_then(Bound::from_value);
    let partitions = match (bound(a.partition), bound(b.partition)) {
        (Some(x), Some(y)) => x.compare(&y).unwrap_or(Ordering::Equal),
        _ => Ordering::Equal,
    };
    directed(partitions, direction.partitions_descending).then_with(|| {
        let range = |read: &KeyRead<'_>| match &read.sort {
            None => Some(Interval::UNBOUNDED),
            Some(condition) => interval(condition, value_of),
        };
        let ranges = match (range(a), range(b)) {
            (Some(x), Some(y)) => Interval::cmp_lower(&x, &y).unwrap_or(Ordering::Equal),
            _ => Ordering::Equal,
        };
        directed(ranges, direction.items_descending)
    })
}

/// A WHERE of a statement read by key, as a filter: each key condition
/// compared with a boolean (see the module documentation) is the key
/// condition when compared with true, and holds for no item when compared
/// with false. The walk follows the AND and OR nesting, as [`split`] does.
#[must_use]
pub fn keyed_condition(expr: &WhereExpr, keys: KeyNames<'_>) -> WhereExpr {
    match expr {
        WhereExpr::And(a, b) => WhereExpr::And(
            Box::new(keyed_condition(a, keys)),
            Box::new(keyed_condition(b, keys)),
        ),
        WhereExpr::Or(a, b) => WhereExpr::Or(
            Box::new(keyed_condition(a, keys)),
            Box::new(keyed_condition(b, keys)),
        ),
        WhereExpr::ComparedWithBoolean(inner, comparison) if is_key_condition(inner, keys) => {
            let inner = keyed_condition(inner, keys);
            if comparison.literal {
                inner
            } else {
                let never = WhereExpr::Not(Box::new(inner.clone()));
                WhereExpr::And(Box::new(inner), Box::new(never))
            }
        }
        WhereExpr::ComparedWithBoolean(inner, comparison) if !comparison.negates() => {
            keyed_condition(inner, keys)
        }
        other => other.clone(),
    }
}

/// The key attribute a single WHERE condition pins by equality.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeyEquality<'a> {
    /// The partition key equals this value.
    Partition(&'a Operand),
    /// The sort key equals this value.
    Sort(&'a Operand),
}

/// `term` as an equality of a key attribute with a literal or parameter,
/// written either way round (`pk = 'a'` or `'a' = pk`).
#[must_use]
pub fn key_equality<'a>(term: &'a WhereExpr, keys: KeyNames<'_>) -> Option<KeyEquality<'a>> {
    match role(term, keys) {
        Role::Partition(value) => Some(KeyEquality::Partition(value)),
        Role::Sort(SortCondition::Compare(CompareOp::Eq, value)) => Some(KeyEquality::Sort(value)),
        _ => None,
    }
}

/// Whether two value operands name the same key value: equal values (a
/// number compared by its value), or the same operand when either does not
/// resolve.
#[must_use]
pub fn same_value(
    a: &Operand,
    b: &Operand,
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> bool {
    match (value_of(a), value_of(b)) {
        (Some(x), Some(y)) => match (Bound::from_value(&x), Bound::from_value(&y)) {
            (Some(x), Some(y)) => x.same(&y),
            _ => x == y,
        },
        _ => a == b,
    }
}

/// The most disjuncts of a WHERE that are listed. AND distributes over OR,
/// so the count multiplies: k ANDed pairs of ORed key equalities make 2^k
/// disjuncts. The ones past these are counted, and whether they read by key
/// is known, without listing them (see [`WhereAnalysis::new`]).
pub const MAX_BRANCHES: usize = 1024;

#[cfg(test)]
mod tests;
