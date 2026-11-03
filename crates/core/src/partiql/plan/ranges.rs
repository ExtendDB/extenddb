// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The sort-key range a key condition reads, and the key values that bound it.

use std::cmp::Ordering;
use std::str::FromStr;

use bigdecimal::BigDecimal;

use super::SortCondition;
use crate::partiql::ast::{CompareOp, Operand};
use crate::types::AttributeValue;
use crate::validation::number::validate_and_normalize_number;

/// The sort-key range a key condition reads, or `None` when a value does not
/// resolve to a key value.
pub(super) fn interval(
    condition: &SortCondition<'_>,
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> Option<Interval> {
    let bound = |op: &Operand| value_of(op).as_ref().and_then(Bound::from_value);
    Some(match *condition {
        SortCondition::Compare(op, value) => {
            let v = bound(value)?;
            match op {
                CompareOp::Eq => Interval::new(Some(v.clone()), true, Some(v), true),
                CompareOp::Ge => Interval::new(Some(v), true, None, false),
                CompareOp::Gt => Interval::new(Some(v), false, None, false),
                CompareOp::Le => Interval::new(None, false, Some(v), true),
                CompareOp::Lt => Interval::new(None, false, Some(v), false),
                CompareOp::Ne => return None,
            }
        }
        SortCondition::Between(low, high) => {
            Interval::new(Some(bound(low)?), true, Some(bound(high)?), true)
        }
        // Every key that starts with the prefix: from the prefix up to the
        // prefix with its last byte raised by one.
        SortCondition::BeginsWith(prefix) => {
            let low = bound(prefix)?;
            let high = match &low {
                Bound::S(bytes) => next_prefix(bytes).map(Bound::S),
                Bound::B(bytes) => next_prefix(bytes).map(Bound::B),
                // A number has no prefix.
                Bound::N(_) => return None,
            };
            Interval::new(Some(low), true, high, false)
        }
    })
}

/// A comparable key value.
#[derive(Debug, Clone)]
pub(super) enum Bound {
    S(Vec<u8>),
    N(BigDecimal),
    B(Vec<u8>),
}

impl Bound {
    pub(super) fn from_value(value: &AttributeValue) -> Option<Self> {
        match value {
            AttributeValue::S(s) => Some(Self::S(s.as_bytes().to_vec())),
            AttributeValue::B(b) => Some(Self::B(b.clone())),
            AttributeValue::N(n) => {
                let normalized = validate_and_normalize_number(n).ok()?;
                BigDecimal::from_str(&normalized).ok().map(Self::N)
            }
            _ => None,
        }
    }

    pub(super) fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::S(a), Self::S(b)) | (Self::B(a), Self::B(b)) => Some(a.cmp(b)),
            (Self::N(a), Self::N(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    pub(super) fn same(&self, other: &Self) -> bool {
        self.compare(other) == Some(Ordering::Equal)
    }
}

/// The least byte string greater than every string with this prefix: the
/// prefix with its last byte raised by one (carrying), or `None` when there
/// is none.
fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    while let Some(last) = next.pop() {
        if last < u8::MAX {
            next.push(last + 1);
            return Some(next);
        }
    }
    None
}

/// A sort-key range; a missing bound is unbounded.
#[derive(Debug, Clone)]
pub(super) struct Interval {
    min: Option<Bound>,
    min_inclusive: bool,
    max: Option<Bound>,
    max_inclusive: bool,
}

impl Interval {
    pub(super) const UNBOUNDED: Self = Self {
        min: None,
        min_inclusive: false,
        max: None,
        max_inclusive: false,
    };

    fn new(
        min: Option<Bound>,
        min_inclusive: bool,
        max: Option<Bound>,
        max_inclusive: bool,
    ) -> Self {
        Self {
            min,
            min_inclusive,
            max,
            max_inclusive,
        }
    }

    /// Order by lower bound, an unbounded one first; on equal bounds an
    /// inclusive one first.
    pub(super) fn cmp_lower(a: &Self, b: &Self) -> Option<Ordering> {
        match (&a.min, &b.min) {
            (None, None) => Some(Ordering::Equal),
            (None, Some(_)) => Some(Ordering::Less),
            (Some(_), None) => Some(Ordering::Greater),
            (Some(x), Some(y)) => Some(match x.compare(y)? {
                Ordering::Equal => b.min_inclusive.cmp(&a.min_inclusive),
                other => other,
            }),
        }
    }

    /// Whether `next`, which sorts at or after `self`, starts before `self`
    /// ends.
    pub(super) fn overlaps(&self, next: &Self) -> Option<bool> {
        let (Some(end), Some(start)) = (&self.max, &next.min) else {
            return Some(true);
        };
        Some(match end.compare(start)? {
            Ordering::Greater => true,
            Ordering::Equal => self.max_inclusive && next.min_inclusive,
            Ordering::Less => false,
        })
    }
}
