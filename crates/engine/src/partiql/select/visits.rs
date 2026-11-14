// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The order in which the pages of a read of disjuncts one after another
//! visit them, and the list each page leaves for the next (measured
//! 2026-11-03).
//!
//! The disjuncts are numbered 0 to N-1 in read order (key order, or the
//! order an ORDER BY gives), the ones held back for a refusal included (see
//! `Step::all`). The first page takes the list 0 to N-1. Each page visits
//! the list the page before it left in the order of [`visit_order`], visits
//! at most [`PAGE_VISITS`] disjuncts, and stops once its `Limit` is spent or
//! its byte budget is reached (see `read_steps`). The list it leaves holds
//! the disjuncts it visited and did not read to their end, in the order it
//! visited them, then the ones it did not visit, in their order:
//!
//! * a disjunct held back for a refusal takes a visit, no unit of `Limit`
//!   and no read, and stays in the list, so it takes a visit again on every
//!   later page that reaches it;
//! * a Query the page stops inside stays in the list, and goes on from
//!   where it stopped when a later page reaches it, which need not be
//!   first.
//!
//! When the list is empty there is no NextToken. A request whose page
//! visits only disjuncts held back gets their refusal instead of a page,
//! also when the page leaves other disjuncts unvisited, so once only
//! disjuncts held back are left, the next request gets it. Measured on
//! tables, an index, the sort key values of one partition and `ORDER BY pk
//! DESC`: 21 point reads give the items of positions 0 to 9, then 16 to 20
//! and 10 to 14, then 15; with `Limit` 3, [0 1 2] [3 4 5] [6 7 8] [16 17
//! 18] [19 20 9] [10 11 12] [13 14 15]; 32 point reads beside a value past
//! the key size limit at position 12 give [0..9], [32 10 11 13..18],
//! [19..27] and [28..31], each later page one item short for the visit the
//! held-back value takes, then the refusal; and 18 point reads beside a
//! Query of 21 items at position 10, with `Limit` 6, give on the page after
//! the Query stopped positions 16, 17 and 18 and then 3 items of the Query.
//! With the filter refusal held back for positions 0 to 9 of 21, the first
//! request is refused, with and without `Limit` 3; for 10 to 19 of 25, the
//! first page holds 0 to 9 and the second request is refused; for 0 to 8
//! of 21, nine pages of one item and a page of three come before the
//! refusal.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;

use super::token_mismatch;

/// The most disjuncts a page visits, whatever its `Limit` (measured
/// 2026-11-03: `pk IN [...]` with 11 values of items that exist gives 10
/// items and then 1, with 25 values 10, 10 and 5, with `Limit` 100 or 15
/// too). A Query the page goes on inside is one of them (measured
/// 2026-11-03: with partition a of 13 items before 20 partitions of one
/// item, `Limit` 12 gives 12 items, then the last item of a and 9
/// partitions, then 10 partitions, then 1).
pub(super) const PAGE_VISITS: usize = 10;

/// The order in which a page visits `left`, the positions the page before
/// it left, in the order it left them: by position modulo a bucket count,
/// and in list order within one bucket. The bucket count is 16, doubled
/// while the list holds more than three quarters of it, so it is the order
/// in which a hash set of the positions with that many buckets lists them
/// when they are added in list order. The count is larger than the list,
/// so the first page takes the disjuncts in read order, and so does every
/// page whose list holds no position at or above the count (measured
/// 2026-11-03: after a first page of 0 to 9 of 21 disjuncts, the list 10 to
/// 20 is visited 16 to 20, then 10 to 15; of 23 to 31 disjuncts, in read
/// order).
pub(super) fn visit_order(left: &[usize]) -> Vec<usize> {
    let mut buckets = 16;
    while left.len() > buckets * 3 / 4 {
        buckets *= 2;
    }
    let mut order = left.to_vec();
    order.sort_by_key(|position| position % buckets);
    order
}

/// Where a read of disjuncts one after another goes on, as a NextToken
/// holds it.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Cursor {
    /// The positions of the disjuncts left, in the order the last page left
    /// them: at most the number of disjuncts, which is at most 50.
    left: Vec<usize>,
    /// The key from which the Query of each position a page stopped inside
    /// goes on.
    after: BTreeMap<usize, Value>,
}

impl Cursor {
    /// The cursor of the first page of a read of `count` disjuncts.
    pub(super) fn first(count: usize) -> Self {
        Self {
            left: (0..count).collect(),
            after: BTreeMap::new(),
        }
    }

    /// The cursor as a NextToken holds it.
    pub(super) fn into_value(self) -> Value {
        let after: Vec<Value> = self
            .after
            .into_iter()
            .map(|(position, key)| Value::Array(vec![Value::from(position), key]))
            .collect();
        let mut out = Map::new();
        out.insert("left".to_owned(), Value::from(self.left));
        out.insert("after".to_owned(), Value::Array(after));
        Value::Object(out)
    }

    /// The cursor a NextToken holds for a read of `count` disjuncts.
    ///
    /// # Errors
    ///
    /// The service's refusal of a NextToken this statement did not give,
    /// for a value that is no list of distinct positions below `count`, or
    /// that holds a key for a position not in the list.
    pub(super) fn from_value(value: &Value, count: usize) -> Result<Self, DynamoDbError> {
        let position = |value: &Value| {
            value
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .filter(|position| *position < count)
        };
        let left: Vec<usize> = value
            .get("left")
            .and_then(Value::as_array)
            .and_then(|left| left.iter().map(position).collect())
            .ok_or_else(token_mismatch)?;
        let mut listed = vec![false; count];
        for &at in &left {
            if std::mem::replace(&mut listed[at], true) {
                return Err(token_mismatch());
            }
        }
        let mut after = BTreeMap::new();
        for entry in value
            .get("after")
            .and_then(Value::as_array)
            .ok_or_else(token_mismatch)?
        {
            let (Some(at), Some(key)) = (entry.get(0).and_then(position), entry.get(1)) else {
                return Err(token_mismatch());
            };
            if !listed[at] || after.insert(at, key.clone()).is_some() {
                return Err(token_mismatch());
            }
        }
        Ok(Self { left, after })
    }
}

/// A disjunct a page reads, and the key its Query goes on from.
pub(super) struct Visit {
    pub(super) position: usize,
    pub(super) start: Option<Value>,
}

/// How the read of a disjunct a page visits ended.
pub(super) enum Read {
    /// Read to its end, with this many units of `Limit` spent.
    Done(usize),
    /// Read to its end, and the page ends after it.
    DoneAndEnd,
    /// The page ends inside the disjunct's Query, which goes on from this
    /// key.
    Stopped(Value),
    /// The page ends before the disjunct, which is left as if it had not
    /// been visited.
    Unread,
}

/// How a page ends.
pub(super) enum Ending {
    /// Every visit of the page went to a disjunct held back for a refusal:
    /// the request gets the refusal instead of the page.
    Refused,
    /// Every disjunct is read: the page has no NextToken.
    Last,
    /// The next page goes on from this cursor.
    More(Cursor),
}

/// The visits of one page.
pub(super) struct Walk<'h> {
    /// Whether each position is held back for a refusal.
    held_back: &'h [bool],
    /// The order the page visits the positions left.
    order: Vec<usize>,
    /// The index in `order` of the next position to visit.
    next: usize,
    visits: usize,
    /// Whether a visit of the page went to a disjunct that is not held back.
    read: bool,
    /// The units of `Limit` left, or `None` without a `Limit`.
    units: Option<usize>,
    /// The positions visited and left in the list, in the order visited.
    kept: Vec<usize>,
    after: BTreeMap<usize, Value>,
    ended: bool,
}

impl<'h> Walk<'h> {
    /// The visits of the page that goes on from `cursor`, with `limit`
    /// units of `Limit`. `held_back` tells, by position, which disjuncts
    /// are held back for a refusal.
    pub(super) fn new(cursor: Cursor, held_back: &'h [bool], limit: Option<usize>) -> Self {
        Self {
            held_back,
            order: visit_order(&cursor.left),
            next: 0,
            visits: 0,
            read: false,
            units: limit,
            kept: Vec::new(),
            after: cursor.after,
            ended: false,
        }
    }

    /// The units of `Limit` the page has left, or `None` without a `Limit`.
    pub(super) fn units(&self) -> Option<usize> {
        self.units
    }

    /// The next disjunct the page reads, or `None` once the page is over. A
    /// disjunct held back for a refusal takes its visit here, without a
    /// read.
    pub(super) fn next_read(&mut self) -> Option<Visit> {
        while !self.ended && self.units != Some(0) && self.visits < PAGE_VISITS {
            let position = *self.order.get(self.next)?;
            self.next += 1;
            self.visits += 1;
            if self.held_back[position] {
                self.kept.push(position);
                continue;
            }
            self.read = true;
            return Some(Visit {
                position,
                start: self.after.get(&position).cloned(),
            });
        }
        None
    }

    /// Record how the read of `position`, the disjunct [`Walk::next_read`]
    /// gave last, ended.
    pub(super) fn record(&mut self, position: usize, read: Read) {
        match read {
            Read::Done(units) => {
                self.after.remove(&position);
                self.units = self.units.map(|left| left.saturating_sub(units));
            }
            Read::DoneAndEnd => {
                self.after.remove(&position);
                self.ended = true;
            }
            Read::Stopped(key) => {
                self.after.insert(position, key);
                self.kept.push(position);
                self.ended = true;
            }
            Read::Unread => {
                self.next -= 1;
                self.ended = true;
            }
        }
    }

    /// How the page ends: refused when every one of its visits went to a
    /// disjunct held back, else with what it leaves, the positions it kept,
    /// in the order it visited them, then those it did not visit, in their
    /// order.
    pub(super) fn finish(self) -> Ending {
        if self.visits > 0 && !self.read {
            return Ending::Refused;
        }
        let mut left = self.kept;
        left.extend_from_slice(&self.order[self.next..]);
        if left.is_empty() {
            return Ending::Last;
        }
        let after = self
            .after
            .into_iter()
            .filter(|(position, _)| left.contains(position))
            .collect();
        Ending::More(Cursor { left, after })
    }
}

#[cfg(test)]
mod tests;
