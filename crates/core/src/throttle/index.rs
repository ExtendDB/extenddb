// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The buckets of a table's provisioned global secondary indexes.
//!
//! A provisioned global secondary index has its own read and write capacity.
//! The service refuses a `Query` or `Scan` that names the index when the
//! index's read capacity is exhausted, and a base-table write when an index's
//! write capacity is (measured against Amazon DynamoDB on 2026-10-24 and
//! 2026-10-25). A local secondary index shares the table's capacity and has
//! no buckets here.

use std::time::Instant;

use super::bucket::TokenBucket;

/// Throughput of one provisioned global secondary index, used to create or
/// update its buckets.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexThroughput {
    /// The index name, as `DescribeTable` lists it.
    pub index_name: String,
    /// Read capacity units per second.
    pub rcu: f64,
    /// Write capacity units per second.
    pub wcu: f64,
}

impl IndexThroughput {
    /// Throughput of a provisioned index.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // CU values are small enough for f64
    pub fn provisioned(index_name: impl Into<String>, rcu: i64, wcu: i64) -> Self {
        Self {
            index_name: index_name.into(),
            rcu: rcu as f64,
            wcu: wcu as f64,
        }
    }
}

/// One index's bucket pair.
pub(super) struct IndexBuckets {
    pub(super) index_name: String,
    pub(super) read: TokenBucket,
    pub(super) write: TokenBucket,
}

/// The index set `throughputs` describes, built from `previous`: an index
/// already in `previous` keeps its balance and takes the new rates, a new one
/// starts with one second of its rates as a new table's buckets do, and one
/// absent from `throughputs` is dropped. The result is in the order of
/// `throughputs`.
pub(super) fn replace(
    mut previous: Vec<IndexBuckets>,
    throughputs: Vec<IndexThroughput>,
) -> Vec<IndexBuckets> {
    throughputs
        .into_iter()
        .map(
            |t| match previous.iter().position(|b| b.index_name == t.index_name) {
                Some(i) => {
                    let mut kept = previous.swap_remove(i);
                    kept.read.update_rate(t.rcu);
                    kept.write.update_rate(t.wcu);
                    kept
                }
                None => IndexBuckets {
                    index_name: t.index_name,
                    read: TokenBucket::new(t.rcu),
                    write: TokenBucket::new(t.wcu),
                },
            },
        )
        .collect()
}

/// Whether the index named `name` has read capacity at `now`: `None` when no
/// index in `set` has that name (the read is the table's), `Some(false)` when
/// the index refuses.
pub(super) fn read_allowed(set: &mut [IndexBuckets], name: &str, now: Instant) -> Option<bool> {
    set.iter_mut()
        .find(|b| b.index_name == name)
        .map(|b| b.read.has_capacity(now))
}

/// The first index in `set`, in registration order, whose write bucket is
/// empty at `now`.
pub(super) fn first_write_exhausted(set: &mut [IndexBuckets], now: Instant) -> Option<&str> {
    for buckets in set.iter_mut() {
        if !buckets.write.has_capacity(now) {
            return Some(buckets.index_name.as_str());
        }
    }
    None
}

/// Charge a served request to the index buckets: `read_units` to the read
/// bucket of the index `index_name` names when it is in `set`, `write_units`
/// to every index's write bucket. Returns whether a read was charged to an
/// index, in which case the table's read bucket is not charged.
pub(super) fn charge(
    set: &mut [IndexBuckets],
    index_name: Option<&str>,
    read_units: f64,
    write_units: f64,
    now: Instant,
) -> bool {
    let mut index_read = false;
    for buckets in set.iter_mut() {
        if read_units > 0.0 && index_name == Some(buckets.index_name.as_str()) {
            buckets.read.consume(read_units, now);
            index_read = true;
        }
        if write_units > 0.0 {
            buckets.write.consume(write_units, now);
        }
    }
    index_read
}
