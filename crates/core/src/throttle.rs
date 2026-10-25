// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Token bucket rate limiter for `DynamoDB` provisioned throughput enforcement.
//!
//! Each bucket refills at a steady rate (the provisioned capacity units per
//! second) and allows bursting up to 300 seconds of accumulated capacity,
//! matching real `DynamoDB` behavior.
//!
//! Buckets are purely in-memory operational state — not cached database state.
//! This is compliant with the no-caching rule (see P51 discussion).

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

mod bucket;
mod index;

use bucket::TokenBucket;
pub use index::IndexThroughput;

/// Default on-demand read capacity ceiling per table.
const ON_DEMAND_DEFAULT_RCU: f64 = 12_000.0;

/// Default on-demand write capacity ceiling per table.
const ON_DEMAND_DEFAULT_WCU: f64 = 4_000.0;

/// Per-partition read capacity limit (3000 RCU/s).
const PARTITION_RCU_LIMIT: f64 = 3_000.0;

/// Per-partition write capacity limit (1000 WCU/s).
const PARTITION_WCU_LIMIT: f64 = 1_000.0;

/// Throughput configuration for a table, used to create/update buckets.
#[derive(Debug, Clone, Copy)]
pub struct TableThroughput {
    /// Read capacity units per second.
    pub rcu: f64,
    /// Write capacity units per second.
    pub wcu: f64,
    /// Whether this is an on-demand table.
    pub on_demand: bool,
    /// Whether the read rate is an `OnDemandThroughput` cap set on the table
    /// rather than the default on-demand ceiling. The service names the limit
    /// differently on a throttle (`MaxOnDemandThroughputExceeded`).
    pub read_capped: bool,
    /// Whether the write rate is an `OnDemandThroughput` cap.
    pub write_capped: bool,
}

impl TableThroughput {
    /// Create throughput config for a provisioned table.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // CU values are small enough for f64
    pub fn provisioned(rcu: i64, wcu: i64) -> Self {
        Self {
            rcu: rcu as f64,
            wcu: wcu as f64,
            on_demand: false,
            read_capped: false,
            write_capped: false,
        }
    }

    /// Create throughput config for an on-demand table with default ceilings.
    #[must_use]
    pub fn on_demand() -> Self {
        Self {
            rcu: ON_DEMAND_DEFAULT_RCU,
            wcu: ON_DEMAND_DEFAULT_WCU,
            on_demand: true,
            read_capped: false,
            write_capped: false,
        }
    }

    /// Create throughput config for an on-demand table whose
    /// `OnDemandThroughput` caps are set. A cap that is absent or not positive
    /// (the service uses `-1` to remove one) keeps the default ceiling for
    /// that direction, and only the capped direction reports the caps limit
    /// on a throttle.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // request-unit caps are small enough for f64
    pub fn on_demand_capped(
        max_read_request_units: Option<i64>,
        max_write_request_units: Option<i64>,
    ) -> Self {
        let cap = |units: Option<i64>, default: f64| match units {
            Some(n) if n > 0 => (n as f64, true),
            _ => (default, false),
        };
        let (rcu, read_capped) = cap(max_read_request_units, ON_DEMAND_DEFAULT_RCU);
        let (wcu, write_capped) = cap(max_write_request_units, ON_DEMAND_DEFAULT_WCU);
        Self {
            rcu,
            wcu,
            on_demand: true,
            read_capped,
            write_capped,
        }
    }
}

/// Key for per-table throttle buckets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableKey {
    account_id: String,
    table_name: String,
}

/// Key for per-partition throttle buckets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    account_id: String,
    table_name: String,
    partition_value: String,
}

/// Per-table bucket pair (read + write).
struct TableBuckets {
    read: TokenBucket,
    write: TokenBucket,
    /// Billing mode the table was registered with; carried into the
    /// [`ThrottleCause`] so the server can name the limit the service would.
    on_demand: bool,
    /// Whether each rate is an `OnDemandThroughput` cap.
    read_capped: bool,
    write_capped: bool,
}

/// Account-level aggregate bucket pair.
struct AccountBuckets {
    read: TokenBucket,
    write: TokenBucket,
}

/// Result of a throttle check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThrottleResult {
    /// Request may proceed.
    Allowed,
    /// Request is throttled on reads.
    ThrottledRead(ThrottleCause),
    /// Request is throttled on writes.
    ThrottledWrite(ThrottleCause),
}

impl ThrottleResult {
    /// The bucket that refused, or `None` when the request was allowed.
    #[must_use]
    pub fn cause(&self) -> Option<&ThrottleCause> {
        match self {
            Self::Allowed => None,
            Self::ThrottledRead(c) | Self::ThrottledWrite(c) => Some(c),
        }
    }
}

/// Which bucket refused a request. The service names the throttled resource
/// and the limit on a throttle error (`ThrottlingReasons`); the server maps
/// this cause and the table's billing mode onto that vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThrottleCause {
    /// The table's own bucket: its provisioned capacity, the default ceiling
    /// of an on-demand table, or an `OnDemandThroughput` cap set on one.
    Table {
        /// Whether the table is billed on demand.
        on_demand: bool,
        /// Whether the rate of the bucket that refused (read or write) is an
        /// `OnDemandThroughput` cap.
        capped: bool,
    },
    /// The per-partition-key bucket (a hot key).
    Partition {
        /// Whether the table is billed on demand.
        on_demand: bool,
    },
    /// A provisioned global secondary index's bucket: its read bucket for a
    /// `Query` or `Scan` that names the index, its write bucket for a write to
    /// the base table. Only provisioned indexes have buckets, so the cause
    /// carries no billing mode.
    Index {
        /// The index whose bucket refused.
        index_name: String,
    },
    /// The per-account aggregate bucket.
    Account,
}

impl ThrottleCause {
    /// The index whose bucket refused, for an [`ThrottleCause::Index`] cause.
    #[must_use]
    pub fn index_name(&self) -> Option<&str> {
        match self {
            Self::Index { index_name } => Some(index_name),
            _ => None,
        }
    }
}

/// Thread-safe throttle manager holding all token buckets.
///
/// Uses `RwLock` for the maps. The write lock is held briefly for bucket
/// creation; the read lock suffices for capacity checks (which mutate
/// individual buckets via interior mutability — but since we need `&mut`
/// for `TokenBucket`, we use the write lock for all operations).
pub struct ThrottleManager {
    tables: RwLock<HashMap<TableKey, TableBuckets>>,
    /// Each table's provisioned global secondary indexes, in the order the
    /// table's description lists them. No lock is taken while another of
    /// these maps' locks is held.
    indexes: RwLock<HashMap<TableKey, Vec<index::IndexBuckets>>>,
    partitions: RwLock<HashMap<PartitionKey, TableBuckets>>,
    accounts: RwLock<HashMap<String, AccountBuckets>>,
    account_rcu_limit: f64,
    account_wcu_limit: f64,
    enabled: AtomicBool,
}

impl ThrottleManager {
    /// Create a new throttle manager.
    ///
    /// `account_rcu_limit` and `account_wcu_limit` are the per-account
    /// aggregate limits (from `LimitsConfig`).
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // account limits are small enough for f64
    #[allow(clippy::similar_names)] // rcu_limit vs wcu_limit are intentionally parallel
    pub fn new(account_rcu_limit: u64, account_wcu_limit: u64, enabled: bool) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            partitions: RwLock::new(HashMap::new()),
            accounts: RwLock::new(HashMap::new()),
            account_rcu_limit: account_rcu_limit as f64,
            account_wcu_limit: account_wcu_limit as f64,
            enabled: AtomicBool::new(enabled),
        }
    }

    /// Update the enabled state at runtime (e.g. from the `throttling_enabled`
    /// runtime setting).
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Register or update a table's throughput configuration.
    ///
    /// Called when a table is first accessed or after `UpdateTable` changes
    /// its throughput settings.
    pub fn register_table(&self, account_id: &str, table_name: &str, throughput: TableThroughput) {
        let key = TableKey {
            account_id: account_id.to_owned(),
            table_name: table_name.to_owned(),
        };
        let Ok(mut tables) = self.tables.write() else {
            return;
        };
        if let Some(existing) = tables.get_mut(&key) {
            existing.read.update_rate(throughput.rcu);
            existing.write.update_rate(throughput.wcu);
            existing.on_demand = throughput.on_demand;
            existing.read_capped = throughput.read_capped;
            existing.write_capped = throughput.write_capped;
        } else {
            tables.insert(
                key,
                TableBuckets {
                    read: TokenBucket::new(throughput.rcu),
                    write: TokenBucket::new(throughput.wcu),
                    on_demand: throughput.on_demand,
                    read_capped: throughput.read_capped,
                    write_capped: throughput.write_capped,
                },
            );
        }
    }

    /// Replace a table's set of provisioned global secondary indexes.
    ///
    /// Called with the table's registration, from its description. An index
    /// already registered keeps its balance and takes the new rates, as a
    /// table does on `UpdateTable`; a new one starts with one second of its
    /// rates; one absent from `indexes` loses its buckets, so an `UpdateTable`
    /// that deleted it stops holding back writes. The order of `indexes` is
    /// kept, so a write refused by several indexes names the first in
    /// `DescribeTable` order.
    pub fn register_indexes(
        &self,
        account_id: &str,
        table_name: &str,
        indexes: Vec<IndexThroughput>,
    ) {
        let key = TableKey {
            account_id: account_id.to_owned(),
            table_name: table_name.to_owned(),
        };
        let Ok(mut map) = self.indexes.write() else {
            return;
        };
        let previous = map.remove(&key).unwrap_or_default();
        let set = index::replace(previous, indexes);
        if !set.is_empty() {
            map.insert(key, set);
        }
    }

    /// Remove a table's buckets and its indexes' (called on `DeleteTable`).
    pub fn remove_table(&self, account_id: &str, table_name: &str) {
        let key = TableKey {
            account_id: account_id.to_owned(),
            table_name: table_name.to_owned(),
        };
        if let Ok(mut tables) = self.tables.write() {
            tables.remove(&key);
        }
        if let Ok(mut indexes) = self.indexes.write() {
            indexes.remove(&key);
        }
    }

    /// Check if a table has registered throttle buckets.
    #[must_use]
    pub fn is_registered(&self, account_id: &str, table_name: &str) -> bool {
        let key = TableKey {
            account_id: account_id.to_owned(),
            table_name: table_name.to_owned(),
        };
        self.tables
            .read()
            .is_ok_and(|tables| tables.contains_key(&key))
    }

    /// Check if a data operation should be throttled.
    ///
    /// Checks per-partition, per-table, and per-account buckets in order.
    /// Returns `Allowed` if all have capacity, or the appropriate throttle
    /// result if any is exhausted. A write is also checked against the
    /// table's provisioned global secondary indexes; see
    /// [`Self::check_capacity_with_partition`].
    ///
    /// `partition_value` is the partition key value for item-level operations.
    /// When provided, per-partition limits (1000 WCU/s, 3000 RCU/s) are enforced.
    pub fn check_capacity(
        &self,
        account_id: &str,
        table_name: &str,
        is_read: bool,
        is_write: bool,
    ) -> ThrottleResult {
        self.check_capacity_with_partition(account_id, table_name, is_read, is_write, None, None)
    }

    /// Check capacity with optional per-partition throttling.
    ///
    /// `index_name` is the index a `Query` or `Scan` names. When it names a
    /// provisioned global secondary index registered for the table, the read
    /// is checked against the index's read bucket in place of the table's; a
    /// local secondary index shares the table's capacity, so it and a name no
    /// registered index has read from the table's buckets. A write is checked
    /// against the table's write bucket and then each registered index's write
    /// bucket in order; the first that is empty refuses with
    /// [`ThrottleCause::Index`].
    pub fn check_capacity_with_partition(
        &self,
        account_id: &str,
        table_name: &str,
        is_read: bool,
        is_write: bool,
        partition_value: Option<&str>,
        index_name: Option<&str>,
    ) -> ThrottleResult {
        if !self.enabled.load(Ordering::Relaxed) {
            return ThrottleResult::Allowed;
        }

        let now = Instant::now();
        let mut on_demand = false;
        let key = TableKey {
            account_id: account_id.to_owned(),
            table_name: table_name.to_owned(),
        };

        // A read naming a provisioned global secondary index is served from
        // the index's read bucket and does not consult the table's.
        let mut index_read = false;
        if is_read
            && let Some(name) = index_name
            && let Ok(mut indexes) = self.indexes.write()
            && let Some(set) = indexes.get_mut(&key)
            && let Some(allowed) = index::read_allowed(set, name, now)
        {
            index_read = true;
            if !allowed {
                return ThrottleResult::ThrottledRead(ThrottleCause::Index {
                    index_name: name.to_owned(),
                });
            }
        }

        // Check per-table bucket.
        if let Ok(mut tables) = self.tables.write()
            && let Some(buckets) = tables.get_mut(&key)
        {
            on_demand = buckets.on_demand;
            if is_read && !index_read && !buckets.read.has_capacity(now) {
                return ThrottleResult::ThrottledRead(ThrottleCause::Table {
                    on_demand,
                    capped: buckets.read_capped,
                });
            }
            if is_write && !buckets.write.has_capacity(now) {
                return ThrottleResult::ThrottledWrite(ThrottleCause::Table {
                    on_demand,
                    capped: buckets.write_capped,
                });
            }
        }

        // A write the table's bucket allowed is held back by the first index,
        // in the order registered, whose write bucket is empty.
        if is_write
            && let Ok(mut indexes) = self.indexes.write()
            && let Some(set) = indexes.get_mut(&key)
            && let Some(exhausted) = index::first_write_exhausted(set, now)
        {
            return ThrottleResult::ThrottledWrite(ThrottleCause::Index {
                index_name: exhausted.to_owned(),
            });
        }

        // Check per-partition bucket (1000 WCU/s, 3000 RCU/s per partition).
        if let Some(pv) = partition_value
            && let Ok(mut partitions) = self.partitions.write()
        {
            let pk = PartitionKey {
                account_id: account_id.to_owned(),
                table_name: table_name.to_owned(),
                partition_value: pv.to_owned(),
            };
            let buckets = partitions.entry(pk).or_insert_with(|| TableBuckets {
                read: TokenBucket::new(PARTITION_RCU_LIMIT),
                write: TokenBucket::new(PARTITION_WCU_LIMIT),
                on_demand,
                read_capped: false,
                write_capped: false,
            });
            let cause = ThrottleCause::Partition { on_demand };
            if is_read && !buckets.read.has_capacity(now) {
                return ThrottleResult::ThrottledRead(cause);
            }
            if is_write && !buckets.write.has_capacity(now) {
                return ThrottleResult::ThrottledWrite(cause);
            }
        }

        // Check per-account aggregate bucket.
        if let Ok(mut accounts) = self.accounts.write() {
            let acct = accounts
                .entry(account_id.to_owned())
                .or_insert_with(|| AccountBuckets {
                    read: TokenBucket::new(self.account_rcu_limit),
                    write: TokenBucket::new(self.account_wcu_limit),
                });
            if is_read && !acct.read.has_capacity(now) {
                return ThrottleResult::ThrottledRead(ThrottleCause::Account);
            }
            if is_write && !acct.write.has_capacity(now) {
                return ThrottleResult::ThrottledWrite(ThrottleCause::Account);
            }
        }

        ThrottleResult::Allowed
    }

    /// Consume capacity after a successful operation.
    ///
    /// Deducts tokens from per-table and per-account buckets, and charges a
    /// write to the table's provisioned global secondary indexes as
    /// [`Self::consume_with_partition`] does.
    pub fn consume(&self, account_id: &str, table_name: &str, read_units: f64, write_units: f64) {
        self.consume_with_partition(account_id, table_name, read_units, write_units, None, None);
    }

    /// Consume capacity with optional per-partition tracking.
    ///
    /// A read whose `index_name` names a registered provisioned global
    /// secondary index is charged to the index's read bucket, not the table's.
    /// A write is charged to the table's write bucket and to every registered
    /// index's write bucket, each by `write_units`. The service charges an
    /// index only for an item that carries the index's key attributes; this
    /// layer does not see the item, so every index is charged.
    pub fn consume_with_partition(
        &self,
        account_id: &str,
        table_name: &str,
        read_units: f64,
        write_units: f64,
        partition_value: Option<&str>,
        index_name: Option<&str>,
    ) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let now = Instant::now();
        let key = TableKey {
            account_id: account_id.to_owned(),
            table_name: table_name.to_owned(),
        };

        let mut index_read = false;
        if ((read_units > 0.0 && index_name.is_some()) || write_units > 0.0)
            && let Ok(mut indexes) = self.indexes.write()
            && let Some(set) = indexes.get_mut(&key)
        {
            index_read = index::charge(set, index_name, read_units, write_units, now);
        }

        if let Ok(mut tables) = self.tables.write()
            && let Some(buckets) = tables.get_mut(&key)
        {
            if read_units > 0.0 && !index_read {
                buckets.read.consume(read_units, now);
            }
            if write_units > 0.0 {
                buckets.write.consume(write_units, now);
            }
        }

        // Consume from per-partition bucket if partition value is known.
        if let Some(pv) = partition_value
            && let Ok(mut partitions) = self.partitions.write()
        {
            let pk = PartitionKey {
                account_id: account_id.to_owned(),
                table_name: table_name.to_owned(),
                partition_value: pv.to_owned(),
            };
            let buckets = partitions.entry(pk).or_insert_with(|| TableBuckets {
                read: TokenBucket::new(PARTITION_RCU_LIMIT),
                write: TokenBucket::new(PARTITION_WCU_LIMIT),
                // Partition buckets take their limits from the constants; the
                // billing mode is read from the table's bucket at check time.
                on_demand: false,
                read_capped: false,
                write_capped: false,
            });
            if read_units > 0.0 {
                buckets.read.consume(read_units, now);
            }
            if write_units > 0.0 {
                buckets.write.consume(write_units, now);
            }
        }

        if let Ok(mut accounts) = self.accounts.write() {
            let acct = accounts
                .entry(account_id.to_owned())
                .or_insert_with(|| AccountBuckets {
                    read: TokenBucket::new(self.account_rcu_limit),
                    write: TokenBucket::new(self.account_wcu_limit),
                });
            if read_units > 0.0 {
                acct.read.consume(read_units, now);
            }
            if write_units > 0.0 {
                acct.write.consume(write_units, now);
            }
        }
    }
}

impl Default for ThrottleManager {
    fn default() -> Self {
        Self::new(80_000, 80_000, false)
    }
}

#[cfg(test)]
mod manager_tests;

#[cfg(test)]
mod cause_tests;

#[cfg(test)]
mod index_tests;
