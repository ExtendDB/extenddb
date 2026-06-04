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

/// Default burst window: 300 seconds of accumulated capacity.
const BURST_SECONDS: f64 = 300.0;

/// Default on-demand read capacity ceiling per table.
const ON_DEMAND_DEFAULT_RCU: f64 = 12_000.0;

/// Default on-demand write capacity ceiling per table.
const ON_DEMAND_DEFAULT_WCU: f64 = 4_000.0;

/// Per-partition read capacity limit (3000 RCU/s).
const PARTITION_RCU_LIMIT: f64 = 3_000.0;

/// Per-partition write capacity limit (1000 WCU/s).
const PARTITION_WCU_LIMIT: f64 = 1_000.0;

/// A single token bucket that refills at a constant rate.
#[derive(Debug)]
struct TokenBucket {
    /// Current token count (can go negative after a large operation).
    tokens: f64,
    /// Maximum tokens (burst capacity).
    max_tokens: f64,
    /// Tokens added per second (= provisioned CU/s).
    refill_rate: f64,
    /// Last time tokens were refilled. `None` until the first operation,
    /// preventing burst accumulation during idle time between bucket
    /// creation and first use.
    last_refill: Option<Instant>,
}

impl TokenBucket {
    fn new(refill_rate: f64) -> Self {
        let max_tokens = refill_rate * BURST_SECONDS;
        // Start with 1 second of capacity rather than the full burst window.
        // Burst accumulates over time as unused capacity builds up, matching
        // real DynamoDB's "unused capacity" burst model. Starting full would
        // make throttling untestable for newly created tables.
        //
        // `last_refill` starts as `None` — the refill clock only begins on
        // the first `has_capacity` or `consume` call. This prevents idle
        // time between table creation and first data operation from
        // accumulating burst tokens.
        Self {
            tokens: refill_rate,
            max_tokens,
            refill_rate,
            last_refill: None,
        }
    }

    /// Refill tokens based on elapsed time since the last operation.
    /// On the first call, starts the refill clock without adding tokens.
    fn refill(&mut self, now: Instant) {
        if let Some(last) = self.last_refill {
            let elapsed = now.duration_since(last).as_secs_f64();
            if elapsed > 0.0 {
                self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.max_tokens);
            }
        }
        self.last_refill = Some(now);
    }

    /// Check if the bucket has any tokens available (pre-dispatch check).
    fn has_capacity(&mut self, now: Instant) -> bool {
        self.refill(now);
        self.tokens > 0.0
    }

    /// Consume tokens after a successful operation.
    fn consume(&mut self, amount: f64, now: Instant) {
        self.refill(now);
        self.tokens -= amount;
    }

    /// Update the refill rate (e.g., after `UpdateTable` changes throughput).
    fn update_rate(&mut self, new_rate: f64) {
        let old_max = self.max_tokens;
        self.max_tokens = new_rate * BURST_SECONDS;
        self.refill_rate = new_rate;
        // Scale current tokens proportionally.
        if old_max > 0.0 {
            self.tokens = (self.tokens / old_max * self.max_tokens).min(self.max_tokens);
        } else {
            self.tokens = self.max_tokens;
        }
    }
}

/// Throughput configuration for a table, used to create/update buckets.
#[derive(Debug, Clone, Copy)]
pub struct TableThroughput {
    /// Read capacity units per second.
    pub rcu: f64,
    /// Write capacity units per second.
    pub wcu: f64,
    /// Whether this is an on-demand table.
    pub on_demand: bool,
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
        }
    }

    /// Create throughput config for an on-demand table with default ceilings.
    #[must_use]
    pub fn on_demand() -> Self {
        Self {
            rcu: ON_DEMAND_DEFAULT_RCU,
            wcu: ON_DEMAND_DEFAULT_WCU,
            on_demand: true,
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
}

/// Account-level aggregate bucket pair.
struct AccountBuckets {
    read: TokenBucket,
    write: TokenBucket,
}

/// Result of a throttle check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrottleResult {
    /// Request may proceed.
    Allowed,
    /// Request is throttled on reads.
    ThrottledRead,
    /// Request is throttled on writes.
    ThrottledWrite,
}

/// Thread-safe throttle manager holding all token buckets.
///
/// Uses `RwLock` for the maps. The write lock is held briefly for bucket
/// creation; the read lock suffices for capacity checks (which mutate
/// individual buckets via interior mutability — but since we need `&mut`
/// for `TokenBucket`, we use the write lock for all operations).
pub struct ThrottleManager {
    tables: RwLock<HashMap<TableKey, TableBuckets>>,
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
        } else {
            tables.insert(
                key,
                TableBuckets {
                    read: TokenBucket::new(throughput.rcu),
                    write: TokenBucket::new(throughput.wcu),
                },
            );
        }
    }

    /// Remove a table's buckets (called on `DeleteTable`).
    pub fn remove_table(&self, account_id: &str, table_name: &str) {
        let key = TableKey {
            account_id: account_id.to_owned(),
            table_name: table_name.to_owned(),
        };
        if let Ok(mut tables) = self.tables.write() {
            tables.remove(&key);
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
    /// result if any is exhausted.
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
        self.check_capacity_with_partition(account_id, table_name, is_read, is_write, None)
    }

    /// Check capacity with optional per-partition throttling.
    pub fn check_capacity_with_partition(
        &self,
        account_id: &str,
        table_name: &str,
        is_read: bool,
        is_write: bool,
        partition_value: Option<&str>,
    ) -> ThrottleResult {
        if !self.enabled.load(Ordering::Relaxed) {
            return ThrottleResult::Allowed;
        }

        let now = Instant::now();

        // Check per-table bucket.
        if let Ok(mut tables) = self.tables.write() {
            let key = TableKey {
                account_id: account_id.to_owned(),
                table_name: table_name.to_owned(),
            };
            if let Some(buckets) = tables.get_mut(&key) {
                if is_read && !buckets.read.has_capacity(now) {
                    return ThrottleResult::ThrottledRead;
                }
                if is_write && !buckets.write.has_capacity(now) {
                    return ThrottleResult::ThrottledWrite;
                }
            }
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
            });
            if is_read && !buckets.read.has_capacity(now) {
                return ThrottleResult::ThrottledRead;
            }
            if is_write && !buckets.write.has_capacity(now) {
                return ThrottleResult::ThrottledWrite;
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
                return ThrottleResult::ThrottledRead;
            }
            if is_write && !acct.write.has_capacity(now) {
                return ThrottleResult::ThrottledWrite;
            }
        }

        ThrottleResult::Allowed
    }

    /// Consume capacity after a successful operation.
    ///
    /// Deducts tokens from per-table and per-account buckets.
    pub fn consume(&self, account_id: &str, table_name: &str, read_units: f64, write_units: f64) {
        self.consume_with_partition(account_id, table_name, read_units, write_units, None);
    }

    /// Consume capacity with optional per-partition tracking.
    pub fn consume_with_partition(
        &self,
        account_id: &str,
        table_name: &str,
        read_units: f64,
        write_units: f64,
        partition_value: Option<&str>,
    ) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let now = Instant::now();

        if let Ok(mut tables) = self.tables.write() {
            let key = TableKey {
                account_id: account_id.to_owned(),
                table_name: table_name.to_owned(),
            };
            if let Some(buckets) = tables.get_mut(&key) {
                if read_units > 0.0 {
                    buckets.read.consume(read_units, now);
                }
                if write_units > 0.0 {
                    buckets.write.consume(write_units, now);
                }
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
mod tests {
    use super::*;

    #[test]
    fn token_bucket_starts_with_one_second_capacity() {
        let bucket = TokenBucket::new(100.0);
        assert_eq!(bucket.max_tokens, 100.0 * BURST_SECONDS);
        assert_eq!(bucket.tokens, 100.0); // 1 second of capacity
        assert!(bucket.last_refill.is_none()); // lazy — clock not started
    }

    #[test]
    fn token_bucket_consume_and_refill() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new(100.0);
        // First consume starts the refill clock, then deducts tokens.
        bucket.consume(100.0, now);
        assert!(bucket.tokens <= 0.0);
        assert!(!bucket.has_capacity(now));
    }

    #[test]
    fn token_bucket_no_burst_accumulation_before_first_use() {
        let mut bucket = TokenBucket::new(5.0);
        // Simulate idle time: create bucket, wait, then check capacity.
        // With lazy refill, the bucket should still have only 5 tokens
        // (1 second of capacity) regardless of wall-clock delay.
        let later = Instant::now();
        // First call starts the clock — no tokens added.
        assert!(bucket.has_capacity(later));
        assert_eq!(bucket.tokens, 5.0); // unchanged from initial
    }

    #[test]
    fn throttle_manager_allows_when_disabled() {
        let mgr = ThrottleManager::new(100, 100, false);
        mgr.register_table("acct", "tbl", TableThroughput::provisioned(1, 1));
        assert_eq!(
            mgr.check_capacity("acct", "tbl", true, false),
            ThrottleResult::Allowed
        );
    }

    #[test]
    fn throttle_manager_allows_unregistered_table() {
        let mgr = ThrottleManager::new(100, 100, true);
        assert_eq!(
            mgr.check_capacity("acct", "unknown", true, false),
            ThrottleResult::Allowed
        );
    }

    #[test]
    fn throttle_manager_throttles_exhausted_table() {
        let mgr = ThrottleManager::new(100_000, 100_000, true);
        mgr.register_table("acct", "tbl", TableThroughput::provisioned(1, 1));
        // Drain the table bucket (starts with 1 token = 1 second of 1 CU/s).
        mgr.consume("acct", "tbl", 2.0, 0.0);
        assert_eq!(
            mgr.check_capacity("acct", "tbl", true, false),
            ThrottleResult::ThrottledRead
        );
    }

    #[test]
    fn throttle_manager_throttles_exhausted_writes() {
        let mgr = ThrottleManager::new(100_000, 100_000, true);
        mgr.register_table("acct", "tbl", TableThroughput::provisioned(1, 1));
        mgr.consume("acct", "tbl", 0.0, 2.0);
        assert_eq!(
            mgr.check_capacity("acct", "tbl", false, true),
            ThrottleResult::ThrottledWrite
        );
    }

    #[test]
    fn throttle_manager_remove_table() {
        let mgr = ThrottleManager::new(100_000, 100_000, true);
        mgr.register_table("acct", "tbl", TableThroughput::provisioned(1, 1));
        mgr.remove_table("acct", "tbl");
        // After removal, unregistered table is allowed.
        assert_eq!(
            mgr.check_capacity("acct", "tbl", true, true),
            ThrottleResult::Allowed
        );
    }

    #[test]
    fn throttle_manager_account_level_throttle() {
        let mgr = ThrottleManager::new(1, 1, true);
        mgr.register_table(
            "acct",
            "tbl",
            TableThroughput::provisioned(100_000, 100_000),
        );
        // Table has plenty of capacity, but account limit is 1 CU/s.
        // Drain account bucket (starts with 1 token).
        mgr.consume("acct", "tbl", 2.0, 0.0);
        assert_eq!(
            mgr.check_capacity("acct", "tbl", true, false),
            ThrottleResult::ThrottledRead
        );
    }

    #[test]
    fn on_demand_table_uses_default_ceilings() {
        let mgr = ThrottleManager::new(100_000, 100_000, true);
        mgr.register_table("acct", "tbl", TableThroughput::on_demand());
        // Should allow normal operations.
        assert_eq!(
            mgr.check_capacity("acct", "tbl", true, true),
            ThrottleResult::Allowed
        );
    }

    #[test]
    fn update_rate_adjusts_bucket() {
        let mut bucket = TokenBucket::new(100.0);
        assert_eq!(bucket.max_tokens, 100.0 * BURST_SECONDS);
        bucket.update_rate(200.0);
        assert_eq!(bucket.max_tokens, 200.0 * BURST_SECONDS);
        assert_eq!(bucket.refill_rate, 200.0);
    }
}
