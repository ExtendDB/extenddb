// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for BulkWorkerConfig::from_limits: the floors on lease,
//! concurrency, per-account share, and batch size, and the write-batch
//! bounds.

use super::super::{BulkWorkerConfig, MAX_BULK_PUT_WRITE_BATCH_ROWS};
use super::*;

/// The four settings whose documented meanings assume positive values
/// are floored at 1; a configured positive value passes through.
#[test]
fn from_limits_floors_the_lease_concurrency_share_and_batch_size_at_one() {
    let zeroed = LimitsConfig {
        bulk_operation_lease_seconds: 0,
        bulk_operation_worker_concurrency: 0,
        bulk_operation_worker_per_account: 0,
        bulk_put_batch_size: 0,
        ..LimitsConfig::default()
    };
    let config = BulkWorkerConfig::from_limits(&zeroed);
    assert_eq!(config.lease, Duration::from_secs(1));
    assert_eq!(config.concurrency, 1);
    assert_eq!(config.per_account, 1);
    assert_eq!(config.batch_size, 1);

    let defaults = LimitsConfig::default();
    let config = BulkWorkerConfig::from_limits(&defaults);
    assert_eq!(
        config.lease,
        Duration::from_secs(defaults.bulk_operation_lease_seconds)
    );
    assert_eq!(
        config.concurrency,
        defaults.bulk_operation_worker_concurrency
    );
    assert_eq!(
        config.per_account,
        defaults.bulk_operation_worker_per_account
    );
    assert_eq!(config.batch_size, defaults.bulk_put_batch_size);
}

/// The write batch is floored at 1, capped at 100, and never larger than
/// the checkpoint batch, since the checkpoint flushes it: a value above
/// `bulk_put_batch_size` would silently act as that value, so it is
/// clamped there (and logged) instead.
#[test]
fn from_limits_bounds_the_write_batch_by_the_cap_and_the_checkpoint_batch() {
    let defaults = LimitsConfig::default();
    let config = BulkWorkerConfig::from_limits(&defaults);
    assert_eq!(config.write_batch_rows, defaults.bulk_put_write_batch_rows);

    let zeroed = LimitsConfig {
        bulk_put_write_batch_rows: 0,
        ..LimitsConfig::default()
    };
    assert_eq!(BulkWorkerConfig::from_limits(&zeroed).write_batch_rows, 1);

    // Above the cap, with a checkpoint batch large enough not to bind.
    let huge = LimitsConfig {
        bulk_put_write_batch_rows: 5_000,
        bulk_put_batch_size: 5_000,
        ..LimitsConfig::default()
    };
    assert_eq!(
        BulkWorkerConfig::from_limits(&huge).write_batch_rows,
        MAX_BULK_PUT_WRITE_BATCH_ROWS
    );

    // Raised alone above the default checkpoint batch: bound by it.
    let alone = LimitsConfig {
        bulk_put_write_batch_rows: 100,
        ..LimitsConfig::default()
    };
    let config = BulkWorkerConfig::from_limits(&alone);
    assert_eq!(config.write_batch_rows, config.batch_size);
    assert_eq!(config.write_batch_rows, defaults.bulk_put_batch_size);

    // Raised together: the write batch reaches the configured value.
    let both = LimitsConfig {
        bulk_put_write_batch_rows: 100,
        bulk_put_batch_size: 100,
        ..LimitsConfig::default()
    };
    assert_eq!(BulkWorkerConfig::from_limits(&both).write_batch_rows, 100);
}
