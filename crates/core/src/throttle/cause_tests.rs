// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The cause a refusal carries and the OnDemandThroughput caps sizing.

use super::*;

/// A hot partition key names the partition bucket and carries the table's
/// billing mode, read from the table's own bucket.
#[test]
fn partition_throttle_carries_the_billing_mode() {
    for (throughput, on_demand) in [
        (TableThroughput::provisioned(100_000, 100_000), false),
        (TableThroughput::on_demand(), true),
    ] {
        let mgr = ThrottleManager::new(1_000_000, 1_000_000, true);
        mgr.register_table("acct", "tbl", throughput);
        // Drain the partition bucket (PARTITION_WCU_LIMIT per second, one
        // second of burst), leaving the table bucket full.
        mgr.consume_with_partition("acct", "tbl", 0.0, PARTITION_WCU_LIMIT * 2.0, Some("k"));
        assert_eq!(
            mgr.check_capacity_with_partition("acct", "tbl", false, true, Some("k")),
            ThrottleResult::ThrottledWrite(ThrottleCause::Partition { on_demand }),
            "on_demand={on_demand}"
        );
    }
}

/// An on-demand table with `OnDemandThroughput` caps is sized by the caps
/// and refuses with the capped cause; an absent or non-positive cap keeps
/// the default ceiling for its direction.
#[test]
fn on_demand_caps_size_the_table_bucket() {
    let capped = TableThroughput::on_demand_capped(Some(1), Some(1));
    assert!(capped.on_demand && capped.read_capped && capped.write_capped);
    assert_eq!((capped.rcu, capped.wcu), (1.0, 1.0));

    let half = TableThroughput::on_demand_capped(None, Some(5));
    assert!(!half.read_capped && half.write_capped);
    assert_eq!((half.rcu, half.wcu), (ON_DEMAND_DEFAULT_RCU, 5.0));

    let removed = TableThroughput::on_demand_capped(Some(-1), Some(-1));
    assert!(!removed.read_capped && !removed.write_capped);
    assert_eq!(
        (removed.rcu, removed.wcu),
        (ON_DEMAND_DEFAULT_RCU, ON_DEMAND_DEFAULT_WCU)
    );

    let mgr = ThrottleManager::new(1_000_000, 1_000_000, true);
    mgr.register_table("acct", "tbl", capped);
    mgr.consume("acct", "tbl", 0.0, 2.0);
    assert_eq!(
        mgr.check_capacity("acct", "tbl", false, true),
        ThrottleResult::ThrottledWrite(ThrottleCause::Table {
            on_demand: true,
            capped: true,
        })
    );
    assert_eq!(
        mgr.check_capacity("acct", "tbl", false, true).cause(),
        Some(ThrottleCause::Table {
            on_demand: true,
            capped: true,
        })
    );
    assert_eq!(ThrottleResult::Allowed.cause(), None);
}

/// A table capped in one direction reports the caps limit only for a
/// refusal in that direction; the other direction is the default ceiling.
#[test]
fn half_capped_table_reports_the_cap_for_its_direction_only() {
    let mgr = ThrottleManager::new(1_000_000, 1_000_000, true);
    mgr.register_table(
        "acct",
        "tbl",
        TableThroughput::on_demand_capped(None, Some(1)),
    );
    // Drain both buckets: the write cap of 1 and the default read ceiling.
    mgr.consume("acct", "tbl", ON_DEMAND_DEFAULT_RCU * 2.0, 2.0);
    assert_eq!(
        mgr.check_capacity("acct", "tbl", false, true),
        ThrottleResult::ThrottledWrite(ThrottleCause::Table {
            on_demand: true,
            capped: true,
        })
    );
    assert_eq!(
        mgr.check_capacity("acct", "tbl", true, false),
        ThrottleResult::ThrottledRead(ThrottleCause::Table {
            on_demand: true,
            capped: false,
        })
    );
}

/// Re-registering a table (UpdateTable) moves its billing mode and caps.
#[test]
fn register_table_updates_the_billing_mode() {
    let mgr = ThrottleManager::new(1_000_000, 1_000_000, true);
    mgr.register_table("acct", "tbl", TableThroughput::provisioned(1, 1));
    mgr.register_table(
        "acct",
        "tbl",
        TableThroughput::on_demand_capped(Some(1), Some(1)),
    );
    mgr.consume("acct", "tbl", 2.0, 0.0);
    assert_eq!(
        mgr.check_capacity("acct", "tbl", true, false),
        ThrottleResult::ThrottledRead(ThrottleCause::Table {
            on_demand: true,
            capped: true,
        })
    );
}

/// Moving from the default on-demand ceilings to a cap keeps the same
/// fraction of the burst window: a fresh default bucket holds one second
/// of 4,000 WCU, which becomes one second of the 1 WRU cap (one token,
/// not the 4,000 the table held a moment before), so a write is allowed
/// and a write of more than one unit leaves the bucket refusing with the
/// capped cause.
#[test]
fn register_table_scales_tokens_to_the_new_cap() {
    let mgr = ThrottleManager::new(1_000_000, 1_000_000, true);
    mgr.register_table("acct", "tbl", TableThroughput::on_demand());
    mgr.register_table(
        "acct",
        "tbl",
        TableThroughput::on_demand_capped(Some(1), Some(1)),
    );
    assert_eq!(
        mgr.check_capacity("acct", "tbl", false, true),
        ThrottleResult::Allowed
    );
    mgr.consume("acct", "tbl", 0.0, 1.5);
    assert_eq!(
        mgr.check_capacity("acct", "tbl", false, true),
        ThrottleResult::ThrottledWrite(ThrottleCause::Table {
            on_demand: true,
            capped: true,
        })
    );
}
