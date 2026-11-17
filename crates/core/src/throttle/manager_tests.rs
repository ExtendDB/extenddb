// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The manager's table and account buckets: the enabled switch, registration,
//! removal and the refusal each bucket gives.

use super::*;

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
        ThrottleResult::ThrottledRead(ThrottleCause::Table {
            on_demand: false,
            capped: false,
        })
    );
}

#[test]
fn throttle_manager_throttles_exhausted_writes() {
    let mgr = ThrottleManager::new(100_000, 100_000, true);
    mgr.register_table("acct", "tbl", TableThroughput::provisioned(1, 1));
    mgr.consume("acct", "tbl", 0.0, 2.0);
    assert_eq!(
        mgr.check_capacity("acct", "tbl", false, true),
        ThrottleResult::ThrottledWrite(ThrottleCause::Table {
            on_demand: false,
            capped: false,
        })
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
        ThrottleResult::ThrottledRead(ThrottleCause::Account)
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
