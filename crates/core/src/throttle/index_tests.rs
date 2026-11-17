// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The buckets of a table's provisioned global secondary indexes: the set a
//! registration replaces, the read a named index serves, the write every index
//! holds back, and their removal with the table.

use super::*;

fn index(name: &str, rcu: i64, wcu: i64) -> IndexThroughput {
    IndexThroughput::provisioned(name, rcu, wcu)
}

fn index_read(name: &str) -> ThrottleResult {
    ThrottleResult::ThrottledRead(ThrottleCause::Index {
        index_name: name.to_owned(),
    })
}

fn index_write(name: &str) -> ThrottleResult {
    ThrottleResult::ThrottledWrite(ThrottleCause::Index {
        index_name: name.to_owned(),
    })
}

/// The refusal of a provisioned table's own bucket.
fn table_cause() -> ThrottleCause {
    ThrottleCause::Table {
        on_demand: false,
        capped: false,
    }
}

/// A manager with one provisioned table "tbl" and its `indexes`.
fn manager(table_rcu: i64, table_wcu: i64, indexes: Vec<IndexThroughput>) -> ThrottleManager {
    let mgr = ThrottleManager::new(1_000_000, 1_000_000, true);
    mgr.register_table(
        "acct",
        "tbl",
        TableThroughput::provisioned(table_rcu, table_wcu),
    );
    mgr.register_indexes("acct", "tbl", indexes);
    mgr
}

fn read(mgr: &ThrottleManager, index_name: Option<&str>) -> ThrottleResult {
    mgr.check_capacity_with_partition("acct", "tbl", true, false, None, index_name)
}

fn write(mgr: &ThrottleManager) -> ThrottleResult {
    mgr.check_capacity("acct", "tbl", false, true)
}

fn consume_read(mgr: &ThrottleManager, units: f64, index_name: Option<&str>) {
    mgr.consume_with_partition("acct", "tbl", units, 0.0, None, index_name);
}

/// A read naming a registered index is refused by the index's read bucket,
/// with the index's name, while the table's read bucket is full, and
/// consuming it charges the index and not the table.
#[test]
fn a_read_naming_an_index_is_charged_to_and_refused_by_the_index() {
    let mgr = manager(1, 1, vec![index("gsi", 1, 1)]);
    consume_read(&mgr, 2.0, Some("gsi"));
    let refused = read(&mgr, Some("gsi"));
    assert_eq!(refused, index_read("gsi"));
    assert_eq!(
        refused.cause().and_then(ThrottleCause::index_name),
        Some("gsi")
    );
    // The table's read bucket still holds its first second.
    assert_eq!(read(&mgr, None), ThrottleResult::Allowed);
}

/// A read naming a registered index does not consult the table's read
/// bucket: with the table's drained, the index read is allowed.
#[test]
fn a_read_naming_an_index_does_not_consult_the_table_read_bucket() {
    let mgr = manager(1, 1, vec![index("gsi", 1, 1)]);
    consume_read(&mgr, 2.0, None);
    assert_eq!(
        read(&mgr, None),
        ThrottleResult::ThrottledRead(table_cause())
    );
    assert_eq!(read(&mgr, Some("gsi")), ThrottleResult::Allowed);
}

/// A name no registered index has (a local secondary index, or no index at
/// all) is served from the table's buckets, for the check and the charge.
#[test]
fn a_read_naming_no_registered_index_uses_the_table_buckets() {
    let mgr = manager(1, 1, vec![index("gsi", 1, 1)]);
    consume_read(&mgr, 2.0, Some("lsi"));
    assert_eq!(
        read(&mgr, Some("lsi")),
        ThrottleResult::ThrottledRead(table_cause())
    );
    assert_eq!(read(&mgr, Some("gsi")), ThrottleResult::Allowed);
    assert_eq!(read(&mgr, None).cause(), Some(&table_cause()));
}

/// A write the table's bucket allows is refused by the first exhausted index
/// in the order registered, naming it.
#[test]
fn a_write_is_refused_by_the_first_exhausted_index_in_order() {
    let mgr = manager(
        1_000,
        1_000,
        vec![
            index("roomy", 1, 1_000),
            index("second", 1, 1),
            index("third", 1, 1),
        ],
    );
    // Every index is charged the two units: "second" and "third" go below
    // zero, "roomy" and the table stay positive.
    mgr.consume("acct", "tbl", 0.0, 2.0);
    assert_eq!(write(&mgr), index_write("second"));
}

/// With the table's write bucket and an index's both empty, the table's
/// refuses: it is checked first.
#[test]
fn the_table_write_bucket_refuses_before_the_indexes() {
    let mgr = manager(1, 1, vec![index("gsi", 1, 1)]);
    mgr.consume("acct", "tbl", 0.0, 2.0);
    assert_eq!(write(&mgr), ThrottleResult::ThrottledWrite(table_cause()));
}

/// A write charges every index's write bucket, not only the first: after
/// one write drains both indexes, reordering the set shows the other one
/// empty too.
#[test]
fn a_write_charges_every_index_write_bucket() {
    let mgr = manager(1_000, 1_000, vec![index("a", 1, 1), index("b", 1, 1)]);
    mgr.consume("acct", "tbl", 0.0, 2.0);
    assert_eq!(write(&mgr), index_write("a"));
    mgr.register_indexes("acct", "tbl", vec![index("b", 1, 1), index("a", 1, 1)]);
    assert_eq!(write(&mgr), index_write("b"));
}

/// Registration replaces the set: a kept name keeps its balance, a dropped
/// name is gone and a read naming it is served from the table, and a new name
/// has buckets of its own.
#[test]
fn register_indexes_replaces_the_set() {
    let mgr = manager(1, 1, vec![index("kept", 1, 1), index("dropped", 1, 1)]);
    consume_read(&mgr, 2.0, Some("kept"));
    consume_read(&mgr, 2.0, Some("dropped"));
    assert_eq!(read(&mgr, Some("kept")), index_read("kept"));
    assert_eq!(read(&mgr, Some("dropped")), index_read("dropped"));

    mgr.register_indexes(
        "acct",
        "tbl",
        vec![index("kept", 1, 1), index("added", 1, 1)],
    );
    assert_eq!(read(&mgr, Some("kept")), index_read("kept"));
    assert_eq!(read(&mgr, Some("dropped")), ThrottleResult::Allowed);
    // The dropped name now reads from the table: draining the table refuses
    // it with the table's cause.
    consume_read(&mgr, 2.0, None);
    assert_eq!(
        read(&mgr, Some("dropped")),
        ThrottleResult::ThrottledRead(table_cause())
    );
    assert_eq!(read(&mgr, Some("added")), ThrottleResult::Allowed);
    consume_read(&mgr, 2.0, Some("added"));
    assert_eq!(read(&mgr, Some("added")), index_read("added"));

    // An empty set removes every index.
    mgr.register_indexes("acct", "tbl", Vec::new());
    assert_eq!(
        read(&mgr, Some("kept")),
        ThrottleResult::ThrottledRead(table_cause())
    );
}

/// A kept index takes the new rates. Its balance keeps the same fraction of
/// the burst window, so one second of 1 RCU (one token) becomes one second of
/// 1,000 RCU: a 500-unit read leaves it positive, where at the old rate it
/// would refuse.
#[test]
fn a_kept_index_takes_the_new_rate() {
    let mgr = manager(1_000, 1_000, vec![index("gsi", 1, 1)]);
    mgr.register_indexes("acct", "tbl", vec![index("gsi", 1_000, 1_000)]);
    consume_read(&mgr, 500.0, Some("gsi"));
    assert_eq!(read(&mgr, Some("gsi")), ThrottleResult::Allowed);
    mgr.consume("acct", "tbl", 0.0, 500.0);
    assert_eq!(write(&mgr), ThrottleResult::Allowed);
}

/// `remove_table` drops the index buckets with the table's, so a table
/// created again under the name starts with none.
#[test]
fn remove_table_drops_the_index_buckets() {
    let mgr = manager(1_000, 1_000, vec![index("gsi", 1, 1)]);
    mgr.consume("acct", "tbl", 0.0, 2.0);
    assert_eq!(write(&mgr), index_write("gsi"));
    mgr.remove_table("acct", "tbl");
    mgr.register_table("acct", "tbl", TableThroughput::provisioned(1_000, 1_000));
    assert_eq!(write(&mgr), ThrottleResult::Allowed);
}

/// A disabled manager allows every request and charges nothing: enabled
/// again, the buckets still hold their first second.
#[test]
fn a_disabled_manager_allows_and_charges_nothing() {
    let mgr = manager(1, 1, vec![index("gsi", 1, 1)]);
    mgr.set_enabled(false);
    mgr.consume_with_partition("acct", "tbl", 2.0, 2.0, None, Some("gsi"));
    assert_eq!(read(&mgr, Some("gsi")), ThrottleResult::Allowed);
    assert_eq!(write(&mgr), ThrottleResult::Allowed);
    mgr.set_enabled(true);
    assert_eq!(read(&mgr, Some("gsi")), ThrottleResult::Allowed);
    assert_eq!(write(&mgr), ThrottleResult::Allowed);
}

/// Only an index cause names an index.
#[test]
fn only_an_index_cause_names_an_index() {
    assert_eq!(table_cause().index_name(), None);
    assert_eq!(ThrottleCause::Account.index_name(), None);
    assert_eq!(
        ThrottleCause::Partition { on_demand: true }.index_name(),
        None
    );
}

/// An index read skips only the table's read bucket: the account aggregate
/// is still checked and still charged. With a 1 RCU account limit, an index
/// read that drains it is refused by the account on the next check.
#[test]
fn an_index_read_is_checked_against_and_charged_to_the_account_bucket() {
    let mgr = ThrottleManager::new(1, 1_000_000, true);
    mgr.register_table(
        "acct",
        "tbl",
        TableThroughput::provisioned(1_000_000, 1_000_000),
    );
    mgr.register_indexes("acct", "tbl", vec![index("gsi", 1_000_000, 1_000_000)]);
    assert_eq!(read(&mgr, Some("gsi")), ThrottleResult::Allowed);
    consume_read(&mgr, 2.0, Some("gsi"));
    assert_eq!(
        read(&mgr, Some("gsi")),
        ThrottleResult::ThrottledRead(ThrottleCause::Account)
    );
}

/// The same for the partition bucket when a partition value is known.
#[test]
fn an_index_read_is_checked_against_and_charged_to_the_partition_bucket() {
    // The table at 1 RCU: a 6,000 unit charge that reached its read bucket
    // would refuse the final read.
    let mgr = manager(1, 1_000_000, vec![index("gsi", 1_000_000, 1_000_000)]);
    let check =
        || mgr.check_capacity_with_partition("acct", "tbl", true, false, Some("k"), Some("gsi"));
    assert_eq!(check(), ThrottleResult::Allowed);
    mgr.consume_with_partition(
        "acct",
        "tbl",
        PARTITION_RCU_LIMIT * 2.0,
        0.0,
        Some("k"),
        Some("gsi"),
    );
    assert_eq!(
        check(),
        ThrottleResult::ThrottledRead(ThrottleCause::Partition { on_demand: false })
    );
    // The table's read bucket took none of it.
    assert_eq!(read(&mgr, None), ThrottleResult::Allowed);
    assert_eq!(read(&mgr, Some("gsi")), ThrottleResult::Allowed);
}
