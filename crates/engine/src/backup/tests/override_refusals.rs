// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `RestoreTableFromBackup` overrides that conflict with the indexes or the
//! billing mode of the backup, refused with DynamoDB's texts and in its order
//! (measured on DynamoDB in us-east-1 on 13, 14 and 17 Nov 2026). Each row
//! runs on the legacy path and on the store-backed path ([`restored_input`]).

use super::*;

/// The backups of the rows, built on `definition`: GSI `gsi1` on `gpk` with
/// projection `ALL`, LSI `lsi1` on `pk` and `lsk` with projection `KEYS_ONLY`.
#[derive(Clone, Copy, Debug)]
enum Backup {
    /// On demand, no index.
    OnDemand,
    /// On demand, `gsi1` and `lsi1`.
    Indexed,
    /// PROVISIONED, `gsi1` with a throughput.
    Provisioned,
    /// On demand with an on-demand maximum, `gsi1`.
    Ceilings,
}

fn backup(kind: Backup) -> BackupRestoreSource {
    let mut def = definition(match kind {
        Backup::Provisioned => "PROVISIONED",
        _ => "PAY_PER_REQUEST",
    });
    if matches!(kind, Backup::OnDemand) {
        def.global_secondary_indexes.clear();
    }
    if !matches!(kind, Backup::Indexed) {
        def.local_secondary_indexes.clear();
    }
    if matches!(kind, Backup::Ceilings) {
        def.on_demand_throughput = Some(OnDemandThroughput {
            max_read_request_units: Some(7),
            max_write_request_units: Some(8),
        });
    }
    source(Some(def))
}

/// `entry` with `member` set to `value`.
pub(super) fn with(mut entry: Value, member: &str, value: Value) -> Value {
    entry[member] = value;
    entry
}

/// An index named `ByWeight` with `key_schema`, which the backup does not
/// have.
fn by_weight(key_schema: Value) -> Value {
    json!({
        "IndexName": "ByWeight",
        "KeySchema": key_schema,
        "Projection": { "ProjectionType": "ALL" },
    })
}

fn pt(read: i64, write: i64) -> Value {
    json!({ "ReadCapacityUnits": read, "WriteCapacityUnits": write })
}

/// Each row's request is refused with the text, on both paths, with no
/// target registered.
async fn assert_refused(rows: Vec<(Backup, Value, &str)>) {
    for (kind, body, expected) in rows {
        let err = restored_input(backup(kind), body.clone())
            .await
            .unwrap_err();
        assert_eq!(validation_message(err), expected, "{kind:?} {body}");
    }
}

/// The table's `BillingModeOverride`, `ProvisionedThroughputOverride` and
/// `OnDemandThroughputOverride`. With both throughput overrides, the
/// on-demand one is refused first.
#[tokio::test]
async fn table_throughput_overrides_are_refused_with_the_services_texts() {
    assert_refused(vec![
        (
            Backup::OnDemand,
            json!({ "ProvisionedThroughputOverride": pt(1, 1) }),
            "Invalid Request: Cannot override ProvisionedThroughput unless BillingMode is \
             PROVISIONED",
        ),
        (
            Backup::OnDemand,
            json!({
                "BillingModeOverride": "PROVISIONED",
                "ProvisionedThroughputOverride": pt(0, 1),
            }),
            "1 validation error detected: Value '0' at \
             'provisionedThroughputOverride.readCapacityUnits' failed to satisfy constraint: \
             Member must have value greater than or equal to 1",
        ),
        // Not measured: the model's constraint on the write member.
        (
            Backup::OnDemand,
            json!({
                "BillingModeOverride": "PROVISIONED",
                "ProvisionedThroughputOverride": pt(5, -1),
            }),
            "1 validation error detected: Value '-1' at \
             'provisionedThroughputOverride.writeCapacityUnits' failed to satisfy constraint: \
             Member must have value greater than or equal to 1",
        ),
        (
            Backup::Provisioned,
            json!({
                "BillingModeOverride": "PROVISIONED",
                "ProvisionedThroughputOverride": pt(0, 0),
            }),
            "2 validation errors detected: Value '0' at \
             'provisionedThroughputOverride.writeCapacityUnits' failed to satisfy constraint: \
             Member must have value greater than or equal to 1; Value '0' at \
             'provisionedThroughputOverride.readCapacityUnits' failed to satisfy constraint: \
             Member must have value greater than or equal to 1",
        ),
        (
            Backup::Indexed,
            json!({ "BillingModeOverride": "PROVISIONED" }),
            "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden \
             to PROVISIONED",
        ),
        (
            Backup::Indexed,
            json!({
                "BillingModeOverride": "PROVISIONED",
                "ProvisionedThroughputOverride": pt(2, 3),
                "OnDemandThroughputOverride": { "MaxReadRequestUnits": 10 },
            }),
            "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput when \
             BillingMode is PROVISIONED",
        ),
        (
            Backup::Provisioned,
            json!({ "OnDemandThroughputOverride": { "MaxReadRequestUnits": 10 } }),
            "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput unless \
             BillingModeOverride is PAY_PER_REQUEST",
        ),
        (
            Backup::Provisioned,
            json!({
                "ProvisionedThroughputOverride": pt(2, 3),
                "OnDemandThroughputOverride": { "MaxReadRequestUnits": 10 },
            }),
            "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput when \
             ProvisionedThroughputOverride is present",
        ),
        (
            Backup::Ceilings,
            json!({
                "ProvisionedThroughputOverride": pt(2, 3),
                "OnDemandThroughputOverride": { "MaxReadRequestUnits": 10 },
            }),
            "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput when \
             ProvisionedThroughputOverride is present",
        ),
        (
            Backup::Ceilings,
            json!({
                "BillingModeOverride": "PAY_PER_REQUEST",
                "ProvisionedThroughputOverride": pt(2, 3),
            }),
            "Invalid Request: Cannot override ProvisionedThroughput if BillingMode is overridden \
             to PAY_PER_REQUEST",
        ),
        // Not measured on a PROVISIONED backup: the text names the override.
        (
            Backup::Provisioned,
            json!({
                "BillingModeOverride": "PAY_PER_REQUEST",
                "ProvisionedThroughputOverride": pt(5, 5),
            }),
            "Invalid Request: Cannot override ProvisionedThroughput if BillingMode is overridden \
             to PAY_PER_REQUEST",
        ),
    ])
    .await;
}

/// An on-demand maximum that sets no member, on a PROVISIONED restore: for
/// the table, and for a GSI entry beside its throughput. DynamoDB accepts
/// both and restores no maximum.
#[tokio::test]
async fn an_on_demand_maximum_that_sets_no_member_restores_provisioned() {
    for body in [
        json!({ "OnDemandThroughputOverride": {} }),
        json!({ "GlobalSecondaryIndexOverride": [
            with(gsi_entry(Some((1, 1))), "OnDemandThroughput", json!({})),
        ] }),
    ] {
        let input = restored_input(source(Some(definition("PROVISIONED"))), body.clone())
            .await
            .unwrap_or_else(|e| panic!("{body}: {e:?}"));
        assert_eq!(input.billing_mode, Some(BillingMode::Provisioned), "{body}");
        assert_eq!(input.on_demand_throughput, None, "{body}");
    }
}

/// An index entry that names no index of the backup, or changes the
/// projection of one.
#[tokio::test]
async fn index_entries_the_backup_does_not_have_are_refused_with_the_services_text() {
    let mismatch = |name: &str| {
        format!(
            "Invalid Request: Index {name} does not match a secondary index that existed in the \
             source table and cannot be created during the restore operation"
        )
    };
    let (by_weight_text, gsi1_text, lsi1_text) =
        (mismatch("ByWeight"), mismatch("gsi1"), mismatch("lsi1"));
    let keys_only = json!({ "ProjectionType": "KEYS_ONLY" });
    assert_refused(vec![
        (
            Backup::Indexed,
            json!({ "GlobalSecondaryIndexOverride": [
                by_weight(json!([{ "AttributeName": "Weight", "KeyType": "HASH" }])),
            ] }),
            by_weight_text.as_str(),
        ),
        (
            Backup::Indexed,
            json!({ "LocalSecondaryIndexOverride": [by_weight(json!([
                { "AttributeName": "pk", "KeyType": "HASH" },
                { "AttributeName": "Weight", "KeyType": "RANGE" },
            ]))] }),
            by_weight_text.as_str(),
        ),
        (
            Backup::Indexed,
            json!({ "GlobalSecondaryIndexOverride": [
                with(gsi_entry(None), "Projection", keys_only),
            ] }),
            gsi1_text.as_str(),
        ),
        (
            Backup::Indexed,
            json!({ "LocalSecondaryIndexOverride": [
                with(lsi_entry(), "Projection", json!({ "ProjectionType": "ALL" })),
            ] }),
            lsi1_text.as_str(),
        ),
    ])
    .await;
}

/// A GSI entry's `ProvisionedThroughput` and `OnDemandThroughput`, and a GSI
/// the request gives no throughput for. The on-demand maximum is refused
/// before a provisioned throughput that is missing or given beside it, except
/// a missing one when `BillingModeOverride` is `PROVISIONED`.
#[tokio::test]
async fn index_throughputs_are_refused_with_the_services_texts() {
    let read_5 = json!({ "MaxReadRequestUnits": 5 });
    assert_refused(vec![
        (
            Backup::Indexed,
            json!({ "GlobalSecondaryIndexOverride": [gsi_entry(Some((4, 5)))] }),
            "Invalid Request: Cannot override ProvisionedThroughput unless BillingMode is \
             PROVISIONED for index gsi1",
        ),
        (
            Backup::Indexed,
            json!({
                "BillingModeOverride": "PROVISIONED",
                "ProvisionedThroughputOverride": pt(2, 3),
            }),
            "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden \
             to PROVISIONED for index gsi1",
        ),
        (
            Backup::Provisioned,
            json!({ "GlobalSecondaryIndexOverride": [gsi_entry(None)] }),
            "Invalid Request: Must specify provisioned throughput for index gsi1",
        ),
        (
            Backup::Provisioned,
            json!({ "GlobalSecondaryIndexOverride": [
                with(gsi_entry(None), "OnDemandThroughput", json!({})),
            ] }),
            "Invalid Request: Must specify provisioned throughput for index gsi1",
        ),
        (
            Backup::Provisioned,
            json!({ "GlobalSecondaryIndexOverride": [
                with(gsi_entry(None), "OnDemandThroughput", read_5.clone()),
            ] }),
            "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput unless \
             BillingModeOverride is PAY_PER_REQUEST for index gsi1",
        ),
        (
            Backup::Provisioned,
            json!({ "GlobalSecondaryIndexOverride": [
                with(gsi_entry(Some((1, 1))), "OnDemandThroughput", read_5.clone()),
            ] }),
            "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput when \
             ProvisionedThroughputOverride is present for index gsi1",
        ),
        (
            Backup::Indexed,
            json!({
                "BillingModeOverride": "PROVISIONED",
                "ProvisionedThroughputOverride": pt(1, 1),
                "GlobalSecondaryIndexOverride": [
                    with(gsi_entry(None), "OnDemandThroughput", read_5.clone()),
                ],
            }),
            "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden \
             to PROVISIONED for index gsi1",
        ),
        (
            Backup::Ceilings,
            json!({
                "BillingModeOverride": "PROVISIONED",
                "ProvisionedThroughputOverride": pt(2, 3),
                "GlobalSecondaryIndexOverride": [with(
                    gsi_entry(Some((1, 1))),
                    "OnDemandThroughput",
                    json!({ "MaxWriteRequestUnits": 5 }),
                )],
            }),
            "Invalid Request: Cannot override MaxWriteRequestUnits for OnDemandThroughput when \
             BillingMode is PROVISIONED for index gsi1",
        ),
        (
            Backup::Ceilings,
            json!({ "GlobalSecondaryIndexOverride": [
                with(gsi_entry(Some((1, 1))), "OnDemandThroughput", read_5),
            ] }),
            "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput when \
             ProvisionedThroughputOverride is present for index gsi1",
        ),
        (
            Backup::Ceilings,
            json!({
                "BillingModeOverride": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexOverride": [gsi_entry(Some((1, 1)))],
            }),
            "Invalid Request: Cannot override ProvisionedThroughput if BillingMode is overridden \
             to PAY_PER_REQUEST for index gsi1",
        ),
    ])
    .await;
}
