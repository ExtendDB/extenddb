// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup and restore integration tests — mirrors Java `BackupRestoreTests`.
//!
//! The service creates a backup asynchronously; so does ExtendDB with a
//! configured `[backup.store]` (CreateBackup answers CREATING and a job
//! uploads it). A backup still CREATING refuses DeleteBackup
//! (`BackupInUseException`) and restore, so every test that goes on to
//! describe, delete or restore a backup it just created waits for AVAILABLE
//! first ([`wait_for_backup_available`]), and deletes the backups it made so
//! the server's store does not keep them. Where a backup lives decides what
//! RestoreTableFromBackup answers this release; the tests read it from
//! `EXTENDDB_TEST_BACKUP_STORE` ([`backup_store`]), as the Python suite does.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, PointInTimeRecoverySpecification,
    ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;
use std::time::{Duration, Instant};

/// Where the server under test writes its backups, from
/// `EXTENDDB_TEST_BACKUP_STORE`: `filesystem` or `s3` (a configured
/// `[backup.store]`, under which a backup is in the store and the restore
/// job restores it) or `none` (the in-catalog create, which the legacy
/// reader restores). Against the service the variable is
/// not read (the restore runs). Against ExtendDB a test whose outcome depends
/// on it fails when it is unset, with the text the Python fixture prints,
/// rather than accept either outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackupStore {
    Filesystem,
    S3,
    None,
}

pub fn backup_store() -> BackupStore {
    const VALUES: [&str; 3] = ["filesystem", "s3", "none"];
    if is_real_dynamodb() {
        return BackupStore::None;
    }
    let raw = std::env::var("EXTENDDB_TEST_BACKUP_STORE").unwrap_or_default();
    match raw.trim() {
        "filesystem" => BackupStore::Filesystem,
        "s3" => BackupStore::S3,
        "none" => BackupStore::None,
        "" => panic!(
            "EXTENDDB_TEST_BACKUP_STORE is unset; set it to one of {VALUES:?} to say where the \
             server under test writes its backups"
        ),
        other => panic!(
            "EXTENDDB_TEST_BACKUP_STORE is {other:?}; set it to one of {VALUES:?} to say where \
             the server under test writes its backups"
        ),
    }
}

/// Poll DescribeBackup until the backup is AVAILABLE. Any other status than
/// CREATING is a failure: a backup that fails is BackupNotFound, never
/// FAILED, on the wire, so the describe itself errors then.
pub async fn wait_for_backup_available(c: &Client, arn: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let resp = c
            .describe_backup()
            .backup_arn(arn)
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!("describe_backup of {arn} while waiting for AVAILABLE: {e:?}")
            });
        let status = resp
            .backup_description()
            .and_then(|b| b.backup_details())
            .map(|d| d.backup_status().as_str().to_owned())
            .unwrap_or_default();
        if status == "AVAILABLE" {
            return;
        }
        assert_eq!(status, "CREATING", "backup {arn} is {status}");
        assert!(
            Instant::now() < deadline,
            "backup {arn} not AVAILABLE in time"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// CreateBackup, then wait for AVAILABLE; the backup's ARN.
pub async fn create_available_backup(c: &Client, table: &str, backup_name: &str) -> String {
    let create = create_backup_with_retry(c, table, backup_name).await;
    let arn = create.backup_details().unwrap().backup_arn().to_string();
    wait_for_backup_available(c, &arn).await;
    arn
}

/// DeleteBackup after the test; a backup already gone is not a failure.
pub async fn delete_backup_quietly(c: &Client, arn: &str) {
    c.delete_backup().backup_arn(arn).send().await.ok();
}

async fn make_table(name: &str) {
    let c = client();
    c.create_table()
        .table_name(name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(c, name).await;
}

/// Create a backup, retrying on `ContinuousBackupsUnavailableException`
/// (real DynamoDB needs time for continuous backups to initialize).
async fn create_backup_with_retry(
    c: &aws_sdk_dynamodb::Client,
    table: &str,
    backup_name: &str,
) -> aws_sdk_dynamodb::operation::create_backup::CreateBackupOutput {
    for _ in 0..10 {
        match c
            .create_backup()
            .table_name(table)
            .backup_name(backup_name)
            .send()
            .await
        {
            Ok(r) => return r,
            Err(e) => {
                if err_code(&e) == Some("ContinuousBackupsUnavailableException") {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
                panic!("create_backup failed: {e:?}");
            }
        }
    }
    panic!("create_backup did not succeed after retries");
}

#[tokio::test]
async fn create_backup_happy_case() {
    let c = client();
    let table = format!("BackupTest_{}", ts());
    make_table(&table).await;

    let resp = create_backup_with_retry(c, &table, "test-backup-1").await;
    let details = resp.backup_details().unwrap();
    assert!(!details.backup_arn().is_empty());
    // The in-catalog create answers AVAILABLE; the service and a store-backed
    // ExtendDB answer CREATING and the backup reaches AVAILABLE later.
    let status = details.backup_status().as_str();
    assert!(
        status == "AVAILABLE" || status == "CREATING",
        "CreateBackup answered {status}"
    );
    let arn = details.backup_arn().to_string();
    wait_for_backup_available(c, &arn).await;
    let desc = c.describe_backup().backup_arn(&arn).send().await.unwrap();
    assert_eq!(
        desc.backup_description()
            .unwrap()
            .backup_details()
            .unwrap()
            .backup_status()
            .as_str(),
        "AVAILABLE"
    );

    delete_backup_quietly(c, &arn).await;
    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn create_backup_non_existent_table() {
    let c = client();
    let err = c
        .create_backup()
        .table_name(format!("NonExistent_{}", ts()))
        .backup_name("bad-backup")
        .send()
        .await
        .unwrap_err();
    // extenddb returns ResourceNotFoundException; real DynamoDB returns
    // TableNotFoundException for backup operations on nonexistent tables.
    let code = err_code(&err);
    assert!(
        code == Some("ResourceNotFoundException") || code == Some("TableNotFoundException"),
        "unexpected error code: {code:?}"
    );
}

#[tokio::test]
async fn describe_backup() {
    let c = client();
    let table = format!("DescBackup_{}", ts());
    make_table(&table).await;

    let arn = create_available_backup(c, &table, "desc-backup").await;

    let resp = c.describe_backup().backup_arn(&arn).send().await.unwrap();
    let desc = resp.backup_description().unwrap();
    assert_eq!(desc.backup_details().unwrap().backup_arn(), arn.as_str());

    delete_backup_quietly(c, &arn).await;
    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn describe_backup_non_existent() {
    let c = client();
    let err = c
        .describe_backup()
        .backup_arn("arn:aws:dynamodb:us-east-1:000000000000:table/x/backup/nonexistent")
        .send()
        .await
        .unwrap_err();
    assert!(err_code(&err).is_some());
}

#[tokio::test]
async fn list_backups() {
    let c = client();
    let table = format!("ListBackup_{}", ts());
    make_table(&table).await;

    let first = create_available_backup(c, &table, "list-1").await;
    let second = create_available_backup(c, &table, "list-2").await;

    let resp = c.list_backups().table_name(&table).send().await.unwrap();
    assert!(resp.backup_summaries().len() >= 2);

    delete_backup_quietly(c, &first).await;
    delete_backup_quietly(c, &second).await;
    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn list_backups_empty() {
    let c = client();
    let table = format!("EmptyBackup_{}", ts());
    make_table(&table).await;

    let resp = c.list_backups().table_name(&table).send().await.unwrap();
    assert!(resp.backup_summaries().is_empty());

    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn delete_backup() {
    let c = client();
    let table = format!("DelBackup_{}", ts());
    make_table(&table).await;

    let arn = create_available_backup(c, &table, "del-backup").await;

    let resp = c.delete_backup().backup_arn(&arn).send().await.unwrap();
    let desc = resp.backup_description().unwrap();
    assert_eq!(
        desc.backup_details().unwrap().backup_status().as_str(),
        "DELETED"
    );

    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn restore_table_from_backup() {
    let c = client();
    let table = format!("RestoreBackup_{}", ts());
    make_table(&table).await;

    for i in 0..5 {
        c.put_item()
            .table_name(&table)
            .item("pk", s(&format!("item_{i}")))
            .item("data", s(&format!("val_{i}")))
            .send()
            .await
            .unwrap();
    }

    let arn = create_available_backup(c, &table, "restore-backup").await;

    let restored = format!("Restored_{}", ts());
    let restore = c
        .restore_table_from_backup()
        .target_table_name(&restored)
        .backup_arn(&arn)
        .send()
        .await;
    // The round trip on either path: a backup in the configured store is
    // restored by the restore job, an in-catalog one by the legacy reader.
    let resp = restore.unwrap();
    let desc = resp.table_description().unwrap();
    assert_eq!(desc.table_status().unwrap().as_str(), "CREATING");
    let summary = desc
        .restore_summary()
        .expect("a CREATING restore target carries its RestoreSummary");
    assert_eq!(summary.source_backup_arn(), Some(arn.as_str()));
    assert!(summary.restore_in_progress());

    wait_for_active(c, &restored).await;
    let scan = c.scan().table_name(&restored).send().await.unwrap();
    assert_eq!(scan.count(), 5);
    let after = c
        .describe_table()
        .table_name(&restored)
        .send()
        .await
        .unwrap();
    assert!(
        after.table().unwrap().restore_summary().is_none(),
        "RestoreSummary is reported only while CREATING"
    );
    println!(
        "restore of a {} backup completed with {} items",
        match backup_store() {
            BackupStore::Filesystem | BackupStore::S3 => "store-backed",
            BackupStore::None => "in-catalog",
        },
        scan.count()
    );
    c.delete_table().table_name(&restored).send().await.ok();

    delete_backup_quietly(c, &arn).await;
    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn describe_continuous_backups() {
    let c = client();
    let table = format!("ContBackup_{}", ts());
    make_table(&table).await;

    let resp = c
        .describe_continuous_backups()
        .table_name(&table)
        .send()
        .await
        .unwrap();
    let desc = resp.continuous_backups_description().unwrap();
    assert_eq!(desc.continuous_backups_status().as_str(), "ENABLED");
    let pitr = desc.point_in_time_recovery_description().unwrap();
    assert_eq!(
        pitr.point_in_time_recovery_status().unwrap().as_str(),
        "DISABLED"
    );
    assert!(
        pitr.earliest_restorable_date_time().is_none(),
        "a disabled recovery must not report an earliest restorable time"
    );
    assert!(
        pitr.latest_restorable_date_time().is_none(),
        "a disabled recovery must not report a latest restorable time"
    );

    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn enable_point_in_time_recovery_is_refused() {
    let c = client();
    let table = format!("PITREnable_{}", ts());
    make_table(&table).await;

    // Point-in-time recovery is not in this release, so enabling it fails
    // with the typed exception rather than report a recovery capability that
    // can never restore.
    let err = c
        .update_continuous_backups()
        .table_name(&table)
        .point_in_time_recovery_specification(
            PointInTimeRecoverySpecification::builder()
                .point_in_time_recovery_enabled(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err_code(&err), err_msg(&err).as_str()),
        (
            Some("ContinuousBackupsUnavailableException"),
            "Point-in-time recovery is not supported by this storage backend"
        ),
        "unexpected error: {err:?}"
    );

    // The refusal must not have changed anything.
    let resp = c
        .describe_continuous_backups()
        .table_name(&table)
        .send()
        .await
        .unwrap();
    let pitr = resp
        .continuous_backups_description()
        .unwrap()
        .point_in_time_recovery_description()
        .unwrap();
    assert_eq!(
        pitr.point_in_time_recovery_status().unwrap().as_str(),
        "DISABLED"
    );

    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn disable_point_in_time_recovery_reports_disabled() {
    let c = client();
    let table = format!("PITRDisable_{}", ts());
    make_table(&table).await;

    // Disabling recovery that was never enabled is a no-op that succeeds and
    // returns the same description DescribeContinuousBackups reports.
    let resp = c
        .update_continuous_backups()
        .table_name(&table)
        .point_in_time_recovery_specification(
            PointInTimeRecoverySpecification::builder()
                .point_in_time_recovery_enabled(false)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let desc = resp.continuous_backups_description().unwrap();
    assert_eq!(desc.continuous_backups_status().as_str(), "ENABLED");
    assert_eq!(
        desc.point_in_time_recovery_description()
            .unwrap()
            .point_in_time_recovery_status()
            .unwrap()
            .as_str(),
        "DISABLED"
    );

    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn restore_table_to_point_in_time_is_refused() {
    let c = client();
    let table = format!("PITRRestore_{}", ts());
    make_table(&table).await;

    // Point-in-time recovery is not in this release, so every restore fails
    // with the typed exception the service models on this operation rather
    // than fabricate a restore of current state.
    let restored = format!("PITRRestored_{}", ts());
    let err = c
        .restore_table_to_point_in_time()
        .source_table_name(&table)
        .target_table_name(&restored)
        .use_latest_restorable_time(true)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err_code(&err), err_msg(&err).as_str()),
        (
            Some("PointInTimeRecoveryUnavailableException"),
            "Point-in-time recovery is not supported by this storage backend"
        ),
        "unexpected error: {err:?}"
    );

    c.delete_table().table_name(&table).send().await.ok();
}

/// A describe and a disable look the table up; an enable and a restore are
/// refused before any table is read.
#[tokio::test]
async fn point_in_time_recovery_operations_report_a_missing_table_as_table_not_found() {
    let c = client();
    let missing = format!("PITRMissing_{}", ts());
    let expected = format!("Table not found: {missing}");
    let check = |what: &str, code: Option<&str>, message: String| {
        assert_eq!(
            (code, message.as_str()),
            (Some("TableNotFoundException"), expected.as_str()),
            "{what}"
        );
    };

    let err = c
        .describe_continuous_backups()
        .table_name(&missing)
        .send()
        .await
        .unwrap_err();
    check("describe", err_code(&err), err_msg(&err));

    let err = c
        .update_continuous_backups()
        .table_name(&missing)
        .point_in_time_recovery_specification(
            PointInTimeRecoverySpecification::builder()
                .point_in_time_recovery_enabled(false)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    check("disable", err_code(&err), err_msg(&err));

    let refused = "Point-in-time recovery is not supported by this storage backend";
    let err = c
        .update_continuous_backups()
        .table_name(&missing)
        .point_in_time_recovery_specification(
            PointInTimeRecoverySpecification::builder()
                .point_in_time_recovery_enabled(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err_code(&err), err_msg(&err).as_str()),
        (Some("ContinuousBackupsUnavailableException"), refused),
        "enable"
    );

    let err = c
        .restore_table_to_point_in_time()
        .source_table_name(&missing)
        .target_table_name(format!("{missing}_restored"))
        .use_latest_restorable_time(true)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err_code(&err), err_msg(&err).as_str()),
        (Some("PointInTimeRecoveryUnavailableException"), refused),
        "restore"
    );
}
