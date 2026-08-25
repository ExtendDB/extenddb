// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup and restore integration tests — mirrors Java `BackupRestoreTests`.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, PointInTimeRecoverySpecification,
    ScalarAttributeType,
};

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
    assert_eq!(details.backup_status().as_str(), "AVAILABLE");

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

    let create = create_backup_with_retry(c, &table, "desc-backup").await;
    let arn = create.backup_details().unwrap().backup_arn().to_string();

    let resp = c.describe_backup().backup_arn(&arn).send().await.unwrap();
    let desc = resp.backup_description().unwrap();
    assert_eq!(desc.backup_details().unwrap().backup_arn(), arn.as_str());

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

    create_backup_with_retry(c, &table, "list-1").await;
    create_backup_with_retry(c, &table, "list-2").await;

    let resp = c.list_backups().table_name(&table).send().await.unwrap();
    assert!(resp.backup_summaries().len() >= 2);

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

    let create = create_backup_with_retry(c, &table, "del-backup").await;
    let arn = create.backup_details().unwrap().backup_arn().to_string();

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

    let create = create_backup_with_retry(c, &table, "restore-backup").await;
    let arn = create.backup_details().unwrap().backup_arn().to_string();

    let restored = format!("Restored_{}", ts());
    let resp = c
        .restore_table_from_backup()
        .target_table_name(&restored)
        .backup_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.table_description()
            .unwrap()
            .table_status()
            .unwrap()
            .as_str(),
        "CREATING"
    );

    wait_for_active(c, &restored).await;
    let scan = c.scan().table_name(&restored).send().await.unwrap();
    assert_eq!(scan.count(), 5);

    c.delete_table().table_name(&restored).send().await.ok();
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
    assert!(resp.continuous_backups_description().is_some());

    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn enable_point_in_time_recovery() {
    let c = client();
    let table = format!("PITREnable_{}", ts());
    make_table(&table).await;

    c.update_continuous_backups()
        .table_name(&table)
        .point_in_time_recovery_specification(
            PointInTimeRecoverySpecification::builder()
                .point_in_time_recovery_enabled(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

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
        "ENABLED"
    );

    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn restore_table_to_point_in_time() {
    let c = client();
    let table = format!("PITRRestore_{}", ts());
    make_table(&table).await;

    // PITR restore is not yet implemented — should return an error.
    let restored = format!("PITRRestored_{}", ts());
    let err = c
        .restore_table_to_point_in_time()
        .source_table_name(&table)
        .target_table_name(&restored)
        .use_latest_restorable_time(true)
        .send()
        .await;
    assert!(err.is_err(), "RestoreTableToPointInTime should return an error (not yet supported)");

    c.delete_table().table_name(&table).send().await.ok();
}
