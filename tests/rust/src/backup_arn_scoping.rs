// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup operations addressed by ARN resolve only within the caller's account.
//!
//! A backup ARN embeds the owning account
//! (`arn:aws:dynamodb:<region>:<account>:table/<table>/backup/<id>`). Backups
//! are account-scoped resources, so `DescribeBackup`, `DeleteBackup`, and
//! `RestoreTableFromBackup` treat an ARN naming a different account as absent —
//! the same answer as an ARN that was never issued, and consistent with
//! `ListBackups`, which only returns the caller's own backups.
//!
//! These tests drive the ARN directly rather than provisioning a second account,
//! so they exercise the resolution rule without needing a second set of
//! credentials.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};

/// An account id that no test ever provisions.
const FOREIGN_ACCOUNT: &str = "999999999999";

/// A well-formed backup ARN owned by `FOREIGN_ACCOUNT`.
fn foreign_backup_arn(table: &str) -> String {
    let region = std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into());
    format!(
        "arn:aws:dynamodb:{region}:{FOREIGN_ACCOUNT}:table/{table}/backup/01489602797149-73d8d5bc"
    )
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

#[tokio::test]
async fn describe_backup_with_another_accounts_arn_reports_not_found() {
    let c = client();
    let err = c
        .describe_backup()
        .backup_arn(foreign_backup_arn("SomeTable"))
        .send()
        .await
        .unwrap_err();

    assert_eq!(
        err_code(&err),
        Some("ResourceNotFoundException"),
        "expected the ARN to resolve as absent, got: {err:?}"
    );
}

#[tokio::test]
async fn delete_backup_with_another_accounts_arn_reports_not_found() {
    let c = client();
    let err = c
        .delete_backup()
        .backup_arn(foreign_backup_arn("SomeTable"))
        .send()
        .await
        .unwrap_err();

    assert_eq!(
        err_code(&err),
        Some("ResourceNotFoundException"),
        "expected the ARN to resolve as absent, got: {err:?}"
    );
}

#[tokio::test]
async fn restore_from_another_accounts_backup_arn_reports_not_found() {
    let c = client();
    let target = format!("BackupScopeRestore_{}", ts());

    let err = c
        .restore_table_from_backup()
        .target_table_name(&target)
        .backup_arn(foreign_backup_arn("SomeTable"))
        .send()
        .await
        .unwrap_err();

    assert_eq!(
        err_code(&err),
        Some("ResourceNotFoundException"),
        "expected the ARN to resolve as absent, got: {err:?}"
    );

    // The target table must not have been created as a side effect.
    let describe = c.describe_table().table_name(&target).send().await;
    assert!(
        describe.is_err(),
        "restore from an unresolvable backup ARN created table {target}"
    );
}

/// Positive control: the scoping rule must not reject a caller's own ARN.
///
/// Without this, all three tests above would also pass if backup resolution
/// were broken outright.
#[tokio::test]
async fn describe_backup_with_own_arn_still_resolves() {
    let c = client();
    let table = format!("BackupScopeOwn_{}", ts());
    make_table(&table).await;

    let create = c
        .create_backup()
        .table_name(&table)
        .backup_name("scope-own")
        .send()
        .await
        .unwrap();
    let arn = create.backup_details().unwrap().backup_arn().to_string();

    let resp = c.describe_backup().backup_arn(&arn).send().await.unwrap();
    assert_eq!(
        resp.backup_description()
            .unwrap()
            .backup_details()
            .unwrap()
            .backup_arn(),
        arn.as_str()
    );

    c.delete_backup().backup_arn(&arn).send().await.ok();
    c.delete_table().table_name(&table).send().await.ok();
}

/// The backup id carries a random component after the timestamp, so two backups
/// taken in the same millisecond still get distinct ARNs.
#[tokio::test]
async fn backup_ids_are_not_derived_from_the_timestamp_alone() {
    let c = client();
    let table = format!("BackupScopeIds_{}", ts());
    make_table(&table).await;

    let mut arns = Vec::new();
    for i in 0..3 {
        let create = c
            .create_backup()
            .table_name(&table)
            .backup_name(format!("scope-id-{i}"))
            .send()
            .await
            .unwrap();
        arns.push(create.backup_details().unwrap().backup_arn().to_string());
    }

    for arn in &arns {
        let id = arn.rsplit('/').next().unwrap();
        let (ts_part, suffix) = id.split_once('-').unwrap_or((id, ""));
        assert!(
            ts_part.chars().all(char::is_numeric) && !ts_part.is_empty(),
            "backup id {id} does not start with a numeric timestamp"
        );
        assert_eq!(
            suffix.len(),
            8,
            "backup id {id} is missing the 8-character suffix"
        );
        assert!(
            suffix.chars().all(|ch| ch.is_ascii_hexdigit()),
            "backup id {id} suffix is not hex"
        );
    }

    let mut unique = arns.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), arns.len(), "duplicate backup ARNs: {arns:?}");

    for arn in &arns {
        c.delete_backup().backup_arn(arn).send().await.ok();
    }
    c.delete_table().table_name(&table).send().await.ok();
}
