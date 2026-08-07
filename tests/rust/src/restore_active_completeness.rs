// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A restored table must not report ACTIVE before its data copy completes.
//!
//! DynamoDB's contract: when `DescribeTable` on a restore target first
//! returns ACTIVE, the restored data is fully present. A restore that flips
//! ACTIVE on a control-plane timer decoupled from the copy exposes an empty
//! or partial table to a client that waits-for-ACTIVE.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, PutRequest,
    ScalarAttributeType, Select, WriteRequest,
};

const ITEMS: usize = 40000;

#[tokio::test]
async fn restored_table_has_all_items_when_first_active() {
    let c = client();
    let src = format!("RestoreRaceSrc_{}", ts());
    c.create_table()
        .table_name(&src)
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
    wait_for_active(&c, &src).await;

    // Enough data that the restore copy takes longer than the control-plane
    // transition delay, so a timer-driven ACTIVE flip would win the race.
    let pad = "x".repeat(2000);
    for chunk in (0..ITEMS).collect::<Vec<_>>().chunks(25) {
        let reqs: Vec<WriteRequest> = chunk
            .iter()
            .map(|i| {
                WriteRequest::builder()
                    .put_request(
                        PutRequest::builder()
                            .item("pk", AttributeValue::S(format!("k{i:06}")))
                            .item("d", AttributeValue::S(pad.clone()))
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect();
        c.batch_write_item()
            .request_items(&src, reqs)
            .send()
            .await
            .unwrap();
    }

    let backup = c
        .create_backup()
        .table_name(&src)
        .backup_name("restore-race-probe")
        .send()
        .await
        .unwrap();
    let arn = backup.backup_details().unwrap().backup_arn().to_string();
    // Wait until the backup is AVAILABLE.
    for _ in 0..240 {
        let d = c.describe_backup().backup_arn(&arn).send().await.unwrap();
        if d.backup_description()
            .and_then(|b| b.backup_details())
            .map(|b| b.backup_status().as_str() == "AVAILABLE")
            .unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let dst = format!("RestoreRaceDst_{}", ts());
    // This backend's RestoreTableFromBackup blocks until the copy completes
    // (real DynamoDB returns immediately), so the ACTIVE-before-data window
    // is only observable by a CONCURRENT client. Race an observer task
    // against the in-flight restore: the moment it sees ACTIVE, it counts.
    let observer = {
        let c2 = client().clone();
        let dst2 = dst.clone();
        tokio::spawn(async move {
            // Wait for the table to exist, then for first ACTIVE.
            loop {
                match c2.describe_table().table_name(&dst2).send().await {
                    Ok(out) => {
                        let st = out.table().unwrap().table_status().unwrap().clone();
                        if st.as_str() == "ACTIVE" {
                            break;
                        }
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            // First ACTIVE observation: count items immediately.
            let mut count = 0usize;
            let mut start_key = None;
            loop {
                let mut req = c2.scan().table_name(&dst2).select(Select::Count);
                if let Some(k) = start_key.take() {
                    req = req.set_exclusive_start_key(Some(k));
                }
                let resp = req.send().await.unwrap();
                count += resp.count() as usize;
                match resp.last_evaluated_key() {
                    Some(k) if !k.is_empty() => start_key = Some(k.clone()),
                    _ => break,
                }
            }
            count
        })
    };

    c.restore_table_from_backup()
        .target_table_name(&dst)
        .backup_arn(&arn)
        .send()
        .await
        .unwrap();

    // The concurrent observer counted at first-ACTIVE while the restore call
    // above was still in flight (or just after, if the copy was fast).
    let count = observer.await.unwrap();

    c.delete_table().table_name(&src).send().await.ok();
    c.delete_table().table_name(&dst).send().await.ok();

    assert_eq!(
        count, ITEMS,
        "restored table reported ACTIVE with {count}/{ITEMS} items present — \
         ACTIVE must imply the restore copy is complete"
    );
}
