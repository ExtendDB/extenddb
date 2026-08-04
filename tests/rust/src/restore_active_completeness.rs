// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A restored table must not report ACTIVE before its data copy completes.
//!
//! DynamoDB's contract: when `DescribeTable` on a restore target first returns
//! ACTIVE, the restored data is fully present. A restore that flips ACTIVE on a
//! control-plane timer decoupled from the copy exposes an empty or partial
//! table to a client that waits-for-ACTIVE.
//!
//! This is a race detector, deliberately. `RestoreTableFromBackup` in this
//! backend blocks until the copy completes, so the ACTIVE-before-data window is
//! only observable from a second client, and whether it is observed depends on
//! whether the copy or the transition timer wins. Two consequences worth
//! knowing before reading a result:
//!
//! - It cannot fail when the ordering is correct. If ACTIVE is only set after
//!   the copy drains, the observed count is always complete.
//! - A PASS is not proof the ordering is correct. On an idle server the `$out`
//!   copy can finish inside the transition window, and the race is simply not
//!   observed. Failures are meaningful; passes are weak evidence.
//!
//! The dataset is sized so the copy takes longer than the transition window on
//! a loaded server. On very fast or idle hardware, raising `ITEMS` widens the
//! window.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, PutRequest,
    ScalarAttributeType, Select, WriteRequest,
};

const ITEMS: usize = 40000;

/// Bound on the observer's wait for first ACTIVE, so a failed restore fails the
/// test rather than hanging it. 10ms per attempt, so this is a 60s ceiling.
const OBSERVER_MAX_ATTEMPTS: usize = 6000;

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
    // Race an observer against the in-flight restore: the moment it sees
    // ACTIVE, it counts. Returns None if ACTIVE never arrives within the
    // bound, which means the restore itself failed.
    let observer = {
        let c2 = client().clone();
        let dst2 = dst.clone();
        tokio::spawn(async move {
            let mut saw_active = false;
            for _ in 0..OBSERVER_MAX_ATTEMPTS {
                if let Ok(out) = c2.describe_table().table_name(&dst2).send().await {
                    let status = out
                        .table()
                        .and_then(|t| t.table_status())
                        .map(|s| s.as_str().to_owned());
                    if status.as_deref() == Some("ACTIVE") {
                        saw_active = true;
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            if !saw_active {
                return None;
            }
            // First ACTIVE observation: count items immediately.
            let mut count = 0usize;
            let mut start_key = None;
            loop {
                // Retry transient scan errors instead of swallowing them. A
                // page that errors must NOT be treated as "no more items": that
                // truncates the count and misreports a transient scan blip as
                // missing data. Clone (don't take) the cursor so a retry
                // re-scans the same page; if a page still fails after the retry
                // budget, fail the test loudly rather than under-counting.
                let resp = {
                    let mut got = None;
                    let mut last_err = None;
                    for _ in 0..10 {
                        let mut req = c2.scan().table_name(&dst2).select(Select::Count);
                        if let Some(k) = start_key.clone() {
                            req = req.set_exclusive_start_key(Some(k));
                        }
                        match req.send().await {
                            Ok(r) => {
                                got = Some(r);
                                break;
                            }
                            Err(e) => {
                                last_err = Some(e);
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            }
                        }
                    }
                    got.unwrap_or_else(|| {
                        panic!(
                            "observer scan failed during item count (transient scan \
                             error, not missing data): {last_err:?}"
                        )
                    })
                };
                count += resp.count() as usize;
                match resp.last_evaluated_key() {
                    Some(k) if !k.is_empty() => start_key = Some(k.clone()),
                    _ => break,
                }
            }
            Some(count)
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
    let observed = observer.await.unwrap();

    c.delete_table().table_name(&src).send().await.ok();
    c.delete_table().table_name(&dst).send().await.ok();

    let count = observed.expect(
        "restore target never reported ACTIVE within the observer bound, \
         so the restore itself did not complete",
    );
    assert_eq!(
        count, ITEMS,
        "restored table reported ACTIVE with {count}/{ITEMS} items present. \
         ACTIVE must imply the restore copy is complete"
    );
}
