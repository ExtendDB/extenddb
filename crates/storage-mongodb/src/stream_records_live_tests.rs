// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for `get_stream_records`: the shard is resolved against
//! the stream its iterator names, and a page is streamed record by record so
//! its size is bounded by `Limit`, not by the 16 MB document cap.
//!
//! They read a MongoDB URI from `EXTENDDB_TEST_MONGODB_URI` (a replica set is
//! not needed here) and are `#[ignore]`d so `cargo test --lib` passes without
//! one; run them with
//! `cargo test -p extenddb-storage-mongodb --lib -- --ignored records_live`.
//! Each test uses its own catalog and data databases and drops them at the
//! end. Rows are inserted directly in the shape the engine writes them.

use std::sync::Arc;

use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::stream_arn;
use mongodb::bson::{self, Document, doc};

use crate::MongoEngine;

const REGION: &str = "us-east-1";
const ACCOUNT: &str = "111111111111";
const OTHER_ACCOUNT: &str = "222222222222";
const LABEL: &str = "2026-01-01T00:00:00.000";
const LATER_LABEL: &str = "2026-02-02T00:00:00.000";

fn uri() -> String {
    std::env::var("EXTENDDB_TEST_MONGODB_URI").expect(
        "set EXTENDDB_TEST_MONGODB_URI to a MongoDB server, e.g. \
         mongodb://127.0.0.1:27771/?directConnection=true",
    )
}

struct Fixture {
    engine: MongoEngine,
}

impl Fixture {
    async fn new() -> Self {
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let client = mongodb::Client::with_uri_str(uri()).await.expect("client");
        let engine = MongoEngine {
            client: client.clone(),
            catalog_db: client.database(&format!("s{}_cat", &tag[..12])),
            data_db: client.database(&format!("s{}_data", &tag[..12])),
            region: REGION.to_owned(),
            gsi_cache: Arc::default(),
            tx_read_concern: mongodb::options::ReadConcern::snapshot(),
            backup_liveness: crate::backup_engine::BackupLiveness::default(),
        };
        Self { engine }
    }

    /// A stream-enabled table with four shards, as `create_table` and
    /// `init_stream_shards` write them. Returns the table id.
    async fn stream_table(&self, account: &str, name: &str, label: &str) -> String {
        let table_id = format!("{name}-{account}-id");
        self.engine
            .catalog_db
            .collection::<Document>("tables")
            .insert_one(doc! {
                "_id": { "account_id": account, "table_name": name },
                "table_id": &table_id,
                "stream_label": label,
            })
            .await
            .expect("table");
        for i in 0..4 {
            self.engine
                .data_db
                .collection::<Document>("stream_shards")
                .insert_one(doc! {
                    "shard_id": super::build_shard_id(&table_id, i),
                    "table_id": &table_id,
                    "starting_sequence_number": format!("{:021}", 0),
                })
                .await
                .expect("shard");
        }
        table_id
    }

    async fn write_records(&self, shard_id: &str, table_id: &str, seqs: &[u64], blob: usize) {
        for seq in seqs {
            let record_bson = bson::to_bson(&record_json(*seq, blob)).expect("record json to bson");
            self.engine
                .data_db
                .collection::<Document>("stream_records")
                .insert_one(doc! {
                    "sequence_number": format!("{seq:021}"),
                    "shard_id": shard_id,
                    "table_id": table_id,
                    "event_name": "INSERT",
                    "record_data": record_bson,
                    "created_at": bson::DateTime::now(),
                })
                .await
                .expect("record");
        }
    }

    async fn relabel(&self, account: &str, name: &str, label: &str) {
        self.engine
            .catalog_db
            .collection::<Document>("tables")
            .update_one(
                doc! { "_id": { "account_id": account, "table_name": name } },
                doc! { "$set": { "stream_label": label } },
            )
            .await
            .expect("relabel");
    }

    async fn finish(self) {
        self.engine.catalog_db.drop().await.expect("drop catalog");
        self.engine.data_db.drop().await.expect("drop data");
    }
}

/// A stream record whose new image carries a string of `blob` bytes.
fn record_json(seq: u64, blob: usize) -> serde_json::Value {
    serde_json::json!({
        "eventID": format!("e{seq}"),
        "eventName": "INSERT",
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodb",
        "awsRegion": REGION,
        "dynamodb": {
            "ApproximateCreationDateTime": 1_700_000_000,
            "Keys": { "pk": { "S": format!("k{seq}") } },
            "NewImage": {
                "pk": { "S": format!("k{seq}") },
                "blob": { "S": "x".repeat(blob) },
            },
            "SequenceNumber": format!("{seq:021}"),
            "SizeBytes": blob,
            "StreamViewType": "NEW_IMAGE"
        }
    })
}

fn assert_stream_not_found(result: extenddb_storage::StreamRecordsResult) {
    match result {
        Err(StorageError::TableNotFound(msg)) => {
            assert_eq!(msg, extenddb_storage::util::STREAM_DOES_NOT_EXIST);
        }
        other => panic!("expected the stream does not exist error, got {other:?}"),
    }
}

/// 40 records of about 450 KB each on one shard, read with `Limit` 1000: the
/// page is about 18 MB, past the 16 MB BSON document cap, so it can only be
/// read if each record is its own output document.
#[tokio::test]
#[ignore = "needs a MongoDB server in EXTENDDB_TEST_MONGODB_URI"]
async fn records_live_a_page_over_16_mb_is_read_in_full() {
    let f = Fixture::new().await;
    let table_id = f.stream_table(ACCOUNT, "big", LABEL).await;
    let arn = stream_arn(REGION, ACCOUNT, "big", LABEL);
    let shard_id = super::build_shard_id(&table_id, 0);
    // Inserted in descending order so the read's sort, not insertion order,
    // produces the sequence order the page is asserted in.
    let seqs: Vec<u64> = (1..=40).rev().collect();
    f.write_records(&shard_id, &table_id, &seqs, 450_000).await;

    let (records, last) = f
        .engine
        .get_stream_records(ACCOUNT, &arn, &shard_id, None, 1000)
        .await
        .expect("a page over 16 MB reads");
    assert_eq!(records.len(), 40);
    let got: Vec<&str> = records
        .iter()
        .map(|r| r.dynamodb.sequence_number.as_str())
        .collect();
    let want: Vec<String> = (1..=40).map(|i| format!("{i:021}")).collect();
    assert_eq!(got, want.iter().map(String::as_str).collect::<Vec<_>>());
    assert_eq!(last.as_deref(), Some(format!("{:021}", 40).as_str()));

    // The idle page after the last record, and a bounded page in the middle.
    let (rest, last) = f
        .engine
        .get_stream_records(ACCOUNT, &arn, &shard_id, last.as_deref(), 1000)
        .await
        .expect("idle page");
    assert!(rest.is_empty());
    assert!(last.is_none());
    let (page, _) = f
        .engine
        .get_stream_records(ACCOUNT, &arn, &shard_id, Some(&format!("{:021}", 10)), 5)
        .await
        .expect("bounded page");
    assert_eq!(page.len(), 5);
    assert_eq!(page[0].dynamodb.sequence_number, format!("{:021}", 11));
    f.finish().await;
}

/// The table takes a new stream label while keeping its shards: the old
/// ARN's iterator is refused and the new ARN reads.
#[tokio::test]
#[ignore = "needs a MongoDB server in EXTENDDB_TEST_MONGODB_URI"]
async fn records_live_an_earlier_label_on_the_same_shards_is_not_found() {
    let f = Fixture::new().await;
    let table_id = f.stream_table(ACCOUNT, "t", LABEL).await;
    let shard_id = super::build_shard_id(&table_id, 0);
    f.write_records(&shard_id, &table_id, &[1], 10).await;
    let old_arn = stream_arn(REGION, ACCOUNT, "t", LABEL);
    f.engine
        .get_stream_records(ACCOUNT, &old_arn, &shard_id, None, 10)
        .await
        .expect("current label reads");

    f.relabel(ACCOUNT, "t", LATER_LABEL).await;
    f.write_records(&shard_id, &table_id, &[2], 10).await;
    assert_stream_not_found(
        f.engine
            .get_stream_records(ACCOUNT, &old_arn, &shard_id, None, 10)
            .await,
    );
    let new_arn = stream_arn(REGION, ACCOUNT, "t", LATER_LABEL);
    let (records, _) = f
        .engine
        .get_stream_records(ACCOUNT, &new_arn, &shard_id, None, 10)
        .await
        .expect("new label reads");
    assert_eq!(records.len(), 2);
    f.finish().await;
}

/// Two accounts own a table of the same name and label: the caller's ARN
/// resolves in the caller's account only, so the other account's shard is
/// refused under the ARN the caller spells.
#[tokio::test]
#[ignore = "needs a MongoDB server in EXTENDDB_TEST_MONGODB_URI"]
async fn records_live_another_accounts_same_named_stream_is_not_found() {
    let f = Fixture::new().await;
    let own_id = f.stream_table(ACCOUNT, "t", LABEL).await;
    let other_id = f.stream_table(OTHER_ACCOUNT, "t", LABEL).await;
    let own_shard = super::build_shard_id(&own_id, 0);
    let other_shard = super::build_shard_id(&other_id, 0);
    f.write_records(&own_shard, &own_id, &[1], 10).await;
    f.write_records(&other_shard, &other_id, &[1], 10).await;

    // From both callers, so whichever table a lookup without the account
    // would pick, one caller reads the other account's shard.
    for (caller, foreign_shard, own) in [
        (ACCOUNT, &other_shard, &own_shard),
        (OTHER_ACCOUNT, &own_shard, &other_shard),
    ] {
        for arn in [
            stream_arn(REGION, ACCOUNT, "t", LABEL),
            stream_arn(REGION, OTHER_ACCOUNT, "t", LABEL),
        ] {
            assert_stream_not_found(
                f.engine
                    .get_stream_records(caller, &arn, foreign_shard, None, 10)
                    .await,
            );
            // The control: the caller's own shard reads under either spelling
            // of the account, since storage binds the caller's account and
            // parses only the table name and label out of the ARN.
            let (records, _) = f
                .engine
                .get_stream_records(caller, &arn, own, None, 10)
                .await
                .expect("own shard reads");
            assert_eq!(records.len(), 1);
        }
    }
    f.finish().await;
}

/// Two tables of one account share a stream label: a shard of table `b`
/// under table `a`'s ARN is refused, so the table name alone decides.
#[tokio::test]
#[ignore = "needs a MongoDB server in EXTENDDB_TEST_MONGODB_URI"]
async fn records_live_another_tables_shard_with_the_same_label_is_not_found() {
    let f = Fixture::new().await;
    let a_id = f.stream_table(ACCOUNT, "a", LABEL).await;
    let b_id = f.stream_table(ACCOUNT, "b", LABEL).await;
    f.write_records(&super::build_shard_id(&a_id, 0), &a_id, &[1], 10)
        .await;
    f.write_records(&super::build_shard_id(&b_id, 0), &b_id, &[1, 2], 10)
        .await;
    let arn_a = stream_arn(REGION, ACCOUNT, "a", LABEL);
    let arn_b = stream_arn(REGION, ACCOUNT, "b", LABEL);

    // Both directions, so whichever table a lookup without the name would
    // pick, one of them reads a shard that is not its own.
    assert_stream_not_found(
        f.engine
            .get_stream_records(ACCOUNT, &arn_a, &super::build_shard_id(&b_id, 0), None, 10)
            .await,
    );
    assert_stream_not_found(
        f.engine
            .get_stream_records(ACCOUNT, &arn_b, &super::build_shard_id(&a_id, 0), None, 10)
            .await,
    );
    assert_stream_not_found(
        f.engine
            .get_stream_records(ACCOUNT, &arn_a, "shardId-forged-000000000000", None, 10)
            .await,
    );
    let (records, _) = f
        .engine
        .get_stream_records(ACCOUNT, &arn_a, &super::build_shard_id(&a_id, 0), None, 10)
        .await
        .expect("own shard reads");
    assert_eq!(records.len(), 1);
    f.finish().await;
}

/// A record document that lacks `record_data` is an `Internal` error, not an
/// idle page: the aggregation marks every joined record, so the read tells it
/// from the shard document that has no record to read.
#[tokio::test]
#[ignore = "needs a MongoDB server in EXTENDDB_TEST_MONGODB_URI"]
async fn records_live_a_record_document_without_record_data_is_an_internal_error() {
    let f = Fixture::new().await;
    let table_id = f.stream_table(ACCOUNT, "t", LABEL).await;
    let arn = stream_arn(REGION, ACCOUNT, "t", LABEL);
    let shard_id = super::build_shard_id(&table_id, 0);
    f.write_records(&shard_id, &table_id, &[1], 10).await;
    f.engine
        .data_db
        .collection::<Document>("stream_records")
        .insert_one(doc! {
            "sequence_number": format!("{:021}", 2),
            "shard_id": &shard_id,
            "table_id": &table_id,
            "event_name": "INSERT",
            "created_at": bson::DateTime::now(),
        })
        .await
        .expect("a record document without record_data");

    // The page before the broken document reads; the page that reaches it fails.
    let (records, last) = f
        .engine
        .get_stream_records(ACCOUNT, &arn, &shard_id, None, 1)
        .await
        .expect("the first record reads");
    assert_eq!(records.len(), 1);
    match f
        .engine
        .get_stream_records(ACCOUNT, &arn, &shard_id, last.as_deref(), 10)
        .await
    {
        Err(StorageError::Internal(msg)) => assert_eq!(msg, "Missing record_data"),
        other => panic!("expected Internal(Missing record_data), got {other:?}"),
    }
    f.finish().await;
}

/// A shard with no record to read is an empty page, not an error. The
/// aggregation keeps the shard document and marks it as carrying no record, so
/// the read tells it from a record document that lacks `record_data`.
#[tokio::test]
#[ignore = "needs a MongoDB server in EXTENDDB_TEST_MONGODB_URI"]
async fn records_live_an_idle_shard_reads_an_empty_page() {
    let f = Fixture::new().await;
    let table_id = f.stream_table(ACCOUNT, "t", LABEL).await;
    let arn = stream_arn(REGION, ACCOUNT, "t", LABEL);
    let written = super::build_shard_id(&table_id, 0);
    let idle = super::build_shard_id(&table_id, 1);
    f.write_records(&written, &table_id, &[1, 2], 10).await;

    let last_written = format!("{:021}", 2);
    for (case, shard, after) in [
        ("a shard that never had a record", &idle, None),
        (
            "a bound at the last record",
            &written,
            Some(last_written.as_str()),
        ),
    ] {
        let (records, last) = f
            .engine
            .get_stream_records(ACCOUNT, &arn, shard, after, 10)
            .await
            .unwrap_or_else(|e| panic!("{case}: {e:?}"));
        assert!(records.is_empty(), "{case}: no record to read");
        assert_eq!(last, None, "{case}: no last sequence number");
    }
    f.finish().await;
}
