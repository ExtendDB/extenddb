// SPDX-License-Identifier: Apache-2.0

//! `get_stream_records` resolves the shard against the stream the iterator
//! names in the same query as the records read.

use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::stream_arn;

use crate::store::SqliteEngine;

const REGION: &str = "us-east-1";
const ACCOUNT: &str = "111111111111";
const OTHER_ACCOUNT: &str = "222222222222";

async fn engine() -> SqliteEngine {
    let engine = SqliteEngine::new(":memory:", 1, REGION, 409_600)
        .await
        .expect("engine");
    crate::schema::apply(&engine.pool).await.expect("schema");
    for account in [ACCOUNT, OTHER_ACCOUNT] {
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, ?)")
            .bind(account)
            .bind(account)
            .execute(&engine.pool)
            .await
            .expect("account");
    }
    engine
}

/// A stream-enabled table with four shards. Returns its table id and the
/// shard ids, which follow `init_stream_shards`.
async fn stream_table(engine: &SqliteEngine, account: &str, name: &str, label: &str) -> String {
    let table_id = format!("{name}-{account}-id");
    sqlx::query(
        "INSERT INTO tables (account_id, table_name, key_schema, attribute_definitions, \
         table_status, table_arn, table_id, stream_label) \
         VALUES (?, ?, '[]', '[]', 'ACTIVE', ?, ?, ?)",
    )
    .bind(account)
    .bind(name)
    .bind(extenddb_storage::util::table_arn(REGION, account, name))
    .bind(&table_id)
    .bind(label)
    .execute(&engine.pool)
    .await
    .expect("table");
    for i in 0..4u32 {
        sqlx::query(
            "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
             VALUES (?, ?, ?)",
        )
        .bind(format!("shardId-{table_id}-{i:016}"))
        .bind(&table_id)
        .bind(format!("{:021}", 0))
        .execute(&engine.pool)
        .await
        .expect("shard");
    }
    table_id
}

fn shard(table_id: &str, i: u32) -> String {
    format!("shardId-{table_id}-{i:016}")
}

fn record_json(seq: u64) -> String {
    serde_json::json!({
        "eventID": format!("e{seq}"),
        "eventName": "INSERT",
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodb",
        "awsRegion": REGION,
        "dynamodb": {
            "ApproximateCreationDateTime": 1_700_000_000,
            "Keys": { "pk": { "S": format!("k{seq}") } },
            "SequenceNumber": format!("{seq:021}"),
            "SizeBytes": 10,
            "StreamViewType": "KEYS_ONLY"
        }
    })
    .to_string()
}

async fn write_records(engine: &SqliteEngine, shard_id: &str, table_id: &str, seqs: &[u64]) {
    for seq in seqs {
        sqlx::query(
            "INSERT INTO stream_records (sequence_number, shard_id, table_id, event_name, \
             record_data) VALUES (?, ?, ?, 'Insert', ?)",
        )
        .bind(format!("{seq:021}"))
        .bind(shard_id)
        .bind(table_id)
        .bind(record_json(*seq))
        .execute(&engine.pool)
        .await
        .expect("record");
    }
}

fn assert_stream_not_found(result: extenddb_storage::StreamRecordsResult, arn: &str) {
    let StorageError::TableNotFound(expected) = extenddb_storage::util::stream_not_found(arn)
    else {
        unreachable!("stream_not_found is a TableNotFound error")
    };
    match result {
        Err(StorageError::TableNotFound(msg)) => assert_eq!(msg, expected),
        other => panic!("expected the stream not-found error, got {other:?}"),
    }
}

#[tokio::test]
async fn own_shard_reads_its_records_and_paginates() {
    let engine = engine().await;
    let table_id = stream_table(&engine, ACCOUNT, "t", "2026-01-01T00:00:00.000").await;
    let arn = stream_arn(REGION, ACCOUNT, "t", "2026-01-01T00:00:00.000");
    let shard_id = shard(&table_id, 0);
    // Inserted out of order so the read's ORDER BY, not insertion order,
    // produces the sequence order the pages are asserted in.
    write_records(&engine, &shard_id, &table_id, &[3, 1, 2]).await;

    let (page, last) = engine
        .get_stream_records(ACCOUNT, &arn, &shard_id, None, 2)
        .await
        .expect("first page");
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].dynamodb.sequence_number, format!("{:021}", 1));
    assert_eq!(page[1].dynamodb.sequence_number, format!("{:021}", 2));
    assert_eq!(last.as_deref(), Some(format!("{:021}", 2).as_str()));

    let (rest, last) = engine
        .get_stream_records(ACCOUNT, &arn, &shard_id, last.as_deref(), 10)
        .await
        .expect("second page");
    assert_eq!(rest.len(), 1);
    assert_eq!(last.as_deref(), Some(format!("{:021}", 3).as_str()));
}

#[tokio::test]
async fn idle_shard_of_the_stream_reads_nothing_without_error() {
    let engine = engine().await;
    let table_id = stream_table(&engine, ACCOUNT, "t", "2026-01-01T00:00:00.000").await;
    let arn = stream_arn(REGION, ACCOUNT, "t", "2026-01-01T00:00:00.000");
    let shard_id = shard(&table_id, 1);

    for after in [None, Some(format!("{:021}", 5))] {
        let (records, last) = engine
            .get_stream_records(ACCOUNT, &arn, &shard_id, after.as_deref(), 10)
            .await
            .expect("idle shard reads");
        assert!(records.is_empty());
        assert!(last.is_none());
    }
}

#[tokio::test]
async fn another_streams_shard_under_an_allowed_arn_is_not_found() {
    let engine = engine().await;
    // Both tables carry the same label, so the table name alone decides.
    let a_id = stream_table(&engine, ACCOUNT, "a", "2026-01-01T00:00:00.000").await;
    let b_id = stream_table(&engine, ACCOUNT, "b", "2026-01-01T00:00:00.000").await;
    let arn_a = stream_arn(REGION, ACCOUNT, "a", "2026-01-01T00:00:00.000");
    let arn_b = stream_arn(REGION, ACCOUNT, "b", "2026-01-01T00:00:00.000");
    write_records(&engine, &shard(&a_id, 0), &a_id, &[1]).await;
    write_records(&engine, &shard(&b_id, 0), &b_id, &[1, 2]).await;

    // Stream A's ARN with stream B's shard: nothing is read, and the answer is
    // the one `validate_shard` gives for a shard that is not the stream's.
    // Both directions, so whichever table a lookup without the name would
    // pick, one of them reads a shard that is not its own.
    assert_stream_not_found(
        engine
            .get_stream_records(ACCOUNT, &arn_a, &shard(&b_id, 0), None, 10)
            .await,
        &arn_a,
    );
    assert_stream_not_found(
        engine
            .get_stream_records(ACCOUNT, &arn_b, &shard(&a_id, 0), None, 10)
            .await,
        &arn_b,
    );
    // A shard id that exists for no table.
    assert_stream_not_found(
        engine
            .get_stream_records(ACCOUNT, &arn_a, "shardId-forged-0000000000000000", None, 10)
            .await,
        &arn_a,
    );
    // The control: stream A's own shard reads.
    let (records, _) = engine
        .get_stream_records(ACCOUNT, &arn_a, &shard(&a_id, 0), None, 10)
        .await
        .expect("own shard");
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn another_accounts_stream_is_not_found_under_its_own_arn() {
    let engine = engine().await;
    let other_id = stream_table(&engine, OTHER_ACCOUNT, "t", "2026-01-01T00:00:00.000").await;
    write_records(&engine, &shard(&other_id, 0), &other_id, &[1]).await;
    // The caller spells the other account's stream ARN exactly; the table
    // lookup is scoped to the caller's account, so it resolves to nothing.
    let arn = stream_arn(REGION, OTHER_ACCOUNT, "t", "2026-01-01T00:00:00.000");
    assert_stream_not_found(
        engine
            .get_stream_records(ACCOUNT, &arn, &shard(&other_id, 0), None, 10)
            .await,
        &arn,
    );
}

#[tokio::test]
async fn another_accounts_same_named_stream_is_not_found() {
    let engine = engine().await;
    // Both accounts own a table `t` with the same label, so the account alone
    // decides which table the ARN names.
    let own_id = stream_table(&engine, ACCOUNT, "t", "2026-01-01T00:00:00.000").await;
    let other_id = stream_table(&engine, OTHER_ACCOUNT, "t", "2026-01-01T00:00:00.000").await;
    write_records(&engine, &shard(&own_id, 0), &own_id, &[1]).await;
    write_records(&engine, &shard(&other_id, 0), &other_id, &[1]).await;

    // From both callers, so whichever table a lookup without the account
    // would pick, one caller reads the other account's shard.
    for (caller, foreign, own) in [
        (ACCOUNT, &other_id, &own_id),
        (OTHER_ACCOUNT, &own_id, &other_id),
    ] {
        for arn in [
            stream_arn(REGION, ACCOUNT, "t", "2026-01-01T00:00:00.000"),
            stream_arn(REGION, OTHER_ACCOUNT, "t", "2026-01-01T00:00:00.000"),
        ] {
            assert_stream_not_found(
                engine
                    .get_stream_records(caller, &arn, &shard(foreign, 0), None, 10)
                    .await,
                &arn,
            );
            // The control: the caller's own shard reads under either spelling
            // of the account, since storage binds the caller's account and
            // parses only the table name and label out of the ARN.
            let (records, _) = engine
                .get_stream_records(caller, &arn, &shard(own, 0), None, 10)
                .await
                .expect("own shard reads");
            assert_eq!(records.len(), 1);
        }
    }
}

#[tokio::test]
async fn an_earlier_stream_label_on_the_same_shards_is_not_found() {
    let engine = engine().await;
    let table_id = stream_table(&engine, ACCOUNT, "t", "2026-01-01T00:00:00.000").await;
    let old_arn = stream_arn(REGION, ACCOUNT, "t", "2026-01-01T00:00:00.000");
    let shard_id = shard(&table_id, 0);
    write_records(&engine, &shard_id, &table_id, &[1]).await;
    engine
        .get_stream_records(ACCOUNT, &old_arn, &shard_id, None, 10)
        .await
        .expect("current label reads");

    // The table keeps its shard rows and takes a new stream label, as a
    // stream re-enabled on a table whose label was cleared does.
    sqlx::query("UPDATE tables SET stream_label = ? WHERE table_id = ?")
        .bind("2026-02-02T00:00:00.000")
        .bind(&table_id)
        .execute(&engine.pool)
        .await
        .expect("relabel");
    write_records(&engine, &shard_id, &table_id, &[2]).await;

    assert_stream_not_found(
        engine
            .get_stream_records(ACCOUNT, &old_arn, &shard_id, None, 10)
            .await,
        &old_arn,
    );
    let new_arn = stream_arn(REGION, ACCOUNT, "t", "2026-02-02T00:00:00.000");
    let (records, _) = engine
        .get_stream_records(ACCOUNT, &new_arn, &shard_id, None, 10)
        .await
        .expect("new label reads");
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn a_malformed_stream_arn_is_a_validation_error() {
    let engine = engine().await;
    let table_id = stream_table(&engine, ACCOUNT, "t", "2026-01-01T00:00:00.000").await;
    let err = engine
        .get_stream_records(ACCOUNT, "not-an-arn", &shard(&table_id, 0), None, 10)
        .await
        .expect_err("malformed ARN");
    assert!(matches!(err, StorageError::Validation(_)), "{err:?}");
}
