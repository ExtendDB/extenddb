// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A stream consumer on PostgreSQL reads every committed write once, and the
//! records of one item in the order their writes committed (#808).
//!
//! A write stores its stream record in `stream_pending`, inside the write's
//! own transaction, and the record gets its sequence number only after the
//! write commits. GetRecords and a LATEST iterator number a batch of the
//! shard's committed records before they read it, and a worker numbers the
//! shards that nobody reads. Before this, a write drew its number inside its
//! transaction, and it could commit after a write that drew a higher number.
//! A consumer that had read the higher number then never read the first
//! write's record.
//!
//! The tests hold a write open at its commit, after it stored its record,
//! with a deferred constraint trigger that waits on an advisory lock, the
//! gate.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`); skips otherwise. Each test builds
//! its own scratch database and drops it when it passes. A failing test
//! leaves its database, named `eddb_sord_*`, behind.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
    KeyType, ReturnValuesOnConditionCheckFailure, ScalarAttributeType, StreamRecord,
    StreamSpecification, StreamViewType, TableKeyInfo,
};
use extenddb_storage::util::stream_arn;
use extenddb_storage::{DataEngine, StreamCapture, StreamEngine, TableEngine, TransactWriteOp};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_stream_order";

/// The advisory lock key of the gate.
const GATE: i64 = 0x5374_4f72;

/// A page size larger than any test writes.
const PAGE: i64 = 1_000;

struct Scratch {
    engine: PostgresEngine,
    db: PgPool,
    admin: PgPool,
    db_name: String,
}

impl Scratch {
    async fn cleanup(self) {
        let Scratch {
            engine,
            db,
            admin,
            db_name,
        } = self;
        drop(engine);
        db.close().await;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
        ))
        .execute(&admin)
        .await
        .expect("drop the scratch database");
        admin.close().await;
    }

    /// The test table, keyed on `pk` and `sk`, with a NEW_AND_OLD_IMAGES
    /// stream. Returns its key info, its table id and its stream ARN.
    async fn stream_table(&self) -> (TableKeyInfo, String, String) {
        let key = |name: &str, key_type| {
            (
                KeySchemaElement {
                    attribute_name: name.to_owned(),
                    key_type,
                },
                AttributeDefinition {
                    attribute_name: name.to_owned(),
                    attribute_type: ScalarAttributeType::S,
                },
            )
        };
        let (key_schema, attribute_definitions) =
            [key("pk", KeyType::Hash), key("sk", KeyType::Range)]
                .into_iter()
                .unzip();
        let desc = self
            .engine
            .create_table(
                ACCOUNT,
                CreateTableInput {
                    table_name: TABLE.to_owned(),
                    key_schema,
                    attribute_definitions,
                    billing_mode: Some(BillingMode::PayPerRequest),
                    stream_specification: Some(StreamSpecification {
                        stream_enabled: true,
                        stream_view_type: Some(StreamViewType::NewAndOldImages),
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("create the table");
        // The stream's label is carried in its shard ids, so the ARN takes the
        // label the table was created with rather than pinning one.
        let label = desc
            .latest_stream_label
            .clone()
            .expect("a stream-enabled table has a label");
        let key_info = self
            .engine
            .table_key_info(ACCOUNT, TABLE)
            .await
            .expect("read the key info");
        (
            key_info,
            desc.table_id,
            stream_arn(REGION, ACCOUNT, TABLE, &label),
        )
    }

    /// The shard the records of partition key `pk` go to.
    async fn shard_of(&self, pk: &str) -> String {
        self.engine
            .assign_shard(ACCOUNT, TABLE, pk)
            .await
            .expect("assign a shard")
    }

    /// Store `item("pk", sk, v)`, with its stream record.
    async fn put(&self, key_info: &TableKeyInfo, pk: &str, sk: &str, v: &str) {
        self.engine
            .put_item(
                key_info,
                item(pk, sk, v),
                false,
                None,
                &ExpressionMaps::default(),
                Some(&capture()),
            )
            .await
            .expect("store the item");
    }

    /// One GetRecords page of `shard`, after `after` or from the start.
    async fn read(
        &self,
        arn: &str,
        shard: &str,
        after: Option<&str>,
    ) -> (Vec<StreamRecord>, Option<String>) {
        self.engine
            .get_stream_records(ACCOUNT, arn, shard, after, PAGE)
            .await
            .expect("read the shard")
    }

    /// How many records of `shard` wait in `stream_pending` for a number.
    async fn pending(&self, shard: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM stream_pending WHERE shard_id = $1")
            .bind(shard)
            .fetch_one(&self.db)
            .await
            .expect("count the pending records")
    }
}

/// The server's base URL, or `None` to skip. With `EXTENDDB_TEST_PG_REQUIRED`
/// set to any value but `0`, a job that must reach a server fails here
/// instead.
fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.trim().is_empty() {
        let required =
            std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0");
        assert!(
            !required,
            "EXTENDDB_TEST_PG_CONNECTION_STRING is not set, and EXTENDDB_TEST_PG_REQUIRED says \
             this job must reach a PostgreSQL server"
        );
        return None;
    }
    Some(conn.trim_end_matches('/').to_owned())
}

/// Every `.sql` file under `migrations/` and `data_migrations/`, each
/// directory in filename order, so a new migration is picked up here without
/// editing this file.
fn shipped_migrations() -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for dir in ["migrations", "data_migrations"] {
        let mut files: Vec<_> = std::fs::read_dir(root.join(dir))
            .expect("read the migrations directory")
            .map(|e| e.expect("directory entry").path())
            .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            .collect();
        files.sort();
        for f in files {
            out.push(std::fs::read_to_string(&f).expect("read a migration file"));
        }
    }
    out
}

fn skip(test: &str) {
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch database in."
    );
}

/// A scratch database with every shipped migration applied, and an engine
/// whose pool holds `pool_size` connections. With `default_isolation`, the
/// engine's sessions start their transactions at that level unless a
/// transaction names its own, as with an operator's
/// `default_transaction_isolation`.
async fn scratch(default_isolation: Option<&str>, pool_size: u32) -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_sord_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin)
        .await
        .expect("create the scratch database");
    let url = format!("{base}/{db_name}");
    let db = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to the scratch database");
    // One scratch database stands in for both the catalog and the data
    // database, so every shipped migration file from both directories is
    // applied to it, in filename order.
    for sql in shipped_migrations() {
        sqlx::raw_sql(&sql)
            .execute(&db)
            .await
            .expect("apply a shipped migration");
    }
    // The catalog schema's `stream_shards` carries a foreign key to `tables`
    // that cannot exist in a deployment (the two live in different
    // databases) and that the engine's insert order violates.
    sqlx::query("ALTER TABLE stream_shards DROP CONSTRAINT IF EXISTS stream_shards_table_id_fkey")
        .execute(&db)
        .await
        .expect("match the two-database layout");
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&db)
    .await
    .expect("pin the control-plane delay to zero");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{db_name}"))
        .execute(&db)
        .await
        .expect("seed the account row");
    if let Some(level) = default_isolation {
        // Sessions opened from here on, the engine's among them, take it.
        sqlx::query(&format!(
            "ALTER DATABASE \"{db_name}\" SET default_transaction_isolation = '{level}'"
        ))
        .execute(&admin)
        .await
        .expect("set the default isolation level");
    }
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: url,
            pool_size,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch database");
    Scratch {
        engine,
        db,
        admin,
        db_name,
    }
}

fn item(pk: &str, sk: &str, v: &str) -> Item {
    BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("sk".to_owned(), AttributeValue::S(sk.to_owned())),
        ("v".to_owned(), AttributeValue::S(v.to_owned())),
    ])
}

fn capture() -> StreamCapture {
    StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from(REGION),
    }
}

/// The `v` of each record's new image, in the order read.
fn values(records: &[StreamRecord]) -> Vec<String> {
    records
        .iter()
        .map(
            |r| match r.dynamodb.new_image.as_ref().and_then(|i| i.get("v")) {
                Some(AttributeValue::S(v)) => v.clone(),
                other => panic!("a record whose new image has no string v: {other:?}"),
            },
        )
        .collect()
}

fn string_key(r: &StreamRecord, name: &str) -> String {
    match r.dynamodb.keys.get(name) {
        Some(AttributeValue::S(v)) => v.clone(),
        other => panic!("a record without a string {name} key: {other:?}"),
    }
}

/// Open a connection to the scratch database outside its pools. Returns it and
/// its backend pid.
async fn connect(s: &Scratch) -> (PgConnection, i32) {
    let base = base_conn().expect("caller checks base_conn() first");
    let mut conn = PgConnection::connect(&format!("{base}/{}", s.db_name))
        .await
        .expect("connect to the scratch database");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .expect("read the backend pid");
    (conn, pid)
}

/// Make a transaction that writes a row of `relation` for which `condition`
/// (an expression on `NEW`) holds wait at its commit while the gate is
/// closed. A deferred constraint trigger runs at commit, after every
/// statement of the transaction, so the write has stored its stream record
/// by then.
async fn install_commit_gate(s: &Scratch, relation: &str, condition: &str) {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION hold_at_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
           IF {condition} THEN PERFORM pg_advisory_xact_lock_shared({GATE}); END IF; \
           RETURN NULL; \
         END $$; \
         CREATE CONSTRAINT TRIGGER hold_at_commit AFTER INSERT OR UPDATE ON {relation} \
           DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION hold_at_commit();"
    ))
    .execute(&s.db)
    .await
    .expect("install the gate trigger");
}

/// Close the gate on a session of its own. Returns the session and its pid.
async fn close_gate(s: &Scratch) -> (PgConnection, i32) {
    let (mut gate, pid) = connect(s).await;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(GATE)
        .execute(&mut gate)
        .await
        .expect("close the gate");
    (gate, pid)
}

async fn open_gate(gate: &mut PgConnection) {
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(GATE)
        .execute(gate)
        .await
        .expect("open the gate");
}

/// The quoted name of the test table's data table.
fn data_table(table_id: &str) -> String {
    format!("\"_ddb_{table_id}\"")
}

/// Wait until a backend waits for a lock of kind `event` that the backend
/// `blocker` holds. Fails after 10 s, so the caller can report what it saw.
async fn wait_until_blocked_by(db: &PgPool, blocker: i32, event: &str) -> Result<(), String> {
    for _ in 0..1_000 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND wait_event = $2 AND $1 = ANY(pg_blocking_pids(pid))",
        )
        .bind(blocker)
        .bind(event)
        .fetch_one(db)
        .await
        .expect("read pg_stat_activity");
        if waiting > 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "no backend waited on the {event} lock of backend {blocker}"
    ))
}

/// The case #808 names. A write stores its record and then waits at its
/// commit. A second write to the same shard commits, and a consumer reads
/// its record. The first write then commits, and the consumer's next read,
/// after the number it holds, must return the first write's record. With
/// the number drawn inside the write's transaction, the first write's
/// number was below the consumer's, and no read after it returned the
/// record.
#[tokio::test]
async fn a_write_that_commits_after_a_read_is_returned_by_the_next_read() {
    let test = "a_write_that_commits_after_a_read_is_returned_by_the_next_read";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(None, 12).await;
    let (key_info, table_id, arn) = s.stream_table().await;
    install_commit_gate(
        &s,
        &data_table(&table_id),
        "NEW.item_data->'v'->>'S' = 'held'",
    )
    .await;
    let shard = s.shard_of("p").await;
    let (mut gate, gate_pid) = close_gate(&s).await;

    let held = s.put(&key_info, "p", "1", "held");
    let rest = async {
        wait_until_blocked_by(&s.db, gate_pid, "advisory").await?;
        s.put(&key_info, "p", "2", "free").await;
        let first = s.read(&arn, &shard, None).await;
        open_gate(&mut gate).await;
        Ok::<_, String>(first)
    };
    let ((), rest) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(held, rest) })
            .await
            .expect("both writes finished");
    let (first, cursor) = rest.unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        values(&first),
        ["free"],
        "the first read returns the committed write only"
    );
    let cursor = cursor.expect("the first read returns a sequence number");

    let (next, _) = s.read(&arn, &shard, Some(&cursor)).await;
    assert_eq!(
        values(&next),
        ["held"],
        "the read after the consumer's number returns the write that committed late"
    );
    assert!(
        next[0].dynamodb.sequence_number > cursor,
        "the late write's number {} is above the number {cursor} the consumer read",
        next[0].dynamodb.sequence_number
    );
    let (all, _) = s.read(&arn, &shard, None).await;
    assert_eq!(
        values(&all),
        ["free", "held"],
        "from the start, the records are in commit order"
    );
    assert_eq!(s.pending(&shard).await, 0, "no record is left pending");

    drop(gate);
    s.cleanup().await;
}

/// Writers race on four hot items while a consumer pages through every
/// shard. The consumer must read each write's record exactly once, and each
/// item's records must chain: the old image of a record is the new image of
/// the item's record before it. A record out of order breaks the chain.
#[tokio::test]
async fn concurrent_writers_lose_no_record_and_keep_each_items_order() {
    let test = "concurrent_writers_lose_no_record_and_keep_each_items_order";
    if base_conn().is_none() {
        return skip(test);
    }
    const WRITERS: usize = 8;
    const WRITES: usize = 150;
    const ITEMS: [&str; 4] = ["h0", "h1", "h2", "h3"];
    let s = scratch(None, 12).await;
    let (key_info, table_id, arn) = s.stream_table().await;
    let shards: Vec<String> =
        sqlx::query_scalar("SELECT shard_id FROM stream_shards WHERE table_id = $1 ORDER BY 1")
            .bind(&table_id)
            .fetch_all(&s.db)
            .await
            .expect("list the shards");

    let done = AtomicBool::new(false);
    let writers = async {
        futures::future::join_all((0..WRITERS).map(|w| {
            let (s, key_info) = (&s, &key_info);
            async move {
                for i in 0..WRITES {
                    let pk = ITEMS[(w + i) % ITEMS.len()];
                    s.put(key_info, pk, "s", &format!("w{w}-{i}")).await;
                }
            }
        }))
        .await;
        done.store(true, Ordering::SeqCst);
    };
    let consumer = async {
        let mut cursors: Vec<Option<String>> = vec![None; shards.len()];
        let mut read: Vec<StreamRecord> = Vec::new();
        loop {
            // Read once more after the writers finish, to take the rest.
            let finished = done.load(Ordering::SeqCst);
            for (shard, cursor) in shards.iter().zip(cursors.iter_mut()) {
                loop {
                    let (page, last) = s.read(&arn, shard, cursor.as_deref()).await;
                    if page.is_empty() {
                        break;
                    }
                    *cursor = last;
                    read.extend(page);
                }
            }
            if finished {
                return read;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    let ((), read) = tokio::time::timeout(Duration::from_secs(120), async {
        tokio::join!(writers, consumer)
    })
    .await
    .expect("the writers and the consumer finished");

    let tags = values(&read);
    let distinct: BTreeSet<&String> = tags.iter().collect();
    assert_eq!(
        distinct.len(),
        tags.len(),
        "the consumer read a record twice"
    );
    assert_eq!(
        tags.len(),
        WRITERS * WRITES,
        "the consumer read {} of the {} writes",
        tags.len(),
        WRITERS * WRITES
    );

    // One item's records are all on one shard, and a shard's records are
    // read in order, so the item's records keep their order in `read`.
    let mut last_image: BTreeMap<String, Item> = BTreeMap::new();
    for r in &read {
        let pk = string_key(r, "pk");
        assert_eq!(
            r.dynamodb.old_image.as_ref(),
            last_image.get(&pk),
            "item {pk}: the record {} does not follow the item's record before it",
            r.dynamodb.sequence_number
        );
        let new_image = r.dynamodb.new_image.clone().expect("a put has a new image");
        last_image.insert(pk, new_image);
    }
    for pk in ITEMS {
        let stored: serde_json::Value = sqlx::query_scalar(&format!(
            "SELECT item_data FROM {} WHERE pk = $1",
            data_table(&table_id)
        ))
        .bind(pk)
        .fetch_one(&s.db)
        .await
        .expect("read the stored item");
        let stored: Item = serde_json::from_value(stored).expect("an item");
        assert_eq!(
            last_image.get(pk),
            Some(&stored),
            "item {pk}: the last record is the last write"
        );
    }

    s.cleanup().await;
}

/// A read returns every write that committed before it, in every round of
/// a write followed by a read.
#[tokio::test]
async fn a_read_returns_the_write_that_committed_before_it() {
    let test = "a_read_returns_the_write_that_committed_before_it";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(None, 12).await;
    let (key_info, _, arn) = s.stream_table().await;
    let shard = s.shard_of("r").await;
    let mut cursor: Option<String> = None;
    for i in 0..20 {
        let v = format!("v{i:02}");
        s.put(&key_info, "r", &format!("{i:02}"), &v).await;
        let (page, last) = s.read(&arn, &shard, cursor.as_deref()).await;
        assert_eq!(values(&page), [v.as_str()], "round {i}");
        let seq = &page[0].dynamodb.sequence_number;
        assert_eq!(seq.len(), 21, "round {i}: the number {seq} has 21 digits");
        assert!(
            cursor.as_ref().is_none_or(|c| seq > c),
            "round {i}: the number {seq} is above the one before, {cursor:?}"
        );
        cursor = last;
    }

    s.cleanup().await;
}

/// A LATEST iterator starts after every write that committed before it, so
/// those writes are numbered first. Without that, a shard whose records
/// were all pending had no number to start after, and a LATEST iterator
/// read the shard from the start. A write still open when the iterator is
/// taken is numbered after it commits, and the iterator returns it.
#[tokio::test]
async fn a_latest_iterator_starts_after_the_committed_writes() {
    let test = "a_latest_iterator_starts_after_the_committed_writes";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(None, 12).await;
    let (key_info, table_id, arn) = s.stream_table().await;
    install_commit_gate(
        &s,
        &data_table(&table_id),
        "NEW.item_data->'v'->>'S' = 'held'",
    )
    .await;
    let shard = s.shard_of("p").await;

    s.put(&key_info, "p", "1", "a").await;
    let after_a = s
        .engine
        .latest_sequence_number(&shard)
        .await
        .expect("resolve LATEST")
        .expect("LATEST resolves to the number of the committed write");
    let (page, _) = s.read(&arn, &shard, None).await;
    assert_eq!(values(&page), ["a"]);
    assert_eq!(page[0].dynamodb.sequence_number, after_a);

    s.put(&key_info, "p", "2", "b").await;
    let (mut gate, gate_pid) = close_gate(&s).await;
    let held = s.put(&key_info, "p", "3", "held");
    let rest = async {
        wait_until_blocked_by(&s.db, gate_pid, "advisory").await?;
        let after_b = s
            .engine
            .latest_sequence_number(&shard)
            .await
            .expect("resolve LATEST");
        open_gate(&mut gate).await;
        Ok::<_, String>(after_b)
    };
    let ((), rest) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(held, rest) })
            .await
            .expect("the held write finished");
    let after_b = rest
        .unwrap_or_else(|e| panic!("{e}"))
        .expect("LATEST resolves while a write is open");

    let (page, _) = s.read(&arn, &shard, Some(&after_a)).await;
    assert_eq!(values(&page), ["b", "held"]);
    assert_eq!(
        page[0].dynamodb.sequence_number, after_b,
        "LATEST taken during the open write starts after b"
    );
    let (page, _) = s.read(&arn, &shard, Some(&after_b)).await;
    assert_eq!(
        values(&page),
        ["held"],
        "the write open when LATEST was taken is returned"
    );

    drop(gate);
    s.cleanup().await;
}

/// The numbering worker numbers the shards that nobody reads, and it skips
/// a shard that another transaction is numbering instead of waiting. The
/// number goes in the `sequence_number` column and in the record.
#[tokio::test]
async fn the_worker_numbers_unread_shards_and_skips_a_locked_one() {
    let test = "the_worker_numbers_unread_shards_and_skips_a_locked_one";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(None, 12).await;
    let (key_info, _, _) = s.stream_table().await;
    // Two partition keys on two shards.
    let first = s.shard_of("k0").await;
    let mut other = None;
    for i in 1..100 {
        let pk = format!("k{i}");
        let shard = s.shard_of(&pk).await;
        if shard != first {
            other = Some((pk, shard));
            break;
        }
    }
    let (other_pk, second) = other.expect("a partition key on another shard");
    s.put(&key_info, "k0", "s", "one").await;
    s.put(&key_info, &other_pk, "s", "two").await;
    assert_eq!((s.pending(&first).await, s.pending(&second).await), (1, 1));

    // An outside transaction holds the first shard's row, as a numbering of
    // that shard does.
    let mut holder = s.db.begin().await.expect("begin the outside transaction");
    sqlx::query("SELECT 1 FROM stream_shards WHERE shard_id = $1 FOR NO KEY UPDATE")
        .bind(&first)
        .execute(&mut *holder)
        .await
        .expect("lock the first shard");
    let moved = tokio::time::timeout(
        Duration::from_secs(10),
        s.engine.number_pending_stream_records_for_tests(),
    )
    .await
    .expect("the worker does not wait for the locked shard")
    .expect("number the pending records");
    assert_eq!(moved, 1, "only the unlocked shard is numbered");
    assert_eq!((s.pending(&first).await, s.pending(&second).await), (1, 0));
    holder.rollback().await.expect("release the first shard");

    let moved = s
        .engine
        .number_pending_stream_records_for_tests()
        .await
        .expect("number the pending records");
    assert_eq!(moved, 1, "the released shard is numbered");
    assert_eq!((s.pending(&first).await, s.pending(&second).await), (0, 0));

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT sequence_number, record_data->'dynamodb'->>'SequenceNumber' FROM stream_records",
    )
    .fetch_all(&s.db)
    .await
    .expect("read the numbered records");
    assert_eq!(rows.len(), 2);
    for (column, in_record) in &rows {
        assert_eq!(column, in_record, "the record carries its number");
        assert_eq!(column.len(), 21, "{column} has 21 digits");
    }

    s.cleanup().await;
}

/// Under an operator's REPEATABLE READ default, a read of a shard that
/// another transaction numbers must succeed and return what that numbering
/// commits. The read waits for the numbering with no connection held, and its
/// own numbering transaction is READ COMMITTED whatever the default.
#[tokio::test]
async fn a_read_during_a_numbering_succeeds_under_repeatable_read() {
    let test = "a_read_during_a_numbering_succeeds_under_repeatable_read";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(Some("repeatable read"), 12).await;
    let (key_info, _, arn) = s.stream_table().await;
    // The first numbering waits at its commit, holding the shard's row.
    install_commit_gate(
        &s,
        "stream_records",
        "NEW.record_data->'dynamodb'->'NewImage'->'v'->>'S' = 'held'",
    )
    .await;
    let shard = s.shard_of("p").await;
    s.put(&key_info, "p", "1", "held").await;
    let (mut gate, gate_pid) = close_gate(&s).await;

    let numbering = s.engine.number_pending_stream_records_for_tests();
    let read = async {
        wait_until_blocked_by(&s.db, gate_pid, "advisory").await?;
        let started = std::time::Instant::now();
        let read = s
            .engine
            .get_stream_records(ACCOUNT, &arn, &shard, None, PAGE)
            .await;
        Ok::<_, String>((read, started.elapsed()))
    };
    let release = async {
        let parked = wait_until_blocked_by(&s.db, gate_pid, "advisory").await;
        // The read looks at the shard while the numbering holds it.
        tokio::time::sleep(Duration::from_millis(200)).await;
        open_gate(&mut gate).await;
        parked
    };
    let (numbered, read, parked) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(numbering, read, release)
    })
    .await
    .expect("the numbering and the read finished");
    let (read, waited) = read.unwrap_or_else(|e| panic!("{e}"));
    if let Err(e) = parked {
        panic!("{e}; the read returned {read:?}");
    }
    assert_eq!(numbered.expect("the first numbering commits"), 1);
    let (records, _) = read.expect("the read during the numbering succeeds");
    assert_eq!(values(&records), ["held"]);
    assert!(
        waited >= Duration::from_millis(150),
        "the read returned after {waited:?}, before the numbering committed"
    );

    drop(gate);
    s.cleanup().await;
}

/// Readers of a shard that another transaction numbers wait for it with no
/// pool connection held. Twice as many readers as the pool has connections
/// wait on a held shard, and a write must still get a connection at once.
/// When each waiting reader held a connection, the write waited until the
/// shard was released.
#[tokio::test]
async fn readers_waiting_for_a_numbering_leave_the_pool_to_writes() {
    let test = "readers_waiting_for_a_numbering_leave_the_pool_to_writes";
    if base_conn().is_none() {
        return skip(test);
    }
    const POOL: u32 = 10;
    let s = scratch(None, POOL).await;
    let (key_info, _, arn) = s.stream_table().await;
    let shard = s.shard_of("p").await;
    s.put(&key_info, "p", "1", "pending").await;

    // An outside transaction holds the shard's row, as a running numbering
    // does.
    let mut holder = s.db.begin().await.expect("begin the outside transaction");
    sqlx::query("SELECT 1 FROM stream_shards WHERE shard_id = $1 FOR NO KEY UPDATE")
        .bind(&shard)
        .execute(&mut *holder)
        .await
        .expect("hold the shard");
    let readers = futures::future::join_all((0..2 * POOL).map(|_| s.read(&arn, &shard, None)));
    let write = async {
        // The readers reach the held shard first.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = std::time::Instant::now();
        let put = tokio::time::timeout(
            Duration::from_secs(5),
            s.put(&key_info, "q", "1", "unrelated"),
        )
        .await;
        let took = started.elapsed();
        holder.rollback().await.expect("release the shard");
        (put.is_ok(), took)
    };
    let (reads, (put_done, took)) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(readers, write)
    })
    .await
    .expect("the readers and the write finished");
    assert!(
        put_done && took < Duration::from_secs(1),
        "the write took {took:?} while {} readers waited on the held shard",
        2 * POOL
    );
    for (records, _) in &reads {
        assert_eq!(values(records), ["pending"], "each reader gets the record");
    }

    s.cleanup().await;
}

/// A read numbers one batch of the shard's pending records before it reads
/// the shard, so the time GetRecords spends numbering is bounded by one
/// batch. Of a backlog of 1,500 records, the first read numbers and returns
/// the first 1,000, and the rest wait. The next read numbers and returns the
/// rest, after the first read's numbers. When a read numbered up to 10,000
/// records, the first read numbered all 1,500.
#[tokio::test]
async fn a_read_numbers_one_batch_of_a_backlog() {
    let test = "a_read_numbers_one_batch_of_a_backlog";
    if base_conn().is_none() {
        return skip(test);
    }
    const BACKLOG: usize = 1_500;
    let s = scratch(None, 12).await;
    let (key_info, _, arn) = s.stream_table().await;
    let shard = s.shard_of("b").await;
    let written: Vec<String> = (0..BACKLOG).map(|i| format!("v{i:04}")).collect();
    for (i, v) in written.iter().enumerate() {
        s.put(&key_info, "b", &format!("{i:04}"), v).await;
    }
    assert_eq!(s.pending(&shard).await, 1_500);

    let (first, cursor) = s.read(&arn, &shard, None).await;
    assert_eq!(
        values(&first),
        &written[..1_000],
        "the first read returns the first batch"
    );
    assert_eq!(
        s.pending(&shard).await,
        500,
        "the first read numbers one batch"
    );

    let (rest, _) = s.read(&arn, &shard, cursor.as_deref()).await;
    assert_eq!(
        values(&rest),
        &written[1_000..],
        "the next read returns the rest in order"
    );
    assert_eq!(s.pending(&shard).await, 0);
    assert!(
        rest[0].dynamodb.sequence_number > first[999].dynamodb.sequence_number,
        "the rest are numbered after the first batch"
    );

    s.cleanup().await;
}

/// A read whose numbering fails returns the records numbered before it,
/// and logs the failure, where it failed GetRecords before. A LATEST
/// iterator taken then starts after the numbered records. Once the cause is
/// gone, the next read numbers and returns the record that waited.
#[tokio::test]
async fn a_read_whose_numbering_fails_returns_the_numbered_records() {
    let test = "a_read_whose_numbering_fails_returns_the_numbered_records";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(None, 12).await;
    let (key_info, _, arn) = s.stream_table().await;
    let shard = s.shard_of("f").await;
    s.put(&key_info, "f", "1", "a").await;
    let (page, after_a) = s.read(&arn, &shard, None).await;
    assert_eq!(values(&page), ["a"]);
    let after_a = after_a.expect("the read returns the number of a");

    // A numbering that copies the record whose v is "refused" fails.
    sqlx::raw_sql(
        "CREATE FUNCTION refuse_numbering() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
           IF NEW.record_data->'dynamodb'->'NewImage'->'v'->>'S' = 'refused' THEN \
             RAISE EXCEPTION 'the test refuses this numbering'; \
           END IF; \
           RETURN NEW; \
         END $$; \
         CREATE TRIGGER refuse_numbering BEFORE INSERT ON stream_records \
           FOR EACH ROW EXECUTE FUNCTION refuse_numbering();",
    )
    .execute(&s.db)
    .await
    .expect("install the failing trigger");
    s.put(&key_info, "f", "2", "refused").await;

    let (page, _) = s.read(&arn, &shard, None).await;
    assert_eq!(
        values(&page),
        ["a"],
        "the read returns the records numbered before"
    );
    assert_eq!(s.pending(&shard).await, 1, "the refused record waits");
    let latest = s
        .engine
        .latest_sequence_number(&shard)
        .await
        .expect("LATEST resolves while the numbering fails");
    assert_eq!(latest.as_deref(), Some(after_a.as_str()));

    sqlx::raw_sql("DROP TRIGGER refuse_numbering ON stream_records")
        .execute(&s.db)
        .await
        .expect("remove the failing trigger");
    let (page, _) = s.read(&arn, &shard, Some(&after_a)).await;
    assert_eq!(
        values(&page),
        ["refused"],
        "the next read returns the record that waited"
    );
    assert_eq!(s.pending(&shard).await, 0);

    s.cleanup().await;
}

/// About 100 KB of text that compresses poorly, from a fixed seed.
fn noise(len: usize, seed: u64) -> String {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            char::from(b"0123456789abcdef"[(x % 16) as usize])
        })
        .collect()
}

/// About 100 KB of text that compresses to a small part of its length.
fn compressible(len: usize, seed: u64) -> String {
    format!("{seed:04}").repeat(len / 4)
}

/// Number every pending record of `shard` in one pass of the worker. Returns
/// the `record_bytes` of the records each numbering transaction moved, one
/// list per transaction, in the order the records were stored.
async fn number_in_batches(s: &Scratch, shard: &str) -> Vec<Vec<i64>> {
    let sizes: Vec<i64> = sqlx::query_scalar(
        "SELECT record_bytes FROM stream_pending WHERE shard_id = $1 ORDER BY id",
    )
    .bind(shard)
    .fetch_all(&s.db)
    .await
    .expect("read the sizes of the pending records");
    let moved = s
        .engine
        .number_pending_stream_records_for_tests()
        .await
        .expect("number the pending records");
    assert_eq!(moved, sizes.len() as u64, "one pass numbers every record");
    // The records one numbering transaction moved share its xmin, and the
    // numbering gives them numbers in the order they were stored.
    let xmins: Vec<String> = sqlx::query_scalar(
        "SELECT xmin::text FROM stream_records WHERE shard_id = $1 ORDER BY sequence_number",
    )
    .bind(shard)
    .fetch_all(&s.db)
    .await
    .expect("read the numbered records");
    assert_eq!(xmins.len(), sizes.len());
    let mut batches: Vec<(String, Vec<i64>)> = Vec::new();
    for (xmin, size) in xmins.into_iter().zip(sizes) {
        match batches.last_mut() {
            Some((x, sizes)) if *x == xmin => sizes.push(size),
            _ => batches.push((xmin, vec![size])),
        }
    }
    batches.into_iter().map(|(_, sizes)| sizes).collect()
}

/// `records` records of about 100 KB took three or more numbering
/// transactions, and in each the records before the last hold less than a
/// mebibyte of JSON text.
fn assert_batches_stop_at_a_mebibyte(batches: &[Vec<i64>], records: u64) {
    assert!(
        batches.len() >= 3,
        "{records} records of about 100 KB took {} transaction(s)",
        batches.len()
    );
    for (i, sizes) in batches.iter().enumerate() {
        let before_last: i64 = sizes[..sizes.len() - 1].iter().sum();
        assert!(
            before_last < 1 << 20,
            "transaction {i} took {} records, {before_last} bytes before its last",
            sizes.len()
        );
    }
}

/// A numbering transaction stops adding records to its batch once the
/// records before the next one hold a mebibyte, so the shard's row is held
/// for a bounded copy. Records of about 100 KB go about ten to a
/// transaction, and one pass of the worker still numbers them all.
#[tokio::test]
async fn a_numbering_batch_stops_at_a_mebibyte_of_records() {
    let test = "a_numbering_batch_stops_at_a_mebibyte_of_records";
    if base_conn().is_none() {
        return skip(test);
    }
    const RECORDS: u64 = 30;
    let s = scratch(None, 12).await;
    let (key_info, _, _) = s.stream_table().await;
    let shard = s.shard_of("big").await;
    for i in 0..RECORDS {
        s.put(&key_info, "big", &format!("{i:02}"), &noise(100_000, i))
            .await;
    }
    let batches = number_in_batches(&s, &shard).await;
    assert_batches_stop_at_a_mebibyte(&batches, RECORDS);

    s.cleanup().await;
}

/// A batch counts each record at the length of its JSON text, not at the
/// size PostgreSQL stores after compression, because the copy decompresses
/// and writes the whole record. Records of about 100 KB that compress to a
/// few kilobytes still go about ten to a transaction. Counted at their
/// stored size, all 30 went into one transaction.
#[tokio::test]
async fn a_numbering_batch_counts_compressible_records_at_their_text_length() {
    let test = "a_numbering_batch_counts_compressible_records_at_their_text_length";
    if base_conn().is_none() {
        return skip(test);
    }
    const RECORDS: u64 = 30;
    let s = scratch(None, 12).await;
    let (key_info, _, _) = s.stream_table().await;
    let shard = s.shard_of("big").await;
    for i in 0..RECORDS {
        s.put(
            &key_info,
            "big",
            &format!("{i:02}"),
            &compressible(100_000, i),
        )
        .await;
    }
    let stored: i64 = sqlx::query_scalar(
        "SELECT max(pg_column_size(record_data))::bigint FROM stream_pending WHERE shard_id = $1",
    )
    .bind(&shard)
    .fetch_one(&s.db)
    .await
    .expect("read the stored sizes");
    assert!(
        stored < 10_000,
        "a record of about 100 KB is stored in {stored} bytes, so it is compressed"
    );
    let batches = number_in_batches(&s, &shard).await;
    assert_batches_stop_at_a_mebibyte(&batches, RECORDS);

    s.cleanup().await;
}

/// Writers put, delete, and put two items on two shards in one
/// TransactWriteItems, on eight hot items, while the numbering worker runs in
/// a loop and two consumers page every shard. Each consumer must read every
/// record once, both must read each shard in the same order, and each item's
/// records must chain: the old image of a record is the new image of the
/// item's record before it, or none after a delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_of_every_kind_beside_the_worker_reach_two_consumers_in_order() {
    let test = "writes_of_every_kind_beside_the_worker_reach_two_consumers_in_order";
    if base_conn().is_none() {
        return skip(test);
    }
    const WRITERS: usize = 8;
    const OPS: usize = 120;
    let s = scratch(None, 20).await;
    let (key_info, table_id, arn) = s.stream_table().await;
    let shards: Vec<String> =
        sqlx::query_scalar("SELECT shard_id FROM stream_shards WHERE table_id = $1 ORDER BY 1")
            .bind(&table_id)
            .fetch_all(&s.db)
            .await
            .expect("list the shards");
    // Four hot items on one shard and four on another.
    let (mut left, mut right) = (Vec::new(), Vec::new());
    for i in 0..200 {
        let pk = format!("h{i}");
        let shard = s.shard_of(&pk).await;
        if shard == shards[0] && left.len() < 4 {
            left.push(pk);
        } else if shard == shards[1] && right.len() < 4 {
            right.push(pk);
        }
    }
    assert_eq!((left.len(), right.len()), (4, 4), "hot items on two shards");

    let maps = ExpressionMaps::default();
    let cap = capture();
    let expected = std::sync::atomic::AtomicUsize::new(0);
    let done = AtomicBool::new(false);
    let writers = async {
        futures::future::join_all((0..WRITERS).map(|w| {
            let (s, key_info, maps, cap, left, right, expected) =
                (&s, &key_info, &maps, &cap, &left, &right, &expected);
            async move {
                for i in 0..OPS {
                    let tag = format!("w{w}-{i}");
                    let (a, b) = (&left[(w + i) % 4], &right[(w * 3 + i) % 4]);
                    match i % 10 {
                        7 => {
                            let key = BTreeMap::from([
                                ("pk".to_owned(), AttributeValue::S(a.clone())),
                                ("sk".to_owned(), AttributeValue::S("s".to_owned())),
                            ]);
                            let old = s
                                .engine
                                .delete_item(key_info, &key, true, None, maps, Some(cap))
                                .await
                                .expect("a delete");
                            if old.is_some() {
                                expected.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        8 | 9 => {
                            let (x, y) = (item(a, "s", &tag), item(b, "s", &format!("{tag}b")));
                            let put = |item| TransactWriteOp::Put {
                                key_info,
                                item,
                                condition: None,
                                maps,
                                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                                stream: Some(cap.clone()),
                            };
                            let ops = [put(&x), put(&y)];
                            if s.engine.transact_write_items(&ops, None).await.is_ok() {
                                expected.fetch_add(2, Ordering::SeqCst);
                            }
                        }
                        _ => {
                            s.put(key_info, a, "s", &tag).await;
                            expected.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
        }))
        .await;
        done.store(true, Ordering::SeqCst);
    };
    let worker = async {
        while !done.load(Ordering::SeqCst) {
            s.engine
                .number_pending_stream_records_for_tests()
                .await
                .expect("a worker pass");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let consumer = || async {
        let mut cursors: Vec<Option<String>> = vec![None; shards.len()];
        let mut read: Vec<Vec<StreamRecord>> = vec![Vec::new(); shards.len()];
        loop {
            let finished = done.load(Ordering::SeqCst);
            for ((shard, cursor), got) in shards.iter().zip(cursors.iter_mut()).zip(read.iter_mut())
            {
                loop {
                    let (page, last) = s.read(&arn, shard, cursor.as_deref()).await;
                    if page.is_empty() {
                        break;
                    }
                    *cursor = last;
                    got.extend(page);
                }
            }
            if finished {
                return read;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    let ((), (), first, second) = tokio::time::timeout(Duration::from_secs(120), async {
        tokio::join!(writers, worker, consumer(), consumer())
    })
    .await
    .expect("the writers, the worker and the consumers finished");

    let expected = expected.load(Ordering::SeqCst);
    for (name, read) in [("first", &first), ("second", &second)] {
        let all: Vec<&StreamRecord> = read.iter().flatten().collect();
        assert_eq!(all.len(), expected, "the {name} consumer's record count");
        let tags: Vec<String> = all
            .iter()
            .filter_map(|r| match r.dynamodb.new_image.as_ref()?.get("v") {
                Some(AttributeValue::S(v)) => Some(v.clone()),
                _ => None,
            })
            .collect();
        let distinct: BTreeSet<&String> = tags.iter().collect();
        assert_eq!(
            distinct.len(),
            tags.len(),
            "the {name} consumer read a write twice"
        );
        for shard in read {
            for pair in shard.windows(2) {
                assert!(
                    pair[0].dynamodb.sequence_number < pair[1].dynamodb.sequence_number,
                    "the {name} consumer's numbers go back on a shard"
                );
            }
        }
        let mut last_image: BTreeMap<String, Option<Item>> = BTreeMap::new();
        for r in &all {
            let pk = string_key(r, "pk");
            let before = last_image.get(&pk).cloned().flatten();
            assert_eq!(
                r.dynamodb.old_image, before,
                "the {name} consumer: item {pk}'s record {} does not follow its record before",
                r.dynamodb.sequence_number
            );
            last_image.insert(pk, r.dynamodb.new_image.clone());
        }
    }
    let numbers = |read: &Vec<Vec<StreamRecord>>| -> Vec<Vec<String>> {
        read.iter()
            .map(|shard| {
                shard
                    .iter()
                    .map(|r| r.dynamodb.sequence_number.clone())
                    .collect()
            })
            .collect()
    };
    assert_eq!(
        numbers(&first),
        numbers(&second),
        "both consumers read the same records"
    );

    s.cleanup().await;
}
