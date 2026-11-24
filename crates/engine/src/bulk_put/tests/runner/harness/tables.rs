// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl Harness {
    pub(crate) async fn new() -> Self {
        Self::new_inner(TableShape::Plain).await
    }

    /// A harness whose table carries one global secondary index.
    pub(crate) async fn new_with_gsi() -> Self {
        Self::new_inner(TableShape::WithGsi).await
    }

    /// A harness whose table has a single hash key `pk` and one
    /// ALL-projection global secondary index on `gsk`, the shape the
    /// overwrite index-usage measurements used: an item
    /// `{pk, gsk, v}` with a 1,500-byte `v` is 1,508 bytes.
    pub(crate) async fn new_with_gsi_hash_key() -> Self {
        Self::new_inner(TableShape::WithGsiHashKey).await
    }

    /// Like `new_with_gsi_hash_key` but the GSI projects KEYS_ONLY, so a
    /// change to a non-projected attribute leaves the projected entry
    /// unchanged under the same index key.
    pub(crate) async fn new_with_gsi_keys_only_hash_key() -> Self {
        Self::new_inner(TableShape::WithGsiKeysOnlyHashKey).await
    }

    /// A harness whose table streams `NEW_AND_OLD_IMAGES`.
    pub(crate) async fn new_with_stream() -> Self {
        Self::new_inner(TableShape::WithStream).await
    }

    /// A harness whose table carries one vector index over `emb`.
    pub(crate) async fn new_with_vector() -> Self {
        Self::new_inner(TableShape::WithVector).await
    }

    async fn new_inner(shape: TableShape) -> Self {
        // A database file with a pool of several connections, as a server
        // runs. An in-memory database pins the pool to one connection, and
        // a test that stops polling a run, or drops one, while a statement
        // of that run holds the connection leaves the next claim waiting
        // for it until the pool's acquire timeout.
        let db_dir = Root::new();
        let config = extenddb_storage_sqlite::SqliteConfig {
            path: db_dir
                .0
                .join("runner.sqlite")
                .to_string_lossy()
                .into_owned(),
            pool_size: 4,
        };
        let mut options = ServerComponentsOptions::default();
        options.bootstrap_if_uninitialized = true;
        let components =
            (extenddb_storage_sqlite::backend().server_components)(&config, REGION, options)
                .await
                .expect("server components");
        // Tables must reach ACTIVE synchronously: no control-plane worker
        // runs in these tests to mature a CREATING row.
        components
            .catalog_store
            .set_setting("control_plane_delay_seconds", "0")
            .await
            .expect("set control plane delay");
        components
            .catalog_store
            .create_account(ACCOUNT, "runner-tests")
            .await
            .expect("create account");
        let catalog = Arc::clone(&components.catalog_store);
        let storage = components.engine;
        let table_id = match shape {
            TableShape::Plain => Self::create_table(&storage, TABLE).await,
            TableShape::WithGsi => Self::create_table_with_gsi(&storage, TABLE).await,
            TableShape::WithGsiHashKey => {
                Self::create_table_with_gsi_hash_key(&storage, TABLE).await
            }
            TableShape::WithGsiKeysOnlyHashKey => {
                Self::create_table_with_gsi_keys_only_hash_key(&storage, TABLE).await
            }
            TableShape::WithStream => Self::create_table_with_stream(&storage, TABLE).await,
            TableShape::WithVector => Self::create_table_with_vector(&storage, TABLE).await,
        };
        Self {
            storage,
            store: store_with_buckets(),
            table_id,
            table_arn: format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{TABLE}"),
            catalog,
            pg: None,
            _db_dir: Some(db_dir),
        }
    }

    pub(crate) async fn create_table(storage: &Arc<dyn StorageEngine>, name: &str) -> String {
        let input: CreateTableInput = serde_json::from_value(json!({
            "TableName": name,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("create table input");
        let description = storage
            .create_table(ACCOUNT, input)
            .await
            .expect("create table");
        description.table_id
    }

    /// A table with one global secondary index keyed on `g`, so a write
    /// charges the index by the old-versus-new delta. Matures to ACTIVE
    /// synchronously like `create_table`, since no control-plane worker
    /// runs in these tests.
    pub(crate) async fn create_table_with_gsi(
        storage: &Arc<dyn StorageEngine>,
        name: &str,
    ) -> String {
        let input: CreateTableInput = serde_json::from_value(json!({
            "TableName": name,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("create table input");
        let description = storage
            .create_table(ACCOUNT, input)
            .await
            .expect("create table with gsi");
        description.table_id
    }

    /// A single-hash-key table (`pk`) with one ALL-projection GSI on
    /// `gsk`, the shape the overwrite index-usage measurements used. No sort key,
    /// so an item `{pk, gsk, v}` carries only the two keys and the payload:
    /// `{pk: "a", gsk: "g", v: <1,500 bytes>}` is 1,508 bytes and with a
    /// 2,000-byte `v` is 2,008.
    pub(crate) async fn create_table_with_gsi_hash_key(
        storage: &Arc<dyn StorageEngine>,
        name: &str,
    ) -> String {
        let input: CreateTableInput = serde_json::from_value(json!({
            "TableName": name,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsk", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gsk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("create table input");
        let description = storage
            .create_table(ACCOUNT, input)
            .await
            .expect("create table with gsi hash key");
        description.table_id
    }

    /// Like `create_table_with_gsi_hash_key` but the GSI projects
    /// KEYS_ONLY, so only `pk` and `gsk` are in the index entry and a
    /// change to `v` leaves the projection unchanged.
    pub(crate) async fn create_table_with_gsi_keys_only_hash_key(
        storage: &Arc<dyn StorageEngine>,
        name: &str,
    ) -> String {
        let input: CreateTableInput = serde_json::from_value(json!({
            "TableName": name,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsk", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gsk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("create table input");
        let description = storage
            .create_table(ACCOUNT, input)
            .await
            .expect("create table with keys-only gsi hash key");
        description.table_id
    }

    /// A table with one vector index over `emb` (four dimensions), so a
    /// write that carries the attribute charges the index in
    /// `BulkPutVectorBytes`. The inline create path leaves the vector index
    /// ACTIVE, and the table matures to ACTIVE synchronously like
    /// `create_table`, since no control-plane worker runs in these tests.
    pub(crate) async fn create_table_with_vector(
        storage: &Arc<dyn StorageEngine>,
        name: &str,
    ) -> String {
        let input: CreateTableInput = serde_json::from_value(json!({
            "TableName": name,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "VectorIndexes": [{
                "IndexName": "vidx",
                "Dimensions": 4,
                "DistanceFunction": "COSINE",
                "VectorAttribute": {"AttributeName": "emb"},
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("create table input");
        let description = storage
            .create_table(ACCOUNT, input)
            .await
            .expect("create table with vector index");
        description.table_id
    }

    /// A table streaming `NEW_AND_OLD_IMAGES`, so a bulk write's stream
    /// capture is observable. Matures to ACTIVE synchronously like
    /// `create_table`, since no control-plane worker runs in these tests.
    pub(crate) async fn create_table_with_stream(
        storage: &Arc<dyn StorageEngine>,
        name: &str,
    ) -> String {
        let input: CreateTableInput = serde_json::from_value(json!({
            "TableName": name,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            },
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("create table input");
        let description = storage
            .create_table(ACCOUNT, input)
            .await
            .expect("create table with stream");
        description.table_id
    }
}
