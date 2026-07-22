// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bootstrapper implementation for `MongoDB`.

use async_trait::async_trait;
use bson::{Document, doc};
use mongodb::IndexModel;
use mongodb::options::IndexOptions;

use extenddb_storage::bootstrapper::{AdminBootstrapResult, Bootstrapper};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};

/// `MongoDB` bootstrapper for init/destroy/migrate operations.
pub struct MongoBootstrapper {
    client: mongodb::Client,
    connection_string: String,
}

impl MongoBootstrapper {
    pub async fn from_config(
        config_path: &str,
        _cli_args: &[String],
    ) -> Result<Self, StorageError> {
        // Read the config file to get the connection string
        let config_content = std::fs::read_to_string(config_path).map_err(|e| {
            StorageError::Internal(format!("Cannot read config file '{config_path}': {e}"))
        })?;

        let config: toml::Value = config_content
            .parse()
            .map_err(|e| StorageError::Internal(format!("Cannot parse config: {e}")))?;

        let connection_string = config
            .get("storage")
            .and_then(|s| s.get("mongodb"))
            .and_then(|m| m.get("connection_string"))
            .and_then(|v| v.as_str())
            .unwrap_or("mongodb://localhost:27017")
            .to_string();

        let client = mongodb::Client::with_uri_str(&connection_string)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self {
            client,
            connection_string,
        })
    }

    fn catalog_db(&self) -> mongodb::Database {
        self.client.database("extenddb_catalog")
    }

    fn data_db(&self) -> mongodb::Database {
        self.client.database("extenddb_data")
    }
}

#[async_trait]
impl Bootstrapper for MongoBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        // MongoDB uses connection-level auth; no separate app user needed
        Ok(())
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        // Not applicable for MongoDB
        Ok(())
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        // MongoDB creates databases implicitly on first write.
        // We'll create a sentinel collection to materialize the database.
        let db = self.catalog_db();
        db.create_collection("schema_history")
            .await
            .map_err(|e| OpError::Internal(format!("Failed to create catalog db: {e}")))?;
        Ok(())
    }

    async fn create_data_db(&self) -> OpResult<()> {
        // MongoDB creates databases implicitly on first write.
        let db = self.data_db();
        db.create_collection("idempotency_tokens")
            .await
            .map_err(|e| OpError::Internal(format!("Failed to create data db: {e}")))?;

        let coll = db.collection::<Document>("idempotency_tokens");

        // DDB spec: `ClientRequestToken` dedups retries within a 10-minute
        // window. MongoDB's TTL monitor runs on a ~60s cadence, so an
        // `expireAfterSeconds = 600` index deletes rows anywhere from
        // 10:00 to ~11:00 minutes after `created_at` — retention drifts
        // above the spec. Shrink to 540s (9 min) so worst-case retention
        // is ≤10 min. The data-plane read path also filters on `created_at`
        // to enforce the boundary strictly, independent of TTL monitor
        // cadence (see `transact_write_items_impl`).
        let ttl_index = IndexModel::builder()
            .keys(doc! { "created_at": 1 })
            .options(
                IndexOptions::builder()
                    .expire_after(std::time::Duration::from_secs(540))
                    .build(),
            )
            .build();
        coll.create_index(ttl_index)
            .await
            .map_err(|e| OpError::Internal(format!("Failed to create TTL index: {e}")))?;

        // Unique compound index on (account_id, token). Without this,
        // two concurrent TransactWriteItems calls with the same token
        // both do a snapshot read that misses the other's uncommitted
        // insert, and both commit — the operation executes twice.
        // With the unique index in place, the second inserter fails
        // E11000 and the write path converts that into a retryable
        // error, giving the client the read-check path on retry.
        coll.create_index(
            IndexModel::builder()
                .keys(doc! { "account_id": 1, "token": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
        )
        .await
        .map_err(|e| OpError::Internal(format!("idempotency_tokens unique index: {e}")))?;

        // stream_shards: unique index on shard_id so a subsequent init/
        // recreate can never insert a duplicate shard document under the
        // same shard_id. Combined with `table_id`-derived shard_ids
        // (see stream_engine::build_shard_id) this rules out cross-tenant
        // shard collisions structurally.
        db.create_collection("stream_shards")
            .await
            .map_err(|e| OpError::Internal(format!("Failed to create stream_shards: {e}")))?;
        db.collection::<Document>("stream_shards")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "shard_id": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("stream_shards shard_id index: {e}")))?;

        // stream_records: TTL index enforcing DDB's 24-hour retention.
        // The `expireAfterSeconds` is the delta from the field value,
        // not a hard cutoff, so a MongoDB background thread will
        // delete records ~1 minute after created_at + 24h. The
        // TTL cleanup worker in `ttl_worker.rs` provides a defense-
        // in-depth deletion path but the primary enforcement is here.
        db.create_collection("stream_records")
            .await
            .map_err(|e| OpError::Internal(format!("Failed to create stream_records: {e}")))?;
        db.collection::<Document>("stream_records")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "created_at": 1 })
                    .options(
                        IndexOptions::builder()
                            .expire_after(std::time::Duration::from_secs(24 * 3600))
                            .build(),
                    )
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("stream_records TTL index: {e}")))?;

        Ok(())
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        let db = self.catalog_db();

        // Create all catalog collections
        let collections = [
            "accounts",
            "tables",
            "indexes",
            "tags",
            "settings",
            "admin_users",
            "iam_users",
            "access_keys",
            "iam_groups",
            "iam_roles",
            "iam_sessions",
            "iam_policies",
            "iam_permissions_boundaries",
            "metrics",
            "login_attempts",
            "backups",
            "continuous_backups",
        ];

        for coll_name in collections {
            // create_collection is idempotent in recent MongoDB versions
            let _ = db.create_collection(coll_name).await;
        }

        // Create indexes for catalog collections

        // accounts: unique index on account_name
        let accounts = db.collection::<Document>("accounts");
        accounts
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "account_name": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("accounts index: {e}")))?;

        // tables: unique index on table_id
        let tables = db.collection::<Document>("tables");
        tables
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "table_id": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("tables table_id index: {e}")))?;

        // iam_users: unique index on user_arn
        let iam_users = db.collection::<Document>("iam_users");
        iam_users
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "user_arn": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("iam_users user_arn index: {e}")))?;

        // access_keys: index on (account_id, user_name)
        let access_keys = db.collection::<Document>("access_keys");
        access_keys
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "account_id": 1, "user_name": 1 })
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("access_keys index: {e}")))?;

        // iam_groups: unique index on group_arn
        let iam_groups = db.collection::<Document>("iam_groups");
        iam_groups
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "group_arn": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("iam_groups index: {e}")))?;

        // iam_roles: unique index on role_arn
        let iam_roles = db.collection::<Document>("iam_roles");
        iam_roles
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "role_arn": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("iam_roles index: {e}")))?;

        // iam_sessions: unique index on access_key_id, TTL on expires_at
        let iam_sessions = db.collection::<Document>("iam_sessions");
        iam_sessions
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "access_key_id": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("iam_sessions access_key index: {e}")))?;
        iam_sessions
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "expires_at": 1 })
                    .options(
                        IndexOptions::builder()
                            .expire_after(std::time::Duration::from_secs(0))
                            .build(),
                    )
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("iam_sessions TTL index: {e}")))?;

        // metrics: index on bucket
        let metrics = db.collection::<Document>("metrics");
        metrics
            .create_index(IndexModel::builder().keys(doc! { "_id.bucket": 1 }).build())
            .await
            .map_err(|e| OpError::Internal(format!("metrics bucket index: {e}")))?;

        // login_attempts: compound index
        let login_attempts = db.collection::<Document>("login_attempts");
        login_attempts
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "principal": 1, "attempted_at": 1 })
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("login_attempts index: {e}")))?;

        // backups: index on (account_id, table_name)
        let backups = db.collection::<Document>("backups");
        backups
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "account_id": 1, "table_name": 1 })
                    .build(),
            )
            .await
            .map_err(|e| OpError::Internal(format!("backups index: {e}")))?;

        // Seed catalog_version in settings
        let settings = db.collection::<Document>("settings");
        let _ = settings
            .update_one(
                doc! { "_id": "catalog_version" },
                doc! { "$setOnInsert": { "value": "0.0.2" } },
            )
            .upsert(true)
            .await;

        // Record migration
        let schema_history = db.collection::<Document>("schema_history");
        let _ = schema_history
            .insert_one(doc! {
                "_id": "001_initial",
                "applied_at": bson::DateTime::now(),
            })
            .await; // Ignore E11000 (already applied)

        Ok(())
    }

    async fn run_data_migrations(&self) -> OpResult<()> {
        // Data database schema is minimal for MongoDB (just idempotency_tokens)
        // Table collections are created on-demand
        Ok(())
    }

    async fn pending_data_migrations(&self) -> OpResult<Vec<String>> {
        // MongoDB has no versioned data-database migrations — table
        // collections are created on-demand, and the idempotency_tokens
        // collection is created in create_data_db. Nothing to apply.
        Ok(Vec::new())
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        let db = self.catalog_db();
        let settings = db.collection::<Document>("settings");

        let data_db_name = "extenddb_data".to_string();
        settings
            .update_one(
                doc! { "_id": "data_database_name" },
                doc! { "$set": { "value": &data_db_name } },
            )
            .upsert(true)
            .await
            .map_err(|e| OpError::Internal(format!("record_data_connection: {e}")))?;

        settings
            .update_one(
                doc! { "_id": "data_connection_string" },
                doc! { "$set": { "value": &self.connection_string } },
            )
            .upsert(true)
            .await
            .map_err(|e| OpError::Internal(format!("record data_connection_string: {e}")))?;

        Ok(())
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        use base64::Engine;
        use rand::RngCore;

        let db = self.catalog_db();
        let settings = db.collection::<Document>("settings");

        // Check if already exists
        let existing = settings
            .find_one(doc! { "_id": "encryption_key" })
            .await
            .map_err(|e| OpError::Internal(format!("check encryption key: {e}")))?;

        if existing.is_some() {
            return Ok(());
        }

        // Generate a 256-bit encryption key
        let mut key_bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut key_bytes);
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(key_bytes);

        // Ignore E11000 (race: someone else created it first)
        let _ = settings
            .insert_one(doc! {
                "_id": "encryption_key",
                "value": &key_b64,
            })
            .await;

        Ok(())
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let db = self.catalog_db();
        let accounts = db.collection::<Document>("accounts");

        // Check if any account exists
        let count = accounts
            .count_documents(doc! {})
            .await
            .map_err(|e| OpError::Internal(format!("count accounts: {e}")))?;

        if count > 0 {
            return Ok(());
        }

        let account_id = uuid::Uuid::new_v4().to_string();
        accounts
            .insert_one(doc! {
                "_id": &account_id,
                "account_name": "default",
                "created_at": bson::DateTime::now(),
            })
            .await
            .map_err(|e| OpError::Internal(format!("create default account: {e}")))?;

        Ok(())
    }

    async fn bootstrap_admin_user(
        &self,
        env_user: Option<&str>,
        env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        let username = env_user.unwrap_or("admin").to_string();

        let db = self.catalog_db();
        let admin_users = db.collection::<Document>("admin_users");

        // Check if admin already exists
        let existing = admin_users
            .find_one(doc! { "_id": &username })
            .await
            .map_err(|e| OpError::Internal(format!("check admin: {e}")))?;

        if existing.is_some() {
            return Ok(AdminBootstrapResult {
                username,
                generated_password: None,
                already_existed: true,
                from_env: env_user.is_some(),
            });
        }

        // Generate or use provided password
        let (password, from_env) = if let Some(pw) = env_password {
            (pw.to_string(), true)
        } else {
            use rand::Rng;
            let pw: String = rand::rng()
                .sample_iter(&rand::distr::Alphanumeric)
                .take(24)
                .map(char::from)
                .collect();
            (pw, false)
        };

        let password_hash = bcrypt::hash(&password, bcrypt::DEFAULT_COST)
            .map_err(|e| OpError::Internal(format!("bcrypt hash: {e}")))?;

        admin_users
            .insert_one(doc! {
                "_id": &username,
                "password_hash": &password_hash,
                "created_at": bson::DateTime::now(),
            })
            .await
            .map_err(|e| OpError::Internal(format!("create admin: {e}")))?;

        Ok(AdminBootstrapResult {
            username,
            generated_password: if from_env { None } else { Some(password) },
            already_existed: false,
            from_env,
        })
    }

    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        let db = self.catalog_db();
        let collections = db
            .list_collection_names()
            .await
            .map_err(|e| OpError::Internal(format!("list collections: {e}")))?;
        Ok(collections.contains(&"settings".to_string()))
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        use futures::TryStreamExt;

        let db = self.catalog_db();
        let tables = db.collection::<Document>("tables");

        let cursor = tables
            .find(doc! {})
            .projection(doc! { "_id.table_name": 1 })
            .await
            .map_err(|e| OpError::Internal(format!("list tables: {e}")))?;

        let docs: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|e| OpError::Internal(format!("collect tables: {e}")))?;

        let names: Vec<String> = docs
            .iter()
            .filter_map(|d| {
                d.get_document("_id")
                    .ok()
                    .and_then(|id| id.get_str("table_name").ok())
                    .map(std::string::ToString::to_string)
            })
            .collect();

        Ok(names)
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        let db = self.catalog_db();
        let settings = db.collection::<Document>("settings");
        let doc = settings
            .find_one(doc! { "_id": "data_database_name" })
            .await
            .map_err(|e| OpError::Internal(format!("get data_db_name: {e}")))?;
        Ok(doc.and_then(|d| {
            d.get_str("value")
                .ok()
                .map(std::string::ToString::to_string)
        }))
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        self.data_db()
            .drop()
            .await
            .map_err(|e| OpError::Internal(format!("drop data db: {e}")))?;
        self.catalog_db()
            .drop()
            .await
            .map_err(|e| OpError::Internal(format!("drop catalog db: {e}")))?;
        Ok(())
    }

    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        let db = self.catalog_db();
        let settings = db.collection::<Document>("settings");
        let doc = settings
            .find_one(doc! { "_id": "catalog_version" })
            .await
            .map_err(|e| OpError::Internal(format!("read catalog_version: {e}")))?;
        Ok(doc.and_then(|d| {
            d.get_str("value")
                .ok()
                .map(std::string::ToString::to_string)
        }))
    }

    fn expected_catalog_version(&self) -> String {
        "0.0.2".to_string()
    }

    fn catalog_database_name(&self) -> String {
        "extenddb_catalog".to_string()
    }

    fn endpoint_info(&self) -> String {
        self.connection_string.clone()
    }

    fn catalog_connection_url(&self) -> String {
        format!("{}/extenddb_catalog", self.connection_string)
    }

    fn generate_backend_config_section(&self) -> String {
        format!(
            r#"[storage.mongodb]
connection_string = "{}"
# max_connections = 50           # Max concurrent connections for data operations (default 50)
# max_catalog_connections = 20   # Max concurrent connections for catalog/management operations (default 20)"#,
            self.connection_string
        )
    }
}
