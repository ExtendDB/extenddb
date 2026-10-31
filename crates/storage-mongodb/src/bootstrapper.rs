// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bootstrapper implementation for `MongoDB`.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bson::{Document, doc};

use extenddb_storage::bootstrapper::{AdminBootstrapResult, Bootstrapper};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};
use extenddb_storage::migrations::HeldLock;

use crate::migrations::{Lease, REQUEST_TIMEOUT};

/// `MongoDB` bootstrapper for init/destroy/migrate operations.
pub struct MongoBootstrapper {
    client: mongodb::Client,
    connection_string: String,
    /// The lease of the migration lock `init` holds while it writes the
    /// bootstrap rows, set by [`Bootstrapper::hold`]; every row's requests
    /// check it first.
    lease: OnceLock<Arc<Lease>>,
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

        // The runner's client: the shared guard's checks (primary reads, a
        // write concern no weaker than majority) and its 10 s timeouts to
        // connect and to select a server, so `init` and `destroy` give up on
        // an unreachable database as `migrate` and `verify` do.
        let client = crate::migrations::connect(&connection_string)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self {
            client,
            connection_string,
            lease: OnceLock::new(),
        })
    }

    fn catalog_db(&self) -> mongodb::Database {
        self.client.database("extenddb_catalog")
    }

    /// Send one of `init`'s bootstrap requests as the runner sends a
    /// migration request: the lock's lease is checked first and nothing is
    /// sent once it is past its margin, the answer is waited for at most
    /// [`REQUEST_TIMEOUT`], and a completed request counts as progress.
    async fn request<T>(
        &self,
        what: &str,
        request: impl std::future::IntoFuture<Output = mongodb::error::Result<T>>,
    ) -> OpResult<T> {
        if let Some(lease) = self.lease.get() {
            lease
                .check()
                .map_err(|e| OpError::Internal(format!("{what}: {e}")))?;
        }
        let answer = tokio::time::timeout(REQUEST_TIMEOUT, request.into_future()).await;
        if let Some(lease) = self.lease.get() {
            lease.progressed();
        }
        match answer {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(OpError::Internal(format!("{what}: {e}"))),
            Err(_) => Err(OpError::Internal(format!(
                "{what}: no answer within {} s",
                REQUEST_TIMEOUT.as_secs()
            ))),
        }
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

    /// Nothing: `init` takes the migration lock before its first write, and
    /// the lock's claim creates the catalog database. A second `init` is
    /// refused under the lock, because the catalog is initialized.
    async fn create_catalog_db(&self) -> OpResult<()> {
        Ok(())
    }

    /// Nothing: migration 1's data body creates the data database's
    /// collections under the lock.
    async fn create_data_db(&self) -> OpResult<()> {
        Ok(())
    }

    /// Hold the lease of the lock `init` took, so each bootstrap row's
    /// requests check it; another backend's lock holds nothing.
    fn hold(&self, lock: &HeldLock) {
        if let Some(lease) = crate::migrations::lease_of(lock) {
            let _ = self.lease.set(lease);
        }
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        let db = self.catalog_db();
        let settings = db.collection::<Document>("settings");

        let data_db_name = "extenddb_data".to_string();
        self.request(
            "record_data_connection",
            settings
                .update_one(
                    doc! { "_id": "data_database_name" },
                    doc! { "$set": { "value": &data_db_name } },
                )
                .upsert(true),
        )
        .await?;

        let redacted = redact_connection_string(&self.connection_string);
        self.request(
            "record data_connection_string",
            settings
                .update_one(
                    doc! { "_id": "data_connection_string" },
                    doc! { "$set": { "value": &redacted } },
                )
                .upsert(true),
        )
        .await?;

        Ok(())
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        use base64::Engine;
        use rand::RngCore;

        let db = self.catalog_db();
        let settings = db.collection::<Document>("settings");

        // Check if already exists
        let existing = self
            .request(
                "check encryption key",
                settings.find_one(doc! { "_id": "encryption_key" }),
            )
            .await?;

        if existing.is_some() {
            return Ok(());
        }

        // Generate a 256-bit encryption key
        let mut key_bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut key_bytes);
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(key_bytes);

        // A duplicate `_id` (code 11000) means another process wrote the key
        // first, which keeps one key; every other error stops `init`.
        self.request("write the encryption key", async {
            match settings
                .insert_one(doc! {
                    "_id": "encryption_key",
                    "value": &key_b64,
                })
                .await
            {
                Ok(_) => Ok(()),
                Err(e) if crate::migrations::is_duplicate_key(&e) => Ok(()),
                Err(e) => Err(e),
            }
        })
        .await
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let db = self.catalog_db();
        let accounts = db.collection::<Document>("accounts");

        // Check if any account exists
        let count = self
            .request("count accounts", accounts.count_documents(doc! {}))
            .await?;

        if count > 0 {
            return Ok(());
        }

        let account_id = uuid::Uuid::new_v4().to_string();
        self.request(
            "create default account",
            accounts.insert_one(doc! {
                "_id": &account_id,
                "account_name": "default",
                "created_at": bson::DateTime::now(),
            }),
        )
        .await?;

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
        let existing = self
            .request(
                "check admin",
                admin_users.find_one(doc! { "_id": &username }),
            )
            .await?;

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

        self.request(
            "create admin",
            admin_users.insert_one(doc! {
                "_id": &username,
                "password_hash": &password_hash,
                "created_at": bson::DateTime::now(),
            }),
        )
        .await?;

        Ok(AdminBootstrapResult {
            username,
            generated_password: if from_env { None } else { Some(password) },
            already_existed: false,
            from_env,
        })
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

    fn catalog_database_name(&self) -> String {
        "extenddb_catalog".to_string()
    }

    fn endpoint_info(&self) -> String {
        self.connection_string.clone()
    }

    fn catalog_connection_url(&self) -> String {
        format!("{}/extenddb_catalog", self.connection_string)
    }

    /// The connection string the config file names, which the server uses
    /// too, and the redacted one `record_data_connection` records, so
    /// taking the lock knows it runs for `init` and may create the catalog.
    fn init_target(&self) -> extenddb_storage::migrations::Target {
        extenddb_storage::migrations::Target {
            data_connection: Some(redact_connection_string(&self.connection_string)),
            ..extenddb_storage::migrations::Target::at(self.connection_string.clone())
        }
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

/// Replace `user:password` userinfo with `user:<redacted>` in a MongoDB URI.
///
/// Returns the original string unchanged if no `@` separator is present or if
/// the userinfo section contains no `:` (username-only, no password).
fn redact_connection_string(uri: &str) -> String {
    let Some(scheme_end) = uri.find("://") else {
        return uri.to_string();
    };
    let after_scheme = scheme_end + 3;
    let Some(at_offset) = uri[after_scheme..].find('@') else {
        return uri.to_string();
    };
    let at_idx = after_scheme + at_offset;
    let userinfo = &uri[after_scheme..at_idx];
    let Some(colon_offset) = userinfo.find(':') else {
        return uri.to_string();
    };
    let user = &userinfo[..colon_offset];
    format!(
        "{}{}:<redacted>{}",
        &uri[..after_scheme],
        user,
        &uri[at_idx..]
    )
}

#[cfg(test)]
mod tests {
    use super::redact_connection_string;

    /// `init` and `destroy` reach the database through this client. Against
    /// a port nothing listens on, a request fails after the runner's 10 s
    /// server selection timeout, not the driver's 30 s default; the outer
    /// timeout turns a hang into a failure instead of a blocked test run.
    #[tokio::test]
    async fn a_request_to_an_unreachable_database_fails_within_the_runner_timeout() {
        use extenddb_storage::bootstrapper::Bootstrapper;
        use std::time::{Duration, Instant};

        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("a free port")
            .port();
        let config = std::env::temp_dir().join(format!(
            "extenddb-unreachable-{}-{port}.toml",
            std::process::id()
        ));
        std::fs::write(
            &config,
            format!(
                "[storage]\nbackend = \"mongodb\"\n\n[storage.mongodb]\nconnection_string = \
                 \"mongodb://127.0.0.1:{port}/?directConnection=true\"\n"
            ),
        )
        .expect("write the config");
        let bootstrapper =
            super::MongoBootstrapper::from_config(config.to_str().expect("a UTF-8 path"), &[])
                .await
                .expect("the client is built without connecting");
        let started = Instant::now();
        let answer = tokio::time::timeout(Duration::from_secs(40), bootstrapper.get_data_db_name())
            .await
            .expect("the request ended instead of waiting");
        let took = started.elapsed();
        let _ = std::fs::remove_file(&config);
        assert!(answer.is_err(), "{answer:?}");
        assert!(
            took < Duration::from_secs(15),
            "the request failed {took:?} after it was sent"
        );
    }

    /// `init` writes the bootstrap rows under the migration lock, and each
    /// row's requests check the lock's lease first, as the runner does before
    /// a migration request. Past the lease's margin nothing is sent: the
    /// write to a port nothing listens on fails at once with the lease's
    /// reason, not after the 10 s server selection timeout.
    #[tokio::test]
    async fn a_bootstrap_row_is_refused_once_the_held_lease_lapses() {
        use crate::migrations::{Lease, Timing};
        use extenddb_storage::bootstrapper::Bootstrapper;
        use std::time::{Duration, Instant};

        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("a free port")
            .port();
        let config = std::env::temp_dir().join(format!(
            "extenddb-lapsed-{}-{port}.toml",
            std::process::id()
        ));
        std::fs::write(
            &config,
            format!(
                "[storage]\nbackend = \"mongodb\"\n\n[storage.mongodb]\nconnection_string = \
                 \"mongodb://127.0.0.1:{port}/?directConnection=true\"\n"
            ),
        )
        .expect("write the config");
        let bootstrapper =
            super::MongoBootstrapper::from_config(config.to_str().expect("a UTF-8 path"), &[])
                .await
                .expect("the client is built without connecting");
        let _ = std::fs::remove_file(&config);
        // A lease last renewed a full lease ago, with no renewal task.
        let renewed = Instant::now()
            .checked_sub(Timing::PRODUCTION.lease)
            .expect("the clock has run for a lease");
        bootstrapper
            .lease
            .set(Lease::new(renewed, Timing::PRODUCTION))
            .ok()
            .expect("no lease was held yet");

        for (what, write) in [
            ("encryption key", bootstrapper.bootstrap_encryption_key()),
            ("default account", bootstrapper.bootstrap_default_account()),
            ("data connection", bootstrapper.record_data_connection()),
        ] {
            let started = Instant::now();
            let error = tokio::time::timeout(Duration::from_secs(5), write)
                .await
                .unwrap_or_else(|_| panic!("the {what} write waited for an answer"))
                .expect_err("refused");
            assert!(
                format!("{error:?}").contains("the migration lock was last renewed"),
                "{what}: {error:?}"
            );
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "the {what} write was sent"
            );
        }
        let admin = tokio::time::timeout(
            Duration::from_secs(5),
            bootstrapper.bootstrap_admin_user(None, None),
        )
        .await
        .expect("the admin write waited for an answer");
        match admin {
            Err(error) => assert!(
                format!("{error:?}").contains("the migration lock was last renewed"),
                "admin: {error:?}"
            ),
            Ok(result) => panic!("the admin row was written: {}", result.username),
        }
    }

    #[test]
    fn redact_userinfo_with_password() {
        assert_eq!(
            redact_connection_string("mongodb://alice:secret@host:27017/?replicaSet=rs0"),
            "mongodb://alice:<redacted>@host:27017/?replicaSet=rs0"
        );
    }

    #[test]
    fn redact_srv_scheme() {
        assert_eq!(
            redact_connection_string("mongodb+srv://alice:p%40ss@cluster.example.com/?tls=true"),
            "mongodb+srv://alice:<redacted>@cluster.example.com/?tls=true"
        );
    }

    #[test]
    fn no_userinfo_untouched() {
        assert_eq!(
            redact_connection_string("mongodb://localhost:27017/?replicaSet=rs0"),
            "mongodb://localhost:27017/?replicaSet=rs0"
        );
    }

    #[test]
    fn user_only_untouched() {
        assert_eq!(
            redact_connection_string("mongodb://alice@host:27017/"),
            "mongodb://alice@host:27017/"
        );
    }

    #[test]
    fn at_in_query_is_not_userinfo() {
        assert_eq!(
            redact_connection_string("mongodb://host:27017/?authSource=admin"),
            "mongodb://host:27017/?authSource=admin"
        );
    }

    #[test]
    fn no_scheme_untouched() {
        assert_eq!(redact_connection_string("not a uri"), "not a uri");
    }
}
