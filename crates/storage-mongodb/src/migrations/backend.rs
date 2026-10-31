// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The runner's MongoDB parts.
//!
//! Per body, in its store: the lease check, the body, the post-check the
//! first time migration 1 is applied to the store, then the ledger row and
//! (in the catalog) the version row, written in one transaction with write
//! concern majority after the body completes. Every request goes through
//! [`MongoStores::request`], which checks the migration lock's lease first
//! and waits at most [`REQUEST_TIMEOUT`] for the answer, so a holder past
//! its deadline stops before its next request and records nothing.

use std::sync::Arc;

use async_trait::async_trait;
use bson::{Bson, Document, doc};
use extenddb_storage::management_store::ManagementStore;
use extenddb_storage::migrations::{
    CatalogTraces, Done, HasManagementStore, LedgerColumns, LedgerRow, LedgerWrites, Migration,
    MigrationBackend, MigrationError, MigrationId, Released, Step, StoreKind, StoreRef,
    in_transaction, record_rows,
};
use futures::TryStreamExt;

use super::guarded::LeasedManagementStore;
use super::locks::{Lease, MigrationLock};
use super::{
    LEDGER, MongoTarget, REQUEST_TIMEOUT, catalog_handle, connect, driver_error, postcheck,
};
use crate::MongoCatalogStore;

/// NamespaceExists: `create` on a collection that exists with other options.
const NAMESPACE_EXISTS: i32 = 48;

/// The handle code bodies receive: both databases, and the management store
/// migration 2 runs through.
pub struct MongoStores {
    pub(crate) catalog: mongodb::Database,
    pub(crate) data: mongodb::Database,
    lease: Option<Arc<Lease>>,
    management: LeasedManagementStore,
    /// Requests left before the runner stops as a killed migrator does.
    #[cfg(feature = "test-util")]
    stop_after: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

impl MongoStores {
    /// Send one request: check the lease, then wait at most
    /// [`REQUEST_TIMEOUT`] for the answer.
    ///
    /// # Errors
    ///
    /// The lease is past its deadline (nothing is sent), the request failed
    /// or it had no answer in time.
    pub(crate) async fn request<T>(
        &self,
        what: impl std::fmt::Display,
        request: impl std::future::Future<Output = mongodb::error::Result<T>>,
    ) -> Result<T, MigrationError> {
        self.check()?;
        #[cfg(feature = "test-util")]
        if super::guarded::stopped(self.stop_after.as_deref()) {
            return Err(MigrationError::Store(format!(
                "{what}: stopped by the test"
            )));
        }
        let answer = tokio::time::timeout(REQUEST_TIMEOUT, request).await;
        if let Some(lease) = &self.lease {
            lease.progressed();
        }
        match answer {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(driver_error(what, &e)),
            Err(_) => Err(MigrationError::Store(format!(
                "{what}: no answer within {} s",
                REQUEST_TIMEOUT.as_secs()
            ))),
        }
    }

    /// Whether this process may still act under the migration lock; always
    /// for a read that holds none.
    ///
    /// # Errors
    ///
    /// The lock was lost or its lease is past the margin.
    pub(crate) fn check(&self) -> Result<(), MigrationError> {
        match &self.lease {
            Some(lease) => lease.check(),
            None => Ok(()),
        }
    }

    fn db(&self, kind: StoreKind) -> &mongodb::Database {
        match kind {
            StoreKind::Catalog => &self.catalog,
            StoreKind::Data | StoreKind::Account => &self.data,
        }
    }
}

impl HasManagementStore for MongoStores {
    fn management_store(&self) -> &dyn ManagementStore {
        &self.management
    }
}

/// The runner's view of one MongoDB deployment.
pub struct MongoMigrations {
    client: mongodb::Client,
    stores: MongoStores,
}

impl MongoMigrations {
    /// Open the deployment `target` names. With `lock`, every request checks
    /// its lease; without one, the handle is for reads (the status).
    ///
    /// # Errors
    ///
    /// The client cannot be built from the connection string.
    pub async fn open(
        target: &MongoTarget,
        lock: Option<&MigrationLock>,
    ) -> Result<Self, MigrationError> {
        let client = connect(&target.connection_string).await?;
        let catalog = catalog_handle(&client, &target.catalog);
        let data = catalog_handle(&client, &target.data);
        let lease = lock.map(MigrationLock::lease);
        let management = LeasedManagementStore::new(
            MongoCatalogStore::on_database(client.clone(), catalog.clone()),
            lease.clone(),
        );
        Ok(Self {
            client,
            stores: MongoStores {
                catalog,
                data,
                lease,
                management,
                #[cfg(feature = "test-util")]
                stop_after: None,
            },
        })
    }

    /// Stop before the request after the first `requests`, as a migrator
    /// killed at that point stops: nothing after it is sent.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn stop_after(mut self, requests: usize) -> Self {
        let left = Arc::new(std::sync::atomic::AtomicUsize::new(requests));
        self.stores.management.stop_after(Arc::clone(&left));
        self.stores.stop_after = Some(left);
        self
    }

    /// Close the client's connections.
    pub async fn close(self) {
        self.client.shutdown().await;
    }

    fn row_error(store: &str, id: &Bson, what: &str) -> MigrationError {
        MigrationError::Store(format!(
            "{LEDGER} document {id} in {store} has no {what}, which this binary cannot read"
        ))
    }

    fn columns(store: &str, d: &Document) -> Result<LedgerColumns<i64>, MigrationError> {
        let id = d.get("_id").cloned().unwrap_or(Bson::Null);
        let number = match &id {
            Bson::Int32(n) => i64::from(*n),
            Bson::Int64(n) => *n,
            _ => return Err(Self::row_error(store, &id, "integer _id")),
        };
        let text = |field: &str| -> Result<String, MigrationError> {
            d.get_str(field)
                .map(str::to_owned)
                .map_err(|_| Self::row_error(store, &id, field))
        };
        let applied_at = match d.get("applied_at") {
            Some(Bson::DateTime(t)) => t.try_to_rfc3339_string().unwrap_or_else(|_| t.to_string()),
            _ => return Err(Self::row_error(store, &id, "applied_at date")),
        };
        Ok((
            number,
            text("name")?,
            text("kind")?,
            text("checksum")?,
            text("outcome")?,
            text("notes")?,
            applied_at,
            text("applied_by")?,
        ))
    }
}

/// The writes that record a body in one database: the ledger row, and in
/// the catalog the version row, in one transaction, so a run stopped between
/// them cannot leave a ledger row for migration 1 beside a released
/// release's dotted version, which reads as written by an older release.
/// The ledger collection is created before the transaction starts.
struct Writes<'a> {
    stores: &'a MongoStores,
    db: &'a mongodb::Database,
    session: tokio::sync::Mutex<mongodb::ClientSession>,
}

impl Writes<'_> {
    async fn begin(&self) -> Result<(), MigrationError> {
        let mut session = self.session.lock().await;
        self.stores
            .request("start the transaction that records the migration", async {
                session
                    .start_transaction()
                    .read_concern(mongodb::options::ReadConcern::local())
                    .write_concern(mongodb::options::WriteConcern::majority())
                    .await
            })
            .await
    }

    async fn commit(&self) -> Result<(), MigrationError> {
        let mut session = self.session.lock().await;
        self.stores
            .request("commit the ledger and version rows", async {
                session.commit_transaction().await
            })
            .await
    }

    async fn abort(&self) {
        let mut session = self.session.lock().await;
        let _ = tokio::time::timeout(REQUEST_TIMEOUT, session.abort_transaction()).await;
    }
}

/// Create a database's ledger collection, accepting one that exists.
async fn create_ledger(stores: &MongoStores, db: &mongodb::Database) -> Result<(), MigrationError> {
    stores
        .request(format!("create {}.{LEDGER}", db.name()), async {
            match db.create_collection(LEDGER).await {
                Err(e) if command_code(&e) == Some(NAMESPACE_EXISTS) => Ok(()),
                other => other,
            }
        })
        .await
}

#[async_trait]
impl LedgerWrites for Writes<'_> {
    type Error = MigrationError;

    /// Done before the transaction starts: [`MongoMigrations::apply`]
    /// creates the collection first.
    async fn create_ledger(&self) -> Result<(), MigrationError> {
        Ok(())
    }

    async fn insert_row(&self, row: &LedgerRow) -> Result<(), MigrationError> {
        // `$literal` keeps a value that starts with `$` from being read as
        // a field path; `applied_at` is the server's clock.
        let literal = |value: &str| doc! { "$literal": value };
        let ledger = self.db.collection::<Document>(LEDGER);
        let update = vec![doc! { "$set": {
            "name": literal(&row.name),
            "kind": literal(row.kind.as_str()),
            "checksum": literal(&row.checksum),
            "outcome": literal(row.outcome.as_str()),
            "notes": literal(&row.notes),
            "applied_at": "$$NOW",
            "applied_by": literal(&row.applied_by),
        } }];
        let mut session = self.session.lock().await;
        self.stores
            .request(
                format!("record migration {} in {}.{LEDGER}", row.id, self.db.name()),
                async {
                    ledger
                        .update_one(doc! { "_id": i64::from(row.id) }, update)
                        .upsert(true)
                        .session(&mut *session)
                        .await
                        .map(|_| ())
                },
            )
            .await
    }

    async fn write_version(&self, version: u32) -> Result<(), MigrationError> {
        let mut session = self.session.lock().await;
        write_version(self.stores, version, Some(&mut session)).await
    }
}

async fn write_version(
    stores: &MongoStores,
    version: u32,
    session: Option<&mut mongodb::ClientSession>,
) -> Result<(), MigrationError> {
    let settings = stores.catalog.collection::<Document>("settings");
    stores
        .request("write the catalog_version settings row", async {
            let update = settings
                .update_one(
                    doc! { "_id": "catalog_version" },
                    doc! { "$set": { "value": version.to_string() } },
                )
                .upsert(true);
            match session {
                Some(session) => update.session(session).await,
                None => update.await,
            }
            .map(|_| ())
        })
        .await
}

fn command_code(error: &mongodb::error::Error) -> Option<i32> {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Command(command) => Some(command.code),
        _ => None,
    }
}

#[async_trait]
impl MigrationBackend for MongoMigrations {
    type Stores = MongoStores;

    fn backend_name(&self) -> &'static str {
        "mongodb"
    }

    fn migration(&self, id: MigrationId) -> Migration<MongoStores> {
        super::migration(id)
    }

    fn released(&self) -> &'static [Released] {
        super::RELEASED
    }

    fn stores(&self) -> &MongoStores {
        &self.stores
    }

    async fn store_names(&self, kind: StoreKind) -> Result<Vec<String>, MigrationError> {
        Ok(match kind {
            StoreKind::Catalog => vec![self.stores.catalog.name().to_owned()],
            StoreKind::Data => vec![self.stores.data.name().to_owned()],
            StoreKind::Account => Vec::new(),
        })
    }

    /// A ledger collection that does not exist reads as no documents; any
    /// other failure is an error.
    async fn read_ledger(&self, store: &StoreRef) -> Result<Vec<LedgerRow>, MigrationError> {
        let ledger = self.stores.db(store.kind).collection::<Document>(LEDGER);
        let docs = self
            .stores
            .request(format!("read {}.{LEDGER}", store.name), async {
                let cursor = ledger.find(doc! {}).sort(doc! { "_id": 1 }).await?;
                cursor.try_collect::<Vec<Document>>().await
            })
            .await?;
        docs.iter()
            .map(|d| LedgerRow::from_columns(Some(&store.name), Self::columns(&store.name, d)?))
            .collect()
    }

    async fn read_traces(&self) -> Result<CatalogTraces, MigrationError> {
        let settings = self.stores.catalog.collection::<Document>("settings");
        let docs = self
            .stores
            .request("read the catalog's settings", async {
                let cursor = settings
                    .find(doc! { "_id": { "$in": ["catalog_version", "resource_tag_migration"] } })
                    .await?;
                cursor.try_collect::<Vec<Document>>().await
            })
            .await?;
        let rows: Vec<(String, String)> = docs
            .iter()
            .filter_map(|d| {
                let key = d.get_str("_id").ok()?.to_owned();
                let value = match d.get("value") {
                    Some(Bson::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                Some((key, value))
            })
            .collect();
        Ok(CatalogTraces::from_settings(&rows))
    }

    /// The claim's upsert creates `settings` in an empty catalog, so a
    /// catalog holding that collection alone holds nothing to detect.
    async fn catalog_is_empty(&self) -> Result<bool, MigrationError> {
        let db = &self.stores.catalog;
        let names = self
            .stores
            .request(format!("list the collections of {}", db.name()), async {
                db.list_collection_names().await
            })
            .await?;
        Ok(names.iter().all(|n| n == "settings"))
    }

    async fn before_first_write(
        &self,
        _from: Option<u32>,
        _to: u32,
    ) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn apply(
        &self,
        store: &StoreRef,
        id: MigrationId,
        step: Step<'_, MongoStores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, MigrationError> {
        self.stores.check()?;
        let done = match step {
            Step::File { name, .. } => {
                return Err(MigrationError::Store(format!(
                    "migration {id} has a file body ({name}), and MongoDB runs code bodies only"
                )));
            }
            // A body stopped by the lease reports the lease, which says why.
            Step::Code(body) => body
                .run(&self.stores)
                .await
                .map_err(|e| self.stores.check().err().unwrap_or(e))?,
            Step::Record => Done::default(),
        };
        // The first application only: on a store at a later migration the
        // objects migration 1 creates may carry what later migrations added.
        if id == MigrationId::Baseline && row.is_some() {
            let expected = match store.kind {
                StoreKind::Catalog => super::CATALOG_EXPECTED,
                StoreKind::Data | StoreKind::Account => super::DATA_EXPECTED,
            };
            postcheck::check(
                &self.stores,
                self.stores.db(store.kind),
                expected,
                &store.name,
            )
            .await?;
        }
        let db = self.stores.db(store.kind);
        if row.is_some() {
            create_ledger(&self.stores, db).await?;
        }
        let session = self
            .stores
            .request("start a session", async {
                self.client.start_session().await
            })
            .await?;
        let writes = Writes {
            stores: &self.stores,
            db,
            session: tokio::sync::Mutex::new(session),
        };
        in_transaction(
            writes.begin(),
            record_rows(&writes, row, &done.notes, catalog_version),
            writes.commit(),
            writes.abort(),
        )
        .await?;
        Ok(done)
    }

    async fn write_catalog_version(&self, version: u32) -> Result<(), MigrationError> {
        write_version(&self.stores, version, None).await
    }

    async fn delete_ledger_row(&self, store: &StoreRef, id: u32) -> Result<(), MigrationError> {
        let ledger = self.stores.db(store.kind).collection::<Document>(LEDGER);
        self.stores
            .request(
                format!("forget migration {id} in {}.{LEDGER}", store.name),
                async {
                    ledger
                        .delete_one(doc! { "_id": i64::from(id) })
                        .await
                        .map(|_| ())
                },
            )
            .await
    }
}
