// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog migrations for the Cloud Bigtable storage backend.
//!
//! Stores migration ledger rows inside `__extenddb_catalog__` under
//! `migration_ledger:<store>:<id:04>` and maintains `version:catalog` for the
//! shared [`extenddb_storage::migrations::Runner`].

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use extenddb_storage::management_store::{ManagementStore, SettingsStore};
use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, CatalogTraces, Done, HeldLock, Kind, LedgerRow, Listed,
    LockedStores, Migration, MigrationBackend, MigrationError, MigrationId, Released, RunKind,
    RunReport, Runner, Step, StoreBody, StoreKind, StoreRef, Target,
};
use serde_json::json;
use uuid::Uuid;

use crate::BigtableCatalogStore;
use crate::catalog::{CATALOG_TABLE, Catalog, keys};
use crate::config::BigtableStorageConfig;
use crate::data::admin::AdminClient;
use crate::data::client::BigtableClient;
use crate::m0001_baseline::M0001Baseline;

const MIGRATION_LOCK_NAME: &str = "catalog_migrations";
const MIGRATION_LOCK_LEASE: Duration = Duration::from_secs(120);

pub struct BigtableCatalogMigrations;

pub struct BigtableMigrationStores {
    pub client: Arc<BigtableClient>,
}

/// Bigtable's entry for every [`MigrationId`] in the workspace registry.
#[must_use]
pub fn migration(id: MigrationId) -> Migration<BigtableMigrationStores> {
    match id {
        MigrationId::Baseline => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &M0001Baseline,
                include_str!("m0001_baseline.rs"),
            )],
        },
        MigrationId::ResourceTagConditionKeys => Migration::NotApplicable(
            "the Bigtable backend did not exist when dynamodb:ResourceTag was accepted",
        ),
        MigrationId::SortKeyDefinitions => Migration::NotApplicable(
            "the Bigtable backend did not exist before the #259 UpdateTable fix",
        ),
        MigrationId::IndexTableBaseKey => {
            Migration::NotApplicable("Bigtable GSI shadow row keys always embed the base key")
        }
        MigrationId::DropContinuousBackups => {
            Migration::NotApplicable("the Bigtable backend never created continuous_backups")
        }
        MigrationId::BackupCatalogIndex => Migration::NotApplicable(
            "logical backups and restores are not supported on the Bigtable backend",
        ),
        MigrationId::StreamPending => {
            Migration::NotApplicable("stream_pending is specific to PostgreSQL")
        }
        MigrationId::RestoreProgress => {
            Migration::NotApplicable("restore_progress is specific to PostgreSQL")
        }
        MigrationId::WarmThroughput => Migration::NotApplicable(
            "Bigtable stores table and GSI descriptions as schemaless JSON documents",
        ),
        MigrationId::AccountMetrics => {
            Migration::NotApplicable("Bigtable metrics rows are keyed by account from baseline")
        }
        MigrationId::AttributeNameEscape => Migration::NotApplicable(
            "Bigtable serializes item attributes as raw JSON bytes rather than PostgreSQL jsonb",
        ),
        MigrationId::UniqueAccessKeyIds => Migration::NotApplicable(
            "Bigtable keys access_key:<id> rows directly by the access key ID",
        ),
    }
}

pub(crate) const RELEASED: &[Released] = &[
    Released {
        id: MigrationId::Baseline,
        store: StoreKind::Catalog,
        checksum: "15fe6100543e944512ba143148baa1ad8546d77b7c5311a64a1e4c1fdae995b8",
        fingerprint: Some("0001_baseline/bigtable-v1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::ResourceTagConditionKeys,
        store: StoreKind::Catalog,
        checksum: "6005df4c45ba91a78216e20daea4c083fdb4e4626cec340fac63d6398a9a7b1b",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::SortKeyDefinitions,
        store: StoreKind::Catalog,
        checksum: "4c1a7f957a15e18ce8b67d5e315dd8842ce2c130321767ed6a74d7c3bd2be8ff",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::IndexTableBaseKey,
        store: StoreKind::Catalog,
        checksum: "9360520018e48ab95430c4e4860e190d02bd2b1d4eaa7fee3c25dd5799485710",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::DropContinuousBackups,
        store: StoreKind::Catalog,
        checksum: "bb58e89ff037a5b8d62046f62324a77a55c77a1f5ea4ca388503e078dd91306d",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::BackupCatalogIndex,
        store: StoreKind::Catalog,
        checksum: "40f635d7ae8f4b84d699d8e1570dbbc500215f994b0650b10301b5d843e32d65",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::StreamPending,
        store: StoreKind::Catalog,
        checksum: "c52d4a33a9de8c2cd067ade20403ac84bdcfb6ed2f5b941c9883815dd74e530b",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::RestoreProgress,
        store: StoreKind::Catalog,
        checksum: "27863f1f409b47b66bc466b8370ac3a89b4c29c127aea3e64f74d632b51b46f3",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::WarmThroughput,
        store: StoreKind::Catalog,
        checksum: "113674d9f30006a2b205ab9b743cfa4d796bcff710e57bf412c1c6386679f6ff",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::AccountMetrics,
        store: StoreKind::Catalog,
        checksum: "0725f80ca491bf7569c9c6dc1a97ed3fa53bac4776d93b7ecfcace13a3e313a1",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::AttributeNameEscape,
        store: StoreKind::Catalog,
        checksum: "b17aff6973f9fc66b763d6f6f2d7b44b49bdb4e38e437ed0afddc4b25f64a34c",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::UniqueAccessKeyIds,
        store: StoreKind::Catalog,
        checksum: "5f398633feda577647c16b44461219db4d1d3b2d68c4a25ba7fbf015d182ecc4",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
];

struct BigtableMigrationRunnerBackend {
    stores: BigtableMigrationStores,
}

impl BigtableMigrationRunnerBackend {
    async fn connect(target: &Target) -> Result<Self, MigrationError> {
        let cfg = BigtableStorageConfig::from_connection_string(&target.location)
            .map_err(|e| MigrationError::store("parse bigtable connection string", e))?;
        let client = BigtableClient::connect(&cfg)
            .await
            .map_err(|e| MigrationError::store("connect to Bigtable", e))?;
        Ok(Self {
            stores: BigtableMigrationStores {
                client: Arc::new(client),
            },
        })
    }

    async fn catalog_table_exists(&self) -> Result<bool, MigrationError> {
        let mut admin = AdminClient::connect(&self.stores.client)
            .await
            .map_err(|e| MigrationError::store("connect Bigtable admin client", e))?;
        let tables = admin
            .list_tables()
            .await
            .map_err(|e| MigrationError::store("list Bigtable tables", e))?;
        Ok(tables.iter().any(|t| t == CATALOG_TABLE))
    }
}

fn value_to_ledger_row(store: &str, v: &serde_json::Value) -> Result<LedgerRow, MigrationError> {
    let id = v
        .get("id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| MigrationError::Store(format!("missing id in {store} ledger row")))?;
    let str_field = |k: &str| {
        v.get(k)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned()
    };
    LedgerRow::from_columns(
        Some(store),
        (
            id,
            str_field("name"),
            str_field("kind"),
            str_field("checksum"),
            str_field("outcome"),
            str_field("notes"),
            str_field("applied_at"),
            str_field("applied_by"),
        ),
    )
}

#[async_trait]
impl MigrationBackend for BigtableMigrationRunnerBackend {
    type Stores = BigtableMigrationStores;

    fn backend_name(&self) -> &'static str {
        crate::BACKEND_NAME
    }

    fn migration(&self, id: MigrationId) -> Migration<Self::Stores> {
        migration(id)
    }

    fn released(&self) -> &'static [Released] {
        RELEASED
    }

    fn stores(&self) -> &Self::Stores {
        &self.stores
    }

    async fn store_names(&self, kind: StoreKind) -> Result<Vec<String>, MigrationError> {
        match kind {
            StoreKind::Catalog => Ok(vec![CATALOG_TABLE.to_owned()]),
            StoreKind::Data | StoreKind::Account => Ok(Vec::new()),
        }
    }

    async fn read_ledger(&self, store: &StoreRef) -> Result<Vec<LedgerRow>, MigrationError> {
        if !self.catalog_table_exists().await? {
            return Ok(Vec::new());
        }
        let cat = Catalog::new(&self.stores.client);
        let prefix = keys::migration_ledger_prefix_for_store(store.kind.as_str());
        let rows = cat
            .scan_prefix(&prefix)
            .await
            .map_err(|e| MigrationError::store("read migration ledger", e))?;
        let mut parsed = Vec::with_capacity(rows.len());
        for (_, v) in rows {
            parsed.push(value_to_ledger_row(&store.name, &v)?);
        }
        parsed.sort_by_key(|r| r.id);
        Ok(parsed)
    }

    async fn read_traces(&self) -> Result<CatalogTraces, MigrationError> {
        if !self.catalog_table_exists().await? {
            return Ok(CatalogTraces::default());
        }
        let cat = Catalog::new(&self.stores.client);
        let row = cat
            .get(keys::VERSION)
            .await
            .map_err(|e| MigrationError::store("read catalog version", e))?;
        let catalog_version = row.and_then(|v| {
            v.get("catalog_version")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
        Ok(CatalogTraces {
            catalog_version,
            resource_tag_marker: false,
        })
    }

    async fn catalog_is_empty(&self) -> Result<bool, MigrationError> {
        Ok(!self.catalog_table_exists().await?)
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
        _id: MigrationId,
        step: Step<'_, Self::Stores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, MigrationError> {
        let done = match step {
            Step::File { name, .. } => {
                return Err(MigrationError::Store(format!(
                    "Bigtable backend does not execute SQL/CQL file body {name}"
                )));
            }
            Step::Code(code) => code.run(&self.stores).await?,
            Step::Record => Done::default(),
        };

        let cat = Catalog::new(&self.stores.client);
        let now_str = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| time::OffsetDateTime::now_utc().to_string());

        if let Some(mut ledger_row) = row {
            if !done.notes.is_empty() {
                ledger_row.notes = done.notes.join("\n");
            }
            let key = keys::migration_ledger_row(store.kind.as_str(), ledger_row.id);
            cat.put(
                &key,
                &json!({
                    "id": ledger_row.id,
                    "name": ledger_row.name,
                    "kind": ledger_row.kind.as_str(),
                    "checksum": ledger_row.checksum,
                    "outcome": ledger_row.outcome.as_str(),
                    "notes": ledger_row.notes,
                    "applied_at": now_str,
                    "applied_by": ledger_row.applied_by,
                }),
            )
            .await
            .map_err(|e| MigrationError::store("write migration ledger row", e))?;
        }

        if let Some(ver) = catalog_version {
            self.write_catalog_version(ver).await?;
        }

        Ok(done)
    }

    async fn write_catalog_version(&self, version: u32) -> Result<(), MigrationError> {
        let cat = Catalog::new(&self.stores.client);
        cat.put(
            keys::VERSION,
            &json!({
                "catalog_version": version.to_string(),
                "applied_at": time::OffsetDateTime::now_utc().to_string(),
            }),
        )
        .await
        .map_err(|e| MigrationError::store("write catalog_version", e))
    }

    async fn delete_ledger_row(&self, store: &StoreRef, id: u32) -> Result<(), MigrationError> {
        let cat = Catalog::new(&self.stores.client);
        let key = keys::migration_ledger_row(store.kind.as_str(), id);
        cat.delete(&key)
            .await
            .map_err(|e| MigrationError::store("delete migration ledger row", e))
    }
}

struct HeldMigrationLease {
    client: Arc<BigtableClient>,
    owner: String,
    has_lock_row: bool,
    dev_mode: bool,
}

struct BigtableLockedStores {
    store: BigtableCatalogStore,
}

impl LockedStores for BigtableLockedStores {
    fn management(&self) -> &dyn ManagementStore {
        &self.store
    }

    fn settings(&self) -> &dyn SettingsStore {
        &self.store
    }
}

#[async_trait]
impl CatalogMigrations for BigtableCatalogMigrations {
    fn listed(&self, id: MigrationId) -> Listed {
        Listed::of(&migration(id))
    }

    fn backup_advice(&self, target: &Target) -> String {
        match BigtableStorageConfig::from_connection_string(&target.location) {
            Ok(cfg) => format!(
                "back up Cloud Bigtable table '{CATALOG_TABLE}' in projects/{}/instances/{}",
                cfg.project_id, cfg.instance_id
            ),
            Err(_) => format!("back up Cloud Bigtable table '{CATALOG_TABLE}'"),
        }
    }

    async fn status(&self, target: &Target) -> Result<CatalogStatus, MigrationError> {
        let backend = BigtableMigrationRunnerBackend::connect(target).await?;
        Runner::new(&backend, env!("CARGO_PKG_VERSION"))
            .status()
            .await
    }

    async fn catalog_ledger(&self, target: &Target) -> Result<Vec<LedgerRow>, MigrationError> {
        let backend = BigtableMigrationRunnerBackend::connect(target).await?;
        let store = StoreRef {
            kind: StoreKind::Catalog,
            name: CATALOG_TABLE.to_owned(),
        };
        backend.read_ledger(&store).await
    }

    async fn lock(&self, target: &Target, timeout: Duration) -> Result<HeldLock, MigrationError> {
        let cfg = BigtableStorageConfig::from_connection_string(&target.location)
            .map_err(|e| MigrationError::store("parse bigtable connection string", e))?;
        let dev_mode = cfg.dev_mode;
        let backend = BigtableMigrationRunnerBackend::connect(target).await?;
        let owner = format!("migrate-{}", Uuid::new_v4());

        if !backend.catalog_table_exists().await? {
            return Ok(HeldLock::new(
                target.location.clone(),
                HeldMigrationLease {
                    client: backend.stores.client,
                    owner,
                    has_lock_row: false,
                    dev_mode,
                },
            ));
        }

        let deadline = Instant::now() + timeout;
        let cat = Catalog::new(&backend.stores.client);
        loop {
            match cat
                .try_lock(MIGRATION_LOCK_NAME, &owner, MIGRATION_LOCK_LEASE)
                .await
            {
                Ok(true) => {
                    return Ok(HeldLock::new(
                        target.location.clone(),
                        HeldMigrationLease {
                            client: backend.stores.client,
                            owner,
                            has_lock_row: true,
                            dev_mode,
                        },
                    ));
                }
                Ok(false) => {
                    if Instant::now() >= deadline {
                        return Err(MigrationError::Locked(format!(
                            "another migration holds lock:{MIGRATION_LOCK_NAME} in {CATALOG_TABLE}"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    return Err(MigrationError::store("acquire migration lock", e));
                }
            }
        }
    }

    async fn run(
        &self,
        lock: &HeldLock,
        release: &str,
        kind: RunKind,
    ) -> Result<RunReport, MigrationError> {
        let lease = lock
            .get::<HeldMigrationLease>()
            .ok_or_else(|| MigrationError::Store("invalid HeldLock for Bigtable".into()))?;
        let backend = BigtableMigrationRunnerBackend {
            stores: BigtableMigrationStores {
                client: lease.client.clone(),
            },
        };
        let runner = Runner::new(&backend, release);
        match kind {
            RunKind::Create | RunKind::Migrate => runner.migrate().await,
            RunKind::Reconverge => runner.reconverge().await,
        }
    }

    async fn forget(&self, lock: &HeldLock, id: u32) -> Result<(), MigrationError> {
        let lease = lock
            .get::<HeldMigrationLease>()
            .ok_or_else(|| MigrationError::Store("invalid HeldLock for Bigtable".into()))?;
        let backend = BigtableMigrationRunnerBackend {
            stores: BigtableMigrationStores {
                client: lease.client.clone(),
            },
        };
        Runner::new(&backend, env!("CARGO_PKG_VERSION"))
            .forget(id)
            .await
    }

    async fn release(&self, lock: HeldLock) {
        if let Some(lease) = lock.get::<HeldMigrationLease>()
            && lease.has_lock_row
        {
            let cat = Catalog::new(&lease.client);
            let _ = cat.release_lock(MIGRATION_LOCK_NAME, &lease.owner).await;
        }
    }

    async fn locked_stores(
        &self,
        lock: &HeldLock,
        _data_connection: Option<&str>,
    ) -> Result<Box<dyn LockedStores>, MigrationError> {
        let lease = lock
            .get::<HeldMigrationLease>()
            .ok_or_else(|| MigrationError::Store("invalid HeldLock for Bigtable".into()))?;
        Ok(Box::new(BigtableLockedStores {
            store: BigtableCatalogStore::new(lease.client.clone(), None, lease.dev_mode),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_storage::migrations::RELEASED_THROUGH;

    #[test]
    fn migration_table_covers_all_ids_and_refuses_additive() {
        extenddb_storage::migrations::refuse_additive(crate::BACKEND_NAME, migration)
            .expect("no additive migrations without previous-release CI");
        if let Err(problems) = extenddb_storage::migrations::check_released(
            crate::BACKEND_NAME,
            migration,
            RELEASED,
            RELEASED_THROUGH,
        ) {
            panic!("{problems}");
        }
        for id in MigrationId::ALL {
            let _ = BigtableCatalogMigrations.listed(id);
        }
    }
}
