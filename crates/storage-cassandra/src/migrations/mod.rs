// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The Cassandra backend's catalog migrations.
//!
//! Stores: the catalog keyspace ([`StoreKind::Catalog`]) and one keyspace
//! per account ([`StoreKind::Account`]). [`migration`] is the table, an
//! exhaustive `match` from every [`MigrationId`] to this backend's bodies.
//! [`RELEASED`] pins the released bodies; the release PR that raises
//! `RELEASED_THROUGH` pastes the lines the checksum test prints.
//!
//! Entry points: [`catalog_status`] reads with no lock and no write;
//! [`MigrationLock`] is the lock a run holds and [`ServingRegistration`]
//! what a server holds while it serves; [`migrate_account`] is how
//! CreateAccount and `init`'s default account run the account migrations on
//! a new keyspace; [`CassandraCatalogMigrations`] is how the commands reach
//! them.
//!
//! Every statement names its keyspace (#632), and the runner waits for
//! every live node to agree on the schema after each DDL statement (#637).

mod agreement;
mod backend;
mod commands;
mod connect;
mod cql;
mod guarded;
mod locks;
// Frozen once released: neither rustfmt nor a later clippy lint may require
// an edit that changes its checksum.
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0006_backup_catalog_index;
pub mod postcheck;
mod registrar;
mod registrations;

use std::sync::Arc;

use extenddb_storage::file_body;
use extenddb_storage::migrations::m0002_resource_tag_condition_keys::{
    self, ResourceTagConditionKeys,
};
use extenddb_storage::migrations::{
    CatalogStatus, Kind, Migration, MigrationError, MigrationId, Released, RunReport, Runner,
    StoreBody, StoreKind,
};

pub use agreement::AGREEMENT_TIMEOUT;
#[cfg(any(test, feature = "test-util"))]
pub(crate) use backend::stopped;
pub use backend::{CassandraMigrations, CassandraStores};
pub use commands::CassandraCatalogMigrations;
pub(crate) use commands::lease_of;
pub use connect::Deployment;
pub(crate) use connect::{LOCKED_DDL_TIMEOUT, REQUEST_TIMEOUT};
pub(crate) use locks::LeaseHandle;
pub use locks::{LeaseTimes, MigrationLock};
pub use registrar::ServerRegistration;
pub use registrations::{RegistrationTimes, SERVE_COMMAND, ServingRegistration};

use crate::cassandra_util::CassandraSession;

/// Migration 1's catalog body: the tests read it, and
/// [`foreign_contents`] reads the `schema_history` rows it inserts.
pub(crate) const CATALOG_BASELINE: &str = file_body!("migrations/catalog", "0001_baseline.cql");

/// Migration 1's account body, for the tests that read it.
#[cfg(test)]
pub(crate) const ACCOUNT_BASELINE: &str = file_body!("migrations/account", "0001_baseline.cql");

/// The definitions migration 1 creates in an empty catalog keyspace, which
/// the post-check compares the keyspace with the first time migration 1 is
/// applied to it.
pub(crate) const CATALOG_EXPECTED: &str =
    include_str!("../../migrations/catalog/0001_baseline.expected.json");

/// The same for an account keyspace.
pub(crate) const ACCOUNT_EXPECTED: &str =
    include_str!("../../migrations/account/0001_baseline.expected.json");

/// Checksums of the released bodies (none until v1.0).
pub(crate) const RELEASED: &[Released] = &[];

/// This backend's entry for every id. Public so `migrate --list` can print
/// each migration's kind without connecting.
#[must_use]
pub fn migration(id: MigrationId) -> Migration<CassandraStores> {
    match id {
        MigrationId::Baseline => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![
                file_body!(
                    StoreKind::Catalog,
                    "migrations/catalog",
                    "0001_baseline.cql"
                ),
                file_body!(
                    StoreKind::Account,
                    "migrations/account",
                    "0001_baseline.cql"
                ),
            ],
        },
        MigrationId::ResourceTagConditionKeys => Migration::Applies {
            kind: Kind::Breaking,
            // A `dynamodb:ResourceTag` key written through the API after the
            // rewrite has no effect, as on the service; running the rewrite
            // again would give it one.
            convergent: false,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &ResourceTagConditionKeys,
                m0002_resource_tag_condition_keys::SOURCE,
            )],
        },
        MigrationId::SortKeyDefinitions => Migration::NotApplicable(
            "the Cassandra backend first shipped in v0.1.14, after the #259 fix in v0.1.5, so no \
             Cassandra catalog holds a sort key definition the pre-fix UpdateTable deleted",
        ),
        MigrationId::IndexTableBaseKey => Migration::NotApplicable(
            "Cassandra index tables are keyed by their own primary key, so there is no base key \
             index to add",
        ),
        MigrationId::DropContinuousBackups => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![file_body!(
                StoreKind::Catalog,
                "migrations/catalog",
                "0005_drop_continuous_backups.cql"
            )],
        },
        // Breaking: a binary without it neither reads the index nor writes
        // the job record on the restore registrations it creates, so a
        // restore it ran would be owned by nobody. Convergent: every
        // statement is `IF NOT EXISTS`, the backfill writes only a
        // registration whose job status is unset, which no registration
        // this release writes is (each is inserted with its job record), and
        // the catalog id is inserted only when absent; so a second run over
        // data written since changes nothing but a registration a release
        // before it wrote, which is the repair `--reconverge` is for.
        MigrationId::BackupCatalogIndex => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0006_backup_catalog_index::BackupCatalogIndex,
                m0006_backup_catalog_index::SOURCE,
            )],
        },
        MigrationId::StreamPending => Migration::NotApplicable(
            "the stream_pending table belongs to the PostgreSQL backend, and Cassandra numbers its \
             stream records with the server's clock (#660)",
        ),
        MigrationId::RestoreProgress => Migration::NotApplicable(
            "a store-backed restore records its progress on the target's catalog tables row \
             (restore_file_index, restore_items_applied, which migration 6 adds); the \
             restore_progress table is PostgreSQL's data database's",
        ),
        MigrationId::WarmThroughput => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![file_body!(
                StoreKind::Catalog,
                "migrations/catalog",
                "0009_warm_throughput.cql"
            )],
        },
        MigrationId::AccountMetrics => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![file_body!(
                StoreKind::Catalog,
                "migrations/catalog",
                "0010_account_metrics.cql"
            )],
        },
        MigrationId::AttributeNameEscape => Migration::NotApplicable(
            "Cassandra stores the documents that carry attribute names as JSON text in text \
             columns, where U+0000 is the six-character escape \\u0000, so every name reads back \
             as it was written",
        ),
    }
}

/// The catalog status. Read-only: no lock is taken and nothing is written,
/// so it runs beside a server. A catalog keyspace that does not exist, or
/// that holds no ledger and no `catalog_version` row, is `Uninitialized`.
///
/// # Errors
///
/// A read fails, including the ledger of an account keyspace, which the
/// error names.
pub async fn catalog_status(
    session: Arc<CassandraSession>,
    keyspace_prefix: &str,
) -> Result<CatalogStatus, MigrationError> {
    let migrations = CassandraMigrations::for_status(session, keyspace_prefix);
    // A read records nothing, so no release is named.
    Runner::new(&migrations, "").status().await
}

/// The catalog status, read as the holder of `lock`, which `migrate --yes`
/// and `--forget` read before they act: each request a code body's `detect`
/// sends goes through the leased management store, which checks the lease
/// before it and records progress when it completes, and each ledger read's
/// schema-agreement wait records progress. A status that runs migration 2's
/// `detect` over every principal of a large catalog then keeps the lock
/// past the stall, where a read through [`catalog_status`] records nothing
/// and the renewal stops after 30 s.
///
/// # Errors
///
/// As [`catalog_status`], or the lock's deadline passed during the read.
pub async fn status_holding(
    lock: &MigrationLock,
    keyspace_prefix: &str,
) -> Result<CatalogStatus, MigrationError> {
    let migrations = CassandraMigrations::locked(lock, keyspace_prefix);
    Runner::new(&migrations, "").status().await
}

/// Serializes the account migrations a process runs (#453): two
/// CreateAccount calls for the same account run the same DDL one after the
/// other, never at once.
static ACCOUNT_MIGRATIONS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Run every account migration this binary carries on `keyspace`, which
/// must exist, through the runner: each body's statements with schema
/// agreement after every DDL statement, and its ledger row after it.
/// CreateAccount and `init`'s default account call it once the keyspace
/// exists, before the account is usable. The catalog's rows are read and
/// none is written.
///
/// # Errors
///
/// The catalog's status forbids a run, or a body fails.
pub async fn migrate_account(
    session: Arc<CassandraSession>,
    keyspace_prefix: &str,
    keyspace: &str,
    release: &str,
) -> Result<RunReport, MigrationError> {
    let _serialized = ACCOUNT_MIGRATIONS.lock().await;
    let migrations = CassandraMigrations::for_account(session, keyspace_prefix, keyspace);
    Runner::new(&migrations, release).migrate().await
}

/// [`migrate_account`] under the lease of the migration lock the caller
/// holds: `init`'s default account. Every write is checked against the
/// lease, every completed request records progress on it, and each schema
/// statement gets the bound of one sent under the lock. The run first waits
/// for any other account run of this process, and that wait records no
/// progress on the lease; `init`'s process runs no other.
///
/// # Errors
///
/// The lease has lapsed, the catalog's status forbids a run, or a body
/// fails.
pub(crate) async fn migrate_account_under(
    session: Arc<CassandraSession>,
    keyspace_prefix: &str,
    keyspace: &str,
    release: &str,
    lease: LeaseHandle,
) -> Result<RunReport, MigrationError> {
    let _serialized = ACCOUNT_MIGRATIONS.lock().await;
    let migrations =
        CassandraMigrations::for_account_under(session, keyspace_prefix, keyspace, lease);
    Runner::new(&migrations, release).migrate().await
}

/// [`migrate_account_under`] with a lease whose deadline has already
/// passed: for the test that an account run under a lapsed lease writes
/// nothing.
///
/// # Errors
///
/// As [`migrate_account_under`]; with a lapsed lease, the first write.
#[cfg(any(test, feature = "test-util"))]
pub async fn migrate_account_under_lapsed_lease(
    session: Arc<CassandraSession>,
    keyspace_prefix: &str,
    keyspace: &str,
) -> Result<RunReport, MigrationError> {
    let lease = LeaseHandle::lapsed(&format!("{keyspace_prefix}_catalog"));
    migrate_account_under(session, keyspace_prefix, keyspace, "test", lease).await
}

/// The bound a schema statement gets in a run under `lock`, in `init`'s
/// default-account run under `lock`'s lease, and in CreateAccount's run,
/// which holds neither: for the test that pins which run gets which bound.
#[cfg(any(test, feature = "test-util"))]
#[must_use]
pub fn schema_statement_bounds(
    lock: &MigrationLock,
    keyspace_prefix: &str,
) -> [std::time::Duration; 3] {
    let account = format!("{keyspace_prefix}_account_000000000000");
    [
        CassandraMigrations::locked(lock, keyspace_prefix).ddl_timeout(),
        CassandraMigrations::for_account_under(
            lock.session().clone(),
            keyspace_prefix,
            &account,
            lock.handle(),
        )
        .ddl_timeout(),
        CassandraMigrations::for_account(lock.session().clone(), keyspace_prefix, &account)
            .ddl_timeout(),
    ]
}

/// Take the migration lock on the deployment `keyspace_prefix` names, apply
/// every pending migration to its catalog keyspace alone, and release the
/// lock. For tests that build a catalog without `init` and create their
/// account keyspaces with [`migrate_account`]; the shared test catalogs hold
/// `accounts` rows whose keyspaces the tests have dropped.
///
/// # Errors
///
/// The lock cannot be taken within `timeout`, or as [`Runner::migrate`].
#[cfg(any(test, feature = "test-util"))]
pub async fn migrate_catalog_keyspace(
    session: Arc<CassandraSession>,
    keyspace_prefix: &str,
    release: &str,
    timeout: std::time::Duration,
) -> Result<RunReport, MigrationError> {
    migrate_catalog_keyspace_through(
        session,
        keyspace_prefix,
        release,
        timeout,
        MigrationId::LATEST,
    )
    .await
}

/// [`migrate_catalog_keyspace`] stopping after migration `through`: for
/// tests of a catalog a later migration has not reached.
///
/// # Errors
///
/// As [`migrate_catalog_keyspace`].
#[cfg(any(test, feature = "test-util"))]
pub async fn migrate_catalog_keyspace_through(
    session: Arc<CassandraSession>,
    keyspace_prefix: &str,
    release: &str,
    timeout: std::time::Duration,
    through: MigrationId,
) -> Result<RunReport, MigrationError> {
    let catalog = format!("{keyspace_prefix}_catalog");
    create_lock_table(&session, &catalog).await?;
    let lock = MigrationLock::acquire(session, &catalog, LeaseTimes::DEFAULT, timeout).await?;
    let result = {
        let migrations = CassandraMigrations::locked_catalog_only(&lock, keyspace_prefix);
        Runner::new(&migrations, release)
            .migrate_through(through)
            .await
    };
    lock.release().await;
    result
}

/// Wait until every live node reports the same schema version, as the
/// runner does after each DDL statement: for tests that create keyspaces
/// and tables themselves before a step that reads them.
///
/// # Errors
///
/// The nodes did not agree within [`AGREEMENT_TIMEOUT`].
#[cfg(any(test, feature = "test-util"))]
pub async fn wait_for_schema_agreement(session: &CassandraSession) -> Result<(), MigrationError> {
    agreement::wait_for_agreement(session, AGREEMENT_TIMEOUT).await
}

/// Run migration 1's body for stores of `kind` in `keyspace` and record
/// nothing, as a release before 1.0 left a catalog with these tables and no
/// ledger: for the tests of adoption.
///
/// # Errors
///
/// A statement or the agreement wait fails.
#[cfg(any(test, feature = "test-util"))]
pub async fn run_baseline_unrecorded(
    session: &CassandraSession,
    keyspace: &str,
    kind: StoreKind,
) -> Result<(), MigrationError> {
    let baseline = migration(MigrationId::Baseline);
    let Some(extenddb_storage::migrations::Body::File { name, text }) = baseline
        .bodies()
        .iter()
        .find(|b| b.store == kind)
        .map(|b| &b.body)
    else {
        return Err(MigrationError::Store(format!(
            "migration 1 has no file body for the {} store",
            kind.as_str()
        )));
    };
    for statement in cql::statements(keyspace, name, text)? {
        // No lock is held here, so a schema statement gets the bound of one
        // outside the lock.
        let bound = if statement.ddl {
            connect::UNLOCKED_DDL_TIMEOUT
        } else {
            connect::REQUEST_TIMEOUT
        };
        connect::send_within(
            session,
            &statement.text,
            cdrs_tokio::query_values!(),
            cdrs_tokio::consistency::Consistency::Quorum,
            None,
            name,
            bound,
        )
        .await?;
        if statement.ddl {
            agreement::wait_for_agreement(session, AGREEMENT_TIMEOUT).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Send one statement that takes no values, bounded by the request timeout:
/// `init`'s grants, which run outside the runner.
///
/// # Errors
///
/// The statement fails or gets no answer within the timeout.
pub(crate) async fn send_statement(
    session: &CassandraSession,
    statement: &str,
    context: &str,
) -> Result<(), MigrationError> {
    connect::send(
        session,
        statement,
        cdrs_tokio::query_values!(),
        cdrs_tokio::consistency::Consistency::Quorum,
        None,
        context,
    )
    .await
    .map(drop)
}

/// The bound on [`create_lock_table`]'s CREATE TABLE. The statement runs
/// before the lock exists, because the lock is a row of that table, so it
/// holds no lease and gets the bound of a schema statement outside the lock.
const SETTINGS_TABLE_BOUND: std::time::Duration = connect::UNLOCKED_DDL_TIMEOUT;

/// Create the catalog's `settings` table, where the migration lock lives,
/// with `IF NOT EXISTS`, waiting for schema agreement before the statement
/// (the keyspace was just created) and after it: `init` runs this before it
/// takes the lock, because the lock is a row of that table. The statement is
/// bounded by [`SETTINGS_TABLE_BOUND`].
///
/// # Errors
///
/// The statement or the agreement wait fails.
pub async fn create_lock_table(
    session: &CassandraSession,
    keyspace: &str,
) -> Result<(), MigrationError> {
    // The keyspace was created just before, and the CREATE TABLE can reach
    // a node that does not have it yet (#637).
    agreement::wait_for_agreement(session, AGREEMENT_TIMEOUT).await?;
    connect::send_within(
        session,
        &format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.settings (key text PRIMARY KEY, value text)"
        ),
        cdrs_tokio::query_values!(),
        cdrs_tokio::consistency::Consistency::Quorum,
        None,
        "create the settings table",
        SETTINGS_TABLE_BOUND,
    )
    .await?;
    agreement::wait_for_agreement(session, AGREEMENT_TIMEOUT).await
}

/// What an uninitialized `keyspace` holds that makes it another program's:
/// the tables migration 1 does not create; a table of a catalog table's
/// name with a column migration 1 does not give it, or defines otherwise,
/// or without a column migration 1 creates it with;
/// a `settings` table without the catalog's `key` and `value` columns;
/// `settings` rows other than the migration lock's; `schema_history` rows
/// other than the ones migration 1 inserts; and a row in any other table.
/// Empty when the keyspace does not exist, and for one an `init` created
/// and stopped in before migration 1 recorded anything: that holds only
/// tables migration 1 creates, with the columns it gives them (a column a
/// later `ALTER TABLE ... ADD` adds may be missing, and migration 1 adds
/// it), no rows but migration 1's `schema_history` rows, and at most the
/// lock row. A table name alone shows nothing: the catalog's names
/// (`metrics`, `tags`, `tables`, `accounts`) are common ones. Read before
/// anything is written to the keyspace: `init` and the migration lock
/// refuse it when this is not empty, so another program's `settings`
/// table never receives the lock row. Call it only for a keyspace whose
/// status is `Uninitialized`, since a catalog's tables hold rows.
///
/// # Errors
///
/// `system_schema` or a table of the keyspace cannot be read.
pub async fn foreign_contents(
    session: &CassandraSession,
    keyspace: &str,
) -> Result<Vec<String>, MigrationError> {
    use cdrs_tokio::consistency::Consistency;

    let expected: postcheck::Definitions = serde_json::from_str(CATALOG_EXPECTED)
        .map_err(|e| MigrationError::Store(format!("read the expected definitions: {e}")))?;
    let ours: std::collections::BTreeSet<&str> = expected
        .columns
        .keys()
        .filter_map(|k| k.split_once('.').map(|(t, _)| t))
        .collect();
    let tables = connect::tables_of(session, keyspace).await?;
    let mut foreign: Vec<String> = tables
        .iter()
        .filter(|t| *t != "extenddb_migrations" && !ours.contains(t.as_str()))
        .map(|t| format!("the table {t}"))
        .collect();
    let actual = postcheck::describe(session, keyspace).await?;
    let added = baseline_added_columns();
    for table in tables.iter().filter(|t| ours.contains(t.as_str())) {
        let differing = postcheck::columns_not_in(&expected, &actual, table);
        if table == "settings"
            && !connect::has_columns(session, keyspace, "settings", &["key", "value"]).await?
        {
            foreign
                .push("the table settings without the catalog's key and value columns".to_owned());
            continue;
        }
        if !differing.is_empty() {
            foreign.push(format!(
                "the table {table}, whose column(s) {} migration 1 does not create",
                differing.join(", ")
            ));
            continue;
        }
        // Migration 1 creates each table with all its columns but the ones
        // a later `ALTER TABLE ... ADD` adds, so only those may be missing.
        let missing: Vec<String> = postcheck::columns_missing(&expected, &actual, table)
            .into_iter()
            .filter(|column| !added.contains(&(table.as_str(), column.as_str())))
            .collect();
        if !missing.is_empty() {
            foreign.push(format!(
                "the table {table}, which lacks the column(s) {} migration 1 creates it with",
                missing.join(", ")
            ));
            continue;
        }
        let context = format!("read the rows of {keyspace}.{table}");
        match table.as_str() {
            "settings" => {
                let found = connect::rows(
                    session,
                    &format!("SELECT key FROM {keyspace}.settings LIMIT 3"),
                    cdrs_tokio::query_values!(),
                    Consistency::Quorum,
                    &context,
                )
                .await?;
                for row in &found {
                    let key: String = connect::column(row, "key", &context)?;
                    if key != locks::LOCK_KEY {
                        foreign.push(format!("the settings row '{key}'"));
                    }
                }
            }
            "schema_history" => {
                let found = connect::rows(
                    session,
                    &format!("SELECT version, description FROM {keyspace}.schema_history"),
                    cdrs_tokio::query_values!(),
                    Consistency::Quorum,
                    &context,
                )
                .await?;
                let inserted = baseline_history();
                for row in &found {
                    let version: i32 = connect::column(row, "version", &context)?;
                    let description: Option<String> =
                        connect::nullable(row, "description", &context)?;
                    if !inserted.contains(&(version, description.as_deref().unwrap_or_default())) {
                        foreign.push(format!("the schema_history row of version {version}"));
                    }
                }
            }
            _ => {
                let found = connect::rows(
                    session,
                    &format!("SELECT * FROM {keyspace}.{table} LIMIT 1"),
                    cdrs_tokio::query_values!(),
                    Consistency::Quorum,
                    &context,
                )
                .await?;
                if !found.is_empty() {
                    foreign.push(format!("rows in the table {table}"));
                }
            }
        }
    }
    Ok(foreign)
}

/// The (table, column) pairs migration 1 adds to the catalog keyspace with
/// `ALTER TABLE ... ADD IF NOT EXISTS`: a table an `init` or a 0.1.x release
/// created before such a statement lacks them, and migration 1 adds them.
fn baseline_added_columns() -> std::collections::BTreeSet<(&'static str, &'static str)> {
    CATALOG_BASELINE
        .lines()
        .filter_map(|line| {
            let rest = line.trim().strip_prefix("ALTER TABLE ")?;
            let (table, rest) = rest.split_once(' ')?;
            let (column, _) = rest.strip_prefix("ADD IF NOT EXISTS ")?.split_once(' ')?;
            Some((table, column))
        })
        .collect()
}

/// The `schema_history` rows migration 1's catalog body inserts, as
/// `(version, description)`.
fn baseline_history() -> Vec<(i32, &'static str)> {
    CATALOG_BASELINE
        .lines()
        .filter_map(|line| {
            let values = line
                .trim()
                .strip_prefix("INSERT INTO schema_history (version, description) VALUES (")?;
            let (version, rest) = values.split_once(", '")?;
            Some((version.parse().ok()?, rest.strip_suffix("');")?))
        })
        .collect()
}

/// The authorizer the node runs, read from `system_views.settings`
/// (Cassandra 4.0 and later).
///
/// # Errors
///
/// The setting cannot be read.
pub async fn authorizer(session: &CassandraSession) -> Result<String, MigrationError> {
    let found = connect::rows(
        session,
        "SELECT value FROM system_views.settings WHERE name = 'authorizer'",
        cdrs_tokio::query_values!(),
        cdrs_tokio::consistency::Consistency::One,
        "read the authorizer from system_views.settings, which Cassandra 4.0 and later have",
    )
    .await?;
    let value = found
        .first()
        .map(|row| connect::nullable::<String>(row, "value", "read the authorizer"))
        .transpose()?
        .flatten();
    value.ok_or_else(|| {
        MigrationError::Store("system_views.settings has no authorizer row".to_owned())
    })
}

/// The statements that grant `role` what `migrate`, `serve` and
/// CreateAccount need on `keyspace` under `CassandraAuthorizer`: SELECT,
/// MODIFY, CREATE and ALTER; on an account keyspace DROP as well, because
/// DeleteTable drops the account's item and index tables and DeleteAccount
/// drops the keyspace; on the catalog keyspace CREATE on ALL KEYSPACES, for
/// the keyspaces CreateAccount creates.
pub(crate) fn grants(keyspace: &str, role: &str, account: bool) -> Vec<String> {
    let mut permissions = vec!["SELECT", "MODIFY", "CREATE", "ALTER"];
    if account {
        permissions.push("DROP");
    }
    let mut statements: Vec<String> = permissions
        .iter()
        .map(|p| format!("GRANT {p} ON KEYSPACE {keyspace} TO '{role}'"))
        .collect();
    if !account {
        statements.push(format!("GRANT CREATE ON ALL KEYSPACES TO '{role}'"));
    }
    statements
}

/// Send [`grants`] for `role` on `keyspace`, each bounded by the request
/// timeout, unless the node runs `AllowAllAuthorizer`, which supports no
/// GRANT and allows everything. Returns the authorizer when it granted.
/// A keyspace belongs to the role that created it, so `init`, which creates
/// the catalog and the default account's keyspace as the admin role, and a
/// run as an admin role that creates a missing account keyspace grant the
/// application role its permissions with this. Under a lease, `check` runs
/// before the authorizer read and before each GRANT, and `progress` after
/// each request completes, so a holder whose lease has lapsed sends no
/// further GRANT.
///
/// # Errors
///
/// `check` fails, the authorizer cannot be read, or a GRANT fails.
pub(crate) async fn grant_role(
    session: &CassandraSession,
    keyspace: &str,
    role: &str,
    account: bool,
    check: impl Fn() -> Result<(), MigrationError>,
    progress: impl Fn(),
) -> Result<Option<String>, MigrationError> {
    check()?;
    let authorizer = authorizer(session).await?;
    progress();
    if authorizer.ends_with("AllowAllAuthorizer") {
        return Ok(None);
    }
    for statement in grants(keyspace, role, account) {
        check()?;
        send_statement(session, &statement, "grant the application role")
            .await
            .map_err(|e| {
                MigrationError::Store(format!("{statement} failed under {authorizer}: {e}"))
            })?;
        progress();
    }
    Ok(Some(authorizer))
}

/// Register a server and read the catalog's status, before anything opens
/// the engine, so a server that refuses has written nothing else. The
/// registration is written at `QUORUM` first, then the migration lock is
/// read at `SERIAL` and the ledgers at `QUORUM`: a migration that starts in
/// between waits for this server, and one that holds the lock makes it exit
/// 6. A catalog without the registration table has not had migration 1, and
/// its status says what to run.
///
/// # Errors
///
/// [`MigrationError::Locked`] (exit 6) while a migration holds the lock;
/// [`MigrationError::Status`] for a status a server refuses; or a read or
/// write failed.
pub async fn register_server(
    session: Arc<CassandraSession>,
    keyspace_prefix: &str,
    host: &str,
    release: &str,
) -> Result<ServerRegistration, MigrationError> {
    register_server_as(session, keyspace_prefix, host, release, SERVE_COMMAND).await
}

/// [`register_server`] for the process `command` names: `extenddb serve`
/// ([`SERVE_COMMAND`]) or a command that opens the catalog as a server does
/// (`extenddb backup list`), whose row carries its name (the registrations
/// module doc says where) and whose refusal while a migration holds the
/// lock names the holder without the server's advice to start the server.
///
/// # Errors
///
/// As [`register_server`].
pub async fn register_server_as(
    session: Arc<CassandraSession>,
    keyspace_prefix: &str,
    host: &str,
    release: &str,
    command: &str,
) -> Result<ServerRegistration, MigrationError> {
    register_server_with(
        session,
        keyspace_prefix,
        host,
        release,
        command,
        RegistrationTimes::DEFAULT,
    )
    .await
}

/// [`register_server`] with other registration times, for tests.
///
/// # Errors
///
/// As [`register_server`].
pub async fn register_server_timed(
    session: Arc<CassandraSession>,
    keyspace_prefix: &str,
    host: &str,
    release: &str,
    times: RegistrationTimes,
) -> Result<ServerRegistration, MigrationError> {
    register_server_with(
        session,
        keyspace_prefix,
        host,
        release,
        SERVE_COMMAND,
        times,
    )
    .await
}

async fn register_server_with(
    session: Arc<CassandraSession>,
    keyspace_prefix: &str,
    host: &str,
    release: &str,
    command: &str,
    times: RegistrationTimes,
) -> Result<ServerRegistration, MigrationError> {
    let release = &registrations::release_cell(release, command);
    let catalog = format!("{keyspace_prefix}_catalog");
    agreement::wait_for_agreement(&session, AGREEMENT_TIMEOUT).await?;
    if !connect::table_exists(&session, &catalog, "extenddb_servers").await? {
        let status = catalog_status(session, keyspace_prefix).await?;
        return Err(match status {
            CatalogStatus::Current { .. } | CatalogStatus::AheadCompatible { .. } => {
                MigrationError::Store(format!(
                    "{catalog}.extenddb_servers, which migration 1 creates, does not exist"
                ))
            }
            status => MigrationError::Status(status),
        });
    }
    let registration =
        ServingRegistration::register_timed(session.clone(), &catalog, host, release, times)
            .await?;
    let checked = async {
        if let Some(holder) = locks::lock_holder(&session, &catalog).await? {
            // A command's refusal ends at the holder; the command's caller
            // adds that it is retried once the migration has finished.
            let then = if command == SERVE_COMMAND {
                "; start the server again when it finishes"
            } else {
                ""
            };
            return Err(MigrationError::Locked(format!(
                "a migration holds the catalog's migration lock (holder {holder}){then}"
            )));
        }
        match catalog_status(session.clone(), keyspace_prefix).await? {
            status @ CatalogStatus::AheadCompatible { .. } => {
                tracing::warn!(
                    "{status}; serving it, because a binary may serve migrations it does not \
                     know when every one of them is additive"
                );
                Ok(status)
            }
            status @ CatalogStatus::Current { .. } => Ok(status),
            status => Err(MigrationError::Status(status)),
        }
    }
    .await;
    match checked {
        Ok(status) => Ok(ServerRegistration::new(
            session,
            keyspace_prefix,
            host,
            release,
            times,
            status,
            registration,
        )),
        Err(e) => {
            registration.release().await;
            Err(e)
        }
    }
}
