// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL` schema migration helpers for catalog and data databases.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::PgPool;

/// Embedded catalog migration files, applied in order.
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_schema.sql",
        include_str!("../../storage-postgres/migrations/001_schema.sql"),
    ),
    (
        "002_vector_indexes.sql",
        include_str!("../../storage-postgres/migrations/002_vector_indexes.sql"),
    ),
    (
        "003_backup_definitions.sql",
        include_str!("../../storage-postgres/migrations/003_backup_definitions.sql"),
    ),
    (
        "004_control_chars_encoding.sql",
        include_str!("../../storage-postgres/migrations/004_control_chars_encoding.sql"),
    ),
];

/// The runner's convergence write, executed after the walk over
/// [`CATALOG_MIGRATIONS`]. Same statement shape as the files' own in-file
/// writes, parameterized on the compiled version.
pub(crate) const SET_CATALOG_VERSION_SQL: &str =
    "UPDATE settings SET value = $1 WHERE key = 'catalog_version'";

/// Run catalog migrations, skipping already-applied ones.
pub(crate) async fn run_catalog_migrations(pool: &PgPool) -> OpResult<()> {
    println!("--- Running catalog migrations...");
    for (filename, sql) in CATALOG_MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        sqlx::raw_sql(sql)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Migration {filename} failed: {e}")))?;
        // TODO(#221): applying this SQL and recording it are separate commits.
        // A crash here can leave a migration applied but unrecorded. Catalog 001
        // is normally shielded from replay by its version write, data 001 has an
        // adoption guard, and data 002 is repeatable, but those are narrow
        // recovery properties: catalog 001 is not idempotent and replaying data
        // 003 drops the token table. The sqlx adoption must remove the files'
        // own BEGIN/COMMIT and commit each ledger row with its migration before
        // another migration lands.
        record_migration(pool, filename).await?;
    }
    // The runner owns the final version write. Each file still writes the
    // version it introduces, but a replay can re-apply an EARLIER file while a
    // later one stays recorded and skipped: the re-applied file's in-file write
    // then leaves the stored version behind the schema actually present, the
    // startup gate refuses the catalog, and a second migrate finds nothing
    // unrecorded and writes nothing, stranding the deployment. Converging on
    // the compiled version after every completed walk closes that gap. The
    // in-file writes stay until #221 moves version ownership into the runner
    // entirely.
    converge_catalog_version(pool).await?;
    for step in converge_catalog_shapes(pool).await? {
        println!("    Converged catalog shape: {step}.");
    }
    println!("    Migrations applied.");
    Ok(())
}

/// Filenames of [`CATALOG_MIGRATIONS`] not recorded in the catalog's
/// `schema_history`. The ledger keys on the filename, so this is the set
/// [`run_catalog_migrations`] would apply, whatever `catalog_version` says:
/// a catalog stamped with the current version by another migration of the
/// same number (a different branch's `003_*`) still lists this binary's file
/// here.
pub(crate) async fn pending_catalog_migrations(pool: &PgPool) -> OpResult<Vec<String>> {
    let mut pending = Vec::new();
    for (filename, _sql) in CATALOG_MIGRATIONS {
        if !is_migration_applied(pool, filename).await? {
            pending.push((*filename).to_owned());
        }
    }
    Ok(pending)
}

/// One catalog shape step: a description for reports, whether the catalog
/// needs it (a query returning one boolean), and the idempotent SQL that
/// applies it.
struct ShapeStep {
    what: &'static str,
    needed: &'static str,
    apply: &'static str,
    /// Whether `serve` applies this step at startup
    /// ([`converge_catalog_shapes_at_serve`]); every step is applied by
    /// `extenddb migrate --yes`.
    at_serve: bool,
}

/// Shape steps for a catalog whose recorded migrations ran an earlier
/// revision of a file (the ledger keys on the filename, so the walk skips
/// the edited file). Each `needed` query is false on a catalog the current
/// files created, and on one where `table_restores` does not exist yet (the
/// walk creates it in the current shape).
///
/// `003_backup_definitions.sql` was edited in place before release: its
/// earlier revision created `table_restores` without `source_table_arn` and
/// with `DEFAULT NOW()` on `restore_date_time`. DescribeTable selects
/// `source_table_arn` for every table, so on that shape every DescribeTable
/// fails. The added column is nullable (rows written by the earlier build
/// have no value, and are reported without a SourceTableArn); every row this
/// build writes sets it.
///
/// Dropping the default is applied by `migrate --yes` only, never by
/// `serve`: the earlier build inserts `table_restores` rows without
/// `restore_date_time` and relies on the default, so dropping it while an
/// earlier-build server is still running makes that server's restores fail.
/// `migrate` is run with every server stopped. Until then the default is
/// harmless to this build, which always writes the value.
const CATALOG_SHAPE_STEPS: &[ShapeStep] = &[
    ShapeStep {
        what: "add table_restores.source_table_arn",
        needed: "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = 'public' AND table_name = 'table_restores') \
                 AND NOT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = 'table_restores' \
                 AND column_name = 'source_table_arn')",
        apply: "ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS source_table_arn TEXT",
        at_serve: true,
    },
    ShapeStep {
        what: "drop the default of table_restores.restore_date_time",
        needed: "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = 'table_restores' \
                 AND column_name = 'restore_date_time' AND column_default IS NOT NULL)",
        apply: "ALTER TABLE table_restores ALTER COLUMN restore_date_time DROP DEFAULT",
        at_serve: false,
    },
];

/// How long a startup shape step waits for its table lock, in seconds. An
/// `ALTER TABLE` waits behind every open transaction that touched the table,
/// and every later statement on the table waits behind the `ALTER`; with
/// other servers still serving, an unbounded wait would stall them all. On
/// timeout the server refuses to start instead.
pub(crate) const SERVE_SHAPE_LOCK_TIMEOUT_SECS: u64 = 5;

async fn shape_step_needed(pool: &PgPool, step: &ShapeStep) -> OpResult<bool> {
    sqlx::query_scalar(step.needed)
        .fetch_one(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Check catalog shape ({}): {e}", step.what)))
}

/// The [`CATALOG_SHAPE_STEPS`] this catalog needs, by description.
pub(crate) async fn pending_catalog_shapes(pool: &PgPool) -> OpResult<Vec<String>> {
    let mut pending = Vec::new();
    for step in CATALOG_SHAPE_STEPS {
        if shape_step_needed(pool, step).await? {
            pending.push(step.what.to_owned());
        }
    }
    Ok(pending)
}

/// Apply the [`CATALOG_SHAPE_STEPS`] this catalog needs; returns their
/// descriptions. Runs at the end of [`run_catalog_migrations`], that is from
/// `extenddb migrate --yes`. A step that is not needed issues no DDL, so on a
/// current catalog this only reads `information_schema`. Every step is
/// idempotent, so two runs at once are safe.
pub(crate) async fn converge_catalog_shapes(pool: &PgPool) -> OpResult<Vec<String>> {
    let mut applied = Vec::new();
    for step in CATALOG_SHAPE_STEPS {
        if !shape_step_needed(pool, step).await? {
            continue;
        }
        sqlx::query(step.apply)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Catalog shape step ({}): {e}", step.what)))?;
        applied.push(step.what.to_owned());
    }
    Ok(applied)
}

/// `serve`'s startup converge: apply the needed [`CATALOG_SHAPE_STEPS`]
/// marked `at_serve`, each in its own transaction under `SET LOCAL
/// lock_timeout` of [`SERVE_SHAPE_LOCK_TIMEOUT_SECS`]. Returns the applied
/// steps' descriptions. A step that cannot take its lock in time fails with
/// an error saying so and changes nothing.
pub(crate) async fn converge_catalog_shapes_at_serve(pool: &PgPool) -> OpResult<Vec<String>> {
    let mut applied = Vec::new();
    for step in CATALOG_SHAPE_STEPS.iter().filter(|s| s.at_serve) {
        if !shape_step_needed(pool, step).await? {
            continue;
        }
        let fail = |e: sqlx::Error| {
            if crate::data::lock_not_available(&e) {
                OpError::Internal(format!(
                    "Catalog shape step ({}): could not lock the table within \
                     {SERVE_SHAPE_LOCK_TIMEOUT_SECS} s, another session holds it",
                    step.what
                ))
            } else {
                OpError::Internal(format!("Catalog shape step ({}): {e}", step.what))
            }
        };
        let mut tx = pool.begin().await.map_err(fail)?;
        sqlx::query(&format!(
            "SET LOCAL lock_timeout = '{SERVE_SHAPE_LOCK_TIMEOUT_SECS}s'"
        ))
        .execute(&mut *tx)
        .await
        .map_err(fail)?;
        sqlx::query(step.apply)
            .execute(&mut *tx)
            .await
            .map_err(fail)?;
        tx.commit().await.map_err(fail)?;
        applied.push(step.what.to_owned());
    }
    Ok(applied)
}

/// Write the compiled catalog version, but never move the stored version
/// backwards and never overwrite one this binary cannot parse: a catalog
/// stamped by a newer binary (or in a scheme this one does not know) stays
/// stamped, so that binary's startup gate keeps working and this one's keeps
/// refusing it. `extenddb migrate` refuses such a catalog before the walk;
/// this is the runner-level guard for every other caller. The rule is
/// `extenddb_storage::util::check_stored_catalog_version`, shared with SQLite
/// and `extenddb migrate`.
async fn converge_catalog_version(pool: &PgPool) -> OpResult<()> {
    let stored: Option<(String,)> =
        sqlx::query_as("SELECT value FROM settings WHERE key = 'catalog_version'")
            .fetch_optional(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Read catalog_version: {e}")))?;
    extenddb_storage::util::check_stored_catalog_version(
        stored.as_ref().map(|(v,)| v.as_str()),
        crate::CATALOG_VERSION,
    )
    .map_err(OpError::Internal)?;
    sqlx::query(SET_CATALOG_VERSION_SQL)
        .bind(crate::CATALOG_VERSION.to_string())
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Write catalog_version: {e}")))?;
    Ok(())
}

/// Embedded data-database migration files, applied in order. Tracked in the
/// data database's own `schema_history` table (a separate database from the
/// catalog), so `extenddb migrate` applies exactly the pending migrations.
pub(crate) const DATA_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_data_schema.sql",
        include_str!("../../storage-postgres/data_migrations/001_data_schema.sql"),
    ),
    (
        "002_gsi_pending.sql",
        include_str!("../../storage-postgres/data_migrations/002_gsi_pending.sql"),
    ),
    (
        "003_idempotency_account_scope.sql",
        include_str!("../../storage-postgres/data_migrations/003_idempotency_account_scope.sql"),
    ),
    (
        "004_vector_index_state.sql",
        include_str!("../../storage-postgres/data_migrations/004_vector_index_state.sql"),
    ),
];

/// Run data database migrations, skipping already-applied ones.
///
/// Mirrors [`run_catalog_migrations`]: each migration is recorded in
/// `schema_history` and skipped on later runs. The data database has its own
/// ledger because it is a separate database from the catalog.
pub(crate) async fn run_data_migrations(pool: &PgPool) -> OpResult<()> {
    println!("--- Running data migrations...");

    // Ensure the data database has a migration ledger before tracking. (The
    // catalog ledger lives in a different database and cannot be reused here.)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_history (\
             filename TEXT PRIMARY KEY, \
             applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\
         )",
    )
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Create data schema_history: {e}")))?;

    // Adopt a pre-tracking deployment: if 001 was applied by an earlier version
    // (its tables exist) but isn't recorded, record it WITHOUT re-running it.
    // Re-running 001 would execute `setval('stream_seq', ...)` again and could
    // regress the stream sequence on a live database, producing duplicate
    // sequence numbers.
    if !is_migration_applied(pool, "001_data_schema.sql").await?
        && table_exists(pool, "stream_shards").await?
    {
        println!("    Adopting existing 001_data_schema.sql (pre-tracking deployment).");
        record_migration(pool, "001_data_schema.sql").await?;
    }

    for (filename, sql) in DATA_MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        sqlx::raw_sql(sql)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Data migration {filename} failed: {e}")))?;
        // TODO(#221): applying this SQL and recording it are separate commits.
        // A crash here can leave a migration applied but unrecorded. Catalog 001
        // is normally shielded from replay by its version write, data 001 has an
        // adoption guard, and data 002 is repeatable, but those are narrow
        // recovery properties: catalog 001 is not idempotent and replaying data
        // 003 drops the token table. The sqlx adoption must remove the files'
        // own BEGIN/COMMIT and commit each ledger row with its migration before
        // another migration lands.
        record_migration(pool, filename).await?;
    }
    println!("    Data migrations applied.");
    Ok(())
}

/// Programmatic ("code") data migrations, tracked in `schema_history` alongside
/// the SQL migrations. Unlike a static `.sql` file, these enumerate the
/// dynamically-named index tables (`_ddb_<id>`) from the catalog and must run
/// outside a transaction (they use `CREATE INDEX CONCURRENTLY`), so they cannot
/// be expressed as SQL in [`DATA_MIGRATIONS`]. Applied by `extenddb migrate`
/// after the SQL migrations, so the operator controls when the change happens.
pub(crate) const DATA_CODE_MIGRATIONS: &[&str] =
    &["003_gsi_base_key_index", "004_escape_control_chars"];

/// Code migrations a server must see recorded before it serves. `003` only adds
/// an index and a server runs correctly without it; `004` changes the stored
/// form of strings, and a server that wrote through the new escape before the
/// migration ran would have its own rows rewritten by it (see
/// [`escape_legacy_control_chars`]). `PostgresEngine::check_data_migrations_applied`
/// refuses to start on a data database missing any of these.
pub(crate) const REQUIRED_DATA_CODE_MIGRATIONS: &[&str] = &["004_escape_control_chars"];

/// `application_name` the init and migrate commands set on their connections,
/// so [`refuse_if_other_clients_connected`] can leave them out of its count.
pub(crate) const MIGRATE_APPLICATION_NAME: &str = "extenddb-migrate";

/// Environment variable that skips [`refuse_if_other_clients_connected`], for a
/// data database whose only other connections are known to be idle (a
/// connection pooler holding server-side connections open, for example).
pub(crate) const IGNORE_CONNECTIONS_ENV: &str = "EXTENDDB_MIGRATE_IGNORE_CONNECTIONS";

/// Refuse to run a data-rewriting migration while anything else is connected
/// to the database `pool` reaches. `which` names that database in the refusal
/// (`"data"` or `"catalog"`).
///
/// `004_escape_control_chars` rewrites rows in place and cannot tell a row a
/// running server writes from a legacy row: an older server still writing
/// leaves unescaped rows behind, and a server of this release writing before
/// the migration finishes gets its rows double-escaped. The upgrade manual
/// requires every server to be stopped first; this check catches the case
/// where one was not. Connections carrying [`MIGRATE_APPLICATION_NAME`] are
/// this process's own and are ignored; every other session on the database
/// counts. Managed environments where a pooler keeps idle connections open
/// can set [`IGNORE_CONNECTIONS_ENV`] to skip the check.
///
/// The migrate command connects as the application role, which is not a
/// superuser. For sessions of other roles, `PostgreSQL` then shows `datname`,
/// `usename`, and `application_name` but hides `backend_type` and
/// `client_addr`, so a filter on `backend_type` alone misses an operator's
/// `psql` or a server running under another role. A session is counted when it
/// has a role (`usename`), which background workers such as autovacuum never
/// have, and its type is either `client backend` or hidden.
pub(crate) async fn refuse_if_other_clients_connected(pool: &PgPool, which: &str) -> OpResult<()> {
    let others: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT usename::text, application_name, client_addr::text \
         FROM pg_stat_activity \
         WHERE datname = current_database() \
           AND usename IS NOT NULL \
           AND coalesce(backend_type, 'client backend') = 'client backend' \
           AND pid <> pg_backend_pid() \
           AND coalesce(application_name, '') <> $1",
    )
    .bind(MIGRATE_APPLICATION_NAME)
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check for other connections: {e}")))?;
    if others.is_empty() {
        return Ok(());
    }
    let mut described: Vec<String> = others
        .iter()
        .map(|(user, app, addr)| {
            format!(
                "{user} from {} ({})",
                addr.as_deref().unwrap_or("no client address shown"),
                match app.as_deref() {
                    Some("") | None => "no application name",
                    Some(a) => a,
                }
            )
        })
        .collect();
    described.sort();
    described.dedup();
    Err(OpError::Internal(format!(
        "{} other connection(s) hold the {which} database: {}. This migration rewrites \
         stored rows and must run with every ExtendDB server stopped and every other \
         session closed. Stop them and run 'extenddb migrate --yes' again. If the only \
         other connections are idle ones held by a connection pooler, set \
         {IGNORE_CONNECTIONS_ENV}=1 to skip this check.",
        others.len(),
        described.join("; ")
    )))
}

/// The server address, port, and database name a pool is connected to.
///
/// Two pools with the same identity reach the same database, so a connection
/// check on one covers the other. The address is `None` over a Unix socket on
/// both sides of a same-host comparison, which still compares equal.
async fn database_identity(pool: &PgPool) -> OpResult<(Option<String>, Option<i32>, String)> {
    sqlx::query_as("SELECT inet_server_addr()::text, inet_server_port(), current_database()::text")
        .fetch_one(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Identify the database: {e}")))
}

/// [`refuse_if_other_clients_connected`] on every database
/// `004_escape_control_chars` rewrites: the data database, and the catalog
/// database when it is a different one, because `backup_items` lives there.
/// A deployment whose catalog and data share one database is checked once.
pub(crate) async fn refuse_if_other_clients_connected_anywhere(
    catalog_pool: &PgPool,
    data_pool: &PgPool,
) -> OpResult<()> {
    refuse_if_other_clients_connected(data_pool, "data").await?;
    if database_identity(catalog_pool).await? != database_identity(data_pool).await? {
        refuse_if_other_clients_connected(catalog_pool, "catalog").await?;
    }
    Ok(())
}

/// True when `004_escape_control_chars` still has to run on this data database
/// and the operator has not set [`IGNORE_CONNECTIONS_ENV`], so the migrate
/// command must hold the databases alone before it writes anything.
pub(crate) async fn needs_exclusive_access(data_pool: &PgPool) -> OpResult<bool> {
    if std::env::var_os(IGNORE_CONNECTIONS_ENV).is_some() {
        return Ok(false);
    }
    Ok(!is_migration_applied(data_pool, "004_escape_control_chars").await?)
}

/// Names in [`REQUIRED_DATA_CODE_MIGRATIONS`] not recorded in this data
/// database's `schema_history`.
pub(crate) async fn unapplied_required_data_migrations(pool: &PgPool) -> OpResult<Vec<String>> {
    let mut missing = Vec::new();
    for name in REQUIRED_DATA_CODE_MIGRATIONS {
        if !is_migration_applied(pool, name).await? {
            missing.push((*name).to_owned());
        }
    }
    Ok(missing)
}

/// Run programmatic data migrations, skipping already-applied ones.
///
/// Needs the catalog pool (to enumerate index tables and their base key schema)
/// and the data pool (where the `_ddb_*` tables and the `schema_history` ledger
/// live). Each step is recorded in `schema_history` and skipped on later runs,
/// exactly like the SQL migrations.
pub(crate) async fn run_data_code_migrations(
    catalog_pool: &PgPool,
    data_pool: &PgPool,
) -> OpResult<()> {
    for name in DATA_CODE_MIGRATIONS {
        if is_migration_applied(data_pool, name).await? {
            println!("    {name} — already applied, skipping.");
            continue;
        }
        println!("    Applying {name}...");
        match *name {
            "003_gsi_base_key_index" => {
                ensure_gsi_base_key_indexes(catalog_pool, data_pool).await?;
            }
            "004_escape_control_chars" => {
                // Rows written by a server that is still running would be missed
                // (an older release) or double-escaped (this release); see
                // `refuse_if_other_clients_connected`. `extenddb migrate` runs
                // the same check before its first write; this one covers any
                // other caller.
                if std::env::var_os(IGNORE_CONNECTIONS_ENV).is_none() {
                    refuse_if_other_clients_connected_anywhere(catalog_pool, data_pool).await?;
                }
                escape_legacy_control_chars(catalog_pool, data_pool).await?;
            }
            other => {
                return Err(OpError::Internal(format!(
                    "Unknown data code migration: {other}"
                )));
            }
        }
        record_migration(data_pool, name).await?;
    }
    Ok(())
}

/// Bring a fresh catalog database and a fresh data database (the same pool
/// twice on the shared layout) to the current schema the way `extenddb
/// migrate --yes` does: every catalog migration, every data migration, then
/// the data code migrations, each recorded in its ledger. For test harnesses
/// in this crate's own `tests/` directory, which otherwise copy the file
/// lists and go stale when a migration is added here.
///
/// # Errors
///
/// Returns the first migration's error, formatted.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub async fn apply_all_migrations(catalog: &PgPool, data: &PgPool) -> Result<(), String> {
    run_catalog_migrations(catalog)
        .await
        .map_err(|e| format!("{e:?}"))?;
    run_data_migrations(data)
        .await
        .map_err(|e| format!("{e:?}"))?;
    run_data_code_migrations(catalog, data)
        .await
        .map_err(|e| format!("{e:?}"))
}

/// Create the base-table-key index on every existing GSI/LSI table.
///
/// During GSI propagation each index table (`_ddb_<id>`) is looked up back to
/// its base item via `WHERE base_pk = $1 AND base_sk_* = $2`; without a leading
/// `(base_pk, base_sk_*)` index that is a sequential scan. New tables get this
/// index at creation time (see `ddl.rs`); this migration adds it to tables
/// created before the index existed. `CREATE INDEX CONCURRENTLY IF NOT EXISTS`
/// is idempotent and does not block concurrent writes.
async fn ensure_gsi_base_key_indexes(catalog_pool: &PgPool, data_pool: &PgPool) -> OpResult<()> {
    use extenddb_core::types::{AttributeDefinition, KeySchemaElement};
    use extenddb_storage::util::{sk_column, sk_column_n};

    // Enumerate every index and its base table key schema from the catalog.
    let rows: Vec<(String, serde_json::Value, serde_json::Value)> = sqlx::query_as(
        "SELECT i.index_id, t.key_schema, t.attribute_definitions \
         FROM indexes i \
         JOIN tables t ON i.table_id = t.table_id",
    )
    .fetch_all(catalog_pool)
    .await
    .map_err(|e| OpError::Internal(format!("Enumerate indexes: {e}")))?;

    for (index_id, ks_json, ad_json) in rows {
        let base_ks: Vec<KeySchemaElement> =
            serde_json::from_value(ks_json).map_err(|e| OpError::Internal(e.to_string()))?;
        let attr_defs: Vec<AttributeDefinition> =
            serde_json::from_value(ad_json).map_err(|e| OpError::Internal(e.to_string()))?;

        let base_sks = crate::data::all_sort_key_info(&base_ks, &attr_defs);
        let idx_table = crate::data::index_table_name(&index_id);
        let idx_name = format!("_ddb_{index_id}_base_key_idx");

        let mut base_key_cols = vec!["base_pk".to_owned()];
        for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
            let col = if i == 0 {
                format!("base_{}", sk_column(sk_type))
            } else {
                format!("base_{}", sk_column_n(i, sk_type))
            };
            base_key_cols.push(col);
        }

        // CONCURRENTLY cannot run inside a transaction, so execute on the pool.
        let sql = format!(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"{}\" ON {} ({})",
            idx_name,
            idx_table,
            base_key_cols.join(", ")
        );
        sqlx::query(&sql)
            .execute(data_pool)
            .await
            .map_err(|e| OpError::Internal(format!("Base key index on {idx_table}: {e}")))?;
    }
    Ok(())
}

/// Progress ledger for multi-table data code migrations: one row per
/// (migration, table) pair, committed in the same transaction as that table's
/// rewrite, so a re-run after a crash knows exactly which tables are done.
const CODE_MIGRATION_PROGRESS_TABLE: &str = "data_code_migration_progress";

/// Create the progress ledger if this database does not have one yet.
async fn ensure_progress_table(pool: &PgPool) -> OpResult<()> {
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {CODE_MIGRATION_PROGRESS_TABLE} (\
             migration_name TEXT NOT NULL, \
             table_name TEXT NOT NULL, \
             completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
             PRIMARY KEY (migration_name, table_name)\
         )"
    ))
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Create migration progress table: {e}")))?;
    Ok(())
}

/// Whether the progress ledger says `table` is already done for `migration`.
async fn is_table_marked(pool: &PgPool, migration: &str, table: &str) -> OpResult<bool> {
    let marked: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM {CODE_MIGRATION_PROGRESS_TABLE} \
         WHERE migration_name = $1 AND table_name = $2)"
    ))
    .bind(migration)
    .bind(table)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check migration progress: {e}")))?;
    Ok(marked)
}

/// One physical table's rewrite work for `004_escape_control_chars`: the text
/// key columns that get the single-statement SQL replace, and the jsonb
/// columns whose rows are re-encoded one by one.
struct EscapeWork {
    /// Bare (unquoted) table name, e.g. `_ddb_<id>` or `gsi_pending`.
    table: String,
    text_columns: Vec<String>,
    jsonb_columns: Vec<String>,
}

/// Rewrite work for a dynamically named `_ddb_*` table, read from
/// `information_schema.columns` so that exactly the sort key columns this
/// table has (`sk_s`, `sk<N>_s`, `base_sk<N>_s`, ...) are covered. Returns
/// `None` when the physical table does not exist (a catalog row mid-create or
/// mid-delete): such a table has no legacy rows to re-encode.
async fn dynamic_table_work(pool: &PgPool, table: String) -> OpResult<Option<EscapeWork>> {
    let text_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 AND data_type = 'text' \
           AND (column_name IN ('pk', 'base_pk') OR column_name::text ~ '^(base_)?sk[0-9]*_s$') \
         ORDER BY ordinal_position",
    )
    .bind(&table)
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("List text key columns of {table}: {e}")))?;

    let jsonb_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 AND data_type = 'jsonb' \
           AND column_name = 'item_data'",
    )
    .bind(&table)
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("List jsonb columns of {table}: {e}")))?;

    if text_columns.is_empty() && jsonb_columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(EscapeWork {
        table,
        text_columns,
        jsonb_columns,
    }))
}

/// Rewrite work for a fixed-name table, with the candidate columns filtered
/// through `information_schema.columns` so a column absent on this deployment
/// is skipped. Returns `None` when the table itself is absent.
async fn fixed_table_work(
    pool: &PgPool,
    table: &str,
    text_candidates: &[&str],
    jsonb_candidates: &[&str],
) -> OpResult<Option<EscapeWork>> {
    if !table_exists(pool, table).await? {
        return Ok(None);
    }
    let text_candidates: Vec<String> = text_candidates.iter().map(|c| (*c).to_owned()).collect();
    let jsonb_candidates: Vec<String> = jsonb_candidates.iter().map(|c| (*c).to_owned()).collect();
    let text_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 AND data_type = 'text' \
           AND column_name::text = ANY($2) \
         ORDER BY ordinal_position",
    )
    .bind(table)
    .bind(&text_candidates)
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("List text columns of {table}: {e}")))?;
    let jsonb_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 AND data_type = 'jsonb' \
           AND column_name::text = ANY($2) \
         ORDER BY ordinal_position",
    )
    .bind(table)
    .bind(&jsonb_candidates)
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("List jsonb columns of {table}: {e}")))?;
    Ok(Some(EscapeWork {
        table: table.to_owned(),
        text_columns,
        jsonb_columns,
    }))
}

/// The re-encoded form of a stored jsonb value, or `None` when re-encoding
/// does not change it. `escape_json_strings` only changes strings and object
/// keys containing U+0000 or U+0001, so a plain document and a document whose
/// strings merely contain the literal six-character text backslash-u-0001 both
/// come back equal and are skipped.
fn reencoded_json(value: &serde_json::Value) -> Option<serde_json::Value> {
    let escaped = extenddb_storage::util::escape_json_strings(value.clone());
    (escaped != *value).then_some(escaped)
}

/// Re-encode the rows of one jsonb column whose stored document contains
/// U+0001. Runs inside the caller's per-table transaction.
///
/// The candidate ctids are snapshotted once, before any row of this column is
/// touched, so no row is ever visited twice; the updates relocate rows (a ctid
/// names a physical row version), but always after that row's value was read.
/// A parameterized `DECLARE CURSOR` cannot be issued through the extended
/// protocol, so the locked-snapshot form of a streamed fetch is used instead,
/// and the values are then fetched in batches of 500 by ctid so a large table
/// never loads into memory. The matching set is tiny in practice; the table
/// scan, not the writes, is the cost.
async fn escape_jsonb_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    quoted_table: &str,
    col: &str,
) -> OpResult<u64> {
    // '\u0001' (six characters) is how jsonb::text renders a raw U+0001 inside
    // a string or key. It also matches a stored string whose content is a
    // literal backslash followed by u0001 (rendered '\\u0001'), which is
    // harmless: that row re-encodes to itself and is skipped below. jsonb text
    // rendering is why SQL-level replace() is not used here: it would corrupt
    // exactly that literal-backslash case. The backslash is spelled chr(92) so
    // the predicate reads the same under either value of
    // standard_conforming_strings; a plain '\u0001' literal would become the
    // single byte 0x01 with the setting off and match nothing.
    let select_ctids = format!(
        "SELECT ctid::text FROM {quoted_table} \
         WHERE position((chr(92) || 'u0001') IN \"{col}\"::text) > 0 FOR UPDATE"
    );
    let ctids: Vec<String> = sqlx::query_scalar(&select_ctids)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| OpError::Internal(format!("Find rows to re-encode in {quoted_table}: {e}")))?;

    let fetch_batch =
        format!("SELECT ctid::text, \"{col}\" FROM {quoted_table} WHERE ctid = ANY($1::tid[])");
    let update_row = format!("UPDATE {quoted_table} SET \"{col}\" = $1 WHERE ctid = $2::tid");
    let mut changed: u64 = 0;
    for batch in ctids.chunks(500) {
        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(&fetch_batch)
            .bind(batch.to_vec())
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| OpError::Internal(format!("Fetch rows from {quoted_table}: {e}")))?;
        for (ctid, value) in rows {
            if let Some(escaped) = reencoded_json(&value) {
                sqlx::query(&update_row)
                    .bind(escaped)
                    .bind(&ctid)
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| {
                        OpError::Internal(format!("Re-encode a row of {quoted_table}: {e}"))
                    })?;
                changed += 1;
            }
        }
    }
    Ok(changed)
}

/// Rewrite one table in one transaction and mark it done in the progress
/// ledger before committing, so the rewrite and the marker are atomic.
async fn escape_rows_in_table(pool: &PgPool, work: &EscapeWork) -> OpResult<u64> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| OpError::Internal(format!("Begin transaction for {}: {e}", work.table)))?;
    let quoted = format!("\"{}\"", work.table);
    let mut rewritten: u64 = 0;

    // Key text columns: TEXT has no escaping layer, so a single SQL statement
    // per column is exact. NULL columns never match (position() is NULL).
    for col in &work.text_columns {
        let sql = format!(
            "UPDATE {quoted} SET \"{col}\" = replace(\"{col}\", chr(1), chr(1) || chr(2)) \
             WHERE position(chr(1) IN \"{col}\") > 0"
        );
        let done = sqlx::query(&sql)
            .execute(&mut *tx)
            .await
            .map_err(|e| OpError::Internal(format!("Re-encode {}.{col}: {e}", work.table)))?;
        rewritten += done.rows_affected();
    }

    for col in &work.jsonb_columns {
        rewritten += escape_jsonb_rows(&mut tx, &quoted, col).await?;
    }

    sqlx::query(&format!(
        "INSERT INTO {CODE_MIGRATION_PROGRESS_TABLE} (migration_name, table_name) \
         VALUES ($1, $2) ON CONFLICT DO NOTHING"
    ))
    .bind("004_escape_control_chars")
    .bind(&work.table)
    .execute(&mut *tx)
    .await
    .map_err(|e| OpError::Internal(format!("Mark {} as re-encoded: {e}", work.table)))?;

    tx.commit()
        .await
        .map_err(|e| OpError::Internal(format!("Commit re-encode of {}: {e}", work.table)))?;
    Ok(rewritten)
}

/// Re-encode legacy rows that contain a raw U+0001 (`004_escape_control_chars`).
///
/// Strings are stored through the order-preserving escape in
/// `extenddb_storage::util::escape_control` (U+0000 becomes U+0001 U+0001,
/// U+0001 becomes U+0001 U+0002). Rows written before the escape existed hold
/// their strings raw. The decoder keeps a stray U+0001 literal, so the only
/// legacy rows it can misread are those where a raw U+0001 is immediately
/// followed by U+0001 or U+0002; rewriting every row containing U+0001 removes
/// that case. Legacy rows never contain U+0000 (PostgreSQL refused it), so for
/// legacy data applying the escape to the raw stored value is exactly the
/// re-encode, and rows without U+0001 encode to themselves and are left alone.
///
/// Crash safety. This migration is NOT idempotent at the row level: escaped
/// data also contains U+0001 (the first character of every escape pair), and a
/// legacy U+0001 U+0002 is indistinguishable from an escaped U+0001, so
/// applying the escape to an already-escaped row would double-escape it. A
/// partially rewritten table must therefore never be rescanned. Each table is
/// processed in one transaction: its matching rows are rewritten, a marker row
/// (migration_name, table_name) is inserted into the small progress table
/// created at the start of this migration, and the transaction commits. A
/// re-run after a crash skips tables that carry a marker and resumes with the
/// rest; when every table is marked, the caller records the migration in
/// `schema_history` exactly as `003` does, and the usual
/// `is_migration_applied` guard makes later runs no-ops. Marker rows are kept
/// forever: deleting them before the `schema_history` row is durable would
/// reopen the rescan window.
///
/// Rows written by THIS build are indistinguishable from legacy rows for the
/// same reason (an escaped U+0001 is stored as U+0001 U+0002), so the server
/// refuses to start on a data database that does not record this migration
/// (`PostgresEngine::check_data_migrations_applied`). The migration therefore
/// only ever sees rows written by older builds, provided every older server
/// is stopped before it runs; `extenddb init` records it on a fresh deployment. `backup_items` lives in the catalog database
/// while every other table lives in the data database, and a transaction
/// cannot span two databases, so each database gets its own progress table and
/// each marker commits with the table it marks.
async fn escape_legacy_control_chars(catalog_pool: &PgPool, data_pool: &PgPool) -> OpResult<()> {
    const NAME: &str = "004_escape_control_chars";
    ensure_progress_table(data_pool).await?;
    ensure_progress_table(catalog_pool).await?;

    let mut work: Vec<(&PgPool, EscapeWork)> = Vec::new();

    // Every data table, index table, and vector index table, enumerated from
    // the catalog. Their text key columns are read from information_schema so
    // single-sort-key, multi-sort-key, and base_* layouts are all covered.
    let table_ids: Vec<String> =
        sqlx::query_scalar("SELECT table_id FROM tables ORDER BY table_id")
            .fetch_all(catalog_pool)
            .await
            .map_err(|e| OpError::Internal(format!("Enumerate tables: {e}")))?;
    for id in table_ids {
        if let Some(w) = dynamic_table_work(data_pool, format!("_ddb_{id}")).await? {
            work.push((data_pool, w));
        }
    }
    let index_ids: Vec<String> =
        sqlx::query_scalar("SELECT index_id FROM indexes ORDER BY index_id")
            .fetch_all(catalog_pool)
            .await
            .map_err(|e| OpError::Internal(format!("Enumerate indexes: {e}")))?;
    for id in index_ids {
        if let Some(w) = dynamic_table_work(data_pool, format!("_ddb_{id}")).await? {
            work.push((data_pool, w));
        }
    }
    let vector_ids: Vec<String> =
        sqlx::query_scalar("SELECT index_id FROM vector_indexes ORDER BY index_id")
            .fetch_all(catalog_pool)
            .await
            .map_err(|e| OpError::Internal(format!("Enumerate vector indexes: {e}")))?;
    for id in vector_ids {
        if let Some(w) = dynamic_table_work(data_pool, format!("_ddb_vec_{id}")).await? {
            work.push((data_pool, w));
        }
    }

    // Fixed tables holding item documents. gsi_pending and stream_records live
    // in the data database; backup_items lives in the catalog database.
    if let Some(w) = fixed_table_work(
        data_pool,
        "gsi_pending",
        &[],
        &["old_item", "new_item", "index_context"],
    )
    .await?
    {
        work.push((data_pool, w));
    }
    if let Some(w) = fixed_table_work(data_pool, "stream_records", &[], &["record_data"]).await? {
        work.push((data_pool, w));
    }
    if let Some(w) =
        fixed_table_work(catalog_pool, "backup_items", &["pk", "sk"], &["item_data"]).await?
    {
        work.push((catalog_pool, w));
    }

    for (pool, w) in &work {
        if is_table_marked(pool, NAME, &w.table).await? {
            println!("      {}: already re-encoded, skipping.", w.table);
            continue;
        }
        let rewritten = escape_rows_in_table(pool, w).await?;
        println!("      {}: {rewritten} row update(s)", w.table);
    }
    Ok(())
}

/// Check if a table exists in the public schema.
pub(crate) async fn table_exists(pool: &PgPool, name: &str) -> OpResult<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_name = $1 AND table_schema = 'public')",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check table exists: {e}")))?;
    Ok(exists)
}

/// Filenames of [`DATA_MIGRATIONS`] not yet applied to this data database.
///
/// Mirrors the apply logic in [`run_data_migrations`] without executing
/// anything, so callers (e.g. `extenddb migrate`) can report and gate on
/// pending work. A pre-tracking baseline (`001_data_schema.sql` whose tables
/// already exist but isn't recorded) is treated as already applied: it will be
/// adopted — recorded without re-running — not applied, so it is not reported
/// as pending.
pub(crate) async fn pending_data_migrations(pool: &PgPool) -> OpResult<Vec<String>> {
    let has_history = table_exists(pool, "schema_history").await?;
    // Pre-tracking deployment: 001 ran under an earlier version (its tables
    // exist) but was never recorded. It is adopted, not re-run.
    let adopts_baseline = !has_history && table_exists(pool, "stream_shards").await?;

    let mut pending = Vec::new();
    for (filename, _sql) in DATA_MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            continue;
        }
        if *filename == "001_data_schema.sql" && adopts_baseline {
            continue;
        }
        pending.push((*filename).to_owned());
    }
    // Code migrations are tracked in the same data-database ledger.
    for name in DATA_CODE_MIGRATIONS {
        if !is_migration_applied(pool, name).await? {
            pending.push((*name).to_owned());
        }
    }
    Ok(pending)
}

/// Check if a migration has already been applied.
async fn is_migration_applied(pool: &PgPool, filename: &str) -> OpResult<bool> {
    if table_exists(pool, "schema_history").await? {
        let applied: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM schema_history WHERE filename = $1)")
                .bind(filename)
                .fetch_one(pool)
                .await
                .map_err(|e| OpError::Internal(format!("Check migration: {e}")))?;
        return Ok(applied.0);
    }
    Ok(false)
}

/// Record a migration in the `schema_history` table.
async fn record_migration(pool: &PgPool, filename: &str) -> OpResult<()> {
    if !table_exists(pool, "schema_history").await? {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO schema_history (filename) VALUES ($1) ON CONFLICT (filename) DO NOTHING",
    )
    .bind(filename)
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Record migration: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::CATALOG_MIGRATIONS;
    use crate::CATALOG_VERSION;

    /// `reencoded_json` is what decides whether a stored jsonb row is
    /// rewritten by `004_escape_control_chars`, so it must change exactly the
    /// rows holding a raw control character and nothing else.
    #[test]
    fn reencoded_json_changes_exactly_the_rows_holding_raw_control_chars() {
        use serde_json::json;

        // Raw U+0001 in a string value and in an object key: rewritten, with
        // each U+0001 becoming U+0001 U+0002 wherever it appears.
        let raw = json!({"k\u{1}": {"S": "v\u{1}"}, "plain": {"N": "1"}});
        let expected = json!({"k\u{1}\u{2}": {"S": "v\u{1}\u{2}"}, "plain": {"N": "1"}});
        assert_eq!(super::reencoded_json(&raw), Some(expected));

        // A string whose content is the literal six-character text
        // backslash-u-0001: the SQL candidate filter over-matches it, and this
        // check is what keeps it byte-identical.
        let literal = json!({"note": {"S": "x\\u0001y"}});
        assert_eq!(super::reencoded_json(&literal), None);

        // A plain document: untouched.
        let plain = json!({"pk": {"S": "p"}, "m": {"M": {"k": {"S": "v"}}}});
        assert_eq!(super::reencoded_json(&plain), None);
    }

    /// The catalog version and the migration list must move together.
    ///
    /// A migration that creates its schema without moving the version leaves a
    /// deployment the binary refuses to serve; moving the version without a
    /// migration leaves one that cannot reach it. Both are caught today only by a
    /// test that needs a live PostgreSQL and a built binary, so this is the
    /// tripwire that fires in an ordinary `cargo test`: adding a migration file
    /// breaks the count, which forces a decision about the version.
    #[test]
    fn the_migration_count_and_the_catalog_version_agree() {
        assert_eq!(
            CATALOG_MIGRATIONS.len(),
            4,
            "a catalog migration was added or removed; update CATALOG_VERSION and this count"
        );
        assert_eq!(CATALOG_VERSION.to_string(), "0.0.5");
    }

    /// The version the binary expects must be the version the schema writes.
    ///
    /// These two live in different languages and different files, so nothing but
    /// a check like this ties them together. Without it a version bump that
    /// forgets the SQL side produces a deployment that migrates "successfully"
    /// and then refuses to start.
    #[test]
    fn the_last_migration_writes_the_expected_catalog_version() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .last()
            .expect("there is at least one catalog migration");
        let expected = format!("'{}'", CATALOG_VERSION);
        assert!(
            sql.contains("catalog_version") && sql.contains(&expected),
            "{filename} must set catalog_version to {expected}"
        );
    }

    /// Each migration is registered under the filename it is stored as.
    ///
    /// The ledger keys on this string, so a mismatch between the registered name
    /// and the file would record one name and look for another, and the migration
    /// would be applied again on every run.
    #[test]
    fn every_migration_is_registered_under_a_sql_filename_with_its_own_contents() {
        for (filename, sql) in CATALOG_MIGRATIONS {
            assert!(filename.ends_with(".sql"), "{filename}");
            assert!(!sql.trim().is_empty(), "{filename} is empty");
        }
        // The failure this guards is a copy-pasted `include_str!` that points one
        // entry at another file's bytes: the ledger would then record one name
        // while the SQL of another ran, and the missed migration would be applied
        // again on every upgrade. Two entries sharing contents is what that looks
        // like, so distinctness is the assertion that delivers the rationale.
        for (i, (left_name, left_sql)) in CATALOG_MIGRATIONS.iter().enumerate() {
            for (right_name, right_sql) in &CATALOG_MIGRATIONS[i + 1..] {
                assert_ne!(
                    left_sql, right_sql,
                    "{left_name} and {right_name} embed identical SQL; check their include_str! paths"
                );
            }
        }
    }

    /// The fail-closed catalog guard (`converge_catalog_version`, via
    /// `check_stored_catalog_version`) against a live catalog: a stored
    /// version this binary cannot parse, or a newer one, is refused and left
    /// exactly as stored; an older one is moved up and an equal one kept.
    /// Needs `EXTENDDB_TEST_PG_CONNECTION_STRING`; reports a skip without it.
    #[tokio::test]
    async fn converge_refuses_an_unreadable_or_newer_catalog_version_and_leaves_it() {
        let Some(base) = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING")
            .ok()
            .filter(|s| !s.trim().is_empty())
        else {
            eprintln!(
                "SKIP converge_refuses_an_unreadable_or_newer_catalog_version: no PostgreSQL"
            );
            return;
        };
        let base = base.trim_end_matches('/').to_owned();
        let name = format!("eddb_conv_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{base}/postgres"))
            .await
            .expect("admin");
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&admin)
            .await
            .expect("create scratch");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{base}/{name}"))
            .await
            .expect("scratch");
        sqlx::query("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            .execute(&pool)
            .await
            .expect("settings");

        let compiled = CATALOG_VERSION.to_string();
        // (stored, expect Ok, stored value afterwards)
        let cases: [(Option<&str>, bool, Option<&str>); 5] = [
            (Some("garbage"), false, Some("garbage")),
            (Some(""), false, Some("")),
            (Some("9.9.9"), false, Some("9.9.9")),
            (Some("0.0.3"), true, Some(compiled.as_str())),
            (Some(compiled.as_str()), true, Some(compiled.as_str())),
        ];
        let mut outcomes = Vec::new();
        for (stored, _, _) in &cases {
            sqlx::query("DELETE FROM settings")
                .execute(&pool)
                .await
                .expect("clear");
            if let Some(v) = stored {
                sqlx::query("INSERT INTO settings (key, value) VALUES ('catalog_version', $1)")
                    .bind(v)
                    .execute(&pool)
                    .await
                    .expect("seed");
            }
            let result = super::converge_catalog_version(&pool).await;
            let after: Option<String> =
                sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_version'")
                    .fetch_optional(&pool)
                    .await
                    .expect("read back");
            outcomes.push((result.map_err(|e| format!("{e:?}")), after));
        }
        // Absent: proceeds, and (the write is an UPDATE) stays absent.
        sqlx::query("DELETE FROM settings")
            .execute(&pool)
            .await
            .expect("clear");
        let absent = super::converge_catalog_version(&pool).await.is_ok();

        pool.close().await;
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
            .execute(&admin)
            .await;
        admin.close().await;

        for ((stored, ok, want_after), (result, after)) in cases.iter().zip(&outcomes) {
            assert_eq!(result.is_ok(), *ok, "stored {stored:?}: {result:?}");
            assert_eq!(
                after.as_deref(),
                *want_after,
                "stored {stored:?} afterwards"
            );
        }
        let garbage = outcomes[0].0.as_ref().expect_err("garbage refused");
        assert!(garbage.contains("refusing to overwrite"), "{garbage}");
        let newer = outcomes[2].0.as_ref().expect_err("newer refused");
        assert!(newer.contains("newer than this binary"), "{newer}");
        assert!(absent, "an absent version proceeds");
    }
}

#[cfg(test)]
mod live_reencode {
    //! Live checks for `004_escape_control_chars` against a scratch database.
    //!
    //! Follows the convention of `tests/key_collation.rs`: each test needs
    //! `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL with no database name,
    //! for example `postgresql://postgres:postgres@127.0.0.1:5432`), builds a
    //! throwaway database, and drops it when it passes. Without the variable
    //! every test here reports a skip and passes. One database serves as both
    //! catalog and data, as the storage-level tests do.

    use serde_json::json;
    use sqlx::PgPool;
    use sqlx::postgres::PgPoolOptions;

    use super::{
        CATALOG_MIGRATIONS, DATA_MIGRATIONS, IGNORE_CONNECTIONS_ENV, MIGRATE_APPLICATION_NAME,
        needs_exclusive_access, pending_data_migrations, refuse_if_other_clients_connected,
        refuse_if_other_clients_connected_anywhere, run_data_code_migrations,
        unapplied_required_data_migrations,
    };

    struct Scratch {
        db: PgPool,
        admin: PgPool,
        db_name: String,
    }

    impl Scratch {
        async fn cleanup(self) {
            let Scratch { db, admin, db_name } = self;
            db.close().await;
            sqlx::query(&format!(
                "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
            ))
            .execute(&admin)
            .await
            .expect("drop the scratch database");
            admin.close().await;
        }
    }

    fn base_conn() -> Option<String> {
        let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
        (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
    }

    fn skip(test: &str) {
        eprintln!(
            "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no \
             PostgreSQL to build a scratch database in."
        );
    }

    async fn scratch() -> Scratch {
        let base = base_conn().expect("caller checks base_conn() first");
        let db_name = format!("eddb_esc_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{base}/postgres"))
            .await
            .expect("connect to the postgres maintenance database");
        sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
            .execute(&admin)
            .await
            .expect("create the scratch database");
        // Named like the migrate command, so the connection guard on 004 treats
        // this pool as the migration's own.
        let db = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(
                format!("{base}/{db_name}")
                    .parse::<sqlx::postgres::PgConnectOptions>()
                    .expect("scratch connection string")
                    .application_name(MIGRATE_APPLICATION_NAME),
            )
            .await
            .expect("connect to the scratch database");
        for (_, sql) in CATALOG_MIGRATIONS.iter().chain(DATA_MIGRATIONS) {
            sqlx::raw_sql(sql)
                .execute(&db)
                .await
                .expect("apply a shipped migration");
        }
        // 003 creates its indexes from parsed catalog key schemas and is not
        // what these tests exercise, so record it as already applied.
        sqlx::query("INSERT INTO schema_history (filename) VALUES ('003_gsi_base_key_index')")
            .execute(&db)
            .await
            .expect("record 003 as applied");
        Scratch { db, admin, db_name }
    }

    /// Seed a data table, an index table, `gsi_pending`, `stream_records`, and
    /// `backup_items` with three row kinds: raw U+0001 in key columns and
    /// inside jsonb strings and object keys, the literal six-character text
    /// backslash-u-0001 inside a jsonb string, and plain rows.
    async fn seed(db: &PgPool) {
        let ks = json!([
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ]);
        let ad = json!([
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"}
        ]);
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
            .bind("111122223333")
            .bind("esc-check")
            .execute(db)
            .await
            .expect("seed the account row");
        sqlx::query(
            "INSERT INTO tables (account_id, table_name, key_schema, attribute_definitions, \
             table_status, table_arn, table_id) \
             VALUES ($1, 't', $2, $3, 'ACTIVE', \
             'arn:aws:dynamodb:us-east-1:111122223333:table/t', 't1')",
        )
        .bind("111122223333")
        .bind(&ks)
        .bind(&ad)
        .execute(db)
        .await
        .expect("seed the tables row");
        sqlx::query(
            "INSERT INTO indexes (table_id, index_id, index_name, index_type, key_schema, \
             projection) VALUES ('t1', 'i1', 'gsi1', 'GSI', $1, $2)",
        )
        .bind(json!([{"AttributeName": "gpk", "KeyType": "HASH"}]))
        .bind(json!({"ProjectionType": "ALL"}))
        .execute(db)
        .await
        .expect("seed the indexes row");

        sqlx::raw_sql(
            r#"CREATE TABLE "_ddb_t1" (
                pk TEXT COLLATE "C" NOT NULL,
                sk_s TEXT COLLATE "C",
                sk_n NUMERIC,
                sk_b BYTEA,
                item_data JSONB NOT NULL,
                PRIMARY KEY (pk, sk_s)
            );
            CREATE TABLE "_ddb_i1" (
                pk TEXT COLLATE "C" NOT NULL,
                sk_s TEXT COLLATE "C",
                sk_n NUMERIC,
                sk_b BYTEA,
                base_pk TEXT COLLATE "C" NOT NULL,
                base_sk_s TEXT COLLATE "C",
                base_sk_n NUMERIC,
                base_sk_b BYTEA,
                item_data JSONB NOT NULL,
                PRIMARY KEY (pk, base_pk, base_sk_s)
            );"#,
        )
        .execute(db)
        .await
        .expect("create the physical data and index tables");

        for (pk, sk, item) in [
            (
                "a\u{1}b",
                "s\u{1}",
                json!({"pk": {"S": "a\u{1}b"}, "k\u{1}": {"S": "v\u{1}"}}),
            ),
            ("lit", "s", json!({"note": {"S": "x\\u0001y"}})),
            ("plain", "s", json!({"pk": {"S": "plain"}})),
        ] {
            sqlx::query("INSERT INTO \"_ddb_t1\" (pk, sk_s, item_data) VALUES ($1, $2, $3)")
                .bind(pk)
                .bind(sk)
                .bind(item)
                .execute(db)
                .await
                .expect("seed a data table row");
        }
        for (pk, sk, base_pk, base_sk, item) in [
            (
                "g\u{1}",
                "gs\u{1}",
                "a\u{1}b",
                "s\u{1}",
                json!({"gpk": {"S": "g\u{1}"}}),
            ),
            ("zz", "zs", "plain", "s", json!({"gpk": {"S": "zz"}})),
        ] {
            sqlx::query(
                "INSERT INTO \"_ddb_i1\" (pk, sk_s, base_pk, base_sk_s, item_data) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(pk)
            .bind(sk)
            .bind(base_pk)
            .bind(base_sk)
            .bind(item)
            .execute(db)
            .await
            .expect("seed an index table row");
        }

        for (new_item, context) in [
            (json!({"pk": {"S": "a\u{1}b"}}), json!({"k\u{1}": 1})),
            (json!({"pk": {"S": "p"}}), json!({"k": 1})),
        ] {
            sqlx::query(
                "INSERT INTO gsi_pending (table_id, worker_partition, old_item, new_item, \
                 index_context) VALUES ('t1', 0, NULL, $1, $2)",
            )
            .bind(new_item)
            .bind(context)
            .execute(db)
            .await
            .expect("seed a gsi_pending row");
        }

        sqlx::query(
            "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
             VALUES ('shard-1', 't1', '1')",
        )
        .execute(db)
        .await
        .expect("seed the stream shard");
        for (seq, record) in [
            ("1", json!({"Keys": {"pk": {"S": "a\u{1}b"}}})),
            ("2", json!({"Keys": {"pk": {"S": "p"}}})),
        ] {
            sqlx::query(
                "INSERT INTO stream_records (shard_id, sequence_number, table_id, event_name, \
                 record_data) VALUES ('shard-1', $1, 't1', 'INSERT', $2)",
            )
            .bind(seq)
            .bind(record)
            .execute(db)
            .await
            .expect("seed a stream record");
        }

        sqlx::query(
            "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, \
             key_schema, attribute_definitions) \
             VALUES ('arn:aws:dynamodb:us-east-1:111122223333:table/t/backup/b1', 'b1', 't1', \
             't', '111122223333', $1, $2)",
        )
        .bind(&ks)
        .bind(&ad)
        .execute(db)
        .await
        .expect("seed the backups row");
        for (pk, sk, item) in [
            ("p\u{1}", Some("s\u{1}"), json!({"pk": {"S": "p\u{1}"}})),
            ("plain", None, json!({"pk": {"S": "plain"}})),
        ] {
            sqlx::query(
                "INSERT INTO backup_items (backup_arn, pk, sk, item_data) \
                 VALUES ('arn:aws:dynamodb:us-east-1:111122223333:table/t/backup/b1', $1, $2, $3)",
            )
            .bind(pk)
            .bind(sk)
            .bind(item)
            .execute(db)
            .await
            .expect("seed a backup item");
        }
    }

    #[tokio::test]
    async fn reencodes_legacy_rows_once_and_records_itself() {
        if base_conn().is_none() {
            skip("reencodes_legacy_rows_once_and_records_itself");
            return;
        }
        let s = scratch().await;
        seed(&s.db).await;

        let pending = pending_data_migrations(&s.db).await.expect("pending list");
        assert!(
            pending.iter().any(|p| p == "004_escape_control_chars"),
            "004 must be pending before the run: {pending:?}"
        );

        run_data_code_migrations(&s.db, &s.db)
            .await
            .expect("run the data code migrations");

        // Data table: raw U+0001 became U+0001 U+0002 in the key columns and
        // inside every jsonb string and object key; the literal-backslash row
        // and the plain row are byte-identical.
        let rows: Vec<(String, String, serde_json::Value)> =
            sqlx::query_as("SELECT pk, sk_s, item_data FROM \"_ddb_t1\" ORDER BY pk")
                .fetch_all(&s.db)
                .await
                .expect("read the data table");
        assert_eq!(
            rows,
            vec![
                (
                    "a\u{1}\u{2}b".to_owned(),
                    "s\u{1}\u{2}".to_owned(),
                    json!({"pk": {"S": "a\u{1}\u{2}b"}, "k\u{1}\u{2}": {"S": "v\u{1}\u{2}"}}),
                ),
                (
                    "lit".to_owned(),
                    "s".to_owned(),
                    json!({"note": {"S": "x\\u0001y"}}),
                ),
                (
                    "plain".to_owned(),
                    "s".to_owned(),
                    json!({"pk": {"S": "plain"}}),
                ),
            ]
        );

        // Index table: pk, sk_s, base_pk, base_sk_s and item_data all re-encoded.
        let rows: Vec<(String, String, String, String, serde_json::Value)> = sqlx::query_as(
            "SELECT pk, sk_s, base_pk, base_sk_s, item_data FROM \"_ddb_i1\" ORDER BY pk",
        )
        .fetch_all(&s.db)
        .await
        .expect("read the index table");
        assert_eq!(
            rows,
            vec![
                (
                    "g\u{1}\u{2}".to_owned(),
                    "gs\u{1}\u{2}".to_owned(),
                    "a\u{1}\u{2}b".to_owned(),
                    "s\u{1}\u{2}".to_owned(),
                    json!({"gpk": {"S": "g\u{1}\u{2}"}}),
                ),
                (
                    "zz".to_owned(),
                    "zs".to_owned(),
                    "plain".to_owned(),
                    "s".to_owned(),
                    json!({"gpk": {"S": "zz"}}),
                ),
            ]
        );

        // gsi_pending: NULL old_item stays NULL, the raw row is re-encoded in
        // both jsonb columns, the plain row is untouched.
        let rows: Vec<(
            Option<serde_json::Value>,
            serde_json::Value,
            serde_json::Value,
        )> =
            sqlx::query_as("SELECT old_item, new_item, index_context FROM gsi_pending ORDER BY id")
                .fetch_all(&s.db)
                .await
                .expect("read gsi_pending");
        assert_eq!(
            rows,
            vec![
                (
                    None,
                    json!({"pk": {"S": "a\u{1}\u{2}b"}}),
                    json!({"k\u{1}\u{2}": 1}),
                ),
                (None, json!({"pk": {"S": "p"}}), json!({"k": 1})),
            ]
        );

        // stream_records.
        let rows: Vec<(serde_json::Value,)> =
            sqlx::query_as("SELECT record_data FROM stream_records ORDER BY sequence_number")
                .fetch_all(&s.db)
                .await
                .expect("read stream_records");
        assert_eq!(
            rows,
            vec![
                (json!({"Keys": {"pk": {"S": "a\u{1}\u{2}b"}}}),),
                (json!({"Keys": {"pk": {"S": "p"}}}),),
            ]
        );

        // backup_items, including the NULL sk row.
        let rows: Vec<(String, Option<String>, serde_json::Value)> =
            sqlx::query_as("SELECT pk, sk, item_data FROM backup_items ORDER BY pk")
                .fetch_all(&s.db)
                .await
                .expect("read backup_items");
        assert_eq!(
            rows,
            vec![
                (
                    "p\u{1}\u{2}".to_owned(),
                    Some("s\u{1}\u{2}".to_owned()),
                    json!({"pk": {"S": "p\u{1}\u{2}"}}),
                ),
                ("plain".to_owned(), None, json!({"pk": {"S": "plain"}}),),
            ]
        );

        // Recorded in schema_history, one progress marker per table, and no
        // longer pending.
        let recorded: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM schema_history \
             WHERE filename = '004_escape_control_chars')",
        )
        .fetch_one(&s.db)
        .await
        .expect("read schema_history");
        assert!(recorded, "schema_history must record the migration");
        let markers: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM data_code_migration_progress \
             WHERE migration_name = '004_escape_control_chars'",
        )
        .fetch_one(&s.db)
        .await
        .expect("count progress markers");
        assert_eq!(markers, 5, "one marker per processed table");
        let pending = pending_data_migrations(&s.db).await.expect("pending list");
        assert!(
            !pending.iter().any(|p| p == "004_escape_control_chars"),
            "004 must not be pending after the run: {pending:?}"
        );

        // A second run is skipped by the is_migration_applied guard and does
        // not double-escape anything.
        run_data_code_migrations(&s.db, &s.db)
            .await
            .expect("run the data code migrations again");
        let (pk, item): (String, serde_json::Value) =
            sqlx::query_as("SELECT pk, item_data FROM \"_ddb_t1\" WHERE sk_s = $1")
                .bind("s\u{1}\u{2}")
                .fetch_one(&s.db)
                .await
                .expect("re-read the re-encoded row");
        assert_eq!(pk, "a\u{1}\u{2}b");
        assert_eq!(
            item,
            json!({"pk": {"S": "a\u{1}\u{2}b"}, "k\u{1}\u{2}": {"S": "v\u{1}\u{2}"}})
        );

        s.cleanup().await;
    }

    /// The jsonb candidate predicate must not depend on the session's
    /// standard_conforming_strings: with it off, a plain '\u0001' literal is a
    /// single 0x01 byte that never appears in jsonb text and matches nothing,
    /// so every jsonb row would be skipped while the migration recorded itself.
    #[tokio::test]
    async fn reencodes_jsonb_rows_with_standard_conforming_strings_off() {
        if base_conn().is_none() {
            skip("reencodes_jsonb_rows_with_standard_conforming_strings_off");
            return;
        }
        let s = scratch().await;
        seed(&s.db).await;

        let base = base_conn().unwrap();
        let scs_off = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::Executor::execute(&mut *conn, "SET standard_conforming_strings = off")
                        .await?;
                    Ok(())
                })
            })
            .connect_with(
                format!("{base}/{}", s.db_name)
                    .parse::<sqlx::postgres::PgConnectOptions>()
                    .unwrap()
                    .application_name(MIGRATE_APPLICATION_NAME),
            )
            .await
            .expect("connect with standard_conforming_strings off");

        run_data_code_migrations(&scs_off, &scs_off)
            .await
            .expect("run 004");

        let (pk, sk, item): (String, String, serde_json::Value) =
            sqlx::query_as("SELECT pk, sk_s, item_data FROM \"_ddb_t1\" WHERE pk LIKE 'a%'")
                .fetch_one(&s.db)
                .await
                .unwrap();
        assert_eq!(pk, "a\u{1}\u{2}b");
        assert_eq!(sk, "s\u{1}\u{2}");
        assert_eq!(
            item,
            json!({"pk": {"S": "a\u{1}\u{2}b"}, "k\u{1}\u{2}": {"S": "v\u{1}\u{2}"}}),
            "jsonb rows must be re-encoded whatever the session setting"
        );
        scs_off.close().await;
        s.cleanup().await;
    }

    /// The server must not serve a data database that has not been through
    /// 004: rows it wrote through the escape would be rewritten by the
    /// migration later. The required list is empty only once 004 is recorded.
    #[tokio::test]
    async fn required_migrations_are_reported_until_004_is_recorded() {
        if base_conn().is_none() {
            skip("required_migrations_are_reported_until_004_is_recorded");
            return;
        }
        let s = scratch().await;
        seed(&s.db).await;

        let missing = unapplied_required_data_migrations(&s.db).await.unwrap();
        assert_eq!(missing, vec!["004_escape_control_chars".to_owned()]);

        run_data_code_migrations(&s.db, &s.db)
            .await
            .expect("run 004");

        let missing = unapplied_required_data_migrations(&s.db).await.unwrap();
        assert!(missing.is_empty(), "{missing:?}");
        s.cleanup().await;
    }

    /// A server (or any other session) still connected to the data database
    /// blocks 004; the migrate command's own connections do not.
    #[tokio::test]
    async fn refuses_004_while_another_client_is_connected() {
        if base_conn().is_none() {
            skip("refuses_004_while_another_client_is_connected");
            return;
        }
        let s = scratch().await;
        seed(&s.db).await;
        let base = base_conn().unwrap();

        // An unnamed pool stands in for a running server.
        let server_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{base}/{}", s.db_name))
            .await
            .expect("connect as a server would");
        sqlx::query("SELECT 1").execute(&server_pool).await.unwrap();

        let err = refuse_if_other_clients_connected(&s.db, "data")
            .await
            .expect_err("an unnamed connection must block the migration");
        let text = format!("{err:?}");
        assert!(
            text.contains("other connection(s) hold the data database"),
            "{text}"
        );
        assert!(text.contains(IGNORE_CONNECTIONS_ENV), "{text}");
        let err = run_data_code_migrations(&s.db, &s.db)
            .await
            .expect_err("004 must not run while the server is connected");
        assert!(format!("{err:?}").contains("other connection(s)"));
        assert!(
            unapplied_required_data_migrations(&s.db)
                .await
                .unwrap()
                .len()
                == 1,
            "nothing may be recorded when the guard refuses"
        );

        // Server stopped: only the migrate command's own connections remain.
        server_pool.close().await;
        refuse_if_other_clients_connected(&s.db, "data")
            .await
            .expect("no foreign connection left");
        run_data_code_migrations(&s.db, &s.db)
            .await
            .expect("run 004");
        s.cleanup().await;
    }

    /// With the catalog in its own database, a session holding only that
    /// database blocks 004 as well: the migration rewrites `backup_items`
    /// there. The refusal names the catalog database, and the check is
    /// skipped once the migration is recorded.
    #[tokio::test]
    async fn refuses_004_while_another_client_holds_a_separate_catalog() {
        if base_conn().is_none() {
            skip("refuses_004_while_another_client_holds_a_separate_catalog");
            return;
        }
        let data = scratch().await;
        let catalog = scratch().await;
        seed(&data.db).await;
        let base = base_conn().unwrap();

        // Nothing but the migrate command's own connections on either side.
        refuse_if_other_clients_connected_anywhere(&catalog.db, &data.db)
            .await
            .expect("both databases are free");
        assert!(
            needs_exclusive_access(&data.db).await.unwrap(),
            "004 is pending, so exclusive access is required"
        );

        // A session on the catalog database alone.
        let catalog_client = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{base}/{}", catalog.db_name))
            .await
            .expect("connect to the catalog as a server would");
        sqlx::query("SELECT 1")
            .execute(&catalog_client)
            .await
            .unwrap();

        refuse_if_other_clients_connected(&data.db, "data")
            .await
            .expect("the data database is still free");
        let err = refuse_if_other_clients_connected_anywhere(&catalog.db, &data.db)
            .await
            .expect_err("a catalog-only session must block the migration");
        let text = format!("{err:?}");
        assert!(
            text.contains("other connection(s) hold the catalog database"),
            "{text}"
        );
        let err = run_data_code_migrations(&catalog.db, &data.db)
            .await
            .expect_err("004 must not run while the catalog is held");
        assert!(format!("{err:?}").contains("hold the catalog database"));
        assert_eq!(
            unapplied_required_data_migrations(&data.db)
                .await
                .unwrap()
                .len(),
            1,
            "nothing may be recorded when the guard refuses"
        );

        catalog_client.close().await;
        run_data_code_migrations(&catalog.db, &data.db)
            .await
            .expect("run 004 once the catalog is free");
        assert!(
            !needs_exclusive_access(&data.db).await.unwrap(),
            "a recorded 004 needs no exclusive access"
        );

        data.cleanup().await;
        catalog.cleanup().await;
    }

    /// The migrate command runs the guard as the application role, which is
    /// not a superuser, and `pg_stat_activity` hides `backend_type` for other
    /// roles' sessions from it. A session held by another role (an operator's
    /// `psql`, a server under a different role) must still count.
    #[tokio::test]
    async fn guard_counts_other_roles_sessions_from_a_non_superuser_pool() {
        if base_conn().is_none() {
            skip("guard_counts_other_roles_sessions_from_a_non_superuser_pool");
            return;
        }
        let s = scratch().await;
        let base = base_conn().unwrap();
        let role = format!(
            "eddb_guard_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        sqlx::query(&format!(
            "CREATE ROLE \"{role}\" LOGIN PASSWORD 'guard-test' NOSUPERUSER"
        ))
        .execute(&s.admin)
        .await
        .expect("create a plain login role");

        // The migrate command's view: the plain role, named as the migration.
        let opts = format!("{base}/{}", s.db_name)
            .parse::<sqlx::postgres::PgConnectOptions>()
            .expect("scratch connection string")
            .username(&role)
            .password("guard-test")
            .application_name(MIGRATE_APPLICATION_NAME);
        let migrate_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .expect("connect as the plain role");

        // Only the superuser scratch pool is connected, and it is named as the
        // migration too, so nothing counts.
        refuse_if_other_clients_connected(&migrate_pool, "data")
            .await
            .expect("no foreign session yet");

        // A superuser session with no application name, as an operator's psql
        // would be. The plain role sees its datname and usename but not its
        // backend_type.
        let holder = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{base}/{}", s.db_name))
            .await
            .expect("connect as another role would");
        sqlx::query("SELECT 1").execute(&holder).await.unwrap();
        let hidden: Option<String> = sqlx::query_scalar(
            "SELECT backend_type FROM pg_stat_activity \
             WHERE datname = current_database() AND usename <> current_user LIMIT 1",
        )
        .fetch_one(&migrate_pool)
        .await
        .expect("read another role's row");
        assert!(
            hidden.is_none(),
            "this test needs a role that cannot see other sessions' backend_type"
        );

        let err = refuse_if_other_clients_connected(&migrate_pool, "data")
            .await
            .expect_err("another role's session must count");
        let text = format!("{err:?}");
        assert!(text.contains("hold the data database"), "{text}");
        assert!(text.contains("no client address shown"), "{text}");

        holder.close().await;
        refuse_if_other_clients_connected(&migrate_pool, "data")
            .await
            .expect("the other role's session is gone");

        migrate_pool.close().await;
        sqlx::query(&format!("DROP ROLE \"{role}\""))
            .execute(&s.admin)
            .await
            .expect("drop the test role");
        s.cleanup().await;
    }
}
