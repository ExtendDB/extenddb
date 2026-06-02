// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB schema migration helpers for catalog and data databases.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::{MySqlConnection, MySqlPool};

use crate::data::{
    DATA_TABLE_METADATA_LIKE_CLAUSE, DATA_TABLE_PARTITIONS, DYNAMODB_HASH_KEY_COLUMN_BYTES,
    DYNAMODB_HASH_KEY_COLUMN_TYPE, NATIVE_INDEX_GENERATED_PK_COLUMN_METADATA_LIKE_CLAUSE,
    NATIVE_INDEX_METADATA_LIKE_CLAUSE, USER_TABLE_FULL_KEYSPACE_SPLITS_MIGRATION,
    user_data_table_region_split_sqls,
};
use crate::table_attributes::{
    fixed_table_region_merge_option_sqls, user_data_table_region_merge_option_sqls,
};

/// Embedded catalog migration files, applied in order.
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_schema.sql",
        include_str!("../../storage-tidb/migrations/001_schema.sql"),
    ),
    (
        "002_backup_metadata_fidelity.sql",
        include_str!("../../storage-tidb/migrations/002_backup_metadata_fidelity.sql"),
    ),
    (
        "003_drop_catalog_stream_data.sql",
        include_str!("../../storage-tidb/migrations/003_drop_catalog_stream_data.sql"),
    ),
    (
        "004_control_plane_leases.sql",
        include_str!("../../storage-tidb/migrations/004_control_plane_leases.sql"),
    ),
    (
        "006_native_ttl_mode.sql",
        include_str!("../../storage-tidb/migrations/006_native_ttl_mode.sql"),
    ),
    (
        "007_native_br_backups.sql",
        include_str!("../../storage-tidb/migrations/007_native_br_backups.sql"),
    ),
    (
        "008_native_index_backup_ids.sql",
        include_str!("../../storage-tidb/migrations/008_native_index_backup_ids.sql"),
    ),
    (
        "009_catalog_native_ttl.sql",
        include_str!("../../storage-tidb/migrations/009_catalog_native_ttl.sql"),
    ),
    (
        "010_session_native_ttl.sql",
        include_str!("../../storage-tidb/migrations/010_session_native_ttl.sql"),
    ),
    (
        "011_remove_control_plane_leases.sql",
        include_str!("../../storage-tidb/migrations/011_remove_control_plane_leases.sql"),
    ),
    (
        "012_drop_catalog_idempotency_tokens.sql",
        include_str!("../../storage-tidb/migrations/012_drop_catalog_idempotency_tokens.sql"),
    ),
    (
        "013_metrics_samples.sql",
        include_str!("../../storage-tidb/migrations/013_metrics_samples.sql"),
    ),
    (
        "014_drop_cached_table_stats.sql",
        include_str!("../../storage-tidb/migrations/014_drop_cached_table_stats.sql"),
    ),
    (
        "015_binary_collation_defaults.sql",
        include_str!("../../storage-tidb/migrations/015_binary_collation_defaults.sql"),
    ),
    (
        "016_catalog_ttl_status.sql",
        include_str!("../../storage-tidb/migrations/016_catalog_ttl_status.sql"),
    ),
    (
        "017_drop_legacy_ttl_flags.sql",
        include_str!("../../storage-tidb/migrations/017_drop_legacy_ttl_flags.sql"),
    ),
    (
        "018_drop_legacy_metrics.sql",
        include_str!("../../storage-tidb/migrations/018_drop_legacy_metrics.sql"),
    ),
    (
        "019_shard_login_attempt_row_ids.sql",
        include_str!("../../storage-tidb/migrations/019_shard_login_attempt_row_ids.sql"),
    ),
    (
        "020_raw_data_hash_key_columns.sql",
        include_str!("../../storage-tidb/migrations/020_raw_data_hash_key_columns.sql"),
    ),
    (
        "021_presplit_append_tables.sql",
        include_str!("../../storage-tidb/migrations/021_presplit_append_tables.sql"),
    ),
    (
        "022_auth_lookup_indexes.sql",
        include_str!("../../storage-tidb/migrations/022_auth_lookup_indexes.sql"),
    ),
    (
        "023_drop_continuous_backups.sql",
        include_str!("../../storage-tidb/migrations/023_drop_continuous_backups.sql"),
    ),
    (
        "024_drop_redundant_session_role_index.sql",
        include_str!("../../storage-tidb/migrations/024_drop_redundant_session_role_index.sql"),
    ),
    (
        "025_drop_redundant_login_success_column.sql",
        include_str!("../../storage-tidb/migrations/025_drop_redundant_login_success_column.sql"),
    ),
    (
        "026_simplify_control_plane_queue_index.sql",
        include_str!("../../storage-tidb/migrations/026_simplify_control_plane_queue_index.sql"),
    ),
];

const DATA_SCHEMA_MIGRATION: &str =
    include_str!("../../storage-tidb/data_migrations/001_data_schema.sql");
const DATA_PRESPLIT_SHARED_TABLES_MIGRATION: &str =
    include_str!("../../storage-tidb/data_migrations/002_presplit_shared_data_tables.sql");
const DATA_STREAM_RECORD_BUCKET_SPLITS_MIGRATION: &str =
    include_str!("../../storage-tidb/data_migrations/004_stream_record_bucket_splits.sql");
const DATA_IDEMPOTENCY_TOKEN_NATIVE_LAYOUT_MARKER_MIGRATION: &str =
    include_str!("../../storage-tidb/data_migrations/005_idempotency_token_native_layout.sql");
pub(crate) const DATA_MIGRATIONS: &[(&str, &str)] = &[
    ("001_data_schema.sql", DATA_SCHEMA_MIGRATION),
    (
        "002_presplit_shared_data_tables.sql",
        DATA_PRESPLIT_SHARED_TABLES_MIGRATION,
    ),
    (
        "004_stream_record_bucket_splits.sql",
        DATA_STREAM_RECORD_BUCKET_SPLITS_MIGRATION,
    ),
    (
        "005_idempotency_token_native_layout.sql",
        DATA_IDEMPOTENCY_TOKEN_NATIVE_LAYOUT_MARKER_MIGRATION,
    ),
];
#[cfg(test)]
const TIDB_BINARY_COLLATION_TABLE_OPTION: &str = "DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
const DATA_NATIVE_TTL_LOOKUP_INDEX_DROPS: &[(&str, &str)] = &[
    (
        "stream_records",
        "ALTER TABLE stream_records DROP INDEX IF EXISTS idx_stream_records_created",
    ),
    (
        "idempotency_tokens",
        "ALTER TABLE idempotency_tokens DROP INDEX IF EXISTS idx_idempotency_tokens_created",
    ),
];
const CATALOG_REGION_MERGE_DENY_TABLES: &[&str] = &["metrics_samples", "login_attempts"];
const DATA_REGION_MERGE_DENY_TABLES: &[&str] = &["stream_records", "idempotency_tokens"];
const MIGRATION_SESSION_INIT_STATEMENTS: &[&str] = &[
    "SET SESSION tidb_scatter_region = 'global'",
    "SET SESSION tidb_wait_split_region_finish = ON",
];

/// Run catalog migrations, skipping already-applied ones.
pub(crate) async fn run_catalog_migrations(pool: &MySqlPool) -> OpResult<()> {
    println!("--- Running catalog migrations...");
    let table_count = catalog_schema_table_count(pool).await?;

    if should_apply_consolidated_catalog_schema(table_count) {
        let (filename, sql) = CATALOG_MIGRATIONS
            .first()
            .expect("catalog migrations are non-empty");
        println!("    Applying consolidated {filename}...");
        run_sql_script(pool, sql, filename).await?;
        record_all_catalog_migrations(pool).await?;
        ensure_catalog_table_region_merge_option(pool).await?;
        println!("    Fresh catalog initialized from consolidated schema.");
        return Ok(());
    }

    for (filename, sql) in CATALOG_MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        run_sql_script(pool, sql, filename).await?;
        record_migration(pool, filename).await?;
    }
    ensure_catalog_table_region_merge_option(pool).await?;
    println!("    Migrations applied.");
    Ok(())
}

fn should_apply_consolidated_catalog_schema(catalog_table_count: i64) -> bool {
    catalog_table_count == 0
}

/// Run data database migrations.
pub(crate) async fn run_data_migrations(pool: &MySqlPool) -> OpResult<()> {
    println!("--- Initializing data database schema...");
    ensure_data_schema_history(pool).await?;
    let initialized: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_name = 'stream_records' AND table_schema = DATABASE())",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check data schema: {e}")))?;

    if initialized {
        println!("    Data schema already initialized.");
        record_data_migration(pool, DATA_MIGRATIONS[0].0).await?;
    } else {
        run_sql_script(pool, DATA_SCHEMA_MIGRATION, "data migration").await?;
        record_data_migration(pool, DATA_MIGRATIONS[0].0).await?;
        println!("    Data schema initialized.");
    }
    for (filename, sql) in DATA_MIGRATIONS.iter().skip(1) {
        if is_data_migration_applied(pool, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        run_sql_script(pool, sql, filename).await?;
        record_data_migration(pool, filename).await?;
    }
    drop_legacy_stream_shards(pool).await?;
    ensure_stream_commit_sequence(pool).await?;
    validate_stream_records_auto_random_handle(pool).await?;
    validate_idempotency_token_native_layout(pool).await?;
    ensure_data_table_binary_defaults(pool).await?;
    validate_dynamodb_data_table_partition_layout(pool).await?;
    validate_dynamodb_hash_key_column_layout(pool).await?;
    validate_dynamodb_secondary_index_global_layout(pool).await?;
    ensure_shared_data_table_region_merge_option(pool).await?;
    ensure_user_data_table_region_merge_option(pool).await?;
    repair_user_data_table_region_splits(pool).await?;
    drop_native_ttl_lookup_indexes(pool).await?;
    ensure_data_table_ttl(pool).await?;
    Ok(())
}

async fn ensure_data_schema_history(pool: &MySqlPool) -> OpResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS data_schema_history (\
             filename VARCHAR(255) PRIMARY KEY CLUSTERED,\
             applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)\
         ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
    )
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Create TiDB data schema history: {e}")))?;
    Ok(())
}

async fn is_data_migration_applied(pool: &MySqlPool, filename: &str) -> OpResult<bool> {
    let applied: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM data_schema_history WHERE filename = ?)")
            .bind(filename)
            .fetch_one(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Check TiDB data migration: {e}")))?;
    Ok(applied)
}

async fn record_data_migration(pool: &MySqlPool, filename: &str) -> OpResult<()> {
    sqlx::query("INSERT IGNORE INTO data_schema_history (filename) VALUES (?)")
        .bind(filename)
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Record TiDB data migration: {e}")))?;
    Ok(())
}

async fn drop_legacy_stream_shards(pool: &MySqlPool) -> OpResult<()> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_name = 'stream_shards' AND table_schema = DATABASE())",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check legacy stream shards: {e}")))?;

    if !exists {
        return Ok(());
    }

    let constraints: Vec<String> = sqlx::query_scalar(
        "SELECT CONSTRAINT_NAME \
         FROM information_schema.referential_constraints \
         WHERE CONSTRAINT_SCHEMA = DATABASE() \
           AND TABLE_NAME = 'stream_records' \
           AND REFERENCED_TABLE_NAME = 'stream_shards'",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Find legacy stream shard FKs: {e}")))?;

    for constraint in constraints {
        let ddl = format!(
            "ALTER TABLE stream_records DROP FOREIGN KEY `{}`",
            constraint.replace('`', "``")
        );
        sqlx::query(&ddl)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Drop legacy stream shard FK: {e}")))?;
    }

    sqlx::query("DROP TABLE IF EXISTS stream_shards")
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Drop legacy stream shards: {e}")))?;

    Ok(())
}

async fn ensure_stream_commit_sequence(pool: &MySqlPool) -> OpResult<()> {
    if !table_exists(pool, "stream_records").await? {
        return Ok(());
    }

    for statement in [
        "ALTER TABLE stream_records \
         ADD COLUMN IF NOT EXISTS commit_sequence_number VARCHAR(64) NULL \
         AFTER sequence_number",
        "ALTER TABLE stream_records \
         ADD INDEX IF NOT EXISTS idx_stream_records_commit_sequence \
         (shard_id, commit_sequence_number)",
        "UPDATE stream_records \
         SET commit_sequence_number = sequence_number \
         WHERE commit_sequence_number IS NULL",
    ] {
        sqlx::query(statement).execute(pool).await.map_err(|e| {
            OpError::Internal(format!("Configure TiDB stream commit sequence: {e}"))
        })?;
    }

    Ok(())
}

async fn validate_idempotency_token_native_layout(pool: &MySqlPool) -> OpResult<()> {
    if !table_exists(pool, "idempotency_tokens").await? {
        return Err(incompatible_idempotency_token_layout_error(
            "missing idempotency_tokens table".to_owned(),
        ));
    }
    if idempotency_token_layout_is_native(pool).await? {
        return Ok(());
    }

    Err(incompatible_idempotency_token_layout_error(
        idempotency_token_layout_summary(pool).await?,
    ))
}

async fn idempotency_token_layout_is_native(pool: &MySqlPool) -> OpResult<bool> {
    let token_id: Option<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT c.COLUMN_TYPE, c.IS_NULLABLE, c.COLUMN_KEY, t.TIDB_ROW_ID_SHARDING_INFO \
         FROM information_schema.columns c \
         JOIN information_schema.tables t \
           ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME \
         WHERE c.TABLE_SCHEMA = DATABASE() \
           AND c.TABLE_NAME = 'idempotency_tokens' \
           AND c.COLUMN_NAME = 'token_id'",
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Inspect TiDB idempotency token handle: {e}")))?;

    let Some((column_type, is_nullable, column_key, row_id_sharding_info)) = token_id else {
        return Ok(false);
    };
    if !column_type.to_ascii_lowercase().starts_with("bigint")
        || !is_nullable.eq_ignore_ascii_case("NO")
        || !column_key.eq_ignore_ascii_case("PRI")
        || !row_id_sharding_info
            .as_deref()
            .is_some_and(|info| info.contains("PK_AUTO_RANDOM_BITS="))
    {
        return Ok(false);
    }

    let token_hash: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT COLUMN_TYPE, EXTRA, GENERATION_EXPRESSION \
         FROM information_schema.columns \
         WHERE TABLE_SCHEMA = DATABASE() \
           AND TABLE_NAME = 'idempotency_tokens' \
           AND COLUMN_NAME = 'token_hash'",
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Inspect TiDB idempotency token hash: {e}")))?;

    let Some((column_type, extra, generation_expression)) = token_hash else {
        return Ok(false);
    };
    if !column_type.eq_ignore_ascii_case("bigint unsigned")
        || !extra.to_ascii_lowercase().contains("stored generated")
        || !generation_expression
            .as_deref()
            .is_some_and(|expr| expr.to_ascii_lowercase().contains("crc32(`token`)"))
    {
        return Ok(false);
    }

    let claim_id: Option<(String, String)> = sqlx::query_as(
        "SELECT COLUMN_TYPE, IS_NULLABLE \
         FROM information_schema.columns \
         WHERE TABLE_SCHEMA = DATABASE() \
           AND TABLE_NAME = 'idempotency_tokens' \
           AND COLUMN_NAME = 'claim_id'",
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Inspect TiDB idempotency token claim: {e}")))?;

    let Some((column_type, is_nullable)) = claim_id else {
        return Ok(false);
    };
    if !column_type.eq_ignore_ascii_case("varchar(36)") || !is_nullable.eq_ignore_ascii_case("NO") {
        return Ok(false);
    }

    idempotency_token_unique_index_is_native(pool).await
}

async fn idempotency_token_unique_index_is_native(pool: &MySqlPool) -> OpResult<bool> {
    let rows: Vec<(i64, Option<String>, Option<String>, i64)> = sqlx::query_as(
        "SELECT SEQ_IN_INDEX, COLUMN_NAME, EXPRESSION, NON_UNIQUE \
         FROM information_schema.statistics \
         WHERE TABLE_SCHEMA = DATABASE() \
           AND TABLE_NAME = 'idempotency_tokens' \
           AND INDEX_NAME = 'uk_idempotency_tokens_token' \
         ORDER BY SEQ_IN_INDEX",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Inspect TiDB idempotency token index: {e}")))?;

    if rows.len() != 3 || rows.iter().any(|(_, _, _, non_unique)| *non_unique != 0) {
        return Ok(false);
    }

    let expression = rows[0]
        .2
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    Ok(rows[0].0 == 1
        && rows[0].1.is_none()
        && expression.contains("tidb_shard")
        && expression.contains("token_hash")
        && rows[1].0 == 2
        && rows[1].1.as_deref() == Some("token_hash")
        && rows[2].0 == 3
        && rows[2].1.as_deref() == Some("token"))
}

#[derive(sqlx::FromRow)]
struct IdempotencyTokenLayoutColumn {
    column_name: String,
    column_type: String,
    is_nullable: String,
    column_key: String,
    generation_expression: Option<String>,
    row_id_sharding_info: Option<String>,
}

async fn idempotency_token_layout_summary(pool: &MySqlPool) -> OpResult<String> {
    let columns: Vec<IdempotencyTokenLayoutColumn> = sqlx::query_as(
        "SELECT c.COLUMN_NAME AS column_name, \
                    c.COLUMN_TYPE AS column_type, \
                    c.IS_NULLABLE AS is_nullable, \
                    c.COLUMN_KEY AS column_key, \
                    c.GENERATION_EXPRESSION AS generation_expression, \
                    t.TIDB_ROW_ID_SHARDING_INFO AS row_id_sharding_info \
             FROM information_schema.columns c \
             JOIN information_schema.tables t \
               ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME \
             WHERE c.TABLE_SCHEMA = DATABASE() \
               AND c.TABLE_NAME = 'idempotency_tokens' \
               AND c.COLUMN_NAME IN ('token_id', 'token_hash', 'claim_id') \
             ORDER BY c.COLUMN_NAME",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Inspect TiDB idempotency token layout: {e}")))?;

    if columns.is_empty() {
        return Ok("missing token_id, token_hash, and claim_id columns".to_owned());
    }

    let details = columns
        .into_iter()
        .map(|column| {
            format!(
                "{}: type={}, nullable={}, key={}, generated={}, row_id_sharding_info={}",
                column.column_name,
                column.column_type,
                column.is_nullable,
                column.column_key,
                column.generation_expression.unwrap_or_default(),
                column.row_id_sharding_info.unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("; ");

    Ok(details)
}

fn incompatible_idempotency_token_layout_error(observed: String) -> OpError {
    OpError::Internal(format!(
        "TiDB idempotency_tokens must use native AUTO_RANDOM clustered token_id primary key, \
         generated token_hash = CRC32(token), NOT NULL claim_id, and a \
         TIDB_SHARD unique token index; {observed}. Recreate the TiDB data database \
         with the current ExtendDB TiDB schema before serving distributed transaction writes."
    ))
}

async fn ensure_data_table_binary_defaults(pool: &MySqlPool) -> OpResult<()> {
    sqlx::query("ALTER DATABASE CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Configure TiDB data database collation: {e}")))?;

    for table in ["stream_records", "idempotency_tokens"] {
        if !table_exists(pool, table).await? {
            continue;
        }
        let statement =
            format!("ALTER TABLE {table} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
        sqlx::query(&statement)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Configure TiDB binary collation: {e}")))?;
    }
    Ok(())
}

async fn validate_dynamodb_hash_key_column_layout(pool: &MySqlPool) -> OpResult<()> {
    let sql = format!(
        r"SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, GENERATION_EXPRESSION
          FROM information_schema.columns
          WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME {DATA_TABLE_METADATA_LIKE_CLAUSE}
            AND (
                COLUMN_NAME = 'pk'
                OR (COLUMN_NAME {NATIVE_INDEX_GENERATED_PK_COLUMN_METADATA_LIKE_CLAUSE} AND EXTRA LIKE '%GENERATED%')
            )"
    );
    let rows: Vec<(String, String, String, Option<String>)> = sqlx::query_as(&sql)
        .fetch_all(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Inspect TiDB data key columns: {e}")))?;

    for (table_name, column_name, column_type, generation_expression) in rows {
        if !dynamodb_hash_key_column_needs_rebuild(&column_type) {
            continue;
        }
        return Err(incompatible_dynamodb_hash_key_column_error(
            &table_name,
            &column_name,
            &column_type,
            generation_expression.as_deref(),
        ));
    }

    Ok(())
}

fn dynamodb_hash_key_column_needs_rebuild(column_type: &str) -> bool {
    !column_type.eq_ignore_ascii_case(&format!("varbinary({DYNAMODB_HASH_KEY_COLUMN_BYTES})"))
}

fn incompatible_dynamodb_hash_key_column_error(
    table_name: &str,
    column_name: &str,
    column_type: &str,
    generation_expression: Option<&str>,
) -> OpError {
    let generated = generation_expression
        .filter(|expr| !expr.trim().is_empty())
        .map_or("", |_| " generated");
    OpError::Internal(format!(
        "TiDB data table {table_name}.{column_name} uses incompatible{generated} \
         hash-key column type {column_type}; recreate the table with raw \
         {DYNAMODB_HASH_KEY_COLUMN_TYPE} hash-key columns before using this backend version"
    ))
}

#[derive(sqlx::FromRow)]
struct DataTablePartitionLayout {
    table_name: String,
    partition_count: i64,
    min_partition_method: Option<String>,
    max_partition_method: Option<String>,
}

async fn validate_dynamodb_data_table_partition_layout(pool: &MySqlPool) -> OpResult<()> {
    let sql = format!(
        r"SELECT t.TABLE_NAME AS table_name,
                 COUNT(p.PARTITION_NAME) AS partition_count,
                 MIN(p.PARTITION_METHOD) AS min_partition_method,
                 MAX(p.PARTITION_METHOD) AS max_partition_method
          FROM information_schema.tables t
          LEFT JOIN information_schema.partitions p
            ON p.TABLE_SCHEMA = t.TABLE_SCHEMA
           AND p.TABLE_NAME = t.TABLE_NAME
           AND p.PARTITION_NAME IS NOT NULL
          WHERE t.TABLE_SCHEMA = DATABASE()
            AND t.TABLE_NAME {DATA_TABLE_METADATA_LIKE_CLAUSE}
          GROUP BY t.TABLE_NAME"
    );
    let rows: Vec<DataTablePartitionLayout> = sqlx::query_as(&sql)
        .fetch_all(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Inspect TiDB data table partition layout: {e}")))?;

    for row in rows {
        if dynamodb_data_table_partition_layout_is_native(
            row.partition_count,
            row.min_partition_method.as_deref(),
            row.max_partition_method.as_deref(),
        ) {
            continue;
        }
        return Err(incompatible_dynamodb_data_table_partition_error(
            &row.table_name,
            row.partition_count,
            row.min_partition_method.as_deref(),
            row.max_partition_method.as_deref(),
        ));
    }

    Ok(())
}

fn dynamodb_data_table_partition_layout_is_native(
    partition_count: i64,
    min_partition_method: Option<&str>,
    max_partition_method: Option<&str>,
) -> bool {
    partition_count == i64::from(DATA_TABLE_PARTITIONS)
        && min_partition_method.is_some_and(|method| method.eq_ignore_ascii_case("KEY"))
        && max_partition_method.is_some_and(|method| method.eq_ignore_ascii_case("KEY"))
}

fn incompatible_dynamodb_data_table_partition_error(
    table_name: &str,
    partition_count: i64,
    min_partition_method: Option<&str>,
    max_partition_method: Option<&str>,
) -> OpError {
    let min_partition_method = min_partition_method.unwrap_or("<none>");
    let max_partition_method = max_partition_method.unwrap_or("<none>");
    OpError::Internal(format!(
        "TiDB data table {table_name} must use native PARTITION BY KEY(pk) \
         PARTITIONS {DATA_TABLE_PARTITIONS}; observed partition_count={partition_count}, \
         partition_method_range={min_partition_method}..{max_partition_method}. \
         Recreate the table with the current ExtendDB TiDB schema before serving \
         distributed reads or writes."
    ))
}

async fn validate_dynamodb_secondary_index_global_layout(pool: &MySqlPool) -> OpResult<()> {
    let sql = format!(
        r"SELECT TABLE_NAME, KEY_NAME, MIN(IS_GLOBAL), MAX(IS_GLOBAL)
          FROM information_schema.tidb_indexes
          WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME {DATA_TABLE_METADATA_LIKE_CLAUSE}
            AND KEY_NAME {NATIVE_INDEX_METADATA_LIKE_CLAUSE}
          GROUP BY TABLE_NAME, KEY_NAME
          HAVING MIN(IS_GLOBAL) <> 1 OR MAX(IS_GLOBAL) <> 1"
    );
    let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(&sql)
        .fetch_all(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Inspect TiDB data table global indexes: {e}")))?;

    if let Some((table_name, index_name, min_is_global, max_is_global)) = rows.into_iter().next() {
        return Err(incompatible_dynamodb_secondary_index_global_error(
            &table_name,
            &index_name,
            min_is_global,
            max_is_global,
        ));
    }

    Ok(())
}

fn incompatible_dynamodb_secondary_index_global_error(
    table_name: &str,
    index_name: &str,
    min_is_global: i64,
    max_is_global: i64,
) -> OpError {
    OpError::Internal(format!(
        "TiDB data table {table_name}.{index_name} uses a local secondary index \
         layout (IS_GLOBAL range {min_is_global}..{max_is_global}); recreate it as \
         a TiDB GLOBAL index before serving DynamoDB IndexName reads."
    ))
}

async fn validate_stream_records_auto_random_handle(pool: &MySqlPool) -> OpResult<()> {
    if !table_exists(pool, "stream_records").await? {
        return Ok(());
    }

    let row: Option<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT c.COLUMN_TYPE, c.IS_NULLABLE, c.COLUMN_KEY, t.TIDB_ROW_ID_SHARDING_INFO \
         FROM information_schema.columns c \
         JOIN information_schema.tables t \
           ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME \
         WHERE c.TABLE_SCHEMA = DATABASE() \
           AND c.TABLE_NAME = 'stream_records' \
           AND c.COLUMN_NAME = 'record_id'",
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Inspect TiDB stream record handle: {e}")))?;

    let Some((column_type, is_nullable, column_key, row_id_sharding_info)) = row else {
        return Err(incompatible_stream_record_layout_error(None));
    };
    let is_bigint = column_type.to_ascii_lowercase().starts_with("bigint");
    let is_primary_key = column_key.eq_ignore_ascii_case("PRI");
    let is_not_null = is_nullable.eq_ignore_ascii_case("NO");
    let uses_auto_random = row_id_sharding_info
        .as_deref()
        .is_some_and(|info| info.contains("PK_AUTO_RANDOM_BITS="));

    if is_bigint && is_primary_key && is_not_null && uses_auto_random {
        return Ok(());
    }

    Err(incompatible_stream_record_layout_error(Some((
        column_type,
        is_nullable,
        column_key,
        row_id_sharding_info.unwrap_or_default(),
    ))))
}

fn incompatible_stream_record_layout_error(
    observed: Option<(String, String, String, String)>,
) -> OpError {
    let observed = observed.map_or_else(
        || "missing record_id column".to_owned(),
        |(column_type, is_nullable, column_key, row_id_sharding_info)| {
            format!(
                "record_id column_type={column_type}, nullable={is_nullable}, key={column_key}, row_id_sharding_info={row_id_sharding_info}"
            )
        },
    );
    OpError::Internal(format!(
        "TiDB stream_records must use native AUTO_RANDOM clustered record_id primary key; \
         {observed}. Recreate the TiDB data database with the current ExtendDB TiDB schema \
         before serving stream writes."
    ))
}

async fn repair_user_data_table_region_splits(pool: &MySqlPool) -> OpResult<()> {
    if is_data_migration_applied(pool, USER_TABLE_FULL_KEYSPACE_SPLITS_MIGRATION).await? {
        return Ok(());
    }

    for split_sql in user_data_table_region_split_sqls(pool).await? {
        sqlx::query(&split_sql)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Split TiDB user data table Regions: {e}")))?;
    }

    record_data_migration(pool, USER_TABLE_FULL_KEYSPACE_SPLITS_MIGRATION).await?;
    Ok(())
}

async fn ensure_catalog_table_region_merge_option(pool: &MySqlPool) -> OpResult<()> {
    ensure_fixed_table_region_merge_option(pool, CATALOG_REGION_MERGE_DENY_TABLES, "catalog").await
}

async fn ensure_shared_data_table_region_merge_option(pool: &MySqlPool) -> OpResult<()> {
    ensure_fixed_table_region_merge_option(pool, DATA_REGION_MERGE_DENY_TABLES, "data").await
}

async fn ensure_fixed_table_region_merge_option(
    pool: &MySqlPool,
    table_names: &[&str],
    label: &str,
) -> OpResult<()> {
    for statement in fixed_table_region_merge_option_sqls(pool, table_names).await? {
        sqlx::query(&statement).execute(pool).await.map_err(|e| {
            OpError::Internal(format!(
                "Configure TiDB {label} table Region merge option: {e}"
            ))
        })?;
    }
    Ok(())
}

async fn ensure_user_data_table_region_merge_option(pool: &MySqlPool) -> OpResult<()> {
    for statement in user_data_table_region_merge_option_sqls(pool).await? {
        sqlx::query(&statement).execute(pool).await.map_err(|e| {
            OpError::Internal(format!(
                "Configure TiDB user data table Region merge option: {e}"
            ))
        })?;
    }
    Ok(())
}

async fn drop_native_ttl_lookup_indexes(pool: &MySqlPool) -> OpResult<()> {
    for (table, statement) in DATA_NATIVE_TTL_LOOKUP_INDEX_DROPS {
        if !table_exists(pool, table).await? {
            continue;
        }
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Drop TiDB native TTL lookup index: {e}")))?;
    }
    Ok(())
}

async fn ensure_data_table_ttl(pool: &MySqlPool) -> OpResult<()> {
    for statement in [
        "ALTER TABLE stream_records TTL = `created_at` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h'",
        "ALTER TABLE stream_records TTL_ENABLE = 'ON'",
        "ALTER TABLE idempotency_tokens TTL = `created_at` + INTERVAL 600 SECOND TTL_JOB_INTERVAL = '10m'",
        "ALTER TABLE idempotency_tokens TTL_ENABLE = 'ON'",
    ] {
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Configure TiDB TTL: {e}")))?;
    }
    Ok(())
}

async fn run_sql_script(pool: &MySqlPool, sql: &str, label: &str) -> OpResult<()> {
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| OpError::Internal(format!("Acquire migration connection: {e}")))?;
    configure_migration_session(&mut conn, label).await?;
    let restores_foreign_key_checks = sql.contains("FOREIGN_KEY_CHECKS");
    for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        if let Err(err) = sqlx::query(statement).execute(&mut *conn).await {
            if restores_foreign_key_checks {
                let _ = sqlx::query("SET FOREIGN_KEY_CHECKS = 1")
                    .execute(&mut *conn)
                    .await;
            }
            return Err(OpError::Internal(format!(
                "Migration {label} failed: {err}"
            )));
        }
    }
    Ok(())
}

async fn configure_migration_session(conn: &mut MySqlConnection, label: &str) -> OpResult<()> {
    for statement in MIGRATION_SESSION_INIT_STATEMENTS {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                OpError::Internal(format!("Configure TiDB migration session for {label}: {e}"))
            })?;
    }
    Ok(())
}

/// Check if a table exists in the current database.
pub(crate) async fn table_exists(pool: &MySqlPool, name: &str) -> OpResult<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_name = ? AND table_schema = DATABASE())",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check table exists: {e}")))?;
    Ok(exists)
}

async fn catalog_schema_table_count(pool: &MySqlPool) -> OpResult<i64> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_schema = DATABASE() AND table_name <> 'schema_history'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Count catalog tables: {e}")))
}

/// Check if a migration has already been applied.
async fn is_migration_applied(pool: &MySqlPool, filename: &str) -> OpResult<bool> {
    if table_exists(pool, "schema_history").await? {
        let applied: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM schema_history WHERE filename = ?)")
                .bind(filename)
                .fetch_one(pool)
                .await
                .map_err(|e| OpError::Internal(format!("Check migration: {e}")))?;
        return Ok(applied.0);
    }
    Ok(false)
}

/// Record a migration in the `schema_history` table.
async fn record_migration(pool: &MySqlPool, filename: &str) -> OpResult<()> {
    if !table_exists(pool, "schema_history").await? {
        return Ok(());
    }
    sqlx::query("INSERT IGNORE INTO schema_history (filename) VALUES (?)")
        .bind(filename)
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Record migration: {e}")))?;
    Ok(())
}

async fn record_all_catalog_migrations(pool: &MySqlPool) -> OpResult<()> {
    for (filename, _) in CATALOG_MIGRATIONS {
        record_migration(pool, filename).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use extenddb_storage::management_store::OpError;

    use super::{
        CATALOG_MIGRATIONS, CATALOG_REGION_MERGE_DENY_TABLES, DATA_MIGRATIONS,
        DATA_NATIVE_TTL_LOOKUP_INDEX_DROPS, DATA_REGION_MERGE_DENY_TABLES, DATA_SCHEMA_MIGRATION,
        MIGRATION_SESSION_INIT_STATEMENTS, TIDB_BINARY_COLLATION_TABLE_OPTION,
        dynamodb_data_table_partition_layout_is_native, dynamodb_hash_key_column_needs_rebuild,
        incompatible_dynamodb_data_table_partition_error,
        incompatible_dynamodb_hash_key_column_error,
        incompatible_dynamodb_secondary_index_global_error,
        incompatible_idempotency_token_layout_error, incompatible_stream_record_layout_error,
        should_apply_consolidated_catalog_schema,
    };
    use crate::{CATALOG_VERSION, data::USER_TABLE_FULL_KEYSPACE_SPLITS_MIGRATION};

    #[test]
    fn catalog_migration_pins_binary_collation_defaults() {
        let (_filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "015_binary_collation_defaults.sql")
            .expect("binary collation migration");

        assert!(sql.contains("ALTER DATABASE CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"));
        assert!(
            sql.contains("ALTER TABLE tables CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
        );
        assert!(sql.contains("MODIFY metric VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin"));
        assert!(sql.contains("SET FOREIGN_KEY_CHECKS = 0"));
        assert!(sql.contains("SET FOREIGN_KEY_CHECKS = 1"));
        assert!(sql.contains("0.0.15"));
    }

    #[test]
    fn catalog_migration_adds_explicit_ttl_status_before_dropping_legacy_flags() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "016_catalog_ttl_status.sql")
            .expect("ttl status migration");

        assert_eq!(*filename, "016_catalog_ttl_status.sql");
        assert!(sql.contains("ADD COLUMN IF NOT EXISTS ttl_index_ready"));
        assert!(sql.contains("ADD COLUMN IF NOT EXISTS ttl_native_enabled"));
        assert!(sql.contains("ADD COLUMN IF NOT EXISTS ttl_status"));
        assert!(sql.contains("WHEN ttl_attribute IS NULL THEN 'DISABLED'"));
        assert!(sql.contains("WHEN ttl_index_ready AND ttl_native_enabled THEN 'ENABLED'"));
        assert!(sql.contains("ELSE 'ENABLING'"));
        assert!(sql.contains("0.0.16"));
    }

    #[test]
    fn catalog_migration_drops_legacy_ttl_flags_after_status_migration() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "017_drop_legacy_ttl_flags.sql")
            .expect("drop ttl flags migration");
        let ttl_status_pos = CATALOG_MIGRATIONS
            .iter()
            .position(|(filename, _)| *filename == "016_catalog_ttl_status.sql")
            .expect("ttl status migration position");
        let drop_flags_pos = CATALOG_MIGRATIONS
            .iter()
            .position(|(filename, _)| *filename == "017_drop_legacy_ttl_flags.sql")
            .expect("drop ttl flags migration position");

        assert_eq!(*filename, "017_drop_legacy_ttl_flags.sql");
        assert!(ttl_status_pos < drop_flags_pos);
        assert!(sql.contains("DROP COLUMN IF EXISTS ttl_index_ready"));
        assert!(sql.contains("DROP COLUMN IF EXISTS ttl_native_enabled"));
        assert!(sql.contains("0.0.17"));
    }

    #[test]
    fn catalog_migration_drops_legacy_metrics_table() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "018_drop_legacy_metrics.sql")
            .expect("drop legacy metrics migration");
        let samples_pos = CATALOG_MIGRATIONS
            .iter()
            .position(|(filename, _)| *filename == "013_metrics_samples.sql")
            .expect("metrics samples migration position");
        let drop_metrics_pos = CATALOG_MIGRATIONS
            .iter()
            .position(|(filename, _)| *filename == "018_drop_legacy_metrics.sql")
            .expect("drop legacy metrics migration position");

        assert_eq!(*filename, "018_drop_legacy_metrics.sql");
        assert!(samples_pos < drop_metrics_pos);
        assert!(sql.contains("DROP TABLE IF EXISTS metrics"));
        assert!(sql.contains("0.0.18"));
    }

    #[test]
    fn latest_catalog_migration_shards_login_attempt_row_ids() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "019_shard_login_attempt_row_ids.sql")
            .expect("login attempt shard migration");

        assert_eq!(*filename, "019_shard_login_attempt_row_ids.sql");
        assert!(sql.contains("ALTER TABLE login_attempts SHARD_ROW_ID_BITS = 4"));
        assert!(sql.contains("0.0.19"));
    }

    #[test]
    fn catalog_migration_presplits_append_tables() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "021_presplit_append_tables.sql")
            .expect("presplit append tables migration");

        assert_eq!(*filename, "021_presplit_append_tables.sql");
        assert!(sql.contains("ALTER TABLE metrics_samples ATTRIBUTES 'merge_option=deny'"));
        assert!(sql.contains("ALTER TABLE login_attempts ATTRIBUTES 'merge_option=deny'"));
        assert!(sql.contains("SPLIT TABLE metrics_samples"));
        assert!(sql.contains("SPLIT TABLE login_attempts"));
        assert!(sql.contains("REGIONS 16"));
        assert!(sql.contains("0.0.21"));
    }

    #[test]
    fn migration_sessions_scatter_presplit_regions() {
        assert_eq!(
            MIGRATION_SESSION_INIT_STATEMENTS,
            [
                "SET SESSION tidb_scatter_region = 'global'",
                "SET SESSION tidb_wait_split_region_finish = ON",
            ]
        );
    }

    #[test]
    fn catalog_migration_adds_auth_lookup_indexes() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "022_auth_lookup_indexes.sql")
            .expect("auth lookup migration");

        assert_eq!(*filename, "022_auth_lookup_indexes.sql");
        assert!(sql.contains("CREATE INDEX IF NOT EXISTS idx_iam_group_members_user"));
        assert!(sql.contains("ON iam_group_members (account_id, user_name, group_name)"));
        assert!(!sql.contains("idx_iam_sessions_role_session"));
        assert!(sql.contains("0.0.22"));
    }

    #[test]
    fn catalog_migration_drops_unsupported_pitr_state() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "023_drop_continuous_backups.sql")
            .expect("continuous backups migration");

        assert_eq!(*filename, "023_drop_continuous_backups.sql");
        assert!(sql.contains("DROP TABLE IF EXISTS continuous_backups"));
        assert!(sql.contains("0.0.23"));
    }

    #[test]
    fn catalog_migration_drops_redundant_session_role_index() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "024_drop_redundant_session_role_index.sql")
            .expect("redundant session role index migration");

        assert_eq!(*filename, "024_drop_redundant_session_role_index.sql");
        assert!(sql.contains("DROP INDEX IF EXISTS idx_iam_sessions_role_session ON iam_sessions"));
        assert!(sql.contains("0.0.24"));
    }

    #[test]
    fn catalog_migration_drops_redundant_login_success_column() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "025_drop_redundant_login_success_column.sql")
            .expect("redundant login success column migration");

        assert_eq!(*filename, "025_drop_redundant_login_success_column.sql");
        assert!(sql.contains("ALTER TABLE login_attempts DROP COLUMN IF EXISTS success"));
        assert!(sql.contains("0.0.25"));
    }

    #[test]
    fn latest_catalog_migration_simplifies_control_plane_queue_index() {
        let (filename, sql) = CATALOG_MIGRATIONS.last().expect("latest migration");

        assert_eq!(*filename, "026_simplify_control_plane_queue_index.sql");
        assert!(sql.contains("status_transition_at = CURRENT_TIMESTAMP(6)"));
        assert!(sql.contains("DROP INDEX IF EXISTS idx_tables_pending_transition ON tables"));
        assert!(sql.contains("0.0.26"));
    }

    #[test]
    fn catalog_migration_records_data_key_layout_change() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "020_raw_data_hash_key_columns.sql")
            .expect("data key migration");

        assert_eq!(*filename, "020_raw_data_hash_key_columns.sql");
        assert!(sql.contains("0.0.20"));
    }

    #[test]
    fn compiled_catalog_version_matches_latest_migration() {
        let (_filename, sql) = CATALOG_MIGRATIONS.last().expect("latest migration");

        assert!(sql.contains(&format!(
            "UPDATE settings SET value = '{}' WHERE `key` = 'catalog_version'",
            CATALOG_VERSION
        )));
    }

    #[test]
    fn empty_catalog_uses_consolidated_schema() {
        assert!(should_apply_consolidated_catalog_schema(0));
        assert!(!should_apply_consolidated_catalog_schema(1));
    }

    #[test]
    fn fresh_catalog_schema_uses_native_table_stats() {
        let (filename, sql) = CATALOG_MIGRATIONS.first().expect("fresh catalog migration");

        assert_eq!(*filename, "001_schema.sql");
        let tables_schema = sql
            .split("-- Index metadata.")
            .next()
            .expect("table catalog section");
        assert!(!tables_schema.contains("table_size_bytes"));
        assert!(!tables_schema.contains("item_count"));
        assert!(!tables_schema.contains("ttl_index_ready"));
        assert!(!tables_schema.contains("ttl_native_enabled"));
        assert!(tables_schema.contains("ttl_status VARCHAR(32) NOT NULL DEFAULT 'DISABLED'"));
    }

    #[test]
    fn fresh_catalog_schema_uses_final_native_metrics_shape() {
        let (filename, sql) = CATALOG_MIGRATIONS.first().expect("fresh catalog migration");

        assert_eq!(*filename, "001_schema.sql");
        assert!(!sql.contains("CREATE TABLE IF NOT EXISTS metrics ("));
        assert!(!sql.contains("CREATE INDEX idx_metrics_bucket ON metrics"));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS metrics_samples"));
        assert!(sql.contains("sample_id BIGINT NOT NULL AUTO_RANDOM"));
        assert!(sql.contains("PRE_SPLIT_REGIONS = 4"));
        assert!(sql.contains("ALTER TABLE metrics_samples ATTRIBUTES 'merge_option=deny'"));
        assert!(sql.contains(&format!(
            "INSERT IGNORE INTO settings (`key`, value) VALUES ('catalog_version', '{}')",
            CATALOG_VERSION
        )));
        assert!(!sql.contains("idx_tables_pending_transition"));
        assert!(sql.contains("idx_tables_control_plane_work"));
    }

    #[test]
    fn fresh_catalog_schema_pins_binary_table_defaults() {
        let (filename, sql) = CATALOG_MIGRATIONS.first().expect("fresh catalog migration");

        assert_eq!(*filename, "001_schema.sql");
        assert_eq!(
            sql.matches("CREATE TABLE IF NOT EXISTS").count(),
            sql.matches(TIDB_BINARY_COLLATION_TABLE_OPTION).count()
        );
    }

    #[test]
    fn fresh_catalog_schema_shards_login_attempt_row_ids() {
        let (filename, sql) = CATALOG_MIGRATIONS.first().expect("fresh catalog migration");

        assert_eq!(*filename, "001_schema.sql");
        let login_attempts_schema = sql
            .split("-- Login attempt tracking.")
            .nth(1)
            .and_then(|section| section.split("-- Backup metadata.").next())
            .expect("login attempts schema section");
        assert!(!login_attempts_schema.contains("success"));
        assert!(login_attempts_schema.contains("SHARD_ROW_ID_BITS = 4"));
        assert!(login_attempts_schema.contains("PRE_SPLIT_REGIONS = 4"));
        assert!(login_attempts_schema.contains("TTL = `attempted_at` + INTERVAL 24 HOUR"));
        assert!(
            login_attempts_schema
                .contains("ALTER TABLE login_attempts ATTRIBUTES 'merge_option=deny'")
        );
    }

    #[test]
    fn fresh_catalog_schema_uses_auth_lookup_indexes() {
        let (filename, sql) = CATALOG_MIGRATIONS.first().expect("fresh catalog migration");

        assert_eq!(*filename, "001_schema.sql");
        assert!(sql.contains("CREATE INDEX idx_iam_group_members_user"));
        assert!(sql.contains("ON iam_group_members (account_id, user_name, group_name)"));
        assert!(!sql.contains("idx_iam_sessions_role_session"));
        assert!(sql.contains("access_key_id VARCHAR(128) NOT NULL UNIQUE"));
    }

    #[test]
    fn data_schema_uses_fixed_stream_shards_without_metadata_table() {
        assert!(!DATA_SCHEMA_MIGRATION.contains("stream_shards"));
        assert!(!DATA_SCHEMA_MIGRATION.contains("next_sequence_number"));
        assert!(DATA_SCHEMA_MIGRATION.contains("record_id BIGINT NOT NULL AUTO_RANDOM(4)"));
        assert!(DATA_SCHEMA_MIGRATION.contains("PRIMARY KEY (record_id) CLUSTERED"));
        assert!(
            DATA_SCHEMA_MIGRATION.contains(
                "UNIQUE KEY uk_stream_records_storage_sequence (shard_id, sequence_number)"
            )
        );
        assert!(DATA_SCHEMA_MIGRATION.contains("PRE_SPLIT_REGIONS = 4"));
        assert!(
            DATA_SCHEMA_MIGRATION
                .contains("ALTER TABLE stream_records ATTRIBUTES 'merge_option=deny'")
        );
        assert!(DATA_SCHEMA_MIGRATION.contains("shard_id VARCHAR(128) NOT NULL"));
        assert!(DATA_SCHEMA_MIGRATION.contains("TiDB MVCC commit_ts"));
        assert!(DATA_SCHEMA_MIGRATION.contains("commit_sequence_number VARCHAR(64)"));
    }

    #[test]
    fn data_schema_pins_binary_table_defaults() {
        assert_eq!(
            DATA_SCHEMA_MIGRATION
                .matches(TIDB_BINARY_COLLATION_TABLE_OPTION)
                .count(),
            2
        );
    }

    #[test]
    fn data_migrations_presplit_shared_write_tables_once() {
        assert_eq!(DATA_MIGRATIONS[0].0, "001_data_schema.sql");
        let (filename, sql) = DATA_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "002_presplit_shared_data_tables.sql")
            .expect("shared data split migration");

        assert_eq!(*filename, "002_presplit_shared_data_tables.sql");
        assert!(sql.contains("ALTER TABLE stream_records ATTRIBUTES 'merge_option=deny'"));
        assert!(sql.contains("ALTER TABLE idempotency_tokens ATTRIBUTES 'merge_option=deny'"));
        assert!(sql.contains("SPLIT TABLE stream_records"));
        assert!(
            sql.contains("SPLIT TABLE stream_records INDEX idx_stream_records_commit_sequence")
        );
        assert!(!sql.contains("SPLIT TABLE stream_records BY"));
        assert!(sql.contains("'shardId-000000000001-'"));
        assert!(sql.contains("'shardId-000000000015-'"));
        assert!(!sql.contains("SPLIT TABLE idempotency_tokens"));
        assert!(!sql.contains("BETWEEN ('') AND ('~')"));
    }

    #[test]
    fn data_migrations_split_stream_records_by_bucket_prefix() {
        let (filename, sql) = DATA_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "004_stream_record_bucket_splits.sql")
            .expect("stream bucket split migration");

        assert_eq!(*filename, "004_stream_record_bucket_splits.sql");
        assert!(sql.contains("ALTER TABLE stream_records ATTRIBUTES 'merge_option=deny'"));
        assert!(!sql.contains("SPLIT TABLE stream_records BY"));
        assert!(
            sql.contains("SPLIT TABLE stream_records INDEX idx_stream_records_commit_sequence BY")
        );
        assert!(sql.contains("'shardId-000000000001-'"));
        assert!(sql.contains("'shardId-000000000015-'"));
    }

    #[test]
    fn data_migrations_keep_idempotency_token_native_layout_marker_only() {
        let (filename, sql) = DATA_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "005_idempotency_token_native_layout.sql")
            .expect("idempotency token native marker migration");

        assert_eq!(*filename, "005_idempotency_token_native_layout.sql");
        assert!(sql.contains("ALTER TABLE idempotency_tokens ATTRIBUTES 'merge_option=deny'"));
        assert!(!sql.contains("SPLIT TABLE idempotency_tokens"));
        assert!(!sql.contains("10000000:"));
        assert!(!sql.contains("hash-prefix"));
        assert!(
            DATA_MIGRATIONS
                .iter()
                .all(|(_, sql)| !sql.contains("SPLIT TABLE idempotency_tokens"))
        );
    }

    #[test]
    fn user_table_split_repair_is_a_dynamic_data_migration() {
        assert_eq!(
            USER_TABLE_FULL_KEYSPACE_SPLITS_MIGRATION,
            "003_full_user_table_split_bounds.sql"
        );
        assert!(
            DATA_MIGRATIONS
                .iter()
                .all(|(filename, _)| *filename != USER_TABLE_FULL_KEYSPACE_SPLITS_MIGRATION)
        );
    }

    #[test]
    fn idempotency_token_native_layout_is_validated_not_rebuilt() {
        assert!(
            DATA_MIGRATIONS
                .iter()
                .all(|(filename, _)| *filename != "006_idempotency_token_native_layout.sql")
        );

        let OpError::Internal(error) = incompatible_idempotency_token_layout_error(
            "token_id: type=varchar(255), nullable=NO, key=PRI".to_owned(),
        ) else {
            panic!("expected internal migration error");
        };
        assert!(error.contains("AUTO_RANDOM clustered token_id primary key"));
        assert!(error.contains("TIDB_SHARD unique token index"));
        assert!(error.contains("Recreate the TiDB data database"));
    }

    #[test]
    fn common_handle_stream_tables_are_rejected_instead_of_repaired() {
        let OpError::Internal(error) = incompatible_stream_record_layout_error(None) else {
            panic!("expected internal migration error");
        };

        assert!(error.contains("AUTO_RANDOM clustered record_id primary key"));
        assert!(error.contains("Recreate the TiDB data database"));
    }

    #[test]
    fn data_table_partition_validation_rejects_unpartitioned_tables() {
        assert!(dynamodb_data_table_partition_layout_is_native(
            16,
            Some("KEY"),
            Some("KEY")
        ));
        assert!(!dynamodb_data_table_partition_layout_is_native(
            0, None, None
        ));
        assert!(!dynamodb_data_table_partition_layout_is_native(
            16,
            Some("HASH"),
            Some("HASH")
        ));

        let OpError::Internal(error) =
            incompatible_dynamodb_data_table_partition_error("_ddb_tableid", 0, None, None)
        else {
            panic!("expected internal migration error");
        };
        assert!(error.contains("PARTITION BY KEY(pk)"));
        assert!(error.contains("PARTITIONS 16"));
        assert!(error.contains("Recreate the table"));
    }

    #[test]
    fn data_table_secondary_index_validation_rejects_local_indexes() {
        let OpError::Internal(error) =
            incompatible_dynamodb_secondary_index_global_error("_ddb_tableid", "idx_idx1", 0, 0)
        else {
            panic!("expected internal migration error");
        };
        assert!(error.contains("local secondary index"));
        assert!(error.contains("GLOBAL index"));
        assert!(error.contains("DynamoDB IndexName reads"));
    }

    #[test]
    fn shared_tidb_append_tables_deny_region_merges_on_startup_repair() {
        assert_eq!(
            CATALOG_REGION_MERGE_DENY_TABLES,
            ["metrics_samples", "login_attempts"]
        );
        assert_eq!(
            DATA_REGION_MERGE_DENY_TABLES,
            ["stream_records", "idempotency_tokens"]
        );
    }

    #[test]
    fn data_schema_uses_native_ttl_without_cleanup_indexes() {
        assert!(!DATA_SCHEMA_MIGRATION.contains("idx_stream_records_created"));
        assert!(!DATA_SCHEMA_MIGRATION.contains("idx_idempotency_tokens_created"));
        assert!(!DATA_SCHEMA_MIGRATION.contains("DELETE FROM idempotency_tokens"));
        assert!(DATA_SCHEMA_MIGRATION.contains("TTL = `created_at` + INTERVAL 24 HOUR"));
        assert!(DATA_SCHEMA_MIGRATION.contains("TTL = `created_at` + INTERVAL 600 SECOND"));
        assert_eq!(
            DATA_NATIVE_TTL_LOOKUP_INDEX_DROPS,
            [
                (
                    "stream_records",
                    "ALTER TABLE stream_records DROP INDEX IF EXISTS idx_stream_records_created",
                ),
                (
                    "idempotency_tokens",
                    "ALTER TABLE idempotency_tokens DROP INDEX IF EXISTS idx_idempotency_tokens_created",
                ),
            ]
        );
    }

    #[test]
    fn data_schema_claims_idempotency_tokens_with_unique_key_state() {
        assert!(DATA_SCHEMA_MIGRATION.contains("claim_id    VARCHAR(36) NOT NULL"));
        assert!(DATA_SCHEMA_MIGRATION.contains("token_id    BIGINT NOT NULL AUTO_RANDOM(4)"));
        assert!(
            DATA_SCHEMA_MIGRATION.contains("token_hash  BIGINT UNSIGNED AS (CRC32(token)) STORED")
        );
        assert!(DATA_SCHEMA_MIGRATION.contains(
            "UNIQUE KEY uk_idempotency_tokens_token ((TIDB_SHARD(token_hash)), token_hash, token)"
        ));
        assert!(DATA_SCHEMA_MIGRATION.contains("PRIMARY KEY (token_id) CLUSTERED"));
        assert!(DATA_SCHEMA_MIGRATION.contains("PRE_SPLIT_REGIONS = 4"));
    }

    #[test]
    fn data_key_layout_validation_rejects_old_hash_key_columns() {
        assert!(!dynamodb_hash_key_column_needs_rebuild("varbinary(2048)"));
        assert!(dynamodb_hash_key_column_needs_rebuild("varbinary(3072)"));

        let error = incompatible_dynamodb_hash_key_column_error(
            "_ddb_tableid",
            "edbidx_idx1_pk",
            "varbinary(3072)",
            Some(
                "cast(json_unquote(json_extract(`item_data`, _utf8mb4'$.\"gpk\".\"B\"')) as binary)",
            ),
        );
        let extenddb_storage::management_store::OpError::Internal(message) = error else {
            panic!("expected internal error");
        };
        assert!(message.contains("incompatible generated"));
        assert!(message.contains("raw VARBINARY(2048)"));
    }
}
