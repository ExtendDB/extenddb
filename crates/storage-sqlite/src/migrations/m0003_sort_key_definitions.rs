// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 3, `sort_key_definitions`, SQLite: restore sort key attribute
//! definitions that the pre-fix `UpdateTable` deleted (#259), in table rows
//! and in the definitions stored with backups.
//!
//! Before the fix, `UpdateTable` replaced a table's stored attribute
//! definitions with the request's subset, so adding a GSI dropped the table's
//! own key definitions. A table's sort key type is read back from its data
//! table's PRIMARY KEY column (`sk_s`, `sk_n` or `sk_b`). A backup's is read
//! from the typed value in one of its items, or else from its source table.
//! A partition key definition the same write dropped cannot be recovered,
//! because the partition key column is always TEXT; it is reported as a note.
//! Nothing here stops the migration: a damaged definition it cannot repair is
//! a note, and the table or backup keeps failing loudly until a person fixes
//! it.
//!
//! This file's checksum is pinned once the migration is released. It is
//! excluded from rustfmt and clippy so neither can require an edit, and keeps
//! its own small helpers (`table_exists`, the key column read) instead of
//! calling the crate's, which may change.

use async_trait::async_trait;
use extenddb_core::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};
use extenddb_storage::management_store::{OpError, OpResult};
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use extenddb_storage::util::recover_sort_key_definitions;
use sqlx::SqlitePool;

use super::SqliteStores;

/// This file, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0003_sort_key_definitions.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0003_sort_key_definitions/sqlite/1";

/// What a repair did: the definitions it restored, and what it could not.
#[derive(Default)]
struct Repair {
    repaired: Vec<String>,
    needs_attention: Vec<String>,
}

pub(crate) struct SortKeyDefinitions;

#[async_trait]
impl CodeBody<SqliteStores> for SortKeyDefinitions {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &SqliteStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &SqliteStores) -> Result<Done, MigrationError> {
        let tables = repair_table_key_definitions(&stores.pool).await.map_err(op_error)?;
        let backups = repair_backups(&stores.pool).await.map_err(op_error)?;
        let mut notes = Vec::new();
        for report in [tables, backups] {
            notes.extend(report.repaired.into_iter().map(|line| format!("restored {line}")));
            notes.extend(report.needs_attention);
        }
        Ok(Done { notes })
    }
}

fn op_error(e: OpError) -> MigrationError {
    match e {
        OpError::Validation(m)
        | OpError::AlreadyExists(m)
        | OpError::NotFound(m)
        | OpError::HasDependents(m)
        | OpError::Internal(m) => MigrationError::Store(m),
        OpError::Database { message, sqlstate } => MigrationError::Database { message, sqlstate },
    }
}

/// Whether a table of this name exists.
async fn table_exists(pool: &SqlitePool, name: &str) -> OpResult<bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)")
        .bind(name)
        .fetch_one(pool)
        .await
        .map_err(|e| OpError::Internal(format!("look up table {name}: {e}")))
}

/// The sort key elements of `key_schema` with no attribute definition.
fn missing_sort_keys<'a>(key_schema: &'a [KeySchemaElement], attr_defs: &[AttributeDefinition]) -> Vec<&'a KeySchemaElement> {
    key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Range)
        .filter(|ks| !attr_defs.iter().any(|ad| ad.attribute_name == ks.attribute_name))
        .collect()
}

/// The PRIMARY KEY columns of the data table of `table_id`, in key order, or
/// `None` when the data table does not exist.
async fn primary_key_columns(pool: &SqlitePool, table_id: &str) -> OpResult<Option<Vec<String>>> {
    // PRAGMA takes no bind parameters, so the id is checked as a UUID before
    // it is interpolated.
    if uuid::Uuid::parse_str(table_id).is_err() || !table_exists(pool, &format!("_ddb_{table_id}")).await? {
        return Ok(None);
    }
    let mut key_columns: Vec<(i64, String)> = sqlx::query_as::<_, (i64, String, String, i64, Option<String>, i64)>(
        &format!("PRAGMA table_info(\"_ddb_{table_id}\")"),
    )
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("read the columns of _ddb_{table_id}: {e}")))?
    .into_iter()
    .filter(|(.., pk_pos)| *pk_pos > 0)
    .map(|(_, name, _, _, _, pk_pos)| (pk_pos, name))
    .collect();
    key_columns.sort_unstable();
    Ok(Some(key_columns.into_iter().map(|(_, name)| name).collect()))
}

/// Repair the `tables` rows. The partition key note is reported on every run
/// while the definition stays absent.
async fn repair_table_key_definitions(pool: &SqlitePool) -> OpResult<Repair> {
    let mut report = Repair::default();
    if !table_exists(pool, "tables").await? {
        return Ok(report);
    }
    let rows: Vec<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT account_id, table_name, table_id, key_schema, attribute_definitions \
         FROM tables ORDER BY table_name",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Cannot read tables: {e}")))?;

    for (account_id, table_name, table_id, ks_json, ad_json) in rows {
        let key_schema: Vec<KeySchemaElement> = match serde_json::from_str(&ks_json) {
            Ok(v) => v,
            Err(e) => {
                report.needs_attention.push(format!("{table_name}: unreadable key_schema ({e})"));
                continue;
            }
        };
        let attr_defs: Vec<AttributeDefinition> = serde_json::from_str(&ad_json).unwrap_or_default();

        let pk_missing: Vec<&str> = key_schema
            .iter()
            .filter(|ks| ks.key_type == KeyType::Hash)
            .filter(|ks| !attr_defs.iter().any(|ad| ad.attribute_name == ks.attribute_name))
            .map(|ks| ks.attribute_name.as_str())
            .collect();
        if !pk_missing.is_empty() {
            report.needs_attention.push(format!(
                "{table_name}: partition key definition(s) [{}] are absent; reads and writes are \
                 unaffected, but index key type validation cannot check them",
                pk_missing.join(", ")
            ));
        }

        let missing = missing_sort_keys(&key_schema, &attr_defs);
        if missing.is_empty() {
            continue;
        }
        if uuid::Uuid::parse_str(&table_id).is_err() {
            report.needs_attention.push(format!("{table_name}: table_id {table_id:?} is not a UUID"));
            continue;
        }
        let columns = primary_key_columns(pool, &table_id).await?.unwrap_or_default();
        let recovered = recover_sort_key_definitions(&key_schema, &attr_defs, &columns);
        if recovered.is_empty() {
            report.needs_attention.push(format!(
                "{table_name}: sort key(s) [{}] have no attribute definition and no matching \
                 PRIMARY KEY column was found to recover the type from",
                names(&missing)
            ));
            continue;
        }
        let mut merged = attr_defs;
        merged.extend(recovered.iter().cloned());
        let merged_json = serde_json::to_string(&merged)
            .map_err(|e| OpError::Internal(format!("Cannot serialize definitions: {e}")))?;
        sqlx::query("UPDATE tables SET attribute_definitions = ?1 WHERE account_id = ?2 AND table_name = ?3")
            .bind(&merged_json)
            .bind(&account_id)
            .bind(&table_name)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Cannot repair {table_name}: {e}")))?;
        for def in &recovered {
            report.repaired.push(format!("{table_name}: {} ({:?})", def.attribute_name, def.attribute_type));
        }
    }
    Ok(report)
}

fn names(elements: &[&KeySchemaElement]) -> String {
    elements.iter().map(|ks| ks.attribute_name.as_str()).collect::<Vec<_>>().join(", ")
}

/// The type of `attribute` in one of the backup's items, from its typed
/// value (`{"S": ...}`, `{"N": ...}` or `{"B": ...}`).
async fn type_from_items(pool: &SqlitePool, backup_arn: &str, attribute: &str) -> OpResult<Option<ScalarAttributeType>> {
    let item: Option<String> = sqlx::query_scalar("SELECT item_data FROM backup_items WHERE backup_arn = ? LIMIT 1")
        .bind(backup_arn)
        .fetch_optional(pool)
        .await
        .map_err(|e| OpError::Internal(format!("read an item of backup {backup_arn}: {e}")))?;
    let Some(item) = item else {
        return Ok(None);
    };
    let value: serde_json::Value = serde_json::from_str(&item).unwrap_or_default();
    let descriptor = value
        .get(attribute)
        .and_then(serde_json::Value::as_object)
        .and_then(|typed| typed.keys().next().cloned());
    Ok(match descriptor.as_deref() {
        Some("S") => Some(ScalarAttributeType::S),
        Some("N") => Some(ScalarAttributeType::N),
        Some("B") => Some(ScalarAttributeType::B),
        _ => None,
    })
}

/// Repair the definitions stored with backups, so a backup taken before the
/// fix restores like its table. RestoreTableFromBackup validates a stored
/// definition as CreateTable does, so a backup left unrepaired fails to
/// restore rather than bringing the damage back.
async fn repair_backups(pool: &SqlitePool) -> OpResult<Repair> {
    let mut report = Repair::default();
    if !table_exists(pool, "backups").await? {
        return Ok(report);
    }
    let rows: Vec<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT backup_arn, table_name, table_id, key_schema, attribute_definitions \
         FROM backups ORDER BY backup_arn",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Cannot read backups: {e}")))?;

    for (backup_arn, table_name, table_id, ks_json, ad_json) in rows {
        let label = format!("backup {backup_arn} of {table_name}");
        let Ok(key_schema) = serde_json::from_str::<Vec<KeySchemaElement>>(&ks_json) else {
            report.needs_attention.push(format!("{label}: unreadable key_schema"));
            continue;
        };
        let attr_defs: Vec<AttributeDefinition> = serde_json::from_str(&ad_json).unwrap_or_default();
        let missing = missing_sort_keys(&key_schema, &attr_defs);
        if missing.is_empty() {
            continue;
        }
        let mut recovered = Vec::new();
        for element in &missing {
            if let Some(attribute_type) = type_from_items(pool, &backup_arn, &element.attribute_name).await? {
                recovered.push(AttributeDefinition {
                    attribute_name: element.attribute_name.clone(),
                    attribute_type,
                });
            }
        }
        if recovered.len() < missing.len() {
            let columns = primary_key_columns(pool, &table_id).await?.unwrap_or_default();
            let known: Vec<AttributeDefinition> = attr_defs.iter().chain(recovered.iter()).cloned().collect();
            recovered.extend(recover_sort_key_definitions(&key_schema, &known, &columns));
        }
        if recovered.len() < missing.len() {
            report.needs_attention.push(format!(
                "{label}: sort key(s) [{}] have no attribute definition, and neither its items nor \
                 its source table give the type; restoring it fails until the definition is added",
                names(&missing)
            ));
            continue;
        }
        let mut merged = attr_defs;
        merged.extend(recovered.iter().cloned());
        let merged_json = serde_json::to_string(&merged)
            .map_err(|e| OpError::Internal(format!("Cannot serialize definitions: {e}")))?;
        sqlx::query("UPDATE backups SET attribute_definitions = ? WHERE backup_arn = ?")
            .bind(&merged_json)
            .bind(&backup_arn)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Cannot repair {label}: {e}")))?;
        for def in &recovered {
            report.repaired.push(format!("{label}: {} ({:?})", def.attribute_name, def.attribute_type));
        }
    }
    Ok(report)
}
