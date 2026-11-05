// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 3, `sort_key_definitions`, PostgreSQL: restore sort key
//! attribute definitions that the pre-fix `UpdateTable` deleted (#259), in
//! `tables` rows and in the definitions stored with backups.
//!
//! Before the fix, `UpdateTable` replaced a table's stored attribute
//! definitions with the request's subset, so adding a GSI dropped the table's
//! own key definitions. A table's sort key type is read back from its data
//! table's PRIMARY KEY column (`sk_s`, `sk_n` or `sk_b`) in the data
//! database. A backup's is read from the typed value in one of its items, or
//! else from its source table. A partition key definition the same write
//! dropped cannot be recovered, because the partition key column is always
//! TEXT; it is a note. Nothing here stops the migration: a definition it
//! cannot repair is a note, and the table or backup keeps failing loudly
//! until a person fixes it. The writes go to the catalog, in the runner's
//! transaction; the data database is only read.
//!
//! This file's checksum is pinned once the migration is released. It is
//! excluded from rustfmt and clippy so neither can require an edit, and keeps
//! its own small helpers instead of calling the crate's, which may change.

use async_trait::async_trait;
use extenddb_core::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use extenddb_storage::util::recover_sort_key_definitions;
use serde_json::Value;

use super::PostgresStores;
use super::connect::sql_error;

/// This file, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0003_sort_key_definitions.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0003_sort_key_definitions/postgres/1";

pub(crate) struct SortKeyDefinitions;

/// What one pass repaired, and what it could not.
#[derive(Default)]
struct Repair {
    repaired: Vec<String>,
    needs_attention: Vec<String>,
}

#[async_trait]
impl CodeBody<PostgresStores> for SortKeyDefinitions {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &PostgresStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &PostgresStores) -> Result<Done, MigrationError> {
        let tables = repair_tables(stores).await?;
        let backups = repair_backups(stores).await?;
        let mut notes = Vec::new();
        for report in [tables, backups] {
            notes.extend(report.repaired.into_iter().map(|line| format!("restored {line}")));
            notes.extend(report.needs_attention);
        }
        Ok(Done { notes })
    }
}

/// The sort key elements of `key_schema` with no attribute definition.
fn missing_sort_keys<'a>(key_schema: &'a [KeySchemaElement], attr_defs: &[AttributeDefinition]) -> Vec<&'a KeySchemaElement> {
    key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Range)
        .filter(|ks| !attr_defs.iter().any(|ad| ad.attribute_name == ks.attribute_name))
        .collect()
}

fn names(elements: &[&KeySchemaElement]) -> String {
    elements.iter().map(|ks| ks.attribute_name.as_str()).collect::<Vec<_>>().join(", ")
}

/// The PRIMARY KEY columns of the data table of `table_id`, in key order;
/// empty when the data table does not exist. `to_regclass` returns NULL for
/// a missing table rather than raising.
async fn primary_key_columns(stores: &PostgresStores, table_id: &str) -> Result<Vec<String>, MigrationError> {
    sqlx::query_scalar(
        "SELECT a.attname::text \
         FROM pg_index i \
         JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
         WHERE i.indrelid = to_regclass($1) AND i.indisprimary \
         ORDER BY k.ord",
    )
    .bind(format!("public.\"_ddb_{table_id}\""))
    .fetch_all(&stores.data)
    .await
    .map_err(|e| sql_error(&format!("read the PRIMARY KEY of _ddb_{table_id}"), &e))
}

/// Repair the `tables` rows. The partition key note is reported on every run
/// while the definition stays absent.
async fn repair_tables(stores: &PostgresStores) -> Result<Repair, MigrationError> {
    let mut report = Repair::default();
    let rows: Vec<(String, String, String, Value, Value)> = sqlx::query_as(
        "SELECT account_id, table_name, table_id, key_schema, attribute_definitions \
         FROM tables ORDER BY table_name, account_id",
    )
    .fetch_all(&stores.catalog)
    .await
    .map_err(|e| sql_error("read tables", &e))?;

    for (account_id, table_name, table_id, ks_json, ad_json) in rows {
        let key_schema: Vec<KeySchemaElement> = match serde_json::from_value(ks_json) {
            Ok(v) => v,
            Err(e) => {
                report.needs_attention.push(format!("{table_name}: unreadable key_schema ({e})"));
                continue;
            }
        };
        let attr_defs: Vec<AttributeDefinition> = serde_json::from_value(ad_json).unwrap_or_default();

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
        let columns = primary_key_columns(stores, &table_id).await?;
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
        let merged_json = serde_json::to_value(&merged)
            .map_err(|e| MigrationError::Store(format!("serialize the definitions of {table_name}: {e}")))?;
        sqlx::query("UPDATE tables SET attribute_definitions = $1 WHERE account_id = $2 AND table_name = $3")
            .bind(&merged_json)
            .bind(&account_id)
            .bind(&table_name)
            .execute(&stores.catalog)
            .await
            .map_err(|e| sql_error(&format!("repair {table_name}"), &e))?;
        for def in &recovered {
            report.repaired.push(format!("{table_name}: {} ({:?})", def.attribute_name, def.attribute_type));
        }
    }
    Ok(report)
}

/// The type of `attribute` in one of the backup's items, from its typed
/// value (`{"S": ...}`, `{"N": ...}` or `{"B": ...}`).
async fn type_from_items(stores: &PostgresStores, backup_arn: &str, attribute: &str) -> Result<Option<ScalarAttributeType>, MigrationError> {
    let item: Option<Value> = sqlx::query_scalar("SELECT item_data FROM backup_items WHERE backup_arn = $1 LIMIT 1")
        .bind(backup_arn)
        .fetch_optional(&stores.catalog)
        .await
        .map_err(|e| sql_error(&format!("read an item of backup {backup_arn}"), &e))?;
    let descriptor = item
        .as_ref()
        .and_then(|value| value.get(attribute))
        .and_then(Value::as_object)
        .and_then(|typed| typed.keys().next().cloned());
    Ok(match descriptor.as_deref() {
        Some("S") => Some(ScalarAttributeType::S),
        Some("N") => Some(ScalarAttributeType::N),
        Some("B") => Some(ScalarAttributeType::B),
        _ => None,
    })
}

/// Repair the definitions stored with backups (`backups.attribute_definitions`),
/// so a backup taken before the fix restores like its table.
/// RestoreTableFromBackup validates a stored definition as CreateTable does,
/// so a backup left unrepaired fails to restore rather than bringing the
/// damage back.
async fn repair_backups(stores: &PostgresStores) -> Result<Repair, MigrationError> {
    let mut report = Repair::default();
    let rows: Vec<(String, String, String, Value, Value)> = sqlx::query_as(
        "SELECT backup_arn, table_name, table_id, key_schema, attribute_definitions \
         FROM backups ORDER BY backup_arn",
    )
    .fetch_all(&stores.catalog)
    .await
    .map_err(|e| sql_error("read backups", &e))?;

    for (backup_arn, table_name, table_id, ks_json, ad_json) in rows {
        let label = format!("backup {backup_arn} of {table_name}");
        let Ok(key_schema) = serde_json::from_value::<Vec<KeySchemaElement>>(ks_json) else {
            report.needs_attention.push(format!("{label}: unreadable key_schema"));
            continue;
        };
        let attr_defs: Vec<AttributeDefinition> = serde_json::from_value(ad_json).unwrap_or_default();
        let missing = missing_sort_keys(&key_schema, &attr_defs);
        if missing.is_empty() {
            continue;
        }
        let mut recovered = Vec::new();
        for element in &missing {
            if let Some(attribute_type) = type_from_items(stores, &backup_arn, &element.attribute_name).await? {
                recovered.push(AttributeDefinition {
                    attribute_name: element.attribute_name.clone(),
                    attribute_type,
                });
            }
        }
        if recovered.len() < missing.len() {
            let columns = primary_key_columns(stores, &table_id).await?;
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
        let merged_json = serde_json::to_value(&merged)
            .map_err(|e| MigrationError::Store(format!("serialize the definitions of {label}: {e}")))?;
        sqlx::query("UPDATE backups SET attribute_definitions = $1 WHERE backup_arn = $2")
            .bind(&merged_json)
            .bind(&backup_arn)
            .execute(&stores.catalog)
            .await
            .map_err(|e| sql_error(&format!("repair {label}"), &e))?;
        for def in &recovered {
            report.repaired.push(format!("{label}: {} ({:?})", def.attribute_name, def.attribute_type));
        }
    }
    Ok(report)
}
