// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data relations checked against the catalog, for `extenddb catalog-check`.
//!
//! Every data relation this backend creates for a table is named from a
//! catalog id by [`table_storage_relations`], the list `DeleteTable` drops,
//! and every such name starts with [`STORAGE_RELATION_PREFIX`]. The check
//! lists the relations with that prefix in the data database and compares
//! them with the names the catalog's ids give, for rows in any status.
//!
//! The order of the reads keeps the check correct while a server elsewhere
//! changes tables. `CreateTable` and a vector index add commit their catalog
//! rows before they create relations. An `UpdateTable` that adds a GSI
//! creates the relation first and commits the row after it, inside a catalog
//! transaction that holds `ROW EXCLUSIVE` on `indexes` from its insert on.
//!
//! - Orphans come from a listing taken before the catalog is read. A
//!   relation created after the listing is not in it, and one that a
//!   catalog-first path created before the listing already has its row.
//! - Missing relations come from a listing taken after the catalog is read,
//!   so a table whose row committed just before the read is not reported
//!   missing for the moment before its relations exist.
//! - The drop holds `SHARE` locks on `tables`, `indexes` and
//!   `vector_indexes`. Taking them waits for an `UpdateTable` whose GSI
//!   relation exists and whose row has not committed, and holding them keeps
//!   new rows out until the drop ends. Under the locks the catalog is read
//!   again, and only reported relations that no row names are dropped.
//!
//! `migrate`'s advisory lock would not help: servers do not hold it, so it
//! does not exclude a running `CreateTable` or `UpdateTable`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use extenddb_storage::diagnostics::{CatalogRelation, OrphanDrop, StorageRelationCheck};
use sqlx::{PgConnection, PgPool};

use super::ddl::set_lock_timeout_until;
use super::{
    STORAGE_RELATION_PREFIX, data_table_name, index_table_name, table_storage_relations,
    vector_table_name,
};

/// How long `catalog-check --fix` waits, in all, for catalog changes in
/// flight to finish and for the relations it drops.
pub(crate) const FIX_LOCK_BUDGET: Duration = Duration::from_secs(30);

/// Every catalog row that names data relations, with its table's row. One
/// statement, so the three tables are read from one snapshot.
const CATALOG_ROWS: &str = "\
    SELECT 'table' AS kind, t.table_id, NULL::text AS index_id, NULL::text AS index_name, \
           NULL::text AS index_status, t.account_id, t.table_name, t.table_status \
      FROM tables t \
    UNION ALL \
    SELECT 'index', i.table_id, i.index_id, i.index_name, i.index_status, \
           t.account_id, t.table_name, t.table_status \
      FROM indexes i LEFT JOIN tables t ON t.table_id = i.table_id \
    UNION ALL \
    SELECT 'vector', v.table_id, v.index_id, v.index_name, v.index_status, \
           t.account_id, t.table_name, t.table_status \
      FROM vector_indexes v LEFT JOIN tables t ON t.table_id = v.table_id";

type CatalogRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// A relation name the catalog gives, and the row that gives it.
struct Named {
    owner: String,
    /// Whether the relation should exist: the row and its table are present
    /// and neither is being deleted.
    expected: bool,
}

/// One table's catalog rows, keyed by the relation each one names.
#[derive(Default)]
struct TableRows {
    index_ids: Vec<String>,
    vector_index_ids: Vec<String>,
    owners: BTreeMap<String, Named>,
}

/// A relation name as `pg_tables` lists it, without the quotes the naming
/// functions put around it.
fn bare(quoted: &str) -> String {
    quoted.trim_matches('"').to_owned()
}

/// Every relation name the catalog gives, unquoted, for rows in any status.
async fn named_relations(
    catalog: &mut PgConnection,
) -> Result<BTreeMap<String, Named>, sqlx::Error> {
    let rows: Vec<CatalogRow> = sqlx::query_as(CATALOG_ROWS).fetch_all(catalog).await?;
    let mut tables: BTreeMap<String, TableRows> = BTreeMap::new();
    for (kind, table_id, index_id, index_name, index_status, account, name, status) in rows {
        let table = match (account, name) {
            (Some(account), Some(name)) => format!("table {account}/{name}"),
            _ => format!("table id {table_id}"),
        };
        let table_live = status.is_some_and(|s| s != "DELETING");
        let entry = tables.entry(table_id.clone()).or_default();
        match (kind.as_str(), index_id) {
            ("index" | "vector", Some(index_id)) => {
                let vector = kind == "vector";
                let relation = if vector {
                    entry.vector_index_ids.push(index_id.clone());
                    vector_table_name(&index_id)
                } else {
                    entry.index_ids.push(index_id.clone());
                    index_table_name(&index_id)
                };
                let index = index_name.unwrap_or(index_id);
                let noun = if vector { "vector index" } else { "index" };
                entry.owners.insert(
                    bare(&relation),
                    Named {
                        owner: format!("{noun} {index} of {table}"),
                        expected: table_live && index_status.is_some_and(|s| s != "DELETING"),
                    },
                );
            }
            ("table", _) => {
                entry.owners.insert(
                    bare(&data_table_name(&table_id)),
                    Named {
                        owner: table,
                        expected: table_live,
                    },
                );
            }
            // `index_id` is NOT NULL in both index tables.
            _ => {}
        }
    }

    // The names come from the list DeleteTable drops, so a relation family
    // added there is covered here too. The owners above only label them.
    let mut named = BTreeMap::new();
    for (table_id, mut entry) in tables {
        for relation in
            table_storage_relations(&table_id, &entry.index_ids, &entry.vector_index_ids)
        {
            let relation = bare(&relation);
            let owner = entry.owners.remove(&relation).unwrap_or_else(|| Named {
                owner: format!("table id {table_id}"),
                expected: false,
            });
            named.insert(relation, owner);
        }
    }
    Ok(named)
}

/// The data relations this backend created that the data database holds now.
async fn storage_relations(data: &PgPool) -> Result<BTreeSet<String>, sqlx::Error> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT tablename::text FROM pg_tables \
         WHERE schemaname = current_schema() AND starts_with(tablename::text, $1)",
    )
    .bind(STORAGE_RELATION_PREFIX)
    .fetch_all(data)
    .await?;
    Ok(rows.into_iter().map(|(name,)| name).collect())
}

/// Compare the data database's relations with the catalog.
pub(crate) async fn check(
    catalog: &PgPool,
    data: &PgPool,
) -> Result<StorageRelationCheck, sqlx::Error> {
    let before = storage_relations(data).await?;
    let named = named_relations(&mut *catalog.acquire().await?).await?;
    let after = storage_relations(data).await?;
    let orphaned = before
        .into_iter()
        .filter(|relation| !named.contains_key(relation))
        .collect();
    let missing = named
        .into_iter()
        .filter(|(relation, row)| row.expected && !after.contains(relation))
        .map(|(relation, row)| CatalogRelation {
            relation,
            owner: row.owner,
        })
        .collect();
    Ok(StorageRelationCheck { orphaned, missing })
}

/// Drop the relations in `orphaned` that no catalog row names once catalog
/// changes are held off, waiting `budget` in all.
///
/// Fails only when the catalog cannot be locked or read. A relation that
/// cannot be dropped gets [`OrphanDrop::Failed`] and the rest still run.
pub(crate) async fn drop_orphaned(
    catalog: &PgPool,
    data: &PgPool,
    orphaned: Vec<String>,
    budget: Duration,
) -> Result<Vec<(String, OrphanDrop)>, sqlx::Error> {
    if orphaned.is_empty() {
        return Ok(Vec::new());
    }
    let deadline = Instant::now() + budget;
    let mut hold = catalog.begin().await?;
    set_lock_timeout_until(&mut hold, deadline).await?;
    sqlx::query("LOCK TABLE tables, indexes, vector_indexes IN SHARE MODE")
        .execute(&mut *hold)
        .await?;
    let named = named_relations(&mut hold).await?;

    let mut outcomes = Vec::with_capacity(orphaned.len());
    for relation in orphaned {
        let outcome = if !relation.starts_with(STORAGE_RELATION_PREFIX) {
            OrphanDrop::Kept("not a data relation of this backend".to_owned())
        } else if let Some(row) = named.get(&relation) {
            OrphanDrop::Kept(format!("the catalog names it: {}", row.owner))
        } else {
            match drop_relation(data, &relation, deadline).await {
                Ok(()) => OrphanDrop::Dropped,
                Err(e) => OrphanDrop::Failed(e.to_string()),
            }
        };
        outcomes.push((relation, outcome));
    }

    // The transaction only locked and read; ending it releases the locks.
    hold.rollback().await?;
    Ok(outcomes)
}

async fn drop_relation(
    data: &PgPool,
    relation: &str,
    deadline: Instant,
) -> Result<(), sqlx::Error> {
    let mut tx = data.begin().await?;
    set_lock_timeout_until(&mut tx, deadline).await?;
    let quoted = format!("\"{}\"", relation.replace('"', "\"\""));
    sqlx::query(&format!("DROP TABLE IF EXISTS {quoted}"))
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_storage_relation_name_carries_the_listing_prefix() {
        let names = table_storage_relations(
            "8f2b5c1e-0000-4000-8000-000000000001",
            &["8f2b5c1e-0000-4000-8000-000000000002".to_owned()],
            &["8f2b5c1e-0000-4000-8000-000000000003".to_owned()],
        );
        assert_eq!(names.len(), 3);
        for name in names {
            assert!(
                bare(&name).starts_with(STORAGE_RELATION_PREFIX),
                "{name} would be invisible to catalog-check"
            );
        }
    }

    #[test]
    fn bare_removes_the_identifier_quotes() {
        assert_eq!(bare(&data_table_name("abc")), "_ddb_abc");
        assert_eq!(bare(&vector_table_name("abc")), "_ddb_vec_abc");
    }
}
