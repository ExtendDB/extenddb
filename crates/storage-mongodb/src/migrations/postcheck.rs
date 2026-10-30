// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The check after migration 1: the collections and indexes a store has,
//! compared with the definitions migration 1 creates in an empty store
//! (`migrations/<store>/0001_baseline.expected.json`).
//!
//! A collection is described by its type and its `listCollections` options,
//! an index by its key, in order, and every option `listIndexes` reports
//! except the index version `v` (unique, TTL seconds, sparse, partial
//! filter, collation). Numbers compare by value, so an Int32 and a Double 1
//! are the same direction. Only the collections the expected file names
//! are read, and only the indexes it names on them: a store also holds
//! per-table collections and indexes that no migration creates.

use std::collections::BTreeMap;

use bson::{Bson, Document, doc};
use extenddb_storage::migrations::MigrationError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::MongoStores;

/// One collection's definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Collection {
    /// `collection`, `view` or `timeseries`.
    #[serde(rename = "type")]
    pub kind: String,
    pub options: Value,
    pub indexes: BTreeMap<String, Index>,
}

/// One index's definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    /// The key's fields and directions, in order.
    pub key: Vec<(String, Value)>,
    /// Every other option, by name.
    pub options: BTreeMap<String, Value>,
}

/// A store's definitions, by collection name.
pub type Definitions = BTreeMap<String, Collection>;

/// A BSON value as JSON, with every integral number as an integer.
fn json(value: &Bson) -> Value {
    match value {
        Bson::Int32(i) => Value::from(i64::from(*i)),
        Bson::Int64(i) => Value::from(*i),
        #[allow(clippy::cast_possible_truncation)]
        Bson::Double(f) if f.fract() == 0.0 && f.abs() < 9.0e15 => Value::from(*f as i64),
        Bson::Double(f) => serde_json::json!(f),
        Bson::String(s) => Value::from(s.clone()),
        Bson::Boolean(b) => Value::from(*b),
        Bson::Null => Value::Null,
        Bson::Document(d) => object(d),
        Bson::Array(items) => Value::Array(items.iter().map(json).collect()),
        other => Value::from(other.to_string()),
    }
}

fn object(d: &Document) -> Value {
    Value::Object(d.iter().map(|(k, v)| (k.clone(), json(v))).collect())
}

/// The definitions of the named collections that exist in `db`.
///
/// # Errors
///
/// A request failed.
pub async fn describe(
    stores: &MongoStores,
    db: &mongodb::Database,
    names: &[String],
) -> Result<Definitions, MigrationError> {
    let listed = stores
        .request(format!("list the collections of {}", db.name()), async {
            db.run_command(doc! {
                "listCollections": 1,
                "filter": { "name": { "$in": names } },
                "cursor": { "batchSize": 1000 },
            })
            .await
        })
        .await?;
    let mut definitions = Definitions::new();
    for spec in first_batch(&listed, db.name())? {
        let name = spec.get_str("name").unwrap_or_default().to_owned();
        let kind = spec.get_str("type").unwrap_or("collection").to_owned();
        let options = spec
            .get_document("options")
            .map_or(Value::Object(serde_json::Map::new()), object);
        let indexes = if kind == "collection" {
            list_indexes(stores, db, &name).await?
        } else {
            BTreeMap::new()
        };
        definitions.insert(
            name,
            Collection {
                kind,
                options,
                indexes,
            },
        );
    }
    Ok(definitions)
}

/// Every collection of `db` except the per-table ones (`_ddb_*` and
/// `_backup_*`) and the ledger, which the runner creates: what migration 1
/// leaves in an empty store, for the expected files and the schema dump.
///
/// # Errors
///
/// A request failed.
#[cfg(feature = "test-util")]
pub async fn describe_store(
    stores: &MongoStores,
    db: &mongodb::Database,
) -> Result<Definitions, MigrationError> {
    let names = stores
        .request(format!("list the collections of {}", db.name()), async {
            db.list_collection_names().await
        })
        .await?;
    let names: Vec<String> = names
        .into_iter()
        .filter(|n| !n.starts_with("_ddb_") && !n.starts_with("_backup_") && n != super::LEDGER)
        .collect();
    describe(stores, db, &names).await
}

async fn list_indexes(
    stores: &MongoStores,
    db: &mongodb::Database,
    collection: &str,
) -> Result<BTreeMap<String, Index>, MigrationError> {
    let listed = stores
        .request(
            format!("list the indexes of {}.{collection}", db.name()),
            async {
                db.run_command(doc! { "listIndexes": collection, "cursor": { "batchSize": 1000 } })
                    .await
            },
        )
        .await?;
    let mut indexes = BTreeMap::new();
    for spec in first_batch(&listed, db.name())? {
        let name = spec.get_str("name").unwrap_or_default().to_owned();
        let key = spec
            .get_document("key")
            .map(|k| k.iter().map(|(f, d)| (f.clone(), json(d))).collect())
            .unwrap_or_default();
        let options = spec
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "v" | "key" | "name" | "ns"))
            .map(|(k, v)| (k.clone(), json(v)))
            .collect();
        indexes.insert(name, Index { key, options });
    }
    Ok(indexes)
}

/// The documents of a command's cursor, which must be complete in its first
/// batch: the lists read here are a few dozen entries long.
fn first_batch<'a>(reply: &'a Document, db: &str) -> Result<Vec<&'a Document>, MigrationError> {
    let cursor = reply
        .get_document("cursor")
        .map_err(|e| MigrationError::Store(format!("read the definitions in {db}: {e}")))?;
    if cursor.get_i64("id").unwrap_or(0) != 0 {
        return Err(MigrationError::Store(format!(
            "read the definitions in {db}: the server returned them in more than one batch"
        )));
    }
    Ok(cursor
        .get_array("firstBatch")
        .map(|batch| batch.iter().filter_map(Bson::as_document).collect())
        .unwrap_or_default())
}

/// Compare `actual` with the definitions `expected` names. Returns the
/// first difference, naming the object.
fn compare(expected: &Definitions, actual: &Definitions, store: &str) -> Option<String> {
    for (name, want) in expected {
        let Some(have) = actual.get(name) else {
            return Some(format!("{store}: the collection {name} is missing"));
        };
        if have.kind != want.kind || have.options != want.options {
            return Some(format!(
                "{store}: the collection {name} is a {} with options {}; migration 1 creates a {} \
                 with options {}",
                have.kind, have.options, want.kind, want.options
            ));
        }
        for (index, want_index) in &want.indexes {
            match have.indexes.get(index) {
                None => return Some(format!("{store}: the index {index} on {name} is missing")),
                Some(have_index) if have_index != want_index => {
                    return Some(format!(
                        "{store}: the index {index} on {name} has key {:?} and options {:?}; \
                         migration 1 creates key {:?} and options {:?}",
                        have_index.key, have_index.options, want_index.key, want_index.options
                    ));
                }
                Some(_) => {}
            }
        }
    }
    None
}

/// Check `db` against the expected definitions, after migration 1's body
/// ran in it for the first time.
///
/// # Errors
///
/// A difference, naming the object, or a request failed.
pub(crate) async fn check(
    stores: &MongoStores,
    db: &mongodb::Database,
    expected: &str,
    store: &str,
) -> Result<(), MigrationError> {
    let expected: Definitions = serde_json::from_str(expected).map_err(|e| {
        MigrationError::Store(format!(
            "the expected definitions of {store} cannot be read: {e}"
        ))
    })?;
    let names: Vec<String> = expected.keys().cloned().collect();
    let actual = describe(stores, db, &names).await?;
    match compare(&expected, &actual, store) {
        None => Ok(()),
        Some(difference) => Err(MigrationError::Store(format!(
            "after migration 1, {difference}; nothing was recorded for this store"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn index(key: &[(&str, i64)], options: &[(&str, Value)]) -> Index {
        Index {
            key: key
                .iter()
                .map(|(f, d)| ((*f).to_owned(), json!(d)))
                .collect(),
            options: options
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.clone()))
                .collect(),
        }
    }

    fn collection(indexes: &[(&str, Index)]) -> Collection {
        Collection {
            kind: "collection".to_owned(),
            options: json!({}),
            indexes: indexes
                .iter()
                .map(|(n, i)| ((*n).to_owned(), i.clone()))
                .collect(),
        }
    }

    #[test]
    fn numbers_compare_by_value_and_keys_keep_their_order() {
        assert_eq!(json(&Bson::Double(1.0)), json(&Bson::Int32(1)));
        assert_eq!(json(&Bson::Int64(540)), json!(540));
        let d = doc! { "b": 1, "a": -1 };
        let key: Vec<(String, Value)> = d.iter().map(|(f, v)| (f.clone(), json(v))).collect();
        assert_eq!(
            key,
            vec![("b".to_owned(), json!(1)), ("a".to_owned(), json!(-1))]
        );
    }

    #[test]
    fn a_difference_names_the_object_and_unnamed_objects_are_ignored() {
        let unique = index(&[("a", 1)], &[("unique", json!(true))]);
        let expected: Definitions =
            [("c".to_owned(), collection(&[("a_1", unique.clone())]))].into();
        let mut actual = expected.clone();
        actual.insert("_ddb_x".to_owned(), collection(&[]));
        actual
            .get_mut("c")
            .unwrap()
            .indexes
            .insert("extra_1".to_owned(), index(&[("extra", 1)], &[]));
        assert_eq!(compare(&expected, &actual, "s"), None);

        let plain = index(&[("a", 1)], &[]);
        actual
            .get_mut("c")
            .unwrap()
            .indexes
            .insert("a_1".to_owned(), plain);
        let difference = compare(&expected, &actual, "s").unwrap();
        assert!(difference.contains("the index a_1 on c"), "{difference}");

        actual.get_mut("c").unwrap().indexes.remove("a_1");
        assert_eq!(
            compare(&expected, &actual, "s").unwrap(),
            "s: the index a_1 on c is missing"
        );
        let swapped = index(&[("b", 1), ("a", 1)], &[]);
        let ordered = index(&[("a", 1), ("b", 1)], &[]);
        let expected: Definitions = [("c".to_owned(), collection(&[("i", ordered)]))].into();
        let actual: Definitions = [("c".to_owned(), collection(&[("i", swapped)]))].into();
        assert!(compare(&expected, &actual, "s").is_some());

        let mut capped = collection(&[]);
        capped.options = json!({ "capped": true });
        let expected: Definitions = [("c".to_owned(), collection(&[]))].into();
        let actual: Definitions = [("c".to_owned(), capped)].into();
        assert!(
            compare(&expected, &actual, "s")
                .unwrap()
                .contains("the collection c is a")
        );
        assert_eq!(
            compare(&expected, &Definitions::new(), "s").unwrap(),
            "s: the collection c is missing"
        );
    }

    #[test]
    fn the_expected_files_name_every_collection_migration_1_creates() {
        let catalog: Definitions = serde_json::from_str(super::super::CATALOG_EXPECTED).unwrap();
        let data: Definitions = serde_json::from_str(super::super::DATA_EXPECTED).unwrap();
        assert_eq!(catalog.len(), 19);
        assert!(catalog.contains_key("extenddb_servers"));
        assert!(!catalog.contains_key(super::super::LEDGER));
        assert_eq!(
            data.keys().map(String::as_str).collect::<Vec<_>>(),
            ["idempotency_tokens", "stream_records", "stream_shards"]
        );
        let unique = &data["idempotency_tokens"].indexes["account_id_1_token_1"];
        assert_eq!(unique.options.get("unique"), Some(&json!(true)));
    }
}
