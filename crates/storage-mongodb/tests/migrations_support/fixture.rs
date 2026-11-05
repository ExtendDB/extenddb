// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Released catalogs, as `tests/fixtures/released/dump.js` prints them, and
//! their loading into a deployment's databases.
//!
//! The fixtures are canonical Extended JSON. They are read here with every
//! object's key order kept, because MongoDB compares embedded documents,
//! such as the compound `_id` of `tables` and `indexes`, field by field in
//! order, and an index key's order is the index.

use std::collections::BTreeMap;

use bson::{Bson, Document, doc};
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

use super::Deployment;

/// A JSON value with its objects' key order kept.
enum Json {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Ordered;
        impl<'de> Visitor<'de> for Ordered {
            type Value = Json;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("JSON")
            }
            fn visit_unit<E>(self) -> Result<Json, E> {
                Ok(Json::Null)
            }
            fn visit_bool<E>(self, v: bool) -> Result<Json, E> {
                Ok(Json::Bool(v))
            }
            fn visit_i64<E>(self, v: i64) -> Result<Json, E> {
                Ok(Json::Number(v.into()))
            }
            fn visit_u64<E>(self, v: u64) -> Result<Json, E> {
                Ok(Json::Number(v.into()))
            }
            fn visit_f64<E>(self, v: f64) -> Result<Json, E> {
                Ok(serde_json::Number::from_f64(v).map_or(Json::Null, Json::Number))
            }
            fn visit_str<E>(self, v: &str) -> Result<Json, E> {
                Ok(Json::String(v.to_owned()))
            }
            fn visit_string<E>(self, v: String) -> Result<Json, E> {
                Ok(Json::String(v))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Json, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(Json::Array(items))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
                let mut fields = Vec::new();
                while let Some((k, v)) = map.next_entry()? {
                    fields.push((k, v));
                }
                Ok(Json::Object(fields))
            }
        }
        deserializer.deserialize_any(Ordered)
    }
}

impl Json {
    fn plain(&self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(b) => (*b).into(),
            Self::Number(n) => serde_json::Value::Number(n.clone()),
            Self::String(s) => s.clone().into(),
            Self::Array(items) => items.iter().map(Self::plain).collect(),
            Self::Object(fields) => serde_json::Value::Object(
                fields.iter().map(|(k, v)| (k.clone(), v.plain())).collect(),
            ),
        }
    }

    /// The BSON value: an object whose first key starts with `$` is an
    /// Extended JSON wrapper (`$numberInt`, `$date`, `$oid`, ...), whose own
    /// key order does not matter; every other object keeps its order.
    fn bson(&self) -> Bson {
        match self {
            Self::Object(fields) if fields.first().is_some_and(|(k, _)| k.starts_with('$')) => {
                Bson::try_from(self.plain()).expect("an Extended JSON value")
            }
            Self::Object(fields) => {
                Bson::Document(fields.iter().map(|(k, v)| (k.clone(), v.bson())).collect())
            }
            Self::Array(items) => Bson::Array(items.iter().map(Self::bson).collect()),
            Self::String(s) => Bson::String(s.clone()),
            Self::Bool(b) => Bson::Boolean(*b),
            Self::Null => Bson::Null,
            Self::Number(n) => n
                .as_i64()
                .map_or_else(|| Bson::Double(n.as_f64().unwrap_or(0.0)), Bson::Int64),
        }
    }
}

/// One collection of a fixture.
pub struct Collection {
    pub name: String,
    pub options: Document,
    pub indexes: Vec<Document>,
    pub documents: Vec<Document>,
}

/// A released catalog: each database's collections.
pub struct Fixture {
    pub databases: BTreeMap<String, Vec<Collection>>,
}

/// The fixture `tests/fixtures/released/<name>.json`.
///
/// # Panics
///
/// The file is missing or is not a fixture.
pub fn read(name: &str) -> Fixture {
    let path = format!(
        "{}/tests/fixtures/released/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let root = match serde_json::from_str::<Json>(&text)
        .expect("fixture JSON")
        .bson()
    {
        Bson::Document(d) => d,
        other => panic!("{path}: {other:?}"),
    };
    let mut databases = BTreeMap::new();
    for (name, database) in root.get_document("databases").expect("databases") {
        let collections = database
            .as_document()
            .and_then(|d| d.get_array("collections").ok())
            .expect("collections")
            .iter()
            .map(|c| {
                let c = c.as_document().expect("collection");
                let docs = |field: &str| -> Vec<Document> {
                    c.get_array(field)
                        .expect(field)
                        .iter()
                        .map(|d| d.as_document().expect("document").clone())
                        .collect()
                };
                Collection {
                    name: c.get_str("name").expect("name").to_owned(),
                    options: c.get_document("options").cloned().unwrap_or_default(),
                    indexes: docs("indexes"),
                    documents: docs("documents"),
                }
            })
            .collect();
        databases.insert(name.clone(), collections);
    }
    Fixture { databases }
}

/// Load `fixture` into `deployment`'s databases, which must be empty:
/// `extenddb_catalog` into its catalog database, `extenddb_data` into its
/// data database.
pub async fn load(deployment: &Deployment, fixture: &Fixture) {
    for (name, collections) in &fixture.databases {
        let db = match name.as_str() {
            "extenddb_catalog" => deployment.catalog(),
            "extenddb_data" => deployment.data(),
            other => panic!("the fixture names the database {other}"),
        };
        for c in collections {
            let mut create = doc! { "create": &c.name };
            create.extend(c.options.clone());
            db.run_command(create).await.expect("create");
            let indexes: Vec<Bson> = c
                .indexes
                .iter()
                .filter(|i| i.get_str("name") != Ok("_id_"))
                .map(|i| Bson::Document(i.clone()))
                .collect();
            if !indexes.is_empty() {
                db.run_command(doc! { "createIndexes": &c.name, "indexes": indexes })
                    .await
                    .expect("createIndexes");
            }
            if !c.documents.is_empty() {
                db.collection::<Document>(&c.name)
                    .insert_many(c.documents.clone())
                    .await
                    .expect("insert");
            }
        }
    }
}
