// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Table and index shapes the read-options tests run against.

use extenddb_core::types::{
    AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType, TableKeyInfo,
};

pub(super) const ACCOUNT: &str = "000000000000";
pub(super) const ORDERS: &str = "Orders";
pub(super) const USERS: &str = "Users";
pub(super) const GSI: &str = "ByCategory";
pub(super) const EVENTS: &str = "Events";
pub(super) const LSI: &str = "ByCreated";

fn key_element(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn string_attribute(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    }
}

pub(super) fn gsi_info() -> IndexInfo {
    IndexInfo {
        index_name: GSI.to_owned(),
        index_id: "idx-by-category".to_owned(),
        index_type: IndexType::Gsi,
        key_schema: vec![key_element("category", KeyType::Hash)],
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
    }
}

/// A local secondary index on `Events`: the table's partition key with
/// `created` as the sort key. The service serves an LSI read strongly
/// consistent on request. An LSI needs a table with a range key, which is why
/// it sits on `Events` and not on `Orders`.
pub(super) fn lsi_info() -> IndexInfo {
    IndexInfo {
        index_name: LSI.to_owned(),
        index_id: "idx-by-created".to_owned(),
        index_type: IndexType::Lsi,
        key_schema: vec![
            key_element("pk", KeyType::Hash),
            key_element("created", KeyType::Range),
        ],
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
    }
}

/// The three tables: `Orders` (hash key `pk`, the GSI), `Users` (hash key
/// `id`, no index) and `Events` (hash key `pk`, range key `sk`, the LSI).
pub(super) fn table_info(name: &str) -> TableKeyInfo {
    let (key_schema, mut attribute_definitions, gsis, lsis) = match name {
        ORDERS => (
            vec![key_element("pk", KeyType::Hash)],
            vec![string_attribute("pk"), string_attribute("category")],
            vec![gsi_info()],
            Vec::new(),
        ),
        USERS => (
            vec![key_element("id", KeyType::Hash)],
            vec![string_attribute("id")],
            Vec::new(),
            Vec::new(),
        ),
        EVENTS => (
            vec![
                key_element("pk", KeyType::Hash),
                key_element("sk", KeyType::Range),
            ],
            vec![string_attribute("pk"), string_attribute("sk")],
            Vec::new(),
            vec![lsi_info()],
        ),
        other => panic!("no fixture table named {other}"),
    };
    if !lsis.is_empty() {
        attribute_definitions.push(string_attribute("created"));
    }
    TableKeyInfo {
        table_name: name.to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_id: format!("tbl-{name}"),
        key_schema: key_schema.clone(),
        base_key_schema: key_schema,
        attribute_definitions,
        has_lsi: !lsis.is_empty(),
        global_secondary_indexes: gsis,
        local_secondary_indexes: lsis,
        stream_specification: None,
        vector_indexes: Vec::new(),
    }
}
