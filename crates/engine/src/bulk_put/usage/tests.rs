// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serde_json::json;

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, IndexInfo, IndexType, KeySchemaElement, KeyType,
    Projection, ProjectionType, ScalarAttributeType, item_size_bytes,
};

fn key(name: &str) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type: KeyType::Hash,
    }
}

fn item(entries: &[(&str, &str)]) -> Item {
    let mut item = Item::new();
    for (name, value) in entries {
        item.insert((*name).to_owned(), AttributeValue::S((*value).to_owned()));
    }
    item
}

#[test]
fn twenty_five_items_of_70_bytes_report_25600_bytes() {
    // The measured case: 25 items of 70 bytes each round to one
    // kilobyte apiece.
    let mut usage = UsageAccum::default();
    for _ in 0..25 {
        usage.add_base(70);
    }
    assert_eq!(usage.table_bytes, 25_600);
}

#[test]
fn base_bytes_are_the_logical_size_with_a_one_kilobyte_floor() {
    // Measured on the service: 512 charges 1,024; 1,500
    // charges 1,500 (not 2,048); 2,048 charges 2,048; 2,049 charges
    // 2,049 (not 3,072).
    assert_eq!(charged_bytes(0), 1024);
    assert_eq!(charged_bytes(512), 1024);
    assert_eq!(charged_bytes(1024), 1024);
    assert_eq!(charged_bytes(1500), 1500);
    assert_eq!(charged_bytes(2048), 2048);
    assert_eq!(charged_bytes(2049), 2049);
    let mut usage = UsageAccum::default();
    for _ in 0..200 {
        usage.add_base(1500);
    }
    assert_eq!(usage.table_bytes, 300_000, "200 rows of 1,500 bytes");
}

fn info_with_indexes(gsi_projection: ProjectionType, lsi: bool) -> TableKeyInfo {
    let projection = |projection_type| Projection {
        projection_type,
        non_key_attributes: None,
    };
    TableKeyInfo {
        table_name: "orders".to_owned(),
        account_id: "111122223333".to_owned(),
        table_id: "t-1".to_owned(),
        key_schema: vec![key("pk")],
        base_key_schema: vec![key("pk")],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }],
        has_lsi: lsi,
        global_secondary_indexes: vec![IndexInfo {
            index_name: "gsi1".to_owned(),
            index_id: "i-1".to_owned(),
            index_type: IndexType::Gsi,
            key_schema: vec![key("g1")],
            projection: projection(gsi_projection),
        }],
        local_secondary_indexes: if lsi {
            vec![IndexInfo {
                index_name: "lsi1".to_owned(),
                index_id: "i-2".to_owned(),
                index_type: IndexType::Lsi,
                key_schema: vec![key("pk"), key("l1")],
                projection: projection(ProjectionType::All),
            }]
        } else {
            Vec::new()
        },
        stream_specification: None,
        vector_indexes: Vec::new(),
    }
}

fn row_of_1500_bytes() -> Item {
    let mut row = item(&[("pk", "f7-0001"), ("g1", "g00001"), ("l1", "l00001")]);
    let fixed = item_size_bytes(&row) + "pad".len();
    row.insert(
        "pad".to_owned(),
        AttributeValue::S("p".repeat(1500 - fixed)),
    );
    assert_eq!(item_size_bytes(&row), 1500);
    row
}

#[test]
fn keys_only_index_on_a_large_row_charges_the_floor_and_the_table_its_bytes() {
    // 1,500 byte rows, KEYS_ONLY index. Table 1,500 per row,
    // the index 1,024 per row, the top-level figure the sum.
    let info = info_with_indexes(ProjectionType::KeysOnly, false);
    let row = row_of_1500_bytes();
    let mut usage = UsageAccum::default();
    usage.add_base(item_size_bytes(&row));
    usage.add_index_writes(None, Some(&row), &info);
    assert_eq!(usage.table_bytes, 1500);
    assert_eq!(usage.gsi.get("gsi1"), Some(&1024));
    let value = usage.to_value("arn:aws:dynamodb:us-east-1:111122223333:table/orders");
    assert_eq!(
        value["BulkPutBytes"], 2524,
        "top level is table plus indexes"
    );
    assert_eq!(value["Table"]["BulkPutBytes"], 1500);
}

#[test]
fn all_projection_indexes_on_a_large_row_charge_its_bytes() {
    // 1,500 byte rows with an ALL projection GSI and LSI charge
    // 1,500 per row on the table and on each index; the top level is
    // three times that.
    let info = info_with_indexes(ProjectionType::All, true);
    let row = row_of_1500_bytes();
    let mut usage = UsageAccum::default();
    usage.add_base(item_size_bytes(&row));
    usage.add_index_writes(None, Some(&row), &info);
    assert_eq!(usage.gsi.get("gsi1"), Some(&1500));
    assert_eq!(usage.lsi.get("lsi1"), Some(&1500));
    let value = usage.to_value("arn:aws:dynamodb:us-east-1:111122223333:table/orders");
    assert_eq!(value["BulkPutBytes"], 4500);
}

#[test]
fn an_index_no_row_projects_into_is_absent_from_the_usage() {
    // A GSI whose key attribute no row carries has no entry at
    // all, so the wire value carries no GlobalSecondaryIndexes member.
    let info = info_with_indexes(ProjectionType::All, false);
    let row = item(&[("pk", "a")]);
    let mut usage = UsageAccum::default();
    usage.add_base(item_size_bytes(&row));
    usage.add_index_writes(None, Some(&row), &info);
    assert!(usage.gsi.is_empty());
    let value = usage.to_value("arn:aws:dynamodb:us-east-1:111122223333:table/orders");
    assert!(
        value
            .get("GlobalSecondaryIndexes")
            .is_none_or(|m| m.as_object().is_some_and(serde_json::Map::is_empty))
    );
    assert_eq!(value["BulkPutBytes"], 1024);
}

#[test]
fn index_writes_land_in_the_index_maps_not_the_table_figure() {
    let info = TableKeyInfo {
        table_name: "orders".to_owned(),
        account_id: "111122223333".to_owned(),
        table_id: "t-1".to_owned(),
        key_schema: vec![key("pk")],
        base_key_schema: vec![key("pk")],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }],
        has_lsi: false,
        global_secondary_indexes: vec![IndexInfo {
            index_name: "gsi1".to_owned(),
            index_id: "i-1".to_owned(),
            index_type: IndexType::Gsi,
            key_schema: vec![key("gsi_pk")],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
        }],
        local_secondary_indexes: Vec::new(),
        stream_specification: None,
        vector_indexes: Vec::new(),
    };
    let written = item(&[("pk", "a"), ("gsi_pk", "b")]);

    let mut usage = UsageAccum::default();
    usage.add_base(item_size_bytes(&written));
    usage.add_index_writes(None, Some(&written), &info);

    assert_eq!(usage.table_bytes, 1024, "the base charge alone");
    assert_eq!(usage.gsi.get("gsi1"), Some(&1024));
    assert!(usage.lsi.is_empty());
    assert!(usage.vector.is_empty());

    // An item outside the sparse index charges the index nothing.
    let outside = item(&[("pk", "c")]);
    usage.add_index_writes(None, Some(&outside), &info);
    assert_eq!(usage.gsi.get("gsi1"), Some(&1024));
}

#[test]
fn usage_round_trips_through_the_stored_json() {
    let mut usage = UsageAccum {
        table_bytes: 25_600,
        ..UsageAccum::default()
    };
    usage.gsi.insert("gsi1".to_owned(), 4096);
    usage.lsi.insert("lsi1".to_owned(), 2048);
    usage.vector.insert("vi1".to_owned(), 8192);

    let value = usage.to_value("arn:aws:dynamodb:us-east-1:111122223333:table/orders");
    assert_eq!(UsageAccum::restore(&value), usage);
}

#[test]
fn usage_value_carries_the_wire_shape() {
    let mut usage = UsageAccum {
        table_bytes: 25_600,
        ..UsageAccum::default()
    };
    usage.vector.insert("vi1".to_owned(), 8192);

    let arn = "arn:aws:dynamodb:us-east-1:111122223333:table/orders";
    let value = usage.to_value(arn);
    assert_eq!(
        value["TableName"], "orders",
        "the service carries the bare table name, not the ARN"
    );
    assert_eq!(
        value["BulkPutBytes"], 25_600,
        "no secondary index: the total is the table figure"
    );
    assert_eq!(value["Table"]["BulkPutBytes"], 25_600);
    assert_eq!(
        value["VectorIndexes"]["vi1"]["BulkPutVectorBytes"], 8192,
        "vector bytes carry their own member name and stay out of the top-level figure"
    );
}

#[test]
fn a_new_run_renders_the_bare_table_name() {
    // The runner passes the table ARN; the usage entry carries the bare
    // name the service emits (measured on the service).
    let usage = UsageAccum {
        table_bytes: 1024,
        ..UsageAccum::default()
    };
    let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/wt1";
    let wire = usage_from_value(&usage.to_value(arn), "wt1");
    assert_eq!(wire.table_name, "wt1");
}

#[test]
fn a_stored_record_holding_an_arn_renders_the_bare_name() {
    // A record written before this fix stored the ARN in TableName; the
    // renderer substitutes the bare name after the final `table/`.
    let stored = json!({
        "TableName": "arn:aws:dynamodb:us-east-1:123456789012:table/wt1",
        "BulkPutBytes": 1024,
        "Table": { "BulkPutBytes": 1024 }
    });
    let wire = usage_from_value(&stored, "wt1");
    assert_eq!(wire.table_name, "wt1");
}

#[test]
fn a_no_index_run_has_exactly_table_name_bulk_put_bytes_and_table() {
    // A run on a table whose indexes received nothing carries no index
    // map (measured on the service): the usage entry is exactly
    // TableName, BulkPutBytes and Table.
    let usage = UsageAccum {
        table_bytes: 1024,
        ..UsageAccum::default()
    };
    let value = usage.to_value("arn:aws:dynamodb:us-east-1:123456789012:table/wt1");
    let mut keys: Vec<&str> = value
        .as_object()
        .expect("usage is an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["BulkPutBytes", "Table", "TableName"]);
}

#[test]
fn a_stored_record_holding_empty_index_maps_renders_without_them() {
    // A record stored with empty index maps renders without those
    // members: usage_from_value reads them as empty and the empty maps
    // are omitted on serialization.
    let stored = json!({
        "TableName": "wt1",
        "BulkPutBytes": 1024,
        "Table": { "BulkPutBytes": 1024 },
        "GlobalSecondaryIndexes": {},
        "LocalSecondaryIndexes": {},
        "VectorIndexes": {}
    });
    let wire = usage_from_value(&stored, "wt1");
    let value = serde_json::to_value(&wire).expect("usage serializes");
    let object = value.as_object().expect("usage is an object");
    assert!(!object.contains_key("GlobalSecondaryIndexes"));
    assert!(!object.contains_key("LocalSecondaryIndexes"));
    assert!(!object.contains_key("VectorIndexes"));
    assert_eq!(object.len(), 3, "only TableName, BulkPutBytes and Table");
}

#[test]
fn usage_restores_zeroes_from_the_initial_empty_object() {
    let restored = UsageAccum::restore(&json!({}));
    assert_eq!(restored, UsageAccum::default());
}

#[test]
fn the_reader_and_the_accumulator_agree_on_every_member_name() {
    // One value written by the accumulator, read back by the wire
    // reader: every figure must land in the same member it left from.
    let mut usage = UsageAccum {
        table_bytes: 25_600,
        ..UsageAccum::default()
    };
    usage.gsi.insert("gsi1".to_owned(), 4096);
    usage.lsi.insert("lsi1".to_owned(), 2048);
    usage.vector.insert("vi1".to_owned(), 8192);

    let arn = "arn:aws:dynamodb:us-east-1:111122223333:table/orders";
    let wire = usage_from_value(&usage.to_value(arn), "orders");
    assert_eq!(wire.table_name, "orders");
    assert_eq!(
        wire.bulk_put_bytes,
        25_600 + 4096 + 2048,
        "the top level sums the table and every secondary index"
    );
    assert_eq!(wire.table.bulk_put_bytes, 25_600);
    assert_eq!(wire.global_secondary_indexes["gsi1"].bulk_put_bytes, 4096);
    assert_eq!(wire.local_secondary_indexes["lsi1"].bulk_put_bytes, 2048);
    assert_eq!(wire.vector_indexes["vi1"].bulk_put_vector_bytes, 8192);
}
