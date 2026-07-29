// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `UpdateTable` attribute-definition lifecycle across GSI add and remove.
//!
//! `AttributeDefinitions` supplied with a `GlobalSecondaryIndexUpdate` is not a
//! replacement for the table's stored set. The stored set is merged with the
//! request, then pruned to the attributes the table key schema or a surviving
//! index still references. That produces four observable behaviours, each
//! covered below:
//!
//! * sequential GSI adds accumulate definitions rather than overwriting them;
//! * a definition for an attribute no key or index uses is dropped;
//! * redeclaring an existing key attribute with a different type is accepted
//!   and the stored type is kept;
//! * removing a GSI drops the definitions only that index used.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, GlobalSecondaryIndexUpdate, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType, CreateGlobalSecondaryIndexAction,
    DeleteGlobalSecondaryIndexAction,
};
use std::collections::BTreeMap;

/// Create a fresh hash-only table for a single test and return its name.
async fn fresh_table(label: &str) -> String {
    let c = client();
    let name = format!("UtAttrDefs{label}{}", ts());
    c.create_table()
        .table_name(&name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .expect("table creation must succeed");
    wait_for_active(client(), &name).await;
    name
}

/// Add a GSI keyed on `attr` (type S), declaring `extra_defs` alongside it.
async fn add_gsi(table: &str, index: &str, attr: &str, extra_defs: Vec<AttributeDefinition>) {
    let c = client();
    let mut req = c
        .update_table()
        .table_name(table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(attr)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        );
    for d in extra_defs {
        req = req.attribute_definitions(d);
    }
    req.global_secondary_index_updates(
        GlobalSecondaryIndexUpdate::builder()
            .create(
                CreateGlobalSecondaryIndexAction::builder()
                    .index_name(index)
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name(attr)
                            .key_type(KeyType::Hash)
                            .build()
                            .unwrap(),
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .build(),
    )
    .send()
    .await
    .unwrap_or_else(|e| panic!("adding GSI {index} must succeed: {}", err_msg(&e)));
    wait_for_active(client(), table).await;
}

/// Stored attribute definitions as a name -> type map.
async fn stored_defs(table: &str) -> BTreeMap<String, String> {
    let c = client();
    let d = c
        .describe_table()
        .table_name(table)
        .send()
        .await
        .expect("describe must succeed");
    d.table()
        .expect("table description")
        .attribute_definitions()
        .iter()
        .map(|ad| {
            (
                ad.attribute_name().to_owned(),
                ad.attribute_type().as_str().to_owned(),
            )
        })
        .collect()
}

/// Each GSI add contributes its key attribute; earlier definitions survive.
#[tokio::test]
async fn sequential_gsi_adds_accumulate_attribute_definitions() {
    let table = fresh_table("Seq").await;
    add_gsi(&table, "g1idx", "g1", vec![]).await;
    add_gsi(&table, "g2idx", "g2", vec![]).await;
    add_gsi(&table, "g3idx", "g3", vec![]).await;

    let defs = stored_defs(&table).await;
    for attr in ["pk", "g1", "g2", "g3"] {
        assert!(
            defs.contains_key(attr),
            "definition for {attr} must survive later GSI adds, got: {defs:?}"
        );
    }
}

/// A definition for an attribute no key or index references is not stored.
#[tokio::test]
async fn unused_attribute_definition_supplied_with_a_gsi_add_is_dropped() {
    let table = fresh_table("Unused").await;
    add_gsi(
        &table,
        "usedidx",
        "used",
        vec![AttributeDefinition::builder()
            .attribute_name("neverIndexed")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap()],
    )
    .await;

    let defs = stored_defs(&table).await;
    assert!(
        defs.contains_key("used"),
        "the index key definition must be stored, got: {defs:?}"
    );
    assert!(
        !defs.contains_key("neverIndexed"),
        "a definition no key or index uses must be dropped, got: {defs:?}"
    );
}

/// Redeclaring an existing key attribute with a different type is accepted, and
/// the stored type is kept so a live index key cannot be retyped underneath.
#[tokio::test]
async fn conflicting_redeclaration_keeps_the_stored_attribute_type() {
    let table = fresh_table("Conflict").await;
    let c = client();

    // Declare pk as N while adding a GSI: pk is already stored as S.
    c.update_table()
        .table_name(&table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("cg")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_index_updates(
            GlobalSecondaryIndexUpdate::builder()
                .create(
                    CreateGlobalSecondaryIndexAction::builder()
                        .index_name("cgidx")
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("cg")
                                .key_type(KeyType::Hash)
                                .build()
                                .unwrap(),
                        )
                        .projection(
                            Projection::builder()
                                .projection_type(ProjectionType::All)
                                .build(),
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("a conflicting redeclaration must be accepted");
    wait_for_active(client(), &table).await;

    let defs = stored_defs(&table).await;
    assert_eq!(
        defs.get("pk").map(String::as_str),
        Some("S"),
        "the stored type for pk must win over the redeclaration, got: {defs:?}"
    );
}

/// Removing a GSI drops the definitions only that index referenced, and keeps
/// the ones the table key or a surviving index still uses.
#[tokio::test]
async fn removing_a_gsi_drops_only_its_own_attribute_definitions() {
    let table = fresh_table("Remove").await;
    add_gsi(&table, "keepidx", "keepAttr", vec![]).await;
    add_gsi(&table, "dropidx", "dropAttr", vec![]).await;

    let c = client();
    c.update_table()
        .table_name(&table)
        .global_secondary_index_updates(
            GlobalSecondaryIndexUpdate::builder()
                .delete(
                    DeleteGlobalSecondaryIndexAction::builder()
                        .index_name("dropidx")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("removing a GSI must succeed");
    wait_for_active(client(), &table).await;

    let defs = stored_defs(&table).await;
    assert!(
        !defs.contains_key("dropAttr"),
        "the removed index's definition must be dropped, got: {defs:?}"
    );
    assert!(
        defs.contains_key("keepAttr") && defs.contains_key("pk"),
        "definitions still referenced must survive, got: {defs:?}"
    );
}
