// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! CreateTable refusals of a malformed table or index `KeySchema`, a local
//! secondary index fault, an unused attribute definition and a PROVISIONED
//! table without `ProvisionedThroughput`, with the texts the service gives.
//!
//! Measured in us-east-1 on 13 and 14 Nov 2026. Each request is an on-demand
//! table keyed on `pk`, or on `pk` and `sk` for the local index rows, with
//! every key attribute defined as S and the index named `idx` unless the row
//! names another.

use std::collections::BTreeSet;

use super::*;
use crate::types::{LsiInput, StreamSpecification, StreamViewType};

/// `"pk HASH, sk RANGE"` as key schema elements.
fn keys(spec: &str) -> Vec<KeySchemaElement> {
    spec.split(", ")
        .map(|element| {
            let (name, key_type) = element.split_once(' ').expect("name and key type");
            let key_type = match key_type {
                "HASH" => KeyType::Hash,
                "RANGE" => KeyType::Range,
                other => panic!("key type {other}"),
            };
            make_ks(name, key_type)
        })
        .collect()
}

/// An on-demand table with the given key schemas (name and elements for each
/// index, projection ALL), every key attribute defined as S.
fn table(table_keys: &str, gsis: &[(&str, &str)], lsis: &[(&str, &str)]) -> CreateTableInput {
    let projection = || Projection {
        projection_type: ProjectionType::All,
        non_key_attributes: None,
    };
    let key_schema = keys(table_keys);
    let gsis: Vec<GsiInput> = gsis
        .iter()
        .map(|(name, spec)| GsiInput {
            index_name: (*name).to_owned(),
            key_schema: keys(spec),
            projection: projection(),
            provisioned_throughput: None,
            warm_throughput: None,
        })
        .collect();
    let lsis: Vec<LsiInput> = lsis
        .iter()
        .map(|(name, spec)| LsiInput {
            index_name: (*name).to_owned(),
            key_schema: keys(spec),
            projection: projection(),
        })
        .collect();
    let definitions = key_schema
        .iter()
        .chain(gsis.iter().flat_map(|gsi| &gsi.key_schema))
        .chain(lsis.iter().flat_map(|lsi| &lsi.key_schema))
        .map(|element| element.attribute_name.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|name| make_ad(name, ScalarAttributeType::S))
        .collect();
    let mut input = base_input(key_schema, definitions);
    input.global_secondary_indexes = (!gsis.is_empty()).then_some(gsis);
    input.local_secondary_indexes = (!lsis.is_empty()).then_some(lsis);
    input
}

/// A table keyed on `pk` with one GSI `idx`.
fn gsi(spec: &str) -> CreateTableInput {
    table("pk HASH", &[("idx", spec)], &[])
}

/// A table keyed on `pk` and `sk` with one LSI `idx`.
fn lsi(spec: &str) -> CreateTableInput {
    table("pk HASH, sk RANGE", &[], &[("idx", spec)])
}

/// A table keyed on `pk` with a second, unused, attribute definition.
fn unused_definition(stream: Option<StreamSpecification>) -> CreateTableInput {
    let mut input = table("pk HASH", &[], &[]);
    input
        .attribute_definitions
        .push(make_ad("unused", ScalarAttributeType::S));
    input.stream_specification = stream;
    input
}

const INVALID: &str = "One or more parameter values were invalid: ";
const FIRST_NOT_HASH: &str = "Invalid KeySchema: The first KeySchemaElement is not a HASH key type";
const HASH_BEFORE_RANGE: &str = "One or more parameter values were invalid: All HASH key \
                                 attributes must precede RANGE key attributes in the KeySchema";

/// The length refusal of the key `spec`, with its elements written as the
/// service writes them.
fn too_long(spec: &str, max: usize) -> String {
    let elements: Vec<String> = spec
        .split(", ")
        .map(|element| {
            let (name, key_type) = element.split_once(' ').expect("name and key type");
            format!("KeySchemaElement(attributeName={name}, keyType={key_type})")
        })
        .collect();
    format!(
        "1 validation error detected: Value '[{}]' at 'keySchema' failed to satisfy \
         constraint: Member must have length less than or equal to {max}",
        elements.join(", ")
    )
}

/// Every measured request with the service's text: a label, the request and
/// the expected message.
fn rows() -> Vec<(&'static str, CreateTableInput, String)> {
    let six_lsis: Vec<(String, String)> = (0..6)
        .map(|n| (format!("lsi{n}"), format!("pk HASH, alt{n} RANGE")))
        .collect();
    let six_lsis: Vec<(&str, &str)> = six_lsis
        .iter()
        .map(|(name, spec)| (name.as_str(), spec.as_str()))
        .collect();
    let mut provisioned = table("pk HASH", &[], &[]);
    provisioned.billing_mode = Some(BillingMode::Provisioned);
    let stream = StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    };
    let nine =
        "h0 HASH, h1 HASH, h2 HASH, h3 HASH, r0 RANGE, r1 RANGE, r2 RANGE, r3 RANGE, r4 RANGE";
    vec![
        (
            "table [sk RANGE, pk HASH]",
            table("sk RANGE, pk HASH", &[], &[]),
            FIRST_NOT_HASH.to_owned(),
        ),
        (
            "gsi [b RANGE, a HASH]",
            gsi("b RANGE, a HASH"),
            FIRST_NOT_HASH.to_owned(),
        ),
        ("gsi [r0 RANGE]", gsi("r0 RANGE"), FIRST_NOT_HASH.to_owned()),
        (
            "lsi [l RANGE, pk HASH]",
            lsi("l RANGE, pk HASH"),
            FIRST_NOT_HASH.to_owned(),
        ),
        ("lsi [l1 RANGE]", lsi("l1 RANGE"), FIRST_NOT_HASH.to_owned()),
        (
            "lsi [pk HASH]",
            lsi("pk HASH"),
            format!("{INVALID}Index KeySchema does not have a range key for index: idx"),
        ),
        (
            "lsi [pk HASH, l1 HASH]",
            lsi("pk HASH, l1 HASH"),
            "Invalid KeySchema: The second KeySchemaElement is not a RANGE key type".to_owned(),
        ),
        (
            "gsi [a HASH, b RANGE, c HASH]",
            gsi("a HASH, b RANGE, c HASH"),
            HASH_BEFORE_RANGE.to_owned(),
        ),
        (
            "gsi [a HASH, b RANGE, c RANGE, d HASH]",
            gsi("a HASH, b RANGE, c RANGE, d HASH"),
            HASH_BEFORE_RANGE.to_owned(),
        ),
        (
            "gsi [a HASH, b HASH, c RANGE, d HASH]",
            gsi("a HASH, b HASH, c RANGE, d HASH"),
            HASH_BEFORE_RANGE.to_owned(),
        ),
        (
            "gsi [a HASH, b RANGE, c HASH, d RANGE]",
            gsi("a HASH, b RANGE, c HASH, d RANGE"),
            HASH_BEFORE_RANGE.to_owned(),
        ),
        (
            "gsi five HASH",
            gsi("h0 HASH, h1 HASH, h2 HASH, h3 HASH, h4 HASH"),
            format!(
                "{INVALID}The KeySchema exceeds the maximum allowed number of HASH key attributes"
            ),
        ),
        (
            "gsi one HASH, five RANGE",
            gsi("h0 HASH, r0 RANGE, r1 RANGE, r2 RANGE, r3 RANGE, r4 RANGE"),
            format!(
                "{INVALID}The KeySchema exceeds the maximum allowed number of RANGE key attributes"
            ),
        ),
        ("gsi nine elements", gsi(nine), too_long(nine, 8)),
        (
            "lsi [pk HASH, l1 RANGE, l2 RANGE]",
            lsi("pk HASH, l1 RANGE, l2 RANGE"),
            too_long("pk HASH, l1 RANGE, l2 RANGE", 2),
        ),
        (
            "lsi [pk HASH, b RANGE, c HASH]",
            lsi("pk HASH, b RANGE, c HASH"),
            too_long("pk HASH, b RANGE, c HASH", 2),
        ),
        (
            "lsi byOther [other HASH, sk RANGE]",
            table(
                "pk HASH, sk RANGE",
                &[],
                &[("byOther", "other HASH, sk RANGE")],
            ),
            format!(
                "{INVALID}Index KeySchema does not have the same leading hash key as table \
                 KeySchema for index: byOther. index hash key: other, table hash key: pk"
            ),
        ),
        (
            "six lsis",
            table("pk HASH, sk RANGE", &[], &six_lsis),
            format!("{INVALID}Number of LocalSecondaryIndexes exceeds per-table limit of 5"),
        ),
        (
            "unused definition",
            unused_definition(None),
            format!(
                "{INVALID}Number of attributes in KeySchema does not exactly match number of \
                 attributes defined in AttributeDefinitions"
            ),
        ),
        (
            "unused definition with a stream",
            unused_definition(Some(stream)),
            format!(
                "{INVALID}Number of attributes in KeySchema does not exactly match number of \
                 attributes defined in AttributeDefinitions"
            ),
        ),
        (
            "PROVISIONED without ProvisionedThroughput",
            provisioned,
            format!(
                "{INVALID}ReadCapacityUnits and WriteCapacityUnits must both be specified when \
                 BillingMode is PROVISIONED"
            ),
        ),
    ]
}

#[test]
fn the_measured_requests_get_the_service_texts() {
    let mismatches: Vec<String> = rows()
        .iter()
        .filter_map(|(label, input, expected)| {
            let answer = match validate_create_table(input, &LimitsConfig::default()) {
                Err(DynamoDbError::ValidationException(message)) => message,
                other => format!("{other:?}"),
            };
            (answer != *expected).then(|| format!("{label}: {answer}"))
        })
        .collect();
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

/// The requests the rows are built from are otherwise well formed: the same
/// table with a valid index and a defined key is accepted, so each refusal
/// above comes from the fault the row names.
#[test]
fn the_well_formed_shapes_are_accepted() {
    let limits = LimitsConfig::default();
    for input in [
        table("pk HASH", &[], &[]),
        gsi("a HASH, b RANGE"),
        gsi("h0 HASH, h1 HASH, h2 HASH, h3 HASH, r0 RANGE, r1 RANGE, r2 RANGE, r3 RANGE"),
        lsi("pk HASH, l1 RANGE"),
    ] {
        validate_create_table(&input, &limits).expect("a well formed table is accepted");
    }
}
