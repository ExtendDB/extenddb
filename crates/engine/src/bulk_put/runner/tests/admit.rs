// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the admit concern of the runner.

use super::super::*;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};

// Usage arithmetic lives in `bulk_put::usage`, tested there.

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

// newImage presence by fired check.

#[test]
fn a_missing_key_fault_carries_the_submitted_image() {
    let missing = item(&[("pk", "a")]);
    assert_eq!(
        item_fault_new_image(PutPayloadFault::MissingKey, &missing, 400 * 1024),
        Some(&missing)
    );
}

#[test]
fn a_type_mismatch_fault_carries_no_image() {
    let present = item(&[("pk", "a"), ("other", "b")]);
    assert_eq!(
        item_fault_new_image(PutPayloadFault::KeyTypeMismatch, &present, 400 * 1024),
        None
    );
}

#[test]
fn an_index_key_fault_carries_the_image() {
    // Measured on the service: wrong-typed and empty-string index key
    // records both embed the submitted item.
    let present = item(&[("pk", "a"), ("g1", "")]);
    assert_eq!(
        item_fault_new_image(PutPayloadFault::IndexKey, &present, 400 * 1024),
        Some(&present)
    );
}

#[test]
fn an_oversized_fault_carries_no_image() {
    let present = item(&[("pk", "a")]);
    assert_eq!(
        item_fault_new_image(PutPayloadFault::Oversized, &present, 400 * 1024),
        None
    );
    assert_eq!(
        item_fault_new_image(PutPayloadFault::Other, &present, 400 * 1024),
        None
    );
}

#[test]
fn a_missing_key_fault_on_an_oversized_item_carries_no_image() {
    let missing = item(&[("pk", "a"), ("big", "bbbbbbbbbb")]);
    assert_eq!(
        item_fault_new_image(PutPayloadFault::MissingKey, &missing, 4),
        None,
        "an item past the size limit is not embedded in the record"
    );
}

// Row admission through the shared write core.

/// A `TableKeyInfo` with string keys `pk` and `sk`, enough for the
/// admission checks.
fn key_info() -> TableKeyInfo {
    TableKeyInfo {
        key_schema: vec![key("pk"), key("sk")],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        ..TableKeyInfo::default()
    }
}

fn limits() -> LimitsConfig {
    LimitsConfig::default()
}

#[test]
fn a_row_with_a_missing_key_and_an_unparseable_condition_reports_the_condition_message_as_put_item_does()
 {
    // handle_put_item resolves the condition fields before validating
    // the item, so the expression message wins when a row carries both
    // faults; the shared core fixes the runner to the same order.
    let row = Row {
        item: item(&[("pk", "a")]),
        condition: Some(RowCondition {
            expression: "attribute_not_exists((".to_owned(),
            names: None,
            values: None,
        }),
        repeated_member_bytes: 0,
    };
    let Admission::Fault { message, kind } = admit_row(&row, &key_info(), &limits()) else {
        panic!("the row must be refused");
    };
    // The record text is the shared parser's message for the same expression
    // without the single-item prefix (measured on the service).
    let put_item = crate::expression_helpers::resolve_condition(
        Some("attribute_not_exists(("),
        None,
        None,
        None,
        None,
        &limits(),
    )
    .expect_err("the condition expression is invalid")
    .to_string();
    let expected = put_item
        .strip_prefix("Invalid ConditionExpression: ")
        .expect("the single-item path prefixes the condition fault");
    assert_eq!(
        message, expected,
        "the condition message wins over the missing key"
    );
    assert_eq!(kind, None, "a condition fault carries no image kind");
}

#[test]
fn a_row_with_a_missing_key_and_a_parseable_condition_reports_the_missing_key() {
    let row = Row {
        item: item(&[("pk", "a")]),
        condition: Some(RowCondition {
            expression: "attribute_not_exists(pk)".to_owned(),
            names: None,
            values: None,
        }),
        repeated_member_bytes: 0,
    };
    let Admission::Fault { message, kind } = admit_row(&row, &key_info(), &limits()) else {
        panic!("the row must be refused");
    };
    assert_eq!(
        message,
        "One or more parameter values were invalid: Missing the key sk in the item"
    );
    assert_eq!(kind, Some(PutPayloadFault::MissingKey));
}

#[test]
fn a_row_whose_index_key_has_the_wrong_type_or_is_empty_fails_the_row_with_the_put_item_message() {
    use extenddb_core::types::{IndexInfo, IndexType, Projection, ProjectionType};
    let mut info = key_info();
    info.attribute_definitions.push(AttributeDefinition {
        attribute_name: "g1".to_owned(),
        attribute_type: ScalarAttributeType::S,
    });
    info.global_secondary_indexes.push(IndexInfo {
        index_name: "gsi1".to_owned(),
        index_id: "gsi1-id".to_owned(),
        index_type: IndexType::Gsi,
        key_schema: vec![key("g1")],
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
    });
    let mut mismatch = item(&[("pk", "a"), ("sk", "b")]);
    mismatch.insert("g1".to_owned(), AttributeValue::N("5".to_owned()));
    let Admission::Fault { message, kind } = admit_row(
        &Row {
            item: mismatch,
            condition: None,
            repeated_member_bytes: 0,
        },
        &info,
        &limits(),
    ) else {
        panic!("the type-mismatched index key must fail the row");
    };
    assert_eq!(
        message,
        "One or more parameter values were invalid: Type mismatch for Index Key g1 Expected: S \
             Actual: N IndexName: gsi1"
    );
    assert_eq!(kind, Some(PutPayloadFault::IndexKey));

    let empty = item(&[("pk", "a"), ("sk", "b"), ("g1", "")]);
    let Admission::Fault { message, .. } = admit_row(
        &Row {
            item: empty,
            condition: None,
            repeated_member_bytes: 0,
        },
        &info,
        &limits(),
    ) else {
        panic!("the empty index key must fail the row");
    };
    assert!(
        message.contains("cannot contain an empty string value. IndexName: gsi1, IndexKey: g1"),
        "{message}"
    );

    // An item without the index attribute is admitted (sparse index).
    let sparse = item(&[("pk", "a"), ("sk", "b")]);
    assert!(matches!(
        admit_row(
            &Row {
                item: sparse,
                condition: None,
                repeated_member_bytes: 0,
            },
            &info,
            &limits()
        ),
        Admission::Write(None)
    ));
}

#[test]
fn a_valid_row_is_admitted_with_its_parsed_condition() {
    let row = Row {
        item: item(&[("pk", "a"), ("sk", "b")]),
        condition: Some(RowCondition {
            expression: "attribute_not_exists(pk)".to_owned(),
            names: None,
            values: None,
        }),
        repeated_member_bytes: 0,
    };
    match admit_row(&row, &key_info(), &limits()) {
        Admission::Write(Some(_)) => {}
        _ => panic!("the row must be admitted with a parsed condition"),
    }
    let plain = Row {
        item: item(&[("pk", "a"), ("sk", "b")]),
        condition: None,
        repeated_member_bytes: 0,
    };
    match admit_row(&plain, &key_info(), &limits()) {
        Admission::Write(None) => {}
        _ => panic!("the plain row must be admitted without a condition"),
    }
}

/// The bytes of the set members the parser removed as repeats count toward
/// the item size (measured on the service), through the same check and at
/// the same place in the order: an item within the limit once deduplicated
/// and over it with its repeats fails as oversized, the same item with no
/// repeats is admitted, and a missing key is still reported first.
#[test]
fn removed_repeated_set_members_count_toward_the_item_size_in_the_put_item_order() {
    let mut doubled = item(&[("pk", "a"), ("sk", "b")]);
    doubled.insert(
        "ss".to_owned(),
        AttributeValue::SS(["x".repeat(250_000)].into_iter().collect()),
    );
    let row = Row {
        item: doubled.clone(),
        condition: None,
        repeated_member_bytes: 250_000,
    };
    let Admission::Fault { message, kind } = admit_row(&row, &key_info(), &limits()) else {
        panic!("the item is over the limit with its repeated member counted");
    };
    assert_eq!(message, "Item size has exceeded the maximum allowed size");
    assert_eq!(kind, Some(PutPayloadFault::Oversized));

    let once = Row {
        repeated_member_bytes: 0,
        ..row
    };
    assert!(matches!(
        admit_row(&once, &key_info(), &limits()),
        Admission::Write(None)
    ));

    doubled.remove("sk");
    let missing = Row {
        item: doubled,
        condition: None,
        repeated_member_bytes: 250_000,
    };
    let Admission::Fault { kind, .. } = admit_row(&missing, &key_info(), &limits()) else {
        panic!("the row must be refused");
    };
    assert_eq!(kind, Some(PutPayloadFault::MissingKey));
}

/// A 65,536-byte attribute name fails the row with the standalone `PutItem`
/// text and the `Other` kind, which carries no image, whether or not a key
/// attribute is also missing: the name check runs ahead of the key checks.
#[test]
fn a_long_attribute_name_is_reported_ahead_of_a_missing_key_with_the_other_kind() {
    let long_name = "n".repeat(65_536);
    let with_keys = item(&[("pk", "a"), ("sk", "b"), (long_name.as_str(), "v")]);
    let without_sk = item(&[("pk", "a"), (long_name.as_str(), "v")]);
    for item in [with_keys, without_sk] {
        let row = Row {
            item,
            condition: None,
            repeated_member_bytes: 0,
        };
        let Admission::Fault { message, kind } = admit_row(&row, &key_info(), &limits()) else {
            panic!("the row must be refused");
        };
        assert_eq!(
            message,
            "1 validation error detected: Attribute name is too large, must be less than 65536 \
             bytes"
        );
        assert_eq!(kind, Some(PutPayloadFault::Other));
        let image = kind
            .and_then(|kind| item_fault_new_image(kind, &row.item, limits().max_item_size_bytes));
        assert_eq!(image, None, "the record carries no image");
    }
}

// Skip-region fault routing.

#[test]
fn only_a_line_over_the_limit_abandons_the_skip_region() {
    assert!(skip_fault_abandons(&SourceError::LineTooLong));
    assert!(skip_fault_abandons(&SourceError::InvalidUtf8));
    assert!(skip_fault_abandons(&SourceError::Corrupt));
}

#[test]
fn io_jail_and_file_type_faults_in_the_skip_region_fail_the_operation() {
    assert!(!skip_fault_abandons(&SourceError::Io(
        std::io::Error::other("disk read failed")
    )));
    assert!(!skip_fault_abandons(&SourceError::Jail(
        "path escapes the account root".to_owned()
    )));
    assert!(!skip_fault_abandons(&SourceError::NotRegularFile));
}
