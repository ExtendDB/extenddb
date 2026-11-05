// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

mod kinds;
mod linear;
mod records;
mod repeats;

// Row parsing: the happy path.

#[test]
fn plain_item_line_parses_with_no_condition() {
    let row = rows::parse_row(r#"{"Item": {"pk": {"S": "a"}, "n": {"N": "1"}}}"#, 1 << 20)
        .unwrap()
        .expect("a plain item line is a row");
    assert_eq!(row.item["pk"], AttributeValue::S("a".to_owned()));
    assert_eq!(row.item["n"], AttributeValue::N("1".to_owned()));
    assert!(row.condition.is_none());
}

#[test]
fn bare_condition_expression_is_carried() {
    let row = rows::parse_row(
        r#"{"Item": {"pk": {"S": "b"}}, "ConditionExpression": "attribute_not_exists(pk)"}"#,
        1 << 20,
    )
    .unwrap()
    .expect("row");
    let condition = row.condition.expect("condition is carried");
    assert_eq!(condition.expression, "attribute_not_exists(pk)");
    assert!(condition.names.is_none());
    assert!(condition.values.is_none());
}

#[test]
fn expression_names_and_values_are_carried() {
    let line = concat!(
        r##"{"Item": {"pk": {"S": "c"}, "v": {"N": "2"}}, "##,
        r##""ConditionExpression": "#v < :max", "##,
        r##""ExpressionAttributeNames": {"#v": "v"}, "##,
        r##""ExpressionAttributeValues": {":max": {"N": "10"}}}"##
    );
    let row = rows::parse_row(line, 1 << 20).unwrap().expect("row");
    let condition = row.condition.expect("condition is carried");
    assert_eq!(condition.expression, "#v < :max");
    let names = condition.names.expect("names are carried");
    assert_eq!(names, HashMap::from([("#v".to_owned(), "v".to_owned())]));
    let values = condition.values.expect("values are carried");
    assert_eq!(
        values,
        HashMap::from([(":max".to_owned(), AttributeValue::N("10".to_owned()))])
    );
}

// Measured on the service: a top-level key outside the four the record shape
// allows abandons the file, and the check runs before the missing-`Item`
// one, so a misspelled `Item` answers the unknown-field message. The
// September reading that such keys are ignored no longer holds.
#[test]
fn an_unknown_top_level_key_abandons_the_file() {
    let error = rows::parse_row(
        r#"{"Item": {"pk": {"S": "a"}}, "SizeBytes": 42, "Comment": "x"}"#,
        1 << 20,
    )
    .unwrap_err();
    assert_eq!(
        error,
        rows::RowError::Parse(rows::UNKNOWN_FIELD_MESSAGE.to_owned())
    );
}

#[test]
fn a_misspelled_item_key_answers_the_unknown_field_message_not_the_missing_item_one() {
    let error = rows::parse_row(r#"{"Itme": {"pk": {"S": "a"}}}"#, 1 << 20).unwrap_err();
    assert_eq!(
        error,
        rows::RowError::Parse(rows::UNKNOWN_FIELD_MESSAGE.to_owned())
    );
    // With no stray key the missing-`Item` message stands.
    let error = rows::parse_row(
        r#"{"ConditionExpression": "attribute_not_exists(pk)"}"#,
        1 << 20,
    )
    .unwrap_err();
    assert_eq!(
        error,
        rows::RowError::Parse(rows::ITEM_MISSING_MESSAGE.to_owned())
    );
}

#[test]
fn the_four_record_keys_together_are_accepted() {
    let row = rows::parse_row(
        r##"{"Item": {"pk": {"S": "a"}}, "ConditionExpression": "#v = :v", "ExpressionAttributeNames": {"#v": "v"}, "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        1 << 20,
    )
    .unwrap()
    .expect("a row with every allowed key parses");
    assert!(row.condition.is_some());
}

// Measured on the service: 63 map levels parse (the row then fails alone at the
// item nesting limit); 64 and 65 abandon the file with the long nesting
// message. The parser reports the bound before the JSON is read, so the
// recursion the parser would otherwise refuse at 128 brackets never runs.
#[test]
fn nesting_at_sixty_three_levels_parses_and_sixty_four_abandons_the_file() {
    assert!(
        rows::parse_row(&nested_item_line(63), 1 << 20)
            .unwrap()
            .is_some()
    );
    for depth in [64, 65, 200] {
        let error = rows::parse_row(&nested_item_line(depth), 1 << 20).unwrap_err();
        assert_eq!(
            error,
            rows::RowError::Parse(rows::NESTING_ABANDON_MESSAGE.to_owned()),
            "depth {depth}"
        );
    }
}

#[test]
fn brackets_inside_string_literals_do_not_count_toward_the_nesting_bound() {
    let brackets = "{[".repeat(200);
    let line = json!({"Item": {"pk": {"S": brackets}, "q": {"S": "a\"{[b"}}}).to_string();
    let row = rows::parse_row(&line, 1 << 20).unwrap().expect("a row");
    assert_eq!(row.item["pk"], AttributeValue::S(brackets));
}

#[test]
fn blank_line_is_skipped() {
    assert!(rows::parse_row("", 1 << 20).unwrap().is_none());
}

#[test]
fn whitespace_only_line_is_skipped() {
    assert!(rows::parse_row(" \t  ", 1 << 20).unwrap().is_none());
}

// Row parsing: every parse-level fault carries the one abandon message.

#[test]
fn invalid_json_abandons_the_file() {
    // Measured on the service: text that is not JSON answers the tokenizer's
    // message, not the type-tag one.
    assert_eq!(abandon_message("not json {"), rows::CORRUPT_STREAM_MESSAGE);
}

#[test]
fn truncated_json_abandons_the_file() {
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "a"#),
        rows::CORRUPT_STREAM_MESSAGE
    );
}

#[test]
fn line_without_an_item_key_abandons_the_file_with_its_own_message() {
    // Measured: a missing Item carries its own message and still follows
    // the parse-level path, so the file is abandoned.
    assert_eq!(
        abandon_message(r#"{"ConditionExpression": "attribute_not_exists(pk)"}"#),
        "Expected 'Item' top level container. Remainder of the file will not be processed."
    );
}

#[test]
fn item_as_an_array_abandons_the_file_with_its_own_message() {
    // Measured: a non-object Item carries its own message and still
    // follows the parse-level path.
    assert_eq!(
        abandon_message(r#"{"Item": [{"S": "a"}]}"#),
        "Expected character {. Remainder of the file will not be processed."
    );
}

#[test]
fn item_as_a_string_abandons_the_file_with_its_own_message() {
    assert_eq!(
        abandon_message(r#"{"Item": "pk"}"#),
        "Expected character {. Remainder of the file will not be processed."
    );
}

#[test]
fn unknown_type_tag_abandons_the_file() {
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"BOOM": true}}}"#),
        ABANDON
    );
}

#[test]
fn two_type_tags_abandon_the_file() {
    // Measured on the service: a value carrying two tags abandons
    // the file with the message naming the closing brace, not the type-tag
    // text this case used to answer.
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "a", "N": "1"}}}"#),
        rows::REPEATED_MEMBER_MESSAGE
    );
}

#[test]
fn empty_attribute_value_abandons_the_file() {
    assert_eq!(abandon_message(r#"{"Item": {"pk": {}}}"#), ABANDON);
}

#[test]
fn a_binary_value_that_is_not_base64_abandons_the_file_with_its_own_message() {
    // Measured on the service: the value did not decode and the file is
    // abandoned, with a message naming the base64 fault.
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "a"}, "b": {"B": "!!!notbase64"}}}"#),
        rows::BINARY_NOT_BASE64_MESSAGE
    );
}

#[test]
fn an_empty_string_set_fails_the_row_with_the_put_item_message() {
    assert_eq!(
        item_message(r#"{"Item": {"pk": {"S": "a"}, "ss": {"SS": []}}}"#),
        "One or more parameter values were invalid: An string set  may not be empty"
    );
}

#[test]
fn an_empty_number_set_fails_the_row_with_the_put_item_message() {
    assert_eq!(
        item_message(r#"{"Item": {"pk": {"S": "a"}, "ns": {"NS": []}}}"#),
        "One or more parameter values were invalid: An number set  may not be empty"
    );
}

#[test]
fn a_duplicate_set_member_is_accepted_and_stored_deduplicated() {
    // Measured on the service: the bulk path writes `["a", "a"]` as `["a"]`,
    // processed and not failed, where PutItem refuses the same value.
    let row = rows::parse_row(
        r#"{"Item": {"pk": {"S": "a"}, "ss": {"SS": ["a", "a"]}, "ns": {"NS": ["1", "2", "1"]}, "m": {"M": {"bs": {"BS": ["AA==", "AA=="]}}}}}"#,
        1 << 20,
    )
    .unwrap()
    .expect("a repeated member is not a fault");
    assert_eq!(
        row.item["ss"],
        AttributeValue::SS(["a".to_owned()].into_iter().collect())
    );
    assert_eq!(
        row.item["ns"],
        AttributeValue::NS(["1".to_owned(), "2".to_owned()].into_iter().collect())
    );
    match &row.item["m"] {
        AttributeValue::M(map) => assert_eq!(
            map["bs"],
            AttributeValue::BS([vec![0u8]].into_iter().collect())
        ),
        other => panic!("map expected, got {other:?}"),
    }
}

#[test]
fn a_set_under_an_attribute_named_like_a_type_tag_is_still_deduplicated() {
    // Measured on the service: the service stores a
    // set held by an attribute literally named `SS`, `NS` or `BS`, and a set
    // nested under a map named `SS`, deduplicated like any other set.
    let row = rows::parse_row(
        r#"{"Item": {"pk": {"S": "a"}, "SS": {"SS": ["x", "x"]}, "NS": {"NS": ["1", "1"]}, "BS": {"BS": ["AQ==", "AQ=="]}, "nested": {"M": {"SS": {"M": {"inner": {"SS": ["y", "y"]}}}}}, "list": {"L": [{"SS": ["z", "z"]}, {"M": {"BS": {"BS": ["AQ==", "AQ=="]}}}]}}}"#,
        1 << 20,
    )
    .unwrap()
    .expect("a repeated member under a tag-named attribute is not a fault");
    assert_eq!(
        row.item["SS"],
        AttributeValue::SS(["x".to_owned()].into_iter().collect())
    );
    assert_eq!(
        row.item["NS"],
        AttributeValue::NS(["1".to_owned()].into_iter().collect())
    );
    assert_eq!(
        row.item["BS"],
        AttributeValue::BS([vec![1u8]].into_iter().collect())
    );
    let AttributeValue::M(nested) = &row.item["nested"] else {
        panic!("map expected")
    };
    let AttributeValue::M(under_ss) = &nested["SS"] else {
        panic!("map expected under the attribute named SS")
    };
    assert_eq!(
        under_ss["inner"],
        AttributeValue::SS(["y".to_owned()].into_iter().collect())
    );
    let AttributeValue::L(list) = &row.item["list"] else {
        panic!("list expected")
    };
    assert_eq!(
        list[0],
        AttributeValue::SS(["z".to_owned()].into_iter().collect())
    );
    let AttributeValue::M(in_list) = &list[1] else {
        panic!("map expected inside the list")
    };
    assert_eq!(
        in_list["BS"],
        AttributeValue::BS([vec![1u8]].into_iter().collect())
    );
}

#[test]
fn a_string_value_containing_nul_fails_the_row() {
    assert_eq!(
        item_message(r#"{"Item": {"pk": {"S": "a\u0000b"}}}"#),
        rows::NUL_IN_STRING_MESSAGE
    );
}

#[test]
fn an_attribute_name_containing_nul_fails_the_row() {
    assert_eq!(
        item_message(r#"{"Item": {"pk": {"S": "a"}, "x\u0000y": {"N": "1"}}}"#),
        rows::NUL_IN_STRING_MESSAGE
    );
}

#[test]
fn nul_nested_in_a_map_inside_a_list_fails_the_row() {
    assert_eq!(
        item_message(
            r#"{"Item": {"pk": {"S": "a"}, "l": {"L": [{"M": {"k": {"SS": ["ok", "no\u0000"]}}}]}}}"#
        ),
        rows::NUL_IN_STRING_MESSAGE
    );
}

#[test]
fn a_binary_value_decoding_to_a_zero_byte_parses() {
    // U+0000 is refused in text; a zero byte in a binary value is bytes
    // and is stored as such.
    let row = rows::parse_row(
        r#"{"Item": {"pk": {"S": "a"}, "b": {"B": "AAA="}}}"#,
        1 << 20,
    )
    .unwrap()
    .expect("binary zero bytes parse");
    assert_eq!(row.item["b"], AttributeValue::B(vec![0, 0]));
}

#[test]
fn line_one_byte_over_the_cap_abandons_the_file() {
    let line = r#"{"Item": {"pk": {"S": "aaaaaaaa"}}}"#;
    match rows::parse_row(line, line.len() - 1) {
        Err(rows::RowError::Parse(message)) => assert_eq!(message, ABANDON),
        other => panic!("expected a parse-level error, got {other:?}"),
    }
}

#[test]
fn line_exactly_at_the_cap_parses() {
    let line = r#"{"Item": {"pk": {"S": "aaaaaaaa"}}}"#;
    let row = rows::parse_row(line, line.len())
        .unwrap()
        .expect("a line exactly at the cap is not over it");
    assert_eq!(row.item["pk"], AttributeValue::S("aaaaaaaa".to_owned()));
}

// Row parsing: item-level faults are not the parser's business.

#[test]
fn row_missing_a_key_attribute_still_parses() {
    // The parser has no table schema; key checks belong to the runner.
    let row = rows::parse_row(r#"{"Item": {"other": {"S": "a"}}}"#, 1 << 20)
        .unwrap()
        .expect("key validation is not the parser's");
    assert_eq!(row.item.len(), 1);
}

#[test]
fn row_with_an_oversized_string_still_parses() {
    // 500 KB exceeds the 400 KB item cap; size checks belong to the runner.
    let big = "a".repeat(500 * 1024);
    let line = format!(r#"{{"Item": {{"pk": {{"S": "{big}"}}}}}}"#);
    let row = rows::parse_row(&line, 1 << 20)
        .unwrap()
        .expect("item size validation is not the parser's");
    assert_eq!(row.item["pk"], AttributeValue::S(big));
}

// Row parsing: expression maps without a condition are an item-level
// fault. The row fails alone with the PutItem message for the same case
// and the file continues, unlike a parse-level fault.

#[test]
fn names_without_an_expression_is_an_item_fault() {
    let line = r##"{"Item": {"pk": {"S": "a"}}, "ExpressionAttributeNames": {"#v": "v"}}"##;
    match rows::parse_row(line, 1 << 20) {
        Err(rows::RowError::Item(message)) => assert_eq!(
            message,
            "ExpressionAttributeNames can only be specified when using expressions"
        ),
        other => panic!("expected an item-level error, got {other:?}"),
    }
}

#[test]
fn values_without_an_expression_is_an_item_fault() {
    let line =
        r#"{"Item": {"pk": {"S": "a"}}, "ExpressionAttributeValues": {":max": {"N": "10"}}}"#;
    match rows::parse_row(line, 1 << 20) {
        Err(rows::RowError::Item(message)) => assert_eq!(
            message,
            "ExpressionAttributeValues can only be specified when using expressions: \
             ConditionExpression is null"
        ),
        other => panic!("expected an item-level error, got {other:?}"),
    }
}

#[test]
fn both_maps_without_an_expression_answer_the_names_message() {
    let line = concat!(
        r##"{"Item": {"pk": {"S": "a"}}, "ExpressionAttributeNames": {"#v": "v"}, "##,
        r##""ExpressionAttributeValues": {":max": {"N": "10"}}}"##
    );
    match rows::parse_row(line, 1 << 20) {
        Err(rows::RowError::Item(message)) => assert_eq!(
            message,
            "ExpressionAttributeNames can only be specified when using expressions"
        ),
        other => panic!("expected an item-level error, got {other:?}"),
    }
}

#[test]
fn empty_names_map_without_an_expression_parses_unconditional() {
    let line = r#"{"Item": {"pk": {"S": "a"}}, "ExpressionAttributeNames": {}}"#;
    let row = rows::parse_row(line, 1 << 20)
        .unwrap()
        .expect("an empty map references nothing and does not trip the check");
    assert!(row.condition.is_none());
}

#[test]
fn empty_condition_expression_with_a_names_map_fails_the_row() {
    // handle_put_item treats an empty expression as absent, so the maps
    // have nothing to reference and the row fails item-level exactly as it
    // would with no ConditionExpression key at all.
    let line = concat!(
        r##"{"Item": {"pk": {"S": "a"}}, "ConditionExpression": "", "##,
        r##""ExpressionAttributeNames": {"#v": "v"}}"##
    );
    match rows::parse_row(line, 1 << 20) {
        Err(rows::RowError::Item(message)) => assert_eq!(
            message,
            "ExpressionAttributeNames can only be specified when using expressions"
        ),
        other => panic!("expected an item-level error, got {other:?}"),
    }
}

#[test]
fn empty_condition_expression_alone_parses_unconditional() {
    let line = r#"{"Item": {"pk": {"S": "a"}}, "ConditionExpression": ""}"#;
    let row = rows::parse_row(line, 1 << 20)
        .unwrap()
        .expect("an empty expression is no expression, matching PutItem");
    assert!(row.condition.is_none());
}

#[test]
fn whitespace_padded_line_over_the_cap_abandons_the_file() {
    // The cap applies to the raw byte length of the line, before trimming,
    // matching the spec's "a line longer than max_bulk_line_bytes".
    let json_part = r#"{"Item": {"pk": {"S": "a"}}}"#;
    let line = format!("{json_part}{}", " ".repeat(30));
    let cap = json_part.len() + 10;
    assert!(line.len() > cap && json_part.len() < cap);
    match rows::parse_row(&line, cap) {
        Err(rows::RowError::Parse(message)) => assert_eq!(message, ABANDON),
        other => panic!("expected a parse-level error, got {other:?}"),
    }
}

#[test]
fn non_string_condition_expression_abandons_the_file() {
    // A condition field of the wrong JSON type is parse-level: the tree has
    // no PutItem message for a malformed field, only for a missing or
    // unreferenced one.
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "a"}}, "ConditionExpression": 5}"#),
        ABANDON
    );
}

#[test]
fn non_object_expression_names_abandons_the_file() {
    assert_eq!(
        abandon_message(r#"{"Item": {"pk": {"S": "a"}}, "ExpressionAttributeNames": "x"}"#),
        ABANDON
    );
}
