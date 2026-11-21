// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Operation id and ARN formatting.

#[test]
fn new_operation_id_formats_the_known_example() {
    assert_eq!(
        arn::new_operation_id(1_789_060_589_767, 0xda9d_dd4f),
        "01789060589767-da9ddd4f"
    );
}

#[test]
fn new_operation_id_zero_pads_the_timestamp_to_14_digits() {
    assert_eq!(arn::new_operation_id(1, 0), "00000000000001-00000000");
}

#[test]
fn generate_operation_id_matches_the_id_shape() {
    let id = arn::generate_operation_id();
    assert_eq!(id.len(), 23, "14 digits, a hyphen, 8 hex digits: {id}");
    let (stamp, hex) = id.split_once('-').expect("id carries one hyphen");
    assert_eq!(stamp.len(), 14);
    assert!(stamp.bytes().all(|b| b.is_ascii_digit()), "bad stamp: {id}");
    assert_eq!(hex.len(), 8);
    assert!(
        hex.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "bad hex: {id}"
    );
}

#[test]
fn format_arn_produces_the_bulk_operation_arn_shape() {
    assert_eq!(
        arn::format_arn(REGION, ACCOUNT, "01789060589767-da9ddd4f"),
        VALID_ARN
    );
}

// parse_arn layer 1: shape faults answer with the bare message.

#[test]
fn parse_arn_rejects_a_plain_word() {
    assert_eq!(parse_arn_message("orders"), NOT_AN_ARN);
}

#[test]
fn parse_arn_rejects_a_five_part_arn() {
    assert_eq!(
        parse_arn_message("arn:aws:dynamodb:us-east-1:123456789012"),
        NOT_AN_ARN
    );
}

#[test]
fn parse_arn_rejects_a_36_char_string() {
    // Six parts and the right resource type, so only the minimum length
    // (37, from the model) can reject it.
    let input = "arn:aws:dynamodb:::bulk-operation/xy";
    assert_eq!(input.len(), 36);
    assert_eq!(parse_arn_message(input), NOT_AN_ARN);
}

#[test]
fn parse_arn_rejects_a_1025_char_string() {
    let prefix = "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/";
    let input = format!("{prefix}{}", "a".repeat(1025 - prefix.len()));
    assert_eq!(input.len(), 1025);
    assert_eq!(parse_arn_message(&input), NOT_AN_ARN);
}

#[test]
fn parse_arn_rejects_a_table_resource_arn() {
    assert_eq!(
        parse_arn_message("arn:aws:dynamodb:us-east-1:123456789012:table/x"),
        NOT_AN_ARN
    );
}

// parse_arn layer 2: a well-formed ARN whose id has the wrong shape.

#[test]
fn parse_arn_rejects_a_uuid_shaped_id() {
    assert_eq!(
        parse_arn_message(
            "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/\
             f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
        ),
        BAD_ID
    );
}

#[test]
fn parse_arn_rejects_a_13_digit_timestamp_id() {
    assert_eq!(
        parse_arn_message(
            "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/1789060589767-da9ddd4f"
        ),
        BAD_ID
    );
}

#[test]
fn parse_arn_rejects_uppercase_hex_in_the_id() {
    assert_eq!(
        parse_arn_message(
            "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/01789060589767-DA9DDD4F"
        ),
        BAD_ID
    );
}

#[test]
fn parse_arn_rejects_an_id_without_the_hyphen() {
    assert_eq!(
        parse_arn_message(
            "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/01789060589767da9ddd4f"
        ),
        BAD_ID
    );
}

// parse_arn success.

#[test]
fn parse_arn_returns_the_three_parts_of_a_valid_arn() {
    let parsed = arn::parse_arn(VALID_ARN).unwrap();
    assert_eq!(parsed.region, REGION);
    assert_eq!(parsed.account, ACCOUNT);
    assert_eq!(parsed.operation_id, "01789060589767-da9ddd4f");
}

#[test]
fn parse_arn_does_not_decide_account_foreignness() {
    // Layer 3, the existence check, and the foreign-account decision belong
    // to the handler; a foreign but well-formed ARN parses.
    let parsed = arn::parse_arn(
        "arn:aws:dynamodb:eu-west-1:999999999999:bulk-operation/01789060589767-da9ddd4f",
    )
    .unwrap();
    assert_eq!(parsed.region, "eu-west-1");
    assert_eq!(parsed.account, "999999999999");
}

// table_name_from_name_or_arn.

#[test]
fn table_name_passes_a_bare_name_through() {
    assert_eq!(
        arn::table_name_from_name_or_arn("orders", REGION, ACCOUNT).unwrap(),
        "orders"
    );
}

#[test]
fn table_name_is_extracted_from_a_matching_table_arn() {
    assert_eq!(
        arn::table_name_from_name_or_arn(
            "arn:aws:dynamodb:us-east-1:123456789012:table/orders",
            REGION,
            ACCOUNT
        )
        .unwrap(),
        "orders"
    );
}

#[test]
fn table_name_denies_a_foreign_account_arn() {
    // Measured: a well-formed table ARN of another account is denied, not
    // called invalid.
    let input = "arn:aws:dynamodb:us-east-1:999999999999:table/orders";
    match arn::table_name_from_name_or_arn(input, REGION, ACCOUNT).unwrap_err() {
        DynamoDbError::AccessDeniedException(message) => {
            assert_eq!(message, "Access is denied");
        }
        other => panic!("expected AccessDeniedException, got {other:?}"),
    }
}

#[test]
fn table_name_rejects_a_wrong_region_arn_with_the_region_message() {
    // Measured: a table ARN of the caller's account in another region is
    // a validation failure at step 2, not a denial.
    let input = "arn:aws:dynamodb:eu-west-1:123456789012:table/orders";
    let err = arn::table_name_from_name_or_arn(input, REGION, ACCOUNT).unwrap_err();
    assert_eq!(
        validation_message(&err),
        format!("1 validation error detected: Invalid AWS region in '{input}'")
    );
}

#[test]
fn table_name_rejects_an_index_arn() {
    let input = "arn:aws:dynamodb:us-east-1:123456789012:table/orders/index/gsi1";
    let err = arn::table_name_from_name_or_arn(input, REGION, ACCOUNT).unwrap_err();
    assert_eq!(
        validation_message(&err),
        format!(
            "1 validation error detected: Value '{input}' at 'tableName' failed to satisfy \
             constraint: Valid ARN format is \
             'arn:<awsPartition>:<vendor>:<region>:<subscriber>:resourceType/resourceName', \
             where Resource name must have length less than or equal to 255, Resource name \
             must have length greater than or equal to 3, Resource name must satisfy regular \
             expression pattern: [a-zA-Z0-9_.-]+"
        )
    );
}

#[test]
fn table_name_rejects_an_arn_of_another_shape() {
    let input = "arn:aws:s3:::orders";
    let err = arn::table_name_from_name_or_arn(input, REGION, ACCOUNT).unwrap_err();
    assert_eq!(
        validation_message(&err),
        format!("Invalid table ARN: {input}")
    );
}

#[test]
fn table_name_rejects_a_colon_after_the_table_name() {
    // No table name may contain a colon, so a seventh colon-separated field
    // is not the table ARN shape and must not resolve as the name.
    let input = "arn:aws:dynamodb:us-east-1:123456789012:table/orders:extra";
    let err = arn::table_name_from_name_or_arn(input, REGION, ACCOUNT).unwrap_err();
    assert_eq!(
        validation_message(&err),
        format!("Invalid table ARN: {input}")
    );
}

#[test]
fn table_name_rejects_a_wrong_partition_arn() {
    // Resolving across a partition boundary is forbidden by the exact-shape
    // rule; only the aws partition matches.
    let input = "arn:aws-cn:dynamodb:us-east-1:123456789012:table/orders";
    let err = arn::table_name_from_name_or_arn(input, REGION, ACCOUNT).unwrap_err();
    assert_eq!(
        validation_message(&err),
        format!("Invalid table ARN: {input}")
    );
}

#[test]
fn table_name_rejects_an_empty_table_name() {
    let input = "arn:aws:dynamodb:us-east-1:123456789012:table/";
    let err = arn::table_name_from_name_or_arn(input, REGION, ACCOUNT).unwrap_err();
    assert_eq!(
        validation_message(&err),
        format!("Invalid table ARN: {input}")
    );
}

#[test]
fn parse_arn_answers_the_id_message_at_the_lower_length_bound() {
    // A 37 character string with six parts, the aws partition, the
    // dynamodb service, and the right resource type passes every layer 1
    // check, so a malformed id must answer the layer 2 message. This pins
    // the lower length bound from inside: raising it past 37 flips this
    // input to the layer 1 message.
    let input = "arn:aws:dynamodb:::bulk-operation/xyz";
    assert_eq!(input.len(), 37);
    assert_eq!(parse_arn_message(input), BAD_ID);
}

#[test]
fn parse_arn_answers_the_id_message_at_the_upper_length_bound() {
    // A well-formed 1024 character ARN is within the model's bounds, so a
    // malformed id must answer the layer 2 message. This pins the upper
    // length bound from inside: lowering it past 1024 flips this input to
    // the layer 1 message.
    let prefix = "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/";
    let input = format!("{prefix}{}", "a".repeat(1024 - prefix.len()));
    assert_eq!(input.len(), 1024);
    assert_eq!(parse_arn_message(&input), BAD_ID);
}

#[test]
fn parse_arn_rejects_a_wrong_partition_or_service_segment() {
    // The resource evaluated for Describe and Cancel is this ARN, and
    // policy resource matching is literal, so a respelling of the
    // partition or service is a string no Deny written against the
    // documented shape could match. Layer 2 refuses both (measured); the
    // refusal still lands before any policy is fetched.
    assert_eq!(
        parse_arn_message(
            "arn:aws:s3:us-east-1:123456789012:bulk-operation/01789060589767-da9ddd4f"
        ),
        BAD_ID
    );
    assert_eq!(
        parse_arn_message(
            "arn:aws-cn:dynamodb:us-east-1:123456789012:bulk-operation/01789060589767-da9ddd4f"
        ),
        BAD_ID
    );
}
