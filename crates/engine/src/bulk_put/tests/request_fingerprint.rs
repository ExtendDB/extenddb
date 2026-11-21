// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn identical_requests_share_a_fingerprint() {
    assert_eq!(base_fingerprint(), base_fingerprint());
}

#[test]
fn fingerprint_is_64_lowercase_hex_chars() {
    let fp = base_fingerprint();
    assert_eq!(fp.len(), 64);
    assert!(
        fp.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "not lowercase hex: {fp}"
    );
}

#[test]
fn input_member_order_does_not_change_the_fingerprint() {
    let reordered: serde_json::Value = serde_json::from_str(
        r#"{"InputCompressionType": "GZIP", "InputFormat": "DYNAMODB_JSON",
            "S3KeyPrefix": "orders/2026-09-10/", "S3Bucket": "loads"}"#,
    )
    .unwrap();
    let fp = fingerprint::request_fingerprint(
        TABLE_ARN,
        &reordered,
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    assert_eq!(fp, base_fingerprint());
}

#[test]
fn output_type_order_does_not_change_the_fingerprint() {
    let mut output = base_output();
    output["OutputType"] = json!(["CONDITION_CHECK_FAILURE", "ERROR"]);
    let fp = fingerprint::request_fingerprint(
        TABLE_ARN,
        &base_input(),
        &output,
        Some(1000),
        Some("STANDARD"),
    );
    assert_eq!(
        fp,
        base_fingerprint(),
        "OutputType is a set on the wire; its order must not matter"
    );
}

#[test]
fn null_members_equal_absent_members() {
    let mut input = base_input();
    input["S3BucketOwner"] = serde_json::Value::Null;
    let mut output = base_output();
    output["S3SseAlgorithm"] = serde_json::Value::Null;
    let fp =
        fingerprint::request_fingerprint(TABLE_ARN, &input, &output, Some(1000), Some("STANDARD"));
    assert_eq!(
        fp,
        base_fingerprint(),
        "null members are removed before hashing, so null equals absent"
    );
}

#[test]
fn arrays_other_than_output_type_keep_their_order() {
    // No real configuration member is an ordered array today; the canonical
    // form must still only sort OutputType, so an artificial array member
    // discriminates an implementation that sorts every array.
    let mut a = base_input();
    a["Marker"] = json!(["x", "y"]);
    let mut b = base_input();
    b["Marker"] = json!(["y", "x"]);
    let fp_a = fingerprint::request_fingerprint(
        TABLE_ARN,
        &a,
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    let fp_b = fingerprint::request_fingerprint(
        TABLE_ARN,
        &b,
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    assert_ne!(fp_a, fp_b);
}

#[test]
fn changing_the_table_arn_changes_the_fingerprint() {
    let fp = fingerprint::request_fingerprint(
        "arn:aws:dynamodb:us-east-1:123456789012:table/other",
        &base_input(),
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    assert_ne!(fp, base_fingerprint());
}

#[test]
fn changing_the_input_bucket_changes_the_fingerprint() {
    let mut input = base_input();
    input["S3Bucket"] = json!("other-loads");
    let fp = fingerprint::request_fingerprint(
        TABLE_ARN,
        &input,
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    assert_ne!(fp, base_fingerprint());
}

#[test]
fn changing_the_input_prefix_changes_the_fingerprint() {
    let mut input = base_input();
    input["S3KeyPrefix"] = json!("orders/2026-09-11/");
    let fp = fingerprint::request_fingerprint(
        TABLE_ARN,
        &input,
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    assert_ne!(fp, base_fingerprint());
}

#[test]
fn changing_the_compression_changes_the_fingerprint() {
    let mut input = base_input();
    input["InputCompressionType"] = json!("NONE");
    let fp = fingerprint::request_fingerprint(
        TABLE_ARN,
        &input,
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    assert_ne!(fp, base_fingerprint());
}

#[test]
fn changing_the_output_type_changes_the_fingerprint() {
    let mut output = base_output();
    output["OutputType"] = json!(["ERROR"]);
    let fp = fingerprint::request_fingerprint(
        TABLE_ARN,
        &base_input(),
        &output,
        Some(1000),
        Some("STANDARD"),
    );
    assert_ne!(fp, base_fingerprint());
}

#[test]
fn escaped_string_values_keep_distinct_requests_distinct() {
    // Under an unescaped rendering, a bucket value spelling
    // `x","S3KeyPrefix":"a` would collapse the one-member configuration
    // into the two-member one below (the keys already sort that way), and
    // two different requests would replay each other's operation. The
    // canonical form escapes the quotes, so the fingerprints differ.
    let crafted = json!({"S3Bucket": "x\",\"S3KeyPrefix\":\"a"});
    let plain = json!({"S3Bucket": "x", "S3KeyPrefix": "a"});
    let output = base_output();
    let crafted_fp = fingerprint::request_fingerprint(TABLE_ARN, &crafted, &output, None, None);
    let plain_fp = fingerprint::request_fingerprint(TABLE_ARN, &plain, &output, None, None);
    assert_ne!(
        crafted_fp, plain_fp,
        "escaping keeps the canonical form injective"
    );
}

#[test]
fn changing_the_write_request_units_changes_the_fingerprint() {
    let some_other = fingerprint::request_fingerprint(
        TABLE_ARN,
        &base_input(),
        &base_output(),
        Some(2000),
        Some("STANDARD"),
    );
    assert_ne!(some_other, base_fingerprint());
    let absent = fingerprint::request_fingerprint(
        TABLE_ARN,
        &base_input(),
        &base_output(),
        None,
        Some("STANDARD"),
    );
    assert_ne!(absent, base_fingerprint());
}

#[test]
fn changing_the_performance_tier_changes_the_fingerprint() {
    let accelerated = fingerprint::request_fingerprint(
        TABLE_ARN,
        &base_input(),
        &base_output(),
        Some(1000),
        Some("ACCELERATED"),
    );
    assert_ne!(accelerated, base_fingerprint());
    let absent = fingerprint::request_fingerprint(
        TABLE_ARN,
        &base_input(),
        &base_output(),
        Some(1000),
        None,
    );
    assert_ne!(absent, base_fingerprint());
}

#[test]
fn escaped_characters_in_the_prefix_produce_distinct_fingerprints() {
    // A quote, a backslash, and a control character are legal in
    // S3KeyPrefix under the spec's path rules, so the canonical form must
    // stay injective across them for idempotency to compare requests
    // correctly.
    let fp = |prefix: &str| {
        let mut input = base_input();
        input["S3KeyPrefix"] = json!(prefix);
        fingerprint::request_fingerprint(
            TABLE_ARN,
            &input,
            &base_output(),
            Some(1000),
            Some("STANDARD"),
        )
    };
    let quote = fp("a\"b");
    let backslash = fp("a\\b");
    let control = fp("a\u{1}b");
    assert_ne!(quote, backslash);
    assert_ne!(quote, control);
    assert_ne!(backslash, control);
    assert_eq!(fp("a\"b"), quote);
}

#[test]
fn a_prefix_that_mimics_the_canonical_form_does_not_collide() {
    // Under a canonical writer that copies strings without escaping, this
    // prefix renders byte-identically to the two separate members below,
    // so two different requests would share a fingerprint and a token
    // replay would be answered with the wrong operation instead of
    // IdempotentParameterMismatchException.
    let mut smuggled = base_input();
    smuggled["S3KeyPrefix"] = json!(r#"a","S3Z":"b"#);
    let mut split = base_input();
    split["S3KeyPrefix"] = json!("a");
    split["S3Z"] = json!("b");
    let fp_smuggled = fingerprint::request_fingerprint(
        TABLE_ARN,
        &smuggled,
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    let fp_split = fingerprint::request_fingerprint(
        TABLE_ARN,
        &split,
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    assert_ne!(fp_smuggled, fp_split);
}

#[test]
fn nested_null_members_equal_absent_members() {
    // Null members are removed recursively, so a null one level down
    // hashes as absent too.
    let mut with_null = base_input();
    with_null["Nested"] = json!({"Keep": "x", "Drop": null});
    let mut without = base_input();
    without["Nested"] = json!({"Keep": "x"});
    let fp_with_null = fingerprint::request_fingerprint(
        TABLE_ARN,
        &with_null,
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    let fp_without = fingerprint::request_fingerprint(
        TABLE_ARN,
        &without,
        &base_output(),
        Some(1000),
        Some("STANDARD"),
    );
    assert_eq!(fp_with_null, fp_without);
}

// The service replays a retry that adds a default a request may omit
// without changing the operation. The handler normalizes these before
// fingerprinting; `normalized_fp` mirrors that composition
// (`strip_replayed_defaults` on each configuration, PerformanceTier
// STANDARD to an absent tier).

/// The account the TABLE_ARN names; an own-account S3BucketOwner is a
/// replayed default.
const OWN_ACCOUNT: &str = "123456789012";

fn wm4_input() -> serde_json::Value {
    json!({ "S3Bucket": "loads", "InputFormat": "DYNAMODB_JSON" })
}

fn wm4_output() -> serde_json::Value {
    json!({ "S3Bucket": "reports", "OutputType": ["ERROR"] })
}

fn normalized_fp(
    input: &serde_json::Value,
    output: &serde_json::Value,
    tier: Option<&str>,
) -> String {
    fingerprint::request_fingerprint(
        TABLE_ARN,
        &fingerprint::strip_replayed_defaults(input, OWN_ACCOUNT),
        &fingerprint::strip_replayed_defaults(output, OWN_ACCOUNT),
        Some(1000),
        tier.filter(|t| *t != "STANDARD"),
    )
}

#[test]
fn an_explicit_none_compression_replays() {
    let mut input = wm4_input();
    input["InputCompressionType"] = json!("NONE");
    assert_eq!(
        normalized_fp(&input, &wm4_output(), None),
        normalized_fp(&wm4_input(), &wm4_output(), None),
        "a retry that adds InputCompressionType NONE replays"
    );
}

#[test]
fn an_explicit_standard_tier_replays() {
    assert_eq!(
        normalized_fp(&wm4_input(), &wm4_output(), Some("STANDARD")),
        normalized_fp(&wm4_input(), &wm4_output(), None),
        "a retry that adds PerformanceTier STANDARD replays"
    );
}

#[test]
fn an_own_account_input_bucket_owner_replays() {
    let mut input = wm4_input();
    input["S3BucketOwner"] = json!(OWN_ACCOUNT);
    assert_eq!(
        normalized_fp(&input, &wm4_output(), None),
        normalized_fp(&wm4_input(), &wm4_output(), None),
        "a retry that adds an own-account input S3BucketOwner replays"
    );
}

#[test]
fn an_own_account_output_bucket_owner_replays() {
    let mut output = wm4_output();
    output["S3BucketOwner"] = json!(OWN_ACCOUNT);
    assert_eq!(
        normalized_fp(&wm4_input(), &output, None),
        normalized_fp(&wm4_input(), &wm4_output(), None),
        "a retry that adds an own-account output S3BucketOwner replays"
    );
}

#[test]
fn an_added_sse_algorithm_mismatches() {
    let mut output = wm4_output();
    output["S3SseAlgorithm"] = json!("AES256");
    assert_ne!(
        normalized_fp(&wm4_input(), &output, None),
        normalized_fp(&wm4_input(), &wm4_output(), None),
        "a retry that adds S3SseAlgorithm AES256 mismatches"
    );
}

#[test]
fn a_foreign_bucket_owner_is_not_a_replayed_default() {
    let mut input = wm4_input();
    input["S3BucketOwner"] = json!("999999999999");
    assert_ne!(
        normalized_fp(&input, &wm4_output(), None),
        normalized_fp(&wm4_input(), &wm4_output(), None),
        "only the caller's own account is the S3BucketOwner default"
    );
}

#[test]
fn a_request_without_defaults_keeps_its_pre_change_fingerprint() {
    // Stored-record compatibility: strip_replayed_defaults removes only a
    // member present at its default value, so a configuration that spells
    // no default (or a non-default value) is returned unchanged. The
    // canonical document, and therefore the fingerprint, is byte-identical
    // to what the code produced before this change, so a token issued
    // before an upgrade still replays. A record whose original request
    // spelled a default explicitly is the one case that changes, which is
    // the intended normalization.
    let mut input = wm4_input();
    input["InputCompressionType"] = json!("GZIP");
    input["S3BucketOwner"] = json!("999999999999");
    assert_eq!(
        fingerprint::strip_replayed_defaults(&input, OWN_ACCOUNT),
        input
    );
    assert_eq!(
        fingerprint::strip_replayed_defaults(&wm4_output(), OWN_ACCOUNT),
        wm4_output()
    );
    // With nothing stripped, the normalized fingerprint equals the raw
    // request_fingerprint of the same request.
    assert_eq!(
        normalized_fp(&input, &wm4_output(), Some("ACCELERATED")),
        fingerprint::request_fingerprint(
            TABLE_ARN,
            &input,
            &wm4_output(),
            Some(1000),
            Some("ACCELERATED")
        )
    );
}
