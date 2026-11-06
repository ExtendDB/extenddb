// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The S3 location rules the start handler applies: the bucket name
//! constraints on both configurations, the required output bucket, and the
//! key prefix rules. The validation step order and the scalar field
//! constraints are in `start_validation.rs`.

use super::*;

#[tokio::test]
async fn a_one_character_input_bucket_fails_the_pattern_not_a_length_bound() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "S3Bucket", json!("a"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains("Member must satisfy regular expression pattern"),
        "a one-character name fails the pattern: {message}"
    );
    assert!(
        !message.contains("greater than or equal to 3"),
        "the removed minimum length must not be reported: {message}"
    );
}

#[tokio::test]
async fn a_two_character_input_bucket_fails_the_service_minimum() {
    // Measured on the service: the model has no minimum, but the service refuses
    // a bucket under three characters after the pattern check, with the
    // `Invalid Request:` prefix. A one-character name answers the pattern.
    let h = Harness::new().await;
    let body = with_input(start_body(), "S3Bucket", json!("ab"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert_eq!(
        message,
        "Invalid Request: 1 validation error detected: Value 'ab' at \
         'inputS3Configuration.s3Bucket' failed to satisfy constraint: Member must have \
         length greater than or equal to 3"
    );
}

#[tokio::test]
async fn an_output_configuration_without_a_bucket_is_refused_at_start() {
    // Measured on the service: an output configuration whose
    // OutputType is set with no S3Bucket answers this at Start. The
    // bucket's existence and its owner are still decided at the first
    // report record.
    let h = Harness::new().await;
    let mut body = start_body();
    body["OutputS3Configuration"]
        .as_object_mut()
        .unwrap()
        .remove("S3Bucket");
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        "Invalid Request: OutputS3Configuration.S3Bucket is invalid"
    );
}

#[tokio::test]
async fn an_output_bucket_without_a_key_prefix_is_accepted() {
    // Measured on the service: with the bucket present and no
    // S3KeyPrefix the start is accepted, and the report tree is written
    // at the bucket root.
    let h = Harness::new().await;
    let mut body = start_body();
    body["OutputS3Configuration"]
        .as_object_mut()
        .unwrap()
        .remove("S3KeyPrefix");
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(
        description["OutputS3Configuration"]["S3Bucket"],
        json!(OUT_BUCKET)
    );
    assert!(
        description["OutputS3Configuration"]
            .get("S3KeyPrefix")
            .is_none(),
        "an absent output prefix is not echoed: {description}"
    );
}

#[tokio::test]
async fn a_dotted_ip_input_bucket_is_refused() {
    // Measured on the service: `10.20.30.40` answers 400
    // ValidationException with this text. The literal passes the model
    // pattern and both length bounds, so nothing but this check can
    // refuse it.
    let h = Harness::new().await;
    let body = with_input(start_body(), "S3Bucket", json!("10.20.30.40"));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        "Invalid Request: InputS3Configuration.S3Bucket is invalid"
    );
}

#[tokio::test]
async fn a_dotted_ip_output_bucket_is_refused() {
    // The output bucket shares the input bucket's constraints, so the
    // same literal is refused there, named by its own member. The IP case
    // on the output side is extrapolated from, not measured.
    let h = Harness::new().await;
    let body = with_output(start_body(), "S3Bucket", json!("10.20.30.40"));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        "Invalid Request: OutputS3Configuration.S3Bucket is invalid"
    );
}

#[tokio::test]
async fn a_name_that_only_resembles_an_ip_literal_is_accepted() {
    // The refusal is for four dot-separated groups of digits. A name with
    // a non-digit group, one with five groups, and one with three are not
    // IP literals and pass the bucket rules, so they must reach the
    // missing-bucket path instead of the refusal.
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    for name in ["10.20.30.4a", "10.20.30.40.50", "10.20.30"] {
        let body = with_input(start_body(), "S3Bucket", json!(name));
        let description = start_ok(&ctx, body).await;
        assert_eq!(
            description["FailureCode"],
            json!("S3NoSuchBucket"),
            "{name} must pass validation and fail on the bucket directory"
        );
    }
}

#[tokio::test]
async fn a_short_or_empty_input_prefix_fails_the_service_minimum() {
    // Measured on the service: a present S3KeyPrefix under three characters is
    // refused ('x/'; the empty string on both the
    // input and the output configuration), an
    // absent prefix is accepted and reads the whole bucket, and a
    // three-character prefix is accepted.
    let h = Harness::new().await;
    for prefix in ["ab", ""] {
        let body = with_input(start_body(), "S3KeyPrefix", json!(prefix));
        let message = expect_validation(start(&base_ctx(&h), body).await);
        assert_eq!(
            message,
            format!(
                "Invalid Request: 1 validation error detected: Value '{prefix}' at \
                 'inputS3Configuration.s3KeyPrefix' failed to satisfy constraint: Member must \
                 have length greater than or equal to 3"
            )
        );
    }
    let body = with_input(start_body(), "S3KeyPrefix", json!("abc"));
    let description = start_ok(&base_ctx(&h), body).await;
    assert_eq!(description["BulkOperationStatus"], json!("IN_PROGRESS"));
}

#[tokio::test]
async fn a_long_input_bucket_fails_the_length_constraint() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "S3Bucket", json!("a".repeat(256)));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains("less than or equal to 255"),
        "the maximum length must be reported: {message}"
    );
}

#[tokio::test]
async fn an_input_bucket_with_a_slash_fails_the_pattern_constraint() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "S3Bucket", json!("bad/name"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains("Member must satisfy regular expression pattern"),
        "the bucket pattern must be reported: {message}"
    );
}

#[tokio::test]
async fn a_one_character_output_bucket_fails_the_pattern_not_a_length_bound() {
    let h = Harness::new().await;
    let body = with_output(start_body(), "S3Bucket", json!("a"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains("Member must satisfy regular expression pattern"),
        "the output bucket shares the pattern: {message}"
    );
    assert!(
        message.contains("outputS3Configuration.s3Bucket"),
        "the violation must name the output member: {message}"
    );
    assert!(
        !message.contains("greater than or equal to 3"),
        "the removed minimum length must not be reported: {message}"
    );
}

#[tokio::test]
async fn a_long_output_bucket_fails_the_length_constraint() {
    let h = Harness::new().await;
    let body = with_output(start_body(), "S3Bucket", json!("a".repeat(256)));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains("less than or equal to 255"),
        "the output bucket shares the maximum length: {message}"
    );
    assert!(
        message.contains("outputS3Configuration.s3Bucket"),
        "the violation must name the output member: {message}"
    );
}

#[tokio::test]
async fn an_output_bucket_with_a_slash_fails_the_pattern_constraint() {
    let h = Harness::new().await;
    let body = with_output(start_body(), "S3Bucket", json!("bad/name"));
    let message = expect_validation(start(&base_ctx(&h), body).await);
    assert!(
        message.contains("Member must satisfy regular expression pattern"),
        "the output bucket shares the pattern: {message}"
    );
    assert!(
        message.contains("outputS3Configuration.s3Bucket"),
        "the violation must name the output member: {message}"
    );
}

#[tokio::test]
async fn a_parent_component_in_the_input_prefix_is_rejected() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "S3KeyPrefix", json!("a/../b"));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        BAD_PREFIX
    );
}

#[tokio::test]
async fn doubled_and_leading_slashes_in_the_input_prefix_are_accepted() {
    // Measured on the service: all three prefixes start
    // successfully. The input prefix is matched byte-wise against
    // bucket-relative paths and is never joined onto a path, so none of
    // them can reach outside the bucket directory; each simply matches no
    // file, which is not a failure.
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    // One non-terminal operation per table, so each prefix gets its own.
    for (index, prefix) in ["a3//b//c/", "a3/b//c/", "/a3/b/"].iter().enumerate() {
        let table = format!("slashes{index}");
        Harness::create_table(&h.storage, &table).await;
        let body = with_input(
            with_top(start_body(), "TableName", json!(table)),
            "S3KeyPrefix",
            json!(prefix),
        );
        let description = start_ok(&ctx, body).await;
        assert_eq!(
            description["BulkOperationStatus"],
            json!("IN_PROGRESS"),
            "the start with prefix {prefix} must be accepted"
        );
        assert_eq!(
            description["TotalInputBytes"],
            json!(0),
            "prefix {prefix} matches no file"
        );
    }
}

#[tokio::test]
async fn a_leading_slash_in_the_output_prefix_is_rejected() {
    // The output prefix is joined onto the report base path, where an
    // absolute prefix would replace the base, so the refusal stays on
    // that side. A leading slash on the output prefix is not measured.
    let h = Harness::new().await;
    let body = with_output(start_body(), "S3KeyPrefix", json!("/etc"));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        BAD_PREFIX
    );
}

#[tokio::test]
async fn a_backslash_in_the_input_prefix_is_rejected() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "S3KeyPrefix", json!("a\\b"));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        BAD_PREFIX
    );
}

#[tokio::test]
async fn a_control_character_in_the_input_prefix_is_rejected() {
    let h = Harness::new().await;
    let body = with_input(start_body(), "S3KeyPrefix", json!("a\u{1}b"));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        BAD_PREFIX
    );
}

#[tokio::test]
async fn a_parent_component_in_the_output_prefix_is_rejected() {
    let h = Harness::new().await;
    let body = with_output(start_body(), "S3KeyPrefix", json!("a/../b"));
    assert_eq!(
        expect_validation(start(&base_ctx(&h), body).await),
        BAD_PREFIX
    );
}
