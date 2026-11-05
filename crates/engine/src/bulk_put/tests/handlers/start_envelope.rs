// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The validation envelope of `StartBulkPutItems`: the tableName pass on
//! its own, then every model constraint of the other members in one
//! envelope, in the service's member order, at most ten clauses. Every
//! expected message is a measured service answer.

use super::*;

const BUCKET_PATTERN: &str = "^[a-z0-9A-Z]+[\\.\\-\\w]*[a-z0-9A-Z]+$";

fn bucket_clause(value: &str, path: &str) -> String {
    format!(
        "Value '{value}' at '{path}' failed to satisfy constraint: Member must satisfy regular \
         expression pattern: {BUCKET_PATTERN}"
    )
}

fn enum_clause(value: &str, path: &str, set: &str) -> String {
    format!(
        "Value '{value}' at '{path}' failed to satisfy constraint: Member must satisfy enum \
         value set: {set}"
    )
}

fn compression_clause() -> String {
    enum_clause(
        "BZIP2",
        "inputS3Configuration.inputCompressionType",
        "[ZSTD, NONE, GZIP]",
    )
}

fn format_clause() -> String {
    enum_clause(
        "CSV",
        "inputS3Configuration.inputFormat",
        "[ION, DYNAMODB_JSON]",
    )
}

fn tier_clause() -> String {
    enum_clause("NO_SUCH", "performanceTier", "[ACCELERATED, STANDARD]")
}

fn envelope(clauses: &[String]) -> String {
    let count = clauses.len();
    format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        clauses.join("; ")
    )
}

async fn answer(body: Value) -> String {
    let h = Harness::new().await;
    expect_validation(start(&base_ctx(&h), body).await)
}

#[tokio::test]
async fn two_input_faults_answer_one_envelope() {
    // Measured: InputFormat CSV beside a bucket `b`.
    let body = with_input(
        with_input(start_body(), "InputFormat", json!("CSV")),
        "S3Bucket",
        json!("b"),
    );
    assert_eq!(
        answer(body).await,
        envelope(&[
            format_clause(),
            bucket_clause("b", "inputS3Configuration.s3Bucket")
        ])
    );
}

#[tokio::test]
async fn an_output_type_fault_and_an_output_bucket_fault_answer_one_envelope() {
    // Measured: OutputType [EVERYTHING] beside an output bucket `b`.
    let body = with_output(
        with_output(start_body(), "OutputType", json!(["EVERYTHING"])),
        "S3Bucket",
        json!("b"),
    );
    assert_eq!(
        answer(body).await,
        envelope(&[
            "Value '[EVERYTHING]' at 'outputS3Configuration.outputType' failed to satisfy \
             constraint: Member must satisfy constraint: [Member must satisfy enum value set: \
             [ERROR, CONDITION_CHECK_FAILURE]]"
                .to_owned(),
            bucket_clause("b", "outputS3Configuration.s3Bucket"),
        ])
    );
}

#[tokio::test]
async fn input_clauses_follow_the_measured_member_order() {
    // Measured: a bucket `b`, a prefix of 1,025 characters and InputFormat
    // CSV beside MaxWriteRequestUnits 1, and the same with BZIP2.
    let prefix = "x".repeat(1025);
    let body = with_top(
        with_input(
            with_input(
                with_input(start_body(), "S3Bucket", json!("b")),
                "S3KeyPrefix",
                json!(prefix),
            ),
            "InputFormat",
            json!("CSV"),
        ),
        "MaxWriteRequestUnits",
        json!(1),
    );
    let body = with_input(body, "InputCompressionType", json!("BZIP2"));
    assert_eq!(
        answer(body).await,
        envelope(&[
            compression_clause(),
            format_clause(),
            format!(
                "Value '{prefix}' at 'inputS3Configuration.s3KeyPrefix' failed to satisfy \
                 constraint: Member must have length less than or equal to 1024"
            ),
            bucket_clause("b", "inputS3Configuration.s3Bucket"),
            "Value '1' at 'maxWriteRequestUnits' failed to satisfy constraint: Member must have \
             value greater than or equal to 1024"
                .to_owned(),
        ])
    );
}

#[tokio::test]
async fn output_and_top_level_clauses_come_before_the_input_clauses() {
    // Measured: an output bucket `b` before an input fault; PerformanceTier
    // before an input fault; ClientToken between PerformanceTier and the
    // input members.
    let body = with_output(
        with_input(start_body(), "InputCompressionType", json!("BZIP2")),
        "S3Bucket",
        json!("b"),
    );
    assert_eq!(
        answer(body).await,
        envelope(&[
            bucket_clause("b", "outputS3Configuration.s3Bucket"),
            compression_clause()
        ])
    );
    let body = with_top(
        with_top(
            with_output(start_body(), "S3Bucket", json!("b")),
            "ClientToken",
            json!("a$b"),
        ),
        "PerformanceTier",
        json!("NO_SUCH"),
    );
    assert_eq!(
        answer(body).await,
        envelope(&[
            bucket_clause("b", "outputS3Configuration.s3Bucket"),
            tier_clause(),
            "Value 'a$b' at 'clientToken' failed to satisfy constraint: Member must satisfy \
             regular expression pattern: ^[^\\$]+$"
                .to_owned(),
        ])
    );
    let body = with_output(
        with_output(start_body(), "S3SseAlgorithm", json!("XX")),
        "S3Bucket",
        json!("b"),
    );
    assert_eq!(
        answer(body).await,
        envelope(&[
            enum_clause(
                "XX",
                "outputS3Configuration.s3SseAlgorithm",
                "[AES256, KMS]"
            ),
            bucket_clause("b", "outputS3Configuration.s3Bucket"),
        ])
    );
}

#[tokio::test]
async fn thirteen_faults_answer_the_first_ten() {
    // Measured: thirteen member faults answer `10 validation errors
    // detected` with the first ten clauses in member order.
    let mut body = start_body();
    body["InputS3Configuration"] = json!({
        "S3Bucket": "b", "S3KeyPrefix": "x".repeat(1025), "InputFormat": "CSV",
        "InputCompressionType": "BZIP2", "S3BucketOwner": "abc"
    });
    body["OutputS3Configuration"] = json!({
        "S3Bucket": "c", "S3KeyPrefix": "y".repeat(1025), "OutputType": ["NOPE"],
        "S3BucketOwner": "def", "S3SseAlgorithm": "XX", "S3SseKmsKeyId": "k".repeat(2049)
    });
    body["PerformanceTier"] = json!("NO_SUCH");
    body["MaxWriteRequestUnits"] = json!(1);
    let message = answer(body).await;
    assert!(
        message.starts_with("10 validation errors detected: "),
        "{message}"
    );
    let paths: Vec<&str> = message
        .split("' at '")
        .skip(1)
        .map(|rest| rest.split('\'').next().unwrap_or_default())
        .collect();
    assert_eq!(
        paths,
        [
            "outputS3Configuration.s3SseAlgorithm",
            "outputS3Configuration.outputType",
            "outputS3Configuration.s3BucketOwner",
            "outputS3Configuration.s3KeyPrefix",
            "outputS3Configuration.s3Bucket",
            "outputS3Configuration.s3SseKmsKeyId",
            "performanceTier",
            "inputS3Configuration.inputCompressionType",
            "inputS3Configuration.s3BucketOwner",
            "inputS3Configuration.inputFormat",
        ]
    );
}

#[tokio::test]
async fn a_bad_table_name_is_answered_alone() {
    // Measured: the tableName pass answers before the configuration, so
    // the enum and bucket faults beside it raise no clause.
    let faulted = with_input(
        with_input(start_body(), "InputCompressionType", json!("BZIP2")),
        "S3Bucket",
        json!("b"),
    );
    assert_eq!(
        answer(with_top(faulted.clone(), "TableName", json!("a*"))).await,
        envelope(&[
            "Value 'a*' at 'tableName' failed to satisfy constraint: Member must have length \
             greater than or equal to 3"
                .to_owned(),
            "Value 'a*' at 'tableName' failed to satisfy constraint: Member must satisfy \
             regular expression pattern: [a-zA-Z0-9_.-]+"
                .to_owned(),
        ])
    );
    let own = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/ab");
    assert_eq!(
        answer(with_top(faulted.clone(), "TableName", json!(own))).await,
        format!(
            "1 validation error detected: Value '{own}' at 'tableName' failed to satisfy \
             constraint: Table name of ARN must have length greater than or equal to 3"
        )
    );
    let index = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/x/index/gsi");
    let message = answer(with_top(faulted, "TableName", json!(index))).await;
    assert!(
        message.starts_with(&format!(
            "1 validation error detected: Value '{index}' at 'tableName' failed to satisfy \
             constraint: Valid ARN format is"
        )),
        "{message}"
    );
}

#[tokio::test]
async fn a_foreign_account_table_arn_is_denied_before_the_configuration() {
    // Measured: Access is denied beside a bad compression type, and beside
    // a name that breaks the rule.
    let h = Harness::new().await;
    for name in ["xyz", "ab"] {
        let arn = format!("arn:aws:dynamodb:{REGION}:{OTHER_ACCOUNT}:table/{name}");
        let body = with_top(
            with_input(start_body(), "InputCompressionType", json!("BZIP2")),
            "TableName",
            json!(arn),
        );
        match start(&base_ctx(&h), body).await {
            Err(DynamoDbError::AccessDeniedException(message)) => {
                assert_eq!(message, "Access is denied");
            }
            other => panic!("expected AccessDeniedException, got {other:?}"),
        }
    }
}
