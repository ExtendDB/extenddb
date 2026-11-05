// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The model-constraint pass of `StartBulkPutItems`: every enum, pattern,
//! length and range constraint the model carries on the configuration and
//! top-level members, checked on the raw body and answered together in one
//! `N validation errors detected: c1; c2; ...` envelope.
//!
//! Measured on the service: a request wrong at several members answers one
//! envelope with a clause per fault, in a fixed order that is not
//! alphabetical (`MEMBER_ORDER` below), with at most ten clauses, and the
//! count names the clauses listed. `TableName` is judged in a pass of its
//! own before this one, and the service's `Invalid Request:` checks after
//! it.
//!
//! A member of the wrong JSON type raises no clause here; deserialization
//! answers it as the server's `SerializationException`.

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;

use super::{
    MAX_KMS_KEY_ID_CHARS, MAX_PREFIX_CHARS, MIN_WRITE_REQUEST_UNITS, bucket_matches_pattern,
};

/// The most clauses the service lists in one envelope (measured: thirteen
/// faults answered `10 validation errors detected` with the first ten).
const MAX_CLAUSES: usize = 10;

/// The longest bucket name the model admits.
const MAX_BUCKET_CHARS: usize = 255;

/// The order the service lists the members' clauses in (measured across
/// nineteen cells, each order consistent with every other).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Member {
    OutputSseAlgorithm,
    OutputType,
    OutputBucketOwner,
    OutputKeyPrefix,
    OutputBucket,
    OutputKmsKeyId,
    PerformanceTier,
    ClientToken,
    InputCompressionType,
    InputBucketOwner,
    InputFormat,
    InputKeyPrefix,
    InputBucket,
    MaxWriteRequestUnits,
}

/// Collect the model-constraint clauses of a `StartBulkPutItems` body and
/// answer them in one envelope.
///
/// # Errors
///
/// `ValidationException` with every clause, in the service's member order,
/// capped at ten.
pub(super) fn check_model_constraints(body: &Value) -> Result<(), DynamoDbError> {
    let Some(obj) = body.as_object() else {
        return Ok(());
    };
    let mut clauses: Vec<(Member, String)> = Vec::new();
    if let Some(output) = obj.get("OutputS3Configuration").and_then(Value::as_object) {
        output_clauses(output, &mut clauses);
    }
    if let Some(Value::String(found)) = obj.get("PerformanceTier")
        && !matches!(found.as_str(), "STANDARD" | "ACCELERATED")
    {
        clauses.push((
            Member::PerformanceTier,
            enum_clause(found, "performanceTier", "[ACCELERATED, STANDARD]"),
        ));
    }
    if let Some(Value::String(token)) = obj.get("ClientToken")
        // The pattern also refuses U+0000: the token is stored verbatim in
        // a text column, and PostgreSQL refuses a NUL byte in text, so the
        // byte is answered with the pattern clause.
        && (token.is_empty() || token.contains('$') || token.contains('\0'))
    {
        clauses.push((
            Member::ClientToken,
            format!(
                "Value '{token}' at 'clientToken' failed to satisfy constraint: Member must \
                 satisfy regular expression pattern: ^[^\\$]+$"
            ),
        ));
    }
    if let Some(input) = obj.get("InputS3Configuration").and_then(Value::as_object) {
        input_clauses(input, &mut clauses);
    }
    if let Some(units) = obj.get("MaxWriteRequestUnits").and_then(Value::as_i64)
        && units < MIN_WRITE_REQUEST_UNITS
    {
        // Every value under the minimum answers the model clause (measured
        // for 0, 1, 1000 and 1023).
        clauses.push((
            Member::MaxWriteRequestUnits,
            format!(
                "Value '{units}' at 'maxWriteRequestUnits' failed to satisfy constraint: Member \
                 must have value greater than or equal to {MIN_WRITE_REQUEST_UNITS}"
            ),
        ));
    }
    envelope(clauses)
}

fn output_clauses(output: &Map<String, Value>, clauses: &mut Vec<(Member, String)>) {
    if let Some(Value::String(found)) = output.get("S3SseAlgorithm")
        && !matches!(found.as_str(), "AES256" | "KMS")
    {
        clauses.push((
            Member::OutputSseAlgorithm,
            enum_clause(
                found,
                "outputS3Configuration.s3SseAlgorithm",
                "[AES256, KMS]",
            ),
        ));
    }
    if let Some(clause) = output_type_clause(output.get("OutputType")) {
        clauses.push((Member::OutputType, clause));
    }
    if let Some(Value::String(owner)) = output.get("S3BucketOwner") {
        owner_clause(owner, "outputS3Configuration.s3BucketOwner")
            .into_iter()
            .for_each(|clause| clauses.push((Member::OutputBucketOwner, clause)));
    }
    if let Some(Value::String(prefix)) = output.get("S3KeyPrefix") {
        prefix_clause(prefix, "outputS3Configuration.s3KeyPrefix")
            .into_iter()
            .for_each(|clause| clauses.push((Member::OutputKeyPrefix, clause)));
    }
    if let Some(Value::String(bucket)) = output.get("S3Bucket") {
        for clause in bucket_clauses(bucket, "outputS3Configuration.s3Bucket") {
            clauses.push((Member::OutputBucket, clause));
        }
    }
    if let Some(Value::String(key_id)) = output.get("S3SseKmsKeyId") {
        let length = key_id.chars().count();
        let rule = if length == 0 {
            Some("greater than or equal to 1".to_owned())
        } else if length > MAX_KMS_KEY_ID_CHARS {
            Some(format!("less than or equal to {MAX_KMS_KEY_ID_CHARS}"))
        } else {
            None
        };
        if let Some(rule) = rule {
            clauses.push((
                Member::OutputKmsKeyId,
                format!(
                    "Value '{key_id}' at 'outputS3Configuration.s3SseKmsKeyId' failed to satisfy \
                     constraint: Member must have length {rule}"
                ),
            ));
        }
    }
}

fn input_clauses(input: &Map<String, Value>, clauses: &mut Vec<(Member, String)>) {
    if let Some(Value::String(found)) = input.get("InputCompressionType")
        && !matches!(found.as_str(), "GZIP" | "ZSTD" | "NONE")
    {
        clauses.push((
            Member::InputCompressionType,
            enum_clause(
                found,
                "inputS3Configuration.inputCompressionType",
                "[ZSTD, NONE, GZIP]",
            ),
        ));
    }
    if let Some(Value::String(owner)) = input.get("S3BucketOwner") {
        owner_clause(owner, "inputS3Configuration.s3BucketOwner")
            .into_iter()
            .for_each(|clause| clauses.push((Member::InputBucketOwner, clause)));
    }
    if let Some(Value::String(found)) = input.get("InputFormat")
        && !matches!(found.as_str(), "DYNAMODB_JSON" | "ION")
    {
        clauses.push((
            Member::InputFormat,
            enum_clause(
                found,
                "inputS3Configuration.inputFormat",
                "[ION, DYNAMODB_JSON]",
            ),
        ));
    }
    if let Some(Value::String(prefix)) = input.get("S3KeyPrefix") {
        prefix_clause(prefix, "inputS3Configuration.s3KeyPrefix")
            .into_iter()
            .for_each(|clause| clauses.push((Member::InputKeyPrefix, clause)));
    }
    if let Some(Value::String(bucket)) = input.get("S3Bucket") {
        for clause in bucket_clauses(bucket, "inputS3Configuration.s3Bucket") {
            clauses.push((Member::InputBucket, clause));
        }
    }
}

/// The enum clause: the full lowerCamel path and the set in the service's
/// own order (measured).
fn enum_clause(found: &str, path: &str, set_text: &str) -> String {
    format!(
        "Value '{found}' at '{path}' failed to satisfy constraint: Member must satisfy enum \
         value set: {set_text}"
    )
}

/// The `OutputType` clause: the whole submitted list printed as the value
/// in brackets, and the enum constraint wrapped in the list constraint
/// (measured). A list holding a non-string element is left to
/// deserialization.
fn output_type_clause(value: Option<&Value>) -> Option<String> {
    let Value::Array(items) = value? else {
        return None;
    };
    let has_bad = items
        .iter()
        .any(|item| matches!(item, Value::String(s) if !matches!(s.as_str(), "ERROR" | "CONDITION_CHECK_FAILURE")));
    if !has_bad {
        return None;
    }
    let rendered: Vec<String> = items
        .iter()
        .map(|item| match item {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .collect();
    Some(format!(
        "Value '[{}]' at 'outputS3Configuration.outputType' failed to satisfy constraint: Member \
         must satisfy constraint: [Member must satisfy enum value set: [ERROR, \
         CONDITION_CHECK_FAILURE]]",
        rendered.join(", ")
    ))
}

/// Both `S3BucketOwner` fields: twelve ASCII digits (measured text).
fn owner_clause(owner: &str, path: &str) -> Option<String> {
    (owner.len() != 12 || !owner.bytes().all(|byte| byte.is_ascii_digit())).then(|| {
        format!(
            "Value '{owner}' at '{path}' failed to satisfy constraint: Member must satisfy \
             regular expression pattern: [0-9]{{12}}"
        )
    })
}

/// Both `S3KeyPrefix` fields: at most 1024 characters (measured text). The
/// service's three-character minimum is an `Invalid Request:` check of the
/// second pass.
fn prefix_clause(prefix: &str, path: &str) -> Option<String> {
    (prefix.chars().count() > MAX_PREFIX_CHARS).then(|| {
        format!(
            "Value '{prefix}' at '{path}' failed to satisfy constraint: Member must have length \
             less than or equal to {MAX_PREFIX_CHARS}"
        )
    })
}

/// Both `S3Bucket` fields: at most 255 characters, then the pattern
/// `^[a-z0-9A-Z]+[\.\-\w]*[a-z0-9A-Z]+$`. The model has no minimum length;
/// a name of one character fails the pattern. A name failing both rules
/// answers both clauses, length first, the way the service answers a bare
/// table name that fails two rules.
fn bucket_clauses(bucket: &str, path: &str) -> Vec<String> {
    let mut clauses = Vec::new();
    if bucket.chars().count() > MAX_BUCKET_CHARS {
        clauses.push(format!(
            "Value '{bucket}' at '{path}' failed to satisfy constraint: Member must have length \
             less than or equal to {MAX_BUCKET_CHARS}"
        ));
    }
    if !bucket_matches_pattern(bucket) {
        clauses.push(format!(
            "Value '{bucket}' at '{path}' failed to satisfy constraint: Member must satisfy \
             regular expression pattern: ^[a-z0-9A-Z]+[\\.\\-\\w]*[a-z0-9A-Z]+$"
        ));
    }
    clauses
}

/// Answer the collected clauses in one envelope: sorted into the service's
/// member order (stable, so two clauses of one member keep their order),
/// capped at ten, and counted as listed.
fn envelope(mut clauses: Vec<(Member, String)>) -> Result<(), DynamoDbError> {
    if clauses.is_empty() {
        return Ok(());
    }
    clauses.sort_by_key(|(member, _)| *member);
    clauses.truncate(MAX_CLAUSES);
    let count = clauses.len();
    let joined: Vec<String> = clauses.into_iter().map(|(_, clause)| clause).collect();
    Err(DynamoDbError::ValidationException(format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        joined.join("; ")
    )))
}
