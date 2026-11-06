// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request validation for the bulk operation handlers: required-member
//! checks on the raw body, the operation ARN null guard, and the field
//! constraint checks the start handler applies in spec order.

use super::*;

/// Whether any global secondary or vector index of the table is in a state
/// other than `ACTIVE`. Local secondary indexes are created with the table
/// and have no lifecycle of their own.
pub(super) fn has_index_operation(table: &TableDescription) -> bool {
    let gsi_busy = table
        .global_secondary_indexes
        .as_ref()
        .is_some_and(|indexes| indexes.iter().any(|index| index.index_status != "ACTIVE"));
    let vector_busy = table
        .vector_indexes
        .as_ref()
        .is_some_and(|indexes| indexes.iter().any(|index| !index.index_status.is_active()));
    gsi_busy || vector_busy
}

/// One required member to check on the raw request body before
/// deserialization. `parent` is the JSON name of the enclosing object for a
/// nested member, or `None` for a top level member; `json_name` is the
/// member's JSON name; `path` is the lowerCamel path the error message
/// names.
pub(super) struct RequiredMember {
    parent: Option<&'static str>,
    json_name: &'static str,
    path: &'static str,
}

/// `StartBulkPutItems` required members other than `TableName`, in model
/// member order. Each present parent's required members follow it, so a
/// missing top level object is reported without also reporting its
/// children. `TableName` is checked ahead of these and alone, with its own
/// message (measured on the service: a request missing both `TableName` and
/// `InputS3Configuration` answers only the `TableName` message).
pub(super) const START_REQUIRED: &[RequiredMember] = &[
    RequiredMember {
        parent: None,
        json_name: "InputS3Configuration",
        path: "inputS3Configuration",
    },
    RequiredMember {
        parent: Some("InputS3Configuration"),
        json_name: "S3Bucket",
        path: "inputS3Configuration.s3Bucket",
    },
    RequiredMember {
        parent: Some("InputS3Configuration"),
        json_name: "InputFormat",
        path: "inputS3Configuration.inputFormat",
    },
    RequiredMember {
        parent: None,
        json_name: "OutputS3Configuration",
        path: "outputS3Configuration",
    },
    RequiredMember {
        parent: Some("OutputS3Configuration"),
        json_name: "OutputType",
        path: "outputS3Configuration.outputType",
    },
];

/// The hosted answer for a `StartBulkPutItems` body with no `TableName`
/// (absent or JSON null), measured on the service. It is the message the
/// server's other table operations use for the same fault, and it is
/// answered before any other required member is checked.
pub(super) const TABLE_NAME_REQUIRED_MESSAGE: &str =
    "The parameter 'TableName' is required but was not present in the request";

/// `DescribeBulkOperation` and `CancelBulkOperation` with no
/// `BulkOperationArn` (absent or JSON null) answer the ARN parser's shape
/// layer message, `Invalid Bulk operation ARN`, the same answer a malformed
/// ARN gets (measured on the service), not the null-member envelope.
pub(super) fn require_bulk_operation_arn(body: &Value) -> Result<(), DynamoDbError> {
    if body
        .as_object()
        .is_some_and(|obj| is_absent_or_null(obj.get("BulkOperationArn")))
    {
        return Err(extenddb_core::arn::bulk_operation_arn_shape_error());
    }
    Ok(())
}

/// A member absent from a JSON object, or present as JSON null, is a null
/// required member.
pub(super) fn is_absent_or_null(value: Option<&Value>) -> bool {
    matches!(value, None | Some(Value::Null))
}

/// Check the model's required members on the raw body before deserializing.
/// A null or absent required member answers the hosted `ValidationException`
/// (`Member must not be null`) that the rest of the server uses for this
/// fault, in place of the serde `SerializationException` a straight
/// deserialize gives. A nested member is checked only when its parent object
/// is present and is a JSON object; a parent that is absent or null is
/// reported in its own right, and a parent of the wrong type is left to
/// deserialization. Missing members are reported in model member order,
/// joined into the server's "N validation errors detected" envelope.
pub(super) fn require_members(
    body: &Value,
    required: &[RequiredMember],
) -> Result<(), DynamoDbError> {
    let Some(obj) = body.as_object() else {
        return Ok(());
    };
    let mut clauses: Vec<String> = Vec::new();
    for member in required {
        let present = match member.parent {
            None => !is_absent_or_null(obj.get(member.json_name)),
            Some(parent) => match obj.get(parent).and_then(Value::as_object) {
                Some(parent_obj) => !is_absent_or_null(parent_obj.get(member.json_name)),
                None => true,
            },
        };
        if !present {
            clauses.push(format!(
                "Value null at '{}' failed to satisfy constraint: Member must not be null",
                member.path
            ));
        }
    }
    if clauses.is_empty() {
        return Ok(());
    }
    let count = clauses.len();
    Err(DynamoDbError::ValidationException(format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        clauses.join("; ")
    )))
}

/// A bare table name on `StartBulkPutItems` is held to the table-name rules
/// (3 to 255 characters, `[a-zA-Z0-9_.-]+`), reported together in one
/// envelope the way the hosted service answers an empty name (measured
/// "2 validation errors detected:... greater than or equal to
/// 3; ... regular expression pattern: [a-zA-Z0-9_.-]+"). A name over 1024
/// characters is answered by the `TableArn` maximum before this.
pub(super) fn validate_bare_table_name(name: &str) -> Result<(), DynamoDbError> {
    let mut clauses: Vec<String> = Vec::new();
    let chars = name.chars().count();
    if chars < 3 {
        clauses.push(format!(
            "Value '{name}' at 'tableName' failed to satisfy constraint: Member must have \
             length greater than or equal to 3"
        ));
    }
    if chars > 255 {
        clauses.push(format!(
            "Value '{name}' at 'tableName' failed to satisfy constraint: Member must have \
             length less than or equal to 255"
        ));
    }
    // The pattern requires at least one character, so an empty name fails
    // it as well as the minimum (both clauses measured together).
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    {
        clauses.push(format!(
            "Value '{name}' at 'tableName' failed to satisfy constraint: Member must satisfy \
             regular expression pattern: [a-zA-Z0-9_.-]+"
        ));
    }
    if clauses.is_empty() {
        return Ok(());
    }
    let count = clauses.len();
    Err(DynamoDbError::ValidationException(format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        clauses.join("; ")
    )))
}

/// The service's answer for a bucket name it refuses outright, as opposed
/// to the model constraint messages above, which name the lowerCamel
/// member path. `member_name` is the wire spelling the message carries,
/// `InputS3Configuration.S3Bucket` or `OutputS3Configuration.S3Bucket`
/// (measured on the service).
pub(super) fn bucket_is_invalid(member_name: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!("Invalid Request: {member_name} is invalid"))
}

/// The `Invalid Request:` bucket checks the service adds beyond the model,
/// answered after every model constraint (measured on the service: an empty
/// OutputType beside a bad bucket pattern answers the pattern, which is a
/// model constraint, so these run in the second pass).
pub(super) fn validate_bucket_service(
    bucket: &str,
    member_path: &str,
    member_name: &str,
) -> Result<(), DynamoDbError> {
    // The service adds a minimum of three characters the model does not
    // carry, checked after the pattern and reported with the `Invalid
    // Request:` prefix (measured: 'a' answers the pattern, 'ab' answers
    // this).
    if bucket.chars().count() < MIN_SERVICE_NAME_CHARS {
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid Request: 1 validation error detected: Value '{bucket}' at '{member_path}' \
             failed to satisfy constraint: Member must have length greater than or equal to \
             {MIN_SERVICE_NAME_CHARS}"
        )));
    }
    // A name spelled as a dotted IPv4 literal is refused (measured
    // with `10.20.30.40`). The literal passes the model pattern and both
    // length bounds, so it needs its own check; the order relative to the
    // checks above is not measured, because no string can fail this one and
    // any of those.
    if is_dotted_ipv4(bucket) {
        return Err(bucket_is_invalid(member_name));
    }
    Ok(())
}

/// Whether the name is spelled as a dotted IPv4 literal: four
/// dot-separated groups of one to three ASCII digits. The bucket naming
/// rules exclude a name of this shape, and the service refuses it at
/// Start.
fn is_dotted_ipv4(bucket: &str) -> bool {
    let mut groups = 0_u8;
    for group in bucket.split('.') {
        groups += 1;
        if groups > 4
            || !(1..=3).contains(&group.len())
            || !group.bytes().all(|byte| byte.is_ascii_digit())
        {
            return false;
        }
    }
    groups == 4
}

/// True when the name matches the model's bucket pattern: first and last
/// byte alphanumeric, every byte between them alphanumeric, `_`, `.`, or
/// `-`. The pattern excludes `/`, `\`, NUL, and the bare `.` and `..`.
pub(super) fn bucket_matches_pattern(bucket: &str) -> bool {
    let bytes = bucket.as_bytes();
    let Some((&first, rest)) = bytes.split_first() else {
        return false;
    };
    let Some((&last, middle)) = rest.split_last() else {
        return false;
    };
    first.is_ascii_alphanumeric()
        && last.is_ascii_alphanumeric()
        && middle
            .iter()
            .all(|&byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

/// The service's `Invalid Request:` minimum of three characters on an
/// `S3KeyPrefix`, a bound the model does not carry, answered after every
/// model constraint (measured with 'x/'). An absent prefix is not checked.
pub(super) fn validate_prefix_length_service(
    prefix: &str,
    member_path: &str,
) -> Result<(), DynamoDbError> {
    if prefix.chars().count() < MIN_SERVICE_NAME_CHARS {
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid Request: 1 validation error detected: Value '{prefix}' at '{member_path}' \
             failed to satisfy constraint: Member must have length greater than or equal to \
             {MIN_SERVICE_NAME_CHARS}"
        )));
    }
    Ok(())
}

/// The path rules both `S3KeyPrefix` fields share: no `..` path
/// component, no `\`, NUL, or control character. Lexical checks only; the
/// canonical containment checks run on every path before any filesystem
/// operation.
///
/// Doubled and leading separators are not rules here. An input prefix is
/// matched byte-wise against bucket-relative paths and is never joined
/// onto a path, and the service accepts `a3//b//c/`, `a3/b//c/`, and
/// `/a3/b/` at Start (measured on the service).
pub(super) fn validate_prefix(prefix: &str) -> Result<(), DynamoDbError> {
    let invalid = prefix
        .chars()
        .any(|character| character == '\\' || character.is_control())
        || prefix.split('/').any(|component| component == "..");
    if invalid {
        return Err(DynamoDbError::ValidationException(
            "S3KeyPrefix contains an invalid path component".to_owned(),
        ));
    }
    Ok(())
}

/// The output prefix carries one rule the input prefix does not: it must
/// not begin with `/`. It is joined onto the report base path, where an
/// absolute prefix would replace the base outright, and the jail re-checks
/// it there before any path is opened. A leading slash on the output side
/// is not measured, so the refusal stays.
pub(super) fn validate_output_prefix(prefix: &str) -> Result<(), DynamoDbError> {
    if prefix.starts_with('/') {
        return Err(DynamoDbError::ValidationException(
            "S3KeyPrefix contains an invalid path component".to_owned(),
        ));
    }
    validate_prefix(prefix)
}

/// When present, `S3BucketOwner` must equal the caller's account id.
/// The serialized configurations together may not exceed
/// [`MAX_CONFIGURATION_BYTES`], so the catalog row is bounded.
pub(super) fn validate_configuration_size(
    input: &Value,
    output: &Value,
) -> Result<(), DynamoDbError> {
    let size = input.to_string().len() + output.to_string().len();
    if size > MAX_CONFIGURATION_BYTES {
        return Err(DynamoDbError::ValidationException(format!(
            "S3 configuration exceeds {MAX_CONFIGURATION_BYTES} bytes"
        )));
    }
    Ok(())
}

/// No string field of either configuration may carry U+0000. The bucket
/// pattern and the prefix rules already exclude it; this check covers the
/// recorded fields (`S3BucketOwner`, `S3SseKmsKeyId`) as well. A NUL can
/// never name a real bucket, prefix, KMS key, or owner, and the stored
/// configurations are `jsonb` columns on the `PostgreSQL` backend, which
/// refuses the byte, so the request is refused before anything is created.
pub(super) fn validate_configuration_characters(
    input: &Value,
    output: &Value,
) -> Result<(), DynamoDbError> {
    if value_contains_nul(input) || value_contains_nul(output) {
        return Err(DynamoDbError::ValidationException(
            "S3 configuration contains an invalid character".to_owned(),
        ));
    }
    Ok(())
}

/// Whether any string in the JSON tree carries U+0000. Keys need no check:
/// they come from the struct field names, not the request.
fn value_contains_nul(value: &Value) -> bool {
    match value {
        Value::String(text) => text.contains('\0'),
        Value::Array(items) => items.iter().any(value_contains_nul),
        Value::Object(map) => map.values().any(value_contains_nul),
        _ => false,
    }
}
