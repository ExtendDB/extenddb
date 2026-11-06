// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Rendering stored bulk operation records onto the wire: the shared
//! description and summary renderers, the start outcome response builder,
//! status and tier parsing, the pagination cursor codec, and the error
//! helpers the handlers share.

use super::*;
use crate::bulk_put::runner::OBJECTS_COUNT_LIMIT_CODE;

/// Layer 3 of the operation ARN validation: a well-formed ARN that names no
/// operation in the caller's account, a foreign account, or another region.
/// The message lives in `extenddb_core::arn` because the server's
/// authorization layer answers a foreign ARN with the same error.
pub(super) fn does_not_exist(arn: &str) -> DynamoDbError {
    extenddb_core::arn::bulk_operation_does_not_exist(arn)
}

/// A stored row the start handler wrote cannot be rendered. Client input
/// never reaches this path, so the client sees only the sanitized error.
fn corrupt_record(operation_id: &str, what: &str) -> DynamoDbError {
    tracing::error!(operation_id, what, "bulk operation record does not render");
    DynamoDbError::InternalServerError("Internal server error".to_owned())
}

/// The wire answer for one start transaction outcome, shared by the
/// saturated-account path and the enumerated path so the messages cannot
/// diverge.
pub(super) fn start_response(
    outcome: StartOutcome,
    region: &str,
    account_limit: u32,
) -> Result<Value, DynamoDbError> {
    match outcome {
        StartOutcome::Created(record) | StartOutcome::IdempotentReplay(record) => {
            serialize_output(&StartBulkPutItemsOutput {
                bulk_operation_description: describe(&record, region)?,
            })
        }
        StartOutcome::IdempotentMismatch => {
            Err(DynamoDbError::IdempotentParameterMismatchException(
                IDEMPOTENT_MISMATCH_MESSAGE.to_owned(),
            ))
        }
        StartOutcome::TableBusy => Err(DynamoDbError::ResourceInUseException(
            TABLE_BUSY_MESSAGE.to_owned(),
        )),
        // The text is the measured one, missing separators between the
        // first three fragments included ("Other facts": a
        // sixth concurrent Start on the service answered it verbatim); the
        // configured limit is substituted where the hosted service names
        // its quota.
        StartOutcome::AccountLimitReached => Err(DynamoDbError::LimitExceededException(format!(
            "Subscriber limit exceededService quota exceeded You have reached the maximum \
             number of concurrent bulk operations for the selected PerformanceTier, which is \
             {account_limit}. Wait until other jobs finish, and try again. You can also reach \
             out to AWS Support to request a quota increase."
        ))),
        // The terminal-born cap, enforced exactly under the per-account
        // lock inside the start transaction. Same throttle the pre-check
        // fast path returns, so a burst that races past the pre-check is
        // still refused here.
        StartOutcome::FailedStartLimit => Err(DynamoDbError::ThrottlingException(
            "Rate of requests exceeds the allowed throughput.".to_owned(),
        )),
        // The index check the handler makes before enumeration, made again
        // inside the start transaction, so the same answer.
        StartOutcome::IndexOperationInProgress => Err(DynamoDbError::ResourceInUseException(
            INDEX_OPERATION_MESSAGE.to_owned(),
        )),
    }
}

/// Render one stored record as its wire description. Start, Describe, and
/// the behaviour tests share this one renderer so the shapes cannot
/// diverge.
///
/// # Errors
///
/// Returns `InternalServerError` (with a log line, never a panic) when the
/// stored status, tier, or configuration does not parse; the start handler
/// wrote them, so a failure is an internal inconsistency, not client input.
pub(crate) fn describe(
    record: &BulkOperationRecord,
    region: &str,
) -> Result<BulkOperationDescription, DynamoDbError> {
    let status = parse_status(&record.status)
        .ok_or_else(|| corrupt_record(&record.operation_id, "unknown stored status"))?;
    let performance_tier = match record.performance_tier.as_deref() {
        None => None,
        Some("STANDARD") => Some(BulkOperationPerformanceTier::Standard),
        Some("ACCELERATED") => Some(BulkOperationPerformanceTier::Accelerated),
        Some(_) => {
            return Err(corrupt_record(
                &record.operation_id,
                "unknown stored performance tier",
            ));
        }
    };
    let input_s3_configuration: InputS3Configuration =
        serde_json::from_value(record.input_config.clone())
            .map_err(|_| corrupt_record(&record.operation_id, "stored input configuration"))?;
    let output_s3_configuration: OutputS3Configuration =
        serde_json::from_value(record.output_config.clone())
            .map_err(|_| corrupt_record(&record.operation_id, "stored output configuration"))?;
    // The two byte members travel together and are absent when the hosted
    // service never kept an enumerated total: a failure born before the
    // walk (missing input bucket) and the object-count
    // quota failure (50,500 objects: neither member in
    // the description). The per-object size quota and every
    // other terminal shape carry both. ExtendDB stores the enumerated
    // total for the count class too, so the class is recognised by its
    // failure code here rather than by a storage change.
    let total_input_bytes = match record.failure_code.as_deref() {
        Some(OBJECTS_COUNT_LIMIT_CODE) => None,
        _ => record.total_input_bytes.map(non_negative),
    };
    Ok(BulkOperationDescription {
        bulk_operation_arn: arn::format_arn(region, &record.account_id, &record.operation_id),
        bulk_operation_status: status,
        // Re-derived on every render from the name and id the start
        // handler stored with the operation, so no migration is needed and
        // a Describe cannot disagree with the Start response.
        table_details: vec![BulkOperationTableDetail {
            table_name: record.table_name.clone(),
            table_id: record.table_id.clone(),
        }],
        // The model types these two members, but the service never emits
        // them: every description it answered during the runs
        // (326 of them among them) carried the table only in
        // `TableDetails`.
        table_arn: None,
        table_id: None,
        client_token: record.client_token.clone(),
        input_s3_configuration: Some(input_s3_configuration),
        output_s3_configuration: Some(output_s3_configuration),
        start_time: Some(millis_to_seconds(record.start_time_ms)),
        end_time: record.end_time_ms.map(millis_to_seconds),
        processed_item_count: Some(non_negative(record.processed_item_count)),
        processed_input_bytes: total_input_bytes
            .map(|_| non_negative(record.processed_input_bytes)),
        total_input_bytes,
        bulk_operation_usage: status
            .is_terminal()
            .then(|| vec![usage::usage_from_value(&record.usage, &record.table_name)]),
        failed_item_count: Some(non_negative(record.failed_item_count)),
        failure_code: record.failure_code.clone(),
        failure_message: record.failure_message.clone(),
        max_write_request_units: record.max_write_request_units,
        performance_tier,
    })
}

/// Render one stored record as a list summary.
pub(super) fn summary(
    record: &BulkOperationRecord,
    region: &str,
) -> Result<BulkOperationSummary, DynamoDbError> {
    let status = parse_status(&record.status)
        .ok_or_else(|| corrupt_record(&record.operation_id, "unknown stored status"))?;
    Ok(BulkOperationSummary {
        bulk_operation_arn: arn::format_arn(region, &record.account_id, &record.operation_id),
        bulk_operation_status: status,
        start_time: millis_to_seconds(record.start_time_ms),
        end_time: record.end_time_ms.map(millis_to_seconds),
    })
}

/// Parse a stored status string; `None` for anything outside the six wire
/// statuses.
fn parse_status(status: &str) -> Option<BulkOperationStatus> {
    match status {
        "IN_PROGRESS" => Some(BulkOperationStatus::InProgress),
        "COMPLETED" => Some(BulkOperationStatus::Completed),
        "COMPLETED_WITH_ERRORS" => Some(BulkOperationStatus::CompletedWithErrors),
        "CANCELLING" => Some(BulkOperationStatus::Cancelling),
        "CANCELLED" => Some(BulkOperationStatus::Cancelled),
        "FAILED" => Some(BulkOperationStatus::Failed),
        _ => None,
    }
}

/// The stored vocabulary for one tier.
pub(super) fn tier_str(tier: BulkOperationPerformanceTier) -> &'static str {
    match tier {
        BulkOperationPerformanceTier::Standard => "STANDARD",
        BulkOperationPerformanceTier::Accelerated => "ACCELERATED",
    }
}

/// A stored counter for the wire. The record readers refuse corrupt rows
/// where the contract requires it; rendering clamps at zero rather than
/// wrapping a negative into a huge unsigned figure.
fn non_negative(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

/// Milliseconds since the epoch as the fractional seconds the wire carries.
#[allow(clippy::cast_precision_loss)] // epoch milliseconds stay far below 2^53
fn millis_to_seconds(millis: i64) -> f64 {
    millis as f64 / 1000.0
}

/// The model's minimum and maximum length for `NextToken`, in characters.
/// The model constrains the token to `([0-9a-f]{16})+`, 1 to 1024
/// characters; the service adds a minimum of 112 (measured). 112 is a
/// multiple of 16, so a token padded to it still matches the pattern.
const NEXT_TOKEN_MIN_CHARS: usize = 112;
const NEXT_TOKEN_MAX_CHARS: usize = 1024;

/// Render the pagination cursor: 16 hex digits of `start_time_ms`, then the
/// operation id bytes as lowercase hex, zero padded so the digit count is a
/// multiple of 16 and at least 112 (the model constrains the token to
/// `([0-9a-f]{16})+` and the service refuses a token under 112 characters).
/// The decoder strips trailing zero bytes, so the padding does not change
/// the cursor the token decodes to.
pub(super) fn encode_next_token(start_time_ms: i64, operation_id: &str) -> String {
    let mut token = format!("{:016x}", u64::try_from(start_time_ms).unwrap_or(0));
    token.push_str(&hex::encode(operation_id.as_bytes()));
    while token.len() < NEXT_TOKEN_MIN_CHARS || !token.len().is_multiple_of(16) {
        token.push('0');
    }
    token
}

/// Decode a pagination cursor, answering each failing class with the text
/// the service answers (measured). A token over 1024 characters answers the
/// model length maximum; a token that does not match `([0-9a-f]{16})+`
/// (empty, not lowercase hex, or a length that is not a multiple of 16)
/// answers the model pattern; a well-formed hex token under 112 characters
/// answers the service minimum; a well-formed token of 112 or more that
/// does not decode to a cursor answers `Invalid Request: Provided nextToken
/// is invalid`. A token that decodes repositions within the caller's own
/// list, because the query is keyed by the caller's account, so a forged
/// token leaks nothing.
pub(super) fn decode_next_token(token: &str) -> Result<ListCursor, DynamoDbError> {
    let count = token.chars().count();
    if count > NEXT_TOKEN_MAX_CHARS {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{token}' at 'nextToken' failed to satisfy \
             constraint: Member must have length less than or equal to {NEXT_TOKEN_MAX_CHARS}"
        )));
    }
    if token.is_empty()
        || !count.is_multiple_of(16)
        || !token
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{token}' at 'nextToken' failed to satisfy \
             constraint: Member must satisfy regular expression pattern: ([0-9a-f]{{16}})+"
        )));
    }
    if count < NEXT_TOKEN_MIN_CHARS {
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid Request: 1 validation error detected: Value '{token}' at 'nextToken' \
             failed to satisfy constraint: Member must have length greater than or equal to \
             {NEXT_TOKEN_MIN_CHARS}"
        )));
    }
    let not_a_cursor = || {
        DynamoDbError::ValidationException(format!(
            "Invalid Request: Provided nextToken is invalid: {token}"
        ))
    };
    let start_time = u64::from_str_radix(&token[..16], 16).map_err(|_| not_a_cursor())?;
    let start_time_ms = i64::try_from(start_time).map_err(|_| not_a_cursor())?;
    let mut id_bytes = hex::decode(&token[16..]).map_err(|_| not_a_cursor())?;
    while id_bytes.last() == Some(&0) {
        id_bytes.pop();
    }
    if id_bytes.is_empty() {
        return Err(not_a_cursor());
    }
    let operation_id = String::from_utf8(id_bytes).map_err(|_| not_a_cursor())?;
    Ok(ListCursor {
        start_time_ms,
        operation_id,
    })
}
