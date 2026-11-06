// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request handlers for the four bulk operations: `StartBulkPutItems`,
//! `DescribeBulkOperation`, `ListBulkOperations`, and
//! `CancelBulkOperation`.
//!
//! `StartBulkPutItems` validates in seven observable steps: body
//! deserialization, field constraints, feature support, the bulk store,
//! table lookup, input enumeration, and the start transaction. The order is
//! part of the wire contract because each step has its own error; a request
//! wrong at two steps gets the earlier step's message. The other three
//! handlers share the three-layer operation ARN validation: shape, id
//! shape, then existence in the caller's account. A foreign-account or
//! foreign-region ARN answers the third layer without consulting storage,
//! so the caller cannot learn whether the foreign operation exists.

use serde_json::Value;
use sha2::{Digest, Sha256};

use extenddb_core::error::DynamoDbError;
use extenddb_core::error::{ErrorMessageKey, error_message};
use extenddb_core::types::{
    BulkOperationDescription, BulkOperationInputFormat, BulkOperationPerformanceTier,
    BulkOperationStatus, BulkOperationSummary, BulkOperationTableDetail, CancelBulkOperationInput,
    CancelBulkOperationOutput, DescribeBulkOperationInput, DescribeBulkOperationOutput,
    DescribeTableInput, InputCompressionType, InputS3Configuration, ListBulkOperationsInput,
    ListBulkOperationsOutput, OutputS3Configuration, StartBulkPutItemsInput,
    StartBulkPutItemsOutput, TableDescription, TableStatus,
};
use extenddb_storage::{
    BulkOperationRecord, CancelOutcome, InputFile, ListCursor, NewBulkOperation, StartOutcome,
};

use crate::OperationContext;
use crate::bulk_put::arn;
use crate::bulk_put::epoch_millis;
use crate::bulk_put::fingerprint;
use crate::bulk_put::source::{BucketRef, SourceError};
use crate::bulk_put::usage;
use crate::create_table::storage_err_to_dynamo;
use crate::serialize_output;

mod describe;
mod model_pass;
mod ops;
mod start;
mod validate;

#[cfg(test)]
mod tests;

use describe::*;
use model_pass::check_model_constraints;
use validate::*;

pub use ops::{
    handle_cancel_bulk_operation, handle_describe_bulk_operation, handle_list_bulk_operations,
};
pub use start::handle_start_bulk_put_items;

/// The two stored configurations together may serialize to at most this
/// many bytes, so the catalog row is bounded. The cap is this server's
/// own; the hosted service stores S3 locations and has no equivalent.
const MAX_CONFIGURATION_BYTES: usize = 8192;

/// `ListBulkOperations` page size: the model's bounds and the default when
/// `MaxResults` is absent.
const MAX_RESULTS_CEILING: i64 = 25;

/// The page size when `MaxResults` is absent (measured).
const MAX_RESULTS_DEFAULT: i64 = 10;
/// The model's cap on each `S3KeyPrefix` field, in characters.
const MAX_PREFIX_CHARS: usize = 1024;

/// The minimum the service applies to `S3Bucket` and `S3KeyPrefix` beyond
/// the model (measured on the service).
const MIN_SERVICE_NAME_CHARS: usize = 3;

/// The wire spellings the two bucket members are named by when the service
/// refuses the name outright rather than reporting a model constraint
/// (measured on the service: the message is `Invalid Request:
/// <member> is invalid`).
const INPUT_BUCKET_MEMBER: &str = "InputS3Configuration.S3Bucket";
const OUTPUT_BUCKET_MEMBER: &str = "OutputS3Configuration.S3Bucket";
/// The model's cap on `S3SseKmsKeyId`, in characters.
const MAX_KMS_KEY_ID_CHARS: usize = 2048;
/// The model's cap on `TableName` (typed as `TableArn`), in characters. The
/// shape also carries a minimum of 1.
const MAX_TABLE_NAME_CHARS: usize = 1024;

/// Measured mismatch message for a replayed token with different
/// parameters.
const IDEMPOTENT_MISMATCH_MESSAGE: &str = "Specified idempotent token was already used with \
     different request parameters within the idempotency window";

/// Measured message for a second start on a table with a non-terminal
/// operation.
const TABLE_BUSY_MESSAGE: &str = "Table has ongoing bulk operations, please wait and try again";

/// The service's minimum for `MaxWriteRequestUnits` (measured on the service:
/// 1000 and 1023 refused with the `Invalid Request:` message, 1024 started).
const MIN_WRITE_REQUEST_UNITS: i64 = 1024;

/// Measured message for a start on a table being deleted: answered
/// immediately after `DeleteTable` and three seconds
/// later. The table name is not named, unlike the in-use message. The
/// start transaction answers the same text for a table set `DELETING`
/// after the lookup.
const TABLE_DELETING_MESSAGE: &str = extenddb_storage::bulk_operation_store::TABLE_DELETING_MESSAGE;

/// Measured message for a start on a table with an index being created
/// (measured on the service, both creation phases).
const INDEX_OPERATION_MESSAGE: &str =
    "Table has ongoing index operations, please wait and try again";

/// Maps a storage error for the bulk handlers. Every variant the table
/// handlers log at ERROR is mapped and logged here, with the same answer
/// and the same fields, so the line carries the bulk handlers' target
/// rather than the table handlers' one; every other variant maps as the
/// table handlers map it.
pub(in crate::bulk_put) fn bulk_storage_err(
    error: extenddb_storage::error::StorageError,
) -> DynamoDbError {
    use extenddb_storage::error::StorageError;
    let internal = || DynamoDbError::InternalServerError("Internal server error".to_owned());
    match error {
        StorageError::Internal(message) => {
            tracing::error!(internal_error = %message, "bulk operation storage internal error");
            internal()
        }
        StorageError::Connection(message) => {
            tracing::error!(internal_error = %message, "storage connection error");
            DynamoDbError::ServiceUnavailable("Service is temporarily unavailable".to_owned())
        }
        other => storage_err_to_dynamo(other),
    }
}
