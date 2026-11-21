// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ImportTable` and `ExportTableToPointInTime` operation handlers.
//!
//! Each handler first checks that the operation is enabled: a deployment with
//! no import (or export) paths configured refuses it before the request is
//! parsed. That check, and the table-ARN parser export shares with
//! `RestoreTableToPointInTime`, compile on every target. The file I/O that
//! follows lives in `files`, which needs a filesystem; on `wasm32`, which has
//! none, `files` is a stand-in that refuses the operation. The WebAssembly
//! build configures no import or export paths, so its requests are answered by
//! the enabled check and never reach the stand-in.

use serde_json::Value;

use crate::OperationContext;
use extenddb_core::error::DynamoDbError;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod files;

/// The stand-in for the file I/O on `wasm32`, which has no filesystem.
#[cfg(target_arch = "wasm32")]
mod files {
    use serde_json::Value;

    use crate::OperationContext;
    use extenddb_core::error::DynamoDbError;

    pub(super) async fn import_table(
        _body: Value,
        _ctx: &OperationContext,
    ) -> Result<Value, DynamoDbError> {
        Err(no_filesystem("ImportTable"))
    }

    pub(super) async fn export_table(
        _body: Value,
        _ctx: &OperationContext,
    ) -> Result<Value, DynamoDbError> {
        Err(no_filesystem("ExportTableToPointInTime"))
    }

    fn no_filesystem(operation: &str) -> DynamoDbError {
        DynamoDbError::ValidationException(format!(
            "{operation} is not supported by the ExtendDB WebAssembly build: it has no filesystem."
        ))
    }
}

/// Handle an `ImportTable` request.
///
/// Creates a new table from `TableCreationParameters`, then reads items from
/// the local filesystem path in `FileSource` and inserts them. The table must
/// not already exist.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, I/O errors, or parse errors.
pub async fn handle_import_table(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    // P53: Deny import if no import paths are configured (secure default).
    if ctx.import_paths.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "Import is disabled. Configure [import] paths in extenddb.toml to enable.".to_owned(),
        ));
    }
    files::import_table(body, ctx).await
}

/// Handle an `ExportTableToPointInTime` request.
///
/// Reads all items from the table and writes them to a local file.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, I/O errors, or storage errors.
pub async fn handle_export_table(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    // P53: Deny export if no export paths are configured (secure default).
    if ctx.export_paths.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "Export is disabled. Configure [export] paths in extenddb.toml to enable.".to_owned(),
        ));
    }
    files::export_table(body, ctx).await
}

/// Resolve the table an export `TableArn` names, using the one shared resource
/// parser so the engine reads the same table authorization checked.
///
/// `ExportTableToPointInTime` names its source in `TableArn`, which must be
/// exactly a table ARN. A well-formed DynamoDB ARN of the wrong kind (a stream,
/// index, or backup ARN) reaches here after authorization and is refused with
/// the service's message (measured live, `live-r2/arn-kinds-live.json`). An
/// unparseable `TableArn` is refused earlier, before authorization, by
/// `authz_resource::validate_operation_arns`.
///
/// `RestoreTableToPointInTime` reads its `SourceTableArn` through this too,
/// with no check before authorization: an index ARN there is refused here
/// with the same message (measured on DynamoDB). The server refuses every
/// `RestoreTableToPointInTime` before its handler runs.
pub(crate) fn extract_table_name_from_arn(arn: &str) -> Result<String, DynamoDbError> {
    match extenddb_core::arn::parse(arn) {
        Some(extenddb_core::arn::TableResource {
            table, sub: None, ..
        }) => Ok(table),
        _ => Err(DynamoDbError::ValidationException(
            INVALID_TABLE_ARN_REQUEST_MESSAGE.to_owned(),
        )),
    }
}

/// The message DynamoDB returns for an export `TableArn` (or a restore source
/// ARN) that is a well-formed ARN of the wrong resource kind (measured live).
const INVALID_TABLE_ARN_REQUEST_MESSAGE: &str = "Invalid Request: Table ARN is invalid.";

#[cfg(test)]
mod export_arn_tests {
    use super::{INVALID_TABLE_ARN_REQUEST_MESSAGE, extract_table_name_from_arn};
    use extenddb_core::error::DynamoDbError;

    #[test]
    fn exact_table_arn_resolves_to_table_name() {
        assert_eq!(
            extract_table_name_from_arn("arn:aws:dynamodb:us-east-1:1:table/orders").unwrap(),
            "orders"
        );
    }

    #[test]
    fn non_bare_table_arn_is_refused_with_the_measured_message() {
        // A well-formed stream, index, or backup ARN reaches the handler after
        // authorization and is refused with the service's message. A trailing
        // slash and the A1 bypass ARN do not parse and are refused earlier, but
        // the handler refuses them too if reached.
        for arn in [
            "arn:aws:dynamodb:us-east-1:1:table/orders/stream/2026-10-20T16:17:40.188",
            "arn:aws:dynamodb:us-east-1:1:table/orders/index/gsi1",
            "arn:aws:dynamodb:us-east-1:1:table/orders/backup/01-ab",
            "arn:aws:dynamodb:us-east-1:1:table/allowed/stream/table/secret",
            "arn:aws:dynamodb:us-east-1:1:table/orders/",
            "banana",
            "xrn:aws:dynamodb:us-east-1:1:table/orders",
        ] {
            match extract_table_name_from_arn(arn) {
                Err(DynamoDbError::ValidationException(m)) => {
                    assert_eq!(m, INVALID_TABLE_ARN_REQUEST_MESSAGE, "for {arn}");
                }
                other => panic!("expected ValidationException for {arn}, got {other:?}"),
            }
        }
    }
}
