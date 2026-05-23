// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ImportTable` and `ExportTableToPointInTime` operation handlers.
//!
//! extenddb imports from and exports to the local filesystem instead of S3.
//! Both operations are synchronous — they complete before returning.

use std::time::SystemTime;

use serde_json::Value;

use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;
use crate::import_export_io::{read_items, validate_path, validate_path_parent};
use crate::serialize_output;
use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    CreateTableInput, ExportFormat, ImportStatus, ImportTableDescription, ImportTableInput,
    ImportTableOutput, Item, TableCreationParameters,
};
use extenddb_core::validation::{
    validate_attribute_name_sizes, validate_item_keys, validate_item_nesting_depth,
    validate_item_size, validate_key_sizes,
};

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

    let input: ImportTableInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    let start_time = epoch_seconds();
    let tcp = &input.table_creation_parameters;

    let create_input = create_table_input_from_params(tcp);

    let table_desc = ctx
        .storage
        .create_table(&ctx.account_id, create_input)
        .await
        .map_err(storage_err_to_dynamo)?;

    let table_arn = table_desc.table_arn.clone();
    let table_id = table_desc.table_id.clone();

    // Drop any cached negative TableKeyInfo from a prior probe so subsequent
    // requests against the new table see it without TTL lag. (Tags aren't
    // propagated through ImportTable today, so resource_tags doesn't need
    // invalidation here — see create_table_input_from_params.)
    ctx.auth_cache
        .invalidate_table_key_info(&ctx.account_id, &tcp.table_name)
        .await;

    wait_for_table_active(ctx, &tcp.table_name).await?;

    let key_info = ctx
        .storage
        .table_key_info(&ctx.account_id, &tcp.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Validate and canonicalize the source path.
    let source_path = validate_path(&input.file_source.path, &ctx.import_paths)?;

    // Check file size against limit.
    let file_meta = std::fs::metadata(&source_path).map_err(|_| {
        DynamoDbError::ValidationException("Cannot read source file metadata".to_owned())
    })?;
    if file_meta.len() > ctx.limits.max_import_file_bytes {
        return Err(DynamoDbError::ValidationException(format!(
            "Import file size ({} bytes) exceeds maximum ({} bytes)",
            file_meta.len(),
            ctx.limits.max_import_file_bytes
        )));
    }

    // Read items using spawn_blocking to avoid blocking the async runtime.
    let format = input.input_format;
    let format_options = input.input_format_options.clone();
    let max_items = ctx.limits.max_import_item_count;
    let items = tokio::task::spawn_blocking(move || {
        read_items(&source_path, format, format_options.as_ref(), max_items)
    })
    .await
    .map_err(|e| {
        tracing::error!(internal_error = %e, "import spawn_blocking failed");
        DynamoDbError::InternalServerError("Internal server error".to_owned())
    })??;

    let mut imported_count: i64 = 0;
    let mut error_count: i64 = 0;
    let processed_count = i64::try_from(items.len()).unwrap_or(i64::MAX);

    for item in items {
        if let Err(e) =
            validate_item_keys(&item, &key_info.key_schema, &key_info.attribute_definitions)
        {
            tracing::warn!(error = %e, "import: skipping item with invalid keys");
            error_count += 1;
            continue;
        }
        if let Err(e) = validate_item_nesting_depth(&item) {
            tracing::warn!(error = %e, "import: skipping item with excessive nesting");
            error_count += 1;
            continue;
        }
        if let Err(e) = validate_item_size(&item, ctx.limits.max_item_size_bytes) {
            tracing::warn!(error = %e, "import: skipping oversized item");
            error_count += 1;
            continue;
        }
        if let Err(e) = validate_attribute_name_sizes(&item, &ctx.limits) {
            tracing::warn!(error = %e, "import: skipping item with oversized attribute name");
            error_count += 1;
            continue;
        }
        if let Err(e) = validate_key_sizes(&item, &key_info.key_schema, &ctx.limits) {
            tracing::warn!(error = %e, "import: skipping item with oversized key");
            error_count += 1;
            continue;
        }

        let maps = extenddb_core::expression::ExpressionMaps::default();
        ctx.storage
            .put_item(&key_info, item, false, None, &maps, None)
            .await
            .map_err(storage_err_to_dynamo)?;
        imported_count += 1;
    }

    let end_time = epoch_seconds();
    let import_arn = format!("{}:import/{}", table_arn, uuid::Uuid::new_v4());

    let description = ImportTableDescription {
        import_arn,
        import_status: ImportStatus::Completed,
        table_arn,
        table_id: Some(table_id),
        file_source: input.file_source,
        input_format: input.input_format,
        table_creation_parameters: input.table_creation_parameters,
        error_count,
        processed_item_count: processed_count,
        imported_item_count: imported_count,
        start_time: Some(start_time),
        end_time: Some(end_time),
        failure_code: None,
        failure_message: None,
    };

    serialize_output(&ImportTableOutput {
        import_table_description: description,
    })
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

    let input: extenddb_core::types::ExportTableToPointInTimeInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    let start_time = epoch_seconds();
    let export_format = input.export_format.unwrap_or(ExportFormat::DynamoDbJson);

    let table_name = extract_table_name_from_arn(&input.table_arn)?;

    let key_info = ctx
        .storage
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    let table_desc = ctx
        .storage
        .describe_table(
            &ctx.account_id,
            extenddb_core::types::DescribeTableInput {
                table_name: table_name.clone(),
            },
        )
        .await
        .map_err(storage_err_to_dynamo)?;

    let output_path = validate_path_parent(
        input
            .resolve_file_path()
            .map_err(|e| DynamoDbError::ValidationException(e.to_owned()))?,
        &ctx.export_paths,
    )?;
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|_| {
            DynamoDbError::ValidationException("Cannot create output directory".to_owned())
        })?;
    }
    let mut file = tokio::fs::File::create(&output_path)
        .await
        .map_err(|_| DynamoDbError::ValidationException("Cannot create export file".to_owned()))?;

    let mut item_count: i64 = 0;
    let max_export_items = ctx.limits.max_export_item_count;
    let mut exclusive_start_key: Option<Item> = None;
    loop {
        let (items, last_key) = ctx
            .storage
            .scan(
                &key_info,
                Some(1000),
                exclusive_start_key.as_ref(),
                None,
                None,
                None,
            )
            .await
            .map_err(storage_err_to_dynamo)?;

        item_count += i64::from(u16::try_from(items.len()).unwrap_or(u16::MAX));

        if u64::try_from(item_count).unwrap_or(u64::MAX) > max_export_items {
            return Err(DynamoDbError::ValidationException(format!(
                "Export item count exceeds maximum ({max_export_items})"
            )));
        }

        for item in &items {
            let wrapper = serde_json::json!({"Item": item});
            let mut line = serde_json::to_string(&wrapper).map_err(|e| {
                tracing::error!(internal_error = %e, "failed to serialize export item");
                DynamoDbError::InternalServerError("Internal server error".to_owned())
            })?;
            line.push('\n');
            tokio::io::AsyncWriteExt::write_all(&mut file, line.as_bytes())
                .await
                .map_err(|_| {
                    DynamoDbError::ValidationException("Cannot write export file".to_owned())
                })?;
        }

        if last_key.is_none() {
            break;
        }
        exclusive_start_key = last_key;
    }

    let end_time = epoch_seconds();
    let export_arn = format!("{}:export/{}", input.table_arn, uuid::Uuid::new_v4());

    let description = extenddb_core::types::ExportDescription {
        export_arn,
        export_status: extenddb_core::types::ExportStatus::Completed,
        table_arn: input.table_arn,
        table_id: Some(table_desc.table_id),
        export_format,
        item_count,
        billed_size_bytes: 0,
        start_time: Some(start_time),
        end_time: Some(end_time),
        failure_code: None,
        failure_message: None,
    };

    serialize_output(&extenddb_core::types::ExportTableToPointInTimeOutput {
        export_description: description,
    })
}

fn create_table_input_from_params(tcp: &TableCreationParameters) -> CreateTableInput {
    CreateTableInput {
        table_name: tcp.table_name.clone(),
        attribute_definitions: tcp.attribute_definitions.clone(),
        key_schema: tcp.key_schema.clone(),
        billing_mode: tcp.billing_mode,
        provisioned_throughput: tcp.provisioned_throughput.clone(),
        global_secondary_indexes: tcp.global_secondary_indexes.clone(),
        local_secondary_indexes: None,
        stream_specification: None,
        sse_specification: None,
        tags: None,
        deletion_protection_enabled: None,
        table_class: None,
    }
}

async fn wait_for_table_active(
    ctx: &OperationContext,
    table_name: &str,
) -> Result<(), DynamoDbError> {
    use extenddb_core::types::{DescribeTableInput, TableStatus};

    for _ in 0..120 {
        let desc = ctx
            .storage
            .describe_table(
                &ctx.account_id,
                DescribeTableInput {
                    table_name: table_name.to_owned(),
                },
            )
            .await
            .map_err(storage_err_to_dynamo)?;
        if desc.table_status == TableStatus::Active {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Err(DynamoDbError::InternalServerError(format!(
        "Table {table_name} did not become ACTIVE within timeout"
    )))
}

fn extract_table_name_from_arn(arn: &str) -> Result<String, DynamoDbError> {
    arn.rsplit_once("table/")
        .map(|(_, name)| name.to_owned())
        .ok_or_else(|| DynamoDbError::ValidationException(format!("Invalid table ARN: {arn}")))
}

fn epoch_seconds() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
