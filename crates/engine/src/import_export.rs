// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ImportTable` and `ExportTableToPointInTime` operation handlers.
//!
//! extenddb imports from and exports to the local filesystem instead of S3.
//! Both operations are synchronous — they complete before returning.

use std::future::Future;
use std::pin::Pin;
use std::time::SystemTime;

use serde_json::Value;
use tokio::io::AsyncWriteExt;

use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;
use crate::import_export_io::{read_items, validate_path, validate_path_parent};
use crate::serialize_output;
use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    CreateTableInput, ExportFormat, ImportStatus, ImportTableDescription, ImportTableInput,
    ImportTableOutput, Item, TableCreationParameters, TableKeyInfo, extract_key, item_size_bytes,
};
use extenddb_core::validation::{
    validate_attribute_name_sizes, validate_item_keys, validate_item_size, validate_key_sizes,
};
use extenddb_storage::BatchWriteOp;
use extenddb_storage::error::StorageError;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const IMPORT_WRITE_BATCH_MAX_ITEMS: usize = 1_000;
const IMPORT_WRITE_BATCH_MAX_BYTES: usize = 16 * 1024 * 1024;

struct ExportLineSink<'a> {
    file: &'a mut tokio::fs::File,
}

impl extenddb_storage::ItemExportSink for ExportLineSink<'_> {
    fn write_item<'a>(&'a mut self, item: &'a Item) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let wrapper = serde_json::json!({"Item": item});
            let mut line = serde_json::to_string(&wrapper).map_err(|e| {
                tracing::error!(internal_error = %e, "failed to serialize export item");
                StorageError::Internal("Serialize export item".to_owned())
            })?;
            line.push('\n');
            self.file
                .write_all(line.as_bytes())
                .await
                .map_err(|_| StorageError::Validation("Cannot write export file".to_owned()))
        })
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
    let mut write_batch = ImportWriteBatch::new();

    for item in items {
        let item_size = match validate_import_item(&item, &key_info, &ctx.limits) {
            Ok(size) => size,
            Err(e) => {
                tracing::warn!(reason = e.reason, error = %e, "import: skipping item");
                error_count += 1;
                continue;
            }
        };
        let key = extract_key(&item, &key_info.key_schema);

        if write_batch.should_flush_before(&key, item_size) {
            imported_count += write_batch.flush(ctx, &key_info).await?;
        }
        write_batch.push(key, item, item_size);
    }
    imported_count += write_batch.flush(ctx, &key_info).await?;
    ctx.storage
        .refresh_table_statistics(&key_info)
        .await
        .map_err(storage_err_to_dynamo)?;

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

struct ImportWriteBatch {
    items: Vec<Item>,
    keys: Vec<Item>,
    size_bytes: usize,
}

impl ImportWriteBatch {
    fn new() -> Self {
        Self {
            items: Vec::new(),
            keys: Vec::new(),
            size_bytes: 0,
        }
    }

    fn should_flush_before(&self, key: &Item, item_size: usize) -> bool {
        !self.items.is_empty()
            && (self.items.len() >= IMPORT_WRITE_BATCH_MAX_ITEMS
                || self.size_bytes.saturating_add(item_size) > IMPORT_WRITE_BATCH_MAX_BYTES
                || self.keys.contains(key))
    }

    fn push(&mut self, key: Item, item: Item, item_size: usize) {
        self.keys.push(key);
        self.items.push(item);
        self.size_bytes = self.size_bytes.saturating_add(item_size);
    }

    async fn flush(
        &mut self,
        ctx: &OperationContext,
        key_info: &TableKeyInfo,
    ) -> Result<i64, DynamoDbError> {
        if self.items.is_empty() {
            return Ok(0);
        }

        let ops = self.items.iter().map(BatchWriteOp::Put).collect::<Vec<_>>();
        ctx.storage
            .batch_write_items(key_info, &ops, None)
            .await
            .map_err(storage_err_to_dynamo)?;

        let written = i64::try_from(self.items.len()).unwrap_or(i64::MAX);
        self.items.clear();
        self.keys.clear();
        self.size_bytes = 0;
        Ok(written)
    }
}

fn validate_import_item(
    item: &Item,
    key_info: &TableKeyInfo,
    limits: &LimitsConfig,
) -> Result<usize, ImportItemValidationError> {
    validate_item_keys(item, &key_info.key_schema, &key_info.attribute_definitions)
        .map_err(|e| import_item_validation_error("invalid_keys", e))?;
    validate_item_size(item, limits.max_item_size_bytes)
        .map_err(|e| import_item_validation_error("oversized_item", e))?;
    validate_attribute_name_sizes(item, limits)
        .map_err(|e| import_item_validation_error("oversized_attribute_name", e))?;
    validate_key_sizes(item, &key_info.key_schema, limits)
        .map_err(|e| import_item_validation_error("oversized_key", e))?;
    Ok(item_size_bytes(item))
}

#[derive(Debug)]
struct ImportItemValidationError {
    reason: &'static str,
    message: String,
}

impl std::fmt::Display for ImportItemValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

fn import_item_validation_error(
    reason: &'static str,
    error: impl std::fmt::Display,
) -> ImportItemValidationError {
    ImportItemValidationError {
        reason,
        message: error.to_string(),
    }
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
    if let Some(export_time) = input.export_time
        && (!export_time.is_finite() || export_time < 0.0)
    {
        return Err(DynamoDbError::ValidationException(
            "ExportTime must be a non-negative finite epoch timestamp".to_owned(),
        ));
    }

    let start_time = epoch_seconds();
    let export_format = input.export_format.unwrap_or(ExportFormat::DynamoDbJson);

    let table_name = extract_table_name_from_arn(&input.table_arn)?;

    let key_info = ctx
        .storage
        .table_key_info(&ctx.account_id, &table_name)
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

    let mut sink = ExportLineSink { file: &mut file };
    let summary = ctx
        .storage
        .export_table_items(
            &key_info,
            input.export_time,
            ctx.limits.max_export_item_count,
            &mut sink,
        )
        .await
        .map_err(storage_err_to_dynamo)?;

    let end_time = epoch_seconds();
    let export_arn = format!("{}:export/{}", input.table_arn, uuid::Uuid::new_v4());

    let description = extenddb_core::types::ExportDescription {
        export_arn,
        export_status: extenddb_core::types::ExportStatus::Completed,
        table_arn: input.table_arn,
        table_id: Some(key_info.table_id),
        export_format,
        item_count: summary.item_count,
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

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{AttributeValue, KeySchemaElement, KeyType};

    fn hash_key() -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }
    }

    fn item(pk: &str) -> Item {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
        item
    }

    fn key(pk: &str) -> Item {
        extract_key(&item(pk), &[hash_key()])
    }

    #[test]
    fn import_batch_flushes_before_duplicate_key() {
        let mut batch = ImportWriteBatch::new();
        batch.push(key("a"), item("a"), 10);

        assert!(batch.should_flush_before(&key("a"), 10));
        assert!(!batch.should_flush_before(&key("b"), 10));
    }

    #[test]
    fn import_batch_flushes_at_item_limit() {
        let mut batch = ImportWriteBatch::new();
        for i in 0..IMPORT_WRITE_BATCH_MAX_ITEMS {
            let pk = i.to_string();
            batch.push(key(&pk), item(&pk), 1);
        }

        assert!(batch.should_flush_before(&key("next"), 1));
    }

    #[test]
    fn import_batch_flushes_before_byte_limit_overflow() {
        let mut batch = ImportWriteBatch::new();
        batch.push(key("a"), item("a"), IMPORT_WRITE_BATCH_MAX_BYTES - 1);

        assert!(!batch.should_flush_before(&key("b"), 1));
        assert!(batch.should_flush_before(&key("b"), 2));
    }
}
