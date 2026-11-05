// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use serde::Deserialize;

/// All configurable limits with DynamoDB-compatible defaults.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    #[serde(default = "default_max_item_size")]
    pub max_item_size_bytes: usize,
    #[serde(default = "default_max_pk_size")]
    pub max_partition_key_size_bytes: usize,
    #[serde(default = "default_max_sk_size")]
    pub max_sort_key_size_bytes: usize,
    #[serde(default = "default_max_tables")]
    pub max_tables_per_account: usize,
    #[serde(default = "default_max_gsis")]
    pub max_gsis_per_table: usize,
    #[serde(default = "default_max_lsis")]
    pub max_lsis_per_table: usize,
    #[serde(default = "default_list_tables_max")]
    pub list_tables_max_per_page: i32,
    #[serde(default = "default_max_table_name_len")]
    pub max_table_name_length: usize,
    #[serde(default = "default_min_table_name_len")]
    pub min_table_name_length: usize,
    #[serde(default = "default_per_table_max_rcu")]
    pub per_table_max_rcu: u64,
    #[serde(default = "default_per_table_max_wcu")]
    pub per_table_max_wcu: u64,
    #[serde(default = "default_per_account_max_rcu")]
    pub per_account_max_rcu: u64,
    #[serde(default = "default_per_account_max_wcu")]
    pub per_account_max_wcu: u64,
    /// Preview extension: allow multi-part (composite) keys on base tables.
    /// When `false` (default), base tables follow standard `DynamoDB` rules
    /// (1 HASH + optional 1 RANGE). GSIs always allow multi-part keys.
    #[serde(default)]
    pub allow_multipart_table_keys: bool,
    /// REQ-LIM-004: Maximum attribute name size in bytes.
    #[serde(default = "default_max_attribute_name_bytes")]
    pub max_attribute_name_bytes: usize,
    /// Maximum number of tokens in a single expression (condition, update, projection, key-condition).
    #[serde(default = "default_max_expression_tokens")]
    pub max_expression_tokens: usize,
    /// Maximum size in bytes of a single expression string, measured on the
    /// raw text before `#name` / `:value` substitution (DynamoDB: 4096).
    #[serde(default = "default_max_expression_length_bytes")]
    pub max_expression_length_bytes: usize,
    /// Maximum nesting depth in condition expressions (parentheses, NOT, AND/OR).
    #[serde(default = "default_max_expression_depth")]
    pub max_expression_depth: usize,
    /// Maximum policy document size in bytes (before JSON parsing).
    #[serde(default = "default_max_policy_document_bytes")]
    pub max_policy_document_bytes: usize,
    /// Maximum import file size in bytes.
    #[serde(default = "default_max_import_file_bytes")]
    pub max_import_file_bytes: u64,
    /// Maximum number of items in an import.
    #[serde(default = "default_max_import_item_count")]
    pub max_import_item_count: u64,
    /// Maximum export item count.
    #[serde(default = "default_max_export_item_count")]
    pub max_export_item_count: u64,
    /// Reject bare reserved keywords in expressions (DynamoDB-compatible default: true).
    #[serde(default = "default_enforce_reserved_keywords")]
    pub enforce_reserved_keywords: bool,
    /// Maximum non-terminal bulk operations per account. `StartBulkPutItems`
    /// rejects above it.
    #[serde(default = "default_max_concurrent_bulk_operations")]
    pub max_concurrent_bulk_operations: u32,
    /// Maximum enumerated (compressed) input bytes per bulk operation.
    /// `StartBulkPutItems` rejects above it.
    #[serde(default = "default_max_bulk_input_bytes")]
    pub max_bulk_input_bytes: u64,
    /// Maximum enumerated input object count per bulk operation. Above it
    /// the Start is accepted and the operation ends `FAILED` with
    /// `S3ObjectsCountLimitExceeded` (the hosted service's shape; its quota
    /// is 50,000).
    #[serde(default = "default_max_bulk_input_files")]
    pub max_bulk_input_files: u64,
    /// Maximum size of one enumerated input object. Above it the Start is
    /// accepted and the operation ends `FAILED` with `InputObjectTooLarge`
    /// (measured on the service; the hosted service's cap is 5 GiB, rendered
    /// as `5.0 GB` in its message).
    #[serde(default = "default_max_bulk_input_object_bytes")]
    pub max_bulk_input_object_bytes: u64,
    /// Maximum `ClientToken` length in bytes on `StartBulkPutItems`. The API
    /// model puts no length cap on the token; this bounds what one request
    /// can ask the catalog to store.
    #[serde(default = "default_max_bulk_client_token_bytes")]
    pub max_bulk_client_token_bytes: usize,
    /// Days a terminal bulk operation record is kept before the retention
    /// worker purges it, its report records, and its ledger rows.
    #[serde(default = "default_bulk_operation_retention_days")]
    pub bulk_operation_retention_days: u32,
    /// Bulk operations one process runs at once (count of runner tasks).
    #[serde(default = "default_bulk_operation_worker_concurrency")]
    pub bulk_operation_worker_concurrency: usize,
    /// Of the concurrent runner tasks, at most this many per account
    /// (count), so one account cannot hold every slot.
    #[serde(default = "default_bulk_operation_worker_per_account")]
    pub bulk_operation_worker_per_account: usize,
    /// Length in seconds of the lease a runner holds on its operation;
    /// renewed at every checkpoint.
    #[serde(default = "default_bulk_operation_lease_seconds")]
    pub bulk_operation_lease_seconds: u64,
    /// Rows per checkpoint (count): the runner persists cursor, counters,
    /// and report records after this many consumed lines.
    #[serde(default = "default_bulk_put_batch_size")]
    pub bulk_put_batch_size: usize,
    /// Rows per storage transaction (count): a bulk runner hands this many
    /// admitted rows to storage in one batch. Capped at 100 when read, and
    /// bounded by `bulk_put_batch_size` because the batch is flushed at every
    /// checkpoint; raising it alone above the checkpoint size has no effect
    /// and is logged. A batch holds its row locks (and on SQLite the process
    /// write lock) for its whole transaction.
    #[serde(default = "default_bulk_put_write_batch_rows")]
    pub bulk_put_write_batch_rows: usize,
    /// Longest accepted input line in bytes; a longer line abandons the
    /// rest of its file.
    #[serde(default = "default_max_bulk_line_bytes")]
    pub max_bulk_line_bytes: usize,
    /// Physical lines one bulk operation may consume (count, blank lines
    /// included); exceeding it fails the operation.
    #[serde(default = "default_max_bulk_item_count")]
    pub max_bulk_item_count: u64,
    /// Report records kept per bulk operation (count); records beyond it
    /// are dropped and the drop is logged.
    #[serde(default = "default_max_bulk_report_records")]
    pub max_bulk_report_records: u64,
    /// Serialized bytes of report records kept per bulk operation; records
    /// beyond it are dropped and the drop is logged.
    #[serde(default = "default_max_bulk_report_bytes")]
    pub max_bulk_report_bytes: u64,
    /// Terminal-born bulk operation starts per account per hour (rows
    /// inserted already `FAILED`, from requests naming an absent bucket). At
    /// or above it `StartBulkPutItems` answers `ThrottlingException` and
    /// inserts nothing; the handler treats values below 1 as 1.
    #[serde(default = "default_max_bulk_failed_starts_per_hour")]
    pub max_bulk_failed_starts_per_hour: u64,
    /// Ceiling on bulk put writes in this process, in write request units
    /// per second (1 KB of item, rounded up, per unit; the unit
    /// `BulkPutBytes` charges). Shared by every operation the process runs,
    /// so bulk loads leave the rest of the storage write path to request
    /// traffic on the same server. `0` disables the ceiling. A request's
    /// own `MaxWriteRequestUnits` paces that operation on top of this.
    #[serde(default = "default_bulk_put_write_units_per_second")]
    pub bulk_put_write_units_per_second: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_item_size_bytes: default_max_item_size(),
            max_partition_key_size_bytes: default_max_pk_size(),
            max_sort_key_size_bytes: default_max_sk_size(),
            max_tables_per_account: default_max_tables(),
            max_gsis_per_table: default_max_gsis(),
            max_lsis_per_table: default_max_lsis(),
            list_tables_max_per_page: default_list_tables_max(),
            max_table_name_length: default_max_table_name_len(),
            min_table_name_length: default_min_table_name_len(),
            per_table_max_rcu: default_per_table_max_rcu(),
            per_table_max_wcu: default_per_table_max_wcu(),
            per_account_max_rcu: default_per_account_max_rcu(),
            per_account_max_wcu: default_per_account_max_wcu(),
            allow_multipart_table_keys: false,
            max_attribute_name_bytes: default_max_attribute_name_bytes(),
            max_expression_tokens: default_max_expression_tokens(),
            max_expression_length_bytes: default_max_expression_length_bytes(),
            max_expression_depth: default_max_expression_depth(),
            max_policy_document_bytes: default_max_policy_document_bytes(),
            max_import_file_bytes: default_max_import_file_bytes(),
            max_import_item_count: default_max_import_item_count(),
            max_export_item_count: default_max_export_item_count(),
            enforce_reserved_keywords: default_enforce_reserved_keywords(),
            max_concurrent_bulk_operations: default_max_concurrent_bulk_operations(),
            max_bulk_input_bytes: default_max_bulk_input_bytes(),
            max_bulk_input_files: default_max_bulk_input_files(),
            max_bulk_input_object_bytes: default_max_bulk_input_object_bytes(),
            max_bulk_client_token_bytes: default_max_bulk_client_token_bytes(),
            bulk_operation_retention_days: default_bulk_operation_retention_days(),
            bulk_operation_worker_concurrency: default_bulk_operation_worker_concurrency(),
            bulk_operation_worker_per_account: default_bulk_operation_worker_per_account(),
            bulk_operation_lease_seconds: default_bulk_operation_lease_seconds(),
            bulk_put_batch_size: default_bulk_put_batch_size(),
            bulk_put_write_batch_rows: default_bulk_put_write_batch_rows(),
            max_bulk_line_bytes: default_max_bulk_line_bytes(),
            max_bulk_item_count: default_max_bulk_item_count(),
            max_bulk_report_records: default_max_bulk_report_records(),
            max_bulk_report_bytes: default_max_bulk_report_bytes(),
            max_bulk_failed_starts_per_hour: default_max_bulk_failed_starts_per_hour(),
            bulk_put_write_units_per_second: default_bulk_put_write_units_per_second(),
        }
    }
}

fn default_max_item_size() -> usize {
    409_600
}
fn default_max_pk_size() -> usize {
    2048
}
fn default_max_sk_size() -> usize {
    1024
}
fn default_max_tables() -> usize {
    2500
}
fn default_max_gsis() -> usize {
    20
}
fn default_max_lsis() -> usize {
    5
}
fn default_list_tables_max() -> i32 {
    100
}
fn default_max_table_name_len() -> usize {
    255
}
fn default_min_table_name_len() -> usize {
    3
}
fn default_per_table_max_rcu() -> u64 {
    40_000
}
fn default_per_table_max_wcu() -> u64 {
    40_000
}
fn default_per_account_max_rcu() -> u64 {
    80_000
}
fn default_per_account_max_wcu() -> u64 {
    80_000
}
fn default_max_attribute_name_bytes() -> usize {
    65_535
}
fn default_max_expression_tokens() -> usize {
    4096
}
fn default_max_expression_length_bytes() -> usize {
    4096
}
fn default_max_expression_depth() -> usize {
    150
}
fn default_max_policy_document_bytes() -> usize {
    // AWS IAM limit: 6,144 bytes for inline policies.
    6_144
}
fn default_max_import_file_bytes() -> u64 {
    // 10 GB default.
    10_737_418_240
}
fn default_max_import_item_count() -> u64 {
    10_000_000
}
fn default_max_export_item_count() -> u64 {
    10_000_000
}
fn default_enforce_reserved_keywords() -> bool {
    true
}
fn default_max_concurrent_bulk_operations() -> u32 {
    50
}
fn default_max_bulk_input_bytes() -> u64 {
    // 10 GB, matching the import file cap.
    10_737_418_240
}
fn default_max_bulk_input_files() -> u64 {
    // The hosted service's per-operation object quota.
    50_000
}
fn default_max_bulk_input_object_bytes() -> u64 {
    // 5 GiB, the hosted service's per-object cap.
    5_368_709_120
}
fn default_max_bulk_client_token_bytes() -> usize {
    1024
}
fn default_bulk_operation_retention_days() -> u32 {
    30
}
fn default_bulk_operation_worker_concurrency() -> usize {
    4
}
fn default_bulk_operation_worker_per_account() -> usize {
    2
}
fn default_bulk_operation_lease_seconds() -> u64 {
    60
}
fn default_bulk_put_batch_size() -> usize {
    25
}
fn default_bulk_put_write_batch_rows() -> usize {
    25
}
fn default_max_bulk_line_bytes() -> usize {
    // 4 MiB.
    4_194_304
}
fn default_max_bulk_item_count() -> u64 {
    100_000_000
}
fn default_max_bulk_report_records() -> u64 {
    1_000_000
}
fn default_max_bulk_report_bytes() -> u64 {
    // 1 GiB.
    1_073_741_824
}
fn default_max_bulk_failed_starts_per_hour() -> u64 {
    100
}
fn default_bulk_put_write_units_per_second() -> u64 {
    // The per-account write ceiling the request path enforces
    // (`per_account_max_wcu`): bulk writes in one process are held to the
    // same rate one account's request traffic is. Far above what one runner
    // sustains, so it binds only when many operations saturate a server.
    default_per_account_max_wcu()
}
