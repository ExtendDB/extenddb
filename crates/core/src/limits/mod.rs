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
