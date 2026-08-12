//! Row-key namespacing for catalog records inside `__extenddb_catalog__`.

/// Canonical catalog version pointer.
pub const VERSION: &str = "version:catalog";

/// Encrypted master key material.
pub const KEY_MATERIAL_ENC: &str = "key_material:enc";

pub fn account(account_id: &str) -> String {
    format!("acct:{account_id}")
}

pub fn admin(username: &str) -> String {
    format!("admin:{username}")
}

pub fn user(account_id: &str, user_name: &str) -> String {
    format!("user:{account_id}:{user_name}")
}

pub fn role(account_id: &str, role_name: &str) -> String {
    format!("role:{account_id}:{role_name}")
}

pub fn user_policy(account_id: &str, user_name: &str, policy_name: &str) -> String {
    format!("user_policy:{account_id}:{user_name}:{policy_name}")
}

pub fn role_policy(account_id: &str, role_name: &str, policy_name: &str) -> String {
    format!("role_policy:{account_id}:{role_name}:{policy_name}")
}

pub fn access_key(key_id: &str) -> String {
    format!("access_key:{key_id}")
}

pub fn table_meta(account_id: &str, table_name: &str) -> String {
    format!("table_meta:{account_id}:{table_name}")
}

/// Tags attached to a resource (table ARN). DDB tagging is resource-scoped
/// rather than account-scoped, so the ARN is the full disambiguator.
pub fn tags(arn: &str) -> String {
    format!("tags:{arn}")
}

/// ClientRequestToken dedup record for TransactWriteItems.
pub fn idempotency(token: &str) -> String {
    format!("idem:{token}")
}

/// Prefix for scanning expired idempotency records (used by cleanup worker).
pub const IDEMPOTENCY_SCAN_PREFIX: &str = "idem:";

/// Backup metadata row. ARN includes the source table and a timestamp, so
/// it's globally unique.
pub fn backup_meta(arn: &str) -> String {
    format!("backup:{arn}")
}

/// One backup item per row. `seq` is the source-scan order; zero-padded so
/// catalog scans iterate in deterministic order.
pub fn backup_item(arn: &str, seq: u64) -> String {
    format!("backup_item:{arn}:{seq:020}")
}

/// Prefix for scanning all backup-item rows belonging to one backup ARN.
pub fn backup_item_scan_prefix(arn: &str) -> String {
    format!("backup_item:{arn}:")
}

/// Prefix for scanning all backup-meta rows in a region/account. Embed the
/// account so a list-backups call filters server-side instead of pulling
/// every backup across all accounts.
pub fn backup_scan_prefix_for_account(region: &str, account_id: &str) -> String {
    format!("backup:arn:aws:dynamodb:{region}:{account_id}:")
}

/// Continuous-backups (PITR) configuration row.
pub fn continuous_backups(account_id: &str, table_name: &str) -> String {
    format!("continuous_backups:{account_id}:{table_name}")
}

/// Stream metadata row for a single stream ARN.
pub fn stream_meta(arn: &str) -> String {
    format!("stream:{arn}")
}

/// One stream-record row, keyed by stream ARN + shard id + sequence number.
pub fn stream_record(arn: &str, shard_id: &str, seq: &str) -> String {
    format!("stream_record:{arn}:{shard_id}:{seq}")
}

/// Prefix for scanning every record in one shard of a stream.
pub fn stream_record_shard_prefix(arn: &str, shard_id: &str) -> String {
    format!("stream_record:{arn}:{shard_id}:")
}

/// Prefix for scanning every stream metadata row in an account.
pub fn stream_scan_prefix_for_account(region: &str, account_id: &str) -> String {
    format!("stream:arn:aws:dynamodb:{region}:{account_id}:")
}

/// Prefix passed to `Catalog::scan_prefix` to enumerate every user in an account.
pub fn user_scan_prefix(account_id: &str) -> String {
    format!("user:{account_id}:")
}

/// Prefix to enumerate every access key (across accounts — keys are globally addressable by id).
pub const ACCESS_KEY_SCAN_PREFIX: &str = "access_key:";

/// Prefix to enumerate all table metadata across accounts.
pub const TABLE_META_SCAN_PREFIX: &str = "table_meta:";

/// Prefix to enumerate all table metadata in an account.
pub fn table_meta_scan_prefix(account_id: &str) -> String {
    format!("table_meta:{account_id}:")
}

/// Prefix to enumerate all accounts.
pub const ACCOUNT_SCAN_PREFIX: &str = "acct:";
