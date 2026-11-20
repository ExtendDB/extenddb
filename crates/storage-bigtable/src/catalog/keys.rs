// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Row-key namespacing for catalog records inside `__extenddb_catalog__`.

/// Canonical catalog version pointer.
pub const VERSION: &str = "version:catalog";

/// Prefix for migration ledger entries (`migration_ledger:<store>:<id>`).
pub const MIGRATION_LEDGER_PREFIX: &str = "migration_ledger:";

/// Runtime settings prefix (`setting:<key>`).
pub const SETTING_SCAN_PREFIX: &str = "setting:";

pub fn setting(key: &str) -> String {
    format!("setting:{key}")
}

pub fn migration_ledger_prefix_for_store(store: &str) -> String {
    format!("{MIGRATION_LEDGER_PREFIX}{store}:")
}

pub fn migration_ledger_row(store: &str, id: u32) -> String {
    format!("{MIGRATION_LEDGER_PREFIX}{store}:{id:04}")
}

pub fn account(account_id: &str) -> String {
    format!("acct:{account_id}")
}

pub fn admin(username: &str) -> String {
    format!("admin:{username}")
}

pub fn user(account_id: &str, user_name: &str) -> String {
    format!("user:{account_id}:{user_name}")
}

pub fn group(account_id: &str, group_name: &str) -> String {
    format!("group:{account_id}:{group_name}")
}

pub fn group_scan_prefix(account_id: &str) -> String {
    format!("group:{account_id}:")
}

pub fn group_member(account_id: &str, group_name: &str, user_name: &str) -> String {
    format!("group_member:{account_id}:{group_name}:{user_name}")
}

pub fn group_member_scan_prefix(account_id: &str, group_name: &str) -> String {
    format!("group_member:{account_id}:{group_name}:")
}

pub fn group_member_account_prefix(account_id: &str) -> String {
    format!("group_member:{account_id}:")
}

pub fn role(account_id: &str, role_name: &str) -> String {
    format!("role:{account_id}:{role_name}")
}

pub fn role_scan_prefix(account_id: &str) -> String {
    format!("role:{account_id}:")
}

pub fn user_policy(account_id: &str, user_name: &str, policy_name: &str) -> String {
    format!("user_policy:{account_id}:{user_name}:{policy_name}")
}

pub fn user_policy_scan_prefix(account_id: &str, user_name: &str) -> String {
    format!("user_policy:{account_id}:{user_name}:")
}

pub fn user_policy_account_prefix(account_id: &str) -> String {
    format!("user_policy:{account_id}:")
}

pub fn group_policy(account_id: &str, group_name: &str, policy_name: &str) -> String {
    format!("group_policy:{account_id}:{group_name}:{policy_name}")
}

pub fn group_policy_scan_prefix(account_id: &str, group_name: &str) -> String {
    format!("group_policy:{account_id}:{group_name}:")
}

pub fn group_policy_account_prefix(account_id: &str) -> String {
    format!("group_policy:{account_id}:")
}

pub fn role_policy(account_id: &str, role_name: &str, policy_name: &str) -> String {
    format!("role_policy:{account_id}:{role_name}:{policy_name}")
}

pub fn role_policy_scan_prefix(account_id: &str, role_name: &str) -> String {
    format!("role_policy:{account_id}:{role_name}:")
}

pub fn role_policy_account_prefix(account_id: &str) -> String {
    format!("role_policy:{account_id}:")
}

pub fn boundary(account_id: &str, principal_type: &str, principal_name: &str) -> String {
    format!("boundary:{account_id}:{principal_type}:{principal_name}")
}

pub fn boundary_account_prefix(account_id: &str) -> String {
    format!("boundary:{account_id}:")
}

pub fn user_tags(account_id: &str, user_name: &str) -> String {
    format!("user_tags:{account_id}:{user_name}")
}

pub fn user_tags_account_prefix(account_id: &str) -> String {
    format!("user_tags:{account_id}:")
}

pub fn role_tags(account_id: &str, role_name: &str) -> String {
    format!("role_tags:{account_id}:{role_name}")
}

pub fn role_tags_account_prefix(account_id: &str) -> String {
    format!("role_tags:{account_id}:")
}

pub fn access_key(key_id: &str) -> String {
    format!("access_key:{key_id}")
}

pub fn iam_session(session_token: &str) -> String {
    format!("iam_session:{session_token}")
}

pub const IAM_SESSION_SCAN_PREFIX: &str = "iam_session:";

pub fn iam_session_access_key(access_key_id: &str) -> String {
    format!("iam_session_ak:{access_key_id}")
}

pub fn failed_login(principal: &str, nanos: i128, unique: &str) -> String {
    let clamped = nanos.max(0);
    format!("failed_login:{principal}\x00{clamped:020}:{unique}")
}

pub fn failed_login_principal_prefix(principal: &str) -> String {
    format!("failed_login:{principal}\x00")
}

pub fn failed_login_account_prefix(account_id: &str) -> String {
    format!("failed_login:{account_id}/")
}

pub const FAILED_LOGIN_SCAN_PREFIX: &str = "failed_login:";

pub fn metric_row(bucket_secs: i64, dims_hash: &str) -> String {
    let clamped = bucket_secs.max(0);
    format!("metric:{clamped:012}:{dims_hash}")
}

pub fn metric_bucket_prefix(bucket_secs: i64) -> String {
    let clamped = bucket_secs.max(0);
    format!("metric:{clamped:012}:")
}

pub const METRIC_SCAN_PREFIX: &str = "metric:";

pub fn table_meta(account_id: &str, table_name: &str) -> String {
    format!("table_meta:{account_id}:{table_name}")
}

/// Tags attached to a resource (table ARN). DDB tagging is resource-scoped
/// rather than account-scoped, so the ARN is the full disambiguator.
pub fn tags(arn: &str) -> String {
    format!("tags:{arn}")
}

/// ClientRequestToken dedup record for TransactWriteItems (scoped per account).
pub fn idempotency(account_id: &str, token: &str) -> String {
    format!("idem:{account_id}:{token}")
}

/// Prefix for scanning expired idempotency records (used by cleanup worker).
pub const IDEMPOTENCY_SCAN_PREFIX: &str = "idem:";

/// Stream shard row (`ShardMeta`) keyed by its `stream_lineage` shard id.
pub fn stream_shard(shard_id: &str) -> String {
    format!("stream_shard:{shard_id}")
}

/// Prefix for scanning every shard belonging to `table_id`.
pub fn stream_shard_table_prefix(table_id: &str) -> String {
    format!("stream_shard:shardId-{table_id}-")
}

/// One stream-record row, keyed by shard id + 26-digit decimal sequence number.
pub fn stream_record(shard_id: &str, seq: &str) -> String {
    format!("stream_record:{shard_id}:{seq}")
}

/// Prefix for scanning every record in one shard of a stream.
pub fn stream_record_shard_prefix(shard_id: &str) -> String {
    format!("stream_record:{shard_id}:")
}

/// Exact-once stream event deduplication marker (`stream_dedup:<shard_id>:<dedup_key>`)
/// so concurrent or retried `roll_forward` passes never allocate a second sequence slot
/// for an already-published event (`BUG-04`, `BUG-05`).
pub fn stream_dedup(shard_id: &str, dedup_key: &str) -> String {
    format!("stream_dedup:{shard_id}:{dedup_key}")
}

/// Prefix for deleting all stream deduplication markers belonging to `shard_id`.
pub fn stream_dedup_shard_prefix(shard_id: &str) -> String {
    format!("stream_dedup:{shard_id}:")
}

/// Prefix for scanning stream deduplication markers during retention cleanup.
pub const STREAM_DEDUP_SCAN_PREFIX: &str = "stream_dedup:";

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
