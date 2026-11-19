// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BigtableCatalogStore` and `BigtableCredentialStore`: implements
//! `CatalogStore` (`ManagementStore`, `AdminStore`, `PasswordHashStore`,
//! `SettingsStore`, `MetricsStore`, `RateLimitStore`, `AuthorizationStore`,
//! `DiagnosticsStore`) and `extenddb_auth::CredentialStore` on top of
//! `__extenddb_catalog__`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use extenddb_core::error::DynamoDbError;
use extenddb_storage::CatalogStore;
use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::diagnostics::{DiagError, DiagResult, DiagnosticsStore};
use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, AdminEntry, AdminStore, GroupDetail, GroupListEntry,
    ManagementStore, MetricsFilter, MetricsRow, MetricsStore, OpError, OpResult, PasswordHashStore,
    RateLimitStore, RoleDetail, RoleListEntry, SecretPage, SecretWrite, SessionPolicyEntry,
    SettingsStore, StoredSecretRow, UserDetail, UserListEntry,
};
use extenddb_storage::secret_keys::{SecretKeys, SecretRow, SecretTable, require_keys};
use futures::future::BoxFuture;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use serde_json::json;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use crate::catalog::{Catalog, CatalogJsonAction, keys};
use crate::data::admin::AdminClient;
use crate::data::client::BigtableClient;

const DEV_ADMIN_POLICY: &str =
    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;
const DEFAULT_ACCOUNT_SETTING: &str = "default_account_id";

fn now_rfc3339() -> String {
    let now = OffsetDateTime::now_utc();
    now.format(&Rfc3339).unwrap_or_else(|_| now.to_string())
}

fn parse_rfc3339(s: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(s, &Rfc3339).ok()
}

fn parse_created_at(v: &serde_json::Value) -> OffsetDateTime {
    v.get("created_at")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_rfc3339)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

/// Generate a 20-character access key ID (`AKIAEXTENDDB` + 8 uppercase
/// alphanumerics), matching the branded prefix convention across ExtendDB
/// backends so local keys are distinguishable from AWS IAM keys.
fn generate_access_key_id() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let suffix: String = (0..8)
        .map(|_| CHARSET[rand::Rng::random_range(&mut rng, 0..CHARSET.len())] as char)
        .collect();
    format!("AKIAEXTENDDB{suffix}")
}

/// Generate a 40-character secret access key (`extenddb` + 32 base64 characters).
fn generate_secret_access_key() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::rng();
    let suffix: String = (0..32)
        .map(|_| CHARSET[rand::Rng::random_range(&mut rng, 0..CHARSET.len())] as char)
        .collect();
    format!("extenddb{suffix}")
}

fn self_service_policy(user_arn: &str) -> serde_json::Value {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "iam:CreateAccessKey",
                "iam:DeleteAccessKey",
                "iam:ListAccessKeys",
                "iam:ChangePassword"
            ],
            "Resource": user_arn,
        }]
    })
}

fn policy_row_key(
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
    policy_name: &str,
) -> OpResult<String> {
    match principal_type {
        "user" => Ok(keys::user_policy(account_id, principal_name, policy_name)),
        "group" => Ok(keys::group_policy(account_id, principal_name, policy_name)),
        "role" => Ok(keys::role_policy(account_id, principal_name, policy_name)),
        other => Err(OpError::Validation(format!(
            "unsupported principal_type '{other}'"
        ))),
    }
}

fn policy_scan_prefix(
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
) -> OpResult<String> {
    match principal_type {
        "user" => Ok(keys::user_policy_scan_prefix(account_id, principal_name)),
        "group" => Ok(keys::group_policy_scan_prefix(account_id, principal_name)),
        "role" => Ok(keys::role_policy_scan_prefix(account_id, principal_name)),
        other => Err(OpError::Validation(format!(
            "unsupported principal_type '{other}'"
        ))),
    }
}

fn hash_opt_field(hasher: &mut Sha256, opt: Option<&str>) {
    match opt {
        Some(s) => {
            hasher.update([1u8]);
            hasher.update((s.len() as u64).to_be_bytes());
            hasher.update(s.as_bytes());
        }
        None => {
            hasher.update([0u8]);
        }
    }
}

fn metric_dims_hash(row: &MetricsRow) -> String {
    let mut hasher = Sha256::new();
    hash_opt_field(&mut hasher, row.account_id.as_deref());
    hash_opt_field(&mut hasher, Some(&row.metric));
    hash_opt_field(&mut hasher, row.table_name.as_deref());
    hash_opt_field(&mut hasher, row.index_name.as_deref());
    hash_opt_field(&mut hasher, row.operation.as_deref());
    format!("{:x}", hasher.finalize())
}

fn parse_tags_map(v: &serde_json::Value) -> Vec<(String, String)> {
    let Some(obj) = v.as_object() else {
        return Vec::new();
    };
    let mut out: Vec<(String, String)> = obj
        .iter()
        .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_owned())))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn parse_session_tags(v: &serde_json::Value) -> Vec<(String, String)> {
    if let Some(arr) = v.as_array() {
        arr.iter()
            .filter_map(|tag| {
                let k = tag.get("Key").and_then(serde_json::Value::as_str)?;
                let val = tag.get("Value").and_then(serde_json::Value::as_str)?;
                Some((k.to_owned(), val.to_owned()))
            })
            .collect()
    } else if let Some(obj) = v.as_object() {
        obj.iter()
            .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_owned())))
            .collect()
    } else {
        Vec::new()
    }
}

fn session_expires_at(v: &serde_json::Value) -> Option<(i64, OffsetDateTime)> {
    if let Some(secs) = v.get("expires_unix").and_then(serde_json::Value::as_i64)
        && let Ok(dt) = OffsetDateTime::from_unix_timestamp(secs)
    {
        return Some((secs, dt));
    }
    if let Some(s) = v.get("expires_at").and_then(serde_json::Value::as_str)
        && let Some(dt) = parse_rfc3339(s)
    {
        let secs = dt.unix_timestamp();
        let truncated = OffsetDateTime::from_unix_timestamp(secs).unwrap_or(dt);
        return Some((secs, truncated));
    }
    None
}

fn decode_stored_secret(v: &serde_json::Value) -> Option<Vec<u8>> {
    if let Some(b64) = v
        .get("secret_key_encrypted_b64")
        .and_then(serde_json::Value::as_str)
    {
        return BASE64.decode(b64).ok();
    }
    if let Some(b64) = v
        .get("secret_encrypted")
        .and_then(serde_json::Value::as_str)
    {
        return BASE64.decode(b64).ok();
    }
    None
}

pub struct BigtableCatalogStore {
    client: Arc<BigtableClient>,
    keys: Option<Arc<SecretKeys>>,
    dev_mode: bool,
}

impl BigtableCatalogStore {
    pub fn new(client: Arc<BigtableClient>, keys: Option<Arc<SecretKeys>>, dev_mode: bool) -> Self {
        Self {
            client,
            keys,
            dev_mode,
        }
    }

    fn cat(&self) -> Catalog<'_> {
        Catalog::new(&self.client)
    }

    fn user_arn(account_id: &str, user_name: &str) -> String {
        format!("arn:aws:iam::{account_id}:user/{user_name}")
    }

    fn group_arn(account_id: &str, group_name: &str) -> String {
        format!("arn:aws:iam::{account_id}:group/{group_name}")
    }

    fn role_arn(account_id: &str, role_name: &str) -> String {
        format!("arn:aws:iam::{account_id}:role/{role_name}")
    }

    async fn ensure_account_exists(&self, account_id: &str) -> OpResult<()> {
        if self
            .cat()
            .get(&keys::account(account_id))
            .await
            .map_err(OpError::Internal)?
            .is_none()
        {
            return Err(OpError::NotFound("Account not found".to_owned()));
        }
        Ok(())
    }

    async fn upsert_tags_row(&self, row_key: &str, tags: &[(String, String)]) -> OpResult<()> {
        let tags = tags.to_vec();
        self.cat()
            .mutate_json_cas(row_key, OpError::Internal, move |existing| {
                let cur = existing.unwrap_or_else(|| json!({}));
                let mut map: BTreeMap<String, String> = cur
                    .as_object()
                    .map(|o| {
                        o.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                            .collect()
                    })
                    .unwrap_or_default();
                for (k, v) in &tags {
                    map.insert(k.clone(), v.clone());
                }
                let next =
                    serde_json::to_value(map).map_err(|e| OpError::Internal(e.to_string()))?;
                Ok((CatalogJsonAction::Put(next), ()))
            })
            .await
    }

    async fn remove_tags_from_row(&self, row_key: &str, tag_keys: &[String]) -> OpResult<()> {
        let tag_keys = tag_keys.to_vec();
        self.cat()
            .mutate_json_cas(row_key, OpError::Internal, move |existing| {
                let Some(cur) = existing else {
                    return Ok((CatalogJsonAction::NoOp, ()));
                };
                let mut map: BTreeMap<String, String> = cur
                    .as_object()
                    .map(|o| {
                        o.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                            .collect()
                    })
                    .unwrap_or_default();
                for k in &tag_keys {
                    map.remove(k);
                }
                if map.is_empty() {
                    Ok((CatalogJsonAction::Delete, ()))
                } else {
                    let next =
                        serde_json::to_value(map).map_err(|e| OpError::Internal(e.to_string()))?;
                    Ok((CatalogJsonAction::Put(next), ()))
                }
            })
            .await
    }
}

// ── DiagnosticsStore ───────────────────────────────────────────────────

impl DiagnosticsStore for BigtableCatalogStore {
    fn count_tables(&self) -> BoxFuture<'_, DiagResult<i64>> {
        Box::pin(async move {
            let tables = self
                .cat()
                .scan_prefix(keys::TABLE_META_SCAN_PREFIX)
                .await
                .map_err(DiagError::QueryFailed)?;
            Ok(tables.len() as i64)
        })
    }

    fn count_indexes(&self) -> BoxFuture<'_, DiagResult<i64>> {
        Box::pin(async move {
            let tables = self
                .cat()
                .scan_prefix(keys::TABLE_META_SCAN_PREFIX)
                .await
                .map_err(DiagError::QueryFailed)?;
            let mut total = 0i64;
            for (_, v) in tables {
                let desc = v.get("description").unwrap_or(&serde_json::Value::Null);
                total += desc
                    .get("GlobalSecondaryIndexes")
                    .and_then(serde_json::Value::as_array)
                    .map_or(0, |a| a.len() as i64);
                total += desc
                    .get("LocalSecondaryIndexes")
                    .and_then(serde_json::Value::as_array)
                    .map_or(0, |a| a.len() as i64);
            }
            Ok(total)
        })
    }

    fn test_data_database_connection(
        &self,
        _key: &extenddb_storage::KeySource,
    ) -> BoxFuture<'_, DiagResult<String>> {
        Box::pin(async move {
            let mut admin = AdminClient::connect(&self.client)
                .await
                .map_err(DiagError::ConnectionFailed)?;
            admin.list_tables().await.map_err(DiagError::QueryFailed)?;
            Ok(self.client.instance_id.clone())
        })
    }
}

// ── SettingsStore ──────────────────────────────────────────────────────

impl SettingsStore for BigtableCatalogStore {
    fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let key = key.to_owned();
        Box::pin(async move {
            let cat = self.cat();
            let row = cat
                .get(&keys::setting(&key))
                .await
                .map_err(OpError::Internal)?;
            if let Some(val) = row.and_then(|v| {
                v.get("value")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            }) {
                return Ok(Some(val));
            }
            if key == "catalog_version" {
                let ver_row = cat.get(keys::VERSION).await.map_err(OpError::Internal)?;
                return Ok(ver_row.and_then(|v| {
                    v.get("catalog_version")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                }));
            }
            Ok(None)
        })
    }

    fn set_setting(&self, key: &str, value: &str) -> BoxFuture<'_, OpResult<()>> {
        let key = key.to_owned();
        let value = value.to_owned();
        Box::pin(async move {
            self.cat()
                .put(
                    &keys::setting(&key),
                    &json!({
                        "key": key,
                        "value": value,
                    }),
                )
                .await
                .map_err(OpError::Internal)
        })
    }

    fn replace_setting(
        &self,
        key: &str,
        expected: &str,
        value: &str,
    ) -> BoxFuture<'_, OpResult<bool>> {
        let key = key.to_owned();
        let expected = expected.to_owned();
        let value = value.to_owned();
        Box::pin(async move {
            let row_key = keys::setting(&key);
            self.cat()
                .mutate_json_cas(&row_key, OpError::Internal, move |existing| {
                    let Some(parsed) = existing else {
                        return Ok((CatalogJsonAction::NoOp, false));
                    };
                    if parsed.get("value").and_then(serde_json::Value::as_str)
                        != Some(expected.as_str())
                    {
                        return Ok((CatalogJsonAction::NoOp, false));
                    }
                    let next_doc = json!({
                        "key": key,
                        "value": value,
                    });
                    Ok((CatalogJsonAction::Put(next_doc), true))
                })
                .await
        })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async move {
            let cat = self.cat();
            let rows = cat
                .scan_prefix(keys::SETTING_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let mut out: Vec<(String, String)> = rows
                .into_iter()
                .filter_map(|(rk, v)| {
                    let k = v
                        .get("key")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| rk.strip_prefix(keys::SETTING_SCAN_PREFIX))?
                        .to_owned();
                    let val = v
                        .get("value")
                        .and_then(serde_json::Value::as_str)?
                        .to_owned();
                    Some((k, val))
                })
                .collect();
            if !out.iter().any(|(k, _)| k == "catalog_version")
                && let Some(ver_row) = cat.get(keys::VERSION).await.map_err(OpError::Internal)?
                && let Some(ver) = ver_row
                    .get("catalog_version")
                    .and_then(serde_json::Value::as_str)
            {
                out.push(("catalog_version".to_owned(), ver.to_owned()));
            }
            out.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(out)
        })
    }
}

// ── MetricsStore ───────────────────────────────────────────────────────

impl MetricsStore for BigtableCatalogStore {
    fn insert_metrics(&self, rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        let rows = rows.to_vec();
        Box::pin(async move {
            let cat = self.cat();
            for r in &rows {
                let bucket_secs = r.bucket.unix_timestamp();
                let dims = metric_dims_hash(r);
                let rk = keys::metric_row(bucket_secs, &dims);
                let r_clone = r.clone();
                cat.mutate_json_cas(&rk, OpError::Internal, move |existing| {
                    let (sum, count, min, max) = match existing {
                        Some(prev) => {
                            let p_sum = prev
                                .get("sum")
                                .and_then(serde_json::Value::as_f64)
                                .unwrap_or(0.0);
                            let p_cnt = prev
                                .get("count")
                                .and_then(serde_json::Value::as_i64)
                                .unwrap_or(0);
                            let p_min = prev
                                .get("min")
                                .and_then(serde_json::Value::as_f64)
                                .unwrap_or(r_clone.min);
                            let p_max = prev
                                .get("max")
                                .and_then(serde_json::Value::as_f64)
                                .unwrap_or(r_clone.max);
                            (
                                p_sum + r_clone.sum,
                                p_cnt + r_clone.count,
                                p_min.min(r_clone.min),
                                p_max.max(r_clone.max),
                            )
                        }
                        None => (r_clone.sum, r_clone.count, r_clone.min, r_clone.max),
                    };
                    let doc = json!({
                        "bucket_secs": bucket_secs,
                        "account_id": r_clone.account_id,
                        "metric": r_clone.metric,
                        "table_name": r_clone.table_name,
                        "index_name": r_clone.index_name,
                        "operation": r_clone.operation,
                        "sum": sum,
                        "count": count,
                        "min": min,
                        "max": max,
                    });
                    Ok((CatalogJsonAction::Put(doc), ()))
                })
                .await?;
            }
            Ok(())
        })
    }

    fn query_metrics(
        &self,
        start: OffsetDateTime,
        end: OffsetDateTime,
        filter: &MetricsFilter,
    ) -> BoxFuture<'_, OpResult<Vec<MetricsRow>>> {
        let filter = filter.clone();
        Box::pin(async move {
            let start_secs = start.unix_timestamp();
            let end_secs = end.unix_timestamp();
            if end_secs < start_secs {
                return Ok(Vec::new());
            }
            let start_key =
                StartKey::StartKeyClosed(keys::metric_bucket_prefix(start_secs).into_bytes());
            let mut end_bytes = keys::metric_bucket_prefix(end_secs).into_bytes();
            end_bytes.push(0xFF);
            let end_key = EndKey::EndKeyClosed(end_bytes);

            let raw = self
                .cat()
                .scan_range(start_key, end_key, 0)
                .await
                .map_err(OpError::Internal)?;

            let mut out = Vec::with_capacity(raw.len());
            for (_, v) in raw {
                let Some(b_secs) = v.get("bucket_secs").and_then(serde_json::Value::as_i64) else {
                    continue;
                };
                if b_secs < start_secs || b_secs > end_secs {
                    continue;
                }
                let Ok(bucket) = OffsetDateTime::from_unix_timestamp(b_secs) else {
                    continue;
                };
                let account_id = v
                    .get("account_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                let Some(metric) = v
                    .get("metric")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                else {
                    continue;
                };
                let table_name = v
                    .get("table_name")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                let index_name = v
                    .get("index_name")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                let operation = v
                    .get("operation")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);

                if let Some(want_acct) = filter.account_id()
                    && account_id.as_deref() != Some(want_acct)
                {
                    continue;
                }
                if let Some(want_table) = filter.table_name()
                    && table_name.as_deref() != Some(want_table)
                {
                    continue;
                }
                if let Some(want_metric) = filter.metric()
                    && metric != want_metric
                {
                    continue;
                }

                let sum = v
                    .get("sum")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let count = v
                    .get("count")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                let min = v
                    .get("min")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(sum);
                let max = v
                    .get("max")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(sum);

                out.push(MetricsRow {
                    bucket,
                    account_id,
                    metric,
                    table_name,
                    index_name,
                    operation,
                    sum,
                    count,
                    min,
                    max,
                });
            }
            Ok(out)
        })
    }

    fn prune_metrics(&self, retention: Duration) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async move {
            let retention_secs = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
            let cutoff_secs = OffsetDateTime::now_utc()
                .unix_timestamp()
                .saturating_sub(retention_secs);
            let stale = self
                .cat()
                .scan_range(
                    StartKey::StartKeyClosed(keys::METRIC_SCAN_PREFIX.as_bytes().to_vec()),
                    EndKey::EndKeyOpen(keys::metric_bucket_prefix(cutoff_secs).into_bytes()),
                    0,
                )
                .await
                .map_err(OpError::Internal)?;
            let to_delete: Vec<String> = stale
                .into_iter()
                .filter(|(_, v)| {
                    v.get("bucket_secs")
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|b| b < cutoff_secs)
                })
                .map(|(k, _)| k)
                .collect();
            self.cat()
                .delete_keys(to_delete.iter().map(String::as_str))
                .await
                .map_err(OpError::Internal)?;
            Ok(())
        })
    }
}

// ── RateLimitStore ─────────────────────────────────────────────────────

impl RateLimitStore for BigtableCatalogStore {
    fn count_principal_failures(
        &self,
        principal: &str,
        source_ip: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let principal = principal.to_owned();
        let source_ip = source_ip.to_owned();
        Box::pin(async move {
            let cutoff = OffsetDateTime::now_utc()
                .unix_timestamp()
                .saturating_sub(window_seconds);
            let rows = self
                .cat()
                .scan_prefix(&keys::failed_login_principal_prefix(&principal))
                .await
                .map_err(OpError::Internal)?;
            let count = rows
                .into_iter()
                .filter(|(_, v)| {
                    v.get("source_ip").and_then(serde_json::Value::as_str) == Some(&source_ip)
                        && v.get("timestamp_secs")
                            .and_then(serde_json::Value::as_i64)
                            .is_some_and(|ts| ts >= cutoff)
                })
                .count();
            Ok(count as i64)
        })
    }

    fn count_ip_failures(
        &self,
        source_ip: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let source_ip = source_ip.to_owned();
        Box::pin(async move {
            let cutoff = OffsetDateTime::now_utc()
                .unix_timestamp()
                .saturating_sub(window_seconds);
            let rows = self
                .cat()
                .scan_prefix(keys::FAILED_LOGIN_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let count = rows
                .into_iter()
                .filter(|(_, v)| {
                    v.get("source_ip").and_then(serde_json::Value::as_str) == Some(&source_ip)
                        && v.get("timestamp_secs")
                            .and_then(serde_json::Value::as_i64)
                            .is_some_and(|ts| ts >= cutoff)
                })
                .count();
            Ok(count as i64)
        })
    }

    fn record_failed_login(&self, principal: &str, source_ip: Option<&str>) -> BoxFuture<'_, ()> {
        let principal = principal.to_owned();
        let source_ip = source_ip.map(str::to_owned);
        Box::pin(async move {
            let now = OffsetDateTime::now_utc();
            let rk = keys::failed_login(
                &principal,
                now.unix_timestamp_nanos(),
                &Uuid::new_v4().simple().to_string(),
            );
            let _ = self
                .cat()
                .put(
                    &rk,
                    &json!({
                        "principal": principal,
                        "source_ip": source_ip,
                        "timestamp_secs": now.unix_timestamp(),
                    }),
                )
                .await;
        })
    }

    fn cleanup_old_attempts(&self, max_age_seconds: i64) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let cutoff = OffsetDateTime::now_utc()
                .unix_timestamp()
                .saturating_sub(max_age_seconds);
            let Ok(rows) = self.cat().scan_prefix(keys::FAILED_LOGIN_SCAN_PREFIX).await else {
                return;
            };
            let old: Vec<String> = rows
                .into_iter()
                .filter(|(_, v)| {
                    v.get("timestamp_secs")
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|ts| ts < cutoff)
                })
                .map(|(k, _)| k)
                .collect();
            let _ = self.cat().delete_keys(old.iter().map(String::as_str)).await;
        })
    }
}

// ── AdminStore & PasswordHashStore ─────────────────────────────────────

impl AdminStore for BigtableCatalogStore {
    fn create_admin(&self, admin_name: &str, password_hash: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let password_hash = password_hash.to_owned();
        Box::pin(async move {
            let inserted = self
                .cat()
                .put_if_absent(
                    &keys::admin(&admin_name),
                    &json!({
                        "username": admin_name,
                        "password_hash": password_hash,
                        "from_env": false,
                        "created_at": now_rfc3339(),
                    }),
                )
                .await
                .map_err(OpError::Internal)?;
            if !inserted {
                return Err(OpError::AlreadyExists(
                    "Admin user already exists".to_owned(),
                ));
            }
            Ok(())
        })
    }

    fn list_admins(&self) -> BoxFuture<'_, OpResult<Vec<AdminEntry>>> {
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix("admin:")
                .await
                .map_err(OpError::Internal)?;
            let mut entries: Vec<AdminEntry> = rows
                .into_iter()
                .filter_map(|(rk, v)| {
                    let name = v
                        .get("username")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| rk.strip_prefix("admin:"))?
                        .to_owned();
                    Some(AdminEntry {
                        admin_name: name,
                        created_at: parse_created_at(&v),
                    })
                })
                .collect();
            entries.sort_by(|a, b| a.admin_name.cmp(&b.admin_name));
            Ok(entries)
        })
    }

    fn delete_admin(&self, admin_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        Box::pin(async move {
            let deleted = self
                .cat()
                .delete_if_present(&keys::admin(&admin_name))
                .await
                .map_err(OpError::Internal)?;
            if !deleted {
                return Err(OpError::NotFound("Admin user not found".to_owned()));
            }
            Ok(())
        })
    }

    fn change_admin_password(
        &self,
        admin_name: &str,
        password_hash: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let password_hash = password_hash.to_owned();
        Box::pin(async move {
            let rk = keys::admin(&admin_name);
            self.cat()
                .mutate_json_cas(&rk, OpError::Internal, move |existing| {
                    let mut doc = existing
                        .ok_or_else(|| OpError::NotFound("Admin user not found".to_owned()))?;
                    if let Some(obj) = doc.as_object_mut() {
                        obj.insert(
                            "password_hash".to_owned(),
                            serde_json::Value::String(password_hash.clone()),
                        );
                    }
                    Ok((CatalogJsonAction::Put(doc), ()))
                })
                .await
        })
    }

    fn verify_admin_password(
        &self,
        admin_name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<Option<bool>>> {
        let admin_name = admin_name.to_owned();
        let password = password.to_owned();
        Box::pin(async move {
            let Some(hash) = self.get_admin_password_hash(&admin_name).await? else {
                return Ok(None);
            };
            let ok = extenddb_storage::password::verify_password(password, hash).await?;
            Ok(Some(ok))
        })
    }
}

impl PasswordHashStore for BigtableCatalogStore {
    fn get_admin_password_hash(&self, admin_name: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let admin_name = admin_name.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::admin(&admin_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(row.and_then(|v| {
                v.get("password_hash")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            }))
        })
    }

    fn get_user_password_hash(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::user(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(row.and_then(|v| {
                v.get("password_hash")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            }))
        })
    }
}

// ── ManagementStore ────────────────────────────────────────────────────

impl ManagementStore for BigtableCatalogStore {
    fn create_account(&self, account_id: &str, account_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let account_name = account_name.to_owned();
        Box::pin(async move {
            let inserted = self
                .cat()
                .put_if_absent(
                    &keys::account(&account_id),
                    &json!({
                        "account_id": account_id,
                        "account_name": account_name,
                        "created_at": now_rfc3339(),
                    }),
                )
                .await
                .map_err(OpError::Internal)?;
            if !inserted {
                return Err(OpError::AlreadyExists("Account already exists".to_owned()));
            }
            Ok(())
        })
    }

    fn delete_account(&self, account_id: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            self.ensure_account_exists(&account_id).await?;
            let cat = self.cat();
            let tables = cat
                .scan_prefix(&keys::table_meta_scan_prefix(&account_id))
                .await
                .map_err(OpError::Internal)?;
            if !tables.is_empty() {
                return Err(OpError::HasDependents(
                    "Cannot delete account with existing tables. Delete all tables first."
                        .to_owned(),
                ));
            }

            for prefix in [
                keys::user_scan_prefix(&account_id),
                keys::group_scan_prefix(&account_id),
                keys::group_member_account_prefix(&account_id),
                keys::role_scan_prefix(&account_id),
                keys::user_policy_account_prefix(&account_id),
                keys::group_policy_account_prefix(&account_id),
                keys::role_policy_account_prefix(&account_id),
                keys::boundary_account_prefix(&account_id),
                keys::user_tags_account_prefix(&account_id),
                keys::role_tags_account_prefix(&account_id),
                keys::failed_login_account_prefix(&account_id),
            ] {
                cat.delete_prefix(&prefix)
                    .await
                    .map_err(OpError::Internal)?;
            }

            let all_aks = cat
                .scan_prefix(keys::ACCESS_KEY_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let acct_ak_keys: Vec<String> = all_aks
                .into_iter()
                .filter(|(_, v)| {
                    v.get("account_id").and_then(serde_json::Value::as_str) == Some(&account_id)
                })
                .map(|(k, _)| k)
                .collect();
            cat.delete_keys(acct_ak_keys.iter().map(String::as_str))
                .await
                .map_err(OpError::Internal)?;

            let all_sessions = cat
                .scan_prefix(keys::IAM_SESSION_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let mut session_keys_to_delete: Vec<String> = Vec::new();
            for (rk, v) in all_sessions {
                if v.get("account_id").and_then(serde_json::Value::as_str) == Some(&account_id) {
                    session_keys_to_delete.push(rk);
                    if let Some(akid) = v.get("access_key_id").and_then(serde_json::Value::as_str) {
                        session_keys_to_delete.push(keys::iam_session_access_key(akid));
                    }
                }
            }
            cat.delete_keys(session_keys_to_delete.iter().map(String::as_str))
                .await
                .map_err(OpError::Internal)?;

            cat.delete(&keys::account(&account_id))
                .await
                .map_err(OpError::Internal)?;
            Ok(())
        })
    }

    fn list_all_accounts(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async move {
            let full = self.list_all_accounts_full().await?;
            Ok(full.into_iter().map(|(id, name, _)| (id, name)).collect())
        })
    }

    fn default_account_id(&self) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.get_setting(DEFAULT_ACCOUNT_SETTING)
    }

    fn list_all_accounts_full(
        &self,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String, OffsetDateTime)>>> {
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(keys::ACCOUNT_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let mut out: Vec<(String, String, OffsetDateTime)> = rows
                .into_iter()
                .filter_map(|(rk, v)| {
                    let id = v
                        .get("account_id")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| rk.strip_prefix(keys::ACCOUNT_SCAN_PREFIX))?
                        .to_owned();
                    let name = v
                        .get("account_name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("default")
                        .to_owned();
                    Some((id, name, parse_created_at(&v)))
                })
                .collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(out)
        })
    }

    fn list_accounts_for(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::account(&account_id))
                .await
                .map_err(OpError::Internal)?;
            let Some(v) = row else {
                return Ok(Vec::new());
            };
            let name = v
                .get("account_name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("default")
                .to_owned();
            Ok(vec![(account_id, name)])
        })
    }

    fn get_account_detail(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Option<AccountDetail>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let Some(acct) = self
                .cat()
                .get(&keys::account(&account_id))
                .await
                .map_err(OpError::Internal)?
            else {
                return Ok(None);
            };
            let account_name = acct
                .get("account_name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("default")
                .to_owned();
            let users: Vec<String> = self
                .list_users(&account_id)
                .await?
                .into_iter()
                .map(|(_, u, _, _, _)| u)
                .collect();
            let groups: Vec<String> = self
                .list_groups(&account_id)
                .await?
                .into_iter()
                .map(|(_, g, _, _)| g)
                .collect();
            let roles: Vec<String> = self
                .list_roles(&account_id)
                .await?
                .into_iter()
                .map(|(_, r, _, _, _)| r)
                .collect();
            Ok(Some(AccountDetail {
                account_name,
                users,
                groups,
                roles,
            }))
        })
    }

    fn dashboard_counts(&self) -> BoxFuture<'_, OpResult<(i64, i64)>> {
        Box::pin(async move {
            let accts = self
                .cat()
                .scan_prefix(keys::ACCOUNT_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let admins = self
                .cat()
                .scan_prefix("admin:")
                .await
                .map_err(OpError::Internal)?;
            Ok((accts.len() as i64, admins.len() as i64))
        })
    }

    // ── Users ──────────────────────────────────────────────────────

    fn create_user(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: Option<&str>,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password_hash = password_hash.map(str::to_owned);
        Box::pin(async move {
            self.ensure_account_exists(&account_id).await?;
            let cat = self.cat();
            let user_arn = Self::user_arn(&account_id, &user_name);
            let now = now_rfc3339();
            let inserted = cat
                .put_if_absent(
                    &keys::user(&account_id, &user_name),
                    &json!({
                        "account_id": account_id,
                        "user_name": user_name,
                        "user_arn": user_arn,
                        "password_hash": password_hash,
                        "created_at": now,
                    }),
                )
                .await
                .map_err(OpError::Internal)?;
            if !inserted {
                return Err(OpError::AlreadyExists("IAM user already exists".to_owned()));
            }

            // Clear any leftover attachments or failed-login history under this name.
            cat.delete_prefix(&keys::user_policy_scan_prefix(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete(&keys::boundary(&account_id, "user", &user_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete(&keys::user_tags(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete_prefix(&keys::failed_login_principal_prefix(&format!(
                "{account_id}/{user_name}"
            )))
            .await
            .map_err(OpError::Internal)?;

            let self_policy = self_service_policy(&user_arn);
            cat.put(
                &keys::user_policy(&account_id, &user_name, "SelfServicePolicy"),
                &json!({
                    "account_id": account_id,
                    "principal_type": "user",
                    "principal_name": user_name,
                    "policy_name": "SelfServicePolicy",
                    "document": self_policy,
                    "created_at": now,
                }),
            )
            .await
            .map_err(OpError::Internal)?;
            Ok(())
        })
    }

    fn delete_user(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let cat = self.cat();
            let deleted = cat
                .delete_if_present(&keys::user(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?;
            if !deleted {
                return Err(OpError::NotFound("IAM user not found".to_owned()));
            }

            cat.delete_prefix(&keys::user_policy_scan_prefix(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete(&keys::boundary(&account_id, "user", &user_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete(&keys::user_tags(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete_prefix(&keys::failed_login_principal_prefix(&format!(
                "{account_id}/{user_name}"
            )))
            .await
            .map_err(OpError::Internal)?;

            let memberships = cat
                .scan_prefix(&keys::group_member_account_prefix(&account_id))
                .await
                .map_err(OpError::Internal)?;
            let member_keys: Vec<String> = memberships
                .into_iter()
                .filter(|(_, v)| {
                    v.get("user_name").and_then(serde_json::Value::as_str) == Some(&user_name)
                })
                .map(|(k, _)| k)
                .collect();
            cat.delete_keys(member_keys.iter().map(String::as_str))
                .await
                .map_err(OpError::Internal)?;

            let aks = cat
                .scan_prefix(keys::ACCESS_KEY_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let ak_keys: Vec<String> = aks
                .into_iter()
                .filter(|(_, v)| {
                    v.get("account_id").and_then(serde_json::Value::as_str) == Some(&account_id)
                        && v.get("user_name").and_then(serde_json::Value::as_str)
                            == Some(&user_name)
                })
                .map(|(k, _)| k)
                .collect();
            cat.delete_keys(ak_keys.iter().map(String::as_str))
                .await
                .map_err(OpError::Internal)?;

            Ok(())
        })
    }

    fn list_users(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<UserListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(&keys::user_scan_prefix(&account_id))
                .await
                .map_err(OpError::Internal)?;
            let mut out: Vec<UserListEntry> = rows
                .into_iter()
                .filter_map(|(rk, v)| {
                    let uname = v
                        .get("user_name")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| rk.rsplit_once(':').map(|(_, n)| n))?
                        .to_owned();
                    let uarn = v
                        .get("user_arn")
                        .and_then(serde_json::Value::as_str)
                        .map_or_else(|| Self::user_arn(&account_id, &uname), str::to_owned);
                    let has_pw = v
                        .get("password_hash")
                        .and_then(serde_json::Value::as_str)
                        .is_some();
                    Some((
                        account_id.clone(),
                        uname,
                        uarn,
                        has_pw,
                        parse_created_at(&v),
                    ))
                })
                .collect();
            out.sort_by(|a, b| a.1.cmp(&b.1));
            Ok(out)
        })
    }

    fn get_user_detail(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<UserDetail>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            if self
                .cat()
                .get(&keys::user(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?
                .is_none()
            {
                return Ok(None);
            }
            let keys_list: Vec<(String, bool)> = self
                .list_access_keys(&account_id, &user_name)
                .await?
                .into_iter()
                .map(|(k, active, _)| (k, active))
                .collect();
            let policies: Vec<String> = self
                .list_policies(&account_id, "user", &user_name)
                .await?
                .into_iter()
                .map(|(n, _, _)| n)
                .collect();
            let tags = self.list_user_tags(&account_id, &user_name).await?;
            let memberships = self
                .cat()
                .scan_prefix(&keys::group_member_account_prefix(&account_id))
                .await
                .map_err(OpError::Internal)?;
            let mut groups: Vec<String> = memberships
                .into_iter()
                .filter_map(|(_, v)| {
                    if v.get("user_name").and_then(serde_json::Value::as_str) == Some(&user_name) {
                        v.get("group_name")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                    } else {
                        None
                    }
                })
                .collect();
            groups.sort();
            Ok(Some(UserDetail {
                keys: keys_list,
                policies,
                tags,
                groups,
            }))
        })
    }

    fn verify_iam_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<bool>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password = password.to_owned();
        Box::pin(async move {
            let stored_hash = self.get_user_password_hash(&account_id, &user_name).await?;
            extenddb_storage::password::verify_stored_password(password, stored_hash).await
        })
    }

    fn change_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password_hash = password_hash.to_owned();
        Box::pin(async move {
            let rk = keys::user(&account_id, &user_name);
            self.cat()
                .mutate_json_cas(&rk, OpError::Internal, move |existing| {
                    let mut doc = existing
                        .ok_or_else(|| OpError::NotFound("IAM user not found".to_owned()))?;
                    if let Some(obj) = doc.as_object_mut() {
                        obj.insert(
                            "password_hash".to_owned(),
                            serde_json::Value::String(password_hash.clone()),
                        );
                    }
                    Ok((CatalogJsonAction::Put(doc), ()))
                })
                .await
        })
    }

    fn tag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let tags = tags.to_vec();
        Box::pin(async move {
            if self
                .cat()
                .get(&keys::user(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?
                .is_none()
            {
                return Err(OpError::NotFound("IAM user not found".to_owned()));
            }
            self.upsert_tags_row(&keys::user_tags(&account_id, &user_name), &tags)
                .await
        })
    }

    fn untag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let tag_keys = tag_keys.to_vec();
        Box::pin(async move {
            if self
                .cat()
                .get(&keys::user(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?
                .is_none()
            {
                return Err(OpError::NotFound("IAM user not found".to_owned()));
            }
            self.remove_tags_from_row(&keys::user_tags(&account_id, &user_name), &tag_keys)
                .await
        })
    }

    fn list_user_tags(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let Some(v) = self
                .cat()
                .get(&keys::user_tags(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?
            else {
                return Ok(Vec::new());
            };
            Ok(parse_tags_map(&v))
        })
    }

    // ── Groups ─────────────────────────────────────────────────────

    fn create_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move {
            self.ensure_account_exists(&account_id).await?;
            let cat = self.cat();
            let group_arn = Self::group_arn(&account_id, &group_name);
            let inserted = cat
                .put_if_absent(
                    &keys::group(&account_id, &group_name),
                    &json!({
                        "account_id": account_id,
                        "group_name": group_name,
                        "group_arn": group_arn,
                        "created_at": now_rfc3339(),
                    }),
                )
                .await
                .map_err(OpError::Internal)?;
            if !inserted {
                return Err(OpError::AlreadyExists(
                    "IAM group already exists".to_owned(),
                ));
            }
            cat.delete_prefix(&keys::group_policy_scan_prefix(&account_id, &group_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete_prefix(&keys::group_member_scan_prefix(&account_id, &group_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(())
        })
    }

    fn delete_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move {
            let cat = self.cat();
            let deleted = cat
                .delete_if_present(&keys::group(&account_id, &group_name))
                .await
                .map_err(OpError::Internal)?;
            if !deleted {
                return Err(OpError::NotFound("IAM group not found".to_owned()));
            }
            cat.delete_prefix(&keys::group_policy_scan_prefix(&account_id, &group_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete_prefix(&keys::group_member_scan_prefix(&account_id, &group_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(())
        })
    }

    fn list_groups(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<GroupListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(&keys::group_scan_prefix(&account_id))
                .await
                .map_err(OpError::Internal)?;
            let mut out: Vec<GroupListEntry> = rows
                .into_iter()
                .filter_map(|(rk, v)| {
                    let gname = v
                        .get("group_name")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| rk.rsplit_once(':').map(|(_, n)| n))?
                        .to_owned();
                    let garn = v
                        .get("group_arn")
                        .and_then(serde_json::Value::as_str)
                        .map_or_else(|| Self::group_arn(&account_id, &gname), str::to_owned);
                    Some((account_id.clone(), gname, garn, parse_created_at(&v)))
                })
                .collect();
            out.sort_by(|a, b| a.1.cmp(&b.1));
            Ok(out)
        })
    }

    fn get_group_detail(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<GroupDetail>>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move {
            if self
                .cat()
                .get(&keys::group(&account_id, &group_name))
                .await
                .map_err(OpError::Internal)?
                .is_none()
            {
                return Ok(None);
            }
            let member_rows = self
                .cat()
                .scan_prefix(&keys::group_member_scan_prefix(&account_id, &group_name))
                .await
                .map_err(OpError::Internal)?;
            let mut members: Vec<String> = member_rows
                .into_iter()
                .filter_map(|(rk, v)| {
                    v.get("user_name")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| rk.rsplit_once(':').map(|(_, u)| u))
                        .map(str::to_owned)
                })
                .collect();
            members.sort();

            let policies: Vec<String> = self
                .list_policies(&account_id, "group", &group_name)
                .await?
                .into_iter()
                .map(|(n, _, _)| n)
                .collect();
            let all_users: Vec<String> = self
                .list_users(&account_id)
                .await?
                .into_iter()
                .map(|(_, u, _, _, _)| u)
                .collect();
            Ok(Some(GroupDetail {
                members,
                policies,
                all_users,
            }))
        })
    }

    fn add_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let cat = self.cat();
            if cat
                .get(&keys::group(&account_id, &group_name))
                .await
                .map_err(OpError::Internal)?
                .is_none()
                || cat
                    .get(&keys::user(&account_id, &user_name))
                    .await
                    .map_err(OpError::Internal)?
                    .is_none()
            {
                return Err(OpError::NotFound("Group or user not found".to_owned()));
            }
            let inserted = cat
                .put_if_absent(
                    &keys::group_member(&account_id, &group_name, &user_name),
                    &json!({
                        "account_id": account_id,
                        "group_name": group_name,
                        "user_name": user_name,
                    }),
                )
                .await
                .map_err(OpError::Internal)?;
            if !inserted {
                return Err(OpError::AlreadyExists(
                    "User is already a member of this group".to_owned(),
                ));
            }
            Ok(())
        })
    }

    fn remove_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let deleted = self
                .cat()
                .delete_if_present(&keys::group_member(&account_id, &group_name, &user_name))
                .await
                .map_err(OpError::Internal)?;
            if !deleted {
                return Err(OpError::NotFound("Membership not found".to_owned()));
            }
            Ok(())
        })
    }

    // ── Roles ──────────────────────────────────────────────────────

    fn create_role(
        &self,
        account_id: &str,
        role_name: &str,
        trust_policy: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let trust_policy = trust_policy.clone();
        Box::pin(async move {
            self.ensure_account_exists(&account_id).await?;
            let cat = self.cat();
            let role_arn = Self::role_arn(&account_id, &role_name);
            let inserted = cat
                .put_if_absent(
                    &keys::role(&account_id, &role_name),
                    &json!({
                        "account_id": account_id,
                        "role_name": role_name,
                        "role_arn": role_arn,
                        "trust_policy": trust_policy,
                        "created_at": now_rfc3339(),
                    }),
                )
                .await
                .map_err(OpError::Internal)?;
            if !inserted {
                return Err(OpError::AlreadyExists("IAM role already exists".to_owned()));
            }
            cat.delete_prefix(&keys::role_policy_scan_prefix(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete(&keys::boundary(&account_id, "role", &role_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete(&keys::role_tags(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(())
        })
    }

    fn delete_role(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let cat = self.cat();
            let deleted = cat
                .delete_if_present(&keys::role(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?;
            if !deleted {
                return Err(OpError::NotFound("IAM role not found".to_owned()));
            }
            cat.delete_prefix(&keys::role_policy_scan_prefix(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete(&keys::boundary(&account_id, "role", &role_name))
                .await
                .map_err(OpError::Internal)?;
            cat.delete(&keys::role_tags(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?;

            let sessions = cat
                .scan_prefix(keys::IAM_SESSION_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let mut to_del = Vec::new();
            for (rk, v) in sessions {
                if v.get("account_id").and_then(serde_json::Value::as_str) == Some(&account_id)
                    && v.get("role_name").and_then(serde_json::Value::as_str) == Some(&role_name)
                {
                    to_del.push(rk);
                    if let Some(akid) = v.get("access_key_id").and_then(serde_json::Value::as_str) {
                        to_del.push(keys::iam_session_access_key(akid));
                    }
                }
            }
            cat.delete_keys(to_del.iter().map(String::as_str))
                .await
                .map_err(OpError::Internal)?;
            Ok(())
        })
    }

    fn list_roles(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<RoleListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(&keys::role_scan_prefix(&account_id))
                .await
                .map_err(OpError::Internal)?;
            let mut out: Vec<RoleListEntry> = rows
                .into_iter()
                .filter_map(|(rk, v)| {
                    let rname = v
                        .get("role_name")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| rk.rsplit_once(':').map(|(_, n)| n))?
                        .to_owned();
                    let rarn = v
                        .get("role_arn")
                        .and_then(serde_json::Value::as_str)
                        .map_or_else(|| Self::role_arn(&account_id, &rname), str::to_owned);
                    let tp = v.get("trust_policy").cloned().unwrap_or_else(|| json!({}));
                    Some((account_id.clone(), rname, rarn, tp, parse_created_at(&v)))
                })
                .collect();
            out.sort_by(|a, b| a.1.cmp(&b.1));
            Ok(out)
        })
    }

    fn get_role_detail(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<RoleDetail>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let Some(v) = self
                .cat()
                .get(&keys::role(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?
            else {
                return Ok(None);
            };
            let trust_policy = v.get("trust_policy").cloned().unwrap_or_else(|| json!({}));
            let policies: Vec<String> = self
                .list_policies(&account_id, "role", &role_name)
                .await?
                .into_iter()
                .map(|(n, _, _)| n)
                .collect();
            let tags = self.list_role_tags(&account_id, &role_name).await?;
            Ok(Some(RoleDetail {
                trust_policy,
                policies,
                tags,
            }))
        })
    }

    fn get_role_trust_policy(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let Some(v) = self
                .cat()
                .get(&keys::role(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?
            else {
                return Ok(None);
            };
            Ok(v.get("trust_policy").cloned())
        })
    }

    fn tag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let tags = tags.to_vec();
        Box::pin(async move {
            if self
                .cat()
                .get(&keys::role(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?
                .is_none()
            {
                return Err(OpError::NotFound("IAM role not found".to_owned()));
            }
            self.upsert_tags_row(&keys::role_tags(&account_id, &role_name), &tags)
                .await
        })
    }

    fn untag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let tag_keys = tag_keys.to_vec();
        Box::pin(async move {
            if self
                .cat()
                .get(&keys::role(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?
                .is_none()
            {
                return Err(OpError::NotFound("IAM role not found".to_owned()));
            }
            self.remove_tags_from_row(&keys::role_tags(&account_id, &role_name), &tag_keys)
                .await
        })
    }

    fn list_role_tags(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let Some(v) = self
                .cat()
                .get(&keys::role_tags(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?
            else {
                return Ok(Vec::new());
            };
            Ok(parse_tags_map(&v))
        })
    }

    // ── Policies & Permissions Boundaries ──────────────────────────

    fn put_policy(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let principal_type = principal_type.to_owned();
        let principal_name = principal_name.to_owned();
        let policy_name = policy_name.to_owned();
        let document = document.clone();
        Box::pin(async move {
            let rk = policy_row_key(&account_id, &principal_type, &principal_name, &policy_name)?;
            self.cat()
                .mutate_json_cas(&rk, OpError::Internal, move |existing| {
                    let created_at = existing
                        .and_then(|v| {
                            v.get("created_at")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_owned)
                        })
                        .unwrap_or_else(now_rfc3339);
                    let doc = json!({
                        "account_id": account_id,
                        "principal_type": principal_type,
                        "principal_name": principal_name,
                        "policy_name": policy_name,
                        "document": document,
                        "created_at": created_at,
                    });
                    Ok((CatalogJsonAction::Put(doc), ()))
                })
                .await
        })
    }

    fn delete_policy(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let principal_type = principal_type.to_owned();
        let principal_name = principal_name.to_owned();
        let policy_name = policy_name.to_owned();
        Box::pin(async move {
            let rk = policy_row_key(&account_id, &principal_type, &principal_name, &policy_name)?;
            let deleted = self
                .cat()
                .delete_if_present(&rk)
                .await
                .map_err(OpError::Internal)?;
            if !deleted {
                return Err(OpError::NotFound("Policy not found".to_owned()));
            }
            Ok(())
        })
    }

    fn list_policies(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, serde_json::Value, OffsetDateTime)>>> {
        let account_id = account_id.to_owned();
        let principal_type = principal_type.to_owned();
        let principal_name = principal_name.to_owned();
        Box::pin(async move {
            let prefix = policy_scan_prefix(&account_id, &principal_type, &principal_name)?;
            let rows = self
                .cat()
                .scan_prefix(&prefix)
                .await
                .map_err(OpError::Internal)?;
            let mut out: Vec<(String, serde_json::Value, OffsetDateTime)> = rows
                .into_iter()
                .filter_map(|(rk, v)| {
                    let pname = v
                        .get("policy_name")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| rk.rsplit_once(':').map(|(_, n)| n))?
                        .to_owned();
                    let doc = v.get("document")?.clone();
                    Some((pname, doc, parse_created_at(&v)))
                })
                .collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(out)
        })
    }

    fn set_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let document = document.clone();
        Box::pin(async move {
            self.cat()
                .put(
                    &keys::boundary(&account_id, "user", &user_name),
                    &json!({
                        "account_id": account_id,
                        "principal_type": "user",
                        "principal_name": user_name,
                        "document": document,
                    }),
                )
                .await
                .map_err(OpError::Internal)
        })
    }

    fn get_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::boundary(&account_id, "user", &user_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(row.and_then(|v| v.get("document").cloned()))
        })
    }

    fn delete_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            self.cat()
                .delete(&keys::boundary(&account_id, "user", &user_name))
                .await
                .map_err(OpError::Internal)
        })
    }

    fn set_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let document = document.clone();
        Box::pin(async move {
            self.cat()
                .put(
                    &keys::boundary(&account_id, "role", &role_name),
                    &json!({
                        "account_id": account_id,
                        "principal_type": "role",
                        "principal_name": role_name,
                        "document": document,
                    }),
                )
                .await
                .map_err(OpError::Internal)
        })
    }

    fn get_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::boundary(&account_id, "role", &role_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(row.and_then(|v| v.get("document").cloned()))
        })
    }

    fn delete_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            self.cat()
                .delete(&keys::boundary(&account_id, "role", &role_name))
                .await
                .map_err(OpError::Internal)
        })
    }

    // ── Access Keys & Sessions ─────────────────────────────────────

    fn create_access_key(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<AccessKeyCreated>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let keys_holder = require_keys(self.keys.as_deref())?;
            if self
                .cat()
                .get(&keys::user(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?
                .is_none()
            {
                return Err(OpError::NotFound("IAM user not found".to_owned()));
            }
            let secret_access_key = generate_secret_access_key();
            let now = now_rfc3339();
            for _ in 0..8 {
                let access_key_id = generate_access_key_id();
                let aad = SecretRow::access_key(&access_key_id, &account_id, &user_name);
                let sealed = keys_holder
                    .encrypt(&aad, &secret_access_key)
                    .map_err(OpError::Internal)?;
                let inserted = self
                    .cat()
                    .put_if_absent(
                        &keys::access_key(&access_key_id),
                        &json!({
                            "access_key_id": access_key_id,
                            "account_id": account_id,
                            "user_name": user_name,
                            "secret_key_encrypted_b64": BASE64.encode(&sealed),
                            "is_active": true,
                            "created_at": now,
                        }),
                    )
                    .await
                    .map_err(OpError::Internal)?;
                if inserted {
                    return Ok(AccessKeyCreated {
                        access_key_id,
                        secret_access_key,
                    });
                }
            }
            Err(OpError::Internal(
                "failed to allocate unique access_key_id".to_owned(),
            ))
        })
    }

    fn delete_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        key_id: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let key_id = key_id.to_owned();
        Box::pin(async move {
            let rk = keys::access_key(&key_id);
            let Some(v) = self.cat().get(&rk).await.map_err(OpError::Internal)? else {
                return Err(OpError::NotFound("Access key not found".to_owned()));
            };
            if v.get("account_id").and_then(serde_json::Value::as_str) != Some(&account_id)
                || v.get("user_name").and_then(serde_json::Value::as_str) != Some(&user_name)
            {
                return Err(OpError::NotFound("Access key not found".to_owned()));
            }
            self.cat().delete(&rk).await.map_err(OpError::Internal)
        })
    }

    fn list_access_keys(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, bool, OffsetDateTime)>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(keys::ACCESS_KEY_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let mut out: Vec<(String, bool, OffsetDateTime)> = rows
                .into_iter()
                .filter_map(|(rk, v)| {
                    if v.get("account_id").and_then(serde_json::Value::as_str) != Some(&account_id)
                        || v.get("user_name").and_then(serde_json::Value::as_str)
                            != Some(&user_name)
                    {
                        return None;
                    }
                    let kid = v
                        .get("access_key_id")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| rk.strip_prefix(keys::ACCESS_KEY_SCAN_PREFIX))?
                        .to_owned();
                    let active = v
                        .get("is_active")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(true);
                    Some((kid, active, parse_created_at(&v)))
                })
                .collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(out)
        })
    }

    fn import_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let access_key_id = access_key_id.to_owned();
        let secret_access_key = secret_access_key.to_owned();
        Box::pin(async move {
            let keys_holder = require_keys(self.keys.as_deref())?;
            if self
                .cat()
                .get(&keys::user(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?
                .is_none()
            {
                return Err(OpError::NotFound("IAM user not found".to_owned()));
            }
            let aad = SecretRow::access_key(&access_key_id, &account_id, &user_name);
            let sealed = keys_holder
                .encrypt(&aad, &secret_access_key)
                .map_err(OpError::Internal)?;
            let inserted = self
                .cat()
                .put_if_absent(
                    &keys::access_key(&access_key_id),
                    &json!({
                        "access_key_id": access_key_id,
                        "account_id": account_id,
                        "user_name": user_name,
                        "secret_key_encrypted_b64": BASE64.encode(&sealed),
                        "is_active": true,
                        "created_at": now_rfc3339(),
                    }),
                )
                .await
                .map_err(OpError::Internal)?;
            if !inserted {
                return Err(OpError::AlreadyExists(
                    "Access key ID already exists".to_owned(),
                ));
            }
            Ok(())
        })
    }

    fn store_session(
        &self,
        session_token: &str,
        access_key_id: &str,
        secret_key_encrypted: &[u8],
        account_id: &str,
        role_name: &str,
        session_name: &str,
        session_tags: &Option<serde_json::Value>,
        session_policy: &Option<serde_json::Value>,
        expires_at: OffsetDateTime,
    ) -> BoxFuture<'_, OpResult<()>> {
        let session_token = session_token.to_owned();
        let access_key_id = access_key_id.to_owned();
        let secret_key_encrypted = secret_key_encrypted.to_vec();
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session_name = session_name.to_owned();
        let session_tags = session_tags.clone();
        let session_policy = session_policy.clone();
        Box::pin(async move {
            let _ = require_keys(self.keys.as_deref())?;
            let expires_unix = expires_at.unix_timestamp();
            let truncated = OffsetDateTime::from_unix_timestamp(expires_unix).unwrap_or(expires_at);
            let expires_str = truncated
                .format(&Rfc3339)
                .unwrap_or_else(|_| truncated.to_string());
            let cat = self.cat();
            cat.put(
                &keys::iam_session(&session_token),
                &json!({
                    "session_token": session_token,
                    "access_key_id": access_key_id,
                    "secret_key_encrypted_b64": BASE64.encode(&secret_key_encrypted),
                    "account_id": account_id,
                    "role_name": role_name,
                    "session_name": session_name,
                    "session_tags": session_tags,
                    "session_policy": session_policy,
                    "expires_unix": expires_unix,
                    "expires_at": expires_str,
                    "created_at": now_rfc3339(),
                }),
            )
            .await
            .map_err(OpError::Internal)?;
            cat.put(
                &keys::iam_session_access_key(&access_key_id),
                &json!({
                    "session_token": session_token,
                }),
            )
            .await
            .map_err(OpError::Internal)
        })
    }

    fn list_session_policies(&self) -> BoxFuture<'_, OpResult<Vec<SessionPolicyEntry>>> {
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(keys::IAM_SESSION_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let mut out = Vec::new();
            for (rk, v) in rows {
                let Some(policy) = v.get("session_policy").filter(|p| !p.is_null()).cloned() else {
                    continue;
                };
                let token = v
                    .get("session_token")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| rk.strip_prefix(keys::IAM_SESSION_SCAN_PREFIX))
                    .unwrap_or("")
                    .to_owned();
                let account_id = v
                    .get("account_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let role_name = v
                    .get("role_name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let session_name = v
                    .get("session_name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                out.push(SessionPolicyEntry {
                    session_token: token,
                    account_id,
                    role_name,
                    session_name,
                    policy,
                });
            }
            Ok(out)
        })
    }

    fn update_session_policy(
        &self,
        session_token: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let session_token = session_token.to_owned();
        let document = document.clone();
        Box::pin(async move {
            let rk = keys::iam_session(&session_token);
            self.cat()
                .mutate_json_cas(&rk, OpError::Internal, move |existing| {
                    let Some(mut cur) = existing else {
                        return Ok((CatalogJsonAction::NoOp, ()));
                    };
                    if let Some(obj) = cur.as_object_mut() {
                        obj.insert("session_policy".to_owned(), document.clone());
                    }
                    Ok((CatalogJsonAction::Put(cur), ()))
                })
                .await
        })
    }

    fn fetch_caller_tags(
        &self,
        account_id: &str,
        resource: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let resource = resource.to_owned();
        Box::pin(async move {
            let suffix = resource
                .rsplit_once(':')
                .map_or(resource.as_str(), |(_, r)| r);
            if let Some(user_name) = suffix.strip_prefix("user/") {
                return self.list_user_tags(&account_id, user_name).await;
            }
            let role_name = suffix
                .strip_prefix("assumed-role/")
                .or_else(|| suffix.strip_prefix("role/"))
                .map(|rest| rest.split('/').next().unwrap_or(rest));
            if let Some(role_name) = role_name.filter(|r| !r.is_empty()) {
                return self.list_role_tags(&account_id, role_name).await;
            }
            Ok(Vec::new())
        })
    }

    // ── Encryption-Key Commands ────────────────────────────────────

    fn read_secret_page(
        &self,
        table: SecretTable,
        page: Option<&str>,
        page_size: usize,
    ) -> BoxFuture<'_, OpResult<SecretPage>> {
        let page = page.map(str::to_owned);
        Box::pin(async move {
            let page_size = page_size.max(1);
            let prefix = match table {
                SecretTable::AccessKeys => keys::ACCESS_KEY_SCAN_PREFIX,
                SecretTable::IamSessions => keys::IAM_SESSION_SCAN_PREFIX,
            };
            let raw = self
                .cat()
                .scan_prefix(prefix)
                .await
                .map_err(OpError::Internal)?;
            let mut matched: Vec<StoredSecretRow> = Vec::new();
            for (rk, v) in raw {
                let Some(pk) = rk.strip_prefix(prefix).map(str::to_owned) else {
                    continue;
                };
                if let Some(after) = page.as_deref()
                    && pk.as_str() <= after
                {
                    continue;
                }
                let Some(secret_key_encrypted) = decode_stored_secret(&v) else {
                    continue;
                };
                let access_key_id = v
                    .get("access_key_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&pk)
                    .to_owned();
                let account_id = v
                    .get("account_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let created_at = v
                    .get("created_at")
                    .and_then(serde_json::Value::as_str)
                    .and_then(parse_rfc3339);

                match table {
                    SecretTable::AccessKeys => {
                        let principal_name = v
                            .get("user_name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("")
                            .to_owned();
                        matched.push(StoredSecretRow {
                            table,
                            key: pk,
                            access_key_id,
                            account_id,
                            principal_name,
                            session_name: None,
                            expires_at: None,
                            created_at,
                            secret_key_encrypted,
                            write_timestamp: None,
                        });
                    }
                    SecretTable::IamSessions => {
                        let principal_name = v
                            .get("role_name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("")
                            .to_owned();
                        let session_name = v
                            .get("session_name")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned);
                        let expires_at = session_expires_at(&v).map(|(secs, _)| secs);
                        matched.push(StoredSecretRow {
                            table,
                            key: pk,
                            access_key_id,
                            account_id,
                            principal_name,
                            session_name,
                            expires_at,
                            created_at,
                            secret_key_encrypted,
                            write_timestamp: None,
                        });
                    }
                }
            }
            matched.sort_by(|a, b| a.key.cmp(&b.key));
            let has_more = matched.len() > page_size;
            matched.truncate(page_size);
            let next = if has_more {
                matched.last().map(|r| r.key.clone())
            } else {
                None
            };
            Ok(SecretPage {
                rows: matched,
                next,
            })
        })
    }

    fn write_secrets(
        &self,
        table: SecretTable,
        rows: &[SecretWrite],
    ) -> BoxFuture<'_, OpResult<()>> {
        let rows = rows.to_vec();
        Box::pin(async move {
            let cat = self.cat();
            for w in &rows {
                let rk = match table {
                    SecretTable::AccessKeys => keys::access_key(&w.key),
                    SecretTable::IamSessions => keys::iam_session(&w.key),
                };
                let b64 = BASE64.encode(&w.secret_key_encrypted);
                cat.mutate_json_cas(&rk, OpError::Internal, move |existing| {
                    let Some(mut cur) = existing else {
                        return Ok((CatalogJsonAction::NoOp, ()));
                    };
                    if let Some(obj) = cur.as_object_mut() {
                        obj.remove("secret_encrypted");
                        obj.insert(
                            "secret_key_encrypted_b64".to_owned(),
                            serde_json::Value::String(b64.clone()),
                        );
                    }
                    Ok((CatalogJsonAction::Put(cur), ()))
                })
                .await?;
            }
            Ok(())
        })
    }

    fn delete_secrets(&self, table: SecretTable, keys: &[String]) -> BoxFuture<'_, OpResult<()>> {
        let keys_to_del = keys.to_vec();
        Box::pin(async move {
            let cat = self.cat();
            match table {
                SecretTable::AccessKeys => {
                    let rks: Vec<String> =
                        keys_to_del.iter().map(|k| keys::access_key(k)).collect();
                    cat.delete_keys(rks.iter().map(String::as_str))
                        .await
                        .map_err(OpError::Internal)?;
                }
                SecretTable::IamSessions => {
                    let mut rks: Vec<String> = Vec::with_capacity(keys_to_del.len() * 2);
                    for token in &keys_to_del {
                        let sess_rk = keys::iam_session(token);
                        if let Some(v) = cat.get(&sess_rk).await.map_err(OpError::Internal)?
                            && let Some(akid) =
                                v.get("access_key_id").and_then(serde_json::Value::as_str)
                        {
                            rks.push(keys::iam_session_access_key(akid));
                        }
                        rks.push(sess_rk);
                    }
                    cat.delete_keys(rks.iter().map(String::as_str))
                        .await
                        .map_err(OpError::Internal)?;
                }
            }
            Ok(())
        })
    }

    fn delete_setting(&self, key: &str) -> BoxFuture<'_, OpResult<()>> {
        let key = key.to_owned();
        Box::pin(async move {
            self.cat()
                .delete(&keys::setting(&key))
                .await
                .map_err(OpError::Internal)
        })
    }
}

// ── AuthorizationStore ─────────────────────────────────────────────────

impl AuthorizationStore for BigtableCatalogStore {
    fn fetch_user_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let dev = self.dev_mode && user_name == crate::dev_auth::DEV_USER_NAME;
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            if dev {
                return Ok(vec![DEV_ADMIN_POLICY.to_owned()]);
            }
            let rows = self
                .cat()
                .scan_prefix(&keys::user_policy_scan_prefix(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(rows
                .into_iter()
                .filter_map(|(_, v)| v.get("document").map(serde_json::Value::to_string))
                .collect())
        })
    }

    fn fetch_user_group_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let cat = self.cat();
            let memberships = cat
                .scan_prefix(&keys::group_member_account_prefix(&account_id))
                .await
                .map_err(OpError::Internal)?;
            let mut out = Vec::new();
            for (_, v) in memberships {
                if v.get("user_name").and_then(serde_json::Value::as_str) != Some(&user_name) {
                    continue;
                }
                let Some(group_name) = v.get("group_name").and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let policies = cat
                    .scan_prefix(&keys::group_policy_scan_prefix(&account_id, group_name))
                    .await
                    .map_err(OpError::Internal)?;
                for (_, pv) in policies {
                    if let Some(doc) = pv.get("document") {
                        out.push((group_name.to_owned(), doc.to_string()));
                    }
                }
            }
            Ok(out)
        })
    }

    fn fetch_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            Ok(self
                .get_user_boundary(&account_id, &user_name)
                .await?
                .map(|v| v.to_string()))
        })
    }

    fn fetch_role_policies(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(&keys::role_policy_scan_prefix(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(rows
                .into_iter()
                .filter_map(|(_, v)| v.get("document").map(serde_json::Value::to_string))
                .collect())
        })
    }

    fn fetch_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            Ok(self
                .get_role_boundary(&account_id, &role_name)
                .await?
                .map(|v| v.to_string()))
        })
    }

    fn fetch_session_data(
        &self,
        account_id: &str,
        role_name: &str,
        session_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<SessionData>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session_name = session_name.to_owned();
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(keys::IAM_SESSION_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let now = OffsetDateTime::now_utc();
            let mut latest: Option<(OffsetDateTime, serde_json::Value)> = None;
            for (_, v) in rows {
                if v.get("account_id").and_then(serde_json::Value::as_str) != Some(&account_id)
                    || v.get("role_name").and_then(serde_json::Value::as_str) != Some(&role_name)
                    || v.get("session_name").and_then(serde_json::Value::as_str)
                        != Some(&session_name)
                {
                    continue;
                }
                if let Some((_, exp)) = session_expires_at(&v)
                    && exp < now
                {
                    continue;
                }
                let created = parse_created_at(&v);
                if latest.as_ref().is_none_or(|(prev, _)| created >= *prev) {
                    latest = Some((created, v));
                }
            }
            let Some((_, v)) = latest else {
                return Ok(None);
            };
            let session_policy =
                v.get("session_policy")
                    .filter(|p| !p.is_null())
                    .map(|p| match p {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    });
            let session_tags = v
                .get("session_tags")
                .map(parse_session_tags)
                .unwrap_or_default();
            let created_at = v
                .get("created_at")
                .and_then(serde_json::Value::as_str)
                .and_then(parse_rfc3339)
                .and_then(|dt| dt.format(&Rfc3339).ok());
            Ok(Some(SessionData {
                session_policy,
                session_tags,
                created_at,
            }))
        })
    }

    fn fetch_user_tags(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        self.list_user_tags(account_id, user_name)
    }

    fn fetch_role_tags(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        self.list_role_tags(account_id, role_name)
    }

    fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let arn = arn.to_owned();
        Box::pin(async move {
            let Some(cur) = self
                .cat()
                .get(&keys::tags(&arn))
                .await
                .map_err(OpError::Internal)?
            else {
                return Ok(Vec::new());
            };
            Ok(parse_tags_map(&cur))
        })
    }

    fn fetch_user_created_at(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let dev = self.dev_mode && user_name == crate::dev_auth::DEV_USER_NAME;
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            if dev {
                return Ok(Some("1970-01-01T00:00:00Z".to_owned()));
            }
            let row = self
                .cat()
                .get(&keys::user(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(row.and_then(|v| {
                v.get("created_at")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            }))
        })
    }

    fn fetch_role_created_at(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::role(&account_id, &role_name))
                .await
                .map_err(OpError::Internal)?;
            Ok(row.and_then(|v| {
                v.get("created_at")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            }))
        })
    }
}

impl CatalogStore for BigtableCatalogStore {
    fn secret_keys(&self) -> Option<&SecretKeys> {
        self.keys.as_deref()
    }
}

// ── CredentialStore ────────────────────────────────────────────────────

pub struct BigtableCredentialStore {
    client: Arc<BigtableClient>,
    keys: Option<Arc<SecretKeys>>,
}

impl BigtableCredentialStore {
    pub fn new(client: Arc<BigtableClient>, keys: Option<Arc<SecretKeys>>) -> Self {
        Self { client, keys }
    }
}

#[async_trait::async_trait]
impl extenddb_auth::CredentialStore for BigtableCredentialStore {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<extenddb_auth::StoredCredential>, DynamoDbError> {
        let cat = Catalog::new(&self.client);
        let keys_holder = self.keys.as_deref().ok_or_else(|| {
            DynamoDbError::InternalServerError(
                "credential store opened without secret keys".to_owned(),
            )
        })?;

        if access_key_id.starts_with("ASIA") {
            let idx = cat
                .get(&keys::iam_session_access_key(access_key_id))
                .await
                .map_err(|e| DynamoDbError::InternalServerError(format!("catalog read: {e}")))?;
            let Some(idx_val) = idx else {
                return Ok(None);
            };
            let Some(session_token) = idx_val
                .get("session_token")
                .and_then(serde_json::Value::as_str)
            else {
                return Ok(None);
            };
            let Some(sess) = cat
                .get(&keys::iam_session(session_token))
                .await
                .map_err(|e| DynamoDbError::InternalServerError(format!("catalog read: {e}")))?
            else {
                return Ok(None);
            };

            let (expires_unix, expires_dt) = session_expires_at(&sess).ok_or_else(|| {
                DynamoDbError::InternalServerError("session missing expires_at".to_owned())
            })?;
            if expires_dt < OffsetDateTime::now_utc() {
                return Err(DynamoDbError::ExpiredTokenException(
                    "The security token included in the request is expired".to_owned(),
                ));
            }

            let ciphertext = decode_stored_secret(&sess).ok_or_else(|| {
                DynamoDbError::InternalServerError(
                    "session missing secret_key_encrypted_b64".to_owned(),
                )
            })?;
            let account_id = sess
                .get("account_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_owned();
            let role_name = sess
                .get("role_name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_owned();
            let session_name = sess
                .get("session_name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_owned();

            let aad = SecretRow::session(
                access_key_id,
                &account_id,
                &role_name,
                &session_name,
                expires_unix,
            );
            let plaintext = keys_holder
                .decrypt(&aad, &ciphertext)
                .map_err(|e| DynamoDbError::InternalServerError(format!("decrypt session: {e}")))?;

            return Ok(Some(extenddb_auth::StoredCredential {
                secret_key: (*plaintext).clone(),
                account_id,
                principal_name: role_name,
                session_name: Some(session_name),
                is_session: true,
                session_token: Some(session_token.to_owned()),
                is_active: true,
                expires_at: Some(expires_dt),
            }));
        }

        let Some(row) = cat
            .get(&keys::access_key(access_key_id))
            .await
            .map_err(|e| DynamoDbError::InternalServerError(format!("catalog read: {e}")))?
        else {
            return Ok(None);
        };

        let ciphertext = decode_stored_secret(&row).ok_or_else(|| {
            DynamoDbError::InternalServerError(
                "access key missing secret_key_encrypted_b64".to_owned(),
            )
        })?;
        let account_id = row
            .get("account_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned();
        let user_name = row
            .get("user_name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned();
        let is_active = row
            .get("is_active")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);

        let aad = SecretRow::access_key(access_key_id, &account_id, &user_name);
        let plaintext = keys_holder
            .decrypt(&aad, &ciphertext)
            .map_err(|e| DynamoDbError::InternalServerError(format!("decrypt access key: {e}")))?;

        Ok(Some(extenddb_auth::StoredCredential {
            secret_key: (*plaintext).clone(),
            account_id,
            principal_name: user_name,
            session_name: None,
            is_session: false,
            session_token: None,
            is_active,
            expires_at: None,
        }))
    }
}
