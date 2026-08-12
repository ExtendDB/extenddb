//! BigtableCatalogStore: implements `CatalogStore` and all its super-traits
//! (ManagementStore, AdminStore, SettingsStore, MetricsStore, RateLimitStore,
//! AuthorizationStore).
//!
//! Phase 2 reality: very few methods do real work. `cached_encryption_key`
//! returns the key we loaded at construction; everything else returns
//! `OpError::Internal("... lands in phase N")` or a sensible empty default.

use std::sync::Arc;

use extenddb_storage::CatalogStore;
use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, AdminEntry, AdminStore, GroupDetail, GroupListEntry,
    ManagementStore, MetricsRow, MetricsStore, OpError, OpResult, RateLimitStore, RoleDetail,
    RoleListEntry, SettingsStore, UserDetail, UserListEntry,
};
use futures::future::BoxFuture;
use rand::{TryRngCore, rngs::OsRng};
use serde_json::json;

use crate::catalog::{Catalog, keys};
use crate::crypto;
use crate::data::client::BigtableClient;

fn todo_phase(phase: u32, what: &str) -> OpError {
    OpError::Internal(format!("bigtable catalog: {what} lands in phase {phase}"))
}

pub struct BigtableCatalogStore {
    client: Arc<BigtableClient>,
    encryption_key: Option<String>,
    dev_mode: bool,
}

impl BigtableCatalogStore {
    pub fn new(
        client: Arc<BigtableClient>,
        encryption_key: Option<String>,
        dev_mode: bool,
    ) -> Self {
        Self {
            client,
            encryption_key,
            dev_mode,
        }
    }

    fn cat(&self) -> Catalog<'_> {
        Catalog::new(&self.client)
    }

    fn enc_key(&self) -> Result<&str, OpError> {
        self.encryption_key
            .as_deref()
            .ok_or_else(|| OpError::Internal("encryption key not loaded".into()))
    }

    /// Construct ARNs consistently — region-less for IAM.
    fn user_arn(account_id: &str, user_name: &str) -> String {
        format!("arn:aws:iam::{account_id}:user/{user_name}")
    }
}

impl extenddb_storage::diagnostics::DiagnosticsStore for BigtableCatalogStore {
    fn count_tables(&self) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<i64>> {
        Box::pin(async move {
            let cat = self.cat();
            let tables = cat
                .scan_prefix(crate::catalog::keys::TABLE_META_SCAN_PREFIX)
                .await
                .map_err(extenddb_storage::diagnostics::DiagError::QueryFailed)?;
            Ok(tables.len() as i64)
        })
    }

    fn count_indexes(&self) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<i64>> {
        Box::pin(async move {
            let cat = self.cat();
            let tables = cat
                .scan_prefix(crate::catalog::keys::TABLE_META_SCAN_PREFIX)
                .await
                .map_err(extenddb_storage::diagnostics::DiagError::QueryFailed)?;
            let mut count = 0;
            for (_, desc_val) in tables {
                if let Ok(desc) = serde_json::from_value::<extenddb_core::types::TableDescription>(desc_val) {
                    count += desc.global_secondary_indexes.map(|g| g.len()).unwrap_or(0);
                    count += desc.local_secondary_indexes.map(|l| l.len()).unwrap_or(0);
                }
            }
            Ok(count as i64)
        })
    }

    fn test_data_database_connection(&self) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<String>> {
        Box::pin(async move {
            Ok(self.client.instance_id.clone())
        })
    }
}

/// AWS-style access-key id: `AKIA` + 16 uppercase alphanumeric characters.
fn generate_access_key_id() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut buf = [0u8; 16];
    OsRng.try_fill_bytes(&mut buf).expect("OS RNG");
    let mut out = String::from("AKIA");
    for b in &buf {
        out.push(ALPHABET[(*b as usize) % ALPHABET.len()] as char);
    }
    out
}

/// AWS-style secret access key: 40 random base64-ish characters.
fn generate_secret() -> String {
    use base64::Engine;
    let mut buf = [0u8; 30];
    OsRng.try_fill_bytes(&mut buf).expect("OS RNG");
    base64::engine::general_purpose::STANDARD
        .encode(buf)
        .chars()
        .take(40)
        .collect()
}

/// Allow-all DDB policy returned for the dev admin user in dev_mode.
const DEV_ADMIN_POLICY: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]
}"#;

// =========== SettingsStore ===========

impl SettingsStore for BigtableCatalogStore {
    fn get_setting(&self, _key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        Box::pin(async { Ok(None) })
    }

    fn set_setting(&self, _key: &str, _value: &str) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "set_setting")) })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.clone()
    }
}

// =========== MetricsStore ===========

impl MetricsStore for BigtableCatalogStore {
    fn insert_metrics(&self, _rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn query_metrics(
        &self,
        _start: time::OffsetDateTime,
        _end: time::OffsetDateTime,
        _table_name: Option<&str>,
        _metric: Option<&str>,
    ) -> BoxFuture<'_, OpResult<Vec<MetricsRow>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn prune_metrics(&self, _retention: std::time::Duration) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

// =========== RateLimitStore ===========

impl RateLimitStore for BigtableCatalogStore {
    fn count_principal_failures(
        &self,
        _principal: &str,
        _window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        Box::pin(async { Ok(0) })
    }

    fn count_ip_failures(
        &self,
        _source_ip: &str,
        _window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        Box::pin(async { Ok(0) })
    }

    fn record_failed_login(&self, _principal: &str, _source_ip: Option<&str>) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn cleanup_old_attempts(&self, _max_age_seconds: i64) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

// =========== AdminStore ===========

impl AdminStore for BigtableCatalogStore {
    fn create_admin(&self, admin_name: &str, password_hash: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let password_hash = password_hash.to_owned();
        Box::pin(async move {
            self.cat()
                .put(
                    &keys::admin(&admin_name),
                    &json!({
                        "username": admin_name,
                        "password_hash": password_hash,
                        "from_env": false,
                        "created_at": time::OffsetDateTime::now_utc().to_string(),
                    }),
                )
                .await
                .map_err(OpError::Internal)
        })
    }

    fn list_admins(&self) -> BoxFuture<'_, OpResult<Vec<AdminEntry>>> {
        Box::pin(async {
            let rows = self
                .cat()
                .scan_prefix("admin:")
                .await
                .map_err(OpError::Internal)?;
            let mut out = Vec::with_capacity(rows.len());
            for (_, v) in rows {
                let name = v
                    .get("username")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string();
                let created_at = v
                    .get("created_at")
                    .and_then(|s| s.as_str())
                    .and_then(|s| time::OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT).ok())
                    .unwrap_or_else(time::OffsetDateTime::now_utc);
                out.push(AdminEntry {
                    admin_name: name,
                    created_at,
                });
            }
            Ok(out)
        })
    }

    fn delete_admin(&self, admin_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        Box::pin(async move {
            self.cat()
                .delete(&keys::admin(&admin_name))
                .await
                .map_err(OpError::Internal)
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
            let mut row = self
                .cat()
                .get(&keys::admin(&admin_name))
                .await
                .map_err(OpError::Internal)?
                .ok_or_else(|| OpError::NotFound(admin_name.clone()))?;
            if let Some(obj) = row.as_object_mut() {
                obj.insert("password_hash".into(), json!(password_hash));
            }
            self.cat()
                .put(&keys::admin(&admin_name), &row)
                .await
                .map_err(OpError::Internal)
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
            let row = self
                .cat()
                .get(&keys::admin(&admin_name))
                .await
                .map_err(OpError::Internal)?;
            let Some(row) = row else { return Ok(None) };
            let hash = row
                .get("password_hash")
                .and_then(|s| s.as_str())
                .ok_or_else(|| OpError::Internal("admin missing password_hash".into()))?;
            Ok(Some(bcrypt::verify(&password, hash).unwrap_or(false)))
        })
    }
}

// =========== ManagementStore (massive) ===========

impl ManagementStore for BigtableCatalogStore {
    fn default_account_id(&self) -> BoxFuture<'_, OpResult<Option<String>>> {
        Box::pin(async move {
            let accounts = self
                .cat()
                .scan_prefix(crate::catalog::keys::ACCOUNT_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            Ok(accounts
                .first()
                .and_then(|(k, _)| k.strip_prefix("acct:"))
                .map(str::to_owned))
        })
    }

    fn create_account(&self, account_id: &str, account_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let account_name = account_name.to_owned();
        Box::pin(async move {
            self.cat()
                .put(
                    &keys::account(&account_id),
                    &json!({
                        "account_id": account_id,
                        "account_name": account_name,
                        "created_at": time::OffsetDateTime::now_utc().to_string(),
                    }),
                )
                .await
                .map_err(OpError::Internal)
        })
    }

    fn delete_account(&self, account_id: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            // Refuse if any tables remain in the account (HasDependents).
            let tables = self
                .cat()
                .scan_prefix(&keys::table_meta_scan_prefix(&account_id))
                .await
                .map_err(OpError::Internal)?;
            if !tables.is_empty() {
                return Err(OpError::Internal(format!(
                    "account {account_id} still owns {} table(s)",
                    tables.len()
                )));
            }
            self.cat()
                .delete(&keys::account(&account_id))
                .await
                .map_err(OpError::Internal)
        })
    }

    fn list_all_accounts(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async {
            let rows = self
                .cat()
                .scan_prefix(keys::ACCOUNT_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            Ok(rows
                .into_iter()
                .map(|(_, v)| {
                    let id = v
                        .get("account_id")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string();
                    let name = v
                        .get("account_name")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string();
                    (id, name)
                })
                .collect())
        })
    }

    fn list_all_accounts_full(
        &self,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String, time::OffsetDateTime)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_accounts_for(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get_account_detail(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, OpResult<Option<AccountDetail>>> {
        Box::pin(async { Ok(None) })
    }

    fn dashboard_counts(&self) -> BoxFuture<'_, OpResult<(i64, i64)>> {
        Box::pin(async { Ok((0, 0)) })
    }

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
            self.cat()
                .put(
                    &keys::user(&account_id, &user_name),
                    &json!({
                        "user_name": user_name,
                        "account_id": account_id,
                        "password_hash": password_hash,
                        "has_password": password_hash.is_some(),
                        "user_arn": Self::user_arn(&account_id, &user_name),
                        "created_at": time::OffsetDateTime::now_utc().to_string(),
                    }),
                )
                .await
                .map_err(OpError::Internal)
        })
    }

    fn delete_user(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            // Cascade: delete the user's policies and access keys.
            let polices_prefix = format!("user_policy:{account_id}:{user_name}:");
            self.cat()
                .delete_prefix(&polices_prefix)
                .await
                .map_err(OpError::Internal)?;
            // Delete access keys owned by this user.
            let keys_rows = self
                .cat()
                .scan_prefix(keys::ACCESS_KEY_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            for (k, v) in keys_rows {
                let owner_acct = v.get("account_id").and_then(|s| s.as_str()).unwrap_or("");
                let owner_user = v.get("user_name").and_then(|s| s.as_str()).unwrap_or("");
                if owner_acct == account_id && owner_user == user_name {
                    self.cat().delete(&k).await.map_err(OpError::Internal)?;
                }
            }
            self.cat()
                .delete(&keys::user(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)
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
            Ok(rows
                .into_iter()
                .map(|(_, v)| {
                    let name = v
                        .get("user_name")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string();
                    let arn = v
                        .get("user_arn")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string();
                    let has_pw = v
                        .get("has_password")
                        .and_then(|b| b.as_bool())
                        .unwrap_or(false);
                    let created = v
                        .get("created_at")
                        .and_then(|s| s.as_str())
                        .and_then(|s| time::OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT).ok())
                        .unwrap_or_else(time::OffsetDateTime::now_utc);
                    (account_id.clone(), name, arn, has_pw, created)
                })
                .collect())
        })
    }

    fn get_user_detail(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<UserDetail>>> {
        Box::pin(async { Ok(None) })
    }

    fn verify_iam_user_password(
        &self,
        _account_id: &str,
        _user_name: &str,
        _password: &str,
    ) -> BoxFuture<'_, OpResult<bool>> {
        Box::pin(async { Ok(false) })
    }

    fn change_user_password(
        &self,
        _account_id: &str,
        _user_name: &str,
        _password_hash: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "change_user_password")) })
    }

    fn tag_user(
        &self,
        _account_id: &str,
        _user_name: &str,
        _tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn untag_user(
        &self,
        _account_id: &str,
        _user_name: &str,
        _tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn list_user_tags(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn create_group(&self, _account_id: &str, _group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "create_group")) })
    }

    fn delete_group(&self, _account_id: &str, _group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "delete_group")) })
    }

    fn list_groups(&self, _account_id: &str) -> BoxFuture<'_, OpResult<Vec<GroupListEntry>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get_group_detail(
        &self,
        _account_id: &str,
        _group_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<GroupDetail>>> {
        Box::pin(async { Ok(None) })
    }

    fn add_group_member(
        &self,
        _account_id: &str,
        _group_name: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "add_group_member")) })
    }

    fn remove_group_member(
        &self,
        _account_id: &str,
        _group_name: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "remove_group_member")) })
    }

    fn create_role(
        &self,
        _account_id: &str,
        _role_name: &str,
        _trust_policy: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "create_role")) })
    }

    fn delete_role(&self, _account_id: &str, _role_name: &str) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "delete_role")) })
    }

    fn list_roles(&self, _account_id: &str) -> BoxFuture<'_, OpResult<Vec<RoleListEntry>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get_role_detail(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<RoleDetail>>> {
        Box::pin(async { Ok(None) })
    }

    fn get_role_trust_policy(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        Box::pin(async { Ok(None) })
    }

    fn tag_role(
        &self,
        _account_id: &str,
        _role_name: &str,
        _tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn untag_role(
        &self,
        _account_id: &str,
        _role_name: &str,
        _tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn list_role_tags(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn put_policy(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let key = match principal_type {
            "user" => keys::user_policy(account_id, principal_name, policy_name),
            "role" => keys::role_policy(account_id, principal_name, policy_name),
            other => {
                let other = other.to_owned();
                return Box::pin(async move {
                    Err(OpError::Internal(format!(
                        "unknown principal_type {other:?}"
                    )))
                });
            }
        };
        let document = document.clone();
        let policy_name = policy_name.to_owned();
        Box::pin(async move {
            self.cat()
                .put(
                    &key,
                    &json!({
                        "policy_name": policy_name,
                        "document": document,
                        "created_at": time::OffsetDateTime::now_utc().to_string(),
                    }),
                )
                .await
                .map_err(OpError::Internal)
        })
    }

    fn delete_policy(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let key = match principal_type {
            "user" => keys::user_policy(account_id, principal_name, policy_name),
            "role" => keys::role_policy(account_id, principal_name, policy_name),
            other => {
                let other = other.to_owned();
                return Box::pin(async move {
                    Err(OpError::Internal(format!(
                        "unknown principal_type {other:?}"
                    )))
                });
            }
        };
        Box::pin(async move { self.cat().delete(&key).await.map_err(OpError::Internal) })
    }

    fn list_policies(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, serde_json::Value, time::OffsetDateTime)>>> {
        let prefix = format!("{principal_type}_policy:{account_id}:{principal_name}:");
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(&prefix)
                .await
                .map_err(OpError::Internal)?;
            Ok(rows
                .into_iter()
                .map(|(_, v)| {
                    let name = v
                        .get("policy_name")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string();
                    let doc = v.get("document").cloned().unwrap_or(serde_json::Value::Null);
                    let created = v
                        .get("created_at")
                        .and_then(|s| s.as_str())
                        .and_then(|s| time::OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT).ok())
                        .unwrap_or_else(time::OffsetDateTime::now_utc);
                    (name, doc, created)
                })
                .collect())
        })
    }

    fn set_user_boundary(
        &self,
        _account_id: &str,
        _user_name: &str,
        _document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "set_user_boundary")) })
    }

    fn get_user_boundary(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        Box::pin(async { Ok(None) })
    }

    fn delete_user_boundary(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn set_role_boundary(
        &self,
        _account_id: &str,
        _role_name: &str,
        _document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "set_role_boundary")) })
    }

    fn get_role_boundary(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        Box::pin(async { Ok(None) })
    }

    fn delete_role_boundary(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn create_access_key(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<AccessKeyCreated>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            // Verify the user exists before minting a key for it.
            if self
                .cat()
                .get(&keys::user(&account_id, &user_name))
                .await
                .map_err(OpError::Internal)?
                .is_none()
            {
                return Err(OpError::NotFound(user_name));
            }
            let key_id = generate_access_key_id();
            let secret = generate_secret();
            let enc_key = self.enc_key()?;
            let sealed = crypto::encrypt(enc_key, secret.as_bytes()).map_err(OpError::Internal)?;
            self.cat()
                .put(
                    &keys::access_key(&key_id),
                    &json!({
                        "access_key_id": key_id,
                        "account_id": account_id,
                        "user_name": user_name,
                        "secret_encrypted": sealed,
                        "is_active": true,
                        "is_session": false,
                        "created_at": time::OffsetDateTime::now_utc().to_string(),
                    }),
                )
                .await
                .map_err(OpError::Internal)?;
            Ok(AccessKeyCreated {
                access_key_id: key_id,
                secret_access_key: secret,
            })
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
            // Verify ownership before deletion.
            let row = self
                .cat()
                .get(&keys::access_key(&key_id))
                .await
                .map_err(OpError::Internal)?
                .ok_or_else(|| OpError::NotFound(key_id.clone()))?;
            let owner_acct = row.get("account_id").and_then(|s| s.as_str()).unwrap_or("");
            let owner_user = row.get("user_name").and_then(|s| s.as_str()).unwrap_or("");
            if owner_acct != account_id || owner_user != user_name {
                return Err(OpError::NotFound(key_id));
            }
            self.cat()
                .delete(&keys::access_key(&key_id))
                .await
                .map_err(OpError::Internal)
        })
    }

    fn list_access_keys(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, bool, time::OffsetDateTime)>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(keys::ACCESS_KEY_SCAN_PREFIX)
                .await
                .map_err(OpError::Internal)?;
            let mut out = Vec::new();
            for (_, v) in rows {
                let owner_acct = v.get("account_id").and_then(|s| s.as_str()).unwrap_or("");
                let owner_user = v.get("user_name").and_then(|s| s.as_str()).unwrap_or("");
                if owner_acct != account_id || owner_user != user_name {
                    continue;
                }
                let kid = v
                    .get("access_key_id")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string();
                let active = v.get("is_active").and_then(|b| b.as_bool()).unwrap_or(false);
                let created = v
                    .get("created_at")
                    .and_then(|s| s.as_str())
                    .and_then(|s| time::OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT).ok())
                    .unwrap_or_else(time::OffsetDateTime::now_utc);
                out.push((kid, active, created));
            }
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
            let enc_key = self.enc_key()?;
            let sealed =
                crypto::encrypt(enc_key, secret_access_key.as_bytes()).map_err(OpError::Internal)?;
            self.cat()
                .put(
                    &keys::access_key(&access_key_id),
                    &json!({
                        "access_key_id": access_key_id,
                        "account_id": account_id,
                        "user_name": user_name,
                        "secret_encrypted": sealed,
                        "is_active": true,
                        "is_session": false,
                        "created_at": time::OffsetDateTime::now_utc().to_string(),
                    }),
                )
                .await
                .map_err(OpError::Internal)
        })
    }

    fn store_session(
        &self,
        _session_token: &str,
        _access_key_id: &str,
        _secret_key_encrypted: &[u8],
        _account_id: &str,
        _role_name: &str,
        _session_name: &str,
        _session_tags: &Option<serde_json::Value>,
        _session_policy: &Option<serde_json::Value>,
        _expires_at: time::OffsetDateTime,
    ) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Err(todo_phase(2, "store_session")) })
    }

    fn fetch_caller_tags(
        &self,
        _account_id: &str,
        _resource: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

// =========== AuthorizationStore ===========

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
                return Ok(vec![DEV_ADMIN_POLICY.to_string()]);
            }
            let prefix = format!("user_policy:{account_id}:{user_name}:");
            let rows = self
                .cat()
                .scan_prefix(&prefix)
                .await
                .map_err(OpError::Internal)?;
            Ok(rows
                .into_iter()
                .filter_map(|(_, v)| v.get("document").map(|d| d.to_string()))
                .collect())
        })
    }

    fn fetch_user_group_policies(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_user_boundary(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        Box::pin(async { Ok(None) })
    }

    fn fetch_role_policies(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let prefix = format!("role_policy:{account_id}:{role_name}:");
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(&prefix)
                .await
                .map_err(OpError::Internal)?;
            Ok(rows
                .into_iter()
                .filter_map(|(_, v)| v.get("document").map(|d| d.to_string()))
                .collect())
        })
    }

    fn fetch_role_boundary(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        Box::pin(async { Ok(None) })
    }

    fn fetch_session_data(
        &self,
        _account_id: &str,
        _role_name: &str,
        _session_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<SessionData>>> {
        Box::pin(async { Ok(None) })
    }

    fn fetch_user_tags(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_role_tags(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_resource_tags(
        &self,
        _arn: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

// =========== CatalogStore supertrait ===========

impl CatalogStore for BigtableCatalogStore {
    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.clone()
    }
}

// =========== CredentialStore (for auth provider) ===========

pub struct BigtableCredentialStore {
    #[allow(dead_code)]
    client: Arc<BigtableClient>,
    #[allow(dead_code)]
    encryption_key: String,
}

impl BigtableCredentialStore {
    pub fn new(client: Arc<BigtableClient>, encryption_key: String) -> Self {
        Self {
            client,
            encryption_key,
        }
    }
}

#[async_trait::async_trait]
impl extenddb_auth::CredentialStore for BigtableCredentialStore {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<extenddb_auth::StoredCredential>, extenddb_core::error::DynamoDbError> {
        let cat = crate::catalog::Catalog::new(&self.client);
        let row = cat
            .get(&keys::access_key(access_key_id))
            .await
            .map_err(|e| {
                extenddb_core::error::DynamoDbError::InternalServerError(format!(
                    "catalog read: {e}"
                ))
            })?;
        let Some(row) = row else { return Ok(None) };

        let sealed = row
            .get("secret_encrypted")
            .and_then(|s| s.as_str())
            .ok_or_else(|| {
                extenddb_core::error::DynamoDbError::InternalServerError(
                    "access key missing secret_encrypted".into(),
                )
            })?;
        let secret_bytes = crypto::decrypt(&self.encryption_key, sealed).map_err(|e| {
            extenddb_core::error::DynamoDbError::InternalServerError(format!("decrypt: {e}"))
        })?;
        let secret = String::from_utf8(secret_bytes).map_err(|e| {
            extenddb_core::error::DynamoDbError::InternalServerError(format!(
                "secret utf8: {e}"
            ))
        })?;

        let account_id = row.get("account_id").and_then(|s| s.as_str()).unwrap_or("");
        let user_name = row.get("user_name").and_then(|s| s.as_str()).unwrap_or("");
        let is_active = row.get("is_active").and_then(|b| b.as_bool()).unwrap_or(true);
        let is_session = row.get("is_session").and_then(|b| b.as_bool()).unwrap_or(false);
        let session_token = row
            .get("session_token")
            .and_then(|s| s.as_str())
            .map(str::to_owned);
        let session_name = row
            .get("session_name")
            .and_then(|s| s.as_str())
            .map(str::to_owned);
        let expires_at = row
            .get("expires_at")
            .and_then(|v| v.as_str())
            .and_then(|s| {
                time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok()
            });

        Ok(Some(extenddb_auth::StoredCredential {
            secret_key: secret,
            account_id: account_id.to_owned(),
            principal_name: user_name.to_owned(),
            session_name,
            is_session,
            session_token,
            is_active,
            expires_at,
        }))
    }
}
