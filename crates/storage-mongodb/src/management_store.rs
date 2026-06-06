// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ManagementStore` trait implementation for `MongoDB`.

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{self, Binary, DateTime as BsonDateTime, Document, doc};
use mongodb::options::{FindOptions, UpdateOptions};

use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, AdminEntry, GroupDetail, GroupListEntry, ManagementStore,
    MetricsRow, OpError, OpResult, RoleDetail, RoleListEntry, UserDetail, UserListEntry,
};

use crate::catalog_store::MongoCatalogStore;

fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    matches!(
        *e.kind,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
            mongodb::error::WriteError { code: 11000, .. }
        ))
    )
}

fn to_offset_dt(dt: BsonDateTime) -> time::OffsetDateTime {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(dt.timestamp_millis()) * 1_000_000)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
}

fn now_bson() -> BsonDateTime {
    BsonDateTime::now()
}

// ── ManagementStore ─────────────────────────────────────────────────────

impl ManagementStore for MongoCatalogStore {
    fn create_account(&self, account_id: &str, account_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let account_name = account_name.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("accounts");
            let result = coll
                .insert_one(doc! {
                    "account_id": &account_id,
                    "account_name": &account_name,
                    "created_at": now_bson(),
                })
                .await;
            match result {
                Ok(_) => Ok(()),
                Err(e) if is_duplicate_key(&e) => {
                    Err(OpError::AlreadyExists("Account already exists".to_owned()))
                }
                Err(e) => {
                    tracing::error!("create_account failed: {e}");
                    Err(OpError::Internal("Database error".to_owned()))
                }
            }
        })
    }

    fn delete_account(&self, account_id: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let tables_coll = self.catalog_db().collection::<Document>("tables");
            let has_tables = tables_coll
                .count_documents(doc! { "account_id": &account_id })
                .await
                .map_err(|e| {
                    tracing::error!("delete_account check tables: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            if has_tables > 0 {
                return Err(OpError::HasDependents(
                    "Cannot delete account with existing tables. Delete all tables first."
                        .to_owned(),
                ));
            }

            let coll = self.catalog_db().collection::<Document>("accounts");
            let result = coll
                .delete_one(doc! { "account_id": &account_id })
                .await
                .map_err(|e| {
                    tracing::error!("delete_account: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            if result.deleted_count == 0 {
                return Err(OpError::NotFound("Account not found".to_owned()));
            }
            Ok(())
        })
    }

    fn list_all_accounts(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async {
            let coll = self.catalog_db().collection::<Document>("accounts");
            let opts = FindOptions::builder()
                .sort(doc! { "account_id": 1 })
                .build();
            let cursor = coll.find(doc! {}).with_options(opts).await.map_err(|e| {
                tracing::error!("list_all_accounts: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_all_accounts cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("account_id").ok()?.to_owned(),
                        d.get_str("account_name").ok()?.to_owned(),
                    ))
                })
                .collect())
        })
    }

    fn list_all_accounts_full(
        &self,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String, time::OffsetDateTime)>>> {
        Box::pin(async {
            let coll = self.catalog_db().collection::<Document>("accounts");
            let opts = FindOptions::builder()
                .sort(doc! { "account_id": 1 })
                .build();
            let cursor = coll.find(doc! {}).with_options(opts).await.map_err(|e| {
                tracing::error!("list_all_accounts_full: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_all_accounts_full cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("account_id").ok()?.to_owned(),
                        d.get_str("account_name").ok()?.to_owned(),
                        to_offset_dt(d.get_datetime("created_at").ok()?.to_owned()),
                    ))
                })
                .collect())
        })
    }

    fn list_accounts_for(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("accounts");
            let cursor = coll
                .find(doc! { "account_id": &account_id })
                .await
                .map_err(|e| {
                    tracing::error!("list_accounts_for: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_accounts_for cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("account_id").ok()?.to_owned(),
                        d.get_str("account_name").ok()?.to_owned(),
                    ))
                })
                .collect())
        })
    }

    fn get_account_detail(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Option<AccountDetail>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("accounts");
            let acct = coll
                .find_one(doc! { "account_id": &account_id })
                .await
                .map_err(|e| {
                    tracing::error!("get_account_detail: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let Some(acct_doc) = acct else {
                return Ok(None);
            };

            let account_name = acct_doc
                .get_str("account_name")
                .unwrap_or_default()
                .to_owned();

            let users_coll = self.catalog_db().collection::<Document>("iam_users");
            let users_cursor = users_coll
                .find(doc! { "account_id": &account_id })
                .with_options(FindOptions::builder().sort(doc! { "user_name": 1 }).build())
                .await
                .map_err(|e| {
                    tracing::error!("get_account_detail users: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let users: Vec<String> = users_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_account_detail users cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    d.get_str("user_name")
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                })
                .collect();

            let groups_coll = self.catalog_db().collection::<Document>("iam_groups");
            let groups_cursor = groups_coll
                .find(doc! { "account_id": &account_id })
                .with_options(
                    FindOptions::builder()
                        .sort(doc! { "group_name": 1 })
                        .build(),
                )
                .await
                .map_err(|e| {
                    tracing::error!("get_account_detail groups: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let groups: Vec<String> = groups_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_account_detail groups cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    d.get_str("group_name")
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                })
                .collect();

            let roles_coll = self.catalog_db().collection::<Document>("iam_roles");
            let roles_cursor = roles_coll
                .find(doc! { "account_id": &account_id })
                .with_options(FindOptions::builder().sort(doc! { "role_name": 1 }).build())
                .await
                .map_err(|e| {
                    tracing::error!("get_account_detail roles: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let roles: Vec<String> = roles_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_account_detail roles cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    d.get_str("role_name")
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                })
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
        Box::pin(async {
            let accounts_coll = self.catalog_db().collection::<Document>("accounts");
            let account_count = accounts_coll.count_documents(doc! {}).await.map_err(|e| {
                tracing::error!("dashboard_counts accounts: {e}");
                OpError::Internal("Database error".to_owned())
            })? as i64;

            let admins_coll = self.catalog_db().collection::<Document>("admin_users");
            let admin_count = admins_coll.count_documents(doc! {}).await.map_err(|e| {
                tracing::error!("dashboard_counts admins: {e}");
                OpError::Internal("Database error".to_owned())
            })? as i64;

            Ok((account_count, admin_count))
        })
    }

    fn create_user(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: Option<&str>,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password_hash = password_hash.map(std::borrow::ToOwned::to_owned);
        Box::pin(async move {
            let user_arn = format!("arn:aws:iam::{account_id}:user/{user_name}");

            let mut user_doc = doc! {
                "account_id": &account_id,
                "user_name": &user_name,
                "user_arn": &user_arn,
                "created_at": now_bson(),
            };
            if let Some(ref ph) = password_hash {
                user_doc.insert("password_hash", ph.as_str());
            }

            let coll = self.catalog_db().collection::<Document>("iam_users");
            let result = coll.insert_one(user_doc).await;
            match result {
                Ok(_) => {}
                Err(e) if is_duplicate_key(&e) => {
                    return Err(OpError::AlreadyExists("IAM user already exists".to_owned()));
                }
                Err(e) => {
                    tracing::error!("create_user failed: {e}");
                    return Err(OpError::Internal("Database error".to_owned()));
                }
            }

            // Seed default self-service policy.
            let self_service_policy = serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": [
                        "iam:CreateAccessKey",
                        "iam:DeleteAccessKey",
                        "iam:ListAccessKeys",
                        "iam:ChangePassword"
                    ],
                    "Resource": format!("arn:aws:iam::{}:user/{}", account_id, user_name)
                }]
            });

            let policies_coll = self.catalog_db().collection::<Document>("iam_policies");
            let policy_doc = doc! {
                "account_id": &account_id,
                "principal_type": "user",
                "principal_name": &user_name,
                "policy_name": "SelfServicePolicy",
                "policy_document": bson::to_bson(&self_service_policy).unwrap_or_default(),
                "created_at": now_bson(),
            };
            // Use upsert to avoid errors on conflict
            let filter = doc! {
                "account_id": &account_id,
                "principal_type": "user",
                "principal_name": &user_name,
                "policy_name": "SelfServicePolicy",
            };
            let opts = UpdateOptions::builder().upsert(true).build();
            if let Err(e) = policies_coll
                .update_one(filter, doc! { "$setOnInsert": policy_doc })
                .with_options(opts)
                .await
            {
                tracing::error!("seed self-service policy failed: {e}");
                return Err(OpError::Internal("Database error".to_owned()));
            }

            Ok(())
        })
    }

    fn delete_user(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_users");
            let result = coll
                .delete_one(doc! { "account_id": &account_id, "user_name": &user_name })
                .await
                .map_err(|e| {
                    tracing::error!("delete_user failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.deleted_count == 0 {
                return Err(OpError::NotFound("IAM user not found".to_owned()));
            }
            // Cascade: delete access keys, policies, group memberships
            let keys_coll = self.catalog_db().collection::<Document>("access_keys");
            let _ = keys_coll
                .delete_many(doc! { "account_id": &account_id, "user_name": &user_name })
                .await;
            let policies_coll = self.catalog_db().collection::<Document>("iam_policies");
            let _ = policies_coll
                .delete_many(doc! { "account_id": &account_id, "principal_type": "user", "principal_name": &user_name })
                .await;
            let members_coll = self
                .catalog_db()
                .collection::<Document>("iam_group_members");
            let _ = members_coll
                .delete_many(doc! { "account_id": &account_id, "user_name": &user_name })
                .await;
            Ok(())
        })
    }

    fn list_users(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<UserListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_users");
            let opts = FindOptions::builder().sort(doc! { "user_name": 1 }).build();
            let cursor = coll
                .find(doc! { "account_id": &account_id })
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("list_users: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_users cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("account_id").ok()?.to_owned(),
                        d.get_str("user_name").ok()?.to_owned(),
                        d.get_str("user_arn").ok()?.to_owned(),
                        d.get_str("password_hash").is_ok(),
                        to_offset_dt(d.get_datetime("created_at").ok()?.to_owned()),
                    ))
                })
                .collect())
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
            let coll = self.catalog_db().collection::<Document>("iam_users");
            let exists = coll
                .find_one(doc! { "account_id": &account_id, "user_name": &user_name })
                .await
                .map_err(|e| {
                    tracing::error!("get_user_detail exists: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if exists.is_none() {
                return Ok(None);
            }

            let keys_coll = self.catalog_db().collection::<Document>("access_keys");
            let keys_cursor = keys_coll
                .find(doc! { "account_id": &account_id, "user_name": &user_name })
                .with_options(
                    FindOptions::builder()
                        .sort(doc! { "access_key_id": 1 })
                        .build(),
                )
                .await
                .map_err(|e| {
                    tracing::error!("get_user_detail keys: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let keys: Vec<(String, bool)> = keys_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_user_detail keys cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("access_key_id").ok()?.to_owned(),
                        d.get_bool("is_active").unwrap_or(true),
                    ))
                })
                .collect();

            let policies_coll = self.catalog_db().collection::<Document>("iam_policies");
            let policies_cursor = policies_coll
                .find(doc! {
                    "account_id": &account_id,
                    "principal_type": "user",
                    "principal_name": &user_name,
                })
                .with_options(
                    FindOptions::builder()
                        .sort(doc! { "policy_name": 1 })
                        .build(),
                )
                .await
                .map_err(|e| {
                    tracing::error!("get_user_detail policies: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let policies: Vec<String> = policies_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_user_detail policies cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    d.get_str("policy_name")
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                })
                .collect();

            let tags_coll = self.catalog_db().collection::<Document>("iam_user_tags");
            let tags_cursor = tags_coll
                .find(doc! { "account_id": &account_id, "user_name": &user_name })
                .with_options(FindOptions::builder().sort(doc! { "tag_key": 1 }).build())
                .await
                .map_err(|e| {
                    tracing::error!("get_user_detail tags: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let tags: Vec<(String, String)> = tags_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_user_detail tags cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("tag_key").ok()?.to_owned(),
                        d.get_str("tag_value").ok()?.to_owned(),
                    ))
                })
                .collect();

            let members_coll = self
                .catalog_db()
                .collection::<Document>("iam_group_members");
            let groups_cursor = members_coll
                .find(doc! { "account_id": &account_id, "user_name": &user_name })
                .with_options(
                    FindOptions::builder()
                        .sort(doc! { "group_name": 1 })
                        .build(),
                )
                .await
                .map_err(|e| {
                    tracing::error!("get_user_detail groups: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let groups: Vec<String> = groups_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_user_detail groups cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    d.get_str("group_name")
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                })
                .collect();

            Ok(Some(UserDetail {
                keys,
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
            let coll = self.catalog_db().collection::<Document>("iam_users");
            let doc = coll
                .find_one(doc! { "account_id": &account_id, "user_name": &user_name })
                .await
                .map_err(|e| {
                    tracing::error!("verify_iam_user_password: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let Some(user_doc) = doc else {
                return Ok(false);
            };

            let Some(hash) = user_doc.get_str("password_hash").ok() else {
                return Ok(false);
            };

            let hash = hash.to_owned();
            Ok(tokio::task::spawn_blocking(move || {
                bcrypt::verify(password, &hash).unwrap_or(false)
            })
            .await
            .unwrap_or(false))
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
            let coll = self.catalog_db().collection::<Document>("iam_users");
            let result = coll
                .update_one(
                    doc! { "account_id": &account_id, "user_name": &user_name },
                    doc! { "$set": { "password_hash": &password_hash } },
                )
                .await
                .map_err(|e| {
                    tracing::error!("change_user_password failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.matched_count == 0 {
                return Err(OpError::NotFound("IAM user not found".to_owned()));
            }
            Ok(())
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
            let coll = self.catalog_db().collection::<Document>("iam_user_tags");
            for (key, value) in &tags {
                let filter = doc! {
                    "account_id": &account_id,
                    "user_name": &user_name,
                    "tag_key": key,
                };
                let update = doc! {
                    "$set": {
                        "account_id": &account_id,
                        "user_name": &user_name,
                        "tag_key": key,
                        "tag_value": value,
                    }
                };
                let opts = UpdateOptions::builder().upsert(true).build();
                coll.update_one(filter, update)
                    .with_options(opts)
                    .await
                    .map_err(|e| {
                        tracing::error!("tag_user failed: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
            }
            Ok(())
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
            let coll = self.catalog_db().collection::<Document>("iam_user_tags");
            for key in &tag_keys {
                coll.delete_one(doc! {
                    "account_id": &account_id,
                    "user_name": &user_name,
                    "tag_key": key,
                })
                .await
                .map_err(|e| {
                    tracing::error!("untag_user failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            }
            Ok(())
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
            let coll = self.catalog_db().collection::<Document>("iam_user_tags");
            let opts = FindOptions::builder().sort(doc! { "tag_key": 1 }).build();
            let cursor = coll
                .find(doc! { "account_id": &account_id, "user_name": &user_name })
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("list_user_tags: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_user_tags cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("tag_key").ok()?.to_owned(),
                        d.get_str("tag_value").ok()?.to_owned(),
                    ))
                })
                .collect())
        })
    }

    fn create_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move {
            let group_arn = format!("arn:aws:iam::{account_id}:group/{group_name}");
            let coll = self.catalog_db().collection::<Document>("iam_groups");
            let result = coll
                .insert_one(doc! {
                    "account_id": &account_id,
                    "group_name": &group_name,
                    "group_arn": &group_arn,
                    "created_at": now_bson(),
                })
                .await;
            match result {
                Ok(_) => Ok(()),
                Err(e) if is_duplicate_key(&e) => Err(OpError::AlreadyExists(
                    "IAM group already exists".to_owned(),
                )),
                Err(e) => {
                    tracing::error!("create_group failed: {e}");
                    Err(OpError::Internal("Database error".to_owned()))
                }
            }
        })
    }

    fn delete_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_groups");
            let result = coll
                .delete_one(doc! { "account_id": &account_id, "group_name": &group_name })
                .await
                .map_err(|e| {
                    tracing::error!("delete_group failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.deleted_count == 0 {
                return Err(OpError::NotFound("IAM group not found".to_owned()));
            }
            Ok(())
        })
    }

    fn list_groups(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<GroupListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_groups");
            let opts = FindOptions::builder()
                .sort(doc! { "group_name": 1 })
                .build();
            let cursor = coll
                .find(doc! { "account_id": &account_id })
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("list_groups: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_groups cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("account_id").ok()?.to_owned(),
                        d.get_str("group_name").ok()?.to_owned(),
                        d.get_str("group_arn").ok()?.to_owned(),
                        to_offset_dt(d.get_datetime("created_at").ok()?.to_owned()),
                    ))
                })
                .collect())
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
            let coll = self.catalog_db().collection::<Document>("iam_groups");
            let exists = coll
                .find_one(doc! { "account_id": &account_id, "group_name": &group_name })
                .await
                .map_err(|e| {
                    tracing::error!("get_group_detail exists: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if exists.is_none() {
                return Ok(None);
            }

            let members_coll = self
                .catalog_db()
                .collection::<Document>("iam_group_members");
            let members_cursor = members_coll
                .find(doc! { "account_id": &account_id, "group_name": &group_name })
                .with_options(FindOptions::builder().sort(doc! { "user_name": 1 }).build())
                .await
                .map_err(|e| {
                    tracing::error!("get_group_detail members: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let members: Vec<String> = members_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_group_detail members cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    d.get_str("user_name")
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                })
                .collect();

            let policies_coll = self.catalog_db().collection::<Document>("iam_policies");
            let policies_cursor = policies_coll
                .find(doc! {
                    "account_id": &account_id,
                    "principal_type": "group",
                    "principal_name": &group_name,
                })
                .with_options(
                    FindOptions::builder()
                        .sort(doc! { "policy_name": 1 })
                        .build(),
                )
                .await
                .map_err(|e| {
                    tracing::error!("get_group_detail policies: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let policies: Vec<String> = policies_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_group_detail policies cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    d.get_str("policy_name")
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                })
                .collect();

            let users_coll = self.catalog_db().collection::<Document>("iam_users");
            let all_users_cursor = users_coll
                .find(doc! { "account_id": &account_id })
                .with_options(FindOptions::builder().sort(doc! { "user_name": 1 }).build())
                .await
                .map_err(|e| {
                    tracing::error!("get_group_detail all_users: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let all_users: Vec<String> = all_users_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_group_detail all_users cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    d.get_str("user_name")
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                })
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
            let coll = self
                .catalog_db()
                .collection::<Document>("iam_group_members");
            let result = coll
                .insert_one(doc! {
                    "account_id": &account_id,
                    "group_name": &group_name,
                    "user_name": &user_name,
                })
                .await;
            match result {
                Ok(_) => Ok(()),
                Err(e) if is_duplicate_key(&e) => Err(OpError::AlreadyExists(
                    "User is already a member of this group".to_owned(),
                )),
                Err(e) => {
                    tracing::error!("add_group_member failed: {e}");
                    Err(OpError::Internal("Database error".to_owned()))
                }
            }
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
            let coll = self
                .catalog_db()
                .collection::<Document>("iam_group_members");
            let result = coll
                .delete_one(doc! {
                    "account_id": &account_id,
                    "group_name": &group_name,
                    "user_name": &user_name,
                })
                .await
                .map_err(|e| {
                    tracing::error!("remove_group_member failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.deleted_count == 0 {
                return Err(OpError::NotFound("Membership not found".to_owned()));
            }
            Ok(())
        })
    }

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
            let role_arn = format!("arn:aws:iam::{account_id}:role/{role_name}");
            let coll = self.catalog_db().collection::<Document>("iam_roles");
            let result = coll
                .insert_one(doc! {
                    "account_id": &account_id,
                    "role_name": &role_name,
                    "role_arn": &role_arn,
                    "trust_policy": bson::to_bson(&trust_policy).unwrap_or_default(),
                    "created_at": now_bson(),
                })
                .await;
            match result {
                Ok(_) => Ok(()),
                Err(e) if is_duplicate_key(&e) => {
                    Err(OpError::AlreadyExists("IAM role already exists".to_owned()))
                }
                Err(e) => {
                    tracing::error!("create_role failed: {e}");
                    Err(OpError::Internal("Database error".to_owned()))
                }
            }
        })
    }

    fn delete_role(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_roles");
            let result = coll
                .delete_one(doc! { "account_id": &account_id, "role_name": &role_name })
                .await
                .map_err(|e| {
                    tracing::error!("delete_role failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.deleted_count == 0 {
                return Err(OpError::NotFound("IAM role not found".to_owned()));
            }
            Ok(())
        })
    }

    fn list_roles(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<RoleListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_roles");
            let opts = FindOptions::builder().sort(doc! { "role_name": 1 }).build();
            let cursor = coll
                .find(doc! { "account_id": &account_id })
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("list_roles: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_roles cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    let tp_bson = d.get("trust_policy")?;
                    let trust_policy: serde_json::Value = bson::from_bson(tp_bson.clone()).ok()?;
                    Some((
                        d.get_str("account_id").ok()?.to_owned(),
                        d.get_str("role_name").ok()?.to_owned(),
                        d.get_str("role_arn").ok()?.to_owned(),
                        trust_policy,
                        to_offset_dt(d.get_datetime("created_at").ok()?.to_owned()),
                    ))
                })
                .collect())
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
            let coll = self.catalog_db().collection::<Document>("iam_roles");
            let role_doc = coll
                .find_one(doc! { "account_id": &account_id, "role_name": &role_name })
                .await
                .map_err(|e| {
                    tracing::error!("get_role_detail role: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let Some(role_doc) = role_doc else {
                return Ok(None);
            };

            let trust_policy: serde_json::Value = role_doc
                .get("trust_policy")
                .and_then(|b| bson::from_bson(b.clone()).ok())
                .unwrap_or(serde_json::Value::Null);

            let policies_coll = self.catalog_db().collection::<Document>("iam_policies");
            let policies_cursor = policies_coll
                .find(doc! {
                    "account_id": &account_id,
                    "principal_type": "role",
                    "principal_name": &role_name,
                })
                .with_options(
                    FindOptions::builder()
                        .sort(doc! { "policy_name": 1 })
                        .build(),
                )
                .await
                .map_err(|e| {
                    tracing::error!("get_role_detail policies: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let policies: Vec<String> = policies_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_role_detail policies cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    d.get_str("policy_name")
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                })
                .collect();

            let tags_coll = self.catalog_db().collection::<Document>("iam_role_tags");
            let tags_cursor = tags_coll
                .find(doc! { "account_id": &account_id, "role_name": &role_name })
                .with_options(FindOptions::builder().sort(doc! { "tag_key": 1 }).build())
                .await
                .map_err(|e| {
                    tracing::error!("get_role_detail tags: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let tags: Vec<(String, String)> = tags_cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|e| {
                    tracing::error!("get_role_detail tags cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("tag_key").ok()?.to_owned(),
                        d.get_str("tag_value").ok()?.to_owned(),
                    ))
                })
                .collect();

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
            let coll = self.catalog_db().collection::<Document>("iam_roles");
            let doc = coll
                .find_one(doc! { "account_id": &account_id, "role_name": &role_name })
                .await
                .map_err(|e| {
                    tracing::error!("get_role_trust_policy: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(doc.and_then(|d| {
                d.get("trust_policy")
                    .and_then(|b| bson::from_bson(b.clone()).ok())
            }))
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
            let coll = self.catalog_db().collection::<Document>("iam_role_tags");
            for (key, value) in &tags {
                let filter = doc! {
                    "account_id": &account_id,
                    "role_name": &role_name,
                    "tag_key": key,
                };
                let update = doc! {
                    "$set": {
                        "account_id": &account_id,
                        "role_name": &role_name,
                        "tag_key": key,
                        "tag_value": value,
                    }
                };
                let opts = UpdateOptions::builder().upsert(true).build();
                coll.update_one(filter, update)
                    .with_options(opts)
                    .await
                    .map_err(|e| {
                        tracing::error!("tag_role failed: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
            }
            Ok(())
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
            let coll = self.catalog_db().collection::<Document>("iam_role_tags");
            for key in &tag_keys {
                coll.delete_one(doc! {
                    "account_id": &account_id,
                    "role_name": &role_name,
                    "tag_key": key,
                })
                .await
                .map_err(|e| {
                    tracing::error!("untag_role failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            }
            Ok(())
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
            let coll = self.catalog_db().collection::<Document>("iam_role_tags");
            let opts = FindOptions::builder().sort(doc! { "tag_key": 1 }).build();
            let cursor = coll
                .find(doc! { "account_id": &account_id, "role_name": &role_name })
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("list_role_tags: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_role_tags cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("tag_key").ok()?.to_owned(),
                        d.get_str("tag_value").ok()?.to_owned(),
                    ))
                })
                .collect())
        })
    }

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
            let coll = self.catalog_db().collection::<Document>("iam_policies");
            let filter = doc! {
                "account_id": &account_id,
                "principal_type": &principal_type,
                "principal_name": &principal_name,
                "policy_name": &policy_name,
            };
            let update = doc! {
                "$set": {
                    "account_id": &account_id,
                    "principal_type": &principal_type,
                    "principal_name": &principal_name,
                    "policy_name": &policy_name,
                    "policy_document": bson::to_bson(&document).unwrap_or_default(),
                },
                "$setOnInsert": {
                    "created_at": now_bson(),
                }
            };
            let opts = UpdateOptions::builder().upsert(true).build();
            coll.update_one(filter, update)
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("put_policy failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(())
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
            let coll = self.catalog_db().collection::<Document>("iam_policies");
            let result = coll
                .delete_one(doc! {
                    "account_id": &account_id,
                    "principal_type": &principal_type,
                    "principal_name": &principal_name,
                    "policy_name": &policy_name,
                })
                .await
                .map_err(|e| {
                    tracing::error!("delete_policy failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.deleted_count == 0 {
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
    ) -> BoxFuture<'_, OpResult<Vec<(String, serde_json::Value, time::OffsetDateTime)>>> {
        let account_id = account_id.to_owned();
        let principal_type = principal_type.to_owned();
        let principal_name = principal_name.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_policies");
            let opts = FindOptions::builder()
                .sort(doc! { "policy_name": 1 })
                .build();
            let cursor = coll
                .find(doc! {
                    "account_id": &account_id,
                    "principal_type": &principal_type,
                    "principal_name": &principal_name,
                })
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("list_policies: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_policies cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    let policy_name = d.get_str("policy_name").ok()?.to_owned();
                    let policy_document: serde_json::Value = d
                        .get("policy_document")
                        .and_then(|b| bson::from_bson(b.clone()).ok())?;
                    let created_at = to_offset_dt(d.get_datetime("created_at").ok()?.to_owned());
                    Some((policy_name, policy_document, created_at))
                })
                .collect())
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
            let coll = self
                .catalog_db()
                .collection::<Document>("iam_permissions_boundaries");
            let filter = doc! {
                "account_id": &account_id,
                "principal_type": "user",
                "principal_name": &user_name,
            };
            let update = doc! {
                "$set": {
                    "account_id": &account_id,
                    "principal_type": "user",
                    "principal_name": &user_name,
                    "policy_document": bson::to_bson(&document).unwrap_or_default(),
                }
            };
            let opts = UpdateOptions::builder().upsert(true).build();
            coll.update_one(filter, update)
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("set_user_boundary failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(())
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
            let coll = self
                .catalog_db()
                .collection::<Document>("iam_permissions_boundaries");
            let doc = coll
                .find_one(doc! {
                    "account_id": &account_id,
                    "principal_type": "user",
                    "principal_name": &user_name,
                })
                .await
                .map_err(|e| {
                    tracing::error!("get_user_boundary: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(doc.and_then(|d| {
                d.get("policy_document")
                    .and_then(|b| bson::from_bson(b.clone()).ok())
            }))
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
            let coll = self
                .catalog_db()
                .collection::<Document>("iam_permissions_boundaries");
            let result = coll
                .delete_one(doc! {
                    "account_id": &account_id,
                    "principal_type": "user",
                    "principal_name": &user_name,
                })
                .await
                .map_err(|e| {
                    tracing::error!("delete_user_boundary failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.deleted_count == 0 {
                return Err(OpError::NotFound("Permissions boundary not set".to_owned()));
            }
            Ok(())
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
            let coll = self
                .catalog_db()
                .collection::<Document>("iam_permissions_boundaries");
            let filter = doc! {
                "account_id": &account_id,
                "principal_type": "role",
                "principal_name": &role_name,
            };
            let update = doc! {
                "$set": {
                    "account_id": &account_id,
                    "principal_type": "role",
                    "principal_name": &role_name,
                    "policy_document": bson::to_bson(&document).unwrap_or_default(),
                }
            };
            let opts = UpdateOptions::builder().upsert(true).build();
            coll.update_one(filter, update)
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("set_role_boundary failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(())
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
            let coll = self
                .catalog_db()
                .collection::<Document>("iam_permissions_boundaries");
            let doc = coll
                .find_one(doc! {
                    "account_id": &account_id,
                    "principal_type": "role",
                    "principal_name": &role_name,
                })
                .await
                .map_err(|e| {
                    tracing::error!("get_role_boundary: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(doc.and_then(|d| {
                d.get("policy_document")
                    .and_then(|b| bson::from_bson(b.clone()).ok())
            }))
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
            let coll = self
                .catalog_db()
                .collection::<Document>("iam_permissions_boundaries");
            let result = coll
                .delete_one(doc! {
                    "account_id": &account_id,
                    "principal_type": "role",
                    "principal_name": &role_name,
                })
                .await
                .map_err(|e| {
                    tracing::error!("delete_role_boundary failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.deleted_count == 0 {
                return Err(OpError::NotFound("Permissions boundary not set".to_owned()));
            }
            Ok(())
        })
    }

    fn create_access_key(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<AccessKeyCreated>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            // Check user exists (MongoDB has no FK constraints)
            let users_coll = self.catalog_db().collection::<Document>("iam_users");
            let user_exists = users_coll
                .find_one(doc! { "account_id": &account_id, "user_name": &user_name })
                .await
                .map_err(|e| {
                    tracing::error!("create_access_key user check: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if user_exists.is_none() {
                return Err(OpError::NotFound("User not found".to_owned()));
            }

            let enc_key = self.get_encryption_key().await?;

            let access_key_id = generate_access_key_id();
            let secret_key = generate_secret_key();
            let encrypted = encrypt_secret(&secret_key, &enc_key, &access_key_id).map_err(|e| {
                tracing::error!("create_access_key encryption: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let coll = self.catalog_db().collection::<Document>("access_keys");
            coll.insert_one(doc! {
                "access_key_id": &access_key_id,
                "account_id": &account_id,
                "user_name": &user_name,
                "secret_key_encrypted": Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: encrypted },
                "is_active": true,
                "created_at": now_bson(),
            })
            .await
            .map_err(|e| {
                if is_duplicate_key(&e) {
                    OpError::NotFound("User not found".to_owned())
                } else {
                    tracing::error!("create_access_key failed: {e}");
                    OpError::Internal("Database error".to_owned())
                }
            })?;

            Ok(AccessKeyCreated {
                access_key_id,
                secret_access_key: secret_key,
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
            let coll = self.catalog_db().collection::<Document>("access_keys");
            let result = coll
                .delete_one(doc! {
                    "access_key_id": &key_id,
                    "account_id": &account_id,
                    "user_name": &user_name,
                })
                .await
                .map_err(|e| {
                    tracing::error!("delete_access_key failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.deleted_count == 0 {
                return Err(OpError::NotFound("Access key not found".to_owned()));
            }
            Ok(())
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
            let coll = self.catalog_db().collection::<Document>("access_keys");
            let opts = FindOptions::builder()
                .sort(doc! { "created_at": 1 })
                .build();
            let cursor = coll
                .find(doc! { "account_id": &account_id, "user_name": &user_name })
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("list_access_keys: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_access_keys cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("access_key_id").ok()?.to_owned(),
                        d.get_bool("is_active").unwrap_or(true),
                        to_offset_dt(d.get_datetime("created_at").ok()?.to_owned()),
                    ))
                })
                .collect())
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
            let enc_key = self.get_encryption_key().await?;

            let encrypted =
                encrypt_secret(&secret_access_key, &enc_key, &access_key_id).map_err(|e| {
                    tracing::error!("import_access_key encryption: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let coll = self.catalog_db().collection::<Document>("access_keys");
            let result = coll
                .insert_one(doc! {
                    "access_key_id": &access_key_id,
                    "account_id": &account_id,
                    "user_name": &user_name,
                    "secret_key_encrypted": Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: encrypted },
                    "is_active": true,
                    "created_at": now_bson(),
                })
                .await;
            match result {
                Ok(_) => Ok(()),
                Err(e) if is_duplicate_key(&e) => Err(OpError::AlreadyExists(
                    "Access key ID already exists".to_owned(),
                )),
                Err(e) => {
                    tracing::error!("import_access_key failed: {e}");
                    Err(OpError::Internal("Database error".to_owned()))
                }
            }
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
        expires_at: time::OffsetDateTime,
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
            let coll = self.catalog_db().collection::<Document>("iam_sessions");
            let expires_bson = BsonDateTime::from_millis(expires_at.unix_timestamp() * 1000);

            let mut session_doc = doc! {
                "session_token": &session_token,
                "access_key_id": &access_key_id,
                "secret_key_encrypted": Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: secret_key_encrypted },
                "account_id": &account_id,
                "role_name": &role_name,
                "session_name": &session_name,
                "expires_at": expires_bson,
            };

            if let Some(ref tags) = session_tags {
                session_doc.insert("session_tags", bson::to_bson(tags).unwrap_or_default());
            }
            if let Some(ref policy) = session_policy {
                session_doc.insert("session_policy", bson::to_bson(policy).unwrap_or_default());
            }

            coll.insert_one(session_doc).await.map_err(|e| {
                tracing::error!("store_session failed: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(())
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
            if let Some(user_name) = resource.strip_prefix("user/") {
                let coll = self.catalog_db().collection::<Document>("iam_user_tags");
                let cursor = coll
                    .find(doc! { "account_id": &account_id, "user_name": user_name })
                    .await
                    .map_err(|e| {
                        tracing::error!("fetch_caller_tags user: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
                let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                    tracing::error!("fetch_caller_tags user cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                Ok(docs
                    .into_iter()
                    .filter_map(|d| {
                        Some((
                            d.get_str("tag_key").ok()?.to_owned(),
                            d.get_str("tag_value").ok()?.to_owned(),
                        ))
                    })
                    .collect())
            } else if let Some(role_name) = resource.strip_prefix("role/") {
                let coll = self.catalog_db().collection::<Document>("iam_role_tags");
                let cursor = coll
                    .find(doc! { "account_id": &account_id, "role_name": role_name })
                    .await
                    .map_err(|e| {
                        tracing::error!("fetch_caller_tags role: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
                let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                    tracing::error!("fetch_caller_tags role cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                Ok(docs
                    .into_iter()
                    .filter_map(|d| {
                        Some((
                            d.get_str("tag_key").ok()?.to_owned(),
                            d.get_str("tag_value").ok()?.to_owned(),
                        ))
                    })
                    .collect())
            } else if let Some(rest) = resource.strip_prefix("assumed-role/") {
                let role_name = rest.split('/').next().unwrap_or("");
                if role_name.is_empty() {
                    return Ok(Vec::new());
                }
                let coll = self.catalog_db().collection::<Document>("iam_role_tags");
                let cursor = coll
                    .find(doc! { "account_id": &account_id, "role_name": role_name })
                    .await
                    .map_err(|e| {
                        tracing::error!("fetch_caller_tags assumed-role: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
                let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                    tracing::error!("fetch_caller_tags assumed-role cursor: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                Ok(docs
                    .into_iter()
                    .filter_map(|d| {
                        Some((
                            d.get_str("tag_key").ok()?.to_owned(),
                            d.get_str("tag_value").ok()?.to_owned(),
                        ))
                    })
                    .collect())
            } else {
                Ok(Vec::new())
            }
        })
    }
}

// ── SettingsStore ───────────────────────────────────────────────────────

impl extenddb_storage::management_store::SettingsStore for MongoCatalogStore {
    fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let key = key.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("settings");
            let doc = coll.find_one(doc! { "_id": &key }).await.map_err(|e| {
                tracing::error!("get_setting: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(doc.and_then(|d| d.get_str("value").ok().map(std::borrow::ToOwned::to_owned)))
        })
    }

    fn set_setting(&self, key: &str, value: &str) -> BoxFuture<'_, OpResult<()>> {
        let key = key.to_owned();
        let value = value.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("settings");
            let filter = doc! { "_id": &key };
            let update = doc! { "$set": { "value": &value } };
            let opts = UpdateOptions::builder().upsert(true).build();
            coll.update_one(filter, update)
                .with_options(opts)
                .await
                .map_err(|e| {
                    tracing::error!("set_setting failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(())
        })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async {
            let coll = self.catalog_db().collection::<Document>("settings");
            let opts = FindOptions::builder().sort(doc! { "_id": 1 }).build();
            let cursor = coll.find(doc! {}).with_options(opts).await.map_err(|e| {
                tracing::error!("list_settings: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_settings cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some((
                        d.get_str("_id").ok()?.to_owned(),
                        d.get_str("value").ok()?.to_owned(),
                    ))
                })
                .collect())
        })
    }

    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.clone()
    }
}

// ── MetricsStore ────────────────────────────────────────────────────────

impl extenddb_storage::management_store::MetricsStore for MongoCatalogStore {
    fn insert_metrics(&self, rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        let rows = rows.to_vec();
        Box::pin(async move {
            if rows.is_empty() {
                return Ok(());
            }
            let coll = self.catalog_db().collection::<Document>("metrics");
            let docs: Vec<Document> = rows
                .into_iter()
                .map(|r| {
                    let mut d = doc! {
                        "bucket": BsonDateTime::from_millis(r.bucket.unix_timestamp() * 1000),
                        "metric": &r.metric,
                        "sum": r.sum,
                        "count": r.count,
                        "min": r.min,
                        "max": r.max,
                    };
                    if let Some(ref tn) = r.table_name {
                        d.insert("table_name", tn.as_str());
                    }
                    if let Some(ref idx) = r.index_name {
                        d.insert("index_name", idx.as_str());
                    }
                    if let Some(ref op) = r.operation {
                        d.insert("operation", op.as_str());
                    }
                    d
                })
                .collect();
            coll.insert_many(docs).await.map_err(|e| {
                tracing::error!("insert_metrics failed: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(())
        })
    }

    fn query_metrics(
        &self,
        start: time::OffsetDateTime,
        end: time::OffsetDateTime,
        table_name: Option<&str>,
        metric: Option<&str>,
    ) -> BoxFuture<'_, OpResult<Vec<MetricsRow>>> {
        let table_name = table_name.map(std::borrow::ToOwned::to_owned);
        let metric = metric.map(std::borrow::ToOwned::to_owned);
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("metrics");
            let start_bson = BsonDateTime::from_millis(start.unix_timestamp() * 1000);
            let end_bson = BsonDateTime::from_millis(end.unix_timestamp() * 1000);

            let mut filter = doc! {
                "bucket": { "$gte": start_bson, "$lte": end_bson }
            };
            if let Some(ref tn) = table_name {
                filter.insert("table_name", tn.as_str());
            }
            if let Some(ref m) = metric {
                filter.insert("metric", m.as_str());
            }

            let cursor = coll.find(filter).await.map_err(|e| {
                tracing::error!("query_metrics: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("query_metrics cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some(MetricsRow {
                        bucket: to_offset_dt(d.get_datetime("bucket").ok()?.to_owned()),
                        metric: d.get_str("metric").ok()?.to_owned(),
                        table_name: d
                            .get_str("table_name")
                            .ok()
                            .map(std::borrow::ToOwned::to_owned),
                        index_name: d
                            .get_str("index_name")
                            .ok()
                            .map(std::borrow::ToOwned::to_owned),
                        operation: d
                            .get_str("operation")
                            .ok()
                            .map(std::borrow::ToOwned::to_owned),
                        sum: d.get_f64("sum").ok()?,
                        count: d
                            .get_i64("count")
                            .ok()
                            .or_else(|| d.get_i32("count").ok().map(i64::from))?,
                        min: d.get_f64("min").ok()?,
                        max: d.get_f64("max").ok()?,
                    })
                })
                .collect())
        })
    }

    fn prune_metrics(&self, retention: std::time::Duration) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("metrics");
            let cutoff = time::OffsetDateTime::now_utc() - retention;
            let cutoff_bson = BsonDateTime::from_millis(cutoff.unix_timestamp() * 1000);
            coll.delete_many(doc! { "bucket": { "$lt": cutoff_bson } })
                .await
                .map_err(|e| {
                    tracing::error!("prune_metrics failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(())
        })
    }
}

// ── RateLimitStore ──────────────────────────────────────────────────────

impl extenddb_storage::management_store::RateLimitStore for MongoCatalogStore {
    fn count_principal_failures(
        &self,
        principal: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let principal = principal.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("failed_logins");
            let cutoff = time::OffsetDateTime::now_utc()
                - std::time::Duration::from_secs(window_seconds as u64);
            let cutoff_bson = BsonDateTime::from_millis(cutoff.unix_timestamp() * 1000);
            let count = coll
                .count_documents(doc! {
                    "principal": &principal,
                    "attempted_at": { "$gte": cutoff_bson },
                })
                .await
                .map_err(|e| {
                    tracing::error!("count_principal_failures: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
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
            let coll = self.catalog_db().collection::<Document>("failed_logins");
            let cutoff = time::OffsetDateTime::now_utc()
                - std::time::Duration::from_secs(window_seconds as u64);
            let cutoff_bson = BsonDateTime::from_millis(cutoff.unix_timestamp() * 1000);
            let count = coll
                .count_documents(doc! {
                    "source_ip": &source_ip,
                    "attempted_at": { "$gte": cutoff_bson },
                })
                .await
                .map_err(|e| {
                    tracing::error!("count_ip_failures: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(count as i64)
        })
    }

    fn record_failed_login(&self, principal: &str, source_ip: Option<&str>) -> BoxFuture<'_, ()> {
        let principal = principal.to_owned();
        let source_ip = source_ip.map(std::borrow::ToOwned::to_owned);
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("failed_logins");
            let mut login_doc = doc! {
                "principal": &principal,
                "attempted_at": now_bson(),
            };
            if let Some(ref ip) = source_ip {
                login_doc.insert("source_ip", ip.as_str());
            }
            if let Err(e) = coll.insert_one(login_doc).await {
                tracing::error!("record_failed_login: {e}");
            }
        })
    }

    fn cleanup_old_attempts(&self, max_age_seconds: i64) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("failed_logins");
            let cutoff = time::OffsetDateTime::now_utc()
                - std::time::Duration::from_secs(max_age_seconds as u64);
            let cutoff_bson = BsonDateTime::from_millis(cutoff.unix_timestamp() * 1000);
            if let Err(e) = coll
                .delete_many(doc! { "attempted_at": { "$lt": cutoff_bson } })
                .await
            {
                tracing::error!("cleanup_old_attempts: {e}");
            }
        })
    }
}

// ── AdminStore ──────────────────────────────────────────────────────────

impl extenddb_storage::management_store::AdminStore for MongoCatalogStore {
    fn create_admin(&self, admin_name: &str, password_hash: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let password_hash = password_hash.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("admin_users");
            let result = coll
                .insert_one(doc! {
                    "_id": &admin_name,
                    "password_hash": &password_hash,
                    "created_at": now_bson(),
                })
                .await;
            match result {
                Ok(_) => Ok(()),
                Err(e) if is_duplicate_key(&e) => Err(OpError::AlreadyExists(
                    "Admin user already exists".to_owned(),
                )),
                Err(e) => {
                    tracing::error!("create_admin failed: {e}");
                    Err(OpError::Internal("Database error".to_owned()))
                }
            }
        })
    }

    fn list_admins(&self) -> BoxFuture<'_, OpResult<Vec<AdminEntry>>> {
        Box::pin(async {
            let coll = self.catalog_db().collection::<Document>("admin_users");
            let opts = FindOptions::builder().sort(doc! { "_id": 1 }).build();
            let cursor = coll.find(doc! {}).with_options(opts).await.map_err(|e| {
                tracing::error!("list_admins: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("list_admins cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    Some(AdminEntry {
                        admin_name: d.get_str("_id").ok()?.to_owned(),
                        created_at: to_offset_dt(d.get_datetime("created_at").ok()?.to_owned()),
                    })
                })
                .collect())
        })
    }

    fn delete_admin(&self, admin_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("admin_users");
            let result = coll
                .delete_one(doc! { "_id": &admin_name })
                .await
                .map_err(|e| {
                    tracing::error!("delete_admin failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.deleted_count == 0 {
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
            let coll = self.catalog_db().collection::<Document>("admin_users");
            let result = coll
                .update_one(
                    doc! { "_id": &admin_name },
                    doc! { "$set": { "password_hash": &password_hash } },
                )
                .await
                .map_err(|e| {
                    tracing::error!("change_admin_password failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.matched_count == 0 {
                return Err(OpError::NotFound("Admin user not found".to_owned()));
            }
            Ok(())
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
            let coll = self.catalog_db().collection::<Document>("admin_users");
            let doc = coll
                .find_one(doc! { "_id": &admin_name })
                .await
                .map_err(|e| {
                    tracing::error!("verify_admin_password: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let Some(admin_doc) = doc else {
                return Ok(None);
            };
            let Some(hash) = admin_doc.get_str("password_hash").ok() else {
                return Ok(None);
            };
            let hash = hash.to_owned();
            Ok(Some(
                tokio::task::spawn_blocking(move || {
                    bcrypt::verify(password, &hash).unwrap_or(false)
                })
                .await
                .unwrap_or(false),
            ))
        })
    }
}

// ── Helper: encryption key retrieval ────────────────────────────────────

impl MongoCatalogStore {
    async fn get_encryption_key(&self) -> OpResult<String> {
        if let Some(ref cached) = self.encryption_key {
            return Ok(cached.clone());
        }
        let coll = self.catalog_db().collection::<Document>("settings");
        let doc = coll
            .find_one(doc! { "_id": "encryption_key" })
            .await
            .map_err(|e| {
                tracing::error!("get_encryption_key: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
        doc.and_then(|d| d.get_str("value").ok().map(std::borrow::ToOwned::to_owned))
            .ok_or_else(|| OpError::Internal("Encryption key not configured".to_owned()))
    }
}

// ── Crypto helpers ──────────────────────────────────────────────────────

fn generate_access_key_id() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let suffix: String = (0..8)
        .map(|_| CHARSET[rand::Rng::random_range(&mut rng, 0..CHARSET.len())] as char)
        .collect();
    format!("AKIAEXTENDDB{suffix}")
}

fn generate_secret_key() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::rng();
    let suffix: String = (0..32)
        .map(|_| CHARSET[rand::Rng::random_range(&mut rng, 0..CHARSET.len())] as char)
        .collect();
    format!("extenddb{suffix}")
}

fn encrypt_secret(plaintext: &str, key_b64: &str, aad: &str) -> Result<Vec<u8>, String> {
    use aes_gcm::Aes256Gcm;
    use aes_gcm::KeyInit;
    use aes_gcm::aead::Aead;
    use aes_gcm::aead::Payload;
    use base64::Engine;

    let key_bytes = base64::engine::general_purpose::STANDARD
        .decode(key_b64)
        .map_err(|e| format!("decode encryption key: {e}"))?;

    let key = aes_gcm::Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    let nonce_bytes: [u8; 12] = rand::random();
    let nonce = aes_gcm::Nonce::from_slice(&nonce_bytes);

    let payload = Payload {
        msg: plaintext.as_bytes(),
        aad: aad.as_bytes(),
    };
    let ciphertext = cipher
        .encrypt(nonce, payload)
        .map_err(|e| format!("encrypt: {e}"))?;

    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}
