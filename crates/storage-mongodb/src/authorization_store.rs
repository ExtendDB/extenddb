// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AuthorizationStore` trait implementation for `MongoDB`.

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{self, Document, doc};
use mongodb::options::FindOptions;

use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::management_store::{OpError, OpResult};

use crate::catalog_store::MongoCatalogStore;

impl AuthorizationStore for MongoCatalogStore {
    fn fetch_user_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_policies");
            let cursor = coll
                .find(doc! {
                    "account_id": &account_id,
                    "principal_type": "user",
                    "principal_name": &user_name,
                })
                .await
                .map_err(|e| {
                    tracing::error!("fetch_user_policies: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("fetch_user_policies cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    let bson_val = d.get("policy_document")?;
                    let json_val: serde_json::Value = bson::from_bson(bson_val.clone()).ok()?;
                    Some(json_val.to_string())
                })
                .collect())
        })
    }

    fn fetch_user_group_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            // First get the groups this user belongs to
            let members_coll = self
                .catalog_db()
                .collection::<Document>("iam_group_members");
            let members_cursor = members_coll
                .find(doc! { "account_id": &account_id, "user_name": &user_name })
                .await
                .map_err(|e| {
                    tracing::error!("fetch_user_group_policies members: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let member_docs: Vec<Document> = members_cursor.try_collect().await.map_err(|e| {
                tracing::error!("fetch_user_group_policies members cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let group_names: Vec<&str> = member_docs
                .iter()
                .filter_map(|d| d.get_str("group_name").ok())
                .collect();

            if group_names.is_empty() {
                return Ok(Vec::new());
            }

            // Now get all policies for those groups
            let policies_coll = self.catalog_db().collection::<Document>("iam_policies");
            let cursor = policies_coll
                .find(doc! {
                    "account_id": &account_id,
                    "principal_type": "group",
                    "principal_name": { "$in": &group_names },
                })
                .await
                .map_err(|e| {
                    tracing::error!("fetch_user_group_policies policies: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("fetch_user_group_policies policies cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    let bson_val = d.get("policy_document")?;
                    let json_val: serde_json::Value = bson::from_bson(bson_val.clone()).ok()?;
                    Some(json_val.to_string())
                })
                .collect())
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
                    tracing::error!("fetch_user_boundary: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(doc.and_then(|d| {
                let bson_val = d.get("policy_document")?;
                let json_val: serde_json::Value = bson::from_bson(bson_val.clone()).ok()?;
                Some(json_val.to_string())
            }))
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
            let coll = self.catalog_db().collection::<Document>("iam_policies");
            let cursor = coll
                .find(doc! {
                    "account_id": &account_id,
                    "principal_type": "role",
                    "principal_name": &role_name,
                })
                .await
                .map_err(|e| {
                    tracing::error!("fetch_role_policies: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("fetch_role_policies cursor: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(docs
                .into_iter()
                .filter_map(|d| {
                    let bson_val = d.get("policy_document")?;
                    let json_val: serde_json::Value = bson::from_bson(bson_val.clone()).ok()?;
                    Some(json_val.to_string())
                })
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
                    tracing::error!("fetch_role_boundary: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(doc.and_then(|d| {
                let bson_val = d.get("policy_document")?;
                let json_val: serde_json::Value = bson::from_bson(bson_val.clone()).ok()?;
                Some(json_val.to_string())
            }))
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
            let coll = self.catalog_db().collection::<Document>("iam_sessions");
            let now_bson = mongodb::bson::DateTime::now();
            let doc = coll
                .find_one(doc! {
                    "account_id": &account_id,
                    "role_name": &role_name,
                    "session_name": &session_name,
                    "expires_at": { "$gt": now_bson },
                })
                .await
                .map_err(|e| {
                    tracing::error!("fetch_session_data: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let Some(session_doc) = doc else {
                return Ok(None);
            };

            let session_policy = session_doc.get("session_policy").and_then(|b| {
                let json_val: serde_json::Value = bson::from_bson(b.clone()).ok()?;
                Some(json_val.to_string())
            });

            let mut session_tags = Vec::new();
            if let Some(tags_bson) = session_doc.get("session_tags") {
                if let Ok(tags_val) = bson::from_bson::<serde_json::Value>(tags_bson.clone()) {
                    if let Some(arr) = tags_val.as_array() {
                        for tag in arr {
                            if let (Some(k), Some(v)) = (
                                tag.get("Key").and_then(|k| k.as_str()),
                                tag.get("Value").and_then(|v| v.as_str()),
                            ) {
                                session_tags.push((k.to_owned(), v.to_owned()));
                            }
                        }
                    } else if let Some(obj) = tags_val.as_object() {
                        for (k, v) in obj {
                            if let Some(v_str) = v.as_str() {
                                session_tags.push((k.clone(), v_str.to_owned()));
                            }
                        }
                    }
                }
            }

            Ok(Some(SessionData {
                session_policy,
                session_tags,
            }))
        })
    }

    fn fetch_user_tags(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_user_tags");
            let cursor = coll
                .find(doc! { "account_id": &account_id, "user_name": &user_name })
                .await
                .map_err(|e| {
                    tracing::error!("fetch_user_tags: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("fetch_user_tags cursor: {e}");
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

    fn fetch_role_tags(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_role_tags");
            let cursor = coll
                .find(doc! { "account_id": &account_id, "role_name": &role_name })
                .await
                .map_err(|e| {
                    tracing::error!("fetch_role_tags: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("fetch_role_tags cursor: {e}");
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

    fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let arn = arn.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("tags");
            let cursor = coll
                .find(doc! { "resource_arn": &arn })
                .await
                .map_err(|e| {
                    tracing::error!("fetch_resource_tags: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let docs: Vec<Document> = cursor.try_collect().await.map_err(|e| {
                tracing::error!("fetch_resource_tags cursor: {e}");
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
}
