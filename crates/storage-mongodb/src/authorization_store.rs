// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AuthorizationStore` trait implementation for `MongoDB`.
//!
//! A stored policy, permissions boundary, session policy, session tags value,
//! group membership or tag that these loaders cannot read (a field missing or
//! of another type) fails the load with `OpError::Internal`, as a failed query
//! does: a document left out can hold the Deny that matches the request. Two
//! reads stay lenient, as on the other backends. A session tag entry whose key
//! or value is not a string is left out, because `AssumeRole` stores the
//! session tags as the caller sent them. A `created_at` that cannot be read
//! gives `None`, because sessions written before 0.1.14 have none.

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{self, Document, doc};

use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::management_store::{OpError, OpResult};

use crate::catalog_store::MongoCatalogStore;
use crate::management_store::stored_document;

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
            docs.iter()
                .map(|d| policy_document("iam_policies", d))
                .collect()
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

            let group_names = member_docs
                .iter()
                .map(|d| string_field("iam_group_members", d, "group_name"))
                .collect::<OpResult<Vec<&str>>>()?;

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
            docs.iter()
                .map(|d| {
                    let group = string_field("iam_policies", d, "principal_name")?;
                    let policy = policy_document("iam_policies", d)?;
                    Ok((group.to_owned(), policy))
                })
                .collect()
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
            doc.map(|d| policy_document("iam_permissions_boundaries", &d))
                .transpose()
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
            docs.iter()
                .map(|d| policy_document("iam_policies", d))
                .collect()
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
            doc.map(|d| policy_document("iam_permissions_boundaries", &d))
                .transpose()
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

            let session_policy =
                json_field("iam_sessions", &session_doc, "session_policy")?.map(|v| v.to_string());

            let mut session_tags = Vec::new();
            if let Some(tags_val) = json_field("iam_sessions", &session_doc, "session_tags")? {
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

            Ok(Some(SessionData {
                session_policy,
                session_tags,
                created_at: session_doc
                    .get_datetime("created_at")
                    .ok()
                    .and_then(bson_datetime_to_rfc3339),
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
            docs.iter().map(|d| tag("iam_user_tags", d)).collect()
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
            docs.iter().map(|d| tag("iam_role_tags", d)).collect()
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
            docs.iter().map(|d| tag("tags", d)).collect()
        })
    }

    fn fetch_user_created_at(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let coll = self.catalog_db().collection::<Document>("iam_users");
            let doc = coll
                .find_one(doc! { "account_id": &account_id, "user_name": &user_name })
                .await
                .map_err(|e| {
                    tracing::error!("fetch_user_created_at: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(doc.and_then(|d| {
                d.get_datetime("created_at")
                    .ok()
                    .and_then(bson_datetime_to_rfc3339)
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
            let coll = self.catalog_db().collection::<Document>("iam_roles");
            let doc = coll
                .find_one(doc! { "account_id": &account_id, "role_name": &role_name })
                .await
                .map_err(|e| {
                    tracing::error!("fetch_role_created_at: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(doc.and_then(|d| {
                d.get_datetime("created_at")
                    .ok()
                    .and_then(bson_datetime_to_rfc3339)
            }))
        })
    }
}

/// Log a stored document of `collection` that cannot be read, and give the
/// error of the load that needs it.
fn unreadable(collection: &str, d: &Document, fault: &str) -> OpError {
    tracing::error!(
        collection,
        id = %d.get("_id").map_or_else(String::new, ToString::to_string),
        "a stored IAM document cannot be read ({fault}); an authorization load \
         that needs it fails until the document is repaired or deleted"
    );
    OpError::Internal("Stored IAM document cannot be read".to_owned())
}

/// A string field of a stored document. `ExtendDB` writes these fields as
/// strings.
fn string_field<'d>(collection: &str, d: &'d Document, field: &str) -> OpResult<&'d str> {
    d.get_str(field)
        .map_err(|e| unreadable(collection, d, &format!("{field}: {e}")))
}

/// A field of a stored document as JSON, or `None` when the document has no
/// such field. A value with no JSON form, such as a Decimal128 or binary data
/// of the generic subtype, cannot be read.
fn json_field(collection: &str, d: &Document, field: &str) -> OpResult<Option<serde_json::Value>> {
    d.get(field)
        .map(|value| {
            bson::from_bson(value.clone())
                .map_err(|e| unreadable(collection, d, &format!("{field}: {e}")))
        })
        .transpose()
}

/// The `policy_document` of a stored policy or permissions boundary, as
/// JSON text.
fn policy_document(collection: &str, d: &Document) -> OpResult<String> {
    stored_document(d)
        .map(|v| v.to_string())
        .map_err(|fault| unreadable(collection, d, &fault))
}

/// The key and the value of a stored tag.
fn tag(collection: &str, d: &Document) -> OpResult<(String, String)> {
    Ok((
        string_field(collection, d, "tag_key")?.to_owned(),
        string_field(collection, d, "tag_value")?.to_owned(),
    ))
}

/// Format a BSON datetime as RFC 3339 UTC, matching how the other backends
/// return `created_at` for the `aws:userid` derivation and `aws:TokenIssueTime`.
fn bson_datetime_to_rfc3339(dt: &mongodb::bson::DateTime) -> Option<String> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(dt.timestamp_millis()) * 1_000_000)
        .ok()
        .and_then(|odt| {
            odt.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
}
