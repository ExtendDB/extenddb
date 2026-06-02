// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AuthorizationStore` implementation for `TidbCatalogStore`.

use extenddb_storage::authorization_store::{
    AuthorizationStore, RoleAuthorizationData, SessionData, UserAuthorizationData,
    merge_session_tags,
};
use extenddb_storage::management_store::{OpError, OpResult};
use futures::future::BoxFuture;

use super::catalog_store::TidbCatalogStore;

type UserAuthorizationRow = (
    String,
    Option<serde_json::Value>,
    Option<String>,
    Option<String>,
);
type RoleAuthorizationRow = (
    String,
    Option<serde_json::Value>,
    Option<String>,
    Option<String>,
    Option<serde_json::Value>,
);

fn user_authorization_sql(include_resource_tags: bool) -> String {
    let mut sql = String::from(
        "SELECT 'policy' AS row_kind, policy_document AS document, \
                NULL AS tag_key, NULL AS tag_value \
         FROM iam_policies \
         WHERE account_id = ? AND principal_type = 'user' AND principal_name = ? \
         UNION ALL \
         SELECT 'policy' AS row_kind, p.policy_document AS document, \
                NULL AS tag_key, NULL AS tag_value \
         FROM iam_policies p \
         JOIN iam_group_members gm ON p.account_id = gm.account_id \
           AND p.principal_type = 'group' \
           AND p.principal_name = gm.group_name \
         WHERE gm.account_id = ? AND gm.user_name = ? \
         UNION ALL \
         SELECT 'boundary' AS row_kind, policy_document AS document, \
                NULL AS tag_key, NULL AS tag_value \
         FROM iam_permissions_boundaries \
         WHERE account_id = ? AND principal_type = 'user' AND principal_name = ? \
         UNION ALL \
         SELECT 'principal_tag' AS row_kind, NULL AS document, \
                tag_key, tag_value \
         FROM iam_user_tags \
         WHERE account_id = ? AND user_name = ?",
    );
    if include_resource_tags {
        sql.push_str(
            " UNION ALL \
              SELECT 'resource_tag' AS row_kind, NULL AS document, \
                     tag_key, tag_value \
              FROM tags \
              WHERE resource_arn = ?",
        );
    }
    sql
}

fn role_authorization_sql(include_resource_tags: bool) -> String {
    let mut sql = String::from(
        "SELECT 'policy' AS row_kind, policy_document AS document, \
                NULL AS tag_key, NULL AS tag_value, NULL AS session_tags \
         FROM iam_policies \
         WHERE account_id = ? AND principal_type = 'role' AND principal_name = ? \
         UNION ALL \
         SELECT 'boundary' AS row_kind, policy_document AS document, \
                NULL AS tag_key, NULL AS tag_value, NULL AS session_tags \
         FROM iam_permissions_boundaries \
         WHERE account_id = ? AND principal_type = 'role' AND principal_name = ? \
         UNION ALL \
         SELECT 'principal_tag' AS row_kind, NULL AS document, \
                tag_key, tag_value, NULL AS session_tags \
         FROM iam_role_tags \
         WHERE account_id = ? AND role_name = ? \
         UNION ALL \
         SELECT 'session' AS row_kind, session_policy AS document, \
                NULL AS tag_key, NULL AS tag_value, session_tags \
         FROM iam_sessions \
         WHERE account_id = ? AND role_name = ? AND session_name = ? \
           AND access_key_id = ? \
           AND expires_at > CURRENT_TIMESTAMP(6)",
    );
    if include_resource_tags {
        sql.push_str(
            " UNION ALL \
              SELECT 'resource_tag' AS row_kind, NULL AS document, \
                     tag_key, tag_value, NULL AS session_tags \
              FROM tags \
              WHERE resource_arn = ?",
        );
    }
    sql
}

fn json_to_string(value: serde_json::Value) -> String {
    value.to_string()
}

fn push_tag(target: &mut Vec<(String, String)>, key: Option<String>, value: Option<String>) {
    if let (Some(key), Some(value)) = (key, value) {
        target.push((key, value));
    }
}

fn session_tags_from_value(value: Option<serde_json::Value>) -> Vec<(String, String)> {
    let mut tags = Vec::new();
    let Some(value) = value else {
        return tags;
    };

    if let Some(arr) = value.as_array() {
        for tag in arr {
            if let (Some(k), Some(v)) = (
                tag.get("Key").and_then(|k| k.as_str()),
                tag.get("Value").and_then(|v| v.as_str()),
            ) {
                tags.push((k.to_owned(), v.to_owned()));
            }
        }
    } else if let Some(obj) = value.as_object() {
        for (k, v) in obj {
            if let Some(v_str) = v.as_str() {
                tags.push((k.clone(), v_str.to_owned()));
            }
        }
    }

    tags
}

impl AuthorizationStore for TidbCatalogStore {
    fn fetch_user_authorization<'a>(
        &'a self,
        account_id: &'a str,
        user_name: &'a str,
        resource_arn: &'a str,
    ) -> BoxFuture<'a, OpResult<UserAuthorizationData>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let resource_arn = resource_arn.to_owned();
        Box::pin(async move {
            let include_resource_tags = !resource_arn.ends_with("/*");
            let sql = user_authorization_sql(include_resource_tags);
            let mut query = sqlx::query_as::<_, UserAuthorizationRow>(&sql)
                .bind(&account_id)
                .bind(&user_name)
                .bind(&account_id)
                .bind(&user_name)
                .bind(&account_id)
                .bind(&user_name)
                .bind(&account_id)
                .bind(&user_name);
            if include_resource_tags {
                query = query.bind(&resource_arn);
            }

            let rows = query.fetch_all(self.pool()).await.map_err(|e| {
                tracing::error!("fetch_user_authorization: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let mut identity_policies = Vec::new();
            let mut boundary = None;
            let mut principal_tags = Vec::new();
            let mut resource_tags = Vec::new();

            for (row_kind, document, tag_key, tag_value) in rows {
                match row_kind.as_str() {
                    "policy" => {
                        if let Some(document) = document {
                            identity_policies.push(json_to_string(document));
                        }
                    }
                    "boundary" => {
                        if let Some(document) = document {
                            boundary = Some(json_to_string(document));
                        }
                    }
                    "principal_tag" => push_tag(&mut principal_tags, tag_key, tag_value),
                    "resource_tag" => push_tag(&mut resource_tags, tag_key, tag_value),
                    other => {
                        tracing::error!("unknown TiDB user authorization row kind: {other}");
                        return Err(OpError::Internal("Database error".to_owned()));
                    }
                }
            }

            Ok(UserAuthorizationData {
                identity_policies,
                boundary,
                principal_tags,
                resource_tags,
            })
        })
    }

    fn fetch_role_authorization<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
        session_name: &'a str,
        access_key_id: &'a str,
        resource_arn: &'a str,
    ) -> BoxFuture<'a, OpResult<RoleAuthorizationData>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session_name = session_name.to_owned();
        let access_key_id = access_key_id.to_owned();
        let resource_arn = resource_arn.to_owned();
        Box::pin(async move {
            let include_resource_tags = !resource_arn.ends_with("/*");
            let sql = role_authorization_sql(include_resource_tags);
            let mut query = sqlx::query_as::<_, RoleAuthorizationRow>(&sql)
                .bind(&account_id)
                .bind(&role_name)
                .bind(&account_id)
                .bind(&role_name)
                .bind(&account_id)
                .bind(&role_name)
                .bind(&account_id)
                .bind(&role_name)
                .bind(&session_name)
                .bind(&access_key_id);
            if include_resource_tags {
                query = query.bind(&resource_arn);
            }

            let rows = query.fetch_all(self.pool()).await.map_err(|e| {
                tracing::error!("fetch_role_authorization: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let mut identity_policies = Vec::new();
            let mut boundary = None;
            let mut session_policy = None;
            let mut principal_tags = Vec::new();
            let mut session_tags = Vec::new();
            let mut resource_tags = Vec::new();

            for (row_kind, document, tag_key, tag_value, session_tags_json) in rows {
                match row_kind.as_str() {
                    "policy" => {
                        if let Some(document) = document {
                            identity_policies.push(json_to_string(document));
                        }
                    }
                    "boundary" => {
                        if let Some(document) = document {
                            boundary = Some(json_to_string(document));
                        }
                    }
                    "principal_tag" => push_tag(&mut principal_tags, tag_key, tag_value),
                    "session" => {
                        session_policy = document.map(json_to_string);
                        session_tags = session_tags_from_value(session_tags_json);
                    }
                    "resource_tag" => push_tag(&mut resource_tags, tag_key, tag_value),
                    other => {
                        tracing::error!("unknown TiDB role authorization row kind: {other}");
                        return Err(OpError::Internal("Database error".to_owned()));
                    }
                }
            }
            merge_session_tags(&mut principal_tags, session_tags);

            Ok(RoleAuthorizationData {
                identity_policies,
                boundary,
                session_policy,
                principal_tags,
                resource_tags,
            })
        })
    }

    fn fetch_user_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_policies \
                 WHERE account_id = ? AND principal_type = 'user' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_policies: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(rows.into_iter().map(|(v,)| v.to_string()).collect())
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
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(
                "SELECT p.policy_document \
                 FROM iam_policies p \
                 JOIN iam_group_members gm ON p.account_id = gm.account_id \
                   AND p.principal_type = 'group' \
                   AND p.principal_name = gm.group_name \
                 WHERE gm.account_id = ? AND gm.user_name = ?",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_group_policies: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(rows.into_iter().map(|(v,)| v.to_string()).collect())
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
            let row: Option<(serde_json::Value,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_permissions_boundaries \
                 WHERE account_id = ? AND principal_type = 'user' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_boundary: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(row.map(|(v,)| v.to_string()))
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
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_policies \
                 WHERE account_id = ? AND principal_type = 'role' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_role_policies: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(rows.into_iter().map(|(v,)| v.to_string()).collect())
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
            let row: Option<(serde_json::Value,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_permissions_boundaries \
                 WHERE account_id = ? AND principal_type = 'role' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_role_boundary: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(row.map(|(v,)| v.to_string()))
        })
    }

    fn fetch_session_data(
        &self,
        account_id: &str,
        role_name: &str,
        session_name: &str,
        access_key_id: &str,
    ) -> BoxFuture<'_, OpResult<Option<SessionData>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session_name = session_name.to_owned();
        let access_key_id = access_key_id.to_owned();
        Box::pin(async move {
            let row: Option<(Option<serde_json::Value>, Option<serde_json::Value>)> =
                sqlx::query_as(
                    "SELECT session_policy, session_tags FROM iam_sessions \
                     WHERE account_id = ? AND role_name = ? AND session_name = ? \
                     AND access_key_id = ? \
                     AND expires_at > now()",
                )
                .bind(&account_id)
                .bind(&role_name)
                .bind(&session_name)
                .bind(&access_key_id)
                .fetch_optional(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("fetch_session_data: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let Some((policy_value, tags_value)) = row else {
                return Ok(None);
            };

            let session_policy = policy_value.map(|v| v.to_string());

            let session_tags = session_tags_from_value(tags_value);

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
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_user_tags \
                 WHERE account_id = ? AND user_name = ?",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_tags: {e}");
                OpError::Internal("Database error".to_owned())
            })
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
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_role_tags \
                 WHERE account_id = ? AND role_name = ?",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_role_tags: {e}");
                OpError::Internal("Database error".to_owned())
            })
        })
    }

    fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let arn = arn.to_owned();
        Box::pin(async move {
            sqlx::query_as("SELECT tag_key, tag_value FROM tags WHERE resource_arn = ?")
                .bind(&arn)
                .fetch_all(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("fetch_resource_tags: {e}");
                    OpError::Internal("Database error".to_owned())
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placeholder_count(sql: &str) -> usize {
        sql.bytes().filter(|byte| *byte == b'?').count()
    }

    #[test]
    fn aggregate_user_authorization_sql_binds_exact_inputs() {
        let without_resource_tags = user_authorization_sql(false);
        assert_eq!(placeholder_count(&without_resource_tags), 8);
        assert!(!without_resource_tags.contains("FROM tags"));

        let with_resource_tags = user_authorization_sql(true);
        assert_eq!(placeholder_count(&with_resource_tags), 9);
        assert!(with_resource_tags.contains("FROM tags"));
        assert_eq!(with_resource_tags.matches("UNION ALL").count(), 4);
    }

    #[test]
    fn aggregate_role_authorization_sql_binds_exact_inputs() {
        let without_resource_tags = role_authorization_sql(false);
        assert_eq!(placeholder_count(&without_resource_tags), 10);
        assert!(without_resource_tags.contains("AND access_key_id = ?"));
        assert!(!without_resource_tags.contains("FROM tags"));

        let with_resource_tags = role_authorization_sql(true);
        assert_eq!(placeholder_count(&with_resource_tags), 11);
        assert!(with_resource_tags.contains("AND access_key_id = ?"));
        assert!(with_resource_tags.contains("FROM tags"));
        assert_eq!(with_resource_tags.matches("UNION ALL").count(), 4);
    }

    #[test]
    fn session_tags_parse_aws_shape_and_catalog_map_shape() {
        let aws_shape = serde_json::json!([
            {"Key": "tenant", "Value": "blue"},
            {"Key": "env", "Value": "prod"}
        ]);
        assert_eq!(
            session_tags_from_value(Some(aws_shape)),
            vec![
                ("tenant".to_owned(), "blue".to_owned()),
                ("env".to_owned(), "prod".to_owned())
            ]
        );

        let map_shape = serde_json::json!({"tenant": "green", "ignored": 7});
        assert_eq!(
            session_tags_from_value(Some(map_shape)),
            vec![("tenant".to_owned(), "green".to_owned())]
        );
    }
}
