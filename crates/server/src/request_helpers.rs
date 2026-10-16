// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request parsing and authorization helpers for the `DynamoDB` wire protocol.

use axum::http::HeaderMap;
use extenddb_core::error::DynamoDbError;
use serde_json::Value;

use crate::AppState;
use crate::authorization;

/// Extract operation name from X-Amz-Target header.
/// Accepts both `DynamoDB_20120810` and `DynamoDBStreams_20120810` wire-format prefixes.
pub(crate) fn extract_operation(headers: &HeaderMap) -> Result<String, DynamoDbError> {
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            // S-7: Real DynamoDB returns MissingAuthenticationToken only when auth
            // headers are also absent. When auth headers are present but X-Amz-Target
            // is missing, it returns UnknownOperationException.
            if headers.contains_key("authorization") {
                DynamoDbError::UnknownOperationException(String::new())
            } else {
                DynamoDbError::MissingAuthenticationToken("Missing Authentication Token".to_owned())
            }
        })?;

    target
        .strip_prefix("DynamoDB_20120810.")
        .or_else(|| target.strip_prefix("DynamoDBStreams_20120810."))
        .map(std::borrow::ToOwned::to_owned)
        .ok_or_else(|| DynamoDbError::UnknownOperationException(String::new()))
}

/// Extract the table name from a `DynamoDB` request body.
///
/// Most operations use `TableName`. Batch and transact operations embed table
/// names in nested structures — returns `None` for those; the caller maps
/// `None` to `*` via `build_resource_arn`.
pub(crate) fn extract_table_name(input: &Value) -> Option<String> {
    input
        .get("TableName")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
}

/// Evaluate IAM policies for an authenticated identity.
///
/// Returns the pre-fetched `TableKeyInfo` for single-table item-level operations
/// (P118 optimization #2). The caller passes this into `OperationContext` to
/// avoid a redundant catalog roundtrip in the engine layer.
///
/// All authorization data is fetched via `state.authz_cache`, which sits on
/// top of the underlying `AuthorizationStore` and serves cached, pre-parsed
/// `PolicyDocument`s.
pub(crate) async fn authorize_request(
    state: &AppState,
    identity: &extenddb_auth::AuthIdentity,
    input: &Value,
    operation: &str,
    account_id: &str,
) -> Result<Option<extenddb_core::types::TableKeyInfo>, DynamoDbError> {
    let table_name = extract_table_name(input);

    // P118: Fetch table_key_info for item-level operations via the SWR cache.
    // The result is used for LeadingKeys extraction here AND returned to the
    // caller to avoid a redundant fetch in the engine layer.
    let key_info = match operation {
        "GetItem" | "PutItem" | "DeleteItem" | "UpdateItem" | "Query" | "Scan" => {
            if let Some(ref tn) = table_name {
                state
                    .table_key_info_cache
                    .get_optional(account_id, tn)
                    .await
            } else {
                None
            }
        }
        _ => None,
    };

    let pk_attr = key_info
        .as_ref()
        .map(|ki| ki.key_schema[0].attribute_name.clone());

    evaluate_request_policies(
        state.authz_cache.as_ref(),
        state.dev_mode,
        &state.region,
        identity,
        input,
        operation,
        account_id,
        pk_attr.as_deref(),
    )
    .await?;

    Ok(key_info)
}

/// The IAM decision for one request: every (action, resource) pair the
/// operation authorizes on must be allowed.
///
/// Split from [`authorize_request`] so it can be exercised without an
/// `AppState`; `pk_attr` is the table's partition-key attribute for item
/// operations (for `dynamodb:LeadingKeys`), `None` otherwise.
#[allow(clippy::too_many_arguments)]
async fn evaluate_request_policies(
    cache: &crate::authz_cache::CachedAuthzStore,
    dev_mode: bool,
    region: &str,
    identity: &extenddb_auth::AuthIdentity,
    input: &Value,
    operation: &str,
    account_id: &str,
    pk_attr: Option<&str>,
) -> Result<(), DynamoDbError> {
    // Developer mode opens authorization for the authenticated caller. SigV4
    // verification already ran upstream, so the request is authenticated; only
    // the IAM policy decision is skipped.
    //
    // This check is deliberately positioned ahead of EVERY authorization branch
    // below, not merely the generic one. It previously sat after the batch and
    // transaction branch, which returns early, so those four operations were
    // still evaluated against IAM and denied in a dev-mode build while single
    // item operations passed. Any future operation-specific branch must be added
    // below this point, or it will reintroduce that defect.
    if dev_mode {
        return Ok(());
    }

    // Batch and transaction operations address multiple tables in nested request
    // structures with no top-level TableName. DynamoDB authorizes each of them
    // per table, against that table's specific ARN, using the IAM action the
    // operation maps to (BatchGetItem/BatchWriteItem are their own actions;
    // TransactGetItems decomposes to GetItem; TransactWriteItems decomposes to
    // the per-sub-op action). Authorizing the whole request against a single
    // `table/*` wildcard, as the generic path below does, lets an explicit
    // Deny on one table be bypassed. Evaluate every (action, table) pair and
    // reject the entire request if any is denied (all-or-nothing), matching
    // DynamoDB. Verified against the AWS IAM Service Authorization Reference.
    if let Some(targets) = batch_transact_authz_targets(operation, input) {
        for (action_op, table) in targets {
            let resource_arn = build_resource_arn(region, account_id, Some(&table));
            authorization::check_authorization(
                cache,
                identity,
                &action_op,
                &resource_arn,
                false,
                extenddb_auth::policy::context::RequestParams::default(),
            )
            .await?;
        }
        return Ok(());
    }

    // Backup and restore operations name their resources in members other
    // than `TableName` (`BackupArn`, `TargetTableName`, `SourceTableName`), so
    // the generic path below would authorize them against `table/*` and a
    // policy scoped to a backup or a target table could never match. Evaluate
    // every resource the service authorizes on, all of which must allow.
    if let Some(resources) = backup_authz_resources(operation, input, region, account_id) {
        for resource_arn in resources? {
            authorization::check_authorization(
                cache,
                identity,
                operation,
                &resource_arn,
                false,
                extenddb_auth::policy::context::RequestParams::default(),
            )
            .await?;
        }
        return Ok(());
    }

    let table_name = extract_table_name(input);
    let resource_arn = build_resource_arn(region, account_id, table_name.as_deref());

    let params = extenddb_auth::policy::context::RequestParams {
        leading_keys: extract_leading_keys(input, operation, pk_attr),
        attributes: extract_attributes(input),
        select: input
            .get("Select")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned),
        return_values: input
            .get("ReturnValues")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned),
        return_consumed_capacity: input
            .get("ReturnConsumedCapacity")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned),
        ..Default::default()
    };
    authorization::check_authorization(
        cache,
        identity,
        operation,
        &resource_arn,
        operation == "Scan",
        params,
    )
    .await
}

/// Extract partition key values from the request body for `dynamodb:LeadingKeys`.
///
/// For item-level operations (`GetItem`, `PutItem`, `DeleteItem`, `UpdateItem`),
/// extracts the partition key value from the `Key` or `Item` using the table's
/// key schema. For `Query`, the leading key comes from `KeyConditionExpression`
/// values, but extracting that requires expression parsing — deferred to the
/// engine layer.
/// Returns `None` for table-level and batch/transact operations, or when
/// `pk_attr` is not available.
fn extract_leading_keys(
    input: &Value,
    operation: &str,
    pk_attr: Option<&str>,
) -> Option<Vec<String>> {
    let pk_attr = pk_attr?;
    match operation {
        "GetItem" | "DeleteItem" | "UpdateItem" => extract_pk_value(input.get("Key")?, pk_attr),
        "PutItem" => extract_pk_value(input.get("Item")?, pk_attr),
        _ => None,
    }
}

/// Extract the partition key value from a `DynamoDB` key/item map using the
/// known PK attribute name.
///
/// `DynamoDB` keys are `{"attrName": {"S": "value"}}`. We extract the typed
/// value of the partition key attribute as a string.
fn extract_pk_value(map: &Value, pk_attr: &str) -> Option<Vec<String>> {
    let obj = map.as_object()?;
    let type_val = obj.get(pk_attr)?;
    let type_obj = type_val.as_object()?;
    let (_, val) = type_obj.iter().next()?;
    let s = val.as_str().unwrap_or_default();
    Some(vec![s.to_owned()])
}

/// Extract attribute names from the request for `dynamodb:Attributes`.
///
/// Collects attribute names from `ProjectionExpression` (comma-separated list
/// of top-level names). Resolves `ExpressionAttributeNames` placeholders
/// (e.g. `#n` → `name`) when present.
/// Returns `None` when no projection is specified.
pub(crate) fn extract_attributes(input: &Value) -> Option<Vec<String>> {
    let proj = input.get("ProjectionExpression")?.as_str()?;
    let ean = input
        .get("ExpressionAttributeNames")
        .and_then(|v| v.as_object());
    let names: Vec<String> = proj
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            ean.and_then(|m| m.get(s))
                .and_then(|v| v.as_str())
                .unwrap_or(s)
                .to_owned()
        })
        .collect();
    if names.is_empty() { None } else { Some(names) }
}

/// IAM `(action, table)` pairs to authorize for a batch/transact operation.
///
/// Returns `None` for non-batch/transact operations (the caller uses the
/// single-table path). Actions follow the AWS IAM Service Authorization
/// Reference: `BatchGetItem`/`BatchWriteItem` authorize under their own action
/// per table; `TransactGetItems` authorizes as `GetItem` per item's table;
/// `TransactWriteItems` authorizes per sub-op (`Put`→`PutItem`,
/// `Delete`→`DeleteItem`, `Update`→`UpdateItem`, `ConditionCheck`→
/// `ConditionCheckItem`). Pairs are de-duplicated. Table names are the bare
/// operation-string form (no `dynamodb:` prefix), matching `check_authorization`
/// which prepends `dynamodb:`.
fn batch_transact_authz_targets(operation: &str, input: &Value) -> Option<Vec<(String, String)>> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut push = |action: &str, table: &str| {
        let pair = (action.to_owned(), table.to_owned());
        if !out.contains(&pair) {
            out.push(pair);
        }
    };
    match operation {
        "BatchGetItem" | "BatchWriteItem" => {
            // RequestItems is a map keyed by table name.
            let items = input.get("RequestItems")?.as_object()?;
            for table in items.keys() {
                push(operation, table);
            }
        }
        "TransactGetItems" => {
            let items = input.get("TransactItems")?.as_array()?;
            for entry in items {
                if let Some(t) = entry
                    .get("Get")
                    .and_then(|g| g.get("TableName"))
                    .and_then(Value::as_str)
                {
                    push("GetItem", t);
                }
            }
        }
        "TransactWriteItems" => {
            let items = input.get("TransactItems")?.as_array()?;
            for entry in items {
                for (sub, action) in [
                    ("Put", "PutItem"),
                    ("Delete", "DeleteItem"),
                    ("Update", "UpdateItem"),
                    ("ConditionCheck", "ConditionCheckItem"),
                ] {
                    if let Some(t) = entry
                        .get(sub)
                        .and_then(|s| s.get("TableName"))
                        .and_then(Value::as_str)
                    {
                        push(action, t);
                    }
                }
            }
        }
        _ => return None,
    }
    Some(out)
}

/// Resource ARNs to authorize for a backup or restore operation.
///
/// Returns `None` for any other operation (the caller uses the generic
/// `TableName` path). The resource types follow the AWS Service Authorization
/// Reference for Amazon `DynamoDB`
/// (<https://docs.aws.amazon.com/service-authorization/latest/reference/list_dynamodb.html>,
/// also published as
/// <https://servicereference.us-east-1.amazonaws.com/v1/dynamodb/dynamodb.json>;
/// both read 2026-10-15):
///
/// | Action | Resource types (all required) | Evaluated here |
/// |---|---|---|
/// | `CreateBackup` | `table` | generic path: `table/<TableName>` |
/// | `DescribeContinuousBackups` | `table` | generic path: `table/<TableName>` |
/// | `UpdateContinuousBackups` | `table` | generic path: `table/<TableName>` |
/// | `DescribeBackup` | `backup` | the `BackupArn` |
/// | `DeleteBackup` | `backup` | the `BackupArn` |
/// | `RestoreTableFromBackup` | `backup`, `table` | the `BackupArn` and `table/<TargetTableName>` |
/// | `ListBackups` | none | `table/*`, also when `TableName` filters the list |
/// | `RestoreTableToPointInTime` | `table` (the source, per the developer guide's backup IAM page) | `table/<SourceTableName>`; `table/*` for `SourceTableArn` |
///
/// The `backup` ARN format is
/// `arn:${Partition}:dynamodb:${Region}:${Account}:table/${TableName}/backup/${BackupName}`.
///
/// A well-formed `BackupArn` is evaluated exactly as supplied. Every backend
/// resolves a backup by equality on the full ARN string (and the account), so
/// the string evaluated is the resource acted on; an ARN that differs in any
/// character, region or partition included, resolves to no backup.
///
/// A `BackupArn` that is missing, not a string, or not a backup ARN by
/// [`extenddb_storage::util::parse_backup_arn`] is evaluated against
/// `table/*`, the resource every one of these operations was evaluated against
/// before this check existed, so no caller is allowed more than before. Such a
/// request names no backup; the engine then answers `ValidationException`.
/// A missing `TargetTableName` likewise contributes `table/*`.
///
/// A well-formed `BackupArn` naming another account is refused here with the
/// engine's own `AccessDeniedException` and no policy evaluation. `DynamoDB`
/// backups take no resource-based policy, so an identity policy cannot grant
/// access to another account's backup and the service denies it whatever
/// the caller's policies say. Evaluating it would also look up the other
/// account's resource tags for `aws:ResourceTag` conditions, which a caller
/// could use to probe them.
fn backup_authz_resources(
    operation: &str,
    input: &Value,
    region: &str,
    account_id: &str,
) -> Option<Result<Vec<String>, DynamoDbError>> {
    let wildcard = || build_resource_arn(region, account_id, None);
    let str_member = |name: &str| input.get(name).and_then(Value::as_str);
    let backup = || -> Result<String, DynamoDbError> {
        let Some(arn) = str_member("BackupArn") else {
            return Ok(wildcard());
        };
        match extenddb_storage::util::parse_backup_arn(arn) {
            None => Ok(wildcard()),
            Some(parts) if parts.account_id != account_id => {
                tracing::warn!(
                    action = format!("dynamodb:{operation}"),
                    resource = arn,
                    "Authorization denied: backup ARN names another account"
                );
                Err(DynamoDbError::AccessDeniedException(
                    "Access is denied".to_owned(),
                ))
            }
            Some(_) => Ok(arn.to_owned()),
        }
    };
    let resources = match operation {
        "DescribeBackup" | "DeleteBackup" => backup().map(|b| vec![b]),
        "RestoreTableFromBackup" => backup().map(|b| {
            let target = build_resource_arn(region, account_id, str_member("TargetTableName"));
            vec![b, target]
        }),
        "ListBackups" => Ok(vec![wildcard()]),
        "RestoreTableToPointInTime" => {
            let mut out = Vec::new();
            if let Some(name) = str_member("SourceTableName") {
                out.push(build_resource_arn(region, account_id, Some(name)));
            }
            // ExtendDB does not implement point-in-time recovery and the
            // engine refuses the operation; a SourceTableArn is not resolved
            // here and keeps the pre-existing `table/*` evaluation.
            if out.is_empty() || input.get("SourceTableArn").is_some() {
                out.push(wildcard());
            }
            Ok(out)
        }
        _ => return None,
    };
    Some(resources)
}

/// Build a `DynamoDB` table ARN for authorization.
///
/// If no table name is available (e.g. `ListTables`, `DescribeEndpoints`),
/// uses `*` as the resource.
fn build_resource_arn(region: &str, account_id: &str, table_name: Option<&str>) -> String {
    match table_name {
        Some(name) => format!("arn:aws:dynamodb:{region}:{account_id}:table/{name}"),
        None => format!("arn:aws:dynamodb:{region}:{account_id}:table/*"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_attributes_resolves_expression_attribute_names() {
        let input = json!({
            "ProjectionExpression": "#n, #v",
            "ExpressionAttributeNames": {
                "#n": "name",
                "#v": "value"
            }
        });
        let result = extract_attributes(&input);
        assert_eq!(result, Some(vec!["name".to_owned(), "value".to_owned()]));
    }

    #[test]
    fn extract_attributes_mixed_placeholders_and_literals() {
        let input = json!({
            "ProjectionExpression": "#n, age",
            "ExpressionAttributeNames": {
                "#n": "name"
            }
        });
        let result = extract_attributes(&input);
        assert_eq!(result, Some(vec!["name".to_owned(), "age".to_owned()]));
    }

    #[test]
    fn extract_attributes_no_expression_attribute_names() {
        let input = json!({
            "ProjectionExpression": "name, age"
        });
        let result = extract_attributes(&input);
        assert_eq!(result, Some(vec!["name".to_owned(), "age".to_owned()]));
    }

    #[test]
    fn extract_attributes_no_projection() {
        let input = json!({"TableName": "test"});
        assert_eq!(extract_attributes(&input), None);
    }

    #[test]
    fn missing_target_no_auth_returns_missing_auth_token() {
        // S-7: No Authorization header + no X-Amz-Target → MissingAuthenticationToken
        let headers = HeaderMap::new();
        let err = extract_operation(&headers).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::MissingAuthenticationToken(_)),
            "Expected MissingAuthenticationToken, got: {err:?}"
        );
    }

    #[test]
    fn missing_target_with_auth_returns_unknown_operation() {
        // S-7: Authorization header present but no X-Amz-Target → UnknownOperationException
        use axum::http::HeaderValue;
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=AKID/20260415/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=abc"));
        let err = extract_operation(&headers).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::UnknownOperationException(_)),
            "Expected UnknownOperationException, got: {err:?}"
        );
    }
}

/// Authorization of the backup and restore operations: the resources each
/// one is evaluated against, and policy decisions over the input space of
/// `BackupArn` (own table, another table, another account, malformed,
/// missing) and `TargetTableName` (present, missing).
#[cfg(test)]
mod backup_authz_tests {
    use std::sync::{Arc, Mutex};

    use extenddb_auth::AuthIdentity;
    use extenddb_auth::policy::matcher::arn_match;
    use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
    use extenddb_storage::management_store::OpResult;
    use futures::future::BoxFuture;
    use serde_json::{Value, json};

    use super::{backup_authz_resources, evaluate_request_policies};
    use crate::authz_cache::{AuthzCacheConfig, CachedAuthzStore};
    use extenddb_core::error::DynamoDbError;

    const REGION: &str = "us-east-1";
    const ACCT: &str = "123456789012";
    const OTHER_ACCT: &str = "210987654321";
    const USER: &str = "alice";
    const ROLE: &str = "restorer";
    const SESSION: &str = "s1";

    fn table(name: &str) -> String {
        format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/{name}")
    }
    fn backup(table_name: &str, id: &str) -> String {
        format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/{table_name}/backup/{id}")
    }
    fn wildcard() -> String {
        table("*")
    }
    const A: &str = "01700000000000-0000000a";
    const B: &str = "01700000000000-0000000b";

    /// Policies for one user and one role; records the ARNs whose resource
    /// tags were fetched.
    #[derive(Default)]
    struct Store {
        policies: Vec<String>,
        resource_tag_lookups: Mutex<Vec<String>>,
    }

    impl AuthorizationStore for Store {
        fn fetch_user_policies(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Vec<String>>> {
            let v = self.policies.clone();
            Box::pin(async move { Ok(v) })
        }
        fn fetch_user_group_policies(
            &self,
            _: &str,
            _: &str,
        ) -> BoxFuture<'_, OpResult<Vec<String>>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn fetch_user_boundary(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
            Box::pin(async { Ok(None) })
        }
        fn fetch_role_policies(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Vec<String>>> {
            let v = self.policies.clone();
            Box::pin(async move { Ok(v) })
        }
        fn fetch_role_boundary(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
            Box::pin(async { Ok(None) })
        }
        fn fetch_session_data(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> BoxFuture<'_, OpResult<Option<SessionData>>> {
            Box::pin(async { Ok(None) })
        }
        fn fetch_user_tags(
            &self,
            _: &str,
            _: &str,
        ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn fetch_role_tags(
            &self,
            _: &str,
            _: &str,
        ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
            self.resource_tag_lookups
                .lock()
                .unwrap()
                .push(arn.to_owned());
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    /// One statement: `(effect, actions, resources)`.
    type Stmt<'a> = (&'a str, &'a [&'a str], &'a [&'a str]);

    fn policy(statements: &[Stmt<'_>]) -> String {
        let st: Vec<Value> = statements
            .iter()
            .map(|(effect, actions, resources)| {
                json!({"Effect": effect, "Action": actions, "Resource": resources})
            })
            .collect();
        json!({"Version": "2012-10-17", "Statement": st}).to_string()
    }

    fn harness(statements: &[Stmt<'_>]) -> (Arc<Store>, CachedAuthzStore) {
        let store = Arc::new(Store {
            policies: if statements.is_empty() {
                Vec::new()
            } else {
                vec![policy(statements)]
            },
            ..Store::default()
        });
        let cache = CachedAuthzStore::pass_through(store.clone(), AuthzCacheConfig::default());
        (store, cache)
    }

    fn user() -> AuthIdentity {
        AuthIdentity::User {
            account_id: ACCT.to_owned(),
            user_name: USER.to_owned(),
        }
    }

    async fn decide_as(
        cache: &CachedAuthzStore,
        identity: &AuthIdentity,
        dev_mode: bool,
        op: &str,
        input: &Value,
    ) -> Result<(), DynamoDbError> {
        evaluate_request_policies(cache, dev_mode, REGION, identity, input, op, ACCT, None).await
    }

    async fn decide(
        cache: &CachedAuthzStore,
        op: &str,
        input: &Value,
    ) -> Result<(), DynamoDbError> {
        decide_as(cache, &user(), false, op, input).await
    }

    fn assert_denied(r: &Result<(), DynamoDbError>, what: &str) {
        assert!(
            matches!(r, Err(DynamoDbError::AccessDeniedException(_))),
            "{what}: expected AccessDeniedException, got {r:?}"
        );
    }

    fn resources(op: &str, input: &Value) -> Vec<String> {
        backup_authz_resources(op, input, REGION, ACCT)
            .unwrap_or_else(|| panic!("{op} must take the backup path"))
            .unwrap_or_else(|e| panic!("{op}: unexpected error {e:?}"))
    }

    fn restore(arn: &str, target: &str) -> Value {
        json!({"BackupArn": arn, "TargetTableName": target})
    }

    /// Malformed `BackupArn` values: each is refused by `parse_backup_arn`.
    fn malformed_arns() -> Vec<String> {
        vec![
            String::new(),
            "garbage".to_owned(),
            format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/Music"),
            format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/Music/backup/"),
            format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/Music/backup/{A}/x"),
            format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/Music/stream/{A}"),
            format!("arn:aws:dynamodb:{REGION}:{ACCT}:table//backup/{A}"),
            format!("arn:aws:dynamodb:{REGION}::table/Music/backup/{A}"),
            format!("arn:aws:s3:{REGION}:{ACCT}:table/Music/backup/{A}"),
            format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/Music/backup/{A}:extra"),
            format!("dynamodb:{REGION}:{ACCT}:table/Music/backup/{A}"),
        ]
    }

    // ── Resources evaluated per operation ─────────────────────────────

    #[test]
    fn describe_and_delete_backup_evaluate_the_backup_arn() {
        for op in ["DescribeBackup", "DeleteBackup"] {
            let arn = backup("Music", A);
            assert_eq!(resources(op, &json!({"BackupArn": arn})), vec![arn]);
        }
    }

    #[test]
    fn restore_evaluates_the_backup_arn_and_the_target_table() {
        let arn = backup("Music", A);
        assert_eq!(
            resources("RestoreTableFromBackup", &restore(&arn, "Restored")),
            vec![arn, table("Restored")]
        );
    }

    #[test]
    fn restore_without_target_name_evaluates_table_wildcard_for_the_target() {
        let arn = backup("Music", A);
        assert_eq!(
            resources("RestoreTableFromBackup", &json!({"BackupArn": arn})),
            vec![arn.clone(), wildcard()]
        );
        // A non-string TargetTableName is treated as missing.
        assert_eq!(
            resources(
                "RestoreTableFromBackup",
                &json!({"BackupArn": arn, "TargetTableName": 7})
            ),
            vec![arn, wildcard()]
        );
    }

    #[test]
    fn backup_arn_for_another_table_name_is_evaluated_as_supplied() {
        // The table component of the ARN is part of the resource: a backup of
        // Other is `table/Other/backup/<id>`, whatever the request is about.
        let arn = backup("Other", A);
        for op in ["DescribeBackup", "DeleteBackup"] {
            assert_eq!(resources(op, &json!({"BackupArn": arn})), vec![arn.clone()]);
        }
        assert_eq!(
            resources("RestoreTableFromBackup", &restore(&arn, "Music")),
            vec![arn.clone(), table("Music")]
        );
    }

    #[test]
    fn backup_arn_in_another_region_is_evaluated_as_supplied() {
        // Not rewritten to the server's region: the backend resolves the exact
        // string, so this ARN names no backup this server created.
        let arn = format!("arn:aws:dynamodb:eu-west-1:{ACCT}:table/Music/backup/{A}");
        assert_eq!(
            resources("DescribeBackup", &json!({"BackupArn": arn})),
            vec![arn]
        );
    }

    #[test]
    fn backup_arn_of_another_account_is_access_denied_without_evaluation() {
        let arn = format!("arn:aws:dynamodb:{REGION}:{OTHER_ACCT}:table/Music/backup/{A}");
        for (op, input) in [
            ("DescribeBackup", json!({"BackupArn": arn})),
            ("DeleteBackup", json!({"BackupArn": arn})),
            ("RestoreTableFromBackup", restore(&arn, "Restored")),
            // The ARN account is checked before the target table name.
            ("RestoreTableFromBackup", json!({"BackupArn": arn})),
        ] {
            let r = backup_authz_resources(op, &input, REGION, ACCT).expect("backup path");
            match r {
                Err(DynamoDbError::AccessDeniedException(m)) => assert_eq!(m, "Access is denied"),
                other => panic!("{op}: expected AccessDeniedException, got {other:?}"),
            }
        }
    }

    #[test]
    fn malformed_backup_arn_evaluates_table_wildcard() {
        for bad in malformed_arns() {
            assert!(
                extenddb_storage::util::parse_backup_arn(&bad).is_none(),
                "fixture {bad:?} must be malformed"
            );
            for op in ["DescribeBackup", "DeleteBackup"] {
                assert_eq!(
                    resources(op, &json!({"BackupArn": bad})),
                    vec![wildcard()],
                    "{op} {bad:?}"
                );
            }
            assert_eq!(
                resources("RestoreTableFromBackup", &restore(&bad, "Restored")),
                vec![wildcard(), table("Restored")],
                "restore {bad:?}"
            );
        }
    }

    #[test]
    fn missing_or_non_string_backup_arn_evaluates_table_wildcard() {
        for input in [
            json!({}),
            json!({"BackupArn": null}),
            json!({"BackupArn": 1}),
        ] {
            for op in ["DescribeBackup", "DeleteBackup"] {
                assert_eq!(resources(op, &input), vec![wildcard()], "{op} {input}");
            }
        }
        assert_eq!(
            resources(
                "RestoreTableFromBackup",
                &json!({"TargetTableName": "Restored"})
            ),
            vec![wildcard(), table("Restored")]
        );
        assert_eq!(
            resources("RestoreTableFromBackup", &json!({})),
            vec![wildcard(), wildcard()]
        );
    }

    #[test]
    fn list_backups_evaluates_table_wildcard_even_with_a_table_filter() {
        assert_eq!(resources("ListBackups", &json!({})), vec![wildcard()]);
        assert_eq!(
            resources("ListBackups", &json!({"TableName": "Music"})),
            vec![wildcard()]
        );
    }

    #[test]
    fn point_in_time_restore_evaluates_the_source_table() {
        let op = "RestoreTableToPointInTime";
        assert_eq!(
            resources(
                op,
                &json!({"SourceTableName": "Music", "TargetTableName": "R"})
            ),
            vec![table("Music")]
        );
        assert_eq!(
            resources(
                op,
                &json!({"SourceTableArn": table("Music"), "TargetTableName": "R"})
            ),
            vec![wildcard()]
        );
        assert_eq!(
            resources(
                op,
                &json!({"SourceTableName": "Music", "SourceTableArn": table("Music")})
            ),
            vec![table("Music"), wildcard()]
        );
        assert_eq!(
            resources(op, &json!({"TargetTableName": "R"})),
            vec![wildcard()]
        );
    }

    #[test]
    fn table_scoped_backup_operations_stay_on_the_generic_path() {
        for op in [
            "CreateBackup",
            "DescribeContinuousBackups",
            "UpdateContinuousBackups",
            "GetItem",
            "ListTables",
        ] {
            assert!(
                backup_authz_resources(op, &json!({"TableName": "Music"}), REGION, ACCT).is_none(),
                "{op}"
            );
        }
    }

    // ── Policy matcher semantics the model relies on ──────────────────

    #[test]
    fn arn_wildcard_crosses_slash_and_a_table_arn_does_not_cover_its_backups() {
        let b = backup("Music", A);
        assert!(
            arn_match(&wildcard(), &b),
            "table/* must match a backup ARN"
        );
        assert!(arn_match("*", &b));
        assert!(arn_match("arn:aws:dynamodb:*:*:table/*", &b));
        assert!(arn_match(
            &format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/Music/backup/*"),
            &b
        ));
        assert!(
            !arn_match(&table("Music"), &b),
            "table/Music is not table/Music/backup/x"
        );
        assert!(!arn_match(&backup("Music", B), &b));
        assert!(!arn_match(
            &format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/Other/backup/*"),
            &b
        ));
        // The wildcard resource itself is matched by table/* only literally.
        assert!(arn_match(&wildcard(), &wildcard()));
        assert!(!arn_match(&backup("Music", "*"), &wildcard()));
        assert!(!arn_match(&table("Music"), &wildcard()));
    }

    // ── Decisions ─────────────────────────────────────────────────────

    const BACKUP_OPS: &[&str] = &[
        "dynamodb:CreateBackup",
        "dynamodb:DescribeBackup",
        "dynamodb:DeleteBackup",
        "dynamodb:ListBackups",
        "dynamodb:RestoreTableFromBackup",
        "dynamodb:DescribeContinuousBackups",
        "dynamodb:UpdateContinuousBackups",
        "dynamodb:RestoreTableToPointInTime",
    ];

    fn every_op_input() -> Vec<(&'static str, Value)> {
        vec![
            (
                "CreateBackup",
                json!({"TableName": "Music", "BackupName": "b"}),
            ),
            ("DescribeBackup", json!({"BackupArn": backup("Music", A)})),
            ("DeleteBackup", json!({"BackupArn": backup("Music", A)})),
            ("ListBackups", json!({})),
            ("ListBackups", json!({"TableName": "Music"})),
            (
                "RestoreTableFromBackup",
                restore(&backup("Music", A), "Restored"),
            ),
            ("DescribeContinuousBackups", json!({"TableName": "Music"})),
            (
                "UpdateContinuousBackups",
                json!({"TableName": "Music", "PointInTimeRecoverySpecification": {}}),
            ),
            (
                "RestoreTableToPointInTime",
                json!({"SourceTableName": "Music", "TargetTableName": "R"}),
            ),
        ]
    }

    #[tokio::test]
    async fn table_wildcard_policy_still_allows_every_backup_operation() {
        for resource in [
            wildcard(),
            "arn:aws:dynamodb:*:*:table/*".to_owned(),
            "*".to_owned(),
        ] {
            let (_, cache) = harness(&[("Allow", BACKUP_OPS, &[resource.as_str()])]);
            for (op, input) in every_op_input() {
                let r = decide(&cache, op, &input).await;
                assert!(r.is_ok(), "{op} under Allow {resource}: {r:?}");
            }
        }
    }

    #[tokio::test]
    async fn no_policy_denies_every_backup_operation() {
        let (_, cache) = harness(&[]);
        for (op, input) in every_op_input() {
            assert_denied(&decide(&cache, op, &input).await, op);
        }
    }

    #[tokio::test]
    async fn dev_mode_bypasses_policy_evaluation_for_every_backup_operation() {
        let (store, cache) = harness(&[]);
        let foreign = format!("arn:aws:dynamodb:{REGION}:{OTHER_ACCT}:table/Music/backup/{A}");
        let mut inputs = every_op_input();
        // Another account's ARN passes authz in dev mode; the engine still
        // refuses it with AccessDeniedException.
        inputs.push(("DescribeBackup", json!({"BackupArn": foreign})));
        inputs.push(("DescribeBackup", json!({"BackupArn": "garbage"})));
        for (op, input) in inputs {
            let r = decide_as(&cache, &user(), true, op, &input).await;
            assert!(r.is_ok(), "{op} in dev mode: {r:?}");
        }
        assert!(store.resource_tag_lookups.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn explicit_deny_on_one_backup_denies_only_that_backup() {
        let denied = backup("Music", A);
        let (_, cache) = harness(&[
            ("Allow", BACKUP_OPS, &["*"]),
            (
                "Deny",
                &[
                    "dynamodb:DescribeBackup",
                    "dynamodb:DeleteBackup",
                    "dynamodb:RestoreTableFromBackup",
                ],
                &[denied.as_str()],
            ),
        ]);
        let other = backup("Music", B);
        for op in ["DescribeBackup", "DeleteBackup"] {
            assert_denied(
                &decide(&cache, op, &json!({"BackupArn": denied})).await,
                &format!("{op} of the denied backup"),
            );
            let r = decide(&cache, op, &json!({"BackupArn": other})).await;
            assert!(r.is_ok(), "{op} of another backup: {r:?}");
        }
        assert_denied(
            &decide(
                &cache,
                "RestoreTableFromBackup",
                &restore(&denied, "Restored"),
            )
            .await,
            "restore of the denied backup",
        );
        assert!(
            decide(
                &cache,
                "RestoreTableFromBackup",
                &restore(&other, "Restored")
            )
            .await
            .is_ok()
        );
        // Without a target name the denied backup is still checked first.
        assert_denied(
            &decide(
                &cache,
                "RestoreTableFromBackup",
                &json!({"BackupArn": denied}),
            )
            .await,
            "restore of the denied backup without a target name",
        );
    }

    #[tokio::test]
    async fn deny_on_a_table_backups_does_not_cover_another_tables_backups() {
        let (_, cache) = harness(&[
            ("Allow", BACKUP_OPS, &["*"]),
            (
                "Deny",
                &["dynamodb:DescribeBackup"],
                &[&format!(
                    "arn:aws:dynamodb:{REGION}:{ACCT}:table/Music/backup/*"
                )],
            ),
        ]);
        assert_denied(
            &decide(
                &cache,
                "DescribeBackup",
                &json!({"BackupArn": backup("Music", A)}),
            )
            .await,
            "Music backup",
        );
        assert!(
            decide(
                &cache,
                "DescribeBackup",
                &json!({"BackupArn": backup("Other", A)})
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn restore_needs_both_the_backup_and_the_target_table() {
        let music_backups = format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/Music/backup/*");
        let restored = table("Restored");
        let action: &[&str] = &["dynamodb:RestoreTableFromBackup"];

        let (_, both) = harness(&[(
            "Allow",
            action,
            &[music_backups.as_str(), restored.as_str()],
        )]);
        let input = restore(&backup("Music", A), "Restored");
        let r = decide(&both, "RestoreTableFromBackup", &input).await;
        assert!(r.is_ok(), "backup and target allowed: {r:?}");
        // Same allow, statements split.
        let (_, split) = harness(&[
            ("Allow", action, &[music_backups.as_str()]),
            ("Allow", action, &[restored.as_str()]),
        ]);
        assert!(
            decide(&split, "RestoreTableFromBackup", &input)
                .await
                .is_ok()
        );

        let (_, backup_only) = harness(&[("Allow", action, &[music_backups.as_str()])]);
        assert_denied(
            &decide(&backup_only, "RestoreTableFromBackup", &input).await,
            "target not allowed",
        );
        let (_, target_only) = harness(&[("Allow", action, &[restored.as_str()])]);
        assert_denied(
            &decide(&target_only, "RestoreTableFromBackup", &input).await,
            "backup not allowed",
        );

        // Other cells under the backup+target allow.
        for (input, what) in [
            (restore(&backup("Music", A), "Elsewhere"), "another target"),
            (
                restore(&backup("Other", A), "Restored"),
                "another table's backup",
            ),
            (
                json!({"BackupArn": backup("Music", A)}),
                "missing target name",
            ),
            (json!({"TargetTableName": "Restored"}), "missing backup ARN"),
            (restore("garbage", "Restored"), "malformed backup ARN"),
        ] {
            assert_denied(&decide(&both, "RestoreTableFromBackup", &input).await, what);
        }
    }

    #[tokio::test]
    async fn restore_as_role_session_uses_the_same_resources() {
        let music_backups = format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/Music/backup/*");
        let restored = table("Restored");
        let role = AuthIdentity::RoleSession {
            account_id: ACCT.to_owned(),
            role_name: ROLE.to_owned(),
            session_name: SESSION.to_owned(),
        };
        let action: &[&str] = &["dynamodb:RestoreTableFromBackup"];
        let (_, both) = harness(&[(
            "Allow",
            action,
            &[music_backups.as_str(), restored.as_str()],
        )]);
        let input = restore(&backup("Music", A), "Restored");
        assert!(
            decide_as(&both, &role, false, "RestoreTableFromBackup", &input)
                .await
                .is_ok()
        );
        let (_, backup_only) = harness(&[("Allow", action, &[music_backups.as_str()])]);
        assert_denied(
            &decide_as(&backup_only, &role, false, "RestoreTableFromBackup", &input).await,
            "role: target not allowed",
        );
    }

    #[tokio::test]
    async fn table_arn_alone_does_not_allow_operations_on_its_backups() {
        // Behaviour change: before, these were evaluated against table/* and
        // denied by this policy too; now they are evaluated against the backup
        // ARN, which table/Music does not match either. A policy that should
        // cover a table's backups names table/Music/backup/*.
        let music = table("Music");
        let (_, cache) = harness(&[("Allow", BACKUP_OPS, &[music.as_str()])]);
        for op in ["DescribeBackup", "DeleteBackup"] {
            assert_denied(
                &decide(&cache, op, &json!({"BackupArn": backup("Music", A)})).await,
                op,
            );
        }
        // Table-scoped operations on Music are allowed by it.
        for (op, input) in [
            (
                "CreateBackup",
                json!({"TableName": "Music", "BackupName": "b"}),
            ),
            ("DescribeContinuousBackups", json!({"TableName": "Music"})),
            ("UpdateContinuousBackups", json!({"TableName": "Music"})),
            (
                "RestoreTableToPointInTime",
                json!({"SourceTableName": "Music", "TargetTableName": "R"}),
            ),
        ] {
            let r = decide(&cache, op, &input).await;
            assert!(r.is_ok(), "{op}: {r:?}");
        }
        // ...and not for another table.
        assert_denied(
            &decide(&cache, "CreateBackup", &json!({"TableName": "Other"})).await,
            "CreateBackup of Other",
        );
    }

    #[tokio::test]
    async fn list_backups_cannot_be_granted_or_denied_per_table() {
        // Behaviour change: a TableName filter used to make ListBackups
        // evaluate table/<TableName>. The service has no resource type for
        // ListBackups, and the unfiltered list returns the same summaries.
        let music = table("Music");
        let action: &[&str] = &["dynamodb:ListBackups"];
        let (_, scoped) = harness(&[("Allow", action, &[music.as_str()])]);
        assert_denied(
            &decide(&scoped, "ListBackups", &json!({"TableName": "Music"})).await,
            "ListBackups under Allow table/Music",
        );
        let (_, all) = harness(&[("Allow", action, &["*"])]);
        assert!(
            decide(&all, "ListBackups", &json!({"TableName": "Music"}))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn another_accounts_backup_is_denied_and_its_tags_are_not_read() {
        let foreign = format!("arn:aws:dynamodb:{REGION}:{OTHER_ACCT}:table/Music/backup/{A}");
        let (store, cache) = harness(&[("Allow", BACKUP_OPS, &["*"])]);
        for (op, input) in [
            ("DescribeBackup", json!({"BackupArn": foreign})),
            ("DeleteBackup", json!({"BackupArn": foreign})),
            ("RestoreTableFromBackup", restore(&foreign, "Restored")),
        ] {
            assert_denied(&decide(&cache, op, &input).await, op);
        }
        assert!(
            store.resource_tag_lookups.lock().unwrap().is_empty(),
            "no resource lookups for a refused request: {:?}",
            store.resource_tag_lookups.lock().unwrap()
        );
        // Liveness: an own-account backup under the same policy is evaluated,
        // with its tags read.
        let own = backup("Music", A);
        assert!(
            decide(&cache, "DescribeBackup", &json!({"BackupArn": own}))
                .await
                .is_ok()
        );
        assert_eq!(*store.resource_tag_lookups.lock().unwrap(), vec![own]);
    }

    #[tokio::test]
    async fn malformed_or_missing_backup_arn_is_no_more_permissive_than_table_wildcard() {
        // Under a backup-scoped allow, a request naming no valid backup is
        // denied (it would need table/*). Under table/* it reaches the engine,
        // which answers ValidationException.
        let music_backups = format!("arn:aws:dynamodb:{REGION}:{ACCT}:table/Music/backup/*");
        let action: &[&str] = &["dynamodb:DescribeBackup", "dynamodb:DeleteBackup"];
        let (_, scoped) = harness(&[("Allow", action, &[music_backups.as_str()])]);
        let (_, all) = harness(&[("Allow", action, &[wildcard().as_str()])]);
        let mut inputs: Vec<Value> = malformed_arns()
            .into_iter()
            .map(|a| json!({"BackupArn": a}))
            .collect();
        inputs.push(json!({}));
        inputs.push(json!({"BackupArn": 5}));
        for input in &inputs {
            for op in ["DescribeBackup", "DeleteBackup"] {
                assert_denied(&decide(&scoped, op, input).await, &format!("{op} {input}"));
                let r = decide(&all, op, input).await;
                assert!(r.is_ok(), "{op} {input} under table/*: {r:?}");
            }
        }
        // Liveness for the scoped policy: a well-formed Music backup passes.
        assert!(
            decide(
                &scoped,
                "DescribeBackup",
                &json!({"BackupArn": backup("Music", A)})
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn malformed_backup_arn_still_honours_a_deny_on_table_wildcard() {
        let (_, cache) = harness(&[
            ("Allow", BACKUP_OPS, &["*"]),
            ("Deny", &["dynamodb:DescribeBackup"], &[wildcard().as_str()]),
        ]);
        assert_denied(
            &decide(&cache, "DescribeBackup", &json!({"BackupArn": "garbage"})).await,
            "malformed under Deny table/*",
        );
        // table/* also covers every backup ARN.
        assert_denied(
            &decide(
                &cache,
                "DescribeBackup",
                &json!({"BackupArn": backup("Music", A)}),
            )
            .await,
            "backup under Deny table/*",
        );
    }
}
