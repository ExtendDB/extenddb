// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request parsing and authorization helpers for the `DynamoDB` wire protocol.

use std::collections::HashMap;

use axum::http::HeaderMap;
use extenddb_core::error::DynamoDbError;
use extenddb_storage::error::StorageError;
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
/// names in nested structures, so this returns `None` for those; the caller
/// maps `None` to `*` via `build_resource_arn`.
pub(crate) fn extract_table_name(input: &Value) -> Option<String> {
    input
        .get("TableName")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
}

/// Decide how a `TableKeyInfo` lookup result feeds request authorization.
///
/// A present table yields its key info. A missing table yields `None`: the
/// `dynamodb:LeadingKeys` value stays absent and the engine returns
/// `ResourceNotFoundException`, which is what the service returns for a
/// LeadingKeys-restricted principal on a missing table (measured 2026-10-20).
///
/// Any other lookup failure (connection, transient, backend internal) must not
/// reach the policy evaluator with the key absent. A
/// `ForAllValues:StringEquals dynamodb:LeadingKeys` condition is satisfied
/// vacuously when the key is absent, so a request whose key could not be
/// determined would be authorized and the engine could then read the real
/// table on its own lookup. Such a failure returns an error before dispatch, so
/// no request is ever authorized on a key the server could not determine.
///
/// The error is classified by the engine's own `storage_err_to_dynamo`, so the
/// server and the engine return the same wire error for the same storage fault:
/// a transient or connection fault is `ServiceUnavailable` (503, SDK-retryable),
/// the catalog and backend-internal faults are `InternalServerError` (500), and
/// a not-active table is `ResourceInUseException`. Using one mapper keeps a
/// database hiccup from returning 503 when the engine hits it and 500 when
/// authorization hits it first. The cache reports a missing table as
/// `StorageError::TableNotFound` and every other fault as a distinct error, so
/// the two are told apart here rather than collapsed to `None`. Every fault
/// class fails closed: none authorizes the request.
fn key_info_for_authz(
    result: Result<extenddb_core::types::TableKeyInfo, StorageError>,
) -> Result<Option<extenddb_core::types::TableKeyInfo>, DynamoDbError> {
    match result {
        Ok(info) => Ok(Some(info)),
        Err(StorageError::TableNotFound(_)) => Ok(None),
        Err(e) => Err(extenddb_engine::storage_err_to_dynamo(e)),
    }
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
                key_info_for_authz(state.table_key_info_cache.get(account_id, tn).await)?
            } else {
                None
            }
        }
        _ => None,
    };

    // Developer mode opens authorization for the authenticated caller. SigV4
    // verification already ran upstream, so the request is authenticated; only
    // the IAM policy decision is skipped. key_info is still returned so the
    // engine layer can reuse it.
    //
    // This check is deliberately positioned ahead of EVERY authorization branch
    // below, not merely the generic one. It previously sat after the batch and
    // transaction branch, which returns early, so those four operations were
    // still evaluated against IAM and denied in a dev-mode build while single
    // item operations passed. Any future operation-specific branch must be added
    // below this point, or it will reintroduce that defect.
    if state.dev_mode {
        return Ok(key_info);
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
    // `batch_transact_targets` returns `Some` for exactly the batch and
    // transaction operations. Batch operations authorize once per table with
    // the set of keys that table receives; transaction operations authorize
    // once per item with that item's key. Each target carries the
    // `dynamodb:LeadingKeys` value(s) the service evaluates, extracted per the
    // table's key schema. The resource ARN per target is unchanged: each is
    // still that table's ARN. (Measured against live DynamoDB 2026-10-20.)
    if let Some(skeleton) =
        crate::leading_keys::batch_transact_targets(operation, input, &HashMap::new())
    {
        // Fetch each distinct table's key info for partition-key extraction.
        let tables: std::collections::BTreeSet<String> =
            skeleton.iter().map(|t| t.table.clone()).collect();
        let mut key_info_by_table = HashMap::new();
        for table in tables {
            if let Some(ki) =
                key_info_for_authz(state.table_key_info_cache.get(account_id, &table).await)?
            {
                key_info_by_table.insert(table, ki);
            }
        }
        let targets =
            crate::leading_keys::batch_transact_targets(operation, input, &key_info_by_table)
                .unwrap_or(skeleton);

        if targets.is_empty() {
            // Malformed or empty body: still require the operation's action on
            // the table wildcard, as the generic path would, then let the
            // engine reject the body. This never silently skips authorization.
            let resource_arn = build_resource_arn(&state.region, account_id, None);
            authorization::check_authorization(
                state.authz_cache.as_ref(),
                identity,
                operation,
                &resource_arn,
                false,
                extenddb_auth::policy::context::RequestParams::default(),
            )
            .await?;
        } else {
            for target in targets {
                let resource_arn =
                    build_resource_arn(&state.region, account_id, Some(&target.table));
                let params = extenddb_auth::policy::context::RequestParams {
                    leading_keys: target.leading_keys,
                    ..Default::default()
                };
                authorization::check_authorization(
                    state.authz_cache.as_ref(),
                    identity,
                    &target.action,
                    &resource_arn,
                    false,
                    params,
                )
                .await?;
            }
        }
        return Ok(None);
    }

    let resource_arn = build_resource_arn(&state.region, account_id, table_name.as_deref());

    let params = extenddb_auth::policy::context::RequestParams {
        leading_keys: crate::leading_keys::single_table_leading_keys(
            input,
            operation,
            key_info.as_ref(),
            state.limits.max_expression_tokens,
        ),
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
        state.authz_cache.as_ref(),
        identity,
        operation,
        &resource_arn,
        operation == "Scan",
        params,
    )
    .await?;

    Ok(key_info)
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
    use extenddb_core::types::TableKeyInfo;
    use serde_json::json;

    fn sample_key_info() -> TableKeyInfo {
        TableKeyInfo {
            table_name: "t".to_owned(),
            account_id: "123".to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn key_info_for_authz_present_table_returns_key_info() {
        let out =
            key_info_for_authz(Ok(sample_key_info())).expect("a present table is not an error");
        assert!(out.is_some(), "a present table must yield its key info");
    }

    #[test]
    fn key_info_for_authz_missing_table_returns_none() {
        let out = key_info_for_authz(Err(StorageError::TableNotFound("t".to_owned())))
            .expect("a missing table is not a lookup failure");
        assert!(
            out.is_none(),
            "a missing table must leave the leading key absent so the engine returns not-found"
        );
    }

    #[test]
    fn key_info_for_authz_lookup_error_fails_closed() {
        // Each non-not-found fault fails closed, classified the same way the
        // engine classifies it: transient and connection faults are retryable
        // 503s, the catalog and backend-internal faults are 500s, and a
        // not-active table is the engine's ResourceInUseException. None yields
        // an Ok(Some)/Ok(None) that would authorize the request.
        let service_unavailable = [
            StorageError::Transient("db hiccup".to_owned()),
            StorageError::Connection("pool timeout".to_owned()),
        ];
        for err in service_unavailable {
            let out = key_info_for_authz(Err(err.clone()));
            assert!(
                matches!(out, Err(DynamoDbError::ServiceUnavailable(_))),
                "a transient or connection fault must map to ServiceUnavailable, got: {out:?} for {err:?}"
            );
        }

        let internal = [
            StorageError::Internal("backend fault".to_owned()),
            StorageError::CatalogVersionMismatch {
                expected: "3".to_owned(),
                found: "2".to_owned(),
            },
            StorageError::CatalogNotInitialized,
        ];
        for err in internal {
            let out = key_info_for_authz(Err(err.clone()));
            assert!(
                matches!(out, Err(DynamoDbError::InternalServerError(_))),
                "a catalog or backend-internal fault must map to InternalServerError, got: {out:?} for {err:?}"
            );
        }

        // TableNotActive maps to whatever the engine maps it to, so the server
        // and the engine agree on a not-active table. It still fails closed (an
        // Err, not an authorizing Ok).
        let out = key_info_for_authz(Err(StorageError::TableNotActive("t".to_owned())));
        let engine = extenddb_engine::storage_err_to_dynamo(StorageError::TableNotActive(
            "t".to_owned(),
        ));
        assert!(
            matches!(out, Err(ref e) if std::mem::discriminant(e) == std::mem::discriminant(&engine)),
            "TableNotActive must map to the engine's error class ({engine:?}), got: {out:?}"
        );
        assert!(
            matches!(engine, DynamoDbError::ResourceInUseException(_)),
            "the engine maps TableNotActive to ResourceInUseException, got: {engine:?}"
        );
    }

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
