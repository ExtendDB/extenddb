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
    let resource_arn = build_resource_arn(&state.region, account_id, table_name.as_deref());

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

    let params = extenddb_auth::policy::context::RequestParams {
        leading_keys: extract_leading_keys(input, operation, pk_attr.as_deref()),
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
