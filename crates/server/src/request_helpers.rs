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

/// Normalize any table ARN supplied in place of a bare table name.
///
/// A table ARN is accepted wherever a `TableName` is expected, matching Amazon
/// DynamoDB and DynamoDB Local. This rewrites the wire request in place so
/// validation, authorization, throttling, and the storage lookup all operate on
/// the bare name. It returns the `(bare, original)` pairs for any name that was
/// an ARN, so the response layer can echo the caller's original ARN back in
/// `ConsumedCapacity.TableName` and batch response keys, matching Amazon
/// DynamoDB (which echoes the supplied ARN verbatim).
pub(crate) fn normalize_table_arns(
    input: &mut Value,
    operation: &str,
) -> Result<Vec<(String, String)>, DynamoDbError> {
    let mut echo: Vec<(String, String)> = Vec::new();
    match operation {
        "GetItem" | "PutItem" | "DeleteItem" | "UpdateItem" | "Query" | "Scan" => {
            resolve_table_name_field(input, &mut echo)?;
        }
        "BatchGetItem" | "BatchWriteItem" => normalize_request_items_keys(input, &mut echo)?,
        "TransactGetItems" | "TransactWriteItems" => normalize_transact_items(input, &mut echo)?,
        _ => {}
    }
    Ok(echo)
}

/// Restore the caller's original ARN in the echoed table names of a response.
///
/// Swaps the bare name back to the supplied ARN in `ConsumedCapacity.TableName`
/// (object or per-table array) and in the table-name-keyed response maps for the
/// operation, so the response mirrors what the caller sent, as Amazon DynamoDB
/// does. The keyed-map set is operation-scoped: `ItemCollectionMetrics` is a
/// table-keyed map only for batch-write/transact-write; for single-item writes
/// it is a field-keyed object and must not be rewritten.
pub(crate) fn denormalize_table_arns(body: &mut Value, operation: &str, echo: &[(String, String)]) {
    if echo.is_empty() {
        return;
    }
    let bare_to_original: std::collections::HashMap<&str, &str> =
        echo.iter().map(|(b, o)| (b.as_str(), o.as_str())).collect();

    if let Some(cc) = body.get_mut("ConsumedCapacity") {
        match cc {
            Value::Array(entries) => {
                for entry in entries {
                    swap_table_name_field(entry, &bare_to_original);
                }
            }
            other => swap_table_name_field(other, &bare_to_original),
        }
    }
    let table_keyed_maps: &[&str] = match operation {
        "BatchGetItem" => &["Responses", "UnprocessedKeys"],
        "BatchWriteItem" => &["UnprocessedItems", "ItemCollectionMetrics"],
        "TransactWriteItems" => &["ItemCollectionMetrics"],
        _ => &[],
    };
    for key in table_keyed_maps {
        if let Some(map) = body.get_mut(*key).and_then(Value::as_object_mut) {
            rename_map_keys(map, &bare_to_original);
        }
    }
}

fn swap_table_name_field(
    entry: &mut Value,
    bare_to_original: &std::collections::HashMap<&str, &str>,
) {
    if let Some(name) = entry.get("TableName").and_then(Value::as_str)
        && let Some(original) = bare_to_original.get(name)
    {
        entry["TableName"] = Value::String((*original).to_owned());
    }
}

fn rename_map_keys(
    map: &mut serde_json::Map<String, Value>,
    bare_to_original: &std::collections::HashMap<&str, &str>,
) {
    let renames: Vec<(String, String)> = map
        .keys()
        .filter_map(|k| {
            bare_to_original
                .get(k.as_str())
                .map(|o| (k.clone(), (*o).to_owned()))
        })
        .collect();
    for (bare, original) in renames {
        if let Some(value) = map.remove(&bare) {
            map.insert(original, value);
        }
    }
}

/// Resolve `input["TableName"]` if it is a string ARN, recording the swap.
fn resolve_table_name_field(
    input: &mut Value,
    echo: &mut Vec<(String, String)>,
) -> Result<(), DynamoDbError> {
    if let Some(name) = input.get("TableName").and_then(Value::as_str) {
        let resolved = extenddb_core::validation::resolve_table_arn(name)?;
        if resolved != name {
            let pair = (resolved.to_owned(), name.to_owned());
            input["TableName"] = Value::String(pair.0.clone());
            echo.push(pair);
        }
    }
    Ok(())
}

/// Batch operations key `RequestItems` by table name. Rebuild the map with any
/// ARN keys resolved to bare names, recording each swap for response echo. When
/// an ARN key and a bare key resolve to the same table, the entries collapse to
/// one; Amazon DynamoDB likewise collapses duplicate table references rather
/// than rejecting them.
fn normalize_request_items_keys(
    input: &mut Value,
    echo: &mut Vec<(String, String)>,
) -> Result<(), DynamoDbError> {
    let Some(items) = input.get_mut("RequestItems").and_then(Value::as_object_mut) else {
        return Ok(());
    };
    if !items.keys().any(|k| k.starts_with("arn:")) {
        return Ok(());
    }
    let mut rebuilt = serde_json::Map::with_capacity(items.len());
    for (key, value) in std::mem::take(items) {
        let resolved = extenddb_core::validation::resolve_table_arn(&key)?;
        if resolved != key {
            echo.push((resolved.to_owned(), key.clone()));
        }
        rebuilt.insert(resolved.to_owned(), value);
    }
    *items = rebuilt;
    Ok(())
}

/// Transact operations carry a `TableName` inside each sub-operation object.
fn normalize_transact_items(
    input: &mut Value,
    echo: &mut Vec<(String, String)>,
) -> Result<(), DynamoDbError> {
    let Some(items) = input.get_mut("TransactItems").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    for item in items {
        let Some(sub_op) = item.as_object_mut() else {
            continue;
        };
        for value in sub_op.values_mut() {
            resolve_table_name_field(value, echo)?;
        }
    }
    Ok(())
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
mod arn_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn top_level_table_name_arn_is_resolved() {
        let mut input = json!({
            "TableName": "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl",
            "Key": {"pk": {"S": "a"}}
        });
        normalize_table_arns(&mut input, "GetItem").unwrap();
        assert_eq!(input["TableName"], json!("Tbl"));
    }

    #[test]
    fn top_level_arn_echo_pair_is_recorded() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl";
        let mut input = json!({"TableName": arn});
        let echo = normalize_table_arns(&mut input, "PutItem").unwrap();
        assert_eq!(echo, vec![("Tbl".to_owned(), arn.to_owned())]);
    }

    #[test]
    fn bare_table_name_is_untouched() {
        let mut input = json!({"TableName": "Tbl"});
        normalize_table_arns(&mut input, "PutItem").unwrap();
        assert_eq!(input["TableName"], json!("Tbl"));
    }

    #[test]
    fn index_arn_table_name_is_rejected() {
        let mut input = json!({
            "TableName": "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl/index/i"
        });
        let err = normalize_table_arns(&mut input, "Query").unwrap_err();
        assert!(matches!(err, DynamoDbError::ValidationException(_)));
    }

    #[test]
    fn batch_request_items_keys_are_resolved() {
        let mut input = json!({
            "RequestItems": {
                "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl": {
                    "Keys": [{"pk": {"S": "a"}}]
                }
            }
        });
        normalize_table_arns(&mut input, "BatchGetItem").unwrap();
        let items = input["RequestItems"].as_object().unwrap();
        assert!(items.contains_key("Tbl"));
        assert!(!items.keys().any(|k| k.starts_with("arn:")));
    }

    #[test]
    fn transact_item_table_names_are_resolved() {
        let mut input = json!({
            "TransactItems": [
                {"Put": {"TableName": "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl",
                          "Item": {"pk": {"S": "a"}}}},
                {"Delete": {"TableName": "other", "Key": {"pk": {"S": "b"}}}}
            ]
        });
        normalize_table_arns(&mut input, "TransactWriteItems").unwrap();
        assert_eq!(input["TransactItems"][0]["Put"]["TableName"], json!("Tbl"));
        assert_eq!(
            input["TransactItems"][1]["Delete"]["TableName"],
            json!("other")
        );
    }

    #[test]
    fn denormalize_restores_arn_in_consumed_capacity_object() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl";
        let echo = vec![("Tbl".to_owned(), arn.to_owned())];
        let mut body = json!({"ConsumedCapacity": {"TableName": "Tbl", "CapacityUnits": 0.5}});
        denormalize_table_arns(&mut body, "GetItem", &echo);
        assert_eq!(body["ConsumedCapacity"]["TableName"], json!(arn));
    }

    #[test]
    fn denormalize_restores_arn_in_consumed_capacity_array() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl";
        let echo = vec![("Tbl".to_owned(), arn.to_owned())];
        let mut body = json!({"ConsumedCapacity": [{"TableName": "Tbl", "CapacityUnits": 1.0}]});
        denormalize_table_arns(&mut body, "BatchGetItem", &echo);
        assert_eq!(body["ConsumedCapacity"][0]["TableName"], json!(arn));
    }

    #[test]
    fn denormalize_restores_arn_in_batch_response_keys() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl";
        let echo = vec![("Tbl".to_owned(), arn.to_owned())];
        let mut body = json!({
            "Responses": {"Tbl": [{"pk": {"S": "a"}}]},
            "UnprocessedKeys": {}
        });
        denormalize_table_arns(&mut body, "BatchGetItem", &echo);
        let responses = body["Responses"].as_object().unwrap();
        assert!(responses.contains_key(arn));
        assert!(!responses.contains_key("Tbl"));
    }

    #[test]
    fn denormalize_batch_echo_is_selective_per_table() {
        // One ARN table and one bare table in the same batch: only the ARN
        // table's key is rewritten; the bare table's key is left untouched.
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl";
        let echo = vec![("Tbl".to_owned(), arn.to_owned())];
        let mut body = json!({
            "Responses": {"Tbl": [{"pk": {"S": "a"}}], "Other": [{"pk": {"S": "b"}}]}
        });
        denormalize_table_arns(&mut body, "BatchGetItem", &echo);
        let responses = body["Responses"].as_object().unwrap();
        assert!(responses.contains_key(arn));
        assert!(responses.contains_key("Other"));
        assert!(!responses.contains_key("Tbl"));
    }

    #[test]
    fn denormalize_leaves_single_op_item_collection_metrics_untouched() {
        // For single-item writes ItemCollectionMetrics is field-keyed, not
        // table-keyed, so its keys must never be rewritten.
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl";
        let echo = vec![("Tbl".to_owned(), arn.to_owned())];
        let mut body = json!({
            "ItemCollectionMetrics": {"ItemCollectionKey": {}, "SizeEstimateRangeGB": [0.0, 1.0]}
        });
        denormalize_table_arns(&mut body, "PutItem", &echo);
        let icm = body["ItemCollectionMetrics"].as_object().unwrap();
        assert!(icm.contains_key("ItemCollectionKey"));
        assert!(icm.contains_key("SizeEstimateRangeGB"));
    }

    #[test]
    fn denormalize_is_noop_without_echo() {
        let mut body = json!({"ConsumedCapacity": {"TableName": "Tbl"}});
        denormalize_table_arns(&mut body, "GetItem", &[]);
        assert_eq!(body["ConsumedCapacity"]["TableName"], json!("Tbl"));
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
