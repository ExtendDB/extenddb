// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request parsing and authorization helpers for the `DynamoDB` wire protocol.

use axum::http::HeaderMap;
use extenddb_core::error::DynamoDbError;
use serde_json::Value;

use crate::AppState;
use crate::authorization;
use crate::authz_resource;

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

/// Extract the top-level `TableName` from a request body.
///
/// Used to pre-fetch table key info for item operations. Batch and transact
/// operations embed table names in nested structures and carry no top-level
/// `TableName`, so this returns `None` for them. The authorization resource is
/// derived separately by `authz_resource::authz_targets`, which never falls
/// back to a `table/*` wildcard.
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

    // TableName may be a table ARN. dynamodb:LeadingKeys and the other item
    // condition keys are read with the table's key schema, so key info is
    // looked up by the table name the ARN names (this account and region only).
    // Without this, the lookup misses, LeadingKeys is absent, and a
    // ForAllValues condition is satisfied vacuously. The key info found this
    // way is used for authorization only and is not handed to the engine, which
    // resolves the request's TableName itself as before.
    let named_by_arn = table_name.as_deref().is_some_and(|t| t.starts_with("arn:"));

    // P118: Fetch table_key_info for item-level operations via the SWR cache.
    // The result is used for LeadingKeys extraction here AND returned to the
    // caller to avoid a redundant fetch in the engine layer.
    let key_info = match operation {
        "GetItem" | "PutItem" | "DeleteItem" | "UpdateItem" | "Query" | "Scan" => {
            if let Some(ref tn) = table_name {
                let lookup = authz_resource::key_lookup_name(tn, &state.region, account_id);
                state
                    .table_key_info_cache
                    .get_optional(account_id, &lookup)
                    .await
            } else {
                None
            }
        }
        _ => None,
    };
    let engine_key_info = |ki: Option<extenddb_core::types::TableKeyInfo>| {
        if named_by_arn { None } else { ki }
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
        return Ok(engine_key_info(key_info));
    }

    // DescribeEndpoints needs a signed request (SigV4 verification already ran
    // upstream) but no IAM permission, as on DynamoDB: a caller with no policy,
    // or with an explicit Deny on dynamodb:DescribeEndpoints or dynamodb:*, gets
    // the endpoint list (measured 2026-10-20).
    if operation == "DescribeEndpoints" {
        return Ok(None);
    }

    // Tag operations validate the ResourceArn shape before the IAM decision, as
    // the service does: an index ARN (tags are accepted only on a table or
    // stream) is rejected with ValidationException for every principal, before
    // authorization. A table or stream ARN passes; its account, region, and
    // existence are checked after authorization by the engine tag handler.
    if matches!(
        operation,
        "TagResource" | "UntagResource" | "ListTagsOfResource"
    ) && let Some(arn) = input.get("ResourceArn").and_then(Value::as_str)
        && !arn.is_empty()
    {
        authz_resource::validate_tag_resource_arn_shape(arn)?;
    }

    // GetRecords carries its resource only inside the opaque shard iterator, not
    // in a request body field. Decode the iterator for the stream ARN and
    // authorize against that specific stream, as the service does. The engine
    // separately confirms the shard belongs to that stream before reading, so a
    // token that names an allowed stream but a different stream's shard reads
    // nothing.
    //
    // An iterator that does not decode or names no stream (a forged token, or
    // one issued by a build before the stream ARN was carried) is rejected with
    // ValidationException "Invalid ShardIterator" before authorization. The
    // service answers every iterator it did not issue that way, whatever the
    // caller's policy: an allow-all principal, a principal allowed only
    // ListTables, and one with an explicit Deny on GetRecords for every stream
    // all get it (measured 2026-10-20).
    if operation == "GetRecords" {
        let resource_arn =
            authz_resource::getrecords_stream_resource(input, &state.region, account_id)
                .ok_or_else(|| {
                    DynamoDbError::ValidationException("Invalid ShardIterator".to_owned())
                })?;
        authorization::check_authorization(
            state.authz_cache.as_ref(),
            identity,
            "GetRecords",
            &resource_arn,
            false,
            extenddb_auth::policy::context::RequestParams::default(),
        )
        .await?;
        return Ok(None);
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
            // The RequestItems key (or TransactItems TableName) may be a bare
            // name or a table ARN; canonicalize either to this deployment's
            // table ARN so a respelled ARN cannot evade a per-table Deny.
            let resource_arn =
                authz_resource::table_resource_arn(&table, &state.region, account_id);
            authorization::check_authorization(
                state.authz_cache.as_ref(),
                identity,
                &action_op,
                &resource_arn,
                false,
                extenddb_auth::policy::context::RequestParams::default(),
            )
            .await?;
        }
        return Ok(None);
    }

    // Derive the (action, resource ARN) pairs the service authorizes this
    // operation against. The resource comes from the field the operation
    // actually carries it in (ResourceArn, StreamArn, BackupArn, TableArn,
    // IndexName, or a table ARN in TableName), canonicalized to this
    // deployment, never a `table/*` fallback. See `authz_resource`.
    let targets = authz_resource::authz_targets(operation, input, &state.region, account_id);
    let is_scan = operation == "Scan";

    if targets.len() == 1 {
        // Single-resource operations carry the request params the policy engine
        // reads for item-level condition keys; build them exactly as before.
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
        let (action, resource_arn) = &targets[0];
        authorization::check_authorization(
            state.authz_cache.as_ref(),
            identity,
            action,
            resource_arn,
            is_scan,
            params,
        )
        .await?;
    } else {
        // Multi-permission operations (restore). Each pair is authorized with
        // default params; a Deny on any required resource fails the request.
        for (action, resource_arn) in &targets {
            authorization::check_authorization(
                state.authz_cache.as_ref(),
                identity,
                action,
                resource_arn,
                false,
                extenddb_auth::policy::context::RequestParams::default(),
            )
            .await?;
        }
    }

    Ok(engine_key_info(key_info))
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
