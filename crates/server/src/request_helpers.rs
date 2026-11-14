// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request parsing and authorization helpers for the `DynamoDB` wire protocol.

use axum::http::HeaderMap;
use extenddb_core::error::DynamoDbError;
use extenddb_storage::error::StorageError;
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

/// Operations whose top-level `TableName` names the table the request acts
/// on. These are the operations the engine dispatches whose input shape has a
/// top-level `TableName` in botocore 1.43.125's `dynamodb` and
/// `dynamodbstreams` models, less `ListBackups` and `ListStreams`.
///
/// Each of these succeeds only when the table exists (`CreateTable` once it
/// does), so a successful request records metrics under a real table. In
/// `ListBackups` and `ListStreams`, `TableName` is a filter over the account's
/// backups or streams, and either answers 200 with an empty list for a table
/// that does not exist. Under it, any valid name a caller makes up would become
/// a dimension value, so they record no table.
///
/// Any other operation ignores or refuses a `TableName` it is sent, so its
/// metrics must not take one from the request either: a `BatchWriteItem` with
/// an extra `TableName` would otherwise record its metrics under a table it
/// never touched.
const TABLE_NAME_OPERATIONS: &[&str] = &[
    "CreateBackup",
    "CreateTable",
    "DeleteItem",
    "DeleteTable",
    "DescribeContinuousBackups",
    "DescribeTable",
    "DescribeTimeToLive",
    "GetItem",
    "PutItem",
    "Query",
    "Scan",
    "SearchVectors",
    "UpdateContinuousBackups",
    "UpdateItem",
    "UpdateTable",
    "UpdateTimeToLive",
];

/// The table name a request's metrics are recorded under, or `None` when the
/// request names no valid table.
///
/// The name comes from the request's top-level `TableName`, and only for an
/// operation that acts on the table it names ([`TABLE_NAME_OPERATIONS`]). A
/// table ARN is reduced to its table name, so a table has one dimension value
/// however the request named it. The result must pass the table name rules
/// (`validate_table_name`); a request that fails validation can carry any
/// string, control characters included, and every reader of the metrics would
/// get it as a dimension value (issue #728). Otherwise the result is `None`,
/// and the request's metrics carry no table.
pub(crate) fn metric_table_name(
    operation: &str,
    input: &Value,
    limits: &extenddb_core::limits::LimitsConfig,
) -> Option<String> {
    if !TABLE_NAME_OPERATIONS.contains(&operation) {
        return None;
    }
    let sent = input.get("TableName")?.as_str()?;
    let name = if sent.starts_with("arn:") {
        extenddb_core::arn::parse(sent)?.table
    } else {
        sent.to_owned()
    };
    extenddb_core::validation::validate_table_name(&name, limits)
        .is_ok()
        .then_some(name)
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
/// Returns the `TableKeyInfo` authorization read for each table it evaluated,
/// including a `None` for a table it found missing (P118 optimization #2). The
/// caller passes this into `OperationContext::authorized_key_info`, and the
/// engine resolves those tables from it instead of reading the catalog again.
/// Authorization and the engine therefore act on one snapshot: a table that
/// authorization saw as missing, and so evaluated with `dynamodb:LeadingKeys`
/// absent, is missing to the engine for the rest of the request, even if it has
/// become ACTIVE in between.
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
    source_ip: Option<&str>,
    user_agent: Option<&str>,
) -> Result<extenddb_engine::AuthorizedKeyInfo, DynamoDbError> {
    // A table name, index name or ARN that the service refuses, in TableName
    // or in any other field that carries one (RequestItems, TransactItems, the
    // restore and import names, the stream, backup and tag ARNs), is refused
    // here, before the key info and resource tag lookups below and before the
    // developer-mode return, the cross-account refusal and the ARN shape
    // checks (see `table_name_gate`).
    crate::table_name_gate::check_table_name_chars(operation, input, account_id)?;

    let table_name = extract_table_name(input);
    let mut authorized_key_info = extenddb_engine::AuthorizedKeyInfo::new();

    // TableName may be a table ARN. dynamodb:LeadingKeys and the other item
    // condition keys are read with the table's key schema, so key info is
    // looked up by the table name the ARN names (this account and region only).
    // Without this, the lookup misses, LeadingKeys is absent, and a
    // ForAllValues condition is satisfied vacuously. The key info found this
    // way is used for authorization only and is not put in the snapshot handed
    // to the engine, which resolves the request's TableName itself as before.
    let named_by_arn = table_name.as_deref().is_some_and(|t| t.starts_with("arn:"));

    // P118: Fetch table_key_info for item-level operations via the SWR cache.
    // The result is used for LeadingKeys extraction here AND returned to the
    // caller to avoid a redundant fetch in the engine layer.
    let mut key_info = None;
    if let Some(ref tn) = table_name
        && matches!(
            operation,
            "GetItem" | "PutItem" | "DeleteItem" | "UpdateItem" | "Query" | "Scan"
        )
    {
        let lookup = authz_resource::key_lookup_name(tn, &state.region, account_id);
        key_info = key_info_for_authz(state.table_key_info_cache.get(account_id, &lookup).await)?;
        if !named_by_arn {
            authorized_key_info.insert(tn.clone(), key_info.clone());
        }
    }

    // The checks from here to the dev-mode return answer a request before the
    // IAM decision, and the service gives each answer to every principal. So
    // they run in dev mode too, and a dev-mode server refuses the same
    // malformed, out-of-scope and unissued resources, and the same resources in
    // another account, as any other build (#450, #451).
    //
    // A StreamArn (DescribeStream, GetShardIterator) that is not a stream ARN,
    // a table, index or backup ARN included, or that has an empty partition,
    // service or account field, is refused with ValidationException "Invalid
    // StreamArn" before the account is looked at, in every account (measured
    // 2026-10-28 and 2026-11-05).
    authz_resource::validate_stream_arn_kind(operation, input)?;

    // A request that names a resource in another account by ARN is refused
    // with the service's answer for the operation and for whether this
    // deployment knows the account, for an admin and a no-policy caller alike
    // (measured 2026-11-03 and 2026-11-05; see `authz_resource::cross_account`).
    // For an account the catalog holds, the shape checks below run first.
    authz_resource::cross_account::refuse(state, identity, operation, input, account_id).await?;

    // The tag operations' ResourceArn shape is validated before the IAM
    // decision: an index ARN in this account (tags are accepted only on a
    // table or stream) is rejected with ValidationException for every
    // principal. A table or stream ARN passes; its partition, service, region
    // and existence are checked after authorization by the engine tag handler,
    // and the IAM decision runs on the ARN as sent (#451).
    if matches!(
        operation,
        "TagResource" | "UntagResource" | "ListTagsOfResource"
    ) && let Some(arn) = input.get("ResourceArn").and_then(Value::as_str)
        && !arn.is_empty()
    {
        authz_resource::validate_tag_resource_arn_shape(arn)?;
    }

    // A StreamArn (DescribeStream, GetShardIterator) or BackupArn
    // (DescribeBackup, DeleteBackup) that is not a well-formed ARN of that kind,
    // and an export TableArn that does not parse, are refused with
    // ValidationException before the IAM decision. Without this, an ARN the
    // engine resolves one way and authorization reads another lets a Deny on
    // the real resource be bypassed. A StreamArn must also be in this
    // deployment's partition, service and region, so the stream authorized is
    // the stream the engine reads and a respelled ARN is never served (#450,
    // #451). A restore's source ARN, and an export TableArn of another kind,
    // are refused by the engine after the IAM decision.
    authz_resource::validate_operation_arns(operation, input, &state.region)?;

    // GetRecords carries its resource only inside the opaque shard iterator, not
    // in a request body field, so the iterator is decoded here for the stream
    // ARN the IAM decision below authorizes against.
    //
    // A token that does not decode, or decodes to neither a current token nor
    // the pre-upgrade shape, is rejected with ValidationException "Invalid
    // ShardIterator" before authorization. A pre-upgrade token (the four
    // pre-stream-ARN fields with a parseable timestamp) is answered with
    // ExpiredIteratorException before authorization, so a consumer reading
    // across the upgrade renews its iterator instead of failing on the
    // non-retryable "Invalid ShardIterator". The service answers an iterator it
    // did not issue the same way whatever the caller's policy (measured
    // 2026-10-20).
    let getrecords_stream = if operation == "GetRecords" {
        match authz_resource::getrecords_resource(input, &state.region, account_id) {
            authz_resource::GetRecordsResource::Stream(arn) => Some(arn),
            authz_resource::GetRecordsResource::Expired => {
                return Err(DynamoDbError::ExpiredIteratorException(
                    extenddb_engine::SHARD_ITERATOR_EXPIRED_MESSAGE.to_owned(),
                ));
            }
            authz_resource::GetRecordsResource::Invalid => {
                return Err(DynamoDbError::ValidationException(
                    "Invalid ShardIterator".to_owned(),
                ));
            }
        }
    } else {
        None
    };

    // Developer mode opens authorization for the authenticated caller. SigV4
    // verification already ran upstream, so the request is authenticated; only
    // the IAM policy decision is skipped. The key info is still returned so the
    // engine layer can reuse it.
    //
    // This check is deliberately positioned ahead of EVERY authorization branch
    // below, not merely the generic one. It previously sat after the batch and
    // transaction branch, which returns early, so those four operations were
    // still evaluated against IAM and denied in a dev-mode build while single
    // item operations passed. Any future operation-specific branch must be added
    // below this point, or it will reintroduce that defect.
    if state.dev_mode {
        return Ok(authorized_key_info);
    }

    // A PartiQL request names its tables inside its statements. The service
    // authorizes each statement with the action of its type
    // (`dynamodb:PartiQLSelect` and the others) against the table or index it
    // reads or writes, and needs no action for the operation itself (measured
    // 2026-11-03). The engine authorizes each statement through the item
    // authorizer the handler gives it, and refuses every statement without
    // one (see `item_authz`).
    if matches!(
        operation,
        "ExecuteStatement" | "BatchExecuteStatement" | "ExecuteTransaction"
    ) {
        return Ok(authorized_key_info);
    }

    // Request-level condition keys every evaluation below carries: the
    // deployment region, aws:SecureTransport, aws:SourceIp and aws:UserAgent,
    // and the tag condition keys the operation populates (aws:TagKeys and
    // aws:RequestTag/<key>, empty for every operation that carries no tag set).
    // The tag keys go in here, not in one operation's branch, so a tag-taking
    // operation reaching any branch below carries them.
    //
    // A branch that authorizes once moves the tag set into its params. Only the
    // three operations that carry a tag set populate it, and each authorizes
    // against one resource, so the loops over several targets below clone an
    // empty set.
    let (request_tags, request_tag_keys) =
        crate::request_tags::tag_condition_keys(operation, input);
    let base_params = |request_tags: Option<std::collections::HashMap<String, String>>,
                       request_tag_keys: Option<Vec<String>>| {
        extenddb_auth::policy::context::RequestParams {
            region: Some(state.region.to_string()),
            secure_transport: state.tls_enabled,
            source_ip: source_ip.map(ToOwned::to_owned),
            user_agent: user_agent.map(ToOwned::to_owned),
            request_tags,
            request_tag_keys,
            ..Default::default()
        }
    };

    // DescribeEndpoints needs a signed request (SigV4 verification already ran
    // upstream) but no IAM permission, as on DynamoDB: a caller with no policy,
    // or with an explicit Deny on dynamodb:DescribeEndpoints or dynamodb:*, gets
    // the endpoint list (measured 2026-10-20).
    if operation == "DescribeEndpoints" {
        return Ok(authorized_key_info);
    }

    // GetRecords is authorized against the specific stream its iterator names,
    // as the service does. The storage read resolves the shard against that
    // stream as part of the read, so a token that names an allowed stream but a
    // different stream's shard reads nothing.
    if let Some(resource_arn) = getrecords_stream {
        authorization::check_authorization(
            state.authz_cache.as_ref(),
            identity,
            &authz_resource::AuthzTarget::new("GetRecords", resource_arn),
            false,
            base_params(request_tags, request_tag_keys),
        )
        .await?;
        return Ok(authorized_key_info);
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
    // `batch_transact_tables` returns `Some` for exactly the batch and
    // transaction operations. Batch operations authorize once per table with
    // the set of keys that table receives; transaction operations authorize
    // once per item with that item's key. Each target carries the
    // `dynamodb:LeadingKeys` value(s) the service evaluates, extracted per the
    // table's key schema, the `dynamodb:Attributes` names of that table's
    // part of the batch or of that item (see `request_attributes`), and its
    // `dynamodb:Select`, `dynamodb:ReturnValues` and
    // `dynamodb:ReturnConsumedCapacity` values (see `parameter_keys`). The
    // resource ARN per target is unchanged: each is still that table's ARN.
    // (Measured against live DynamoDB 2026-10-20, 2026-10-31 and 2026-11-01.)
    // Targets with the same action, table and condition key values are
    // evaluated once, since the decision depends only on those for a given
    // identity.
    if let Some(tables) = crate::leading_keys::batch_transact_tables(operation, input) {
        // A request outside the operation's count limits is refused before any
        // key info is read or any policy is evaluated, with the engine's own
        // error. DynamoDB does the same: an empty or 101-item transaction, an
        // empty RequestItems, a table with no keys or requests, more than 100
        // tables or keys in a BatchGetItem and more than 25 tables or requests
        // in a BatchWriteItem each return ValidationException, including to a
        // principal with no permissions (measured 2026-10-21). Checking first
        // also bounds the tables read and the policies evaluated below by those
        // limits instead of by the body size. A TransactWriteItems item can name
        // up to four sub-operations here, each on its own table, before the
        // handler rejects it, so a transaction reads at most 400 tables.
        extenddb_engine::check_request_item_counts(operation, input)?;

        // Read each distinct table's key info from storage, not the cache, and
        // hand what was read to the engine. The engine resolves these tables
        // from the snapshot, so it never acts on a table authorization saw as
        // missing, and it sees the catalog as fresh as these handlers always
        // read it. A table named by ARN is read under that ARN string, which
        // the catalog does not hold, so it is recorded as missing: LeadingKeys
        // stays absent for it, and the engine answers ResourceNotFound from the
        // same snapshot instead of resolving the ARN to the real table.
        for table in tables {
            let ki = key_info_for_authz(state.storage.table_key_info(account_id, &table).await)?;
            authorized_key_info.insert(table, ki);
        }
        let targets = crate::leading_keys::batch_transact_targets(
            operation,
            input,
            &authorized_key_info,
            &state.limits,
        )
        .unwrap_or_default();

        if targets.is_empty() {
            // Malformed body (the count checks above have already refused an
            // empty one): still require the operation's action on the table
            // wildcard, as the generic path would, then let the engine reject
            // the body. This never silently skips authorization.
            let resource_arn = authz_resource::table_wildcard(&state.region, account_id);
            authorization::check_authorization(
                state.authz_cache.as_ref(),
                identity,
                &authz_resource::AuthzTarget::new(operation, resource_arn),
                false,
                base_params(request_tags, request_tag_keys),
            )
            .await?;
        } else {
            for target in targets {
                // The RequestItems key (or TransactItems TableName) may be a
                // bare name or a table ARN; canonicalize either to this
                // deployment's table ARN so a respelled ARN cannot evade a
                // per-table Deny. The denial names the key as written.
                let authz_target = authz_resource::table_target(
                    &target.action,
                    &target.table,
                    &state.region,
                    account_id,
                );
                let params = extenddb_auth::policy::context::RequestParams {
                    leading_keys: target.leading_keys,
                    attributes: target.attributes,
                    select: target.parameters.select,
                    return_values: target.parameters.return_values,
                    return_consumed_capacity: target.parameters.return_consumed_capacity,
                    ..base_params(request_tags.clone(), request_tag_keys.clone())
                };
                authorization::check_authorization(
                    state.authz_cache.as_ref(),
                    identity,
                    &authz_target,
                    false,
                    params,
                )
                .await?;
            }
        }
        return Ok(authorized_key_info);
    }

    // Derive the targets the service authorizes this operation against. The
    // resource comes from the field the operation actually carries it in
    // (ResourceArn, StreamArn, BackupArn, TableArn, IndexName, or a table ARN
    // in TableName), canonicalized to this deployment for the policy match,
    // never a `table/*` fallback, with the field as written kept for the
    // denial message. See `authz_resource`.
    let targets = authz_resource::authz_targets(operation, input, &state.region, account_id);
    let is_scan = operation == "Scan";

    if targets.len() == 1 {
        // Single-resource operations carry the request params the policy engine
        // reads for item-level condition keys. `dynamodb:Select`,
        // `dynamodb:ReturnValues` and `dynamodb:ReturnConsumedCapacity` hold
        // the value the operation uses when the request leaves the parameter
        // out (see `parameter_keys`).
        let parameters = crate::parameter_keys::single_table(operation, input);
        let params = extenddb_auth::policy::context::RequestParams {
            leading_keys: crate::leading_keys::single_table_leading_keys(
                input,
                operation,
                key_info.as_ref(),
                state.limits.max_expression_tokens,
            ),
            attributes: crate::request_attributes::single_table_attributes(
                operation,
                input,
                key_info.as_ref(),
                &state.limits,
            ),
            select: parameters.select,
            return_values: parameters.return_values,
            return_consumed_capacity: parameters.return_consumed_capacity,
            ..base_params(request_tags, request_tag_keys)
        };
        authorization::check_authorization(
            state.authz_cache.as_ref(),
            identity,
            &targets[0],
            is_scan,
            params,
        )
        .await?;
    } else {
        // Multi-permission operations (restore). Each pair is authorized with
        // the request-level params; a Deny on any required resource fails the
        // request. The pairs are checked in the order `authz_targets` returns
        // them, and the first denied pair is the one the denial names.
        for target in &targets {
            authorization::check_authorization(
                state.authz_cache.as_ref(),
                identity,
                target,
                false,
                base_params(request_tags.clone(), request_tag_keys.clone()),
            )
            .await?;
        }
    }

    Ok(authorized_key_info)
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

        let internal = [StorageError::Internal("backend fault".to_owned())];
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
        let engine =
            extenddb_engine::storage_err_to_dynamo(StorageError::TableNotActive("t".to_owned()));
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

    fn metric_name_of(table_name: Value) -> Option<String> {
        metric_table_name(
            "PutItem",
            &json!({ "TableName": table_name }),
            &extenddb_core::limits::LimitsConfig::default(),
        )
    }

    #[test]
    fn metric_table_name_keeps_a_valid_name() {
        assert_eq!(
            metric_name_of(json!("Orders_v2.prod-1")).as_deref(),
            Some("Orders_v2.prod-1")
        );
    }

    #[test]
    fn metric_table_name_reduces_a_table_arn_to_its_name() {
        for arn in [
            "arn:aws:dynamodb:us-east-1:123456789012:table/Orders",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Orders/index/ByDate",
        ] {
            assert_eq!(
                metric_name_of(json!(arn)).as_deref(),
                Some("Orders"),
                "{arn}"
            );
        }
    }

    #[test]
    fn metric_table_name_drops_a_name_that_breaks_the_rules() {
        // The values issue #728 found in the metrics table after rejected
        // CreateTable calls, a name too short, and ARNs that name no table.
        for sent in [
            "x\";DROP TABLE tables;--".to_owned(),
            "a\u{0}b".to_owned(),
            "ééé".to_owned(),
            "a".repeat(256),
            "ab".to_owned(),
            String::new(),
            "arn:aws:dynamodb:us-east-1:123456789012:table/".to_owned(),
            "arn:aws:s3:::bucket".to_owned(),
            "arn:aws:dynamodb:us-east-1:123456789012:table/a;b".to_owned(),
        ] {
            assert_eq!(metric_name_of(json!(sent)), None, "{sent:?}");
        }
    }

    #[test]
    fn metric_table_name_is_none_without_a_string_table_name() {
        assert_eq!(metric_name_of(json!(42)), None);
        let limits = extenddb_core::limits::LimitsConfig::default();
        assert_eq!(metric_table_name("PutItem", &json!({}), &limits), None);
    }

    /// An operation that does not act on the table its `TableName` names
    /// records no table, whatever valid name the request carries: those whose
    /// input has no top-level `TableName` member, and `ListBackups` and
    /// `ListStreams`, where it is a filter. One that acts on it keeps it.
    #[test]
    fn metric_table_name_reads_table_name_only_for_operations_on_that_table() {
        let limits = extenddb_core::limits::LimitsConfig::default();
        let input = json!({ "TableName": "Orders" });
        for operation in [
            "BatchWriteItem",
            "BatchGetItem",
            "TransactWriteItems",
            "TransactGetItems",
            "ExecuteStatement",
            "BatchExecuteStatement",
            "ExecuteTransaction",
            "ListTables",
            "ListBackups",
            "ListStreams",
            "TagResource",
            "DescribeBackup",
            "RestoreTableFromBackup",
            "ImportTable",
            "NotAnOperation",
        ] {
            assert_eq!(
                metric_table_name(operation, &input, &limits),
                None,
                "{operation}"
            );
        }
        let mut listed = super::TABLE_NAME_OPERATIONS.to_vec();
        listed.sort_unstable();
        // Spelled out, not read from the constant, so a dropped, misspelt or
        // added entry fails here.
        let expected = [
            "CreateBackup",
            "CreateTable",
            "DeleteItem",
            "DeleteTable",
            "DescribeContinuousBackups",
            "DescribeTable",
            "DescribeTimeToLive",
            "GetItem",
            "PutItem",
            "Query",
            "Scan",
            "SearchVectors",
            "UpdateContinuousBackups",
            "UpdateItem",
            "UpdateTable",
            "UpdateTimeToLive",
        ];
        assert_eq!(listed, expected);
        for operation in expected {
            assert_eq!(
                metric_table_name(operation, &input, &limits).as_deref(),
                Some("Orders"),
                "{operation}"
            );
        }
    }
}
