// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DescribeTimeToLive` and `UpdateTimeToLive` operation handlers.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    DescribeTimeToLiveInput, DescribeTimeToLiveOutput, TimeToLiveDescription,
    TimeToLiveSpecification, TimeToLiveSpecificationOutput, TimeToLiveStatus,
    UpdateTimeToLiveInput, UpdateTimeToLiveOutput,
};
use extenddb_core::validation::validate_table_name;
use extenddb_storage::error::StorageError;
use serde_json::Value;

use crate::OperationContext;
use crate::serialize_output;

/// Handle `DescribeTimeToLive` — return TTL configuration for a table.
///
/// # Errors
///
/// Returns `ResourceNotFoundException` if the table does not exist.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_describe_time_to_live(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: DescribeTimeToLiveInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    validate_table_name(&input.table_name, &ctx.limits)?;

    let desc = ctx
        .storage
        .describe_ttl(&ctx.account_id, &input.table_name)
        .await
        .map_err(storage_to_dynamo)?;

    let output = DescribeTimeToLiveOutput {
        time_to_live_description: desc,
    };
    serialize_output(&output)
}

/// Gate an `UpdateTimeToLive` request on the table's current TTL state.
///
/// A table mid-transition rejects further changes: the backfill and the queue
/// drain are both keyed to the current generation, and admitting a toggle now
/// would race them. The client retries once the transition lands (matching
/// DynamoDB, which also refuses updates in ENABLING and DISABLING).
///
/// With TTL enabled, a request that names another attribute is refused in
/// both forms, and the message carries the stored name. The comparison is
/// byte-exact: `DynamoDB` refuses `TTL` and `ttl ` for a table enabled on
/// `ttl`. Steady states otherwise reject only the idempotent no-op.
fn check_ttl_transition(
    current: &TimeToLiveDescription,
    requested: &TimeToLiveSpecification,
) -> Result<(), DynamoDbError> {
    match current.time_to_live_status {
        TimeToLiveStatus::Enabling | TimeToLiveStatus::Disabling => {
            Err(DynamoDbError::ValidationException(
                "Time to live is being updated for this table. Retry the request once the \
                 current change completes."
                    .to_owned(),
            ))
        }
        TimeToLiveStatus::Enabled => match current.attribute_name.as_deref() {
            Some(stored) if stored != requested.attribute_name => {
                Err(DynamoDbError::ValidationException(format!(
                    "TimeToLive is active on a different AttributeName: current AttributeName \
                     is {stored}"
                )))
            }
            _ if requested.enabled => Err(DynamoDbError::ValidationException(
                "TimeToLive is already enabled".to_owned(),
            )),
            _ => Ok(()),
        },
        TimeToLiveStatus::Disabled if !requested.enabled => Err(
            DynamoDbError::ValidationException("TimeToLive is already disabled".to_owned()),
        ),
        TimeToLiveStatus::Disabled => Ok(()),
    }
}

/// Handle `UpdateTimeToLive`: enable or disable TTL on a table attribute.
///
/// When enabling, kicks off creation of a `PostgreSQL` expression index on the
/// TTL attribute. When disabling, marks TTL disabled (sweeper stops) then
/// drops the index.
///
/// # Errors
///
/// Returns `ValidationException` if the attribute name is empty, if TTL is
/// already in the requested state (idempotency check), or if TTL is enabled
/// on an attribute other than the one the request names.
/// Returns `ResourceNotFoundException` if the table does not exist.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_update_time_to_live(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: UpdateTimeToLiveInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    validate_table_name(&input.table_name, &ctx.limits)?;
    validate_ttl_attribute_name(&input.time_to_live_specification.attribute_name)?;

    let current = ctx
        .storage
        .describe_ttl(&ctx.account_id, &input.table_name)
        .await
        .map_err(storage_to_dynamo)?;

    check_ttl_transition(&current, &input.time_to_live_specification)?;

    // Resolve the old attribute before committing the disable. If the catalog
    // is inconsistent, fail without leaving a partially applied request.
    let ttl_attribute_to_drop = if !input.time_to_live_specification.enabled {
        Some(current.attribute_name.clone().ok_or_else(|| {
            DynamoDbError::InternalServerError("TTL attribute is missing".to_owned())
        })?)
    } else {
        None
    };

    ctx.storage
        .update_ttl(
            &ctx.account_id,
            &input.table_name,
            &input.time_to_live_specification.attribute_name,
            input.time_to_live_specification.enabled,
        )
        .await
        .map_err(storage_to_dynamo)?;

    if input.time_to_live_specification.enabled {
        // Backends that report ENABLING (Cassandra, PostgreSQL) get the
        // backfill kicked off in the background: it is a full table scan, and
        // awaiting it made the API call take as long as the table is large.
        // The caller sees ENABLING until readiness is published; if the task
        // dies unfinished, the TTL worker's pending-index pass retries (on
        // Cassandra, from the durable cursor). Backends that report Enabled
        // immediately (SQLite, MongoDB) keep the awaited call: detaching it
        // would leave a table claiming Enabled while its index does not yet
        // exist, with no observable transition state.
        let transitional = ctx
            .storage
            .describe_ttl(&ctx.account_id, &input.table_name)
            .await
            .map(|d| d.time_to_live_status == TimeToLiveStatus::Enabling)
            .unwrap_or(false);
        if transitional {
            let storage = ctx.storage.clone();
            let account_id = ctx.account_id.clone();
            let table_name = input.table_name.clone();
            let attr = input.time_to_live_specification.attribute_name.clone();
            tokio::spawn(async move {
                if let Err(e) = storage
                    .create_ttl_index(&account_id, &table_name, &attr)
                    .await
                {
                    tracing::warn!("TTL queue backfill deferred for {table_name}: {e}");
                }
            });
        } else if let Err(e) = ctx
            .storage
            .create_ttl_index(
                &ctx.account_id,
                &input.table_name,
                &input.time_to_live_specification.attribute_name,
            )
            .await
        {
            tracing::warn!("TTL index creation deferred for {}: {e}", input.table_name);
        }
    } else {
        // Disable path: metadata already updated (sweeper won't pick up this table).
        // Drop the index. Safe because sweeper checks ttl_index_ready which is now FALSE.
        if let Some(ttl_attribute) = ttl_attribute_to_drop.as_deref()
            && let Err(e) = ctx
                .storage
                .drop_ttl_index(&ctx.account_id, &input.table_name, ttl_attribute)
                .await
        {
            tracing::warn!("TTL index drop failed for {}: {e}", input.table_name);
        }
    }

    let output = UpdateTimeToLiveOutput {
        time_to_live_specification: TimeToLiveSpecificationOutput {
            attribute_name: input.time_to_live_specification.attribute_name,
            enabled: input.time_to_live_specification.enabled,
        },
    };
    serialize_output(&output)
}

/// Validate a TTL attribute name.
///
/// Real `DynamoDB` allows any UTF-8 (1–255 bytes). However, the TTL attribute
/// name is interpolated into `PostgreSQL` DDL (expression index creation) where
/// parameterized queries are not possible. We use a strict allowlist:
/// `^[a-zA-Z0-9._-]+$` (1–255 bytes). This eliminates the entire class of
/// SQL injection risk. See `docs/differences-from-dynamodb.md`.
///
/// Defense-in-depth per `docs/adr/sql-injection-defense.md`.
fn validate_ttl_attribute_name(name: &str) -> Result<(), DynamoDbError> {
    if name.is_empty() || name.len() > 255 {
        return Err(DynamoDbError::ValidationException(
            "TimeToLiveSpecification.AttributeName must be between 1 and 255 characters".to_owned(),
        ));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
    {
        return Err(DynamoDbError::ValidationException(
            "TimeToLiveSpecification.AttributeName contains invalid characters".to_owned(),
        ));
    }
    Ok(())
}

fn storage_to_dynamo(e: StorageError) -> DynamoDbError {
    match e {
        // A backend's refusal of the request (the Cassandra backend refuses
        // TTL on a table with an asynchronously propagated GSI): a client
        // error, which SDKs do not retry.
        StorageError::Validation(msg) => DynamoDbError::ValidationException(msg),
        StorageError::TableNotFound(_name) => {
            DynamoDbError::ResourceNotFoundException("Requested resource not found".to_string())
        }
        StorageError::TableNotActive(name) => {
            DynamoDbError::ResourceInUseException(format!("Table {name} is not in ACTIVE state"))
        }
        other => {
            tracing::error!(internal_error = %other, "storage internal error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ttl_attribute_names() {
        assert!(validate_ttl_attribute_name("ttl").is_ok());
        assert!(validate_ttl_attribute_name("TTL_field").is_ok());
        assert!(validate_ttl_attribute_name("my.ttl-attr").is_ok());
        assert!(validate_ttl_attribute_name("a").is_ok());
        assert!(validate_ttl_attribute_name("A0_.-z9").is_ok());
    }

    #[test]
    fn empty_name_rejected() {
        assert!(validate_ttl_attribute_name("").is_err());
    }

    #[test]
    fn too_long_name_rejected() {
        let long = "a".repeat(256);
        assert!(validate_ttl_attribute_name(&long).is_err());
    }

    #[test]
    fn max_length_accepted() {
        let max = "a".repeat(255);
        assert!(validate_ttl_attribute_name(&max).is_ok());
    }

    #[test]
    fn transition_gate() {
        use TimeToLiveStatus::{Disabled, Disabling, Enabled, Enabling};
        const BEING_UPDATED: &str = "Time to live is being updated for this table. Retry the \
                                     request once the current change completes.";
        const OTHER_ATTRIBUTE: &str =
            "TimeToLive is active on a different AttributeName: current AttributeName is ttl";
        const ALREADY_ENABLED: &str = "TimeToLive is already enabled";
        const ALREADY_DISABLED: &str = "TimeToLive is already disabled";
        // (stored state, stored name, request Enabled, request name, answer).
        // The ENABLED rows are what DynamoDB answers for a table enabled on
        // `ttl` (measured 2026-11-21 in us-east-1); the comparison is
        // byte-exact, so `TTL` and `ttl ` are other attributes (the handler's
        // charset check refuses the space before this gate sees it). A disable
        // with the stored name is admitted: the service's cooldown is a
        // documented difference. The name check does not apply to
        // mid-transition rows or to rows without a stored name.
        let rows = [
            (Disabled, None, true, "ttl", Ok(())),
            (Disabled, None, false, "other", Err(ALREADY_DISABLED)),
            (Enabled, Some("ttl"), true, "ttl", Err(ALREADY_ENABLED)),
            (Enabled, Some("ttl"), false, "ttl", Ok(())),
            (Enabled, Some("ttl"), true, "expires", Err(OTHER_ATTRIBUTE)),
            (Enabled, Some("ttl"), false, "other", Err(OTHER_ATTRIBUTE)),
            (Enabled, Some("ttl"), true, "TTL", Err(OTHER_ATTRIBUTE)),
            (Enabled, Some("ttl"), false, "TTL", Err(OTHER_ATTRIBUTE)),
            (Enabled, Some("ttl"), true, "ttl ", Err(OTHER_ATTRIBUTE)),
            (Enabled, Some("ttl"), false, "ttl ", Err(OTHER_ATTRIBUTE)),
            (Enabled, None, true, "ttl", Err(ALREADY_ENABLED)),
            (Enabled, None, false, "ttl", Ok(())),
            (Enabling, Some("ttl"), true, "expires", Err(BEING_UPDATED)),
            (Enabling, Some("ttl"), false, "ttl", Err(BEING_UPDATED)),
            (Disabling, None, true, "ttl", Err(BEING_UPDATED)),
            (Disabling, None, false, "other", Err(BEING_UPDATED)),
        ];
        for (status, stored, enabled, name, expected) in rows {
            let current = TimeToLiveDescription {
                time_to_live_status: status,
                attribute_name: stored.map(str::to_owned),
            };
            let requested = TimeToLiveSpecification {
                enabled,
                attribute_name: name.to_owned(),
            };
            let result = check_ttl_transition(&current, &requested);
            let answer: Result<(), &str> = match &result {
                Ok(()) => Ok(()),
                Err(DynamoDbError::ValidationException(message)) => Err(message),
                Err(other) => panic!("{status:?} {stored:?} {enabled} {name:?}: {other:?}"),
            };
            assert_eq!(
                answer, expected,
                "{status:?} stored {stored:?}, request Enabled={enabled} AttributeName={name:?}"
            );
        }
    }

    #[test]
    fn a_backend_validation_refusal_is_a_validation_exception() {
        let msg = "Time to Live cannot be enabled on table t: index g is asynchronous";
        match storage_to_dynamo(StorageError::Validation(msg.to_owned())) {
            DynamoDbError::ValidationException(m) => assert_eq!(m, msg),
            other => panic!("expected ValidationException, got {other:?}"),
        }
        // Liveness: a fault is still an internal error.
        assert!(matches!(
            storage_to_dynamo(StorageError::Internal("boom".to_owned())),
            DynamoDbError::InternalServerError(_)
        ));
    }

    #[test]
    fn special_chars_rejected() {
        assert!(validate_ttl_attribute_name("it's").is_err());
        assert!(validate_ttl_attribute_name("a\"b").is_err());
        assert!(validate_ttl_attribute_name("a\\b").is_err());
        assert!(validate_ttl_attribute_name("a\0b").is_err());
        assert!(validate_ttl_attribute_name("a b").is_err());
        assert!(validate_ttl_attribute_name("a/b").is_err());
        assert!(validate_ttl_attribute_name("a#b").is_err());
        assert!(validate_ttl_attribute_name("a:b").is_err());
        assert!(validate_ttl_attribute_name("café").is_err());
    }
}
