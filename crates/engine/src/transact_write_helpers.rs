// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper types and functions for `TransactWriteItems`.
//!
//! Extracted from `transact_write_items.rs` to keep both files under the
//! 500-line limit.

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{ExpressionMaps, PathElement, UpdateAction};
use extenddb_core::types::{
    ReturnItemCollectionMetrics, ReturnValuesOnConditionCheckFailure, TableKeyInfo,
    attribute_value_size, extract_key, item_size_bytes,
};
use extenddb_storage::TransactWriteOp;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

use crate::capacity_helpers;

/// Pre-processed write operation with parsed expressions and resolved table info.
pub(crate) enum PreparedOp {
    Put {
        key_info: TableKeyInfo,
        item: extenddb_core::types::Item,
        condition: Option<extenddb_core::expression::Expr>,
        maps: ExpressionMaps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        stream: Option<extenddb_storage::StreamCapture>,
    },
    Delete {
        key_info: TableKeyInfo,
        key: extenddb_core::types::Item,
        condition: Option<extenddb_core::expression::Expr>,
        maps: ExpressionMaps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        stream: Option<extenddb_storage::StreamCapture>,
    },
    Update {
        key_info: TableKeyInfo,
        key: extenddb_core::types::Item,
        actions: Vec<UpdateAction>,
        condition: Option<extenddb_core::expression::Expr>,
        maps: ExpressionMaps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        stream: Option<extenddb_storage::StreamCapture>,
    },
    ConditionCheck {
        key_info: TableKeyInfo,
        key: extenddb_core::types::Item,
        condition: extenddb_core::expression::Expr,
        maps: ExpressionMaps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
    },
}

impl PreparedOp {
    /// The table name for this operation.
    pub(crate) fn table_name(&self) -> &str {
        match self {
            Self::Put { key_info, .. }
            | Self::Delete { key_info, .. }
            | Self::Update { key_info, .. }
            | Self::ConditionCheck { key_info, .. } => &key_info.table_name,
        }
    }

    /// Approximate item size for transaction size limit enforcement.
    ///
    /// For Put operations, this returns the exact item size. For Update, Delete,
    /// and ConditionCheck, the full item is not yet available at validation time
    /// (it will be fetched during execution). DynamoDB's 4MB limit counts the
    /// full item size as it exists or will exist post-mutation.
    ///
    /// To avoid bypassing the limit with updates that produce large items, we
    /// include the size of expression attribute values as a lower-bound estimate
    /// for the data being written by Update operations.
    pub(crate) fn item_size(&self) -> usize {
        match self {
            Self::Put { item, .. } => extenddb_core::types::item_size_bytes(item),
            Self::Delete { key, .. } | Self::ConditionCheck { key, .. } => {
                extenddb_core::types::item_size_bytes(key)
            }
            Self::Update { key, maps, .. } => {
                // Use key size + expression attribute values size as a better
                // approximation of the data involved in the update.
                let key_size = extenddb_core::types::item_size_bytes(key);
                let values_size: usize = maps.values.values().map(attribute_value_size).sum();
                key_size + values_size
            }
        }
    }

    /// Canonical `"table_name:{json_key}"` string for duplicate-target detection.
    ///
    /// Uses JSON serialization of the extracted key (`BTreeMap` guarantees sorted
    /// iteration, and `serde_json` preserves that order) so the representation is
    /// deterministic by explicit contract rather than relying on `Debug` output.
    pub(crate) fn canonical_target(&self) -> String {
        let (table, key) = match self {
            Self::Put { key_info, item, .. } => (
                &key_info.table_name,
                extract_key(item, &key_info.key_schema),
            ),
            Self::Delete { key_info, key, .. }
            | Self::Update { key_info, key, .. }
            | Self::ConditionCheck { key_info, key, .. } => (&key_info.table_name, (*key).clone()),
        };
        // Item (BTreeMap<String, AttributeValue>) is always serializable.
        format!(
            "{table}:{}",
            serde_json::to_string(&key).unwrap_or_default()
        )
    }

    /// Estimate write bytes for capacity metering.
    pub(crate) fn write_bytes(&self) -> usize {
        match self {
            Self::Put { item, .. } => item_size_bytes(item),
            // TODO(fidelity): DynamoDB charges WCU based on old item size for deletes
            // and max(old, new) for updates. Old item size is not available at this point
            // in the transact flow. Using key size as a lower bound.
            Self::Delete { key, .. } | Self::Update { key, .. } => item_size_bytes(key),
            Self::ConditionCheck { .. } => 0,
        }
    }

    pub(crate) fn to_storage_op(&self) -> TransactWriteOp<'_> {
        match self {
            Self::Put {
                key_info,
                item,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => TransactWriteOp::Put {
                key_info,
                item,
                condition: condition.as_ref(),
                maps,
                return_values_on_ccf: *return_values_on_ccf,
                stream: stream.clone(),
            },
            Self::Delete {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => TransactWriteOp::Delete {
                key_info,
                key,
                condition: condition.as_ref(),
                maps,
                return_values_on_ccf: *return_values_on_ccf,
                stream: stream.clone(),
            },
            Self::Update {
                key_info,
                key,
                actions,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => TransactWriteOp::Update {
                key_info,
                key,
                actions,
                condition: condition.as_ref(),
                maps,
                return_values_on_ccf: *return_values_on_ccf,
                stream: stream.clone(),
            },
            Self::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf,
            } => TransactWriteOp::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf: *return_values_on_ccf,
            },
        }
    }

    /// Build an `ItemCollectionMetrics` stub for write operations (not `ConditionCheck`).
    pub(crate) fn item_collection_metric(
        &self,
        ricm: ReturnItemCollectionMetrics,
    ) -> Option<extenddb_core::types::ItemCollectionMetrics> {
        let (key_info, item_or_key) = match self {
            Self::Put { key_info, item, .. } => (key_info, item),
            Self::Delete { key_info, key, .. } | Self::Update { key_info, key, .. } => {
                (key_info, key)
            }
            Self::ConditionCheck { .. } => return None,
        };
        capacity_helpers::item_metrics(ricm, &key_info.key_schema, item_or_key, key_info.has_lsi)
    }
}

/// Parse an optional condition expression string.
pub(crate) fn parse_optional_condition(
    expr: Option<&str>,
    limits: &extenddb_core::limits::LimitsConfig,
) -> Result<Option<extenddb_core::expression::Expr>, DynamoDbError> {
    match expr {
        Some(s) if !s.is_empty() => {
            let tokens = crate::expression_helpers::tokenize_expression(s, limits)?;
            let ast = extenddb_core::expression::parse_condition_with_depth_limit(
                &tokens,
                limits.max_expression_depth,
            )?;
            Ok(Some(ast))
        }
        _ => Ok(None),
    }
}

/// Validate `ClientRequestToken` format.
///
/// Real `DynamoDB` requires 1–36 characters, alphanumeric plus hyphens.
pub(crate) fn validate_client_request_token(token: &str) -> Result<(), DynamoDbError> {
    if token.is_empty() || token.len() > 36 {
        return Err(DynamoDbError::ValidationException(
            "1 validation error detected: Value at 'clientRequestToken' failed to satisfy \
             constraint: Member must have length less than or equal to 36"
                .to_owned(),
        ));
    }
    if !token
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(DynamoDbError::ValidationException(
            "1 validation error detected: Value at 'clientRequestToken' failed to satisfy \
             constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9-]+"
                .to_owned(),
        ));
    }
    Ok(())
}

/// Compute a collision-resistant fingerprint of the transaction operations.
///
/// Uses HMAC-SHA256 keyed by the client request token over the JSON-serialized
/// `TransactItems` array. The keyed hash prevents an attacker from crafting a
/// different request body that produces the same fingerprint for a given token.
/// The result is truncated to 16 hex chars (64 bits) for storage efficiency
/// while remaining collision-resistant for the idempotency use case.
pub(crate) fn compute_fingerprint(body: &Value, token: &str) -> String {
    let items = body.get("TransactItems").unwrap_or(&Value::Null);
    // serde_json::to_string on a Value is infallible — all Value variants
    // are representable as JSON. Using unwrap_or_default as a defensive fallback.
    let json = serde_json::to_string(items).unwrap_or_default();
    // HMAC-SHA256 accepts any key length — new_from_slice cannot fail.
    // `InvalidLength` is impossible for HMAC per RFC 2104 (oversized keys are hashed).
    #[allow(clippy::expect_used)]
    let mut mac =
        Hmac::<Sha256>::new_from_slice(token.as_bytes()).expect("HMAC accepts any key length");
    mac.update(json.as_bytes());
    let result = mac.finalize().into_bytes();
    hex::encode(&result[..8])
}

/// Validate that no update action targets a key attribute.
pub(crate) fn validate_no_key_updates(
    actions: &[UpdateAction],
    key_info: &TableKeyInfo,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    for action in actions {
        let path = match action {
            UpdateAction::Set { path, .. }
            | UpdateAction::Remove { path }
            | UpdateAction::Add { path, .. }
            | UpdateAction::Delete { path, .. } => path,
        };
        if let Some(PathElement::Attribute(name)) = path.first() {
            let resolved = if let Some(ref_name) = name.strip_prefix('#') {
                maps.resolve_name(ref_name)?
            } else {
                name.as_str()
            };
            for ks in &key_info.key_schema {
                if ks.attribute_name == resolved {
                    return Err(DynamoDbError::ValidationException(format!(
                        "One or more parameter values were invalid: Cannot update attribute {}. \
                         This attribute is part of the key",
                        ks.attribute_name
                    )));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transact_condition_redundant_parens_rejected_with_canonical_message() {
        let limits = extenddb_core::limits::LimitsConfig::default();
        let err = parse_optional_condition(Some("((a = :v))"), &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "Invalid ConditionExpression: The expression has redundant parentheses;"),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_token_valid() {
        assert!(validate_client_request_token("abc-123").is_ok());
        assert!(validate_client_request_token("a").is_ok());
        assert!(validate_client_request_token(&"a".repeat(36)).is_ok());
    }

    #[test]
    fn validate_token_empty() {
        assert!(validate_client_request_token("").is_err());
    }

    #[test]
    fn validate_token_too_long() {
        assert!(validate_client_request_token(&"a".repeat(37)).is_err());
    }

    #[test]
    fn validate_token_bad_chars() {
        assert!(validate_client_request_token("abc_123").is_err());
        assert!(validate_client_request_token("abc 123").is_err());
        assert!(validate_client_request_token("abc!").is_err());
    }

    #[test]
    fn fingerprint_deterministic() {
        let body: Value = serde_json::json!({"TransactItems": [{"Put": {"TableName": "t"}}]});
        let a = compute_fingerprint(&body, "tok-1");
        let b = compute_fingerprint(&body, "tok-1");
        assert_eq!(a, b);
    }

    #[test]
    fn fingerprint_differs_for_different_ops() {
        let a: Value = serde_json::json!({"TransactItems": [{"Put": {"TableName": "t1"}}]});
        let b: Value = serde_json::json!({"TransactItems": [{"Put": {"TableName": "t2"}}]});
        assert_ne!(
            compute_fingerprint(&a, "tok-1"),
            compute_fingerprint(&b, "tok-1")
        );
    }

    #[test]
    fn fingerprint_differs_for_different_tokens() {
        let body: Value = serde_json::json!({"TransactItems": [{"Put": {"TableName": "t"}}]});
        assert_ne!(
            compute_fingerprint(&body, "tok-1"),
            compute_fingerprint(&body, "tok-2")
        );
    }
}
