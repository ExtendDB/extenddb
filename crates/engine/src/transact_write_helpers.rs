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
use crate::partiql::request_checks::{MAX_CLIENT_REQUEST_TOKEN, length_constraint};

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
    /// and `ConditionCheck`, the full item is not yet available at validation time
    /// (it will be fetched during execution). `DynamoDB`'s 4MB limit counts the
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

    /// Return a `ValidationError` cancellation reason if this op's primary key
    /// exceeds the size limit (hash > 2048 / range > 1024 bytes by default).
    ///
    /// Real `DynamoDB` cancels the whole transaction with a per-item
    /// `ValidationError` reason for an oversized key in ANY sub-op (Put,
    /// Delete, Update, ConditionCheck). Empty-key values are handled separately
    /// as a top-level `ValidationException`, so this checks size only.
    pub(crate) fn oversized_key_reason(
        &self,
        limits: &extenddb_core::limits::LimitsConfig,
    ) -> Option<extenddb_core::types::CancellationReason> {
        let (key_info, item_or_key) = match self {
            Self::Put { key_info, item, .. } => (key_info, item),
            Self::Delete { key_info, key, .. }
            | Self::Update { key_info, key, .. }
            | Self::ConditionCheck { key_info, key, .. } => (key_info, key),
        };
        match extenddb_core::validation::validate_key_size_limits(
            item_or_key,
            &key_info.key_schema,
            limits,
        ) {
            Ok(()) => None,
            Err(e) => Some(extenddb_core::types::CancellationReason::validation_error(
                e.to_string(),
            )),
        }
    }
}

/// Check a `ClientRequestToken`: 1 to 36 characters of any kind, counted as
/// the PartiQL request checks count them. Measured on 2026-11-23: 36 `é`
/// (72 bytes) pass and 37 do not, and `_`, a space, a tab, U+0001 and
/// U+0000 pass.
pub(crate) fn validate_client_request_token(token: &str) -> Result<(), DynamoDbError> {
    match length_constraint(token, MAX_CLIENT_REQUEST_TOKEN) {
        None => Ok(()),
        Some(constraint) => Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value at 'ClientRequestToken' failed to satisfy \
             constraint: {constraint}"
        ))),
    }
}

/// The form a `ClientRequestToken` is stored in: each `\` doubled and each
/// U+0000 written `\0`, since PostgreSQL's `text` holds no U+0000. Two
/// tokens never share a stored form. A token of `[a-zA-Z0-9-]`, the only
/// kind an earlier build accepted, is stored as sent, so the tokens such a
/// build stored still match.
pub(crate) fn stored_token(token: &str) -> String {
    token.replace('\\', "\\\\").replace('\0', "\\0")
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
    fn validate_token_valid() {
        assert!(validate_client_request_token("abc-123").is_ok());
        assert!(validate_client_request_token("a").is_ok());
        assert!(validate_client_request_token(&"a".repeat(36)).is_ok());
    }

    /// The rows measured on DynamoDB on 2026-11-23 (T0 to T12, a `\` and a
    /// `\` before `0`): each token, the constraint its refusal names (`None`
    /// when it passes) and its stored form when that differs.
    #[test]
    fn client_request_tokens() {
        let short = Some("Member must have length greater than or equal to 1");
        let long = Some("Member must have length less than or equal to 36");
        let rows = [
            ("T0", "a".repeat(36), None, None),
            ("T1", "a".repeat(37), long, None),
            ("T2", "abc_def".to_owned(), None, None),
            ("T3", "abc def".to_owned(), None, None),
            ("T4", String::new(), short, None),
            ("T5", "_".repeat(37), long, None),
            ("T6", "a".repeat(36) + "_", long, None),
            ("T7", "\u{e9}".repeat(36), None, None),
            ("T8", "\u{e9}".repeat(37), long, None),
            ("T9", "abc\u{1}def".to_owned(), None, None),
            ("T10", "abc\tdef".to_owned(), None, None),
            ("T11", "abc\0def".to_owned(), None, Some("abc\\0def")),
            ("T12", "\0".to_owned(), None, Some("\\0")),
            ("backslash", "\\".to_owned(), None, Some("\\\\")),
            ("backslash 0", "\\0".to_owned(), None, Some("\\\\0")),
        ];
        for (id, token, constraint, stored) in rows {
            let answer = match validate_client_request_token(&token) {
                Ok(()) => None,
                Err(DynamoDbError::ValidationException(m)) => Some(m),
                Err(e) => panic!("{id}: {e:?}"),
            };
            let want = constraint.map(|c| {
                format!(
                    "1 validation error detected: Value at 'ClientRequestToken' failed to \
                     satisfy constraint: {c}"
                )
            });
            assert_eq!(answer, want, "{id}");
            assert_eq!(stored_token(&token), stored.unwrap_or(&token), "{id}");
        }
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
