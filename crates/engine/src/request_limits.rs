// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Item-count limits of the batch and transaction operations, and the
//! `ValidationException` each limit returns.
//!
//! The handlers check these limits on the parsed request. The server checks
//! them on the raw request body before authorization, through
//! [`check_request_item_counts`], because `DynamoDB` returns these errors before
//! it evaluates any policy: a 101-item transaction, a 26-request
//! `BatchWriteItem` and a 101-key `BatchGetItem` each return
//! `ValidationException` to a principal with no permissions (measured
//! 2026-10-21). Checking first also bounds the per-item work authorization does
//! by the operation's limit instead of by the body size. Both paths build their
//! errors with the functions here, so they return the same text.

use extenddb_core::error::DynamoDbError;
use serde_json::Value;

/// Maximum number of keys across all tables in one `BatchGetItem` request.
pub(crate) const MAX_BATCH_GET_KEYS: usize = 100;
/// Maximum number of write requests across all tables in one `BatchWriteItem`.
pub(crate) const MAX_BATCH_WRITE_ITEMS: usize = 25;
/// Maximum number of items in one `TransactGetItems` or `TransactWriteItems`.
pub(crate) const MAX_TRANSACT_ITEMS: usize = 100;

/// More than [`MAX_TRANSACT_ITEMS`] items. `member` is `TransactGetItem` or
/// `TransactWriteItem`, repeated once per item as the service's message does.
pub(crate) fn transact_items_too_many(count: usize, member: &str) -> DynamoDbError {
    let items = vec![member; count].join(", ");
    DynamoDbError::ValidationException(format!(
        "1 validation error detected: Value '[{items}]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100"
    ))
}

/// One table in a `BatchWriteItem` has more than [`MAX_BATCH_WRITE_ITEMS`]
/// write requests.
pub(crate) fn batch_write_table_too_many(table_name: &str, count: usize) -> DynamoDbError {
    let items_repr = vec!["WriteRequest"; count].join(", ");
    DynamoDbError::ValidationException(format!(
        "1 validation error detected: Value '{{{table_name}=[{items_repr}]}}' at 'requestItems' failed to satisfy constraint: \
         Map value must satisfy constraint: [Member must have length less than or equal to 25, \
         Member must have length greater than or equal to 1]"
    ))
}

/// A `BatchWriteItem` has more than [`MAX_BATCH_WRITE_ITEMS`] write requests
/// across all of its tables.
pub(crate) fn batch_write_too_many() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "Too many items requested for the BatchWriteItem call".to_owned(),
    )
}

/// One table in a `BatchGetItem` has more than [`MAX_BATCH_GET_KEYS`] keys.
pub(crate) fn batch_get_table_too_many(table_name: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "1 validation error detected: Value at 'RequestItems.{table_name}.member.Keys' failed to satisfy constraint: \
         Member must have length less than or equal to 100"
    ))
}

/// A `BatchGetItem` has more than [`MAX_BATCH_GET_KEYS`] keys across all of
/// its tables.
pub(crate) fn batch_get_too_many() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "Too many items requested for the BatchGetItem call".to_owned(),
    )
}

/// Refuse a batch or transaction request whose item counts exceed the
/// operation's limits, reading the counts from the raw request body.
///
/// The checks and their order are the handlers': a per-table limit is reported
/// before the request-wide limit. A body whose structure is not what the
/// operation expects is left to the handler, which rejects it when it parses
/// the request. Any other operation passes.
///
/// # Errors
///
/// Returns the `ValidationException` the operation's handler returns for the
/// same request.
pub fn check_request_item_counts(operation: &str, body: &Value) -> Result<(), DynamoDbError> {
    match operation {
        "TransactGetItems" | "TransactWriteItems" => {
            let member = if operation == "TransactGetItems" {
                "TransactGetItem"
            } else {
                "TransactWriteItem"
            };
            if let Some(items) = body.get("TransactItems").and_then(Value::as_array)
                && items.len() > MAX_TRANSACT_ITEMS
            {
                return Err(transact_items_too_many(items.len(), member));
            }
        }
        "BatchWriteItem" => {
            if let Some(tables) = body.get("RequestItems").and_then(Value::as_object) {
                let mut total = 0usize;
                for (table_name, requests) in tables {
                    let count = requests.as_array().map_or(0, Vec::len);
                    if count > MAX_BATCH_WRITE_ITEMS {
                        return Err(batch_write_table_too_many(table_name, count));
                    }
                    total += count;
                }
                if total > MAX_BATCH_WRITE_ITEMS {
                    return Err(batch_write_too_many());
                }
            }
        }
        "BatchGetItem" => {
            if let Some(tables) = body.get("RequestItems").and_then(Value::as_object) {
                let mut total = 0usize;
                for (table_name, keys_and_attributes) in tables {
                    let count = keys_and_attributes
                        .get("Keys")
                        .and_then(Value::as_array)
                        .map_or(0, Vec::len);
                    if count > MAX_BATCH_GET_KEYS {
                        return Err(batch_get_table_too_many(table_name));
                    }
                    total += count;
                }
                if total > MAX_BATCH_GET_KEYS {
                    return Err(batch_get_too_many());
                }
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn message(result: Result<(), DynamoDbError>) -> String {
        match result {
            Err(DynamoDbError::ValidationException(m)) => m,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    fn transact(op: &str, n: usize) -> Value {
        let entry = if op == "TransactGetItems" {
            json!({"Get": {"TableName": "t", "Key": {"pk": {"S": "a"}}}})
        } else {
            json!({"ConditionCheck": {"TableName": "t", "Key": {"pk": {"S": "a"}},
                   "ConditionExpression": "attribute_exists(pk)"}})
        };
        json!({ "TransactItems": vec![entry; n] })
    }

    #[test]
    fn transactions_at_the_limit_pass_and_over_it_fail() {
        for (op, member) in [
            ("TransactGetItems", "TransactGetItem"),
            ("TransactWriteItems", "TransactWriteItem"),
        ] {
            assert!(check_request_item_counts(op, &transact(op, 100)).is_ok());
            let m = message(check_request_item_counts(op, &transact(op, 101)));
            assert!(m.starts_with(&format!("1 validation error detected: Value '[{member}, ")));
            assert!(m.ends_with("Member must have length less than or equal to 100"));
            assert_eq!(m.matches(member).count(), 101);
        }
    }

    #[test]
    fn batch_write_per_table_limit_is_reported_before_the_total() {
        let put = json!({"PutRequest": {"Item": {"pk": {"S": "a"}}}});
        let ok = json!({"RequestItems": {"a": vec![put.clone(); 25]}});
        assert!(check_request_item_counts("BatchWriteItem", &ok).is_ok());

        let one = json!({"RequestItems": {"a": vec![put.clone(); 26]}});
        let m = message(check_request_item_counts("BatchWriteItem", &one));
        assert!(m.starts_with("1 validation error detected: Value '{a=[WriteRequest, "));

        let two = json!({"RequestItems": {"a": vec![put.clone(); 13], "b": vec![put; 13]}});
        assert_eq!(
            message(check_request_item_counts("BatchWriteItem", &two)),
            "Too many items requested for the BatchWriteItem call"
        );
    }

    #[test]
    fn batch_get_per_table_limit_is_reported_before_the_total() {
        let key = json!({"pk": {"S": "a"}});
        let ok = json!({"RequestItems": {"a": {"Keys": vec![key.clone(); 100]}}});
        assert!(check_request_item_counts("BatchGetItem", &ok).is_ok());

        let one = json!({"RequestItems": {"a": {"Keys": vec![key.clone(); 101]}}});
        assert_eq!(
            message(check_request_item_counts("BatchGetItem", &one)),
            "1 validation error detected: Value at 'RequestItems.a.member.Keys' failed to satisfy constraint: Member must have length less than or equal to 100"
        );

        let two = json!({"RequestItems": {"a": {"Keys": vec![key.clone(); 60]},
                                          "b": {"Keys": vec![key; 41]}}});
        assert_eq!(
            message(check_request_item_counts("BatchGetItem", &two)),
            "Too many items requested for the BatchGetItem call"
        );
    }

    #[test]
    fn malformed_bodies_and_other_operations_pass() {
        assert!(check_request_item_counts("TransactWriteItems", &json!({})).is_ok());
        assert!(
            check_request_item_counts("BatchWriteItem", &json!({"RequestItems": {"a": 1}})).is_ok()
        );
        assert!(check_request_item_counts("Query", &transact("TransactGetItems", 101)).is_ok());
    }
}
