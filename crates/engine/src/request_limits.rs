// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Count limits of the batch and transaction operations, and the
//! `ValidationException` each limit returns.
//!
//! The handlers check these limits on the parsed request. The server checks
//! them on the raw request body before authorization, through
//! [`check_request_item_counts`], because `DynamoDB` returns these errors before
//! it evaluates any policy: an empty or 101-item transaction, an empty
//! `RequestItems`, a table with no keys or requests, a 26-request
//! `BatchWriteItem` and a 101-key `BatchGetItem` each return
//! `ValidationException` to a principal with no permissions (measured
//! 2026-10-21). Checking first also bounds the work authorization does per
//! request: at most 100 tables and 100 keys for `BatchGetItem`, 25 tables and
//! 25 requests for `BatchWriteItem`, and 100 items for a transaction, instead of
//! a number set by the body size. Both paths run [`check_batch_counts`] and
//! [`check_transact_count`], so they apply the checks in one order and return
//! the same text.

use extenddb_core::error::DynamoDbError;
use serde_json::Value;

/// Maximum number of keys across all tables in one `BatchGetItem` request,
/// and the maximum number of tables in its `RequestItems`.
pub(crate) const MAX_BATCH_GET_KEYS: usize = 100;
/// Maximum number of write requests across all tables in one `BatchWriteItem`,
/// and the maximum number of tables in its `RequestItems`.
pub(crate) const MAX_BATCH_WRITE_ITEMS: usize = 25;
/// Maximum number of items in one `TransactGetItems` or `TransactWriteItems`.
pub(crate) const MAX_TRANSACT_ITEMS: usize = 100;

/// Check the item count of a transaction. `member` is `TransactGetItem` or
/// `TransactWriteItem`. Over the limit, the message lists the member name once
/// per item; the service lists each item's full serialization instead, so the
/// count matches and the item text does not.
///
/// # Errors
///
/// `ValidationException` when `count` is 0 or above [`MAX_TRANSACT_ITEMS`].
pub(crate) fn check_transact_count(count: usize, member: &str) -> Result<(), DynamoDbError> {
    if count == 0 {
        return Err(DynamoDbError::ValidationException(
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned(),
        ));
    }
    if count > MAX_TRANSACT_ITEMS {
        let items = vec![member; count].join(", ");
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '[{items}]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100"
        )));
    }
    Ok(())
}

/// The write-request list of one `BatchWriteItem` table as the service echoes
/// it in a validation message, with each request printed as its type name.
fn write_requests_repr(count: usize) -> String {
    vec!["WriteRequest"; count].join(", ")
}

/// Check the table and item counts of a `BatchGetItem` or `BatchWriteItem`.
///
/// `tables` lists each entry of `RequestItems` with its key count
/// (`BatchGetItem`) or write-request count (`BatchWriteItem`), or `None` when
/// the entry is not the expected shape; the handler's deserialization rejects
/// such an entry, so it counts towards the number of tables only. The checks
/// run in this order: an empty `RequestItems`, too many tables, too many items
/// in one table, too many items in total, a table with no items.
///
/// # Errors
///
/// `ValidationException` for the first check that fails.
pub(crate) fn check_batch_counts(
    operation: &str,
    tables: &[(&str, Option<usize>)],
) -> Result<(), DynamoDbError> {
    let get = operation == "BatchGetItem";
    let limit = if get {
        MAX_BATCH_GET_KEYS
    } else {
        MAX_BATCH_WRITE_ITEMS
    };

    if tables.is_empty() {
        return Err(DynamoDbError::ValidationException(if get {
            "1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned()
        } else {
            "The requestItems parameter is required for BatchWriteItem".to_owned()
        }));
    }

    if tables.len() > limit {
        return Err(DynamoDbError::ValidationException(if get {
            "1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length less than or equal to 100".to_owned()
        } else {
            let map = tables
                .iter()
                .map(|(t, n)| format!("{t}=[{}]", write_requests_repr(n.unwrap_or(0))))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "1 validation error detected: Value '{{{map}}}' at 'requestItems' failed to satisfy constraint: Member must have length less than or equal to 25"
            )
        }));
    }

    for &(table_name, count) in tables {
        if let Some(count) = count
            && count > limit
        {
            return Err(per_table_count_error(get, table_name, count));
        }
    }

    let total: usize = tables.iter().filter_map(|(_, n)| *n).sum();
    if total > limit {
        return Err(DynamoDbError::ValidationException(format!(
            "Too many items requested for the {operation} call"
        )));
    }

    for &(table_name, count) in tables {
        if count == Some(0) {
            return Err(per_table_count_error(get, table_name, 0));
        }
    }
    Ok(())
}

/// One table's key or write-request count is 0 or above the limit. The service
/// returns one message for both bounds of a `BatchWriteItem` table and one
/// message per bound for a `BatchGetItem` table (measured 2026-10-21).
fn per_table_count_error(get: bool, table_name: &str, count: usize) -> DynamoDbError {
    if get {
        let bound = if count == 0 {
            "greater than or equal to 1"
        } else {
            "less than or equal to 100"
        };
        DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value at 'RequestItems.{table_name}.member.Keys' failed to satisfy constraint: Member must have length {bound}"
        ))
    } else {
        DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{{{table_name}=[{}]}}' at 'requestItems' failed to satisfy constraint: \
             Map value must satisfy constraint: [Member must have length less than or equal to 25, \
             Member must have length greater than or equal to 1]",
            write_requests_repr(count)
        ))
    }
}

/// Refuse a batch or transaction request whose counts fall outside the
/// operation's limits, reading the counts from the raw request body.
///
/// Runs [`check_batch_counts`] or [`check_transact_count`], the checks the
/// handlers run on the parsed request. A body whose top-level container is
/// missing or not the expected type is left to the handler, which rejects it
/// when it parses the request. Any other operation passes.
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
            if let Some(items) = body.get("TransactItems").and_then(Value::as_array) {
                check_transact_count(items.len(), member)?;
            }
        }
        "BatchGetItem" | "BatchWriteItem" => {
            if let Some(map) = body.get("RequestItems").and_then(Value::as_object) {
                let tables: Vec<(&str, Option<usize>)> = map
                    .iter()
                    .map(|(table_name, entry)| {
                        let items = if operation == "BatchGetItem" {
                            entry.get("Keys")
                        } else {
                            Some(entry)
                        };
                        (
                            table_name.as_str(),
                            items.and_then(Value::as_array).map(Vec::len),
                        )
                    })
                    .collect();
                check_batch_counts(operation, &tables)?;
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

    fn bgi(tables: &[(&str, usize)]) -> Value {
        let key = json!({"pk": {"S": "a"}});
        let map: serde_json::Map<String, Value> = tables
            .iter()
            .map(|(t, n)| ((*t).to_owned(), json!({"Keys": vec![key.clone(); *n]})))
            .collect();
        json!({ "RequestItems": map })
    }

    fn bwi(tables: &[(&str, usize)]) -> Value {
        let put = json!({"PutRequest": {"Item": {"pk": {"S": "a"}}}});
        let map: serde_json::Map<String, Value> = tables
            .iter()
            .map(|(t, n)| ((*t).to_owned(), json!(vec![put.clone(); *n])))
            .collect();
        json!({ "RequestItems": map })
    }

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("t{i:03}")).collect()
    }

    #[test]
    fn transactions_outside_the_limits_fail() {
        for (op, member) in [
            ("TransactGetItems", "TransactGetItem"),
            ("TransactWriteItems", "TransactWriteItem"),
        ] {
            assert!(check_request_item_counts(op, &transact(op, 1)).is_ok());
            assert!(check_request_item_counts(op, &transact(op, 100)).is_ok());
            let m = message(check_request_item_counts(op, &transact(op, 101)));
            assert!(m.starts_with(&format!("1 validation error detected: Value '[{member}, ")));
            assert!(m.ends_with("Member must have length less than or equal to 100"));
            assert_eq!(m.matches(member).count(), 101);
            assert_eq!(
                message(check_request_item_counts(op, &transact(op, 0))),
                "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1"
            );
        }
    }

    #[test]
    fn batch_get_limits_in_order() {
        assert!(check_request_item_counts("BatchGetItem", &bgi(&[("a", 100)])).is_ok());
        assert_eq!(
            message(check_request_item_counts("BatchGetItem", &bgi(&[]))),
            "1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1"
        );
        // 101 tables of one key each: the table count is reported, not the total.
        let many = names(101);
        let tables: Vec<(&str, usize)> = many.iter().map(|t| (t.as_str(), 1)).collect();
        assert_eq!(
            message(check_request_item_counts("BatchGetItem", &bgi(&tables))),
            "1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length less than or equal to 100"
        );
        // 101 tables with no keys: still the table count.
        let tables: Vec<(&str, usize)> = many.iter().map(|t| (t.as_str(), 0)).collect();
        assert!(
            message(check_request_item_counts("BatchGetItem", &bgi(&tables)))
                .ends_with("Member must have length less than or equal to 100")
        );
        assert_eq!(
            message(check_request_item_counts(
                "BatchGetItem",
                &bgi(&[("a", 101)])
            )),
            "1 validation error detected: Value at 'RequestItems.a.member.Keys' failed to satisfy constraint: Member must have length less than or equal to 100"
        );
        assert_eq!(
            message(check_request_item_counts(
                "BatchGetItem",
                &bgi(&[("a", 60), ("b", 41)])
            )),
            "Too many items requested for the BatchGetItem call"
        );
        assert_eq!(
            message(check_request_item_counts(
                "BatchGetItem",
                &bgi(&[("a", 1), ("b", 0)])
            )),
            "1 validation error detected: Value at 'RequestItems.b.member.Keys' failed to satisfy constraint: Member must have length greater than or equal to 1"
        );
    }

    #[test]
    fn batch_write_limits_in_order() {
        assert!(check_request_item_counts("BatchWriteItem", &bwi(&[("a", 25)])).is_ok());
        assert_eq!(
            message(check_request_item_counts("BatchWriteItem", &bwi(&[]))),
            "The requestItems parameter is required for BatchWriteItem"
        );
        let many = names(26);
        let tables: Vec<(&str, usize)> = many.iter().map(|t| (t.as_str(), 1)).collect();
        let m = message(check_request_item_counts("BatchWriteItem", &bwi(&tables)));
        assert!(m.starts_with("1 validation error detected: Value '{t000=[WriteRequest], "));
        assert!(m.ends_with(
            "}' at 'requestItems' failed to satisfy constraint: Member must have length less than or equal to 25"
        ));
        let m = message(check_request_item_counts(
            "BatchWriteItem",
            &bwi(&[("a", 26)]),
        ));
        assert!(m.starts_with("1 validation error detected: Value '{a=[WriteRequest, "));
        assert!(m.ends_with("Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]"));
        assert_eq!(
            message(check_request_item_counts(
                "BatchWriteItem",
                &bwi(&[("a", 13), ("b", 13)])
            )),
            "Too many items requested for the BatchWriteItem call"
        );
        assert_eq!(
            message(check_request_item_counts(
                "BatchWriteItem",
                &bwi(&[("a", 1), ("b", 0)])
            )),
            "1 validation error detected: Value '{b=[]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]"
        );
    }

    #[test]
    fn malformed_entries_count_as_tables_only() {
        // A table entry of the wrong shape is left to the handler, but it still
        // counts towards the number of tables.
        assert!(
            check_request_item_counts("BatchWriteItem", &json!({"RequestItems": {"a": 1}})).is_ok()
        );
        assert!(
            check_request_item_counts("BatchGetItem", &json!({"RequestItems": {"a": {}}})).is_ok()
        );
        let map: serde_json::Map<String, Value> =
            names(101).into_iter().map(|t| (t, json!({}))).collect();
        assert!(
            message(check_request_item_counts(
                "BatchGetItem",
                &json!({ "RequestItems": map })
            ))
            .ends_with("Member must have length less than or equal to 100")
        );
        assert!(check_request_item_counts("TransactWriteItems", &json!({})).is_ok());
        assert!(check_request_item_counts("Query", &transact("TransactGetItems", 101)).is_ok());
    }
}
