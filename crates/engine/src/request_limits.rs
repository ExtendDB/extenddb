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
//! a number set by the body size. Authorization walks every sub-operation of a
//! `TransactWriteItems` item before the handler's one-operation rule rejects an
//! item with several, so a transaction reads at most 400 tables (100 items, four
//! sub-operations each) and evaluates at most 400 targets. Both paths run
//! [`check_batch_counts`] and
//! [`check_transact_count`], so they apply the checks in one order and return
//! the same text.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{BatchGetItemInput, BatchWriteItemInput};
use serde_json::Value;

/// Maximum number of keys across all tables in one `BatchGetItem` request,
/// and the maximum number of tables in its `RequestItems`.
pub(crate) const MAX_BATCH_GET_KEYS: usize = 100;
/// Maximum number of write requests across all tables in one `BatchWriteItem`,
/// and the maximum number of tables in its `RequestItems`.
pub(crate) const MAX_BATCH_WRITE_ITEMS: usize = 25;
/// Maximum number of items in one `TransactGetItems` or `TransactWriteItems`.
pub(crate) const MAX_TRANSACT_ITEMS: usize = 100;
/// The most failed constraints one refusal lists; its count then says 10 too
/// (measured 2026-11-02).
pub(crate) const MAX_PARTS: usize = 10;

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

/// Check the table and item counts of a `BatchGetItem` or `BatchWriteItem`.
///
/// `tables` lists each entry of `RequestItems` with its key count
/// (`BatchGetItem`) or write-request count (`BatchWriteItem`), or `None` when
/// the entry is not the expected shape or has no `Keys` list; the handler
/// rejects such an entry, so it counts towards the number of tables only.
/// The checks run in this order: an empty `RequestItems`, then too many
/// tables and the tables with too many items or none, in one refusal, then
/// too many items in total.
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

    // The service names the table count first when it is over the limit,
    // then each table with too many items or none, at most `MAX_PARTS`
    // clauses in all, before it counts the total, and counts only the clauses
    // it lists. It listed the tables in a different order from one send to
    // the next (measured 2026-11-20).
    let member = if get { "member.Keys" } else { "member" };
    let failed: Vec<(String, usize)> = (tables.len() > limit)
        .then(|| ("RequestItems".to_owned(), tables.len()))
        .into_iter()
        .chain(tables.iter().filter_map(|&(t, count)| {
            let n = count.filter(|&n| n == 0 || n > limit)?;
            Some((format!("RequestItems.{t}.{member}"), n))
        }))
        .take(MAX_PARTS)
        .collect();
    if !failed.is_empty() {
        return Err(count_error(limit, &failed));
    }

    let total: usize = tables.iter().filter_map(|(_, n)| *n).sum();
    if total > limit {
        return Err(DynamoDbError::ValidationException(format!(
            "Too many items requested for the {operation} call"
        )));
    }
    Ok(())
}

/// Check a parsed `BatchGetItem`: [`check_batch_counts`], then each table
/// value with no `Keys` or a null one. The service names each such table in
/// one refusal, in table name order, not the order of the request (measured
/// 2026-11-20 and 2026-11-21).
///
/// # Errors
///
/// `ValidationException` for the first check that fails.
pub(crate) fn check_batch_get(input: &BatchGetItemInput) -> Result<(), DynamoDbError> {
    let tables: Vec<(&str, Option<usize>)> = input
        .request_items
        .iter()
        .map(|(table, ka)| (table.as_str(), ka.keys.as_ref().map(Vec::len)))
        .collect();
    check_batch_counts("BatchGetItem", &tables)?;
    let mut missing: Vec<&str> = tables
        .iter()
        .filter_map(|&(table, keys)| keys.is_none().then_some(table))
        .collect();
    missing.sort_unstable();
    let clauses: Vec<String> = missing
        .iter()
        .take(MAX_PARTS)
        .map(|t| {
            format!(
                "Value null at 'requestItems.{t}.member.keys' failed to satisfy constraint: Member must not be null"
            )
        })
        .collect();
    if clauses.is_empty() {
        Ok(())
    } else {
        Err(refusal(&clauses))
    }
}

/// The refusal naming each path of `failed`, `RequestItems` or a table's
/// member path, whose count is 0 or over `limit` (measured 2026-11-20).
fn count_error(limit: usize, failed: &[(String, usize)]) -> DynamoDbError {
    let clauses: Vec<String> = failed
        .iter()
        .map(|(path, count)| {
            let bound = if *count == 0 {
                "greater than or equal to 1".to_owned()
            } else {
                format!("less than or equal to {limit}")
            };
            format!(
                "Value at '{path}' failed to satisfy constraint: Member must have length {bound}"
            )
        })
        .collect();
    refusal(&clauses)
}

/// One refusal listing `clauses`, each a failed constraint.
fn refusal(clauses: &[String]) -> DynamoDbError {
    let n = clauses.len();
    DynamoDbError::ValidationException(format!(
        "{n} validation error{} detected: {}",
        if n == 1 { "" } else { "s" },
        clauses.join("; ")
    ))
}

/// Refuse a batch or transaction request whose counts fall outside the
/// operation's limits, reading the counts from the raw request body.
///
/// Runs [`check_batch_counts`] or [`check_transact_count`], the checks the
/// handlers run on the parsed request. A body whose top-level container is
/// missing or not the expected type is left to the handler, which rejects it
/// when it parses the request. A batch outside its counts can get the
/// handler's `SerializationException` instead, as `type_fault` says. Any
/// other operation passes.
///
/// # Errors
///
/// Returns the `ValidationException` or `SerializationException` the
/// operation's handler returns for the same request.
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
                check_batch_counts(operation, &tables)
                    .map_err(|count| type_fault(operation, body).unwrap_or(count))?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// The handler's `SerializationException` for a batch body, when its parse
/// finds a value of another JSON type than the member's. The service answers
/// that ahead of the counts. It answers the count clause for a missing or null
/// member and for the string `"true"` or `"false"` where a boolean goes
/// (measured 2026-11-20 and 2026-11-21). The `AttributeValue` reader names its
/// faults in its own words, which do not tell a null or an accepted string
/// from a value of another type, so those keep the count clause too.
fn type_fault(operation: &str, body: &Value) -> Option<DynamoDbError> {
    let e = if operation == "BatchGetItem" {
        <BatchGetItemInput as serde::Deserialize>::deserialize(body).err()
    } else {
        <BatchWriteItemInput as serde::Deserialize>::deserialize(body).err()
    }?;
    let text = e.to_string();
    let boolean = text == "invalid type: string \"true\", expected a boolean"
        || text == "invalid type: string \"false\", expected a boolean";
    if !text.starts_with("invalid type: ") || text.starts_with("invalid type: null") || boolean {
        return None;
    }
    match crate::deserialize_error(e) {
        fault @ DynamoDbError::SerializationException(_) => Some(fault),
        _ => None,
    }
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
        assert_eq!(
            message(check_request_item_counts(
                "BatchGetItem",
                &bgi(&[("a", 60), ("b", 41)])
            )),
            "Too many items requested for the BatchGetItem call"
        );
    }

    #[test]
    fn batch_write_limits_in_order() {
        assert!(check_request_item_counts("BatchWriteItem", &bwi(&[("a", 25)])).is_ok());
        assert_eq!(
            message(check_request_item_counts("BatchWriteItem", &bwi(&[]))),
            "The requestItems parameter is required for BatchWriteItem"
        );
        assert_eq!(
            message(check_request_item_counts(
                "BatchWriteItem",
                &bwi(&[("a", 13), ("b", 13)])
            )),
            "Too many items requested for the BatchWriteItem call"
        );
    }

    #[test]
    fn batch_count_refusals_name_each_path() {
        // The service's answers to these requests, measured 2026-11-20, and
        // 2026-10-21 for the last two.
        const LE25: &str = "less than or equal to 25";
        const LE100: &str = "less than or equal to 100";
        const GE1: &str = "greater than or equal to 1";
        let at = |path: &str, bound: &str| {
            format!(
                "Value at '{path}' failed to satisfy constraint: Member must have length {bound}"
            )
        };
        let w = |t: &str, bound: &str| at(&format!("RequestItems.{t}.member"), bound);
        let g = |t: &str, bound: &str| at(&format!("RequestItems.{t}.member.Keys"), bound);
        let one = |c: String| format!("1 validation error detected: {c}");
        let two = |a: String, b: String| format!("2 validation errors detected: {a}; {b}");
        let many = names(101);
        let first = |n: usize, count: usize| -> Vec<(&str, usize)> {
            many[..n].iter().map(|t| (t.as_str(), count)).collect()
        };
        let ten = |clause: &dyn Fn(&str) -> String| {
            let clauses: Vec<String> = many[..10].iter().map(|t| clause(t)).collect();
            format!("10 validation errors detected: {}", clauses.join("; "))
        };
        // `n` tables of one item, except none in `empty` and `big.1` at `big.0`.
        let mix = |n: usize, empty: std::ops::Range<usize>, big: Option<(usize, usize)>| {
            let mut tables = first(n, 1);
            tables[empty].iter_mut().for_each(|t| t.1 = 0);
            if let Some((i, count)) = big {
                tables[i].1 = count;
            }
            tables
        };
        let gap = |n: usize, empty: std::ops::Range<usize>| mix(n, empty, None);
        // The table count clause, then `tables`.
        let over = |bound: &str, tables: &[String]| {
            let clauses = [&[at("RequestItems", bound)][..], tables].concat();
            format!(
                "{} validation errors detected: {}",
                clauses.len(),
                clauses.join("; ")
            )
        };
        // The table count clause, then `clause` of each table from t001 to t009.
        // Past 9 failed tables, the service named another 9 on each send, so
        // the rows with 10 failed tables expect the first 9 in name order.
        let nine = |bound: &str, clause: &dyn Fn(&str) -> String| {
            let tables: Vec<String> = many[1..10].iter().map(|t| clause(t)).collect();
            over(bound, &tables)
        };
        let (bw, bg) = ("BatchWriteItem", "BatchGetItem");
        let cases = [
            (bw, bwi(&[("t.2", 26)]), one(w("t.2", LE25))),
            (
                bw,
                bwi(&[("a", 26), ("b", 26)]),
                two(w("a", LE25), w("b", LE25)),
            ),
            (bw, bwi(&[("a", 1), ("b", 26)]), one(w("b", LE25))),
            (bw, bwi(&first(11, 26)), ten(&|t| w(t, LE25))),
            (bw, bwi(&first(26, 1)), one(at("RequestItems", LE25))),
            (bw, bwi(&[("a", 1), ("b", 0)]), one(w("b", GE1))),
            (
                bw,
                bwi(&[("a", 26), ("b", 0)]),
                two(w("a", LE25), w("b", GE1)),
            ),
            (bw, bwi(&[("a", 20), ("b", 10), ("c", 0)]), one(w("c", GE1))),
            (bw, bwi(&first(11, 0)), ten(&|t| w(t, GE1))),
            (bw, bwi(&gap(26, 13..14)), over(LE25, &[w("t013", GE1)])),
            (bw, bwi(&gap(30, 13..14)), over(LE25, &[w("t013", GE1)])),
            (bw, bwi(&gap(26, 1..10)), nine(LE25, &|t| w(t, GE1))),
            (bw, bwi(&gap(26, 1..11)), nine(LE25, &|t| w(t, GE1))),
            (
                bw,
                bwi(&mix(26, 5..6, Some((9, 26)))),
                over(LE25, &[w("t005", GE1), w("t009", LE25)]),
            ),
            (
                bg,
                bgi(&[("a", 101), ("b", 101)]),
                two(g("a", LE100), g("b", LE100)),
            ),
            (bg, bgi(&first(11, 101)), ten(&|t| g(t, LE100))),
            (
                bg,
                bgi(&[("a", 101), ("b", 0)]),
                two(g("a", LE100), g("b", GE1)),
            ),
            (bg, bgi(&[("a", 60), ("b", 41), ("c", 0)]), one(g("c", GE1))),
            (bg, bgi(&gap(101, 50..51)), over(LE100, &[g("t050", GE1)])),
            (bg, bgi(&gap(101, 1..10)), nine(LE100, &|t| g(t, GE1))),
            (bg, bgi(&gap(101, 1..11)), nine(LE100, &|t| g(t, GE1))),
            (
                bg,
                bgi(&mix(101, 5..6, Some((9, 101)))),
                over(LE100, &[g("t005", GE1), g("t009", LE100)]),
            ),
            (bg, bgi(&[("a", 101)]), one(g("a", LE100))),
            (bg, bgi(&[("a", 1), ("b", 0)]), one(g("b", GE1))),
        ];
        let failed: Vec<String> = cases
            .into_iter()
            .enumerate()
            .filter_map(|(row, (op, body, expected))| {
                let got = message(check_request_item_counts(op, &body));
                (got != expected).then(|| format!("row {row}: {got}"))
            })
            .collect();
        assert!(failed.is_empty(), "{}", failed.join("\n"));
    }

    #[test]
    fn malformed_entries_in_order() {
        // Measured 2026-11-20 and 2026-11-21. Rows 0 to 4, 6 and 7 have the
        // service's error type, and row 4 its text too. Rows 8 to 10 and 13 to
        // 16 pin this server's answer, which has the service's type only. Rows
        // 5, 11 and 12 are within the counts, where the check passes.
        let many = names(101);
        // `n` tables of one item each, with the value of `odd` at its indexes.
        let body = |op: &str, n: usize, odd: &[(usize, Value)]| {
            let tables: Vec<(&str, usize)> = many[..n].iter().map(|t| (t.as_str(), 1)).collect();
            let mut body = if op == "BatchGetItem" {
                bgi(&tables)
            } else {
                bwi(&tables)
            };
            for (i, value) in odd {
                body["RequestItems"][many[*i].as_str()] = value.clone();
            }
            body
        };
        let count = |limit: usize| {
            format!(
                "Value at 'RequestItems' failed to satisfy constraint: Member must have length less than or equal to {limit}"
            )
        };
        let one = |c: String| format!("1 validation error detected: {c}");
        // A write list of one put whose attribute `x` is `v`.
        let put_x = |v: Value| json!([{"PutRequest": {"Item": {"pk": {"S": "a"}, "x": v}}}]);
        let ser = || "SerializationException".to_owned();
        let ok = || "Ok".to_owned();
        let (bw, bg) = ("BatchWriteItem", "BatchGetItem");
        let cases = [
            (bw, 26, vec![(5, json!(1))], ser()),
            (bw, 26, vec![(5, json!(1)), (9, json!([]))], ser()),
            (bw, 26, vec![(5, json!({}))], ser()),
            (bw, 26, vec![(5, json!([1]))], ser()),
            (bw, 26, vec![(5, json!([{}]))], one(count(25))),
            (bw, 2, vec![(1, json!(1))], ok()),
            (bg, 101, vec![(5, json!(1))], ser()),
            (bg, 101, vec![(5, json!({"Keys": 1}))], ser()),
            (bg, 101, vec![(5, json!({}))], one(count(100))),
            (bg, 101, vec![(5, json!({"Keys": null}))], one(count(100))),
            (
                bg,
                101,
                vec![(5, json!({})), (9, json!({"Keys": []}))],
                format!(
                    "2 validation errors detected: {}; Value at 'RequestItems.t009.member.Keys' failed to satisfy constraint: Member must have length greater than or equal to 1",
                    count(100)
                ),
            ),
            (bg, 2, vec![(1, json!({}))], ok()),
            (bg, 2, vec![(1, json!(1))], ok()),
            (bw, 26, vec![(5, put_x(json!({"S": null})))], one(count(25))),
            (bw, 26, vec![(5, put_x(json!({"X": "a"})))], one(count(25))),
            (
                bw,
                26,
                vec![(5, put_x(json!({"BOOL": "true"})))],
                one(count(25)),
            ),
            (
                bg,
                101,
                vec![(
                    5,
                    json!({"Keys": [{"pk": {"S": "a"}}], "ConsistentRead": "true"}),
                )],
                one(count(100)),
            ),
        ];
        let failed: Vec<String> = cases
            .into_iter()
            .enumerate()
            .filter_map(|(row, (op, n, odd, expected))| {
                let got = match check_request_item_counts(op, &body(op, n, &odd)) {
                    Ok(()) => ok(),
                    Err(DynamoDbError::SerializationException(_)) => ser(),
                    Err(DynamoDbError::ValidationException(m)) => m,
                    Err(other) => format!("{other:?}"),
                };
                (got != expected).then(|| format!("row {row}: {got}"))
            })
            .collect();
        assert!(failed.is_empty(), "{}", failed.join("\n"));
        assert!(check_request_item_counts("TransactWriteItems", &json!({})).is_ok());
        assert!(check_request_item_counts("Query", &transact("TransactGetItems", 101)).is_ok());
    }

    #[test]
    fn batch_get_tables_without_keys() {
        // The service's answers, measured 2026-11-20 for rows 0 to 2 and
        // 2026-11-21 for the rest. Rows 6 and 7 compare the type only.
        let null = |t: &str| {
            format!(
                "Value null at 'requestItems.{t}.member.keys' failed to satisfy constraint: Member must not be null"
            )
        };
        let one = |c: String| format!("1 validation error detected: {c}");
        let two =
            |a: &str, b: &str| format!("2 validation errors detected: {}; {}", null(a), null(b));
        let get = |items: Value| json!({ "RequestItems": items });
        let keys = || json!({"Keys": [{"pk": {"S": "a"}}]});
        let ser = || "SerializationException".to_owned();
        let cases = [
            (get(json!({"t000": keys(), "t001": {}})), one(null("t001"))),
            (
                get(json!({"t000": keys(), "t001": {"Keys": null}})),
                one(null("t001")),
            ),
            (get(json!({"t000": {}})), one(null("t000"))),
            (get(json!({"t000": {}, "t001": {}})), two("t000", "t001")),
            (
                get(json!({"t000": {}, "t001": keys(), "t002": {"Keys": null}})),
                two("t000", "t002"),
            ),
            (
                get(json!({"zz": {}, "a": {}, "mmm": {}})),
                format!(
                    "3 validation errors detected: {}; {}; {}",
                    null("a"),
                    null("mmm"),
                    null("zz")
                ),
            ),
            (
                get(json!({"t000": {"Keys": null, "ConsistentRead": "x"}})),
                ser(),
            ),
            (
                get(json!({"t000": {"Keys": null, "ProjectionExpression": 5}})),
                ser(),
            ),
            (
                get(json!({"t000": {"Keys": []}})),
                one(
                    "Value at 'RequestItems.t000.member.Keys' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned(),
                ),
            ),
        ];
        let failed: Vec<String> = cases
            .into_iter()
            .enumerate()
            .filter_map(|(row, (body, expected))| {
                let checked = serde_json::from_value::<BatchGetItemInput>(body)
                    .map_err(crate::deserialize_error)
                    .and_then(|input| check_batch_get(&input));
                let got = match checked {
                    Ok(()) => "Ok".to_owned(),
                    Err(DynamoDbError::SerializationException(_)) => ser(),
                    Err(DynamoDbError::ValidationException(m)) => m,
                    Err(other) => format!("{other:?}"),
                };
                (got != expected).then(|| format!("row {row}: {got}"))
            })
            .collect();
        assert!(failed.is_empty(), "{}", failed.join("\n"));
    }
}
