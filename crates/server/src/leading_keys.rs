// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `dynamodb:LeadingKeys` extraction for request authorization.
//!
//! IAM evaluates `dynamodb:LeadingKeys` against the partition key value(s) a
//! request touches. The server populates the key for every operation that
//! targets one or more specific partitions, so a `ForAllValues`/`ForAnyValue`
//! LeadingKeys condition on such a request is evaluated against the partitions
//! it reads or writes, not vacuously. `Scan` and `SearchVectors` target no
//! single partition: they reach the engine, return data, and carry no
//! LeadingKeys, so a `ForAllValues` condition is satisfied vacuously (the read
//! is allowed) and an operator fences them with `ForAnyValue` or a `Null`
//! guard, which is how the service treats a `Scan`. Operations the engine
//! rejects before any data flows (an unparseable or non-equality key condition,
//! a missing key condition, an unresolved placeholder name, a table that does
//! not exist) are left with the key absent on purpose: the engine returns
//! `ValidationException` or `ResourceNotFoundException`, which is what live
//! `DynamoDB` returns for those shapes even under a LeadingKeys policy (measured
//! 2026-10-20).
//!
//! Behaviour measured against live `DynamoDB`:
//!
//! - Item operations (`GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`): the
//!   partition key value from the request's `Key`/`Item`.
//! - `Query` on the base table or a local secondary index: the partition key
//!   value from the key condition (an LSI shares the base partition key).
//! - `Query` on a global secondary index: the index's partition key value from
//!   the key condition. The service binds LeadingKeys to the queried index's
//!   partition key, not the base table's.
//! - `Scan` and `SearchVectors`: no LeadingKeys (neither targets a single
//!   partition, so the service sets none).
//! - `BatchGetItem`/`BatchWriteItem`: one authorization per (action, table)
//!   with the set of every key that table receives in the request.
//! - `TransactGetItems`/`TransactWriteItems`: one authorization per item, each
//!   with that single item's partition key.
//!
//! An N partition key is written into LeadingKeys in one of two forms,
//! depending on the operation, because that is what the service does
//! (measured 2026-10-20). `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`,
//! `Query` and `BatchGetItem` use the normalized number (`010`, `10.0` and
//! `1E1` are all `10`). `BatchWriteItem`, `TransactGetItems` and
//! `TransactWriteItems` use the number as Java's `BigDecimal` prints it, which
//! keeps the scale the request was written with (`10.0` stays `10.0`, `1E1`
//! is `1E+1`, `010` is `10`). See [`NumberForm`]. A B partition key is its
//! base64 text and an S key is the string as sent.

use std::collections::HashMap;

use extenddb_core::expression::{Expr, PathElement, parse_key_condition, tokenize_with_limit};
use extenddb_core::types::{KeyType, TableKeyInfo};
use serde_json::Value;

/// A single `(action, table)` authorization to perform for a batch or
/// transaction operation, carrying the LeadingKeys the service would evaluate.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AuthzTarget {
    /// The bare operation string the action maps to (no `dynamodb:` prefix).
    pub action: String,
    /// The table the action is authorized against.
    pub table: String,
    /// The partition key value(s) for this authorization, or `None` when the
    /// key cannot be determined (the engine then rejects the request).
    pub leading_keys: Option<Vec<String>>,
}

/// Find the HASH (partition) key attribute name of a key schema.
fn hash_attr(schema: &[extenddb_core::types::KeySchemaElement]) -> Option<&str> {
    schema
        .iter()
        .find(|e| e.key_type == KeyType::Hash)
        .map(|e| e.attribute_name.as_str())
}

/// How an N partition key is written into `dynamodb:LeadingKeys`. The service
/// writes it differently depending on the operation (measured against live
/// `DynamoDB` on 2026-10-20 over 21 spellings, including `1E-7`, `1.23E+4`,
/// `0E+5`, `1.00E-130` and negative values).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NumberForm {
    /// The normalized number: `010`, `10.0`, `1E1` and `+10` are all `10`.
    /// Used by `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query` and
    /// `BatchGetItem`.
    Normalized,
    /// The number as Java's `new BigDecimal(s).toString()` prints it, which
    /// keeps the scale the request was written with: `10.0` stays `10.0`, `1E1`
    /// is `1E+1`, `100E-2` is `1.00`, and `010`, `+10` and `1.0E1` are `10`.
    /// Used by `BatchWriteItem` put requests, `TransactGetItems` and every
    /// `TransactWriteItems` action.
    Decimal,
}

/// Turn a `DynamoDB` typed scalar (`{"S": "v"}`, `{"N": "10"}`, `{"B": "AQI="}`)
/// into the string the service compares for LeadingKeys.
///
/// S and B are compared as sent. An N value is written in `form`, the form the
/// service uses for the operation path. The policy value is left exactly as
/// written, which is what the service does, so under a fence of `["10"]` the
/// spelling `10.0` matches on `GetItem` and does not match on
/// `TransactGetItems`. An N value the engine would reject as malformed yields
/// `None`, so the engine returns `ValidationException` before any data flows.
fn extract_typed_value(type_val: &Value, form: NumberForm) -> Option<String> {
    let (type_tag, inner) = type_val.as_object()?.iter().next()?;
    let raw = inner.as_str().unwrap_or_default();
    if type_tag == "N" {
        let normalized =
            extenddb_core::validation::number::validate_and_normalize_number(raw).ok()?;
        return match form {
            NumberForm::Normalized => Some(normalized),
            NumberForm::Decimal => java_decimal_string(raw),
        };
    }
    Some(raw.to_owned())
}

/// Java's `new BigDecimal(raw).toString()` for a number string that has
/// already passed `validate_and_normalize_number`.
///
/// The unscaled value is every digit written, without its leading zeros, and
/// the scale is the number of fraction digits minus the exponent. `toString`
/// prints plain notation when the scale is not negative and the adjusted
/// exponent (the exponent of the first digit) is at least -6, and scientific
/// notation with an explicit exponent sign otherwise. A zero carries no sign.
fn java_decimal_string(raw: &str) -> Option<String> {
    let (negative, rest) = match raw.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, raw.strip_prefix('+').unwrap_or(raw)),
    };
    let (mantissa, exponent) = match rest.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all_digits = format!("{int_part}{frac_part}");
    let coefficient = match all_digits.trim_start_matches('0') {
        "" => "0",
        c => c,
    };
    let scale = i64::try_from(frac_part.len()).ok()? - exponent;
    let adjusted = i64::try_from(coefficient.len()).ok()? - 1 - scale;

    let mut out = String::new();
    if negative && coefficient != "0" {
        out.push('-');
    }
    if scale >= 0 && adjusted >= -6 {
        let scale = usize::try_from(scale).ok()?;
        if scale == 0 {
            out.push_str(coefficient);
        } else if coefficient.len() > scale {
            let (whole, fraction) = coefficient.split_at(coefficient.len() - scale);
            out.push_str(whole);
            out.push('.');
            out.push_str(fraction);
        } else {
            out.push_str("0.");
            out.extend(std::iter::repeat_n('0', scale - coefficient.len()));
            out.push_str(coefficient);
        }
    } else {
        let (first, tail) = coefficient.split_at(1);
        out.push_str(first);
        if !tail.is_empty() {
            out.push('.');
            out.push_str(tail);
        }
        out.push('E');
        if adjusted >= 0 {
            out.push('+');
        }
        out.push_str(&adjusted.to_string());
    }
    Some(out)
}

/// Extract the partition key value from a `Key`/`Item` map given the partition
/// attribute name.
fn pk_from_map(map: &Value, pk_attr: &str, form: NumberForm) -> Option<String> {
    extract_typed_value(map.as_object()?.get(pk_attr)?, form)
}

/// Resolve the top-level attribute name of a parsed key path, applying
/// `ExpressionAttributeNames` for `#name` references. Returns `None` when the
/// reference is undefined (the engine rejects such a request).
fn resolve_attr_name(path: &[PathElement], names: &HashMap<String, String>) -> Option<String> {
    match path.first()? {
        PathElement::Attribute(name) => match name.strip_prefix('#') {
            Some(reference) => names.get(reference).cloned(),
            None => Some(name.clone()),
        },
        PathElement::Index(_) => None,
    }
}

/// The LeadingKeys for a single-table operation, or `None` when the key is not
/// determinable (the engine then produces the request's error).
pub(crate) fn single_table_leading_keys(
    input: &Value,
    operation: &str,
    key_info: Option<&TableKeyInfo>,
    max_tokens: usize,
) -> Option<Vec<String>> {
    let key_info = key_info?;
    match operation {
        "GetItem" | "DeleteItem" | "UpdateItem" => {
            let pk_attr = hash_attr(&key_info.key_schema)?;
            Some(vec![pk_from_map(
                input.get("Key")?,
                pk_attr,
                NumberForm::Normalized,
            )?])
        }
        "PutItem" => {
            let pk_attr = hash_attr(&key_info.key_schema)?;
            Some(vec![pk_from_map(
                input.get("Item")?,
                pk_attr,
                NumberForm::Normalized,
            )?])
        }
        "Query" => query_leading_keys(input, key_info, max_tokens).map(|v| vec![v]),
        // Scan and every other operation set no LeadingKeys.
        _ => None,
    }
}

/// The partition key value a `Query` touches, resolved against the queried key
/// schema (base table, or the named index). `None` when it cannot be
/// determined, which is every shape the engine rejects before returning data.
fn query_leading_keys(input: &Value, key_info: &TableKeyInfo, max_tokens: usize) -> Option<String> {
    // The partition attribute of whatever the query targets: the named index's
    // partition key, or the base table's. A GSI has its own partition key; an
    // LSI shares the base table's. An unknown index name is a request the engine
    // rejects, so leave the key absent.
    let pk_attr: String = match index_name(input) {
        Some(index) => {
            let info = key_info
                .global_secondary_indexes
                .iter()
                .chain(key_info.local_secondary_indexes.iter())
                .find(|i| i.index_name == index)?;
            hash_attr(&info.key_schema)?.to_owned()
        }
        None => hash_attr(&key_info.key_schema)?.to_owned(),
    };

    let names = names_map(input);
    let values = values_map(input);

    if let Some(kce) = input.get("KeyConditionExpression").and_then(Value::as_str) {
        let tokens = tokenize_with_limit(kce, max_tokens).ok()?;
        let mut kc = parse_key_condition(&tokens).ok()?;
        // Assign PK vs SK using the partition attribute, mirroring the engine.
        kc.resolve_pk_sk(&pk_attr, &names).ok()?;
        // The condition must actually pin the partition key; if it does not, the
        // engine rejects the query, so the key is not determinable here.
        if resolve_attr_name(&kc.pk_path, &names).as_deref() != Some(pk_attr.as_str()) {
            return None;
        }
        return value_of(&kc.pk_value, &values);
    }

    // Legacy KeyConditions: an EQ on the partition attribute.
    if let Some(conds) = input.get("KeyConditions").and_then(Value::as_object) {
        let cond = conds.get(&pk_attr)?.as_object()?;
        if cond.get("ComparisonOperator").and_then(Value::as_str) != Some("EQ") {
            return None;
        }
        let list = cond.get("AttributeValueList")?.as_array()?;
        if list.len() != 1 {
            return None;
        }
        return extract_typed_value(&list[0], NumberForm::Normalized);
    }

    None
}

/// Resolve a key-condition value expression to its scalar string. Only a
/// placeholder resolved through `ExpressionAttributeValues` is a valid
/// partition key value in a key condition.
fn value_of(expr: &Expr, values: &HashMap<String, Value>) -> Option<String> {
    match expr {
        Expr::Placeholder(name) => extract_typed_value(values.get(name)?, NumberForm::Normalized),
        _ => None,
    }
}

fn index_name(input: &Value) -> Option<String> {
    input
        .get("IndexName")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

/// `ExpressionAttributeNames` with the leading `#` stripped from each key,
/// matching how the engine builds its name map.
fn names_map(input: &Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(m) = input
        .get("ExpressionAttributeNames")
        .and_then(Value::as_object)
    {
        for (k, v) in m {
            if let Some(s) = v.as_str() {
                out.insert(k.strip_prefix('#').unwrap_or(k).to_owned(), s.to_owned());
            }
        }
    }
    out
}

/// `ExpressionAttributeValues` with the leading `:` stripped from each key,
/// matching how a placeholder name is stored by the tokenizer.
fn values_map(input: &Value) -> HashMap<String, Value> {
    let mut out = HashMap::new();
    if let Some(m) = input
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object)
    {
        for (k, v) in m {
            out.insert(k.strip_prefix(':').unwrap_or(k).to_owned(), v.clone());
        }
    }
    out
}

/// The per-(action, table) or per-item authorizations for a batch or
/// transaction operation, each carrying the LeadingKeys the service evaluates.
///
/// Returns `None` for non-batch/transact operations. Batch operations
/// authorize once per table with the set of all keys for that table;
/// transaction operations authorize once per item with that item's key, which
/// is how the service evaluates a `ForAnyValue` condition differently for the
/// two (a batch of one own and one other key passes `ForAnyValue`; a
/// transaction of the same two does not).
pub(crate) fn batch_transact_targets(
    operation: &str,
    input: &Value,
    key_info_by_table: &HashMap<String, TableKeyInfo>,
) -> Option<Vec<AuthzTarget>> {
    let pk_attr_of = |table: &str| -> Option<String> {
        key_info_by_table
            .get(table)
            .and_then(|ki| hash_attr(&ki.key_schema).map(ToOwned::to_owned))
    };

    match operation {
        "BatchGetItem" | "BatchWriteItem" => {
            let items = input.get("RequestItems")?.as_object()?;
            let mut out = Vec::new();
            for (table, body) in items {
                let keys = batch_table_keys(operation, body, pk_attr_of(table).as_deref());
                out.push(AuthzTarget {
                    action: operation.to_owned(),
                    table: table.clone(),
                    leading_keys: keys,
                });
            }
            Some(out)
        }
        "TransactGetItems" => {
            let items = input.get("TransactItems")?.as_array()?;
            let mut out = Vec::new();
            for entry in items {
                if let Some(get) = entry.get("Get")
                    && let Some(table) = get.get("TableName").and_then(Value::as_str)
                {
                    let lk = pk_attr_of(table)
                        .and_then(|pk| {
                            get.get("Key")
                                .and_then(|k| pk_from_map(k, &pk, NumberForm::Decimal))
                        })
                        .map(|v| vec![v]);
                    out.push(AuthzTarget {
                        action: "GetItem".to_owned(),
                        table: table.to_owned(),
                        leading_keys: lk,
                    });
                }
            }
            Some(out)
        }
        "TransactWriteItems" => {
            let items = input.get("TransactItems")?.as_array()?;
            let mut out = Vec::new();
            for entry in items {
                for (sub, action, key_field) in [
                    ("Put", "PutItem", "Item"),
                    ("Delete", "DeleteItem", "Key"),
                    ("Update", "UpdateItem", "Key"),
                    ("ConditionCheck", "ConditionCheckItem", "Key"),
                ] {
                    if let Some(op) = entry.get(sub)
                        && let Some(table) = op.get("TableName").and_then(Value::as_str)
                    {
                        let lk = pk_attr_of(table)
                            .and_then(|pk| {
                                op.get(key_field)
                                    .and_then(|m| pk_from_map(m, &pk, NumberForm::Decimal))
                            })
                            .map(|v| vec![v]);
                        out.push(AuthzTarget {
                            action: action.to_owned(),
                            table: table.to_owned(),
                            leading_keys: lk,
                        });
                    }
                }
            }
            Some(out)
        }
        _ => None,
    }
}

/// The distinct table names a batch or transaction operation addresses, or
/// `None` for any other operation (and for a batch or transaction request whose
/// items container is absent). The `Some`/`None` result mirrors
/// `batch_transact_targets`, so the caller gates the batch and transaction
/// branch on this and then builds the per-target authorizations once. Walking
/// the body for table names here avoids building a throwaway set of full
/// targets only to discover which tables to fetch key info for.
pub(crate) fn batch_transact_tables(
    operation: &str,
    input: &Value,
) -> Option<std::collections::BTreeSet<String>> {
    let mut tables = std::collections::BTreeSet::new();
    match operation {
        "BatchGetItem" | "BatchWriteItem" => {
            for table in input.get("RequestItems")?.as_object()?.keys() {
                tables.insert(table.clone());
            }
        }
        "TransactGetItems" => {
            for entry in input.get("TransactItems")?.as_array()? {
                if let Some(table) = entry
                    .get("Get")
                    .and_then(|g| g.get("TableName"))
                    .and_then(Value::as_str)
                {
                    tables.insert(table.to_owned());
                }
            }
        }
        "TransactWriteItems" => {
            for entry in input.get("TransactItems")?.as_array()? {
                for sub in ["Put", "Delete", "Update", "ConditionCheck"] {
                    if let Some(table) = entry
                        .get(sub)
                        .and_then(|o| o.get("TableName"))
                        .and_then(Value::as_str)
                    {
                        tables.insert(table.to_owned());
                    }
                }
            }
        }
        _ => return None,
    }
    Some(tables)
}

/// Collect the partition key values a single table receives in a batch request.
/// Returns `None` when the table's key schema is unknown (the engine then
/// rejects), or when no key yields a value.
fn batch_table_keys(operation: &str, body: &Value, pk_attr: Option<&str>) -> Option<Vec<String>> {
    let pk_attr = pk_attr?;
    let mut out = Vec::new();
    if operation == "BatchGetItem" {
        for key in body.get("Keys")?.as_array()? {
            if let Some(v) = pk_from_map(key, pk_attr, NumberForm::Normalized) {
                out.push(v);
            }
        }
    } else {
        // BatchWriteItem: a list of {PutRequest:{Item}} / {DeleteRequest:{Key}}.
        for req in body.as_array()? {
            let map = req
                .get("PutRequest")
                .and_then(|p| p.get("Item"))
                .or_else(|| req.get("DeleteRequest").and_then(|d| d.get("Key")));
            if let Some(m) = map
                && let Some(v) = pk_from_map(m, pk_attr, NumberForm::Decimal)
            {
                out.push(v);
            }
        }
    }
    if out.is_empty() { None } else { Some(out) }
}

#[cfg(test)]
mod tests;
