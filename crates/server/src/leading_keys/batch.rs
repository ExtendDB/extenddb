// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The per-table and per-item authorizations of a batch or transaction
//! operation.

use std::collections::HashMap;

use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::TableKeyInfo;
use serde_json::Value;

use super::{NumberForm, hash_attr, pk_from_map};
use crate::parameter_keys::{self, ParameterKeys};
use crate::request_attributes::{batch_table_attributes, transact_item_attributes};

/// A single `(action, table)` authorization to perform for a batch or
/// transaction operation, carrying the LeadingKeys, Attributes, Select,
/// ReturnValues and ReturnConsumedCapacity values the service would evaluate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AuthzTarget {
    /// The bare operation string the action maps to (no `dynamodb:` prefix).
    pub action: String,
    /// The table the action is authorized against.
    pub table: String,
    /// The partition key value(s) for this authorization, or `None` when the
    /// key cannot be determined (the engine then rejects the request).
    pub leading_keys: Option<Vec<String>>,
    /// The `dynamodb:Attributes` names for this authorization, or `None` when
    /// it names no attribute. See `crate::request_attributes`.
    pub attributes: Option<Vec<String>>,
    /// The `dynamodb:Select`, `dynamodb:ReturnValues` and
    /// `dynamodb:ReturnConsumedCapacity` values. See `crate::parameter_keys`.
    pub parameters: ParameterKeys,
}

/// The per-(action, table) or per-item authorizations for a batch or
/// transaction operation, each carrying the condition key values the service
/// evaluates.
///
/// Returns `None` for non-batch/transact operations. Batch operations
/// authorize once per table with the set of all keys for that table;
/// transaction operations authorize once per item with that item's key, which
/// is how the service evaluates a `ForAnyValue` condition differently for the
/// two (a batch of one own and one other key passes `ForAnyValue`; a
/// transaction of the same two does not). Transaction items that give the same
/// target are returned once, in the order of their first occurrence: the
/// decision for a target depends only on its action, table and condition key
/// values, so a hundred identical `ConditionCheck`s on one partition need one
/// evaluation. Two `Get`s of one partition that differ only in whether they
/// project give two targets, since their `dynamodb:Select` values differ.
///
/// `key_info_by_table` is the authorization snapshot; a `None` entry is a table
/// that does not exist, whose targets carry no LeadingKeys and none of the
/// table's key attributes in Attributes (the engine returns
/// `ResourceNotFoundException`). `limits` are the engine's expression limits,
/// under which the expressions are read for Attributes.
pub(crate) fn batch_transact_targets(
    operation: &str,
    input: &Value,
    key_info_by_table: &HashMap<String, Option<TableKeyInfo>>,
    limits: &LimitsConfig,
) -> Option<Vec<AuthzTarget>> {
    let key_info_of =
        |table: &str| -> Option<&TableKeyInfo> { key_info_by_table.get(table)?.as_ref() };
    let pk_attr_of = |table: &str| -> Option<String> {
        key_info_of(table).and_then(|ki| hash_attr(&ki.key_schema).map(ToOwned::to_owned))
    };
    let mut seen = std::collections::HashSet::new();
    let mut push_unique = |out: &mut Vec<AuthzTarget>, target: AuthzTarget| {
        if seen.insert(target.clone()) {
            out.push(target);
        }
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
                    attributes: batch_table_attributes(operation, body, key_info_of(table), limits),
                    parameters: parameter_keys::batch_table(operation, body, input),
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
                    push_unique(
                        &mut out,
                        AuthzTarget {
                            action: "GetItem".to_owned(),
                            table: table.to_owned(),
                            leading_keys: lk,
                            attributes: transact_item_attributes(
                                "Get",
                                get,
                                key_info_of(table),
                                limits,
                            ),
                            parameters: parameter_keys::transact_item("Get", get, input),
                        },
                    );
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
                        push_unique(
                            &mut out,
                            AuthzTarget {
                                action: action.to_owned(),
                                table: table.to_owned(),
                                leading_keys: lk,
                                attributes: transact_item_attributes(
                                    sub,
                                    op,
                                    key_info_of(table),
                                    limits,
                                ),
                                parameters: parameter_keys::transact_item(sub, op, input),
                            },
                        );
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
