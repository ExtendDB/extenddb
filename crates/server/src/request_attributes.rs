// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `dynamodb:Attributes` extraction for request authorization.
//!
//! IAM evaluates `dynamodb:Attributes` against the top-level attribute names a
//! request accesses. A top-level attribute is accessed when the request names
//! it, or a nested attribute inside it, in a request parameter: a projection of
//! `Name, Address.City` gives `Name` and `Address`. The names come from the
//! request alone. A `DeleteItem` and a read with no projection hold only the
//! names the request gives, not the names of the stored item they delete or
//! return (measured 2026-10-31), and `ReturnValues` and `Select` add no names.
//!
//! What the key holds for each operation:
//!
//! - `GetItem`: the table's key attributes, `AttributesToGet`, and the paths in
//!   `ProjectionExpression`.
//! - `PutItem`: the table's key attributes, the item's attributes, `Expected`,
//!   and the paths in `ConditionExpression`.
//! - `UpdateItem`: the `Key` attributes, the table's key attributes,
//!   `AttributeUpdates`, `Expected`, the paths in `ConditionExpression`, and
//!   the attribute each `SET`, `REMOVE`, `ADD` and `DELETE` action in
//!   `UpdateExpression` names. The paths a `SET` value reads are not in the
//!   key: the service authorizes `SET #n = ssn`, which copies `ssn` into
//!   `name`, under a Deny on `ssn` (measured 2026-11-01).
//! - `DeleteItem`: the `Key` attributes, the table's key attributes, `Expected`,
//!   and the paths in `ConditionExpression`.
//! - `Query`: the key attributes of the table and of the queried index,
//!   `AttributesToGet`, `QueryFilter`, `ExclusiveStartKey`, and the paths in
//!   `FilterExpression` and `ProjectionExpression`. A key condition can name
//!   only key attributes, which the key already holds.
//! - `Scan`: `AttributesToGet`, `ScanFilter`, `ExclusiveStartKey`, and the paths
//!   in `FilterExpression` and `ProjectionExpression`. A `Scan` names no key, so
//!   a `Scan` that gives none of these leaves the key absent.
//! - `SearchVectors`: the paths in `ProjectionExpression`.
//! - `BatchGetItem`: per table, the attributes of every key, the table's key
//!   attributes, `AttributesToGet`, and the paths in `ProjectionExpression`.
//! - `BatchWriteItem`: per table, the attributes of every `PutRequest` item and
//!   every `DeleteRequest` key, and the table's key attributes.
//! - `TransactGetItems` and `TransactWriteItems`: per item, as for the
//!   single-item operation the item maps to. A `ConditionCheck` holds the
//!   table's key attributes and the paths in its `ConditionExpression`.
//!
//! The service's answers were measured on 2026-10-31 and 2026-11-01 for the
//! item attributes of `PutItem`, every kind of `UpdateExpression` action and
//! `SET` value, `DeleteItem`, `ConditionExpression` on `PutItem` and
//! `DeleteItem`, `FilterExpression` on `Query` and `Scan`, `Expected`,
//! `AttributeUpdates`, `ScanFilter`, `AttributesToGet` on `GetItem` and `Scan`,
//! the table's key attributes on `GetItem`, a `ProjectionExpression` naming a
//! document path, reads with no projection, a `ConditionCheck`, and the batch
//! and transaction forms of `GetItem` and `PutItem`. The rest follows the same
//! rules: `ExclusiveStartKey`, `QueryFilter`, `AttributesToGet` on `Query` and
//! `BatchGetItem`, the queried index's key attributes, `DeleteRequest` keys,
//! and the `Update` and `Delete` items of a transaction.
//!
//! An expression is read with the tokenizer and parser the engine uses, under
//! the same size, token and depth limits, so every expression the engine
//! accepts gives its names here. An expression the engine refuses gives none:
//! the engine then answers `ValidationException` without reading or writing an
//! item. A `#name` with no `ExpressionAttributeNames` entry is skipped for the
//! same reason.

use std::collections::BTreeSet;

use extenddb_core::expression::{
    Expr, ExpressionKind, PathElement, UpdateAction, parse_condition_with_depth_limit,
    parse_projection, parse_update_from, tokenize_for, tokenize_with_limit,
};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::TableKeyInfo;
use serde_json::{Map, Value};

/// The `dynamodb:Attributes` value of a single-table operation: the sorted,
/// distinct top-level names the request accesses. `None` when it names none,
/// and for an operation that reads or writes no item.
pub(crate) fn single_table_attributes(
    operation: &str,
    input: &Value,
    key_info: Option<&TableKeyInfo>,
    limits: &LimitsConfig,
) -> Option<Vec<String>> {
    let mut names = Names::new(input, limits);
    match operation {
        "GetItem" => names.get_item(input, key_info),
        "PutItem" => names.put_item(input, key_info),
        "UpdateItem" => names.update_item(input, key_info),
        "DeleteItem" => names.delete_item(input, key_info),
        "Query" => {
            names.key_schema(key_info);
            names.index_key_schema(input, key_info);
            names.map_keys(input.get("QueryFilter"));
            names.range_read(input);
        }
        "Scan" => {
            names.map_keys(input.get("ScanFilter"));
            names.range_read(input);
        }
        "SearchVectors" => names.projection(input.get("ProjectionExpression")),
        _ => return None,
    }
    names.finish()
}

/// The `dynamodb:Attributes` value of one table's part of a `BatchGetItem` or
/// `BatchWriteItem`: `body` is that table's `RequestItems` entry. `None` for
/// any other operation, and when the part names no attribute.
pub(crate) fn batch_table_attributes(
    operation: &str,
    body: &Value,
    key_info: Option<&TableKeyInfo>,
    limits: &LimitsConfig,
) -> Option<Vec<String>> {
    let mut names = Names::new(body, limits);
    match operation {
        "BatchGetItem" => {
            for key in body
                .get("Keys")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                names.map_keys(Some(key));
            }
            names.get_item(body, key_info);
        }
        "BatchWriteItem" => {
            for request in body.as_array().into_iter().flatten() {
                names.map_keys(request.get("PutRequest").and_then(|p| p.get("Item")));
                names.map_keys(request.get("DeleteRequest").and_then(|d| d.get("Key")));
            }
            names.key_schema(key_info);
        }
        _ => return None,
    }
    names.finish()
}

/// The `dynamodb:Attributes` value of one transaction item: `sub` is the
/// item's member name (`Get`, `Put`, `Update`, `Delete` or `ConditionCheck`)
/// and `item` its body. `None` for any other member, and when the item names
/// no attribute.
pub(crate) fn transact_item_attributes(
    sub: &str,
    item: &Value,
    key_info: Option<&TableKeyInfo>,
    limits: &LimitsConfig,
) -> Option<Vec<String>> {
    let mut names = Names::new(item, limits);
    match sub {
        "Get" => {
            names.key_schema(key_info);
            names.projection(item.get("ProjectionExpression"));
        }
        "Put" => names.put_item(item, key_info),
        "Update" => names.update_item(item, key_info),
        "Delete" => names.delete_item(item, key_info),
        "ConditionCheck" => {
            names.key_schema(key_info);
            names.condition(item.get("ConditionExpression"));
        }
        _ => return None,
    }
    names.finish()
}

/// The top-level names one request (or one batch part or transaction item)
/// accesses, with the `ExpressionAttributeNames` its expressions resolve
/// against.
struct Names<'a> {
    found: BTreeSet<String>,
    placeholders: Option<&'a Map<String, Value>>,
    limits: &'a LimitsConfig,
}

impl<'a> Names<'a> {
    fn new(request: &'a Value, limits: &'a LimitsConfig) -> Self {
        Self {
            found: BTreeSet::new(),
            placeholders: request
                .get("ExpressionAttributeNames")
                .and_then(Value::as_object),
            limits,
        }
    }

    fn get_item(&mut self, request: &Value, key_info: Option<&TableKeyInfo>) {
        self.key_schema(key_info);
        self.list(request.get("AttributesToGet"));
        self.projection(request.get("ProjectionExpression"));
    }

    fn put_item(&mut self, request: &Value, key_info: Option<&TableKeyInfo>) {
        self.key_schema(key_info);
        self.map_keys(request.get("Item"));
        self.map_keys(request.get("Expected"));
        self.condition(request.get("ConditionExpression"));
    }

    fn update_item(&mut self, request: &Value, key_info: Option<&TableKeyInfo>) {
        self.map_keys(request.get("Key"));
        self.key_schema(key_info);
        self.map_keys(request.get("AttributeUpdates"));
        self.map_keys(request.get("Expected"));
        self.condition(request.get("ConditionExpression"));
        self.update(request.get("UpdateExpression"));
    }

    fn delete_item(&mut self, request: &Value, key_info: Option<&TableKeyInfo>) {
        self.map_keys(request.get("Key"));
        self.key_schema(key_info);
        self.map_keys(request.get("Expected"));
        self.condition(request.get("ConditionExpression"));
    }

    /// The parameters `Query` and `Scan` share.
    fn range_read(&mut self, request: &Value) {
        self.list(request.get("AttributesToGet"));
        self.map_keys(request.get("ExclusiveStartKey"));
        self.condition(request.get("FilterExpression"));
        self.projection(request.get("ProjectionExpression"));
    }

    /// The table's key attributes. A table that does not exist adds none.
    fn key_schema(&mut self, key_info: Option<&TableKeyInfo>) {
        for element in key_info.into_iter().flat_map(|ki| &ki.key_schema) {
            self.found.insert(element.attribute_name.clone());
        }
    }

    /// The key attributes of the index a `Query` names. An index the table
    /// does not have adds none: the engine refuses that request.
    fn index_key_schema(&mut self, request: &Value, key_info: Option<&TableKeyInfo>) {
        let (Some(key_info), Some(index)) =
            (key_info, request.get("IndexName").and_then(Value::as_str))
        else {
            return;
        };
        let found = key_info
            .global_secondary_indexes
            .iter()
            .chain(&key_info.local_secondary_indexes)
            .find(|i| i.index_name == index);
        for element in found.into_iter().flat_map(|i| &i.key_schema) {
            self.found.insert(element.attribute_name.clone());
        }
    }

    /// The keys of a map of top-level attribute names (`Item`, `Key`,
    /// `Expected`, `AttributeUpdates`, `QueryFilter`, `ScanFilter`,
    /// `ExclusiveStartKey`), as written.
    fn map_keys(&mut self, value: Option<&Value>) {
        if let Some(map) = value.and_then(Value::as_object) {
            self.found.extend(map.keys().cloned());
        }
    }

    /// The entries of `AttributesToGet`, as written: each is a top-level name,
    /// so a `.` in one is part of the name, not a path separator.
    fn list(&mut self, value: Option<&Value>) {
        let entries = value.and_then(Value::as_array).into_iter().flatten();
        self.found
            .extend(entries.filter_map(Value::as_str).map(ToOwned::to_owned));
    }

    /// The expression text in `value`, or `None` when there is none or it is
    /// over the size limit the engine refuses it under.
    fn text<'v>(&self, value: Option<&'v Value>) -> Option<&'v str> {
        value
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= self.limits.max_expression_length_bytes)
    }

    /// The paths in a `ConditionExpression` or `FilterExpression`.
    fn condition(&mut self, value: Option<&Value>) {
        let Some(text) = self.text(value) else {
            return;
        };
        let parsed =
            tokenize_with_limit(text, self.limits.max_expression_tokens).and_then(|tokens| {
                parse_condition_with_depth_limit(&tokens, self.limits.max_expression_depth)
            });
        if let Ok(expr) = parsed {
            self.expr(&expr);
        }
    }

    /// The attribute each `UpdateExpression` action names. The paths a value
    /// reads are left out, as the service leaves them out.
    fn update(&mut self, value: Option<&Value>) {
        let Some(text) = self.text(value) else {
            return;
        };
        let parsed = tokenize_for(
            text,
            self.limits.max_expression_tokens,
            ExpressionKind::Update,
        )
        .and_then(|tokens| parse_update_from(&tokens, text));
        for action in parsed.iter().flatten() {
            match action {
                UpdateAction::Set { path, .. }
                | UpdateAction::Remove { path }
                | UpdateAction::Add { path, .. }
                | UpdateAction::Delete { path, .. } => self.path(path),
            }
        }
    }

    /// The paths in a `ProjectionExpression`.
    fn projection(&mut self, value: Option<&Value>) {
        let Some(text) = self.text(value) else {
            return;
        };
        let parsed = tokenize_for(
            text,
            self.limits.max_expression_tokens,
            ExpressionKind::Projection,
        )
        .and_then(|tokens| parse_projection(&tokens));
        for path in parsed.iter().flatten() {
            self.path(path);
        }
    }

    fn expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Path(path) => self.path(path),
            Expr::Placeholder(_) => {}
            Expr::Compare { left, right, .. }
            | Expr::Arithmetic { left, right, .. }
            | Expr::And(left, right)
            | Expr::Or(left, right) => {
                self.expr(left);
                self.expr(right);
            }
            Expr::Not(inner) => self.expr(inner),
            Expr::Function { args, .. } => {
                for arg in args {
                    self.expr(arg);
                }
            }
            Expr::Between { operand, low, high } => {
                self.expr(operand);
                self.expr(low);
                self.expr(high);
            }
            Expr::In { operand, list } => {
                self.expr(operand);
                for item in list {
                    self.expr(item);
                }
            }
        }
    }

    /// The top-level name of a document path: its first element, with a
    /// `#name` resolved through `ExpressionAttributeNames`.
    fn path(&mut self, path: &[PathElement]) {
        let Some(PathElement::Attribute(first)) = path.first() else {
            return;
        };
        let name = if first.starts_with('#') {
            match self
                .placeholders
                .and_then(|names| names.get(first))
                .and_then(Value::as_str)
            {
                Some(name) => name,
                None => return,
            }
        } else {
            first.as_str()
        };
        self.found.insert(name.to_owned());
    }

    fn finish(self) -> Option<Vec<String>> {
        if self.found.is_empty() {
            None
        } else {
            Some(self.found.into_iter().collect())
        }
    }
}

#[cfg(test)]
mod tests;
