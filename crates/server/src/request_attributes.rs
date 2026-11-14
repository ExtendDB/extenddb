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
//! return (measured 2026-10-31). `ReturnValues` and
//! `ReturnValuesOnConditionCheckFailure` add no names (measured 2026-11-01),
//! and `Select` adds none (not measured).
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
//! - `Query`: the key attributes of the table and of the queried index, the
//!   paths in `KeyConditionExpression`, `KeyConditions`, `AttributesToGet`,
//!   `QueryFilter`, `ExclusiveStartKey`, and the paths in `FilterExpression`
//!   and `ProjectionExpression`. On a table that exists, the engine refuses a
//!   non-key attribute in `KeyConditions`, and in a `KeyConditionExpression`
//!   on a key schema with several HASH or several RANGE attributes. On a key
//!   schema with one HASH attribute and at most one RANGE attribute it checks
//!   a `KeyConditionExpression` only for an equality on the partition key
//!   (the note on the single-attribute path in `handle_query`,
//!   `crates/engine/src/query.rs`), so it accepts a clause on another
//!   attribute, and the key also holds that name. On a table that does not
//!   exist the key holds the names in the key condition, a non-key name
//!   included: under a Deny on one of them the service answers
//!   `AccessDeniedException`, not `ResourceNotFoundException` (measured
//!   2026-11-09 for `KeyConditionExpression` and `KeyConditions`). The
//!   queried index's full key schema is in the key, its range key included,
//!   when the key condition names only the index's partition key (measured
//!   2026-11-09 on a global secondary index; a local secondary index follows
//!   the same rule, not measured).
//! - `Scan`: the table's key attributes, `AttributesToGet`, `ScanFilter`,
//!   `ExclusiveStartKey`, and the paths in `FilterExpression` and
//!   `ProjectionExpression`. A `Scan` that names no attribute, a `Select
//!   COUNT` `Scan` included, holds the table's key attributes (measured
//!   2026-11-01). A `Scan` with `IndexName` does not add the index's key
//!   schema. The index's key attributes are in the key only when the request
//!   names them, as the `ExclusiveStartKey` of a later page does (see below).
//!   Measured 2026-11-09 on a `Scan` with no `ExclusiveStartKey`: the service
//!   allows a `Scan` of a global secondary index under an Allow whose
//!   `ForAllValues` list holds the table's keys and not the index's, and
//!   under a Deny on the index's partition key. It allows a `Scan` of a local
//!   secondary index under an Allow whose list holds the table's keys and the
//!   projected name, and (measured 2026-11-10) under a Deny on the index's
//!   sort key, so the index's sort key is not in the key. The other rules for
//!   a local secondary index follow from that key content.
//! - `SearchVectors`: the paths in `ProjectionExpression`. A `SearchVectors`
//!   with no `ProjectionExpression` leaves the key absent.
//! - `BatchGetItem`: per table, the table's key attributes, `AttributesToGet`,
//!   and the paths in `ProjectionExpression`, as for `GetItem`. The names in
//!   `Keys` are not read: on a table that does not exist, under a Deny on the
//!   key attribute, the service answers `ResourceNotFoundException` (measured
//!   2026-11-09). A `RequestItems` key that is a table ARN gets no key info
//!   from `authorize_request`, so that part holds no key attributes, where a
//!   `GetItem` whose `TableName` is the same ARN holds them.
//! - `BatchWriteItem`: per table, the attributes of every `PutRequest` item and
//!   every `DeleteRequest` key, and the table's key attributes. Each table's
//!   part is evaluated once, with the union of its items' names, as on the
//!   service (measured 2026-11-01).
//! - `TransactGetItems` and `TransactWriteItems`: per item, as for the
//!   single-item operation the item maps to. A `ConditionCheck` holds the
//!   table's key attributes and the paths in its `ConditionExpression`.
//!
//! The service's answers were measured on 2026-10-31 and 2026-11-01 for the
//! item attributes of `PutItem`; every kind of `UpdateExpression` action and
//! `SET` value; `DeleteItem`; `ConditionExpression` on `PutItem`, `DeleteItem`,
//! a `ConditionCheck` and a transaction `Delete`; `FilterExpression` on `Query`
//! and `Scan`; `Expected` on `PutItem`; `AttributeUpdates`; `ScanFilter` and
//! `QueryFilter`; `AttributesToGet` on `GetItem`, `Query`, `Scan` and
//! `BatchGetItem`; the table's key attributes on `GetItem`, `Query`, `Scan`,
//! `UpdateItem`, `DeleteItem` and a `DeleteRequest`; a `ProjectionExpression`
//! naming a document path; reads with no projection; a transaction `Update`;
//! and the batch and transaction forms of `GetItem` and `PutItem`. On
//! 2026-11-09, on a table keyed on `pk` and `sk` with a global and a local
//! secondary index: the table's sort key; the key attributes of a queried
//! global secondary index; the key attributes of a scanned global secondary
//! index and the sort key of a scanned local secondary index, on a `Scan`
//! with no `ExclusiveStartKey`; and the key condition of a `Query` and the
//! `Keys` of a `BatchGetItem` on a table that does not exist. On 2026-11-10,
//! on a table of the same shape holding two items: a later page of a `Scan`
//! of each index and of the table, and the first page of a `Scan` of the
//! local secondary index under a Deny on its sort key. The rest follows the
//! rules measured for the same parameter on other operations:
//! `ProjectionExpression` on `Query` and `Scan`, `Expected` and
//! `ConditionExpression` on `UpdateItem`, and `Expected` on `DeleteItem`.
//!
//! `ExclusiveStartKey` adds its names as written, as on the service. The
//! start key of a `Scan` with `IndexName` holds the table's and the index's
//! key attributes, the shape the engine requires and returns as
//! `LastEvaluatedKey`, and the key holds them. So a later page of a `Scan` of
//! an index holds the index's key attributes, and the first page holds them
//! only when the request names them. Measured 2026-11-10, with the previous
//! page's `LastEvaluatedKey` as the start key: the service refuses a later
//! page of a `Scan` of a global or a local secondary index under an Allow
//! whose `ForAllValues` list holds the table's keys, alone or with the
//! projected name, and allows a later page of a `Scan` of the table under
//! the Allow of the table's keys alone. Under a Deny on the global secondary
//! index's partition key, or on the local secondary index's sort key, it
//! refuses a later page of a `Scan` of that index and allows the first page.
//! On a `Query`, and on a `Scan` of the table, a valid start key holds only
//! key attributes that the key already holds, so the service cannot show
//! whether it adds names of its own there.
//!
//! An expression is read with the tokenizer and parser the engine uses, under
//! the same size, token and depth limits, so on every operation other than
//! `SearchVectors` an expression the engine accepts gives all its names here.
//! `SearchVectors` matches a `#name` against `ExpressionAttributeNames` keys
//! with their leading `#` removed, so it accepts keys this module does not
//! resolve, and its `SearchConditionExpression` is not read here (both #729).
//! An expression the parser refuses gives none, and the engine refuses the
//! same request without reading or writing an item. A key condition also
//! goes through the engine's reserved-word check when
//! `enforce_reserved_keywords` is set, so a key condition with a reserved
//! word written bare gives none. The other expressions skip that check: a
//! reserved word written bare in one of them adds its name, and the engine
//! refuses the request after authorization. A `#name` with no
//! `ExpressionAttributeNames` entry is skipped, as the engine refuses it.

use std::collections::BTreeSet;

use extenddb_core::expression::{
    Expr, ExpressionKind, PathElement, UpdateAction, parse_condition_with_depth_limit,
    parse_key_condition, parse_projection, parse_update_from, tokenize_for, tokenize_with_limit,
    validate_no_reserved_words,
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
            names.key_condition(input.get("KeyConditionExpression"));
            names.map_keys(input.get("KeyConditions"));
            names.map_keys(input.get("QueryFilter"));
            names.range_read(input);
        }
        "Scan" => {
            names.key_schema(key_info);
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
        "BatchGetItem" => names.get_item(body, key_info),
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
    /// does not have adds none: the engine refuses that request. A `Scan` of
    /// an index does not call this (measured 2026-11-09 on a `Scan` with no
    /// `ExclusiveStartKey`). The start key of a later page adds the index's
    /// key attributes in `range_read`, as on the service (measured
    /// 2026-11-10).
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

    /// The paths in a `KeyConditionExpression`, read as the engine's key
    /// condition parser reads it (`parse_key_condition_expr` in
    /// `crates/engine/src/expression_helpers.rs`): under the size and token
    /// limits, with the reserved-word check when `enforce_reserved_keywords`
    /// is set, then `parse_key_condition`. A key condition the key condition
    /// parser refuses gives none.
    fn key_condition(&mut self, value: Option<&Value>) {
        let Some(text) = self.text(value) else {
            return;
        };
        let limits = self.limits;
        let parsed = tokenize_for(
            text,
            limits.max_expression_tokens,
            ExpressionKind::KeyCondition,
        )
        .and_then(|tokens| {
            if limits.enforce_reserved_keywords {
                validate_no_reserved_words(&tokens)?;
            }
            parse_key_condition(&tokens)
        });
        let Ok(condition) = parsed else {
            return;
        };
        self.path(&condition.pk_path);
        for (path, _) in &condition.extra_pk_conditions {
            self.path(path);
        }
        if let Some(sort_key) = &condition.sk_condition {
            self.path(sort_key.path());
        }
        for (path, _) in &condition.extra_sk_conditions {
            self.path(path);
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
