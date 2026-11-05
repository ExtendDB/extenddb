// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the `BatchExecuteStatement` and `ExecuteTransaction`
//! executors: deriving an item's primary key from a parsed statement, and
//! mapping a single-statement error into the short per-statement `Error` shape
//! a batch carries. Every behaviour is justified by a measurement against the
//! live service, cited where it is implemented.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{
    CompareOp, InsertStatement, Operand, SelectStatement, WhereExpr,
};
use extenddb_core::partiql::plan::{
    KeyEquality, KeyNames, SortCondition, WhereAnalysis, key_equality, same_value,
};
use extenddb_core::types::{
    AttributeValue, CancellationReason, KeySchemaElement, KeyType, ScalarAttributeType,
    TableKeyInfo, is_multipart_key_schema,
};

use super::write::{resolve_av, to_json_value};

/// The service's refusal for any PartiQL statement whose target (table or
/// index) has more than one HASH or more than one RANGE key attribute.
pub(super) const MULTI_ATTRIBUTE_KEY_MESSAGE: &str = "PartiQL statements cannot be executed on \
     tables with multi-attribute HASH keys or multi-attribute RANGE keys.";

/// Refuse a target whose key schema has more than one HASH or RANGE element.
///
/// Measured on a GSI with a `HASH, RANGE, RANGE` key: every `ExecuteStatement`
/// form (no WHERE, partial key, full key, filter on a non-key, `ConsistentRead`)
/// returns this `ValidationException`, and a batch carries it as a per-statement
/// `ValidationError`. A multi-part base table exists only under this engine's
/// preview extension; the same refusal applies there.
///
/// # Errors
///
/// Returns `ValidationException` with the service's message.
pub(super) fn ensure_single_attribute_keys(
    schema: &[KeySchemaElement],
) -> Result<(), DynamoDbError> {
    if is_multipart_key_schema(schema) {
        return Err(DynamoDbError::ValidationException(
            MULTI_ATTRIBUTE_KEY_MESSAGE.to_owned(),
        ));
    }
    Ok(())
}

/// The refusal of a key value whose type is not the one the table defines for
/// that key attribute, naming the attribute (measured 2026-11-02 for an INSERT
/// key, for SELECT, UPDATE and DELETE key conditions, and for a
/// `BatchExecuteStatement` SELECT, a number against a string key).
pub(super) fn key_type_error(name: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "Key attribute's data type should match its data type in table's schema: Key {name}"
    ))
}

/// The reason an `ExecuteTransaction` member reports for a key that does not
/// match the table's key schema: a write member's partial key, or a key value
/// of the wrong type in a read or a write member (measured 2026-11-02).
pub(super) const KEY_SCHEMA_MISMATCH: &str = "The provided key element does not match the schema";

/// The reason an `INSERT` member of `ExecuteTransaction` reports when its key
/// is already in the table.
pub(super) fn duplicate_item_reason() -> CancellationReason {
    CancellationReason {
        code: "DuplicateItem".to_owned(),
        message: Some("Duplicate primary key exists in table".to_owned()),
        item: None,
    }
}

/// Refuse a key (in wire form) whose value for a key attribute has a type the
/// table does not define for it, the sort key before the partition key (see
/// [`key_type_error`]). For `pk = 1 AND sk = 1` the service names `sk` on
/// SELECT, UPDATE and DELETE and in `BatchExecuteStatement` (measured
/// 2026-11-02); for some other pairs of key names it names the partition
/// key, and no rule was found.
///
/// # Errors
///
/// Returns [`key_type_error`] for the attribute.
pub(super) fn check_key_types(
    key: &Map<String, Value>,
    key_info: &TableKeyInfo,
) -> Result<(), DynamoDbError> {
    let sort_first = key_info
        .key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Range)
        .chain(
            key_info
                .key_schema
                .iter()
                .filter(|k| k.key_type == KeyType::Hash),
        );
    for element in sort_first {
        let name = element.attribute_name.as_str();
        if let Some(value) = key.get(name)
            && !wire_key_value_matches(key_info, name, value)
        {
            return Err(key_type_error(name));
        }
    }
    Ok(())
}

/// Whether a wire-form key value has the type the table defines for key
/// attribute `name` (see [`check_key_value_type`]). A value that is no
/// attribute value does not.
pub(super) fn wire_key_value_matches(key_info: &TableKeyInfo, name: &str, value: &Value) -> bool {
    AttributeValue::deserialize(value)
        .is_ok_and(|value| check_key_value_type(key_info, name, &value).is_ok())
}

/// The wire type tag (`S`, `N` or `B`) the table defines for attribute `name`,
/// or `None` when it defines none.
fn defined_key_type(key_info: &TableKeyInfo, name: &str) -> Option<&'static str> {
    key_info
        .attribute_definitions
        .iter()
        .find(|d| d.attribute_name == name)
        .map(|d| match d.attribute_type {
            ScalarAttributeType::S => "S",
            ScalarAttributeType::N => "N",
            ScalarAttributeType::B => "B",
        })
}

/// Refuse a key value whose type is not the one the table defines for key
/// attribute `name`; an attribute with no definition takes any value.
///
/// # Errors
///
/// Returns [`key_type_error`] for the attribute.
pub(super) fn check_key_value_type(
    key_info: &TableKeyInfo,
    name: &str,
    value: &AttributeValue,
) -> Result<(), DynamoDbError> {
    let tag = match value {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        _ => "",
    };
    match defined_key_type(key_info, name) {
        Some(defined) if defined != tag => Err(key_type_error(name)),
        _ => Ok(()),
    }
}

/// The partition and optional sort attribute names of a key schema.
pub(super) fn key_names(schema: &[KeySchemaElement]) -> (Option<&str>, Option<&str>) {
    let mut partition = None;
    let mut sort = None;
    for element in schema {
        match element.key_type {
            KeyType::Hash => partition = Some(element.attribute_name.as_str()),
            KeyType::Range => sort = Some(element.attribute_name.as_str()),
        }
    }
    (partition, sort)
}

/// The target table name of any parsed statement.
pub(super) fn statement_table(stmt: &Statement) -> &str {
    match stmt {
        Statement::Select(s) | Statement::Exists(s) => &s.from.table,
        Statement::Insert(i) => &i.table,
        Statement::Update(u) => &u.table,
        Statement::Delete(d) => &d.table,
    }
}

/// The wire-form primary key of a `SELECT` that is a point read, or `None`
/// when the WHERE is not one disjunct that reads by key with an equality on
/// every key attribute and nothing else (see [`WhereAnalysis`]). A value on
/// the left is the mirrored equality (measured 2026-11-02: `'a' = pk` reads
/// the item in `BatchExecuteStatement` and `ExecuteTransaction`). A
/// batch/transaction read must be a point read; `None` is what drives the
/// "must specify the primary key" error at the call site (batch
/// per-statement, transaction request-level).
pub(super) fn point_read_key(
    stmt: &SelectStatement,
    pk: &str,
    sk: Option<&str>,
    params: &[AttributeValue],
) -> Result<Option<Map<String, Value>>, DynamoDbError> {
    let Some(where_clause) = &stmt.where_clause else {
        return Ok(None);
    };
    let value_of = |op: &Operand| bound_value(op, params).cloned();
    let keys = KeyNames {
        partition: pk,
        sort: sk,
    };
    let analysis = WhereAnalysis::new(where_clause, keys, &value_of);
    // A key condition compared with a boolean is not measured in these
    // reads: one compared so that the comparison negates it (`= false`, `<>
    // true`) is no point read, and one that keeps it is the key condition.
    let Some(read) = analysis
        .single()
        .filter(|read| read.filter.is_empty() && !read.comparisons.iter().any(|c| c.negates()))
    else {
        return Ok(None);
    };
    let sort_value = match read.sort {
        None => None,
        Some(SortCondition::Compare(CompareOp::Eq, value)) => Some(value),
        Some(_) => return Ok(None),
    };
    build_key(pk, sk, Some(read.partition), sort_value, params)
}

/// How a write's WHERE pins its item: the top-level `AND` conditions that are
/// equalities of a key attribute with a value, written either way round, and
/// the other conditions.
pub(super) enum WhereKey<'a> {
    /// Every key attribute has one value; `residual` holds the other
    /// conditions in statement order.
    Pinned {
        partition: &'a Operand,
        sort: Option<&'a Operand>,
        residual: Vec<&'a WhereExpr>,
    },
    /// A key attribute has no equality.
    Partial,
    /// A key attribute has equalities with two different values; `name` is
    /// the first such attribute in statement order. `first` is the key the
    /// first equality of each key attribute gives, when every one has an
    /// equality.
    Conflict {
        name: String,
        first: Option<(&'a Operand, Option<&'a Operand>)>,
    },
}

/// Read a write's WHERE for the item it pins (see [`WhereKey`]). Two
/// equalities with one value are one (measured 2026-11-02: `pk = 'a' AND
/// pk = 'a'` and `sk = 'x' AND 'x' = sk` update the item).
pub(super) fn where_key<'a>(
    where_clause: &'a WhereExpr,
    pk: &str,
    sk: Option<&str>,
    params: &[AttributeValue],
) -> WhereKey<'a> {
    let keys = KeyNames {
        partition: pk,
        sort: sk,
    };
    let value_of = |op: &Operand| bound_value(op, params).cloned();
    let mut terms = Vec::new();
    flatten_and_lossy(where_clause, &mut terms);
    let mut pk_value: Option<&Operand> = None;
    let mut sk_value: Option<&Operand> = None;
    let mut residual = Vec::new();
    let mut conflict: Option<&str> = None;
    for term in terms {
        let (existing, value, name) = match key_equality(term, keys) {
            Some(KeyEquality::Partition(value)) => (&mut pk_value, value, pk),
            Some(KeyEquality::Sort(value)) => (&mut sk_value, value, sk.unwrap_or_default()),
            None => {
                residual.push(term);
                continue;
            }
        };
        match *existing {
            Some(seen) => {
                if conflict.is_none() && !same_value(seen, value, &value_of) {
                    conflict = Some(name);
                }
            }
            None => *existing = Some(value),
        }
    }
    let first = match (pk_value, sk_value, sk) {
        (Some(partition), sort, None) | (Some(partition), sort @ Some(_), Some(_)) => {
            Some((partition, sort))
        }
        _ => None,
    };
    match (conflict, first) {
        (Some(name), first) => WhereKey::Conflict {
            name: name.to_owned(),
            first,
        },
        (None, Some((partition, sort))) => WhereKey::Pinned {
            partition,
            sort,
            residual,
        },
        (None, None) => WhereKey::Partial,
    }
}

/// The value of a literal, or of a parameter the request supplies.
pub(super) fn bound_value<'a>(
    operand: &'a Operand,
    params: &'a [AttributeValue],
) -> Option<&'a AttributeValue> {
    match operand {
        Operand::Literal(value) => Some(value),
        Operand::Parameter(index) => params.get(*index),
        _ => None,
    }
}

/// The value an `INSERT`'s VALUE gives each attribute it names, by name: the
/// last one given, which is the value the item a write builds holds. The
/// parser refuses a VALUE that names an attribute twice (measured
/// 2026-11-03), so each name has one value. The write, the authorization of
/// the statement and the check for two statements on one item all take the
/// item's values from here, so they agree on its key.
pub(super) fn insert_values(insert: &InsertStatement) -> BTreeMap<&str, &Operand> {
    insert
        .value
        .iter()
        .map(|(name, operand)| (name.as_str(), operand))
        .collect()
}

/// The wire-form primary key a write statement targets (`INSERT` from its VALUE
/// map, see [`insert_values`], `UPDATE`/`DELETE` from the WHERE key equalities),
/// or `None` when the key is not fully derivable. Used only to detect two
/// statements naming the same item within one batch/transaction.
pub(super) fn write_key(
    stmt: &Statement,
    pk: &str,
    sk: Option<&str>,
    params: &[AttributeValue],
) -> Result<Option<Map<String, Value>>, DynamoDbError> {
    match stmt {
        Statement::Insert(insert) => {
            let values = insert_values(insert);
            let pk_op = values.get(pk).copied();
            let sk_op = sk.and_then(|sk| values.get(sk).copied());
            build_key(pk, sk, pk_op, sk_op, params)
        }
        Statement::Update(u) => match &u.where_clause {
            Some(where_clause) => pinned_key(where_clause, pk, sk, params),
            None => Ok(None),
        },
        Statement::Delete(d) => pinned_key(&d.where_clause, pk, sk, params),
        Statement::Select(_) | Statement::Exists(_) => Ok(None),
    }
}

/// The wire-form key a write's WHERE pins, or `None` when it pins none (see
/// [`where_key`]).
fn pinned_key(
    where_clause: &WhereExpr,
    pk: &str,
    sk: Option<&str>,
    params: &[AttributeValue],
) -> Result<Option<Map<String, Value>>, DynamoDbError> {
    match where_key(where_clause, pk, sk, params) {
        WhereKey::Pinned {
            partition, sort, ..
        } => build_key(pk, sk, Some(partition), sort, params),
        WhereKey::Partial | WhereKey::Conflict { .. } => Ok(None),
    }
}

/// A canonical string identity for an item: its table and wire-form key. Two
/// statements share an identity when they address the same item.
pub(super) fn canonical_identity(table: &str, key: &Map<String, Value>) -> String {
    // A JSON map holds its members in the order the build gives it: the order
    // they were inserted in when the build includes the MongoDB storage crate
    // (any build of the whole workspace), name order otherwise. The key is
    // always built pk before sk, so in either order equal (table, key) pairs
    // serialise identically.
    serde_json::json!({ "t": table, "k": key }).to_string()
}

/// Assemble the wire-form key map from the resolved pk/sk operands, or `None`
/// when the required key parts are absent.
fn build_key(
    pk: &str,
    sk: Option<&str>,
    pk_val: Option<&Operand>,
    sk_val: Option<&Operand>,
    params: &[AttributeValue],
) -> Result<Option<Map<String, Value>>, DynamoDbError> {
    let Some(pk_val) = pk_val else {
        return Ok(None);
    };
    if sk.is_some() && sk_val.is_none() {
        return Ok(None);
    }
    let mut key = Map::new();
    key.insert(pk.to_owned(), to_json_value(&resolve_av(pk_val, params)?)?);
    if let (Some(sk), Some(v)) = (sk, sk_val) {
        key.insert(sk.to_owned(), to_json_value(&resolve_av(v, params)?)?);
    }
    Ok(Some(key))
}

/// Flatten a top-level `AND` chain, treating an `OR`/`NOT` as a single opaque
/// term. An explicit stack keeps a long chain off the call stack.
pub(super) fn flatten_and_lossy<'a>(expr: &'a WhereExpr, out: &mut Vec<&'a WhereExpr>) {
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            WhereExpr::And(a, b) => {
                pending.push(b);
                pending.push(a);
            }
            other => out.push(other),
        }
    }
}

/// The short per-statement `Error` a batch carries for a failing statement.
///
/// Per-statement failures inside a batch surface as short Codes without the
/// `Exception` suffix, and the call itself stays HTTP 200. `TableName` is echoed
/// only for an authorization refusal and the failures of the item operation a
/// statement runs as (see [`StatementError::from_item_error`]); the errors of
/// reading and translating a statement carry the `Error` object alone,
/// matching the service. A conditional failure on a
/// statement that set `ReturnValuesOnConditionCheckFailure` to `ALL_OLD`
/// carries the existing item on the `Error` itself, never on the entry
/// (measured 2026-11-02).
pub(super) struct StatementError {
    pub code: &'static str,
    pub message: String,
    pub include_table_name: bool,
    pub item: Option<Value>,
}

impl StatementError {
    /// Map a single-statement `DynamoDbError` into the batch per-statement shape.
    pub(super) fn from_error(err: &DynamoDbError) -> Self {
        match err {
            DynamoDbError::DuplicateItemException(m) => Self {
                code: "DuplicateItem",
                message: m.clone(),
                include_table_name: true,
                item: None,
            },
            DynamoDbError::ConditionalCheckFailedException(m, item) => Self {
                code: "ConditionalCheckFailed",
                message: m.clone(),
                include_table_name: true,
                item: item.as_ref().and_then(|i| serde_json::to_value(i).ok()),
            },
            DynamoDbError::ResourceNotFoundException(_) => Self {
                code: "ResourceNotFound",
                message: "Requested resource not found".to_owned(),
                include_table_name: false,
                item: None,
            },
            // A statement the caller may not run carries the refusal and its
            // table (measured 2026-11-03).
            DynamoDbError::AccessDeniedException(m) => Self {
                code: "AccessDenied",
                message: m.clone(),
                include_table_name: true,
                item: None,
            },
            // Everything else a well-formed statement can raise per-statement is
            // a validation error (partial key, bad value, key-type mismatch).
            other => Self {
                code: "ValidationError",
                message: other.message().to_owned(),
                include_table_name: false,
                item: None,
            },
        }
    }

    /// Map an error of the item operation a statement runs as: as
    /// [`Self::from_error`], carrying the statement's `TableName`. The service
    /// names the table on each of them: a key value past its size limit
    /// (measured 2026-11-02 for SELECT, INSERT, UPDATE and DELETE), an
    /// UPDATE of a key attribute, a secondary index key of the wrong type,
    /// and an update the item cannot take (measured 2026-11-05). The errors
    /// of reading and translating a statement name none: a key value of the
    /// wrong type, a missing parameter, a WHERE without the full key.
    pub(super) fn from_item_error(err: &DynamoDbError) -> Self {
        let mut error = Self::from_error(err);
        error.include_table_name = true;
        error
    }

    /// A parse failure's per-statement error (always `ValidationError`).
    pub(super) fn parse(message: String) -> Self {
        Self {
            code: "ValidationError",
            message,
            include_table_name: false,
            item: None,
        }
    }

    /// Render this error as a `Responses[i]` entry, attaching `TableName` when
    /// the failure reached the item layer and a table is known.
    pub(super) fn into_entry(self, table: Option<&str>) -> Value {
        let mut error = Map::new();
        error.insert("Code".to_owned(), Value::String(self.code.to_owned()));
        error.insert("Message".to_owned(), Value::String(self.message));
        if let Some(item) = self.item {
            error.insert("Item".to_owned(), item);
        }
        let mut entry = Map::new();
        entry.insert("Error".to_owned(), Value::Object(error));
        if self.include_table_name
            && let Some(table) = table
        {
            entry.insert("TableName".to_owned(), Value::String(table.to_owned()));
        }
        Value::Object(entry)
    }
}

#[cfg(test)]
mod tests;
