// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the `BatchExecuteStatement` and `ExecuteTransaction`
//! executors: deriving an item's primary key from a parsed statement, and
//! mapping a single-statement error into the short per-statement `Error` shape
//! a batch carries. Every behaviour is justified by a measurement against the
//! live service; see the project's PartiQL behavioural spec.

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{CompareOp, Operand, Path, SelectStatement, WhereExpr};
use extenddb_core::types::{
    AttributeValue, KeySchemaElement, KeyType, TableKeyInfo, is_multipart_key_schema,
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

/// The partition and optional sort attribute names of a table's key schema.
pub(super) fn key_names(key_info: &TableKeyInfo) -> (Option<&str>, Option<&str>) {
    let mut partition = None;
    let mut sort = None;
    for element in &key_info.key_schema {
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
        Statement::Select(s) => &s.from.table,
        Statement::Insert(i) => &i.table,
        Statement::Update(u) => &u.table,
        Statement::Delete(d) => &d.table,
    }
}

/// The wire-form primary key of a `SELECT` that is an exact full-primary-key
/// point read, or `None` when the WHERE does not pin the whole key by equality
/// and nothing else. A batch/transaction read must be a point read; `None` is
/// what drives the "must specify the primary key" error at the call site (batch
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
    let mut terms = Vec::new();
    if !flatten_and(where_clause, &mut terms) {
        return Ok(None);
    }

    let mut pk_val: Option<&Operand> = None;
    let mut sk_val: Option<&Operand> = None;
    for term in terms {
        match key_equality(term) {
            Some((name, rhs)) if name == pk && pk_val.is_none() => pk_val = Some(rhs),
            Some((name, rhs)) if Some(name) == sk && sk_val.is_none() => sk_val = Some(rhs),
            // A duplicate key equality, a non-key filter term, or any non-equality
            // predicate means this is not a clean point read.
            _ => return Ok(None),
        }
    }

    build_key(pk, sk, pk_val, sk_val, params)
}

/// The wire-form primary key a write statement targets (`INSERT` from its VALUE
/// map, `UPDATE`/`DELETE` from the WHERE key equalities), or `None` when the key
/// is not fully derivable. Used only to detect two statements naming the same
/// item within one batch/transaction.
pub(super) fn write_key(
    stmt: &Statement,
    pk: &str,
    sk: Option<&str>,
    params: &[AttributeValue],
) -> Result<Option<Map<String, Value>>, DynamoDbError> {
    match stmt {
        Statement::Insert(insert) => {
            let pk_op = insert.value.iter().find(|(n, _)| n == pk).map(|(_, o)| o);
            let sk_op = match sk {
                Some(sk) => insert.value.iter().find(|(n, _)| n == sk).map(|(_, o)| o),
                None => None,
            };
            build_key(pk, sk, pk_op, sk_op, params)
        }
        Statement::Update(u) => where_key(&u.where_clause, pk, sk, params),
        Statement::Delete(d) => where_key(&d.where_clause, pk, sk, params),
        Statement::Select(_) => Ok(None),
    }
}

/// A canonical string identity for an item: its table and wire-form key. Two
/// statements share an identity when they address the same item.
pub(super) fn canonical_identity(table: &str, key: &Map<String, Value>) -> String {
    // serde_json::Map preserves insertion order, and the key is always built
    // pk-before-sk, so equal (table, key) pairs serialise identically.
    serde_json::json!({ "t": table, "k": key }).to_string()
}

/// Extract the full key from a write WHERE clause.
fn where_key(
    where_clause: &WhereExpr,
    pk: &str,
    sk: Option<&str>,
    params: &[AttributeValue],
) -> Result<Option<Map<String, Value>>, DynamoDbError> {
    let mut terms = Vec::new();
    flatten_and_lossy(where_clause, &mut terms);
    let mut pk_val = None;
    let mut sk_val = None;
    for term in terms {
        if let Some((name, rhs)) = key_equality(term) {
            if name == pk && pk_val.is_none() {
                pk_val = Some(rhs);
            } else if Some(name) == sk && sk_val.is_none() {
                sk_val = Some(rhs);
            }
        }
    }
    build_key(pk, sk, pk_val, sk_val, params)
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

/// `term` as `<attr> = <literal|parameter>`, returning the attribute name and
/// the right operand.
fn key_equality(term: &WhereExpr) -> Option<(&str, &Operand)> {
    if let WhereExpr::Compare(Operand::Path(Path { root, steps }), CompareOp::Eq, rhs) = term
        && steps.is_empty()
        && matches!(rhs, Operand::Literal(_) | Operand::Parameter(_))
    {
        Some((root.as_str(), rhs))
    } else {
        None
    }
}

/// Flatten a top-level `AND` chain, returning false when an `OR`/`NOT` sits at
/// the top (which disqualifies a point read).
fn flatten_and<'a>(expr: &'a WhereExpr, out: &mut Vec<&'a WhereExpr>) -> bool {
    match expr {
        WhereExpr::And(a, b) => flatten_and(a, out) && flatten_and(b, out),
        WhereExpr::Or(_, _) | WhereExpr::Not(_) => false,
        other => {
            out.push(other);
            true
        }
    }
}

/// Flatten a top-level `AND` chain, treating an `OR`/`NOT` as a single opaque
/// term (used for write-key derivation, where a non-key term is simply ignored).
fn flatten_and_lossy<'a>(expr: &'a WhereExpr, out: &mut Vec<&'a WhereExpr>) {
    match expr {
        WhereExpr::And(a, b) => {
            flatten_and_lossy(a, out);
            flatten_and_lossy(b, out);
        }
        other => out.push(other),
    }
}

/// The short per-statement `Error` a batch carries for a failing statement.
///
/// Per-statement failures inside a batch surface as short Codes without the
/// `Exception` suffix, and the call itself stays HTTP 200. `TableName` is echoed
/// only for failures that reached the item layer (`DuplicateItem`,
/// `ConditionalCheckFailed`); validation and resource-not-found errors carry the
/// `Error` object alone, matching the service.
pub(super) struct StatementError {
    pub code: &'static str,
    pub message: String,
    pub include_table_name: bool,
}

impl StatementError {
    /// Map a single-statement `DynamoDbError` into the batch per-statement shape.
    pub(super) fn from_error(err: &DynamoDbError) -> Self {
        match err {
            DynamoDbError::DuplicateItemException(m) => Self {
                code: "DuplicateItem",
                message: m.clone(),
                include_table_name: true,
            },
            DynamoDbError::ConditionalCheckFailedException(m, _) => Self {
                code: "ConditionalCheckFailed",
                message: m.clone(),
                include_table_name: true,
            },
            DynamoDbError::ResourceNotFoundException(_) => Self {
                code: "ResourceNotFound",
                message: "Requested resource not found".to_owned(),
                include_table_name: false,
            },
            // Everything else a well-formed statement can raise per-statement is
            // a validation error (partial key, bad value, key-type mismatch).
            other => Self {
                code: "ValidationError",
                message: other.message().to_owned(),
                include_table_name: false,
            },
        }
    }

    /// A parse failure's per-statement error (always `ValidationError`).
    pub(super) fn parse(message: String) -> Self {
        Self {
            code: "ValidationError",
            message,
            include_table_name: false,
        }
    }

    /// Render this error as a `Responses[i]` entry, attaching `TableName` when
    /// the failure reached the item layer and a table is known.
    pub(super) fn into_entry(self, table: Option<&str>) -> Value {
        let mut entry = Map::new();
        entry.insert(
            "Error".to_owned(),
            serde_json::json!({ "Code": self.code, "Message": self.message }),
        );
        if self.include_table_name
            && let Some(table) = table
        {
            entry.insert("TableName".to_owned(), Value::String(table.to_owned()));
        }
        Value::Object(entry)
    }
}
