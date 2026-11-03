// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL write executor: `INSERT`, `UPDATE`, `DELETE`.
//!
//! Drives the engine's put/update/delete handlers with the PartiQL-specific
//! semantics on top: INSERT is create-only (`DuplicateItemException`), UPDATE
//! carries an implicit existence condition (not an upsert), both require the
//! full key, extra WHERE terms become a condition expression, and `RETURNING`
//! shapes the `Items` payload. Every behaviour is justified by a measurement
//! against the live service, cited where it is implemented.
//!
//! The statement is translated into the classic request shapes
//! (`PutItem`/`UpdateItem`/`DeleteItem` bodies) and handed to the existing
//! handlers, so the measured classic semantics (condition evaluation, capacity,
//! item sizing, error mapping) are reused rather than re-implemented.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{Operand, Returning, WhereExpr};
use extenddb_core::types::{AttributeValue, ScalarAttributeType, TableKeyInfo};

use super::{SingleRequest, bind_parameter, common};
use crate::{DispatchResult, OperationContext};

use delete::run_delete;
use expr_builder::ExprBuilder;
use insert::run_insert;
use update::run_update;

pub(super) use delete::build_delete_parts;
pub(super) use exists::build_exists_parts;
pub(super) use insert::build_insert_parts;
pub(super) use update::{UpdateParts, build_update_parts, check_overlapping_paths};

mod delete;
mod exists;
mod expr_builder;
mod insert;
mod update;

/// Execute a parsed write statement (`INSERT`/`UPDATE`/`DELETE`).
///
/// # Errors
///
/// Returns the service's error for the failure (duplicate key, failed
/// condition, missing full key, invalid returning clause), as measured.
pub(super) async fn run(
    ctx: &OperationContext,
    stmt: Statement,
    req: &SingleRequest,
) -> Result<DispatchResult, DynamoDbError> {
    match stmt {
        Statement::Insert(insert) => run_insert(ctx, insert, req).await,
        Statement::Update(update) => run_update(ctx, update, req).await,
        Statement::Delete(delete) => run_delete(ctx, delete, req).await,
        Statement::Exists(_) => Err(exists_outside_transaction()),
        // SELECT is dispatched to the read executor by the caller; reaching
        // here is a routing bug, not a client error.
        Statement::Select(_) => Err(DynamoDbError::InternalServerError(
            "SELECT routed to the write executor".to_owned(),
        )),
    }
}

/// `EXISTS(SELECT ...)` is a condition check for an `ExecuteTransaction`
/// member. `ExecuteStatement` refuses it, and a `BatchExecuteStatement`
/// member reports it as its error (measured 2026-11-02).
pub(super) fn exists_outside_transaction() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "EXISTS can only be used in ExecuteTransaction write requests.".to_owned(),
    )
}

// --- response shaping -----------------------------------------------------

/// Reshape a classic put/update/delete response into the PartiQL write shape:
/// a `RETURNING` item becomes the single element of `Items`, an absent one
/// leaves `Items` empty, and `ConsumedCapacity` passes through when present.
fn reshape(result: DispatchResult) -> Result<DispatchResult, DynamoDbError> {
    let mut items: Vec<Value> = Vec::new();
    if let Some(attributes) = result.body.get("Attributes") {
        items.push(attributes.clone());
    }
    let mut body = json!({ "Items": items });
    if let Some(cc) = result.body.get("ConsumedCapacity") {
        body["ConsumedCapacity"] = cc.clone();
    }
    Ok(DispatchResult {
        body,
        metrics: result.metrics,
    })
}

// --- key / condition translation -----------------------------------------

/// The partition and (optional) sort key attribute names of the target table.
///
/// A multi-part base table key (this engine's preview extension) is refused
/// with the service's multi-attribute message before any write is attempted.
fn key_names(key_info: &TableKeyInfo) -> Result<(&str, Option<&str>), DynamoDbError> {
    super::common::ensure_single_attribute_keys(&key_info.key_schema)?;
    let (partition, sort) = common::key_names(&key_info.key_schema);
    let partition = partition.ok_or_else(|| {
        DynamoDbError::InternalServerError("table has no partition key".to_owned())
    })?;
    Ok((partition, sort))
}

/// Split a write's WHERE into the full primary key (returned in wire form) and
/// the residual condition terms. A key equality may have the value on either
/// side (measured 2026-11-02: `UPDATE ... WHERE 'a' = pk` updates the item).
///
/// # Errors
///
/// The service's refusal of a key attribute with two different values
/// (measured 2026-11-02: `pk = 'a' AND 1 = pk`), and its full-key message
/// when the key is not pinned by equality.
fn split_key_where<'a>(
    where_clause: &'a WhereExpr,
    pk_name: &str,
    sk_name: Option<&str>,
    params: &[AttributeValue],
) -> Result<(Map<String, Value>, Vec<&'a WhereExpr>), DynamoDbError> {
    match split_full_key(where_clause, pk_name, sk_name, params)? {
        FullKey::Pinned(key, residual) => Ok((key, residual)),
        FullKey::Conflict(name) => Err(DynamoDbError::ValidationException(format!(
            "Multiple conditions on same key {name}. Only single item Update/Insert/Delete are \
             supported"
        ))),
        FullKey::Partial => Err(full_key_error()),
    }
}

/// The service's refusal of a write whose WHERE does not pin the whole key.
fn full_key_error() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "Where clause does not contain a mandatory equality on all key attributes".to_owned(),
    )
}

/// How a write's WHERE pins its item (see `common::where_key`), with the key
/// in wire form.
enum FullKey<'a> {
    /// The full primary key, and the residual WHERE terms.
    Pinned(Map<String, Value>, Vec<&'a WhereExpr>),
    /// A key attribute without an equality.
    Partial,
    /// A key attribute with two different values, named.
    Conflict(String),
}

/// Read a write's WHERE for its full primary key.
fn split_full_key<'a>(
    where_clause: &'a WhereExpr,
    pk_name: &str,
    sk_name: Option<&str>,
    params: &[AttributeValue],
) -> Result<FullKey<'a>, DynamoDbError> {
    let (partition, sort, residual) =
        match common::where_key(where_clause, pk_name, sk_name, params) {
            common::WhereKey::Pinned {
                partition,
                sort,
                residual,
            } => (partition, sort, residual),
            common::WhereKey::Partial => return Ok(FullKey::Partial),
            common::WhereKey::Conflict { name, .. } => return Ok(FullKey::Conflict(name)),
        };
    let key = wire_key(pk_name, sk_name, partition, sort, params)?;
    Ok(FullKey::Pinned(key, residual))
}

/// The wire-form primary key of the given key values.
fn wire_key(
    pk_name: &str,
    sk_name: Option<&str>,
    partition: &Operand,
    sort: Option<&Operand>,
    params: &[AttributeValue],
) -> Result<Map<String, Value>, DynamoDbError> {
    let mut key = Map::new();
    key.insert(
        pk_name.to_owned(),
        to_json_value(&resolve_av(partition, params)?)?,
    );
    if let (Some(sort_name), Some(value)) = (sk_name, sort) {
        key.insert(
            sort_name.to_owned(),
            to_json_value(&resolve_av(value, params)?)?,
        );
    }
    Ok(key)
}

/// Build a condition expression from residual WHERE terms, or `None` when there
/// are none. Each term is a self-contained predicate, so multiple terms are
/// ANDed with each wrapped in parentheses.
fn build_condition(
    builder: &mut ExprBuilder,
    residual: &[&WhereExpr],
    params: &[AttributeValue],
) -> Result<Option<String>, DynamoDbError> {
    if residual.is_empty() {
        return Ok(None);
    }
    if residual.len() == 1 {
        return Ok(Some(builder.condition(residual[0], params)?));
    }
    let mut parts = Vec::new();
    for term in residual {
        parts.push(format!("({})", builder.condition(term, params)?));
    }
    Ok(Some(parts.join(" AND ")))
}

// --- helpers --------------------------------------------------------------

/// Resolve an operand that must be a literal or a positional parameter to its
/// AttributeValue (used for INSERT item values and WHERE key equalities).
pub(super) fn resolve_av(
    operand: &Operand,
    params: &[AttributeValue],
) -> Result<AttributeValue, DynamoDbError> {
    match operand {
        Operand::Literal(av) => Ok(av.clone()),
        Operand::Parameter(index) => bind_parameter(params, *index),
        _ => Err(DynamoDbError::ValidationException(
            "Unsupported value expression".to_owned(),
        )),
    }
}

/// Validate that every key attribute is present in an INSERT item and typed to
/// match the table schema, with the PartiQL-specific messages.
fn validate_insert_keys(
    item: &BTreeMap<String, AttributeValue>,
    key_info: &TableKeyInfo,
) -> Result<(), DynamoDbError> {
    for element in &key_info.key_schema {
        let name = &element.attribute_name;
        let Some(value) = item.get(name) else {
            return Err(DynamoDbError::ValidationException(format!(
                "Key attribute should be present in the item: Key {name}"
            )));
        };
        if let Some(definition) = key_info
            .attribute_definitions
            .iter()
            .find(|d| &d.attribute_name == name)
        {
            let matches = match definition.attribute_type {
                ScalarAttributeType::S => matches!(value, AttributeValue::S(_)),
                ScalarAttributeType::N => matches!(value, AttributeValue::N(_)),
                ScalarAttributeType::B => matches!(value, AttributeValue::B(_)),
            };
            if !matches {
                return Err(DynamoDbError::ValidationException(format!(
                    "Key attribute's data type should match its data type in table's schema: Key {name}"
                )));
            }
        }
    }
    Ok(())
}

/// Map a `RETURNING` mode to the classic `ReturnValues` string, or `None` when
/// the statement has no `RETURNING` clause. A `MODIFIED` mode asks for the
/// whole item on its side; `returning::project_modified` then keeps only the
/// statement's paths.
fn returning_to_return_values(returning: Option<Returning>) -> Option<&'static str> {
    returning.map(|mode| match mode {
        Returning::AllOld | Returning::ModifiedOld => "ALL_OLD",
        Returning::AllNew | Returning::ModifiedNew => "ALL_NEW",
    })
}

/// The refusal of any `DELETE` clause other than `RETURNING ALL OLD *`, with
/// the clause named verbatim.
pub(super) fn invalid_delete_returning(returning: Returning) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "Invalid returning clause: RETURNING {}. \
         Only RETURNING ALL OLD * is allowed in DELETE statements.",
        returning_clause_text(returning)
    ))
}

/// The clause text as it appears in the DELETE invalid-returning message.
fn returning_clause_text(returning: Returning) -> &'static str {
    match returning {
        Returning::AllOld => "ALL OLD *",
        Returning::ModifiedOld => "MODIFIED OLD *",
        Returning::AllNew => "ALL NEW *",
        Returning::ModifiedNew => "MODIFIED NEW *",
    }
}

/// Serialize an item map to the DynamoDB JSON wire form.
fn to_json(item: &BTreeMap<String, AttributeValue>) -> Result<Value, DynamoDbError> {
    serde_json::to_value(item).map_err(|e| {
        tracing::error!(internal_error = %e, "failed to serialize PartiQL item");
        DynamoDbError::InternalServerError("Internal server error".to_owned())
    })
}

/// Serialize a single AttributeValue to the DynamoDB JSON wire form.
pub(super) fn to_json_value(av: &AttributeValue) -> Result<Value, DynamoDbError> {
    serde_json::to_value(av).map_err(|e| {
        tracing::error!(internal_error = %e, "failed to serialize PartiQL value");
        DynamoDbError::InternalServerError("Internal server error".to_owned())
    })
}

/// The DynamoDB type label of an attribute value, for the operand-type errors
/// ("operand type: S").
pub(super) fn av_type_label(av: &AttributeValue) -> &'static str {
    match av {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
    }
}

#[cfg(test)]
mod tests;
