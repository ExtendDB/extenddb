// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How far a statement of a `BatchExecuteStatement` gets before it runs.
//!
//! The service translates each statement into the read or the write it runs
//! as. It then sorts the statements that translate into reads and writes,
//! checks the request each one translates into, refuses two statements on
//! one item, and runs each statement (measured 2026-11-05).
//!
//! A statement whose translation fails gets its own error, and counts
//! neither as a read nor as a write, nor in the check for two statements on
//! one item: a missing table or index, a key value of the wrong type, an
//! INSERT without a key attribute, a write whose WHERE does not give the full
//! key, a `?` with no parameter, and a DELETE returning clause other than
//! `RETURNING ALL OLD *`.
//!
//! A statement that translates and fails the check of its request counts as a
//! read or a write, and not in the check for two statements on one item: a
//! read that is not of one item by its full primary key, an UPDATE that sets
//! or removes a key attribute, and a write that gives a secondary index key a
//! value of the wrong type. The two write refusals name the table, as the
//! errors of a write that reached its item do.

use std::collections::HashMap;

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{InsertStatement, UpdateStatement};
use extenddb_core::types::{AttributeValue, Item, TableKeyInfo};
use extenddb_core::validation::{
    IndexKeyRef, validate_index_key_literal_operands, validate_index_key_types,
};

use crate::OperationContext;
use crate::expression_helpers::{build_expression_maps, parse_update_expr};
use crate::partiql::{SingleRequest, common, write};
use crate::transact_write_helpers::validate_no_key_updates;

/// How far a statement that parsed gets before the batch runs it.
pub(super) enum Translation {
    /// The statement runs. `key` is the item it names, when it names one.
    Runs { key: Option<Map<String, Value>> },
    /// The statement fails its translation, with this error.
    Untranslated(DynamoDbError),
    /// The statement translates and fails the check of its request with
    /// `error`, which carries the statement's table when `names_table`.
    Refused {
        error: DynamoDbError,
        names_table: bool,
    },
}

impl Translation {
    /// Whether the statement counts as a read or a write.
    pub(super) fn counts(&self) -> bool {
        !matches!(self, Self::Untranslated(_))
    }

    /// The item the statement names, when it runs and names one.
    pub(super) fn key(&self) -> Option<&Map<String, Value>> {
        match self {
            Self::Runs { key } => key.as_ref(),
            Self::Untranslated(_) | Self::Refused { .. } => None,
        }
    }
}

/// How far the write statement `stmt` gets (see [`Translation`]). Its
/// translation makes the checks [`write::run`] makes before it authorizes
/// the statement.
///
/// # Errors
///
/// A fault of a catalog read, or of reading back a translated map.
pub(super) async fn translate_write(
    ctx: &OperationContext,
    stmt: &Statement,
    req: &SingleRequest,
    key_info: Option<&TableKeyInfo>,
) -> Result<Translation, DynamoDbError> {
    if let Err(error) = write::check_translation(ctx, stmt, &req.parameters).await {
        return Ok(Translation::Untranslated(error));
    }
    let Some(key_info) = key_info else {
        return Ok(Translation::Untranslated(
            DynamoDbError::ResourceNotFoundException(String::new()),
        ));
    };
    let refusal = match stmt {
        Statement::Update(update) => update_refusal(ctx, update, &req.parameters, key_info).await?,
        Statement::Insert(insert) => insert_refusal(ctx, insert, &req.parameters, key_info).await?,
        _ => None,
    };
    if let Some(error) = refusal {
        return Ok(Translation::Refused {
            error,
            names_table: true,
        });
    }
    let (Some(pk), sk) = common::key_names(&key_info.key_schema) else {
        return Err(DynamoDbError::InternalServerError(
            "table has no partition key".to_owned(),
        ));
    };
    Ok(Translation::Runs {
        key: common::write_key(stmt, pk, sk, &req.parameters)?,
    })
}

/// The refusal of the `UpdateItem` an UPDATE translates into, when the
/// update sets or removes a key attribute or gives a secondary index key a
/// value of the wrong type, in the words and the order `UpdateItem` gives
/// them.
async fn update_refusal(
    ctx: &OperationContext,
    update: &UpdateStatement,
    params: &[AttributeValue],
    key_info: &TableKeyInfo,
) -> Result<Option<DynamoDbError>, DynamoDbError> {
    let parts = write::build_update_parts(ctx, update, params).await?;
    let names: HashMap<String, String> = from_wire(&parts.names)?;
    let values: HashMap<String, AttributeValue> = from_wire(&parts.values)?;
    let maps = build_expression_maps(Some(&names), Some(&values));
    let actions = parse_update_expr(&parts.update_expression, &ctx.limits)?;
    Ok(validate_no_key_updates(&actions, key_info, &maps)
        .and_then(|()| {
            validate_index_key_literal_operands(
                &actions,
                &index_keys(key_info),
                &key_info.attribute_definitions,
                &maps,
            )
        })
        .err())
}

/// The refusal of the `PutItem` an INSERT translates into, when its item
/// gives a secondary index key a value of the wrong type.
async fn insert_refusal(
    ctx: &OperationContext,
    insert: &InsertStatement,
    params: &[AttributeValue],
    key_info: &TableKeyInfo,
) -> Result<Option<DynamoDbError>, DynamoDbError> {
    let parts = write::build_insert_parts(ctx, insert, params).await?;
    let Value::Object(item) = &parts.item else {
        return Err(internal());
    };
    let item: Item = from_wire(item)?;
    Ok(validate_index_key_types(
        &item,
        &index_keys(key_info),
        &key_info.attribute_definitions,
    )
    .err())
}

/// The key schema of each secondary index of the table.
fn index_keys(key_info: &TableKeyInfo) -> Vec<IndexKeyRef<'_>> {
    key_info
        .global_secondary_indexes
        .iter()
        .chain(&key_info.local_secondary_indexes)
        .map(|index| IndexKeyRef {
            index_name: &index.index_name,
            key_schema: &index.key_schema,
        })
        .collect()
}

/// Read back a map the translation built in wire form.
fn from_wire<T: serde::de::DeserializeOwned>(map: &Map<String, Value>) -> Result<T, DynamoDbError> {
    serde_json::from_value(Value::Object(map.clone())).map_err(|e| {
        tracing::error!(internal_error = %e, "failed to read a translated PartiQL map");
        internal()
    })
}

fn internal() -> DynamoDbError {
    DynamoDbError::InternalServerError("Internal server error".to_owned())
}
