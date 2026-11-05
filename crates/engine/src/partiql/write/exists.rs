// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The EXISTS check of a transaction member: a ConditionCheck of one item.

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::SelectStatement;
use extenddb_core::partiql::plan::SCALAR_KEY_REFUSAL;
use extenddb_core::types::AttributeValue;

use super::expr_builder::ExprBuilder;
use super::{build_condition, key_names, wire_key};
use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;
use crate::partiql::common;

// --- EXISTS ---------------------------------------------------------------

/// The translated `TransactWriteItems` `ConditionCheck` pieces for an
/// `EXISTS(SELECT ...)` transaction member.
pub(crate) struct ConditionCheckParts {
    pub table: String,
    pub key: Map<String, Value>,
    pub condition: String,
    pub names: Map<String, Value>,
    pub values: Map<String, Value>,
}

/// Translate an `EXISTS(SELECT ...)` transaction member into its
/// `ConditionCheck` pieces. `None` when the SELECT is not a read of one item
/// (the full primary key by equality, on the table) with a further condition:
/// the service refuses a read with no further condition (measured
/// 2026-11-02). The check is the further condition alone: a missing item
/// whose condition holds, such as `w IS MISSING`, lets the transaction write
/// (measured 2026-11-02).
///
/// A key attribute given two values names no item, and such a check fails,
/// with or without a further condition: it reads the item the first values
/// name, with the whole WHERE as its condition (measured 2026-11-02: `pk =
/// 'a' AND pk = 'b' AND a = 1` and `pk = 'a' AND pk = 'b'` cancel the
/// transaction with `ConditionalCheckFailed` for the member).
///
/// A condition that tests a path into a key attribute against values (see
/// `common::where_key`) fails the request, naming the member at position
/// `index` (measured 2026-11-05: `EXISTS(SELECT * FROM t WHERE pk = 'k' AND
/// sk = 'b' AND pk.c = 1)`).
///
/// # Errors
///
/// Returns `ResourceNotFoundException` for a missing table, the
/// multi-attribute refusal for a multi-part key, and the refusal of a path
/// into a key attribute tested against values.
pub(crate) async fn build_exists_parts(
    ctx: &OperationContext,
    index: usize,
    select: &SelectStatement,
    params: &[AttributeValue],
) -> Result<Option<ConditionCheckParts>, DynamoDbError> {
    let key_info = ctx
        .table_key_info(&select.from.table)
        .await
        .map_err(storage_err_to_dynamo)?;
    let (pk_name, sk_name) = key_names(&key_info)?;
    let (None, Some(where_clause)) = (&select.from.index, &select.where_clause) else {
        return Ok(None);
    };
    let (key, condition) = match common::where_key(where_clause, pk_name, sk_name, params) {
        common::WhereKey::Pinned {
            partition,
            sort,
            residual,
        } => (
            wire_key(pk_name, sk_name, partition, sort, params)?,
            residual,
        ),
        common::WhereKey::Conflict {
            first: Some((partition, sort)),
            ..
        } => (
            wire_key(pk_name, sk_name, partition, sort, params)?,
            vec![where_clause],
        ),
        common::WhereKey::IntoKey { name } => {
            return Err(DynamoDbError::ValidationException(format!(
                "Validation failed in TransactStatements[{index}]: {SCALAR_KEY_REFUSAL}{name}"
            )));
        }
        common::WhereKey::Partial | common::WhereKey::Conflict { .. } => return Ok(None),
    };
    let mut builder = ExprBuilder::default();
    let Some(further) = build_condition(&mut builder, &condition, params)? else {
        return Ok(None);
    };
    Ok(Some(ConditionCheckParts {
        table: select.from.table.clone(),
        key,
        condition: further,
        names: builder.names,
        values: builder.values,
    }))
}
