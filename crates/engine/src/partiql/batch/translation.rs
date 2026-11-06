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
//! key or tests a path into a key attribute against values, a `?` with no
//! parameter, and a DELETE returning clause other than `RETURNING ALL OLD *`.
//!
//! A statement that translates and fails the check of its request counts as a
//! read or a write, and not in the check for two statements on one item: a
//! read that is not of one item by its full primary key, an UPDATE that sets
//! or removes a key attribute, a write that gives a secondary index key a
//! value of the wrong type, and, after those two, a write whose WHERE reads a
//! path into a key attribute among its other conditions (measured 2026-11-05
//! on UPDATE and DELETE). The write refusals name the table, as the errors of
//! a write that reached its item do.
//!
//! Every error of a statement's translation, other than a fault of the
//! server, is that statement's own: the others run (measured 2026-11-06 with
//! a call an update cannot make, which the statement checks refuse before
//! the translation).

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::WhereExpr;
use extenddb_core::types::{AttributeValue, TableKeyInfo};

use crate::OperationContext;
use crate::partiql::{SingleRequest, common, write};

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
/// A fault of the server: a catalog read that fails, a translated map that
/// does not read back, or a table without a partition key.
pub(super) async fn translate_write(
    ctx: &OperationContext,
    stmt: &Statement,
    req: &SingleRequest,
    key_info: Option<&TableKeyInfo>,
) -> Result<Translation, DynamoDbError> {
    let params = &req.parameters;
    let request = match stmt {
        Statement::Update(update) => write::build_update_parts(ctx, update, params)
            .await
            .map(Request::Update),
        Statement::Insert(insert) => write::build_insert_parts(ctx, insert, params)
            .await
            .map(Request::Insert),
        _ => write::check_translation(ctx, stmt, params)
            .await
            .map(|()| Request::Other),
    };
    let request = match request {
        Ok(request) => request,
        Err(error) => return untranslated(error),
    };
    let Some(key_info) = key_info else {
        return Ok(Translation::Untranslated(
            DynamoDbError::ResourceNotFoundException(String::new()),
        ));
    };
    let refusal = match (&request, stmt) {
        (Request::Update(parts), Statement::Update(update)) => {
            write::update_refusal(&ctx.limits, parts, key_info).map(|refusal| {
                refusal.or_else(|| {
                    condition_key_refusal(update.where_clause.as_ref(), key_info, params)
                })
            })
        }
        (Request::Insert(parts), _) => write::insert_refusal(parts, key_info),
        (_, Statement::Delete(delete)) => Ok(condition_key_refusal(
            Some(&delete.where_clause),
            key_info,
            params,
        )),
        _ => Ok(None),
    };
    match refusal {
        Ok(Some(error)) => {
            return Ok(Translation::Refused {
                error,
                names_table: true,
            });
        }
        Ok(None) => {}
        Err(error) => return untranslated(error),
    }
    let (Some(pk), sk) = common::key_names(&key_info.key_schema) else {
        return Err(DynamoDbError::InternalServerError(
            "table has no partition key".to_owned(),
        ));
    };
    // The key is built from the values the translation has bound, so it
    // fails only as a fault.
    Ok(Translation::Runs {
        key: common::write_key(stmt, pk, sk, params)?,
    })
}

/// A statement whose translation fails with `error`, which is the
/// statement's own unless it is a fault of the server, such as a catalog
/// read that fails.
fn untranslated(error: DynamoDbError) -> Result<Translation, DynamoDbError> {
    match error {
        DynamoDbError::InternalServerError(_) => Err(error),
        error => Ok(Translation::Untranslated(error)),
    }
}

/// The request a write statement translates into.
enum Request {
    Update(write::UpdateParts),
    Insert(write::PutParts),
    /// A DELETE, whose request has no check of its own here.
    Other,
}

/// The refusal of a write whose WHERE pins its item and reads a path into a
/// key attribute among its other conditions (see
/// `common::condition_key_refusal`). An UPDATE gets it after the refusals
/// of its `UpdateItem` request (measured 2026-11-05: `SET pk = 'z'` and a
/// `SET` of an index key to a value of the wrong type are refused first).
fn condition_key_refusal(
    where_clause: Option<&WhereExpr>,
    key_info: &TableKeyInfo,
    params: &[AttributeValue],
) -> Option<DynamoDbError> {
    let (Some(pk), sk) = common::key_names(&key_info.key_schema) else {
        return None;
    };
    common::condition_key_refusal(where_clause?, pk, sk, params)
        .map(DynamoDbError::ValidationException)
}
