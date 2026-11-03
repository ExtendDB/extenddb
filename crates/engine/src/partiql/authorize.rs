// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The authorization of each statement of a PartiQL request.
//!
//! The service authorizes a PartiQL request statement by statement, through
//! the [`ItemAuthorizer`] of the request's context, and needs no action for
//! the operation itself (measured 2026-11-03):
//!
//! * The action is the statement's type: `PartiQLSelect` (also for an
//!   `EXISTS` check), `PartiQLInsert`, `PartiQLUpdate`, `PartiQLDelete`. The
//!   resource is the table, or for a read of an index the index alone.
//! * A statement is authorized at a point among its checks: some come before
//!   it, the others after it (see [`gate`]).
//! * The condition keys are those of [`access`]. An `ExecuteStatement` read
//!   by key authorizes each of its disjuncts with its own partition value. A
//!   refused disjunct is held back, the way a disjunct that fails a check
//!   made when it is read is held back (see
//!   `select::read_plan::check_reads`), and a statement whose every disjunct
//!   is refused is refused at once.

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{Operand, WhereExpr};
use extenddb_core::partiql::plan::{KeyNames, KeyRead, WhereAnalysis, read_order};
use extenddb_core::types::{
    AttributeValue, KeySchemaElement, TableKeyInfo, is_multipart_key_schema,
};
use extenddb_storage::error::StorageError;

use super::SingleRequest;
use super::common::{self, bound_value};
use super::select::MAX_DECOMPOSED_READS;
use crate::create_table::storage_err_to_dynamo;
use crate::{ItemAccess, ItemAuthorizer, OperationContext};

pub(super) use access::written_value;
pub(super) use gate::Gate;

use access::{Target, access};

mod access;
mod gate;

/// The request a statement is a member of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Surface {
    /// `ExecuteStatement`.
    Statement,
    /// `BatchExecuteStatement`.
    Batch,
    /// `ExecuteTransaction`.
    Transaction,
}

impl Surface {
    /// The `dynamodb:EnclosingOperation` a statement of this request carries
    /// (measured 2026-11-03: absent in `ExecuteStatement`, where
    /// "ExecuteStatement" matches nothing).
    fn enclosing_operation(self) -> Option<&'static str> {
        match self {
            Self::Statement => None,
            Self::Batch => Some("BatchExecuteStatement"),
            Self::Transaction => Some("ExecuteTransaction"),
        }
    }
}

/// The decision on each disjunct of an `ExecuteStatement` SELECT read by
/// key, by partition value as the request writes it. Empty for every other
/// statement, which is authorized as a whole.
#[derive(Debug, Default)]
pub(super) struct Refused {
    /// Whether the statement's disjuncts were authorized one by one.
    by_disjunct: bool,
    /// Each partition value authorized, with its refusal when it is refused.
    by_partition: Vec<(Option<AttributeValue>, Option<DynamoDbError>)>,
}

impl Refused {
    /// The refusal of the disjunct `read` of a statement of `req`, when it
    /// is refused. A disjunct whose partition value was not authorized, one
    /// past [`MAX_DECOMPOSED_READS`], is refused: its statement has more
    /// disjuncts than may run, and is refused whatever the decision.
    pub(super) fn refusal(&self, read: &KeyRead<'_>, req: &SingleRequest) -> Option<DynamoDbError> {
        if !self.by_disjunct {
            return None;
        }
        match self.decision(read, req) {
            Some(decision) => decision.clone(),
            None => Some(unauthorized()),
        }
    }

    /// The decision on the partition value of `read`, when one was made.
    fn decision(&self, read: &KeyRead<'_>, req: &SingleRequest) -> Option<&Option<DynamoDbError>> {
        let value = written_value(read.partition, req);
        self.by_partition
            .iter()
            .find(|(partition, _)| *partition == value)
            .map(|(_, decision)| decision)
    }
}

/// Authorize `stmt`, a member of a request of `surface`, whose table has the
/// key info `key_info` (`None` for a missing table).
///
/// # Errors
///
/// The refusal of the statement (`AccessDeniedException` with the IAM
/// message), or a failed read of the index the statement names.
pub(super) async fn statement(
    ctx: &OperationContext,
    stmt: &Statement,
    key_info: Option<&TableKeyInfo>,
    req: &SingleRequest,
    surface: Surface,
) -> Result<Refused, DynamoDbError> {
    let schema = target_schema(ctx, stmt, key_info).await?;
    let target = Target {
        table_keys: key_info.map(|info| {
            info.key_schema
                .iter()
                .map(|key| key.attribute_name.as_str())
                .collect()
        }),
        keys: schema.as_deref().and_then(single_attribute_keys),
    };
    let value_of = |op: &Operand| bound_value(op, &req.parameters).cloned();
    let analysis = match (where_clause(stmt), target.keys) {
        (Some(where_clause), Some(keys)) => Some(WhereAnalysis::new(where_clause, keys, &value_of)),
        _ => None,
    };
    let access = access(stmt, &target, analysis.as_ref(), req, surface);
    let authorizer = ctx.item_authorizer.as_deref();
    match (stmt, surface, &analysis) {
        (Statement::Select(_), Surface::Statement, Some(analysis)) if analysis.is_keyed() => {
            each_disjunct(authorizer, &access, analysis, req, &value_of).await
        }
        _ => {
            authorize_access(authorizer, &access).await?;
            Ok(Refused::default())
        }
    }
}

/// Ask `authorizer` to allow `access`, refusing it when there is none.
///
/// # Errors
///
/// The authorizer's refusal, or `AccessDeniedException` without one.
pub(super) async fn authorize_access(
    authorizer: Option<&dyn ItemAuthorizer>,
    access: &ItemAccess,
) -> Result<(), DynamoDbError> {
    match authorizer {
        Some(authorizer) => authorizer.authorize(access).await,
        None => {
            tracing::error!(
                action = access.action,
                table = %access.table,
                "no item authorizer in the request context; refusing the statement"
            );
            Err(unauthorized())
        }
    }
}

/// The refusal of what no authorizer allowed.
fn unauthorized() -> DynamoDbError {
    DynamoDbError::AccessDeniedException("Not authorized to perform this action".to_owned())
}

/// Authorize each disjunct of an `ExecuteStatement` SELECT read by key with
/// its own partition value (measured 2026-11-03: under `ForAllValues`
/// `["a"]`, `pk IN ['a', 'b']` and `pk = 'a' OR pk = 'b'` return the items
/// of a with a NextToken, and `pk = 'b'` is refused). A partition value is
/// authorized once, whatever number of disjuncts read it.
///
/// At most [`MAX_DECOMPOSED_READS`] partition values are authorized, the
/// most disjuncts a statement that runs may have: a statement with more is
/// refused before it is authorized (see `select::read_plan::check_keys`),
/// unless a check that comes ahead of that refusal fails. Then the answer
/// only tells AccessDenied from that check's refusal, and is AccessDenied
/// when the values authorized are all refused.
///
/// # Errors
///
/// The refusal of the disjunct read last in key order when every disjunct is
/// refused, as a statement whose every disjunct fails a check made when it is
/// read gets that disjunct's refusal; an error other than a refusal at once.
async fn each_disjunct(
    authorizer: Option<&dyn ItemAuthorizer>,
    access: &ItemAccess,
    analysis: &WhereAnalysis<'_>,
    req: &SingleRequest,
    value_of: &(dyn Fn(&Operand) -> Option<AttributeValue> + Sync),
) -> Result<Refused, DynamoDbError> {
    let mut refused = Refused {
        by_disjunct: true,
        by_partition: Vec::new(),
    };
    let mut allowed = false;
    for read in analysis.reads() {
        let partition = written_value(read.partition, req);
        if refused
            .by_partition
            .iter()
            .any(|(decided, _)| *decided == partition)
        {
            continue;
        }
        if refused.by_partition.len() == MAX_DECOMPOSED_READS {
            break;
        }
        let one = ItemAccess {
            partition_keys: partition.clone().map(|value| vec![value]),
            ..access.clone()
        };
        let decision = match authorize_access(authorizer, &one).await {
            Ok(()) => {
                allowed = true;
                None
            }
            Err(error @ DynamoDbError::AccessDeniedException(_)) => Some(error),
            Err(error) => return Err(error),
        };
        refused.by_partition.push((partition, decision));
    }
    if !allowed
        && let Some(error) = analysis
            .reads()
            .iter()
            .filter_map(|read| {
                let refusal = refused.decision(read, req).cloned().flatten()?;
                Some((read, refusal))
            })
            .max_by(|a, b| read_order(a.0, b.0, value_of))
            .map(|(_, refusal)| refusal)
    {
        return Err(error);
    }
    Ok(refused)
}

/// The key schema of what `stmt` reads or writes: the index's for a read of
/// an index, the table's otherwise. `None` when the table is missing, when
/// the index is missing or a vector index, so that the condition keys read
/// from the key are absent.
///
/// # Errors
///
/// A failed read of the index other than a missing index.
async fn target_schema(
    ctx: &OperationContext,
    stmt: &Statement,
    key_info: Option<&TableKeyInfo>,
) -> Result<Option<Vec<KeySchemaElement>>, DynamoDbError> {
    let Some(key_info) = key_info else {
        return Ok(None);
    };
    let index = match stmt {
        Statement::Select(select) | Statement::Exists(select) => select.from.index.as_deref(),
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => None,
    };
    let Some(index) = index else {
        return Ok(Some(key_info.key_schema.clone()));
    };
    if key_info
        .vector_indexes
        .iter()
        .any(|v| v.index_name == index)
    {
        return Ok(None);
    }
    match ctx
        .storage
        .index_info_by_table_id(&key_info.table_id, index)
        .await
    {
        Ok(info) => Ok(Some(info.key_schema)),
        Err(StorageError::IndexNotFound(_)) => Ok(None),
        Err(e) => Err(storage_err_to_dynamo(e)),
    }
}

/// The key names of a key schema with one partition key and at most one sort
/// key. A multi-attribute key is refused once the statement is authorized,
/// so its condition keys read from the key are absent.
fn single_attribute_keys(schema: &[KeySchemaElement]) -> Option<KeyNames<'_>> {
    if is_multipart_key_schema(schema) {
        return None;
    }
    let (partition, sort) = common::key_names(schema);
    Some(KeyNames {
        partition: partition?,
        sort,
    })
}

/// The WHERE of a statement, when it has one.
fn where_clause(stmt: &Statement) -> Option<&WhereExpr> {
    match stmt {
        Statement::Select(select) | Statement::Exists(select) => select.where_clause.as_ref(),
        Statement::Update(update) => update.where_clause.as_ref(),
        Statement::Delete(delete) => Some(&delete.where_clause),
        Statement::Insert(_) => None,
    }
}

#[cfg(test)]
mod tests;
