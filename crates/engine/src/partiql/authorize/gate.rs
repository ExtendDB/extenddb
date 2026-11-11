// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Where a statement is authorized among its checks.
//!
//! The service checks a statement in one order, and authorizes it at a
//! point in that order that depends on the checks (measured 2026-11-03).
//! Some checks are made before authorization: a caller with no permission
//! gets their refusals. The others are made after it: such a caller gets
//! AccessDenied instead. The executors make their checks in the order the
//! service makes them for a permitted caller, and a [`Gate`] places the
//! authorization in it:
//!
//! * the executor passes the gate after its last check made before
//!   authorization and before it reads or writes an item;
//! * when a check fails before that, the refusal is answered at once if
//!   the service makes that check before authorization, and otherwise once
//!   the statement is authorized, so a caller with no permission gets
//!   AccessDenied ([`Gate::settle`]).
//!
//! So a check made before authorization is answered first only when no
//! check that comes ahead of it for a permitted caller fails. The checks
//! made before authorization are listed in [`made_before_authorization`].

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::parser::PARAMETER_COUNT_MISMATCH;
use extenddb_core::partiql::plan::{OVERLAP_MESSAGE, SCALAR_KEY_REFUSAL};
use extenddb_core::types::TableKeyInfo;

use super::{Refused, Surface, statement};
use crate::OperationContext;
use crate::partiql::SingleRequest;
use crate::partiql::batch::PRIMARY_KEY_REQUIRED;
use crate::partiql::next_token::{INVALID_NEXT_TOKEN, TOKEN_MISMATCH};
use crate::partiql::select::{GSI_STRONG_READ, TOO_MANY_DECOMPOSED_READS};
use crate::partiql::write::INVALID_RETURNING;

/// The authorization of one statement, made once, the first time the
/// statement's executor or [`Gate::settle`] needs it.
pub(in crate::partiql) struct Gate<'a> {
    ctx: &'a OperationContext,
    stmt: &'a Statement,
    key_info: Option<&'a TableKeyInfo>,
    req: &'a SingleRequest,
    surface: Surface,
    decision: Option<Result<Refused, DynamoDbError>>,
}

impl<'a> Gate<'a> {
    /// The gate of `stmt`, a member of a request of `surface`, whose table
    /// has the key info `key_info` (`None` for a missing table).
    pub(in crate::partiql) fn new(
        ctx: &'a OperationContext,
        stmt: &'a Statement,
        key_info: Option<&'a TableKeyInfo>,
        req: &'a SingleRequest,
        surface: Surface,
    ) -> Self {
        Self {
            ctx,
            stmt,
            key_info,
            req,
            surface,
            decision: None,
        }
    }

    /// Authorize the statement, once: the disjuncts of a read by key the
    /// caller may not read, empty for any other statement.
    ///
    /// # Errors
    ///
    /// The statement's refusal (`AccessDeniedException`), or a fault of
    /// the authorizer or of a catalog read.
    pub(in crate::partiql) async fn pass(&mut self) -> Result<&Refused, DynamoDbError> {
        if self.decision.is_none() {
            let decision =
                statement(self.ctx, self.stmt, self.key_info, self.req, self.surface).await;
            self.decision = Some(decision);
        }
        match self.decision.as_ref() {
            Some(Ok(refused)) => Ok(refused),
            Some(Err(error)) => Err(error.clone()),
            None => unreachable!("decided above"),
        }
    }

    /// Whether the statement has been authorized, or refused.
    pub(in crate::partiql) fn decided(&self) -> bool {
        self.decision.is_some()
    }

    /// The answer of a statement whose executor returned `result`. A
    /// refusal raised before the executor passed the gate is answered as it
    /// is when the service makes its check before authorization, and once
    /// the statement is authorized otherwise; an answer is never given for
    /// a statement that has not been authorized. The outer error is a fault
    /// of the authorization, which fails the whole request; a refusal is the
    /// inner one.
    ///
    /// # Errors
    ///
    /// A fault of the authorizer or of a catalog read.
    pub(in crate::partiql) async fn settle<T>(
        &mut self,
        result: Result<T, DynamoDbError>,
    ) -> Result<Result<T, DynamoDbError>, DynamoDbError> {
        if !self.decided()
            && let Err(error) = &result
            && made_before_authorization(error, self.stmt)
        {
            return Ok(result);
        }
        match self.pass().await {
            Ok(_) => Ok(result),
            Err(denied @ DynamoDbError::AccessDeniedException(_)) => Ok(Err(denied)),
            Err(fault) => Err(fault),
        }
    }
}

/// Whether `error` is the refusal of a check the service makes before it
/// authorizes `stmt` (measured 2026-11-03 with a caller that may not run
/// the statement): a statement with fewer parameters than placeholders,
/// `SELECT COUNT(*)`, a strongly consistent read of a global secondary
/// index, a path into a key attribute, overlapping disjuncts, more than 50
/// disjuncts, a NextToken that is invalid or another statement's, a DELETE
/// with a `RETURNING` clause other than `ALL OLD *`, and a
/// `BatchExecuteStatement` read without the full primary key. The checks of
/// a request, the parse of a statement and the checks that need no catalog
/// are made before any [`Gate`] is made.
pub(in crate::partiql) fn made_before_authorization(
    error: &DynamoDbError,
    stmt: &Statement,
) -> bool {
    let DynamoDbError::ValidationException(message) = error else {
        return false;
    };
    let projection_error = match stmt {
        Statement::Select(select) => select.projection_error.as_deref(),
        _ => None,
    };
    [
        PARAMETER_COUNT_MISMATCH,
        GSI_STRONG_READ,
        OVERLAP_MESSAGE,
        TOO_MANY_DECOMPOSED_READS,
        INVALID_NEXT_TOKEN,
        TOKEN_MISMATCH,
        PRIMARY_KEY_REQUIRED,
    ]
    .contains(&message.as_str())
        || [SCALAR_KEY_REFUSAL, INVALID_RETURNING]
            .iter()
            .any(|prefix| message.starts_with(prefix))
        || projection_error == Some(message.as_str())
}

#[cfg(test)]
mod tests;
