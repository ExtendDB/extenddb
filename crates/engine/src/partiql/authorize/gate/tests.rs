// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the place of a statement's authorization among its checks.

use std::sync::Arc;

use serde_json::json;

use extenddb_core::partiql::parse;

use super::*;
use crate::ItemAuthorizer;
use crate::partiql::test_context::{self, Fixed, block_on};

const SELECT: &str = "SELECT * FROM \"t\" WHERE pk = 'a'";

fn validation(message: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(message.to_owned())
}

fn not_found() -> DynamoDbError {
    DynamoDbError::ResourceNotFoundException("Requested resource not found".to_owned())
}

fn refused() -> DynamoDbError {
    DynamoDbError::AccessDeniedException("refused".to_owned())
}

/// An answer of [`Gate::settle`] with its errors written out, the inner one
/// a refusal and the outer one a fault.
type Shown = Result<Result<(), String>, String>;

fn shown(answer: Result<Result<(), DynamoDbError>, DynamoDbError>) -> Shown {
    answer
        .map(|inner| inner.map_err(|e| format!("{e:?}")))
        .map_err(|e| format!("{e:?}"))
}

/// The answer that refuses the statement with `error`.
fn refusal(error: &DynamoDbError) -> Shown {
    Ok(Err(format!("{error:?}")))
}

/// Run `body` with the gate of `sql`, an `ExecuteStatement` on the missing
/// table "t", whose context authorizes with `authorizer`; and give its
/// answer with the number of accesses `authorizer` was asked about.
fn with_gate<T>(
    sql: &str,
    authorizer: &Arc<Fixed>,
    body: impl AsyncFnOnce(&mut Gate<'_>) -> T,
) -> (T, usize) {
    block_on(async {
        let shared: Arc<dyn ItemAuthorizer> = Arc::clone(authorizer) as Arc<dyn ItemAuthorizer>;
        let ctx = test_context::context(Some(shared)).await;
        let stmt = parse(sql).unwrap();
        let req = SingleRequest::from_body(&json!({ "Statement": sql })).unwrap();
        let mut gate = Gate::new(&ctx, &stmt, None, &req, Surface::Statement);
        let answer = body(&mut gate).await;
        (answer, authorizer.asked())
    })
}

/// The answer `settle` gives `sql` for an executor that returned `result`
/// without passing the gate.
fn settled(authorizer: &Arc<Fixed>, result: Result<(), DynamoDbError>) -> (Shown, usize) {
    let (answer, asked) = with_gate(SELECT, authorizer, async |gate| gate.settle(result).await);
    (shown(answer), asked)
}

#[test]
fn a_refusal_made_before_authorization_is_answered_without_it() {
    for message in [
        PARAMETER_COUNT_MISMATCH,
        GSI_STRONG_READ,
        TOO_MANY_DECOMPOSED_READS,
        INVALID_NEXT_TOKEN,
    ] {
        let (answer, asked) = settled(&Fixed::refusing(), Err(validation(message)));
        assert_eq!(answer, refusal(&validation(message)), "{message}");
        assert_eq!(asked, 0, "{message}");
    }
}

#[test]
fn a_refusal_made_after_authorization_is_answered_once_it_allows() {
    let (answer, asked) = settled(&Fixed::allowing(), Err(not_found()));
    assert_eq!(answer, refusal(&not_found()));
    assert_eq!(asked, 1);
    // A caller that may not run the statement gets the refusal of the
    // statement instead (measured 2026-11-03: a missing table, a missing
    // index, a key of the wrong type, ORDER BY on another attribute).
    let (answer, asked) = settled(&Fixed::refusing(), Err(not_found()));
    assert_eq!(answer, refusal(&refused()));
    assert_eq!(asked, 1);
}

#[test]
fn no_answer_is_given_for_a_statement_that_is_not_authorized() {
    let (answer, _) = settled(&Fixed::refusing(), Ok(()));
    assert_eq!(answer, refusal(&refused()));
    let (answer, asked) = settled(&Fixed::allowing(), Ok(()));
    assert_eq!(answer, Ok(Ok(())));
    assert_eq!(asked, 1);
}

#[test]
fn a_fault_of_the_authorizer_fails_the_request() {
    let (answer, _) = settled(&Fixed::failing(), Err(not_found()));
    assert!(
        answer
            .as_ref()
            .is_err_and(|fault| fault.starts_with("InternalServerError")),
        "{answer:?}"
    );
}

#[test]
fn a_statement_is_authorized_once() {
    let (answer, asked) = with_gate(SELECT, &Fixed::allowing(), async |gate| {
        assert!(!gate.decided());
        assert!(gate.pass().await.is_ok());
        assert!(gate.decided());
        assert!(gate.pass().await.is_ok());
        // A refusal raised once the gate is passed is the executor's
        // answer, whatever check made it.
        gate.settle(Err(validation(INVALID_NEXT_TOKEN))).await
    });
    assert_eq!(shown(answer), refusal(&validation(INVALID_NEXT_TOKEN)));
    assert_eq!(asked, 1);
    let (answer, asked) = with_gate(SELECT, &Fixed::refusing(), async |gate| {
        assert!(gate.pass().await.is_err());
        gate.settle(Err(not_found())).await
    });
    assert_eq!(shown(answer), refusal(&refused()));
    assert_eq!(asked, 1);
}

#[test]
fn the_checks_made_before_authorization_are_known_by_their_refusal() {
    // Measured 2026-11-03 with a caller that may not run the statement.
    let select = parse(SELECT).unwrap();
    for message in [
        PARAMETER_COUNT_MISMATCH,
        GSI_STRONG_READ,
        OVERLAP_MESSAGE,
        TOO_MANY_DECOMPOSED_READS,
        INVALID_NEXT_TOKEN,
        TOKEN_MISMATCH,
        PRIMARY_KEY_REQUIRED,
        "Key attributes must be scalars; list random access '[]' and map lookup '.' are not \
         allowed: Key pk",
        "Invalid returning clause: RETURNING ALL NEW *. Only RETURNING ALL OLD * is allowed in \
         DELETE statements.",
    ] {
        assert!(
            made_before_authorization(&validation(message), &select),
            "{message}"
        );
    }
    // `SELECT COUNT(*)`, in its statement only.
    let count = parse("SELECT COUNT(*) FROM \"t\"").unwrap();
    let projected = "Unexpected path component at 1:8:5";
    assert!(made_before_authorization(&validation(projected), &count));
    assert!(!made_before_authorization(&validation(projected), &select));
}

#[test]
fn the_checks_made_after_authorization_are_not() {
    let select = parse(SELECT).unwrap();
    for error in [
        validation("The table does not have the specified index"),
        validation(
            "Filter Expression can only contain non-primary key attributes: Primary key \
             attribute: sk",
        ),
        validation(
            "One or more parameter values were invalid: Size of hashkey has exceeded the \
             maximum size limit of2048 bytes",
        ),
        not_found(),
        DynamoDbError::AccessDeniedException(GSI_STRONG_READ.to_owned()),
    ] {
        assert!(!made_before_authorization(&error, &select), "{error:?}");
    }
}
