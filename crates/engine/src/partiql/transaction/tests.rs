// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the ExecuteTransaction executor.

use super::*;
use extenddb_core::partiql::parse;

fn refusal(sql: &str, index: usize) -> Option<String> {
    reject_returning(&parse(sql).unwrap(), index)
        .err()
        .map(|e| e.message().to_owned())
}

#[test]
fn a_cancellation_lists_every_reason_code_in_order() {
    let err = cancellation(vec![
        CancellationReason::none(),
        not_found_reason(),
        CancellationReason::validation_error(common::KEY_SCHEMA_MISMATCH),
    ]);
    assert_eq!(
        err.message(),
        "Transaction cancelled, please refer cancellation reasons for specific reasons \
         [None, ResourceNotFound, ValidationError]"
    );
}

#[test]
fn every_update_returning_mode_is_refused_with_its_position() {
    for mode in ["ALL OLD", "MODIFIED OLD", "ALL NEW", "MODIFIED NEW"] {
        let sql = format!("UPDATE \"t\" SET a = 1 WHERE pk = 'k' RETURNING {mode} *");
        assert_eq!(
            refusal(&sql, 3).as_deref(),
            Some(
                "Validation failed in TransactStatements[3]: RETURNING clause is not \
                 supported in ExecuteTransaction."
            ),
            "{mode}"
        );
    }
}

#[test]
fn delete_returning_all_old_is_refused_like_an_update() {
    assert_eq!(
        refusal("DELETE FROM \"t\" WHERE pk = 'k' RETURNING ALL OLD *", 0).as_deref(),
        Some(
            "Validation failed in TransactStatements[0]: RETURNING clause is not \
             supported in ExecuteTransaction."
        )
    );
}

#[test]
fn delete_with_another_returning_clause_reports_the_invalid_clause() {
    assert_eq!(
        refusal(
            "DELETE FROM \"t\" WHERE pk = 'k' RETURNING MODIFIED OLD *",
            1
        )
        .as_deref(),
        Some(
            "Validation failed in TransactStatements[1]: Invalid returning clause: \
             RETURNING MODIFIED OLD *. Only RETURNING ALL OLD * is allowed in DELETE \
             statements."
        )
    );
}

#[test]
fn members_without_returning_pass() {
    for sql in [
        "UPDATE \"t\" SET a = 1 WHERE pk = 'k'",
        "DELETE FROM \"t\" WHERE pk = 'k'",
        "INSERT INTO \"t\" VALUE {'pk': 'k'}",
        "SELECT * FROM \"t\" WHERE pk = 'k'",
    ] {
        assert_eq!(refusal(sql, 0), None, "{sql}");
    }
}
