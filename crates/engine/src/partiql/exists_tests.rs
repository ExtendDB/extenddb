// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the refusal of an `EXISTS(SELECT ...)` statement in
//! ExecuteStatement and in a statement of BatchExecuteStatement.

use serde_json::{Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::parser::EXISTS_OUTSIDE_TRANSACTION as EXISTS;

use super::PartiqlExecutor;
use super::test_context::{block_on, context};

const TWO_TYPES: &str = "Supplied AttributeValue has more than one datatypes set, must contain \
                         exactly one of the supported datatypes";

/// Each statement, the `Parameters` it is sent with, and the message both
/// operations answer for it, measured on a table of 40 characters keyed on
/// `pk` (S): 2026-11-13 and 2026-11-14 for the first two, 2026-11-18 for the
/// others. The parse decides every other measured statement on both
/// operations, and the parser's tests hold those.
fn statements() -> Vec<(String, Option<Value>, &'static str)> {
    let t = format!("\"{}\"", "t".repeat(40));
    let missing = format!("\"{}-missing\"", "t".repeat(40));
    [
        ("EXISTS(SELECT * FROM <t> WHERE pk = ?)", None, EXISTS),
        (
            "EXISTS(SELECT * FROM <t> WHERE pk = 1 AND s < true)",
            None,
            EXISTS,
        ),
        (
            "EXISTS(SELECT * FROM <t> WHERE pk = ?)",
            Some(json!([{ "S": "a", "N": "1" }])),
            TWO_TYPES,
        ),
        ("EXISTS(SELECT * FROM <missing> WHERE pk = 1)", None, EXISTS),
    ]
    .into_iter()
    .map(|(statement, parameters, message)| {
        let statement = statement.replace("<t>", &t).replace("<missing>", &missing);
        (statement, parameters, message)
    })
    .collect()
}

#[test]
fn exists_outside_a_transaction_is_refused_ahead_of_its_other_faults() {
    block_on(async {
        let ctx = context(None).await;
        for (statement, parameters, message) in statements() {
            let mut member = json!({ "Statement": statement });
            if let Some(parameters) = parameters {
                member["Parameters"] = parameters;
            }
            let refusal = ctx.execute_statement(member.clone()).await.err();
            assert!(
                matches!(&refusal, Some(DynamoDbError::ValidationException(m)) if m == message),
                "{statement}: {refusal:?}"
            );
            let batch = ctx
                .batch_execute_statement(json!({ "Statements": [member] }))
                .await
                .map(|answer| answer.body)
                .map_err(|e| format!("{e:?}"));
            assert_eq!(
                batch,
                Ok(json!({ "Responses": [
                    { "Error": { "Code": "ValidationError", "Message": message } },
                ]})),
                "{statement}"
            );
        }
    });
}
