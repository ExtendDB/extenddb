// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the request constraint checks.

use serde_json::json;

use super::*;

const HEAD: &str = "SELECT * FROM \"t\" WHERE pk = 'a'";
const TOO_LONG: &str = "Member must have length less than or equal to 8192";
const AT_LEAST_ONE_VALUE: &str = "Member must have value greater than or equal to 1";
const ON_FAILURE_SET: &str = "Member must satisfy enum value set: [ALL_OLD, NONE]";
const CAPACITY_SET: &str = "Member must satisfy enum value set: [INDEXES, TOTAL, NONE]";

/// A statement of `length` code points.
fn statement(length: usize) -> String {
    format!("{HEAD}{}", " ".repeat(length - HEAD.len()))
}

fn message(result: Result<(), DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(message)) => message,
        other => panic!("expected a ValidationException, got {other:?}"),
    }
}

fn part(value: &str, path: &str, constraint: &str) -> String {
    format!("Value '{value}' at '{path}' failed to satisfy constraint: {constraint}")
}

fn null(path: &str) -> String {
    format!("Value null at '{path}' failed to satisfy constraint: Member must not be null")
}

/// The refusal that lists `parts`.
fn errors(parts: &[String]) -> String {
    match parts {
        [part] => format!("1 validation error detected: {part}"),
        parts => format!(
            "{} validation errors detected: {}",
            parts.len(),
            parts.join("; ")
        ),
    }
}

fn plain_batch_member() -> String {
    format!(
        "BatchStatementRequest(statement={HEAD}, parameters=null, consistentRead=null, \
         returnValuesOnConditionCheckFailure=null)"
    )
}

fn plain_transaction_member() -> String {
    format!(
        "ParameterizedStatement(statement={HEAD}, parameters=null, \
         returnValuesOnConditionCheckFailure=null)"
    )
}

#[test]
fn a_request_within_its_constraints_passes() {
    let members: Vec<Value> = (0..25).map(|_| json!({ "Statement": HEAD })).collect();
    assert!(check_batch_execute_statement(&json!({ "Statements": members })).is_ok());
    let members: Vec<Value> = (0..100)
        .map(|_| json!({ "Statement": statement(8192), "Parameters": [{"S": "a"}] }))
        .collect();
    let body = json!({ "TransactStatements": members, "ClientRequestToken": "t".repeat(36) });
    assert!(check_execute_transaction(&body).is_ok());
    let body = json!({ "Statement": HEAD, "Limit": 1, "Parameters": [{"N": "1"}] });
    assert!(check_execute_statement(&body).is_ok());
    // A field of another type than the model's is left to the executors.
    assert!(check_execute_statement(&json!({ "Statement": 5 })).is_ok());
    assert!(check_batch_execute_statement(&json!({ "Statements": "x" })).is_ok());
    assert!(check_execute_transaction(&json!({ "TransactStatements": {} })).is_ok());
}

#[test]
fn a_missing_member_is_reported_as_null() {
    assert_eq!(
        message(check_execute_statement(&json!({}))),
        errors(&[null("statement")])
    );
    assert_eq!(
        message(check_execute_statement(&json!({ "Statement": null }))),
        errors(&[null("statement")])
    );
    assert_eq!(
        message(check_batch_execute_statement(&json!({}))),
        errors(&[null("statements")])
    );
    assert_eq!(
        message(check_execute_transaction(&json!({}))),
        errors(&[null("transactStatements")])
    );
    let body = json!({ "Statements": [{ "Parameters": [{"S": "a"}] }] });
    assert_eq!(
        message(check_batch_execute_statement(&body)),
        errors(&[null("statements.1.member.statement")])
    );
    let body = json!({ "TransactStatements": [{ "Parameters": [{"S": "a"}] }] });
    assert_eq!(
        message(check_execute_transaction(&body)),
        errors(&[null("transactStatements.1.member.statement")])
    );
}

#[test]
fn an_enum_value_outside_its_set_is_refused() {
    let capacity = || part("X", "returnConsumedCapacity", CAPACITY_SET);
    let body = json!({ "Statement": HEAD, "ReturnValuesOnConditionCheckFailure": "X" });
    assert_eq!(
        message(check_execute_statement(&body)),
        errors(&[part(
            "X",
            "returnValuesOnConditionCheckFailure",
            ON_FAILURE_SET
        )])
    );
    let body = json!({ "Statement": HEAD, "ReturnConsumedCapacity": "X" });
    assert_eq!(
        message(check_execute_statement(&body)),
        errors(&[capacity()])
    );
    let member = json!({ "Statement": HEAD, "ReturnValuesOnConditionCheckFailure": "X" });
    assert_eq!(
        message(check_batch_execute_statement(
            &json!({ "Statements": [member] })
        )),
        errors(&[part(
            "X",
            "statements.1.member.returnValuesOnConditionCheckFailure",
            ON_FAILURE_SET
        )])
    );
    assert_eq!(
        message(check_execute_transaction(
            &json!({ "TransactStatements": [member] })
        )),
        errors(&[part(
            "X",
            "transactStatements.1.member.returnValuesOnConditionCheckFailure",
            ON_FAILURE_SET
        )])
    );
    let body = json!({ "Statements": [{ "Statement": HEAD }], "ReturnConsumedCapacity": "X" });
    assert_eq!(
        message(check_batch_execute_statement(&body)),
        errors(&[capacity()])
    );
    let body =
        json!({ "TransactStatements": [{ "Statement": HEAD }], "ReturnConsumedCapacity": "X" });
    assert_eq!(
        message(check_execute_transaction(&body)),
        errors(&[capacity()])
    );
}

#[test]
fn every_value_in_an_enum_set_passes() {
    for value in ["ALL_OLD", "NONE"] {
        let member = json!({ "Statement": HEAD, "ReturnValuesOnConditionCheckFailure": value });
        assert!(check_execute_statement(&member).is_ok(), "{value}");
        assert!(check_batch_execute_statement(&json!({ "Statements": [member] })).is_ok());
        assert!(check_execute_transaction(&json!({ "TransactStatements": [member] })).is_ok());
    }
    for value in ["INDEXES", "TOTAL", "NONE"] {
        let body = json!({ "Statement": HEAD, "ReturnConsumedCapacity": value });
        assert!(check_execute_statement(&body).is_ok(), "{value}");
        let body =
            json!({ "Statements": [{ "Statement": HEAD }], "ReturnConsumedCapacity": value });
        assert!(check_batch_execute_statement(&body).is_ok(), "{value}");
        let body = json!({
            "TransactStatements": [{ "Statement": HEAD }],
            "ReturnConsumedCapacity": value
        });
        assert!(check_execute_transaction(&body).is_ok(), "{value}");
    }
}

#[test]
fn execute_statement_reports_its_fields_in_the_measured_order() {
    let over = statement(8193);
    let long_token = "n".repeat(32769);
    for (token, constraint) in [
        ("", AT_LEAST_ONE),
        (
            long_token.as_str(),
            "Member must have length less than or equal to 32768",
        ),
    ] {
        let body = json!({
            "Statement": over,
            "Parameters": [],
            "Limit": 0,
            "NextToken": token,
            "ReturnConsumedCapacity": "X",
            "ReturnValuesOnConditionCheckFailure": "X",
            "ConsistentRead": true
        });
        assert_eq!(
            message(check_execute_statement(&body)),
            errors(&[
                part("X", "returnValuesOnConditionCheckFailure", ON_FAILURE_SET),
                part(token, "nextToken", constraint),
                part("0", "limit", AT_LEAST_ONE_VALUE),
                part(&over, "statement", TOO_LONG),
                part("X", "returnConsumedCapacity", CAPACITY_SET),
                part("[]", "parameters", AT_LEAST_ONE),
            ])
        );
    }
}

#[test]
fn a_token_holds_one_code_point_up_to_its_limit() {
    for token in ["n".to_owned(), "n".repeat(32768), "\u{1F600}".repeat(32768)] {
        let body = json!({ "Statement": HEAD, "NextToken": token });
        assert!(check_execute_statement(&body).is_ok());
    }
    let body = json!({ "Statement": HEAD, "NextToken": "" });
    assert_eq!(
        message(check_execute_statement(&body)),
        errors(&[part("", "nextToken", AT_LEAST_ONE)])
    );
    for token in ["t".to_owned(), "t".repeat(36), "\u{e9}".repeat(36)] {
        let body =
            json!({ "TransactStatements": [{ "Statement": HEAD }], "ClientRequestToken": token });
        assert!(check_execute_transaction(&body).is_ok());
    }
    let body = json!({ "TransactStatements": [{ "Statement": HEAD }], "ClientRequestToken": "" });
    assert_eq!(
        message(check_execute_transaction(&body)),
        errors(&[part("", "clientRequestToken", AT_LEAST_ONE)])
    );
}

#[test]
fn a_batch_reports_its_capacity_then_its_count_then_each_member() {
    let over = statement(8193);
    let mut members = vec![json!({
        "Statement": over,
        "Parameters": [],
        "ConsistentRead": true,
        "ReturnValuesOnConditionCheckFailure": "X"
    })];
    members.extend((0..25).map(|_| json!({ "Statement": HEAD })));
    let echo = format!(
        "[BatchStatementRequest(statement={over}, parameters=[], consistentRead=true, \
         returnValuesOnConditionCheckFailure=X), {}]",
        vec![plain_batch_member(); 25].join(", ")
    );
    let body = json!({ "Statements": members, "ReturnConsumedCapacity": "X" });
    assert_eq!(
        message(check_batch_execute_statement(&body)),
        errors(&[
            part("X", "returnConsumedCapacity", CAPACITY_SET),
            part(
                &echo,
                "statements",
                "Member must have length less than or equal to 25"
            ),
            part(
                "X",
                "statements.1.member.returnValuesOnConditionCheckFailure",
                ON_FAILURE_SET
            ),
            part(&over, "statements.1.member.statement", TOO_LONG),
            part("[]", "statements.1.member.parameters", AT_LEAST_ONE),
        ])
    );
    let body = json!({ "ReturnConsumedCapacity": "X" });
    assert_eq!(
        message(check_batch_execute_statement(&body)),
        errors(&[
            part("X", "returnConsumedCapacity", CAPACITY_SET),
            null("statements")
        ])
    );
}

#[test]
fn a_transaction_reports_its_capacity_after_its_members() {
    let over = statement(8193);
    let mut members = vec![json!({
        "Statement": over,
        "Parameters": [],
        "ReturnValuesOnConditionCheckFailure": "X"
    })];
    members.extend((0..100).map(|_| json!({ "Statement": HEAD })));
    let token = "t".repeat(37);
    let echo = format!(
        "[ParameterizedStatement(statement={over}, parameters=[], \
         returnValuesOnConditionCheckFailure=X), {}]",
        vec![plain_transaction_member(); 100].join(", ")
    );
    let body = json!({
        "TransactStatements": members,
        "ClientRequestToken": token,
        "ReturnConsumedCapacity": "X"
    });
    assert_eq!(
        message(check_execute_transaction(&body)),
        errors(&[
            part(
                &token,
                "clientRequestToken",
                "Member must have length less than or equal to 36"
            ),
            part(
                &echo,
                "transactStatements",
                "Member must have length less than or equal to 100"
            ),
            part(
                "X",
                "transactStatements.1.member.returnValuesOnConditionCheckFailure",
                ON_FAILURE_SET
            ),
            part(&over, "transactStatements.1.member.statement", TOO_LONG),
            part("[]", "transactStatements.1.member.parameters", AT_LEAST_ONE),
            part("X", "returnConsumedCapacity", CAPACITY_SET),
        ])
    );
}

#[test]
fn a_refusal_lists_the_first_ten_failed_constraints() {
    let members: Vec<Value> = (0..25)
        .map(|_| json!({ "Statement": "", "Parameters": [] }))
        .collect();
    let first_ten: Vec<String> = (1..=5)
        .flat_map(|i| {
            [
                part(
                    "",
                    &format!("statements.{i}.member.statement"),
                    AT_LEAST_ONE,
                ),
                part(
                    "[]",
                    &format!("statements.{i}.member.parameters"),
                    AT_LEAST_ONE,
                ),
            ]
        })
        .collect();
    let refusal = message(check_batch_execute_statement(
        &json!({ "Statements": members }),
    ));
    assert_eq!(refusal, errors(&first_ten));
    assert!(refusal.starts_with("10 validation errors detected: "));
    // Five such statements fail ten constraints, all listed.
    let members: Vec<Value> = (0..5)
        .map(|_| json!({ "Statement": "", "Parameters": [] }))
        .collect();
    assert_eq!(
        message(check_batch_execute_statement(
            &json!({ "Statements": members })
        )),
        errors(&first_ten)
    );
}

#[test]
fn a_null_member_joins_the_other_parts() {
    let body = json!({ "Limit": 0, "Parameters": [] });
    assert_eq!(
        message(check_execute_statement(&body)),
        errors(&[
            part("0", "limit", AT_LEAST_ONE_VALUE),
            null("statement"),
            part("[]", "parameters", AT_LEAST_ONE),
        ])
    );
    let body = json!({ "Statements": [{ "Parameters": [] }] });
    assert_eq!(
        message(check_batch_execute_statement(&body)),
        errors(&[
            null("statements.1.member.statement"),
            part("[]", "statements.1.member.parameters", AT_LEAST_ONE),
        ])
    );
    let token = "t".repeat(37);
    assert_eq!(
        message(check_execute_transaction(
            &json!({ "ClientRequestToken": token })
        )),
        errors(&[
            part(
                &token,
                "clientRequestToken",
                "Member must have length less than or equal to 36"
            ),
            null("transactStatements"),
        ])
    );
}

#[test]
fn a_long_statement_is_refused_as_the_parser_words_it() {
    let over = statement(8193);
    assert_eq!(
        message(check_execute_statement(&json!({ "Statement": over }))),
        statement_length_refusal(&over).unwrap()
    );
    assert_eq!(
        constraint_of(&statement_length_refusal(&over).unwrap()),
        TOO_LONG
    );
}

#[test]
fn the_length_counts_code_points() {
    let head = "SELECT * FROM \"t\" WHERE a = '";
    let at_limit = format!("{head}{}'", "\u{1F600}".repeat(8192 - head.len() - 1));
    assert!(check_execute_statement(&json!({ "Statement": at_limit })).is_ok());
    let over = format!("{head}{}'", "\u{1F600}".repeat(8192 - head.len()));
    assert_eq!(
        message(check_execute_statement(&json!({ "Statement": over }))),
        format!(
            "1 validation error detected: {}",
            part(&over, "statement", TOO_LONG)
        )
    );
}

#[test]
fn an_empty_statement_or_parameter_list_is_too_short() {
    assert_eq!(
        message(check_execute_statement(&json!({ "Statement": "" }))),
        format!(
            "1 validation error detected: {}",
            part("", "statement", AT_LEAST_ONE)
        )
    );
    let body = json!({ "Statement": HEAD, "Parameters": [] });
    assert_eq!(
        message(check_execute_statement(&body)),
        format!(
            "1 validation error detected: {}",
            part("[]", "parameters", AT_LEAST_ONE)
        )
    );
    assert_eq!(
        message(check_batch_execute_statement(&json!({ "Statements": [] }))),
        "1 validation error detected: Value '[]' at 'statements' failed to satisfy \
         constraint: Member must have length greater than or equal to 1"
    );
    assert_eq!(
        message(check_execute_transaction(
            &json!({ "TransactStatements": [] })
        )),
        "1 validation error detected: Value '[]' at 'transactStatements' failed to \
         satisfy constraint: Member must have length greater than or equal to 1"
    );
}

#[test]
fn execute_statement_reports_the_limit_then_the_statement_then_the_parameters() {
    let over = statement(8193);
    let body = json!({ "Statement": over, "Limit": 0, "Parameters": [] });
    assert_eq!(
        message(check_execute_statement(&body)),
        format!(
            "3 validation errors detected: {}; {}; {}",
            part(
                "0",
                "limit",
                "Member must have value greater than or equal to 1"
            ),
            part(&over, "statement", TOO_LONG),
            part("[]", "parameters", AT_LEAST_ONE)
        )
    );
    let body = json!({ "Statement": HEAD, "Limit": -3 });
    assert_eq!(
        message(check_execute_statement(&body)),
        format!(
            "1 validation error detected: {}",
            part(
                "-3",
                "limit",
                "Member must have value greater than or equal to 1"
            )
        )
    );
}

#[test]
fn a_batch_reports_each_statement_in_order() {
    let over = statement(8193);
    let body = json!({ "Statements": [
        { "Statement": "" },
        { "Statement": HEAD },
        { "Statement": over, "Parameters": [] },
    ]});
    assert_eq!(
        message(check_batch_execute_statement(&body)),
        format!(
            "3 validation errors detected: {}; {}; {}",
            part("", "statements.1.member.statement", AT_LEAST_ONE),
            part(&over, "statements.3.member.statement", TOO_LONG),
            part("[]", "statements.3.member.parameters", AT_LEAST_ONE)
        )
    );
}

#[test]
fn a_batch_past_its_limit_is_echoed_before_its_statements() {
    let over = statement(8193);
    let mut members: Vec<Value> = (0..25).map(|_| json!({ "Statement": HEAD })).collect();
    members.push(json!({ "Statement": over }));
    let echo = format!(
        "[{}, BatchStatementRequest(statement={over}, parameters=null, consistentRead=null, \
         returnValuesOnConditionCheckFailure=null)]",
        vec![plain_batch_member(); 25].join(", ")
    );
    assert_eq!(
        message(check_batch_execute_statement(
            &json!({ "Statements": members })
        )),
        format!(
            "2 validation errors detected: {}; {}",
            part(
                &echo,
                "statements",
                "Member must have length less than or equal to 25"
            ),
            part(&over, "statements.26.member.statement", TOO_LONG)
        )
    );
}

#[test]
fn a_transaction_reports_the_token_then_the_count_then_each_statement() {
    let over = statement(8193);
    let mut members: Vec<Value> = (0..100).map(|_| json!({ "Statement": HEAD })).collect();
    members.push(json!({ "Statement": over }));
    let token = "t".repeat(37);
    let body = json!({ "TransactStatements": members, "ClientRequestToken": token });
    let echo = format!(
        "[{}, ParameterizedStatement(statement={over}, parameters=null, \
         returnValuesOnConditionCheckFailure=null)]",
        vec![plain_transaction_member(); 100].join(", ")
    );
    assert_eq!(
        message(check_execute_transaction(&body)),
        format!(
            "3 validation errors detected: {}; {}; {}",
            part(
                &token,
                "clientRequestToken",
                "Member must have length less than or equal to 36"
            ),
            part(
                &echo,
                "transactStatements",
                "Member must have length less than or equal to 100"
            ),
            part(&over, "transactStatements.101.member.statement", TOO_LONG)
        )
    );
    let body = json!({ "TransactStatements": [{ "Statement": HEAD, "Parameters": [] }] });
    assert_eq!(
        message(check_execute_transaction(&body)),
        format!(
            "1 validation error detected: {}",
            part("[]", "transactStatements.1.member.parameters", AT_LEAST_ONE)
        )
    );
}

#[test]
fn a_statement_of_whitespace_alone_is_blank() {
    for blank in [
        " ", "   ", "\t\n ", "\r", "\u{b}", "\u{c}", "\u{1f}", "\u{2028}", "\u{3000}",
    ] {
        assert!(is_blank_statement(blank), "{blank:?}");
    }
    for statement in [
        "\u{0}",
        "\u{1}",
        "\u{85}",
        "\u{a0}",
        "-- a note",
        "/* a note */",
        " ;",
        HEAD,
    ] {
        assert!(!is_blank_statement(statement), "{statement:?}");
    }
}
