// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query of a table that does not exist. Only the size check and the
//! tokenizer run ahead of the table lookup: `_g`, an empty key condition and
//! a key condition over 4096 bytes get Amazon `DynamoDB`'s answers (measured
//! 2026-11-18 and 2026-11-25). The service reads `size` as a name, so
//! `size = :v` gets the missing-table answer. The service also refuses a
//! reserved word such as `status` ahead of the lookup (measured 2026-11-19),
//! and the tokenizer does not check for it.

use serde_json::json;

use crate::partiql::test_context::context;

#[tokio::test]
async fn only_the_size_check_and_the_tokenizer_answer_ahead_of_the_table_lookup() {
    let ctx = context(None).await;
    let refused = |message| ("ValidationException", message);
    let too_big = refused(
        "Invalid KeyConditionExpression: Expression size has exceeded the maximum allowed size;",
    );
    let missing = ("ResourceNotFoundException", "Requested resource not found");
    let v = json!({":v": {"S": "k"}});
    // `parentheses` is 8207 bytes and 8203 tokens long. It gets the size
    // error, not the token limit error.
    let parentheses = format!("{}pk = :v{}", "(".repeat(4100), ")".repeat(4100));
    let spaces = format!("pk = :v{}", " ".repeat(4100));
    let long_name = format!("{} = :v", "a".repeat(4100));
    let rows = [
        (
            json!({"IndexName": "g-keys", "KeyConditionExpression": "_g = :v",
                   "ExpressionAttributeValues": v}),
            refused("Invalid KeyConditionExpression: Syntax error; token: \"_\", near: \"_g\""),
        ),
        (
            json!({"KeyConditionExpression": ""}),
            refused("Invalid KeyConditionExpression: The expression can not be empty;"),
        ),
        (
            json!({"KeyConditionExpression": parentheses, "ExpressionAttributeValues": v}),
            too_big,
        ),
        (
            json!({"KeyConditionExpression": spaces, "ExpressionAttributeValues": v}),
            too_big,
        ),
        (
            json!({"KeyConditionExpression": long_name, "ExpressionAttributeValues": v}),
            too_big,
        ),
        (
            json!({"KeyConditionExpression": "size = :v", "ExpressionAttributeValues": v}),
            missing,
        ),
    ];
    for (mut body, expected) in rows {
        body["TableName"] = json!("t");
        let shown = body.to_string();
        let Err(err) = super::handle_query(body, &ctx).await else {
            panic!("{shown}: served");
        };
        assert_eq!((err.error_type(), err.message()), expected, "{shown}");
    }
}
