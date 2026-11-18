// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query of a table that does not exist. Only the tokenizer runs ahead of the
//! table lookup: `_g` and an empty key condition get Amazon `DynamoDB`'s
//! answers (measured 2026-11-18). The service takes `size` as a name and
//! answers that the table is missing, which shows that the reserved word check
//! does not run here.

use serde_json::json;

use crate::partiql::test_context::context;

#[tokio::test]
async fn only_the_tokenizer_answers_ahead_of_the_table_lookup() {
    let ctx = context(None).await;
    let refused = |message| ("ValidationException", message);
    let missing = ("ResourceNotFoundException", "Requested resource not found");
    let v = json!({":v": {"S": "k"}});
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
