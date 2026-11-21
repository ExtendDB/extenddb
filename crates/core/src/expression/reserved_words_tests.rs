// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use super::validate_no_reserved_words;
use crate::error::DynamoDbError;
use crate::expression::ExpressionKind::{
    self, Condition, Filter, KeyCondition, Projection, Update,
};
use crate::expression::{
    parse_condition, parse_key_condition, parse_projection, parse_update, tokenize,
    validate_search_condition_expression,
};
use crate::types::AttributeValue;

/// Run the reserved word check on `expr`, then the parser of `kind`. The
/// answer is what the parser read, written with `{:?}`, or the message of the
/// `ValidationException`.
fn read(kind: ExpressionKind, expr: &str) -> String {
    let parsed = tokenize(expr).and_then(|tokens| {
        validate_no_reserved_words(&tokens)?;
        Ok(match kind {
            Projection => format!("{:?}", parse_projection(&tokens)?),
            Update => format!("{:?}", parse_update(&tokens)?),
            KeyCondition => format!("{:?}", parse_key_condition(&tokens)?.pk_path),
            Condition | Filter => format!("{:?}", parse_condition(&tokens)?),
        })
    });
    match parsed {
        Ok(read) => read,
        Err(DynamoDbError::ValidationException(msg)) => msg,
        Err(other) => panic!("{kind} {expr}: {other:?}"),
    }
}

/// The expressions of requests sent to Amazon DynamoDB in us-east-1 on
/// 2026-11-14, 2026-11-16 and 2026-11-18. The service read `size` with no
/// `(` after it as an attribute name in each.
#[test]
fn size_with_no_paren_after_it_is_an_attribute_name() {
    let compare = r#"Compare { left: Path([Attribute("size")]), op: Eq, right: Placeholder("v") }"#;
    let rows = [
        (
            Projection,
            "size, a.SIZE",
            r#"[[Attribute("size")], [Attribute("a"), Attribute("SIZE")]]"#,
        ),
        (
            Projection,
            "size.x, b",
            r#"[[Attribute("size"), Attribute("x")], [Attribute("b")]]"#,
        ),
        (
            Update,
            "SET size = :n",
            r#"[Set { path: [Attribute("size")], value: Placeholder("n") }]"#,
        ),
        (
            Update,
            "SET a = size",
            r#"[Set { path: [Attribute("a")], value: Path([Attribute("size")]) }]"#,
        ),
        (
            Update,
            "REMOVE size",
            r#"[Remove { path: [Attribute("size")] }]"#,
        ),
        (KeyCondition, "size = :v", r#"[Attribute("size")]"#),
        (Condition, "size = :v", compare),
        (
            Condition,
            "attribute_exists(size)",
            r#"Function { name: "attribute_exists", args: [Path([Attribute("size")])] }"#,
        ),
    ];
    for (kind, expr, want) in rows {
        assert_eq!(read(kind, expr), want, "{kind} {expr}");
    }
}

#[test]
fn size_stays_reserved_in_a_search_condition() {
    let values = HashMap::from([(":v".to_owned(), AttributeValue::S("k".to_owned()))]);
    let err = validate_search_condition_expression("size = :v", None, Some(&values));
    assert!(
        matches!(&err, Err(DynamoDbError::ValidationException(msg)) if msg
            == "Invalid SearchConditionExpression: Attribute name is a reserved keyword; \
                reserved keyword: size"),
        "got {err:?}"
    );
}
