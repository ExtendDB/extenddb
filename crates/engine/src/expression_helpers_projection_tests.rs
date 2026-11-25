// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use extenddb_core::expression::Projection;

use super::*;

/// The fault Amazon `DynamoDB` reports for a `ProjectionExpression` with
/// several: the first in written order, after any syntax error. Measured in
/// us-east-1. Each read answered as `GetItem` did:
///
/// - the first four rows: `GetItem` on 2026-11-14, and `Query`, `Scan`,
///   `BatchGetItem` and `TransactGetItems` on 2026-11-18;
/// - `#u.name`: `GetItem` and `Query` on 2026-11-18, and `Scan` and
///   `TransactGetItems` on 2026-11-24;
/// - `name.#u`: `GetItem` and `Query` on 2026-11-18, and `Scan`,
///   `BatchGetItem` and `TransactGetItems` on 2026-11-24;
/// - `a..b, name`: `GetItem` on 2026-11-18 and `TransactGetItems` on
///   2026-11-24;
/// - the other rows: `GetItem` on 2026-11-18.
#[test]
fn projection_reports_the_first_fault_in_written_order() {
    let undefined = "An expression attribute name used in the document path is not \
                     defined; attribute name: #u";
    let reserved = "Attribute name is a reserved keyword; reserved keyword: name";
    let overlap = |one: &str, two: &str| {
        format!(
            "Two document paths overlap with each other; must remove or rewrite \
             one of these paths; path one: {one}, path two: {two}"
        )
    };
    let levels = "The document path has too many nesting levels; nesting levels: 33";
    // ExtendDB's own texts for a syntax error. The service words them
    // differently (#839).
    let after_dot = "expected attribute name after '.'";
    let index = "expected numeric index in brackets";
    let long = ["p"; 33].join(".");
    let rows: [(String, String); 14] = [
        ("#u, name".into(), undefined.into()),
        ("a, a.b, name".into(), overlap("[a]", "[a, b]")),
        ("a, a.b, #u".into(), overlap("[a]", "[a, b]")),
        ("a.b, c.d, c, a".into(), overlap("[c, d]", "[c]")),
        ("name, #u".into(), reserved.into()),
        ("a, name, a.b".into(), reserved.into()),
        ("#u.name".into(), undefined.into()),
        ("name.#u".into(), reserved.into()),
        ("name, a..b".into(), after_dot.into()),
        ("a..b, name".into(), after_dot.into()),
        ("a[name]".into(), index.into()),
        (format!("{long}, name"), levels.into()),
        (format!("name, {long}"), reserved.into()),
        (format!("{long}.name"), reserved.into()),
    ];
    let limits = LimitsConfig::default();
    let prefix = "Invalid ProjectionExpression: ";
    for (expr, fault) in rows {
        let answer = parse_projection_expr(&expr, &limits).and_then(|paths| {
            let maps = ExpressionMaps::default();
            Projection::compile(&paths, &maps, true, limits.enforce_reserved_keywords).map(drop)
        });
        let Err(DynamoDbError::ValidationException(m)) = &answer else {
            panic!("{expr}: got {answer:?}");
        };
        assert_eq!(*m, format!("{prefix}{fault}"), "{expr}");
    }
}
