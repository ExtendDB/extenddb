// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use extenddb_core::expression::Projection;

use super::*;

/// The fault Amazon DynamoDB reports for a `ProjectionExpression` with
/// several: the first in written order, after any syntax error. Measured
/// 2026-11-14 on GetItem, and 2026-11-18 on GetItem, Query, Scan,
/// BatchGetItem and TransactGetItems, which answer alike.
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
    // None marks a syntax error. The service words it differently, so only
    // its kind is compared.
    let rows = [
        ("#u, name", Some(undefined.to_owned())),
        ("a, a.b, name", Some(overlap("[a]", "[a, b]"))),
        ("a, a.b, #u", Some(overlap("[a]", "[a, b]"))),
        ("a.b, c.d, c, a", Some(overlap("[c, d]", "[c]"))),
        ("name, #u", Some(reserved.to_owned())),
        ("a, name, a.b", Some(reserved.to_owned())),
        ("#u.name", Some(undefined.to_owned())),
        ("name.#u", Some(reserved.to_owned())),
        ("name, a..b", None),
        ("a..b, name", None),
        ("a[name]", None),
    ];
    let limits = LimitsConfig::default();
    let prefix = "Invalid ProjectionExpression: ";
    for (expr, fault) in rows {
        let answer = parse_projection_expr(expr, &limits).and_then(|paths| {
            let maps = ExpressionMaps::default();
            Projection::compile(&paths, &maps, true, limits.enforce_reserved_keywords).map(drop)
        });
        let Err(DynamoDbError::ValidationException(m)) = &answer else {
            panic!("{expr}: got {answer:?}");
        };
        let m = m.strip_prefix(prefix).expect(expr);
        assert!(fault.map_or(m != reserved, |f| m == f), "{expr}: {m}");
    }
}
