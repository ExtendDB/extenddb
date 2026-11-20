// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the `Limit` of an `ExecuteStatement` request.

use serde_json::{Value, json};

use super::check_execute_statement;
use crate::partiql::SingleRequest;

/// The refusal of a `Limit` that reads as 0.
const ZERO: &str = "1 validation error detected: Value '0' at 'limit' failed to satisfy \
                    constraint: Member must have value greater than or equal to 1";

/// The refusal of a `Limit` that reads as -1.
const MINUS_ONE: &str = "1 validation error detected: Value '-1' at 'limit' failed to satisfy \
                         constraint: Member must have value greater than or equal to 1";

/// The refusal of a `Limit` that reads as the int32 minimum.
const INT32_MIN: &str = "1 validation error detected: Value '-2147483648' at 'limit' failed to \
                         satisfy constraint: Member must have value greater than or equal to 1";

/// The type of the refusal of a `Limit` of another JSON type.
const SER: &str = "SerializationException";

/// The `Limit` a request runs with, or the type and message of its refusal.
type Answer = Result<Option<i64>, (&'static str, &'static str)>;

/// Each `Limit` as a request writes it, and the request's answer (measured
/// 2026-11-14, 2026-11-18 and 2026-11-20, us-east-1).
const LIMITS: &[(&str, Answer)] = &[
    ("1.0", Ok(Some(1))),
    ("1.5", Ok(Some(1))),
    (
        "\"2\"",
        Err((SER, "STRING_VALUE cannot be converted to Integer")),
    ),
    ("0.5", Err(("ValidationException", ZERO))),
    ("0.9999", Err(("ValidationException", ZERO))),
    ("-0.5", Err(("ValidationException", ZERO))),
    ("-0", Err(("ValidationException", ZERO))),
    ("-1.5", Err(("ValidationException", MINUS_ONE))),
    ("-2147483649", Err(("ValidationException", INT32_MIN))),
    ("-1e20", Err(("ValidationException", INT32_MIN))),
    (
        "-9223372036854775809",
        Err(("ValidationException", INT32_MIN)),
    ),
    (
        "true",
        Err((SER, "TRUE_VALUE cannot be converted to Integer")),
    ),
    (
        "false",
        Err((SER, "FALSE_VALUE cannot be converted to Integer")),
    ),
    ("[1]", Err((SER, ""))),
    (
        "{}",
        Err((SER, "Start of structure or map found where not expected")),
    ),
    ("null", Ok(None)),
    // The service returns every item of a partition of four on one page for
    // these (measured 2026-11-14).
    ("2147483648", Ok(Some(2_147_483_647))),
    ("1E+2", Ok(Some(100))),
    ("1e+20", Ok(Some(2_147_483_647))),
    ("100000000000000000000", Ok(Some(2_147_483_647))),
    ("18446744073709551617", Ok(Some(2_147_483_647))),
    // Accepted (measured 2026-11-20).
    ("9223372036854775808", Ok(Some(2_147_483_647))),
];

#[test]
fn a_limit_reads_from_a_json_number_and_another_type_is_refused() {
    for (limit, expected) in LIMITS {
        let body: Value = serde_json::from_str(&format!(
            r#"{{"Statement": "SELECT * FROM \"t\" WHERE pk = 'k'", "Limit": {limit}}}"#
        ))
        .unwrap();
        // A batch or transaction statement object reads whatever its `Limit` is.
        assert!(SingleRequest::from_body(&body).is_ok(), "{limit}");
        let answer = check_execute_statement(&body)
            .map(|()| SingleRequest::from_body(&body).unwrap().limit)
            .map_err(|e| (e.error_type().to_owned(), e.message().to_owned()));
        let expected = expected.map_err(|(kind, message)| (kind.to_owned(), message.to_owned()));
        assert_eq!(answer, expected, "{limit}");
    }
    // The type of the `Limit` is checked ahead of the missing statement.
    let refusal = check_execute_statement(&json!({ "Limit": "2" })).unwrap_err();
    assert_eq!(
        (refusal.error_type(), refusal.message()),
        (SER, "STRING_VALUE cannot be converted to Integer")
    );
}
