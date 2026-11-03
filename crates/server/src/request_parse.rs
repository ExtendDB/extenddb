// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request body JSON parsing with a bounded nesting depth.
//!
//! `serde_json`'s deserializer refuses any document nested past 128 levels with
//! a recursion error, which the server turned into a `SerializationException`.
//! Amazon DynamoDB instead parses requests nested far past that and answers
//! with the validation that applies (the 32 level document limit, or whichever
//! check runs first), so the parser has to reach those checks rather than
//! refuse the request (measured 2026-11-02: a stored attribute nested 63 levels
//! is a `ValidationException` about the nesting limit, not a serialization
//! error).
//!
//! The recursion limit is therefore disabled, and a byte scan bounds the
//! structural nesting first so a hostile body (a 16 MiB request can nest to
//! millions of levels) cannot overflow the worker stack. The ceiling sits far
//! above the depth any request validation needs and well under the stack a
//! worker is given (see [`MAX_REQUEST_JSON_DEPTH`]).

use serde::Deserialize;
use serde_json::Value;

use extenddb_core::error::DynamoDbError;

/// Largest structural nesting (`{`/`[` openers on one path) the request parser
/// accepts; `serde_json`'s own default is 128.
///
/// Amazon DynamoDB parses and validates a request body nested to at least about
/// 2000 document wrappers (roughly 4000 structural levels; measured 2026-11-02,
/// us-east-1), answering it with the nesting `ValidationException`, and only
/// refuses a still deeper body with a serialization error. The ceiling sits
/// above that accepted range: 5200 structural levels is about 2600 single-key
/// wrappers, so every body the service validates is parsed here too.
///
/// A body this deep recurses the same number of levels three times over: the
/// `serde_json` parse, the conversion to typed `AttributeValue`s (which has no
/// recursion limit of its own), and the recursive drop of both. Measured
/// 2026-11-02 (debug): an 8 MiB stack handles about 1500 wrappers and overflows
/// past that, while a 32 MiB stack handles a body at this ceiling and well
/// beyond it (a 5500 wrapper body still completes). `extenddb-app`'s
/// `cmd_serve` gives each worker 32 MiB for that reason, over twice the
/// ceiling's need. A body nested past the ceiling is refused by the byte scan
/// below before any recursive work, so the stack is bounded whatever the depth.
const MAX_REQUEST_JSON_DEPTH: usize = 5200;

/// Whether `body` nests past [`MAX_REQUEST_JSON_DEPTH`], counting `{` and `[`
/// outside string literals and stopping at the first level over the ceiling.
/// The scan never recurses, so it is safe at any depth.
fn exceeds_max_depth(body: &[u8]) -> bool {
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escaped = false;
    for &b in body {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > MAX_REQUEST_JSON_DEPTH {
                    return true;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    false
}

/// Parse a request body into a JSON [`Value`], matching
/// `serde_json::from_slice` for everything within the depth ceiling and
/// refusing a body nested past it.
///
/// A malformed or over deep body is a `SerializationException`, the error the
/// server has always returned for a body it cannot read.
pub(crate) fn parse_request_value(body: &[u8]) -> Result<Value, DynamoDbError> {
    if exceeds_max_depth(body) {
        // A body nested past what it will parse gets HTTP 400 with
        // `{"__type":"com.amazon.coral.service#SerializationException"}` and no
        // message field at all (measured 2026-11-02, us-east-1). An empty
        // message makes `error_response` omit the field, matching that exactly.
        return Err(DynamoDbError::SerializationException(String::new()));
    }
    // The depth ceiling above keeps the disabled recursion limit from letting a
    // deep body overflow the stack.
    let mut de = serde_json::Deserializer::from_slice(body);
    de.disable_recursion_limit();
    let value = Value::deserialize(&mut de).map_err(|e| serialization_error(&e.to_string()))?;
    de.end().map_err(|e| serialization_error(&e.to_string()))?;
    Ok(value)
}

/// The server's long standing wording for a request body it cannot read.
fn serialization_error(detail: &str) -> DynamoDbError {
    DynamoDbError::SerializationException(format!(
        "Start of structure or map found where not expected: {detail}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A PutItem-shaped body whose `data` attribute is `wrappers` single-key
    /// maps around a string leaf: `{"M":{"n": ... {"S":"leaf"} ... }}`.
    fn deep_body(wrappers: usize) -> Vec<u8> {
        let mut s = String::from(r#"{"TableName":"t","Item":{"data":"#);
        for _ in 0..wrappers {
            s.push_str(r#"{"M":{"n":"#);
        }
        s.push_str(r#"{"S":"leaf"}"#);
        for _ in 0..wrappers {
            s.push_str("}}");
        }
        s.push_str("}}");
        s.into_bytes()
    }

    #[test]
    fn parses_a_body_nested_past_serde_default() {
        // 200 wrappers is ~400 structural levels, well past serde_json's 128
        // limit and under the ceiling, so it must parse rather than refuse.
        let body = deep_body(200);
        let value = parse_request_value(&body).expect("200 wrappers must parse");
        assert!(value.get("Item").is_some());
    }

    #[test]
    fn refuses_a_body_past_the_ceiling_with_an_empty_message() {
        // Past the ceiling. The byte scan refuses it before any recursion, so
        // this is safe on the default test-thread stack. The refusal is a
        // SerializationException with no message, so `error_response` omits the
        // message field and matches the service's answer for such a body.
        let body = deep_body(MAX_REQUEST_JSON_DEPTH + 50);
        let err = parse_request_value(&body).unwrap_err();
        assert!(matches!(err, DynamoDbError::SerializationException(_)));
        assert!(
            err.message().is_empty(),
            "an over-ceiling body must carry no message, got {:?}",
            err.message()
        );
    }

    #[test]
    fn the_depth_scan_brackets_the_ceiling() {
        // Scan only (no parse), so this is safe at any depth on the test stack.
        assert!(
            !exceeds_max_depth(&deep_body(100)),
            "100 wrappers is under the ceiling"
        );
        assert!(
            exceeds_max_depth(&deep_body(MAX_REQUEST_JSON_DEPTH)),
            "far past the ceiling"
        );
    }

    #[test]
    fn matches_serde_for_a_shallow_body() {
        let body = br#"{"TableName":"t","Item":{"pk":{"S":"a"}}}"#;
        let value = parse_request_value(body).expect("shallow body must parse");
        assert_eq!(value["TableName"], "t");
    }

    #[test]
    fn braces_inside_a_string_do_not_count_as_nesting() {
        let inner = "{".repeat(MAX_REQUEST_JSON_DEPTH + 100);
        let body = format!(r#"{{"TableName":"{inner}"}}"#).into_bytes();
        let value = parse_request_value(&body).expect("braces in a string are not nesting");
        assert!(value.get("TableName").is_some());
    }

    #[test]
    fn an_escaped_quote_does_not_end_the_string_scan() {
        // The escaped quote keeps the scan inside the string, so the braces that
        // follow it are string content and are not counted.
        let body = br#"{"k":"a\"{{{{{","x":1}"#;
        let value = parse_request_value(body).expect("must parse");
        assert_eq!(value["x"], 1);
    }

    #[test]
    fn a_malformed_body_is_a_serialization_error() {
        let err = parse_request_value(b"{not json").unwrap_err();
        assert!(matches!(err, DynamoDbError::SerializationException(_)));
    }

    #[test]
    fn trailing_bytes_are_rejected_like_from_slice() {
        let err = parse_request_value(br#"{"a":1} tail"#).unwrap_err();
        assert!(matches!(err, DynamoDbError::SerializationException(_)));
    }

    #[test]
    fn the_ceiling_admits_the_required_depth() {
        // The byte scan admits the depth the service validates (2000 wrappers,
        // ~4003 structural) and the deepest the ceiling allows (~2598), and
        // refuses one wrapper more. The scan never recurses, so this is safe at
        // any depth on the test stack.
        assert!(
            !exceeds_max_depth(&deep_body(2000)),
            "2000 wrappers must be admitted"
        );
        assert!(
            !exceeds_max_depth(&deep_body(2598)),
            "the ceiling admits about 2598 wrappers"
        );
        assert!(
            exceeds_max_depth(&deep_body(2599)),
            "one wrapper past the ceiling is refused"
        );
    }

    #[test]
    fn parses_the_required_depth_on_a_worker_sized_stack() {
        // 2000 wrappers (~4003 structural) is within the ceiling and must parse
        // into a Value. Parsing this deep recurses past the default test-thread
        // stack, so run it on a stack the size `cmd_serve` gives each worker.
        let body = deep_body(2000);
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                let value = parse_request_value(&body).expect("2000 wrappers must parse");
                assert!(value.get("Item").is_some());
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
