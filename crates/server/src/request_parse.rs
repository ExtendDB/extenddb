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
//! The recursion limit is therefore disabled, and a byte scan ([`nesting_depth`])
//! bounds the structural nesting first so a hostile body (a 16 MiB request can
//! nest to millions of levels) cannot overflow the worker stack. The service
//! checks a body's JSON syntax before authentication but refuses an over deep
//! body only after it (measured 2026-11-12, us-east-1: unsigned, a 20000
//! wrapper PutItem is a `MissingAuthenticationTokenException`; signed, a
//! `SerializationException`). The handler follows that order: it parses a body
//! within [`UNAUTHENTICATED_PARSE_DEPTH`] before authentication and a deeper
//! one only after the caller is authenticated, so no request recurses past
//! `serde_json`'s default depth on an unauthenticated connection.

use serde::Deserialize;
use serde_json::Value;

use extenddb_core::error::DynamoDbError;

/// Largest structural nesting (`{`/`[` openers on one path) the request parser
/// accepts; `serde_json`'s own default is 128.
///
/// Amazon DynamoDB has no fixed ceiling. A PutItem whose attribute is up to
/// about 1700 single-key wrappers (about 3400 structural levels) gets the
/// nesting `ValidationException` on every send; from about 1800 wrappers the
/// answer varies from request to request between that and a message-less
/// `SerializationException`, with 4000 wrappers also drawing a 500
/// `InternalFailure`; 5000 wrappers and deeper are refused on every send
/// (measured 2026-11-12, us-east-1, 24 sends per depth across four windows).
/// An unknown top-level member is ignored at any depth (20000 levels is a 200).
/// The ceiling, 5200 structural levels or about 2598 wrappers, sits inside the
/// band where the service itself varies: every depth the service answers
/// consistently is parsed here, and a deeper body gets the message-less
/// refusal the service gives in that band.
///
/// A body this deep recurses the same number of levels three times over: the
/// `serde_json` parse, the conversion to typed `AttributeValue`s (which has no
/// recursion limit of its own), and the recursive drop of both. Measured
/// 2026-11-02 (debug): an 8 MiB stack handles about 1500 wrappers and overflows
/// past that, while a 32 MiB stack handles a body at this ceiling and well
/// beyond it (a 5500 wrapper body still completes). The thread that handles an
/// authenticated request therefore needs [`REQUEST_WORKER_STACK_SIZE`] of
/// stack. A body nested past the ceiling is refused by the byte scan before any
/// recursive work, so the stack is bounded whatever the depth.
const MAX_REQUEST_JSON_DEPTH: usize = 5200;

/// Deepest structural nesting the handler parses before the caller is
/// authenticated: `serde_json`'s default recursion limit, which the server
/// parsed on a default 2 MiB worker stack before the limit was lifted. A body
/// nested deeper is only scanned until authentication succeeds, so an
/// unauthenticated request can never make the handler recurse past this depth.
pub(crate) const UNAUTHENTICATED_PARSE_DEPTH: usize = 128;

/// Stack each thread that handles requests needs, in bytes.
///
/// An authenticated body at [`MAX_REQUEST_JSON_DEPTH`] is parsed, converted to
/// typed attribute values and dropped on the handling thread, each step
/// recursing to the body's depth. In a debug build that takes about 13 MiB of
/// stack (measured 2026-11-02), so 32 MiB is about twice the need. On Tokio's
/// default 2 MiB the parse alone overflows well inside the ceiling (measured
/// 2026-11-12, debug: 700 wrappers abort the process). `extenddb-app`'s
/// `cmd_serve` sizes its runtime's threads with this; any other host of
/// [`crate::serve`] or [`crate::start_server`] must do the same.
pub const REQUEST_WORKER_STACK_SIZE: usize = 32 * 1024 * 1024;

/// The deepest structural nesting in `body`: `{` and `[` openers on one path,
/// counted outside string literals (an escaped quote does not end a literal).
/// Counting stops one level past [`MAX_REQUEST_JSON_DEPTH`], so the result
/// saturates at `MAX_REQUEST_JSON_DEPTH + 1` for a deeper body. The scan never
/// recurses, so it is safe at any depth.
pub(crate) fn nesting_depth(body: &[u8]) -> usize {
    let mut depth: usize = 0;
    let mut deepest: usize = 0;
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
                    return depth;
                }
                deepest = deepest.max(depth);
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    deepest
}

/// Parse a request body whose [`nesting_depth`] is `depth` into a JSON
/// [`Value`], matching `serde_json::from_slice` for everything within the depth
/// ceiling and refusing a body nested past it.
///
/// A malformed or over deep body is a `SerializationException`, the error the
/// server has always returned for a body it cannot read.
pub(crate) fn parse_request_value(body: &[u8], depth: usize) -> Result<Value, DynamoDbError> {
    if depth > MAX_REQUEST_JSON_DEPTH {
        // A signed body nested past what the service parses gets HTTP 400 with
        // `{"__type":"com.amazon.coral.service#SerializationException"}` and no
        // message field at all (measured 2026-11-12, us-east-1: 24 of 24 sends
        // at 5000 and at 20000 wrappers). An empty message makes
        // `error_response` omit the field.
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
    /// maps around a string leaf: `{"M":{"n": ... {"S":"leaf"} ... }}`. Its
    /// structural depth is `3 + 2 * wrappers`.
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

    fn parse(body: &[u8]) -> Result<Value, DynamoDbError> {
        parse_request_value(body, nesting_depth(body))
    }

    /// The message a malformed body carries: the server's wording around the
    /// deserializer's own description of the fault.
    fn expected_message(body: &[u8]) -> String {
        let detail = serde_json::from_slice::<Value>(body)
            .unwrap_err()
            .to_string();
        format!("Start of structure or map found where not expected: {detail}")
    }

    #[test]
    fn parses_a_body_nested_past_serde_default() {
        // 200 wrappers is ~400 structural levels, well past serde_json's 128
        // limit and under the ceiling, so it must parse rather than refuse.
        let body = deep_body(200);
        let value = parse(&body).expect("200 wrappers must parse");
        assert!(value.get("Item").is_some());
    }

    #[test]
    fn refuses_a_body_past_the_ceiling_with_an_empty_message() {
        // Past the ceiling. The byte scan refuses it before any recursion, so
        // this is safe on the default test-thread stack. The refusal is a
        // SerializationException with no message, so `error_response` omits the
        // message field and matches the service's answer for such a body.
        let body = deep_body(MAX_REQUEST_JSON_DEPTH + 50);
        let err = parse(&body).unwrap_err();
        assert!(matches!(err, DynamoDbError::SerializationException(_)));
        assert!(
            err.message().is_empty(),
            "an over-ceiling body must carry no message, got {:?}",
            err.message()
        );
    }

    #[test]
    fn the_depth_scan_brackets_the_ceiling_exactly() {
        // Scan only (no parse), so this is safe at any depth on the test stack.
        // Rows: body, the depth the scan must report. The scan saturates one
        // past the ceiling for a deeper body.
        let rows: [(Vec<u8>, usize); 5] = [
            (deep_body(100), 203),
            (deep_body(2000), 4003),
            (b"[".repeat(MAX_REQUEST_JSON_DEPTH), MAX_REQUEST_JSON_DEPTH),
            (
                b"[".repeat(MAX_REQUEST_JSON_DEPTH + 1),
                MAX_REQUEST_JSON_DEPTH + 1,
            ),
            (
                deep_body(MAX_REQUEST_JSON_DEPTH),
                MAX_REQUEST_JSON_DEPTH + 1,
            ),
        ];
        for (body, depth) in &rows {
            assert_eq!(nesting_depth(body), *depth, "{} bytes", body.len());
        }
        assert!(nesting_depth(&rows[2].0) <= MAX_REQUEST_JSON_DEPTH);
        assert!(nesting_depth(&rows[3].0) > MAX_REQUEST_JSON_DEPTH);
    }

    #[test]
    fn matches_serde_for_a_shallow_body() {
        let body = br#"{"TableName":"t","Item":{"pk":{"S":"a"}}}"#;
        let value = parse(body).expect("shallow body must parse");
        assert_eq!(value["TableName"], "t");
    }

    #[test]
    fn braces_inside_a_string_do_not_count_as_nesting() {
        let inner = "{".repeat(MAX_REQUEST_JSON_DEPTH + 100);
        let body = format!(r#"{{"TableName":"{inner}"}}"#).into_bytes();
        let value = parse(&body).expect("braces in a string are not nesting");
        assert!(value.get("TableName").is_some());
    }

    #[test]
    fn an_escaped_quote_does_not_end_the_string_scan() {
        // The escaped quote keeps the scan inside the string, so the braces that
        // follow it are string content and are not counted.
        let body = br#"{"k":"a\"{{{{{","x":1}"#;
        let value = parse(body).expect("must parse");
        assert_eq!(value["x"], 1);
        // Read the other way (the escaped quote taken as the end of the
        // string), every later quote flips the scan's notion of being inside a
        // string, and the over-ceiling attribute that follows is miscounted as
        // string content. The scan must count it and refuse the body; a
        // disabled-limit parse of it would overflow the handling thread.
        let openers = "[".repeat(MAX_REQUEST_JSON_DEPTH + 1);
        let closers = "]".repeat(MAX_REQUEST_JSON_DEPTH + 1);
        let body = format!(r#"{{"k":"a\"b","x":{openers}{closers}}}"#).into_bytes();
        assert!(
            nesting_depth(&body) > MAX_REQUEST_JSON_DEPTH,
            "the over-ceiling attribute behind an escaped quote must be counted"
        );
        assert!(parse(&body).unwrap_err().message().is_empty());
    }

    #[test]
    fn many_shallow_siblings_are_not_nesting() {
        // 3000 single-map attributes at depth 5 open more braces in total than
        // the ceiling; the scan must count depth, not openers, and the body
        // must parse. The service stores such an item (measured 2026-11-12).
        let mut s = String::from(r#"{"TableName":"t","Item":{"pk":{"S":"k"}"#);
        for i in 0..3000 {
            s.push_str(&format!(r#","a{i}":{{"M":{{"x":{{"S":"v"}}}}}}"#));
        }
        s.push_str("}}");
        let body = s.into_bytes();
        assert_eq!(nesting_depth(&body), 5);
        let value = parse(&body).expect("3000 shallow siblings must parse");
        assert_eq!(
            value["Item"].as_object().map(serde_json::Map::len),
            Some(3001)
        );
    }

    #[test]
    fn a_malformed_body_is_a_serialization_error_with_the_usual_message() {
        // Rows: the malformed body. Each gets the server's long standing
        // wording around the deserializer's own description, so a change to
        // `serialization_error`'s format fails here.
        for body in [&b"{not json"[..], br#"{"a":1} tail"#] {
            let err = parse(body).unwrap_err();
            assert!(matches!(err, DynamoDbError::SerializationException(_)));
            assert_eq!(err.message(), expected_message(body), "{body:?}");
        }
    }

    #[test]
    fn the_unauthenticated_depth_is_serde_json_default() {
        // The handler parses a body within this depth before authentication, as
        // the server did with the deserializer's default limit on a default
        // stack; a deeper one waits for authentication.
        assert_eq!(UNAUTHENTICATED_PARSE_DEPTH, 128);
        assert!(nesting_depth(&deep_body(62)) <= UNAUTHENTICATED_PARSE_DEPTH);
        assert!(nesting_depth(&deep_body(63)) > UNAUTHENTICATED_PARSE_DEPTH);
    }

    #[test]
    fn parses_the_required_depth_on_a_worker_sized_stack() {
        // 2000 wrappers (~4003 structural) is within the ceiling and must parse
        // into a Value. Parsing this deep recurses past the default test-thread
        // stack, so run it on the stack a request handling thread must have.
        let body = deep_body(2000);
        std::thread::Builder::new()
            .stack_size(REQUEST_WORKER_STACK_SIZE)
            .spawn(move || {
                let value = parse(&body).expect("2000 wrappers must parse");
                assert!(value.get("Item").is_some());
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
