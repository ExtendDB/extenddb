// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request body JSON parsing with a bounded nesting depth.
//!
//! `serde_json`'s deserializer refuses any document nested 128 levels deep or
//! deeper with a recursion error, which the server turned into a
//! `SerializationException`.
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
//! checks a body's JSON syntax before authentication at every depth but refuses
//! an over deep body only after it (measured 2026-11-19, us-east-1: unsigned, a
//! 20000 wrapper PutItem is a `MissingAuthenticationTokenException`, and the
//! same body without its last byte a `SerializationException`). The handler
//! follows that order. It parses a body within [`UNAUTHENTICATED_PARSE_DEPTH`]
//! before authentication. For a deeper body it runs only
//! [`check_request_syntax`], which does not recurse, and parses the body after
//! the caller is authenticated, so no request recurses past `serde_json`'s
//! default depth on an unauthenticated connection.

use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::Value;

use extenddb_core::error::DynamoDbError;

/// Largest structural nesting (`{`/`[` openers on one path) the request parser
/// accepts; `serde_json`'s own default is 128.
///
/// Amazon DynamoDB has no fixed ceiling. A PutItem whose attribute is up to
/// 1500 single-key wrappers (about 3000 structural levels) gets the nesting
/// `ValidationException` on every send that is a 400; from 1700 wrappers the
/// answer varies from request to request between that and a message-less
/// `SerializationException`; 5000 wrappers and deeper are refused on every
/// send. At 1500 wrappers and deeper a few sends are a 500 `InternalFailure`
/// instead (measured 2026-11-12 and 2026-11-13, us-east-1, 24 or more sends
/// per depth across several windows).
/// The service ignores an unknown top-level member: one nested 5201, 6001 or
/// 20000 levels is a 200 on PutItem, DescribeTable and ListTables (measured
/// 2026-11-19, us-east-1). The ceiling, 5200 structural levels or about 2598
/// wrappers, sits inside the band where the service itself varies. An
/// attribute shallow enough for the service to validate it on every send is
/// parsed here, and a body nested past the ceiling gets the message-less
/// refusal the service gives for an attribute in that band. A body with an
/// unknown top-level member nested past the ceiling is refused here, where
/// the service ignores the member.
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
/// authenticated: the 127 levels `serde_json`'s default recursion limit of 128
/// accepts (it refuses the 128th), which the server parsed on a default 2 MiB
/// worker stack before the limit was lifted. A deeper body only goes through
/// [`check_request_syntax`] until authentication succeeds, so an
/// unauthenticated request can never make the handler recurse past this depth.
pub(crate) const UNAUTHENTICATED_PARSE_DEPTH: usize = 127;

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
        return Err(over_ceiling_refusal());
    }
    // The depth ceiling above keeps the disabled recursion limit from letting a
    // deep body overflow the stack.
    let mut de = serde_json::Deserializer::from_slice(body);
    de.disable_recursion_limit();
    let value = Value::deserialize(&mut de).map_err(|e| serialization_error(&e.to_string()))?;
    de.end().map_err(|e| serialization_error(&e.to_string()))?;
    Ok(value)
}

/// Check the JSON syntax of a request body whose [`nesting_depth`] is `depth`
/// without building it, so the handler can refuse a malformed body nested past
/// [`UNAUTHENTICATED_PARSE_DEPTH`] before authentication, as the service does.
/// `serde_json` skips an ignored value in one loop that keeps the kinds of the
/// open containers on a heap stack, so the check does not recurse at any
/// depth.
///
/// The check reads one JSON value, as the service's check before
/// authentication does: bytes after a complete body, an invalid UTF-8 byte in
/// a string and a lone surrogate escape all pass it (measured 2026-11-19,
/// us-east-1: unsigned, each is a `MissingAuthenticationTokenException`). A
/// body that fails it gets a `SerializationException`, with no message past
/// the ceiling, as from [`parse_request_value`], and with the server's usual
/// wording within it.
pub(crate) fn check_request_syntax(body: &[u8], depth: usize) -> Result<(), DynamoDbError> {
    let mut de = serde_json::Deserializer::from_slice(body);
    match IgnoredAny::deserialize(&mut de) {
        Ok(IgnoredAny) => Ok(()),
        Err(_) if depth > MAX_REQUEST_JSON_DEPTH => Err(over_ceiling_refusal()),
        Err(e) => Err(serialization_error(&e.to_string())),
    }
}

/// The refusal for a body nested past [`MAX_REQUEST_JSON_DEPTH`]. A signed body
/// nested past what the service parses gets HTTP 400 with
/// `{"__type":"com.amazon.coral.service#SerializationException"}` and no
/// message field at all (measured 2026-11-12, us-east-1: 24 of 24 sends at 5000
/// and at 20000 wrappers). An empty message makes `error_response` omit the
/// field.
fn over_ceiling_refusal() -> DynamoDbError {
    DynamoDbError::SerializationException(String::new())
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
        for body in [&b"{not json"[..], br#"{"a":1"#] {
            let err = parse(body).unwrap_err();
            assert!(matches!(err, DynamoDbError::SerializationException(_)));
            assert_eq!(err.message(), expected_message(body), "{body:?}");
        }
    }

    #[test]
    fn the_unauthenticated_depth_is_serde_json_default() {
        // The handler parses a body within this depth before authentication, as
        // the server did with the deserializer's default limit on a default
        // stack; a deeper one only has its syntax checked until authentication.
        // Checked against serde_json itself: with its default limit, a body at
        // this depth parses and a body one level deeper is the recursion error.
        let nested = |levels: usize| [b"[".repeat(levels), b"]".repeat(levels)].concat();
        serde_json::from_slice::<Value>(&nested(UNAUTHENTICATED_PARSE_DEPTH))
            .expect("serde_json's default limit must parse a body at the depth");
        let err =
            serde_json::from_slice::<Value>(&nested(UNAUTHENTICATED_PARSE_DEPTH + 1)).unwrap_err();
        assert!(
            err.to_string().contains("recursion limit exceeded"),
            "{err}"
        );
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

    /// `deep_body(wrappers)` without its last byte.
    fn truncated_body(wrappers: usize) -> Vec<u8> {
        let mut body = deep_body(wrappers);
        body.pop();
        body
    }

    /// `deep_body(wrappers)` with its last `}` written as `]`.
    fn wrong_closer_body(wrappers: usize) -> Vec<u8> {
        let mut body = deep_body(wrappers);
        *body.last_mut().unwrap() = b']';
        body
    }

    #[test]
    fn the_syntax_check_refuses_a_malformed_deep_body_as_the_parse_does() {
        // Rows: a body nested past the pre-auth depth with a syntax fault. The
        // service refuses each before authentication with a
        // SerializationException (measured 2026-11-19, us-east-1, unsigned
        // and badly signed). The check must give the refusal that the parse
        // after authentication gives the same body. The parse recurses, so the
        // rows run on a worker-sized stack.
        let rows = [
            [&b"x"[..], &b"[".repeat(6000)].concat(),
            [&b"x"[..], &b"[".repeat(200)].concat(),
            truncated_body(63),
            truncated_body(200),
            truncated_body(2000),
            truncated_body(20000),
            wrong_closer_body(200),
        ];
        std::thread::Builder::new()
            .stack_size(REQUEST_WORKER_STACK_SIZE)
            .spawn(move || {
                for body in &rows {
                    let depth = nesting_depth(body);
                    assert!(depth > UNAUTHENTICATED_PARSE_DEPTH, "{} bytes", body.len());
                    let checked = check_request_syntax(body, depth).unwrap_err();
                    let parsed = parse_request_value(body, depth).unwrap_err();
                    assert!(matches!(checked, DynamoDbError::SerializationException(_)));
                    assert_eq!(checked.message(), parsed.message(), "{} bytes", body.len());
                }
            })
            .unwrap()
            .join()
            .unwrap();
        // Past the ceiling the refusal carries no message, which the service
        // also gives these bodies; within it, the server's usual wording.
        let past = [&b"x"[..], &b"[".repeat(6000)].concat();
        assert!(
            check_request_syntax(&past, nesting_depth(&past))
                .unwrap_err()
                .message()
                .is_empty()
        );
        let within = truncated_body(200);
        assert_eq!(
            check_request_syntax(&within, nesting_depth(&within))
                .unwrap_err()
                .message(),
            format!(
                "Start of structure or map found where not expected: \
                 EOF while parsing an object at line 1 column {}",
                within.len()
            )
        );
    }

    #[test]
    fn the_syntax_check_passes_what_the_service_authenticates_first() {
        // Rows: bodies the service's check before authentication passes
        // (measured 2026-11-19, us-east-1: unsigned, each is a
        // MissingAuthenticationTokenException). Complete bodies far past the
        // ceiling, bytes after a complete body, an invalid UTF-8 byte in a
        // string and a lone surrogate escape. This runs on the default
        // test-thread stack, where a parse that recursed a million levels
        // would overflow, so it also shows that the check does not recurse.
        let leaf = |with: &[u8]| {
            let body = deep_body(200);
            let at = body.windows(6).position(|w| w == b"\"leaf\"").unwrap();
            [&body[..at], with, &body[at + 6..]].concat()
        };
        let rows = [
            [b"[".repeat(1_000_000), b"]".repeat(1_000_000)].concat(),
            deep_body(20000),
            [deep_body(2000), b"x".to_vec()].concat(),
            leaf(b"\"le\xffaf\""),
            leaf(br#""le\ud800af""#),
        ];
        for body in &rows {
            let depth = nesting_depth(body);
            assert!(depth > UNAUTHENTICATED_PARSE_DEPTH, "{} bytes", body.len());
            assert!(
                check_request_syntax(body, depth).is_ok(),
                "{} bytes",
                body.len()
            );
        }
    }
}
