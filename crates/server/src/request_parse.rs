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
pub(crate) fn parse_request_value(body: &[u8], depth: usize) -> Result<Value, DynamoDbError> {
    if depth > MAX_REQUEST_JSON_DEPTH {
        return Err(serialization_refusal());
    }
    // The depth ceiling above keeps the disabled recursion limit from letting a
    // deep body overflow the stack.
    let mut de = serde_json::Deserializer::from_slice(body);
    de.disable_recursion_limit();
    let value = Value::deserialize(&mut de).map_err(|_| serialization_refusal())?;
    de.end().map_err(|_| serialization_refusal())?;
    Ok(value)
}

/// Check the JSON syntax of a request body without building it, so the handler
/// can refuse a malformed body nested past [`UNAUTHENTICATED_PARSE_DEPTH`]
/// before authentication, as the service does.
/// [`crate::request_syntax::skip_value`] reads the body's first value in one
/// loop that keeps the closers of the open containers on a heap stack, so the
/// check does not recurse at any depth.
///
/// The check reads one JSON value with the grammar
/// [`crate::request_syntax::skip_value`] describes.
/// Bytes after a complete body, an invalid UTF-8 byte in a string and a lone
/// surrogate escape pass it (measured 2026-11-19, us-east-1: unsigned, each is
/// a `MissingAuthenticationTokenException`). So do the numbers `01`, `1.`,
/// `1e` and `-`, the escape `\u12G4`, and NUL, VT and FF between tokens
/// (measured 2026-11-24, us-east-1, at 63 and 20000 wrappers: unsigned, each
/// is a `MissingAuthenticationTokenException`, and badly signed an
/// `InvalidSignatureException`). A body that fails it gets the message-less
/// `SerializationException` of [`serialization_refusal`], as from
/// [`parse_request_value`].
pub(crate) fn check_request_syntax(body: &[u8]) -> Result<(), DynamoDbError> {
    crate::request_syntax::skip_value(body).map_err(|_| serialization_refusal())
}

/// The refusal for a body with a JSON syntax fault or nested past
/// [`MAX_REQUEST_JSON_DEPTH`]. The service answers a signed body with a syntax
/// fault, and a signed body nested past what it parses, with HTTP 400 and
/// `{"__type":"com.amazon.coral.service#SerializationException"}` and no
/// message field at all (measured in us-east-1: 24 of 24 sends at 5000 and at
/// 20000 wrappers on 2026-11-12, and every signed send of an empty body, a body
/// cut short, a trailing comma, a bad literal or escape, or a raw control byte
/// in a string on 2026-11-19 and 2026-11-20). An empty message makes
/// `error_response` omit the field.
fn serialization_refusal() -> DynamoDbError {
    DynamoDbError::SerializationException(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A PutItem-shaped body whose `data` attribute is `wrappers` single-key
    /// maps around a string leaf: `{"M":{"n": ... {"S":"leaf"} ... }}`. Its
    /// structural depth is `3 + 2 * wrappers`.
    fn deep_body(wrappers: usize) -> Vec<u8> {
        deep_body_with_leaf(wrappers, br#"{"S":"leaf"}"#)
    }

    /// [`deep_body`] with `leaf` in place of `{"S":"leaf"}`.
    fn deep_body_with_leaf(wrappers: usize, leaf: &[u8]) -> Vec<u8> {
        let mut body = br#"{"TableName":"t","Item":{"data":"#.to_vec();
        for _ in 0..wrappers {
            body.extend_from_slice(br#"{"M":{"n":"#);
        }
        body.extend_from_slice(leaf);
        for _ in 0..wrappers {
            body.extend_from_slice(b"}}");
        }
        body.extend_from_slice(b"}}");
        body
    }

    /// The leaves the service's check before authentication passes where
    /// `serde_json`'s grammar refuses them (measured 2026-11-24, us-east-1, at
    /// 63 and 20000 wrappers: unsigned, each is a
    /// `MissingAuthenticationTokenException`, and badly signed an
    /// `InvalidSignatureException`): the numbers `01`, `1.`, `1e` and `-`, the
    /// escape `\u12G4`, and FF, NUL and VT before a value.
    const LOOSE_LEAVES: [&[u8]; 8] = [
        br#"{"S":01}"#,
        br#"{"S":1.}"#,
        br#"{"S":1e}"#,
        br#"{"S":-}"#,
        br#"{"S":"le\u12G4af"}"#,
        b"{\"S\":\x0c\"leaf\"}",
        b"{\"S\":\x00\"leaf\"}",
        b"{\"S\":\x0b\"leaf\"}",
    ];

    fn parse(body: &[u8]) -> Result<Value, DynamoDbError> {
        parse_request_value(body, nesting_depth(body))
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
        // Rows: body, the depth the scan must report, whether that depth is
        // within the ceiling. The depths are literals, so a ceiling other than
        // 5200 fails a row. The scan saturates one past the ceiling for a
        // deeper body.
        let rows: [(Vec<u8>, usize, bool); 5] = [
            (deep_body(100), 203, true),
            (deep_body(2000), 4003, true),
            (b"[".repeat(5200), 5200, true),
            (b"[".repeat(5201), 5201, false),
            (deep_body(5200), 5201, false),
        ];
        for (body, depth, within) in &rows {
            let scanned = nesting_depth(body);
            assert_eq!(scanned, *depth, "{} bytes", body.len());
            assert_eq!(
                scanned <= MAX_REQUEST_JSON_DEPTH,
                *within,
                "{} bytes",
                body.len()
            );
        }
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

    #[test]
    fn a_body_with_a_syntax_fault_is_refused_with_no_message() {
        // Rows: a body that parses, a fault the parse meets, a fault within
        // the ceiling that only the check meets before authentication, a
        // fault past the ceiling, and seven faults in the leaf of a 200
        // wrapper body, each with whether the server reads it. The service
        // sends no message field for a signed body with a syntax fault
        // (measured 2026-11-19 and 2026-11-20, us-east-1) and refuses each
        // leaf fault before authentication (measured in us-east-1: the
        // control byte and the unknown escape in a string on 2026-11-20,
        // unsigned, badly signed and signed; the `\u` before the closing
        // quote, SOH before a value, `tru`, the missing colon and the missing
        // comma on 2026-11-24, unsigned and badly signed). The parse and the
        // check give each row the same answer. The parse recurses, so the
        // rows run on a worker-sized stack.
        let rows = [
            (br#"{"TableName": "t"}"#.to_vec(), true),
            (b"".to_vec(), false),
            (truncated_body(200), false),
            (truncated_body(20000), false),
            (deep_body_with_leaf(200, br#"{"S":"le\u"}"#), false),
            (deep_body_with_leaf(200, b"{\"S\":\x01\"leaf\"}"), false),
            (deep_body_with_leaf(200, br#"{"S":tru}"#), false),
            (deep_body_with_leaf(200, br#"{"S" "leaf"}"#), false),
            (deep_body_with_leaf(200, br#"{"S":"leaf" "T":"x"}"#), false),
            (deep_body_with_leaf(200, b"{\"S\":\"le\x01af\"}"), false),
            (deep_body_with_leaf(200, br#"{"S":"le\qaf"}"#), false),
        ];
        std::thread::Builder::new()
            .stack_size(REQUEST_WORKER_STACK_SIZE)
            .spawn(move || {
                for (row, (body, read)) in rows.iter().enumerate() {
                    let parsed = parse(body).map(drop);
                    for answer in [parsed, check_request_syntax(body)] {
                        match answer {
                            Ok(()) => assert!(*read, "row {row} must be refused"),
                            Err(DynamoDbError::SerializationException(m)) => {
                                assert!(!*read && m.is_empty(), "row {row}: {m:?}");
                            }
                            Err(e) => panic!("row {row}: {e:?}"),
                        }
                    }
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn the_syntax_check_passes_what_the_service_authenticates_first() {
        // Rows: bodies the service's check before authentication passes.
        // Complete bodies far past the ceiling, bytes after a complete body,
        // an invalid UTF-8 byte in a string and a lone surrogate escape
        // (measured 2026-11-19, us-east-1: unsigned, each is a
        // MissingAuthenticationTokenException), and each leaf of
        // `LOOSE_LEAVES` at 63 and at 20000 wrappers (measured 2026-11-24).
        // This runs on the default test-thread stack, where a parse that
        // recursed a million levels would overflow, so it also shows that the
        // check does not recurse.
        let mut rows = vec![
            [b"[".repeat(1_000_000), b"]".repeat(1_000_000)].concat(),
            deep_body(20000),
            [deep_body(2000), b"x".to_vec()].concat(),
            deep_body_with_leaf(200, b"{\"S\":\"le\xffaf\"}"),
            deep_body_with_leaf(200, br#"{"S":"le\ud800af"}"#),
        ];
        for wrappers in [63, 20000] {
            rows.extend(LOOSE_LEAVES.map(|leaf| deep_body_with_leaf(wrappers, leaf)));
        }
        for body in &rows {
            let depth = nesting_depth(body);
            assert!(depth > UNAUTHENTICATED_PARSE_DEPTH, "{} bytes", body.len());
            assert!(check_request_syntax(body).is_ok(), "{} bytes", body.len());
        }
    }
}
