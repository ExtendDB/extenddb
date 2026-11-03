// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The opaque `NextToken` of a paged `SELECT`, bound to the statement and the
//! parameter values that produced it.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::Value;
use sha2::{Digest, Sha256};

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::AttributeValue;

/// Encode an opaque pagination token binding a `LastEvaluatedKey` to the exact
/// statement and parameter values that produced it. The token holds digests
/// of both, so its length does not grow with the statement: a request
/// refuses a `NextToken` past 32,768 code points, and a statement of 8,192
/// four-byte characters is 32,768 bytes on its own.
pub(super) fn encode_next_token(
    statement: &str,
    parameters: &[AttributeValue],
    last_evaluated_key: &Value,
) -> String {
    let payload = serde_json::json!({
        "s": statement_digest(statement),
        "p": parameters_digest(parameters),
        "k": last_evaluated_key,
    });
    BASE64.encode(payload.to_string().as_bytes())
}

/// The refusal of a `NextToken` that does not decode.
pub(super) const INVALID_NEXT_TOKEN: &str = "Invalid NextToken";

/// The refusal of a `NextToken` another statement, or other parameter
/// values, gave.
pub(super) const TOKEN_MISMATCH: &str = "NextToken does not match request";

/// Decode a pagination token and return its `LastEvaluatedKey`. A token that
/// does not decode is the service's "Invalid NextToken" (measured 2026-11-02);
/// one whose embedded statement or parameter values differ from the current
/// ones is "NextToken does not match request" (measured 2026-11-02: a token
/// from `pk = ?` with 'p' is refused with 'q' and continues with 'p'). Both
/// are refused before the statement is authorized (measured 2026-11-03 for a
/// token that does not decode).
pub(super) fn decode_next_token(
    token: &str,
    statement: &str,
    parameters: &[AttributeValue],
) -> Result<Value, DynamoDbError> {
    let invalid = || DynamoDbError::ValidationException(INVALID_NEXT_TOKEN.to_owned());
    let bytes = BASE64.decode(token).map_err(|_| invalid())?;
    let decoded: Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    let (Some(issued_for), Some(key)) =
        (decoded.get("s").and_then(Value::as_str), decoded.get("k"))
    else {
        return Err(invalid());
    };
    if issued_for != statement_digest(statement)
        || decoded.get("p").and_then(Value::as_str) != Some(&parameters_digest(parameters))
    {
        return Err(DynamoDbError::ValidationException(
            TOKEN_MISMATCH.to_owned(),
        ));
    }
    Ok(key.clone())
}

/// A digest of the statement text.
fn statement_digest(statement: &str) -> String {
    hex::encode(Sha256::digest(statement.as_bytes()))
}

/// A digest of the parameter values in wire form. Maps and sets serialize in
/// a fixed order, so equal values give equal digests.
fn parameters_digest(parameters: &[AttributeValue]) -> String {
    let wire = serde_json::to_vec(parameters).unwrap_or_default();
    hex::encode(Sha256::digest(&wire))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(err: DynamoDbError) -> String {
        match err {
            DynamoDbError::ValidationException(m) => m,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    fn refused(result: Result<Value, DynamoDbError>) -> bool {
        matches!(
            result,
            Err(DynamoDbError::ValidationException(m)) if m == "NextToken does not match request"
        )
    }

    #[test]
    fn next_token_round_trips_and_binds_to_statement() {
        let lek = serde_json::json!({"pk": {"S": "p1"}, "sk": {"S": "s02"}});
        let token = encode_next_token("SELECT * FROM \"t\" WHERE pk='p1'", &[], &lek);
        let decoded = decode_next_token(&token, "SELECT * FROM \"t\" WHERE pk='p1'", &[]).unwrap();
        assert_eq!(decoded, lek);
        assert!(refused(decode_next_token(
            &token,
            "SELECT * FROM \"t\" WHERE pk='p2'",
            &[]
        )));
    }

    #[test]
    fn a_token_that_does_not_decode_is_invalid() {
        let statement = "SELECT * FROM \"t\" WHERE pk='p1'";
        let not_json = BASE64.encode(b"not-a-token");
        let no_key = BASE64.encode(serde_json::json!({ "s": statement }).to_string());
        let no_statement = BASE64.encode(serde_json::json!({ "k": {} }).to_string());
        for token in [
            "n".repeat(32768),
            "x".to_owned(),
            "a b!".to_owned(),
            not_json,
            no_key,
            no_statement,
        ] {
            let err = decode_next_token(&token, statement, &[]).unwrap_err();
            assert_eq!(message(err), "Invalid NextToken", "{token}");
        }
    }

    #[test]
    fn a_token_for_the_longest_statement_fits_the_token_limit() {
        // 8,192 code points of four bytes each, the longest statement a
        // request carries, and a key of the largest partition and sort key
        // values in four-byte characters.
        let statement = "\u{1F600}".repeat(8192);
        let lek = serde_json::json!({
            "pk": {"S": "\u{1F600}".repeat(512)},
            "sk": {"S": "\u{1F600}".repeat(256)}
        });
        let token = encode_next_token(&statement, &[AttributeValue::S("p".to_owned())], &lek);
        assert!(token.chars().count() <= 32768, "{}", token.len());
        assert_eq!(
            decode_next_token(&token, &statement, &[AttributeValue::S("p".to_owned())]).unwrap(),
            lek
        );
        let other = format!("{statement}x");
        assert!(refused(decode_next_token(
            &token,
            &other,
            &[AttributeValue::S("p".to_owned())]
        )));
    }

    #[test]
    fn next_token_binds_to_the_parameter_values() {
        let statement = "SELECT * FROM \"t\" WHERE pk = ?";
        let p = [AttributeValue::S("p".to_owned())];
        let q = [AttributeValue::S("q".to_owned())];
        let lek = serde_json::json!({"pk": {"S": "p"}, "sk": {"S": "s1"}});
        let token = encode_next_token(statement, &p, &lek);
        assert_eq!(decode_next_token(&token, statement, &p).unwrap(), lek);
        assert!(refused(decode_next_token(&token, statement, &q)));
        assert!(refused(decode_next_token(&token, statement, &[])));
    }
}
