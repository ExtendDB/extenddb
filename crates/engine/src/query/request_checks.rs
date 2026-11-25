// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checks `handle_query` makes on a request before it reads the table,
//! and the refusal authorization makes with them for a key condition with two
//! conditions on one key attribute.

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionKind, ExpressionMaps, KeyCondition, has_duplicate_key_condition, tokenize_for,
};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::QueryInput;

use super::{check_request_parameters, parse_filter_expression};
use crate::expression_helpers::build_expression_maps;

/// A `Query` request read from its body, with its expressions parsed.
pub(super) struct CheckedRequest {
    pub(super) input: QueryInput,
    pub(super) maps: ExpressionMaps,
    pub(super) filter_expression: Option<Expr>,
    /// The key condition, or the fault of its shape, which `handle_query`
    /// answers after the table checks.
    pub(super) key_condition_expression: Option<Result<KeyCondition, DynamoDbError>>,
}

/// Read a `Query` body and refuse its faults that come before the table is
/// read, in the order the service reports them.
pub(super) fn check_request(
    mut body: Value,
    limits: &LimitsConfig,
) -> Result<CheckedRequest, DynamoDbError> {
    // The checks on the raw body, before typed deserialization: Limit, the
    // ReturnConsumedCapacity enum, the parameter mix, the names map and the
    // numbers, in the service's order.
    crate::query_key_values::validate_query_request_body(&body)?;
    // The service checks Select after Limit, the request's parameters, the
    // FilterExpression and the KeyConditionExpression checks below (measured
    // 2026-11-11), so an invalid Select is left out of the typed input and its
    // error waits for those checks. A shape fault elsewhere in the body is
    // reported ahead of it (see `validate_query_select`).
    let select_check = crate::query_key_values::validate_query_select(&body);
    if select_check.is_err()
        && let Some(fields) = body.as_object_mut()
    {
        fields.remove("Select");
    }
    // An invalid Select still comes ahead of a body that does not
    // deserialize, as it did when it was checked before deserialization.
    let input: QueryInput = match serde_json::from_value(body) {
        Ok(input) => input,
        Err(err) => {
            select_check?;
            return Err(crate::deserialize_error(err));
        }
    };

    // Build expression maps from request (used for expression-based parameters)
    let maps = build_expression_maps(
        input.expression_attribute_names.as_ref(),
        input.expression_attribute_values.as_ref(),
    );

    // The service checks a Query in this order (measured 2026-11-11 and
    // 2026-11-23 in us-east-1): Limit, then the request's parameters, then
    // every FilterExpression fault, then every KeyConditionExpression fault
    // except its shape, then Select, the table, the index and ConsistentRead.
    check_request_parameters(&input)?;
    let filter_expression = parse_filter_expression(&input, &maps, limits)?;
    let key_condition_expression = input
        .key_condition_expression
        .as_deref()
        .map(|kce| crate::expression_helpers::parse_key_condition_expr(kce, &maps, limits))
        .transpose()?;
    select_check?;
    Ok(CheckedRequest {
        input,
        maps,
        filter_expression,
        key_condition_expression,
    })
}

/// Refuse a `Query` body whose `KeyConditionExpression` has two conditions on
/// one attribute, with the first fault [`check_request`] finds in it. `names`
/// is the body's `ExpressionAttributeNames` without the `#`. Authorization
/// calls this before the IAM decision, as the service refuses such a request
/// for every principal (measured 2026-11-19, us-east-1).
///
/// # Errors
///
/// The error [`check_request`] returns for the body.
pub fn refuse_duplicate_key_condition(
    body: &Value,
    names: &HashMap<String, String>,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    let Some(kce) = body.get("KeyConditionExpression").and_then(Value::as_str) else {
        return Ok(());
    };
    let duplicate = tokenize_for(
        kce,
        limits.max_expression_tokens,
        ExpressionKind::KeyCondition,
    )
    .is_ok_and(|tokens| has_duplicate_key_condition(&tokens, names));
    if duplicate {
        check_request(body.clone(), limits)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{HashMap, refuse_duplicate_key_condition};
    use extenddb_core::error::DynamoDbError;
    use extenddb_core::limits::LimitsConfig;
    use serde_json::{Value, json};

    fn refuse(kce: &str, extra: Value) -> Result<(), DynamoDbError> {
        let mut body = json!({
            "TableName": "t",
            "KeyConditionExpression": kce,
            "ExpressionAttributeValues": {
                ":own": {"S": "alice"}, ":other": {"S": "bob"}, ":b": {"S": "x"}, ":c": {"S": "y"},
            },
        });
        if let (Some(body), Value::Object(extra)) = (body.as_object_mut(), extra) {
            body.extend(extra);
        }
        refuse_duplicate_key_condition(&body, &HashMap::new(), &LimitsConfig::default())
    }

    /// The service gave each answer below to the table's owner and to every
    /// fenced, unallowed and denied principal (us-east-1, 2026-11-19).
    #[test]
    fn a_duplicate_gets_the_answer_of_the_table_owner() {
        let one_per_key = "Invalid KeyConditionExpression: KeyConditionExpressions must only \
                           contain one condition per key";
        let rows = [
            ("pk = :own AND sk = :b AND sk = :c", json!({}), one_per_key),
            (
                "pk = :own AND pk = :other",
                json!({"FilterExpression": "#q = :own"}),
                "Invalid FilterExpression: An expression attribute name used in the document \
                 path is not defined; attribute name: #q",
            ),
            (
                "pk = :other AND pk = :own AND #u = :c",
                json!({}),
                "Invalid KeyConditionExpression: An expression attribute name used in the \
                 document path is not defined; attribute name: #u",
            ),
            (
                "pk = :other AND pk = :own",
                json!({"Limit": 0}),
                "1 validation error detected: Value at 'Limit' failed to satisfy constraint: \
                 Member must have value greater than or equal to 1",
            ),
        ];
        for (kce, extra, want) in rows {
            let label = format!("{kce} {extra}");
            let got = refuse(kce, extra);
            assert!(
                matches!(&got, Err(DynamoDbError::ValidationException(msg)) if msg == want),
                "{label}: {got:?}"
            );
        }
    }

    /// The service authorizes these first: a principal fenced away from
    /// `alice` gets AccessDeniedException for both (us-east-1, 2026-11-19).
    #[test]
    fn a_key_condition_without_a_duplicate_goes_on_to_authorization() {
        for kce in ["pk = :own AND sk = :b", "pk > :own"] {
            assert!(refuse(kce, json!({})).is_ok(), "{kce}");
        }
    }
}
