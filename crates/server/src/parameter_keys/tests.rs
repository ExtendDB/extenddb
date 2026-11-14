// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the `dynamodb:Select`, `dynamodb:ReturnValues` and
//! `dynamodb:ReturnConsumedCapacity` values.
//!
//! The defaults mirror DynamoDB's answers of 2026-11-01; the module
//! documentation lists which were measured and which follow them.

use std::collections::HashMap;

use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{KeySchemaElement, KeyType, TableKeyInfo};
use serde_json::{Value, json};

use super::*;

fn keys(select: Option<&str>, return_values: Option<&str>, rcc: Option<&str>) -> ParameterKeys {
    ParameterKeys {
        select: select.map(ToOwned::to_owned),
        return_values: return_values.map(ToOwned::to_owned),
        return_consumed_capacity: rcc.map(ToOwned::to_owned),
    }
}

/// `base` with the members of `extra` added.
fn with(base: &Value, extra: &Value) -> Value {
    let mut out = base.clone();
    for (k, v) in extra.as_object().expect("extra is an object") {
        out[k] = v.clone();
    }
    out
}

fn read() -> Value {
    json!({"TableName": "t", "Key": {"pk": {"S": "a"}}})
}

fn projected() -> Value {
    json!({"ProjectionExpression": "#n", "ExpressionAttributeNames": {"#n": "name"}})
}

fn select_of(operation: &str, input: &Value) -> Option<String> {
    single_table(operation, input).select
}

// -- dynamodb:Select ----------------------------------------------------------

#[test]
fn get_item_selects_from_its_projection() {
    assert_eq!(
        select_of("GetItem", &read()).as_deref(),
        Some("ALL_ATTRIBUTES")
    );
    assert_eq!(
        select_of("GetItem", &with(&read(), &projected())).as_deref(),
        Some("SPECIFIC_ATTRIBUTES")
    );
    assert_eq!(
        select_of(
            "GetItem",
            &with(&read(), &json!({"AttributesToGet": ["name"]}))
        )
        .as_deref(),
        Some("SPECIFIC_ATTRIBUTES")
    );
}

#[test]
fn get_item_does_not_read_a_select_member() {
    // GetItem takes no Select: the engine ignores the member and returns the
    // whole item, so the member must not decide the key.
    let unprojected = with(&read(), &json!({"Select": "SPECIFIC_ATTRIBUTES"}));
    assert_eq!(
        select_of("GetItem", &unprojected).as_deref(),
        Some("ALL_ATTRIBUTES")
    );
    let projected = with(&with(&read(), &projected()), &json!({"Select": "COUNT"}));
    assert_eq!(
        select_of("GetItem", &projected).as_deref(),
        Some("SPECIFIC_ATTRIBUTES")
    );
}

#[test]
fn a_null_projection_is_no_projection() {
    // serde reads a null member as absent, so the engine returns every attribute.
    let nulls = with(
        &read(),
        &json!({"ProjectionExpression": null, "AttributesToGet": null, "Select": null}),
    );
    for operation in ["GetItem", "Query", "Scan"] {
        assert_eq!(
            select_of(operation, &nulls).as_deref(),
            Some("ALL_ATTRIBUTES"),
            "{operation}"
        );
    }
    let body = json!({"Keys": [{"pk": {"S": "a"}}], "ProjectionExpression": null});
    assert_eq!(
        batch_table("BatchGetItem", &body, &json!({}))
            .select
            .as_deref(),
        Some("ALL_ATTRIBUTES")
    );
    let get = json!({"TableName": "t", "Key": {"pk": {"S": "a"}}, "ProjectionExpression": null});
    assert_eq!(
        transact_item("Get", &get, &json!({})).select.as_deref(),
        Some("ALL_ATTRIBUTES")
    );
}

#[test]
fn query_and_scan_default_select_from_the_projection_and_the_index() {
    let index = json!({"IndexName": "gsi"});
    for operation in ["Query", "Scan"] {
        let cases = [
            (read(), "ALL_ATTRIBUTES"),
            (with(&read(), &projected()), "SPECIFIC_ATTRIBUTES"),
            (
                with(&read(), &json!({"AttributesToGet": ["name"]})),
                "SPECIFIC_ATTRIBUTES",
            ),
            (with(&read(), &index), "ALL_PROJECTED_ATTRIBUTES"),
            (
                with(&with(&read(), &index), &projected()),
                "SPECIFIC_ATTRIBUTES",
            ),
            (with(&read(), &json!({"IndexName": null})), "ALL_ATTRIBUTES"),
        ];
        for (input, expected) in cases {
            assert_eq!(
                select_of(operation, &input).as_deref(),
                Some(expected),
                "{operation} {input}"
            );
        }
    }
}

#[test]
fn query_and_scan_keep_a_given_select() {
    let index_and_projection = with(&with(&read(), &json!({"IndexName": "gsi"})), &projected());
    for operation in ["Query", "Scan"] {
        for given in [
            "COUNT",
            "ALL_ATTRIBUTES",
            "ALL_PROJECTED_ATTRIBUTES",
            "SPECIFIC_ATTRIBUTES",
        ] {
            for base in [read(), index_and_projection.clone()] {
                let input = with(&base, &json!({"Select": given}));
                assert_eq!(
                    select_of(operation, &input).as_deref(),
                    Some(given),
                    "{operation} {input}"
                );
            }
        }
    }
}

// -- dynamodb:ReturnValues ----------------------------------------------------

#[test]
fn single_item_writes_default_return_values_to_none() {
    // The null case exercises the default path only: `given` treats a null
    // member as absent, so the key holds NONE. A real request carrying
    // `"ReturnValues": null` is refused by the engine with a
    // SerializationException before authorization, so it never reaches here.
    for operation in ["PutItem", "UpdateItem", "DeleteItem"] {
        for input in [read(), with(&read(), &json!({"ReturnValues": null}))] {
            assert_eq!(
                single_table(operation, &input).return_values.as_deref(),
                Some("NONE"),
                "{operation} {input}"
            );
        }
    }
}

#[test]
fn single_item_writes_keep_given_return_values() {
    for operation in ["PutItem", "UpdateItem", "DeleteItem"] {
        for given in ["ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW", "NONE"] {
            let input = with(&read(), &json!({"ReturnValues": given}));
            assert_eq!(
                single_table(operation, &input).return_values.as_deref(),
                Some(given),
                "{operation} {given}"
            );
        }
    }
}

// -- dynamodb:ReturnConsumedCapacity -----------------------------------------

#[test]
fn item_operations_default_return_consumed_capacity_to_none() {
    for operation in [
        "GetItem",
        "PutItem",
        "UpdateItem",
        "DeleteItem",
        "Query",
        "Scan",
    ] {
        assert_eq!(
            single_table(operation, &read())
                .return_consumed_capacity
                .as_deref(),
            Some("NONE"),
            "{operation}"
        );
        for given in ["TOTAL", "INDEXES", "NONE"] {
            let input = with(&read(), &json!({"ReturnConsumedCapacity": given}));
            assert_eq!(
                single_table(operation, &input)
                    .return_consumed_capacity
                    .as_deref(),
                Some(given),
                "{operation} {given}"
            );
        }
    }
}

// -- keys an operation does not take -------------------------------------------

#[test]
fn a_key_the_operation_does_not_take_is_absent() {
    // A member an operation does not take must not set its key. Otherwise a
    // policy that grants a broad action gated only on, say, dynamodb:Select
    // could be satisfied by a Select member added to a write or a DeleteTable.
    let all = json!({"Select": "COUNT", "ReturnValues": "ALL_OLD"});
    for operation in ["PutItem", "UpdateItem", "DeleteItem"] {
        assert_eq!(select_of(operation, &read()), None, "{operation}");
        assert_eq!(
            select_of(operation, &with(&read(), &all)),
            None,
            "{operation} does not read a Select member"
        );
    }
    for operation in ["GetItem", "Query", "Scan"] {
        assert_eq!(
            single_table(operation, &read()).return_values,
            None,
            "{operation}"
        );
        assert_eq!(
            single_table(operation, &with(&read(), &all)).return_values,
            None,
            "{operation} does not read a ReturnValues member"
        );
    }
}

#[test]
fn other_operations_read_only_the_keys_they_take() {
    let given = json!({
        "TableName": "t",
        "Select": "COUNT",
        "ReturnValues": "ALL_OLD",
        "ReturnConsumedCapacity": "TOTAL",
    });
    // Control-plane operations take none of the three keys, so no member of
    // those names is read, with or without it in the body.
    for operation in ["DescribeTable", "CreateTable", "ListTables", "DeleteTable"] {
        assert_eq!(
            single_table(operation, &json!({"TableName": "t"})),
            ParameterKeys::default(),
            "{operation}"
        );
        assert_eq!(
            single_table(operation, &given),
            ParameterKeys::default(),
            "{operation} reads no member"
        );
    }
    // SearchVectors takes ReturnConsumedCapacity, but not Select or ReturnValues.
    assert_eq!(
        single_table("SearchVectors", &json!({"TableName": "t"})),
        ParameterKeys::default()
    );
    assert_eq!(
        single_table("SearchVectors", &given),
        keys(None, None, Some("TOTAL")),
        "SearchVectors reads only ReturnConsumedCapacity"
    );
}

// -- batch and transaction parts -------------------------------------------------

#[test]
fn batch_get_item_tables_select_from_their_own_projection() {
    let keys_only = json!({"Keys": [{"pk": {"S": "a"}}]});
    let request = json!({"RequestItems": {"t": keys_only}});
    assert_eq!(
        batch_table("BatchGetItem", &keys_only, &request),
        keys(Some("ALL_ATTRIBUTES"), None, Some("NONE"))
    );
    for projection in [projected(), json!({"AttributesToGet": ["name"]})] {
        assert_eq!(
            batch_table("BatchGetItem", &with(&keys_only, &projection), &request).select,
            Some("SPECIFIC_ATTRIBUTES".to_owned()),
            "{projection}"
        );
    }
    let with_select = with(&keys_only, &json!({"Select": "SPECIFIC_ATTRIBUTES"}));
    assert_eq!(
        batch_table("BatchGetItem", &with_select, &request)
            .select
            .as_deref(),
        Some("ALL_ATTRIBUTES"),
        "a table part takes no Select"
    );
    let total = with(&request, &json!({"ReturnConsumedCapacity": "TOTAL"}));
    assert_eq!(
        batch_table("BatchGetItem", &keys_only, &total)
            .return_consumed_capacity
            .as_deref(),
        Some("TOTAL")
    );
}

#[test]
fn batch_write_item_tables_carry_only_return_consumed_capacity() {
    let body = json!([{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]);
    let request = json!({"RequestItems": {"t": body}});
    assert_eq!(
        batch_table("BatchWriteItem", &body, &request),
        keys(None, None, Some("NONE"))
    );
    let total = with(&request, &json!({"ReturnConsumedCapacity": "TOTAL"}));
    assert_eq!(
        batch_table("BatchWriteItem", &body, &total),
        keys(None, None, Some("TOTAL"))
    );
}

#[test]
fn transaction_gets_select_from_their_own_projection() {
    let get = json!({"TableName": "t", "Key": {"pk": {"S": "a"}}});
    assert_eq!(
        transact_item("Get", &get, &json!({})),
        keys(Some("ALL_ATTRIBUTES"), None, Some("NONE"))
    );
    assert_eq!(
        transact_item("Get", &with(&get, &projected()), &json!({}))
            .select
            .as_deref(),
        Some("SPECIFIC_ATTRIBUTES")
    );
    let total = json!({"ReturnConsumedCapacity": "TOTAL"});
    assert_eq!(
        transact_item("Get", &get, &total)
            .return_consumed_capacity
            .as_deref(),
        Some("TOTAL")
    );
}

#[test]
fn transaction_writes_hold_return_values_none() {
    let item = json!({"TableName": "t", "Key": {"pk": {"S": "a"}}, "ReturnValues": "ALL_OLD"});
    for sub in ["Put", "Update", "Delete"] {
        // A transaction item takes no ReturnValues; the engine refuses the member.
        assert_eq!(
            transact_item(sub, &item, &json!({})),
            keys(None, Some("NONE"), Some("NONE")),
            "{sub}"
        );
    }
    assert_eq!(
        transact_item("ConditionCheck", &item, &json!({})),
        keys(None, None, Some("NONE"))
    );
    let total = json!({"ReturnConsumedCapacity": "INDEXES"});
    for sub in ["Put", "Update", "Delete", "ConditionCheck"] {
        assert_eq!(
            transact_item(sub, &item, &total)
                .return_consumed_capacity
                .as_deref(),
            Some("INDEXES"),
            "{sub}"
        );
    }
}

// -- the batch and transaction targets ------------------------------------------

fn table(hash: &str, range: Option<&str>) -> TableKeyInfo {
    let mut key_schema = vec![KeySchemaElement {
        attribute_name: hash.to_owned(),
        key_type: KeyType::Hash,
    }];
    if let Some(range) = range {
        key_schema.push(KeySchemaElement {
            attribute_name: range.to_owned(),
            key_type: KeyType::Range,
        });
    }
    TableKeyInfo {
        table_name: "t".to_owned(),
        account_id: "123456789012".to_owned(),
        base_key_schema: key_schema.clone(),
        key_schema,
        ..Default::default()
    }
}

fn targets(operation: &str, input: &Value, ki: TableKeyInfo) -> Vec<(String, ParameterKeys)> {
    let snapshot = HashMap::from([("t".to_owned(), Some(ki))]);
    crate::leading_keys::batch_transact_targets(
        operation,
        input,
        &snapshot,
        &LimitsConfig::default(),
    )
    .expect("a batch or transaction operation")
    .into_iter()
    .map(|t| (t.action, t.parameters))
    .collect()
}

#[test]
fn batch_and_transaction_targets_carry_the_keys() {
    let key = json!({"pk": {"S": "a"}});
    let bg = json!({
        "RequestItems": {"t": {"Keys": [key], "ProjectionExpression": "pk"}},
        "ReturnConsumedCapacity": "TOTAL",
    });
    assert_eq!(
        targets("BatchGetItem", &bg, table("pk", None)),
        [(
            "BatchGetItem".to_owned(),
            keys(Some("SPECIFIC_ATTRIBUTES"), None, Some("TOTAL"))
        )]
    );
    let bw = json!({"RequestItems": {"t": [{"DeleteRequest": {"Key": key}}]}});
    assert_eq!(
        targets("BatchWriteItem", &bw, table("pk", None)),
        [("BatchWriteItem".to_owned(), keys(None, None, Some("NONE")))]
    );
    let tg = json!({"TransactItems": [{"Get": {"TableName": "t", "Key": key}}]});
    assert_eq!(
        targets("TransactGetItems", &tg, table("pk", None)),
        [(
            "GetItem".to_owned(),
            keys(Some("ALL_ATTRIBUTES"), None, Some("NONE"))
        )]
    );
    let tw = json!({"TransactItems": [
        {"Put": {"TableName": "t", "Item": {"pk": {"S": "p"}}}},
        {"Update": {"TableName": "t", "Key": {"pk": {"S": "u"}}, "UpdateExpression": "SET n = :v",
                    "ExpressionAttributeValues": {":v": {"S": "x"}}}},
        {"Delete": {"TableName": "t", "Key": {"pk": {"S": "d"}}}},
        {"ConditionCheck": {"TableName": "t", "Key": {"pk": {"S": "c"}},
                            "ConditionExpression": "attribute_exists(pk)"}},
    ]});
    assert_eq!(
        targets("TransactWriteItems", &tw, table("pk", None)),
        [
            ("PutItem".to_owned(), keys(None, Some("NONE"), Some("NONE"))),
            (
                "UpdateItem".to_owned(),
                keys(None, Some("NONE"), Some("NONE"))
            ),
            (
                "DeleteItem".to_owned(),
                keys(None, Some("NONE"), Some("NONE"))
            ),
            (
                "ConditionCheckItem".to_owned(),
                keys(None, None, Some("NONE"))
            ),
        ]
    );
}

#[test]
fn transaction_gets_that_differ_only_in_select_are_evaluated_apart() {
    // Two Gets of one partition on a table keyed on (pk, sk): the first
    // projects pk, the second projects nothing. Both hold LeadingKeys ["a"]
    // and Attributes ["pk", "sk"], so only dynamodb:Select tells them apart,
    // and a Deny on ALL_ATTRIBUTES must see the second.
    let tg = json!({"TransactItems": [
        {"Get": {"TableName": "t", "Key": {"pk": {"S": "a"}, "sk": {"S": "1"}},
                 "ProjectionExpression": "pk"}},
        {"Get": {"TableName": "t", "Key": {"pk": {"S": "a"}, "sk": {"S": "2"}}}},
    ]});
    let snapshot = HashMap::from([("t".to_owned(), Some(table("pk", Some("sk"))))]);
    let all = crate::leading_keys::batch_transact_targets(
        "TransactGetItems",
        &tg,
        &snapshot,
        &LimitsConfig::default(),
    )
    .expect("a transaction");
    assert_eq!(all.len(), 2, "{all:?}");
    assert_eq!(all[0].leading_keys, all[1].leading_keys);
    assert_eq!(all[0].attributes, all[1].attributes);
    assert_eq!(
        all[0].parameters.select.as_deref(),
        Some("SPECIFIC_ATTRIBUTES")
    );
    assert_eq!(all[1].parameters.select.as_deref(), Some("ALL_ATTRIBUTES"));
}
