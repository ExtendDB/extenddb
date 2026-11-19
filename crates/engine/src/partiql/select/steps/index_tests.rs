// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the pages of a read of an index by several disjuncts.

use serde_json::{Value, json};

use crate::OperationContext;
use crate::partiql::PartiqlExecutor;
use crate::partiql::test_context::{block_on, catalog_context};
use crate::{handle_create_table, handle_put_item};

/// Walks of lsi-all under `Limit` 1 and the pages the service gives, written
/// as [`walk`] writes them. Partition lk holds r1 at a, and r2 and r3 at b
/// (measured 2026-11-14 to 2026-11-19).
const WALKS: [(&str, &str); 2] = [
    ("(lsiSk = 'a' OR lsiSk = 'z')", "[r1] 0.5 next | [] 0.5"),
    (
        "(lsiSk = 'a' OR lsiSk = 'b') ORDER BY lsiSk DESC",
        "[r3] 0.5 next | [r2] 0.5 next | [r1] 0.5 next | [] 0.5",
    ),
];

#[test]
fn a_query_that_stops_before_an_item_it_does_not_match_is_read_to_its_end() {
    block_on(async {
        let ctx = catalog_context().await;
        let attribute = |name: &str| json!({ "AttributeName": name, "AttributeType": "S" });
        let key = |name: &str, kind: &str| json!({ "AttributeName": name, "KeyType": kind });
        let table = json!({
            "TableName": "pages",
            "KeySchema": [key("pk", "HASH"), key("sk", "RANGE")],
            "AttributeDefinitions": [attribute("pk"), attribute("sk"), attribute("lsiSk")],
            "LocalSecondaryIndexes": [{
                "IndexName": "lsi-all",
                "KeySchema": [key("pk", "HASH"), key("lsiSk", "RANGE")],
                "Projection": { "ProjectionType": "ALL" },
            }],
            "BillingMode": "PAY_PER_REQUEST",
        });
        handle_create_table(table, &ctx).await.unwrap();
        for (sk, at) in [("r1", "a"), ("r2", "b"), ("r3", "b")] {
            let item = json!({ "pk": { "S": "lk" }, "sk": { "S": sk }, "lsiSk": { "S": at } });
            let put = json!({ "TableName": "pages", "Item": item });
            let _ = handle_put_item(put, &ctx).await.unwrap();
        }
        for (condition, pages) in WALKS {
            let statement =
                format!("SELECT * FROM \"pages\".\"lsi-all\" WHERE pk = 'lk' AND {condition}");
            assert_eq!(walk(&ctx, &statement, 1).await, pages, "{statement}");
        }
    });
}

/// The pages of `statement` with `limit`, following NextToken: each written
/// as the sort keys of its items and the capacity units it reports, then
/// `next` when it has a NextToken.
async fn walk(ctx: &OperationContext, statement: &str, limit: i64) -> String {
    let mut pages = Vec::new();
    let mut token = None;
    while pages.len() < 10 {
        let mut body = json!({
            "Statement": statement, "Limit": limit, "ReturnConsumedCapacity": "TOTAL"
        });
        if let Some(token) = token.take() {
            body["NextToken"] = Value::String(token);
        }
        let page = ctx.execute_statement(body).await.unwrap().body;
        let items = page["Items"].as_array().unwrap();
        let keys: Vec<&str> = items.iter().filter_map(|i| i["sk"]["S"].as_str()).collect();
        let units = &page["ConsumedCapacity"]["CapacityUnits"];
        token = page
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let next = if token.is_some() { " next" } else { "" };
        pages.push(format!("[{}] {units}{next}", keys.join(" ")));
        if token.is_none() {
            break;
        }
    }
    pages.join(" | ")
}
