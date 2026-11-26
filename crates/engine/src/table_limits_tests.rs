// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `CreateTable` and `UpdateTable` through their handlers, on an account with
//! a quota of 3 tables, per-table maxima of 100 and account maxima of 150.
//! The rows run in order, each on the tables that the rows above it created.
//! `provisioned_limits_tests.rs` pins the sentences against the measured ones.

use std::sync::Arc;

use extenddb_core::limits::LimitsConfig;
use extenddb_storage_sqlite::SqliteEngine;
use serde_json::{Value, json};

use crate::create_table::handle_create_table;
use crate::partiql::test_context::context;
use crate::update_table::handle_update_table;

const GUIDE: &str = "Refer to the Amazon DynamoDB Developer Guide for current limits and how \
                     to request higher limits.";

/// A request to one of the two handlers.
enum Op {
    Create(Value),
    Update(Value),
}

fn throughput(read: i64, write: i64) -> Value {
    json!({ "ReadCapacityUnits": read, "WriteCapacityUnits": write })
}

/// A `CreateTable` request for a provisioned table keyed on `pk`.
fn provisioned(name: &str, read: i64, write: i64) -> Value {
    json!({
        "TableName": name,
        "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
        "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
        "ProvisionedThroughput": throughput(read, write),
    })
}

fn create(name: &str, read: i64, write: i64) -> Op {
    Op::Create(provisioned(name, read, write))
}

fn on_demand(name: &str) -> Op {
    Op::Create(json!({
        "TableName": name,
        "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
        "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
        "BillingMode": "PAY_PER_REQUEST",
    }))
}

fn update(name: &str, read: i64, write: i64) -> Op {
    Op::Update(json!({ "TableName": name, "ProvisionedThroughput": throughput(read, write) }))
}

fn per_table(unit: &str, requested: i64) -> String {
    format!(
        "The requested {unit}, {requested}, is above the per table maximum for the account in \
         us-east-1. Per table maximum: 100. {GUIDE}"
    )
}

/// The account sentence for read units. `table` is what the table and its
/// indexes hold, on `UpdateTable`.
fn account(reserved: i64, table: Option<i64>, requested: i64) -> String {
    let table = table.map_or_else(String::new, |now| {
        format!(" Current ReadCapacityUnits provisioned for the table and indexes: {now}.")
    });
    format!(
        "This request would have caused the ReadCapacityUnits limit to be exceeded for the \
         account in us-east-1. Current ReadCapacityUnits reserved by the account: {reserved}. \
         Limit: 150.{table} Requested: {requested}. {GUIDE}"
    )
}

/// A row: its name, the request, and the error type and message it gets, or
/// `None` when it is accepted.
type Row = (&'static str, Op, Option<(&'static str, String)>);

fn rows() -> Vec<Row> {
    let limit = |message| Some(("LimitExceededException", message));
    let mut undefined_key = provisioned("lim-a", 101, 1);
    undefined_key["AttributeDefinitions"][0]["AttributeName"] = json!("x");
    let gsi_create = Op::Update(json!({
        "TableName": "lim-b",
        "AttributeDefinitions": [{ "AttributeName": "g", "AttributeType": "S" }],
        "GlobalSecondaryIndexUpdates": [{ "Create": {
            "IndexName": "gix",
            "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" },
            "ProvisionedThroughput": throughput(11, 1),
        }}],
    }));
    vec![
        (
            "an undefined key at 101 RCU",
            Op::Create(undefined_key),
            Some((
                "ValidationException",
                "One or more parameter values were invalid: Some index key attributes are not \
                 defined in AttributeDefinitions. Keys: [pk], AttributeDefinitions: [x]"
                    .to_owned(),
            )),
        ),
        (
            "101 RCU",
            create("lim-a", 101, 1),
            limit(per_table("ReadCapacityUnits", 101)),
        ),
        (
            "101 WCU",
            create("lim-a", 1, 101),
            limit(per_table("WriteCapacityUnits", 101)),
        ),
        ("100 RCU", create("lim-a", 100, 1), None),
        (
            "51 RCU with 100 reserved",
            create("lim-b", 51, 1),
            limit(account(100, None, 51)),
        ),
        ("50 RCU", create("lim-b", 50, 1), None),
        (
            "UpdateTable to 101 RCU",
            update("lim-b", 101, 1),
            limit(per_table("ReadCapacityUnits", 101)),
        ),
        (
            "UpdateTable to 60 RCU",
            update("lim-b", 60, 1),
            limit(account(150, Some(50), 60)),
        ),
        ("UpdateTable down to 40 RCU", update("lim-b", 40, 1), None),
        (
            "UpdateTable with a GSI create of 11 RCU",
            gsi_create,
            limit(account(140, Some(40), 51)),
        ),
        ("a third table, on-demand", on_demand("lim-c"), None),
        (
            "a fourth table",
            on_demand("lim-d"),
            limit("There is a soft account quota of 3 tables.".to_owned()),
        ),
        (
            "a taken name at the quota",
            create("lim-a", 1, 1),
            Some((
                "ResourceInUseException",
                "Table already exists: lim-a".to_owned(),
            )),
        ),
    ]
}

#[tokio::test]
async fn create_table_and_update_table_apply_the_account_limits() {
    let mut ctx = context(None).await;
    ctx.storage = Arc::new(
        SqliteEngine::new_in_memory(&ctx.region, &ctx.account_id, 409_600)
            .await
            .expect("an in-memory catalog opens"),
    );
    ctx.limits = Arc::new(LimitsConfig {
        max_tables_per_account: 3,
        per_table_max_rcu: 100,
        per_table_max_wcu: 100,
        per_account_max_rcu: 150,
        per_account_max_wcu: 150,
        ..LimitsConfig::default()
    });
    for (row, op, want) in rows() {
        let answer = match op {
            Op::Create(request) => handle_create_table(request, &ctx).await,
            Op::Update(request) => handle_update_table(request, &ctx).await,
        };
        let got = answer.err().map(|e| {
            (
                e.status_code(),
                e.error_type().to_owned(),
                e.message().to_owned(),
            )
        });
        let want = want.map(|(error_type, message)| (400, error_type.to_owned(), message));
        assert_eq!(got, want, "{row}");
    }
}
