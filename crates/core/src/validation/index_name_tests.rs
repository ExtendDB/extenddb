// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Index names that DynamoDB refuses, in each CreateTable and UpdateTable
//! member that core checks, with DynamoDB's answers measured in us-east-1
//! (#904 on 2026-11-06, #1021 on 2026-11-14, the empty vector index names
//! on 2026-11-20, the other rows on 2026-11-18). DynamoDB lists the two
//! clauses of one member in either order, and ExtendDB writes the length
//! clause first.

use serde_json::{Value, json};

use super::{validate_create_table, validate_vector_index_updates};
use crate::error::DynamoDbError;
use crate::limits::LimitsConfig;
use crate::types::VectorIndexUpdate;

const SHORT: &str = "Member must have length greater than or equal to 3";
const LONG: &str = "Member must have length less than or equal to 255";
const PATTERN: &str = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+";

/// The member that carries the index name, in the shape of the measured
/// request.
#[derive(Clone, Copy, Debug)]
enum Member {
    Gsi,
    /// The second of two GSIs, the first named `gok`.
    SecondGsi,
    Lsi,
    Vector,
    VectorCreate,
    VectorDelete,
}

fn key(name: &str, key_type: &str) -> Value {
    json!({ "AttributeName": name, "KeyType": key_type })
}

fn defined(names: &[&str]) -> Value {
    names
        .iter()
        .map(|n| json!({ "AttributeName": n, "AttributeType": "S" }))
        .collect()
}

/// ExtendDB's refusal of `name` in `member`.
fn refusal(member: Member, name: &str) -> String {
    let keys_only = json!({ "ProjectionType": "KEYS_ONLY" });
    let gsi = |n: &str| json!({ "IndexName": n, "KeySchema": [key("g", "HASH")], "Projection": keys_only });
    let vector = json!({
        "IndexName": name,
        "Dimensions": 4,
        "DistanceFunction": "COSINE",
        "VectorAttribute": { "AttributeName": "emb" },
        "Projection": { "ProjectionType": "ALL" },
    });
    let mut table = json!({
        "TableName": "t1021",
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": defined(&["pk", "g"]),
        "KeySchema": [key("pk", "HASH")],
    });
    let updates = match member {
        Member::Gsi => {
            table["GlobalSecondaryIndexes"] = json!([gsi(name)]);
            None
        }
        Member::SecondGsi => {
            table["GlobalSecondaryIndexes"] = json!([gsi("gok"), gsi(name)]);
            None
        }
        Member::Lsi => {
            table["AttributeDefinitions"] = defined(&["pk", "s", "l"]);
            table["KeySchema"] = json!([key("pk", "HASH"), key("s", "RANGE")]);
            table["LocalSecondaryIndexes"] = json!([{
                "IndexName": name,
                "KeySchema": [key("pk", "HASH"), key("l", "RANGE")],
                "Projection": keys_only,
            }]);
            None
        }
        Member::Vector => {
            table["AttributeDefinitions"] = defined(&["pk"]);
            table["VectorIndexes"] = json!([vector]);
            None
        }
        Member::VectorCreate => Some(json!([{ "Create": vector }])),
        Member::VectorDelete => Some(json!([{ "Delete": { "IndexName": name } }])),
    };
    let result = match updates {
        None => validate_create_table(
            &serde_json::from_value(table).unwrap(),
            &LimitsConfig::default(),
        ),
        Some(updates) => {
            let updates: Vec<VectorIndexUpdate> = serde_json::from_value(updates).unwrap();
            validate_vector_index_updates(Some(&updates), &[])
        }
    };
    match result {
        Err(DynamoDbError::ValidationException(m)) => m,
        other => panic!("{member:?}: {other:?}"),
    }
}

/// DynamoDB's sentence for `clauses` failed by `name` at `path`.
fn sentence(name: &str, path: &str, clauses: &[&str]) -> String {
    let at = |c: &str| format!("Value '{name}' at '{path}' failed to satisfy constraint: {c}");
    match clauses {
        [one] => format!("1 validation error detected: {}", at(one)),
        [first, second] => format!(
            "2 validation errors detected: {}; {}",
            at(first),
            at(second)
        ),
        _ => unreachable!("{clauses:?}"),
    }
}

#[test]
fn an_index_name_is_refused_at_its_member_path_with_one_clause_per_constraint() {
    use Member::{Gsi, Lsi, SecondGsi, Vector, VectorCreate, VectorDelete};
    let emoji2 = "\u{1F600}".repeat(2);
    let emoji128 = "\u{1F600}".repeat(128);
    let a256 = "a".repeat(256);
    let gsi = "globalSecondaryIndexes.1.member.indexName";
    let lsi = "localSecondaryIndexes.1.member.indexName";
    let vector = "vectorIndexes.1.member.indexName";
    let create = "vectorIndexUpdates.1.member.create.indexName";
    let delete = "vectorIndexUpdates.1.member.delete.indexName";
    let rows: [(Member, &str, &str, &[&str]); 27] = [
        // #1021
        (Gsi, &emoji2, gsi, &[SHORT, PATTERN]),
        (Gsi, &emoji128, gsi, &[PATTERN]),
        (Lsi, &emoji2, lsi, &[SHORT, PATTERN]),
        (Lsi, &emoji128, lsi, &[PATTERN]),
        // #904
        (Gsi, "g i", gsi, &[PATTERN]),
        (
            SecondGsi,
            "g\u{0}i",
            "globalSecondaryIndexes.2.member.indexName",
            &[PATTERN],
        ),
        // An empty name is outside the pattern too.
        (Gsi, "", gsi, &[SHORT, PATTERN]),
        (Lsi, "", lsi, &[SHORT, PATTERN]),
        (Vector, "", vector, &[SHORT, PATTERN]),
        (VectorCreate, "", create, &[SHORT, PATTERN]),
        (VectorDelete, "", delete, &[SHORT, PATTERN]),
        // Names inside the pattern, and the other members.
        (Gsi, "ab", gsi, &[SHORT]),
        (Gsi, &a256, gsi, &[LONG]),
        (Lsi, "ab", lsi, &[SHORT]),
        (Lsi, &a256, lsi, &[LONG]),
        (Vector, "ab", vector, &[SHORT]),
        (Vector, &a256, vector, &[LONG]),
        (Vector, &emoji2, vector, &[SHORT, PATTERN]),
        (Vector, &emoji128, vector, &[PATTERN]),
        (VectorCreate, "ab", create, &[SHORT]),
        (VectorCreate, &a256, create, &[LONG]),
        (VectorCreate, &emoji2, create, &[SHORT, PATTERN]),
        (VectorCreate, &emoji128, create, &[PATTERN]),
        (VectorDelete, "ab", delete, &[SHORT]),
        (VectorDelete, &a256, delete, &[LONG]),
        (VectorDelete, &emoji2, delete, &[SHORT, PATTERN]),
        (VectorDelete, &emoji128, delete, &[PATTERN]),
    ];
    for (member, name, path, clauses) in rows {
        assert_eq!(
            refusal(member, name),
            sentence(name, path, clauses),
            "{member:?}, {} characters",
            name.chars().count()
        );
    }
}
