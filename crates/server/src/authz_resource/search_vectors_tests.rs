// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SearchVectors authorizes the vector index it searches, as Query and Scan on
//! an index do.

use super::{AuthzTarget, authz_targets, resource_tag_lookup_arn};
use extenddb_core::validation::validate_index_name;
use serde_json::json;

const R: &str = "us-east-1";
const A: &str = "444455556666";
const TABLE: &str = "arn:aws:dynamodb:us-east-1:444455556666:table/bb";

fn single(input: serde_json::Value) -> AuthzTarget {
    let mut t = authz_targets("SearchVectors", &input, R, A);
    assert_eq!(t.len(), 1, "{t:?}");
    assert_eq!(t[0].action, "SearchVectors");
    t.remove(0)
}

#[test]
fn search_vectors_authorizes_the_index_arn() {
    let index = format!("{TABLE}/index/vidx");
    let t = single(json!({ "TableName": "bb", "IndexName": "vidx" }));
    assert_eq!(t.resource, index);
    assert_eq!(t.supplied, index);
    assert_eq!(t.policy_resource(), index);
    // The index inherits the table's tags, as a secondary index does.
    assert_eq!(resource_tag_lookup_arn(&t.resource).as_deref(), Some(TABLE));

    // A table ARN in TableName: the policy match is canonical and the denial
    // names the ARN as written with the index appended, as for Query.
    let respelled = "arn:aws-cn:dynamodb:us-west-2:444455556666:table/bb";
    let t = single(json!({ "TableName": respelled, "IndexName": "vidx" }));
    assert_eq!(t.resource, index);
    assert_eq!(t.supplied, format!("{respelled}/index/vidx"));
}

/// An empty or missing IndexName authorizes the table, as Query and Scan do;
/// the engine refuses it before it reads the table.
#[test]
fn search_vectors_without_an_index_name_authorizes_the_table() {
    assert_eq!(single(json!({ "TableName": "bb" })).resource, TABLE);
    assert_eq!(
        single(json!({ "TableName": "bb", "IndexName": "" })).resource,
        TABLE
    );
}

/// An IndexName that does not read as one index segment is authorized as
/// written: a Deny on `table/<T>/index/*` still matches it. Such a name reads
/// no resource tags, and no vector index can carry it, so the engine answers
/// that the table has no such index and reads no item.
#[test]
fn search_vectors_adversarial_index_names_name_no_real_index() {
    let long = "a".repeat(300);
    for name in [
        "vidx/extra",
        "vidx/index/other",
        "arn:aws:dynamodb:us-east-1:444455556666:table/other",
        long.as_str(),
    ] {
        let t = single(json!({ "TableName": "bb", "IndexName": name }));
        assert_eq!(t.resource, format!("{TABLE}/index/{name}"), "{name}");
        assert!(validate_index_name(name, "indexName").is_err(), "{name}");
    }
    for name in ["vidx/extra", "vidx/index/other"] {
        let t = single(json!({ "TableName": "bb", "IndexName": name }));
        assert_eq!(resource_tag_lookup_arn(&t.resource), None, "{name}");
    }
}
