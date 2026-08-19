// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for a backend that implements vector indexes.
//!
//! The mirror of `vector_index_unsupported`: that file asserts the refusals a
//! non-participating backend must give, this one asserts the behaviour a
//! participating one must give. Both probe the running backend and skip when it is
//! the wrong kind, so one suite runs everywhere.
//!
//! Hand-built JSON and SigV4 signing for the same reason as the other file: no
//! published `aws-sdk-dynamodb` models vector indexes.

use crate::vector_index_unsupported::{call, expect_vectors, table_name, vectors_supported};

/// Skip guard for this suite, with the anchor that closes the silent-green hole.
///
/// Both vector suites adapt to whatever the backend reports, so on their own they
/// can never assert *which* backend is under test. If the shipping backend silently
/// lost vector support, this suite would skip all of its assertions and the refusal
/// suite would start passing, and the whole positive contract would evaporate with
/// a green run. `EXTENDDB_EXPECT_VECTORS=1` turns that skip into a failure, so a CI
/// job can state the expectation it is there to check.
async fn skip_unless_supported() -> bool {
    let supported = vectors_supported().await;
    assert!(
        !(!supported && expect_vectors() == Some(true)),
        "EXTENDDB_EXPECT_VECTORS=1 but the backend does not support vector \
         indexes, so every assertion in this suite would be skipped: the \
         capability was lost, or the probe is broken"
    );
    !supported
}

/// Create a table with one vector index and wait for it to be usable.
async fn create_vector_table(name: &str, dims: usize, distance: &str, scoped: bool) {
    let search_schema = if scoped {
        r#""SearchSchema": [{"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}],"#
    } else {
        ""
    };
    let attr_defs = if scoped {
        r#"{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "tenant", "AttributeType": "S"}"#
    } else {
        r#"{"AttributeName": "pk", "AttributeType": "S"}"#
    };
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{attr_defs}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{{
            "IndexName": "vidx",
            "Dimensions": {dims},
            "DistanceFunction": "{distance}",
            "VectorAttribute": {{"AttributeName": "emb"}},
            {search_schema}
            "Projection": {{"ProjectionType": "ALL"}}
        }}]
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable failed: {text}");
    wait_for_active(name).await;
}

/// Poll until the table reports ACTIVE.
///
/// CreateTable returns while the table is still CREATING, and a write against a
/// CREATING table is rejected with ResourceNotFound, so without this every test
/// here fails on its first PutItem for a reason unrelated to what it asserts.
pub(crate) async fn wait_for_active(name: &str) {
    for _ in 0..100 {
        let (status, text) = call("DescribeTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
        if status == 200 && text.contains(r#""TableStatus":"ACTIVE""#) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("table {name} never became ACTIVE");
}

fn vector_json(values: &[f32]) -> String {
    let parts: Vec<String> = values
        .iter()
        .map(|v| format!(r#"{{"N": "{v}"}}"#))
        .collect();
    format!("[{}]", parts.join(", "))
}

async fn put_vector(table: &str, pk: &str, tenant: Option<&str>, values: &[f32]) {
    let tenant_attr = tenant
        .map(|t| format!(r#", "tenant": {{"S": "{t}"}}"#))
        .unwrap_or_default();
    let body = format!(
        r#"{{
        "TableName": "{table}",
        "Item": {{"pk": {{"S": "{pk}"}}, "emb": {{"L": {}}}{tenant_attr}}}
    }}"#,
        vector_json(values)
    );
    let (status, text) = call("PutItem", &body).await;
    assert_eq!(status, 200, "PutItem failed: {text}");
}

async fn search(
    table: &str,
    values: &[f32],
    top_k: usize,
    condition: Option<&str>,
) -> serde_json::Value {
    let cond = condition
        .map(|c| {
            format!(
                r#", "SearchConditionExpression": "tenant = :t", "ExpressionAttributeValues": {{":t": {{"S": "{c}"}}}}"#
            )
        })
        .unwrap_or_default();
    let body = format!(
        r#"{{
        "TableName": "{table}",
        "IndexName": "vidx",
        "SearchVector": {},
        "TopK": {top_k}{cond}
    }}"#,
        vector_json(values)
    );
    let (status, text) = call("SearchVectors", &body).await;
    assert_eq!(status, 200, "SearchVectors failed: {text}");
    serde_json::from_str(&text).expect("search response is JSON")
}

fn hit_pks(response: &serde_json::Value) -> Vec<String> {
    response
        .get("SearchResults")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("no results array in: {response}"))
        .iter()
        .map(|hit| {
            hit.get("Item")
                .and_then(|i| i.get("pk"))
                .and_then(|p| p.get("S"))
                .and_then(|s| s.as_str())
                .unwrap_or_else(|| panic!("hit has no pk: {hit}"))
                .to_owned()
        })
        .collect()
}

/// How long to let the index converge before failing.
///
/// Generous on purpose. The propagation delay itself is milliseconds, but a missed
/// worker wake falls back to a one second backstop, and these tests run in parallel
/// against one server, so the bound has to cover a loaded machine rather than a
/// quiet one. Overshooting costs nothing when propagation is fast, because every
/// helper returns as soon as its condition holds.
const CONVERGE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
const CONVERGE_POLL: std::time::Duration = std::time::Duration::from_millis(50);

/// Search until `predicate` accepts the response, then return that response.
///
/// A vector index is eventually consistent, so a single search after a write proves
/// nothing in either direction: a missing item may simply not have propagated, and
/// an item that should have been removed may not have been removed yet. Every
/// assertion about index contents therefore has to be an assertion about what the
/// index converges to.
///
/// Polling rather than sleeping is deliberate. A fixed sleep is simultaneously too
/// slow when propagation is immediate and too short when the machine is loaded,
/// which is exactly how index tests become flaky. `what` is folded into the failure
/// message so a timeout says which condition was never reached.
async fn search_until(
    table: &str,
    values: &[f32],
    top_k: usize,
    condition: Option<&str>,
    what: &str,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    let mut last = search(table, values, top_k, condition).await;
    loop {
        if predicate(&last) {
            return last;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "index never converged: {what}. Last response after {:?}: {last}",
                CONVERGE_TIMEOUT
            );
        }
        tokio::time::sleep(CONVERGE_POLL).await;
        last = search(table, values, top_k, condition).await;
    }
}

/// Poll a caller-supplied `SearchVectors` body until `predicate` accepts the parsed
/// response, then return it.
///
/// For tests whose query the `search` helper cannot express: a different filter
/// attribute, a `ProjectionExpression`, or anything else shaped by hand. Same
/// convergence contract as [`search_until`], and it asserts the status on every
/// attempt so a request that becomes malformed fails loudly instead of timing out.
async fn search_body_until(
    body: &str,
    what: &str,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let (status, text) = call("SearchVectors", body).await;
        assert_eq!(status, 200, "SearchVectors failed: {text}");
        let json: serde_json::Value = serde_json::from_str(&text).expect("search response is JSON");
        if predicate(&json) {
            return json;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "index never converged: {what}. Last response after {CONVERGE_TIMEOUT:?}: {text}"
        );
        tokio::time::sleep(CONVERGE_POLL).await;
    }
}

/// Search until the hits are exactly `expected`, in order.
///
/// Covers presence and absence in one assertion, which matters because they are not
/// separable under eventual consistency: waiting for an item to appear and then
/// asserting a different item is absent would pass whenever the second item is
/// merely late.
async fn search_until_pks(
    table: &str,
    values: &[f32],
    top_k: usize,
    condition: Option<&str>,
    expected: &[&str],
) -> serde_json::Value {
    search_until(
        table,
        values,
        top_k,
        condition,
        &format!("expected hits {expected:?}"),
        |response| hit_pks(response) == expected,
    )
    .await
}

/// Search until the index holds exactly `count` hits, for tests whose subject is the
/// number of rows rather than which rows.
async fn search_until_count(
    table: &str,
    values: &[f32],
    top_k: usize,
    condition: Option<&str>,
    count: usize,
) -> serde_json::Value {
    search_until(
        table,
        values,
        top_k,
        condition,
        &format!("expected {count} hits"),
        |response| hit_pks(response).len() == count,
    )
    .await
}

/// The nearest vector comes back first, and the ordering is by actual distance
/// rather than by insertion order.
#[tokio::test]
async fn search_returns_nearest_first() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_order");
    create_vector_table(&name, 2, "COSINE", false).await;

    // Inserted worst-first, so passing cannot be an artefact of scan order.
    put_vector(&name, "opposite", None, &[-1.0, 0.0]).await;
    put_vector(&name, "orthogonal", None, &[0.0, 1.0]).await;
    put_vector(&name, "exact", None, &[1.0, 0.0]).await;

    let response =
        search_until_pks(&name, &[1.0, 0.0], 3, None, &["exact", "orthogonal", "opposite"]).await;
    assert_eq!(
        hit_pks(&response),
        vec!["exact", "orthogonal", "opposite"],
        "response: {response}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// TopK bounds the result set.
#[tokio::test]
async fn top_k_limits_the_results() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_topk");
    create_vector_table(&name, 2, "COSINE", false).await;
    for i in 0..5 {
        put_vector(&name, &format!("i{i}"), None, &[1.0, i as f32]).await;
    }

    // Wait for all five to be indexed before bounding, otherwise a result of two
    // could just as easily mean three writes had not propagated yet.
    search_until_count(&name, &[1.0, 0.0], 10, None, 5).await;

    let response = search(&name, &[1.0, 0.0], 2, None).await;
    assert_eq!(hit_pks(&response).len(), 2, "response: {response}");

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// Overwriting an item replaces its vector rather than leaving both, which a
/// row keyed by partition instead of by base item would get wrong.
#[tokio::test]
async fn overwriting_an_item_replaces_its_vector() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_replace");
    create_vector_table(&name, 2, "COSINE", false).await;

    put_vector(&name, "a", None, &[-1.0, 0.0]).await;
    put_vector(&name, "a", None, &[1.0, 0.0]).await;

    // Converge on the score rather than the pk set: exactly one row for "a" is
    // already true after the FIRST write, so a pk assertion would be satisfied by a
    // state where the overwrite has not propagated at all.
    let response = search_until(
        &name,
        &[1.0, 0.0],
        10,
        None,
        "the overwrite to have propagated (cosine score ~0)",
        |r| {
            r.pointer("/SearchResults/0/Score")
                .and_then(serde_json::Value::as_f64)
                .is_some_and(|score| score.abs() < 1e-5)
        },
    )
    .await;
    let pks = hit_pks(&response);
    assert_eq!(pks, vec!["a"], "one row per base item: {response}");

    // Row count alone is not enough. A delete-then-insert that reinserted the OLD
    // image would also leave exactly one row for "a", and the assertion above would
    // pass while the stored vector was stale. So check the vector itself, via the
    // score: the query is the new vector, so cosine distance must be ~0, whereas
    // the old vector was its exact opposite and would score ~2.
    let score = response
        .pointer("/SearchResults/0/Score")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_else(|| panic!("no score in: {response}"));
    assert!(
        score.abs() < 1e-5,
        "the stored vector must be the NEW one (cosine ~0), got score {score}: {response}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// Deleting an item removes it from the index.
#[tokio::test]
async fn deleting_an_item_removes_it_from_the_index() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_remove");
    create_vector_table(&name, 2, "COSINE", false).await;
    put_vector(&name, "gone", None, &[1.0, 0.0]).await;
    put_vector(&name, "stays", None, &[0.0, 1.0]).await;

    // Establish that BOTH items are indexed before deleting one. Without this the
    // convergence below is satisfied by a state where "gone" was simply never
    // indexed yet, so the test could pass without the delete being exercised at all.
    search_until_pks(&name, &[1.0, 0.0], 10, None, &["gone", "stays"]).await;

    let body = format!(r#"{{"TableName": "{name}", "Key": {{"pk": {{"S": "gone"}}}}}}"#);
    let (status, text) = call("DeleteItem", &body).await;
    assert_eq!(status, 200, "DeleteItem failed: {text}");

    let response = search_until_pks(&name, &[1.0, 0.0], 10, None, &["stays"]).await;
    assert_eq!(hit_pks(&response), vec!["stays"], "response: {response}");

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// An item with no vector attribute is simply not indexed, exactly as a GSI omits
/// an item missing its index key. It must not be an error, and must not appear.
///
/// Worth being precise about what this proves, because it is less than the name
/// suggests: an item with no vector has no code path that could index it, so its
/// absence is close to trivially true. What it does prove is that a vectorless
/// PutItem is still ACCEPTED against a table carrying a vector index, which is the
/// half that could plausibly regress. The removal case, an item that had a vector
/// and loses it, is the one that needs real coverage and is asserted at the unit
/// level by `an_item_that_loses_its_vector_is_removed_from_the_index`.
#[tokio::test]
async fn an_item_without_a_vector_is_not_indexed() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_novec");
    create_vector_table(&name, 2, "COSINE", false).await;

    let body = format!(r#"{{"TableName": "{name}", "Item": {{"pk": {{"S": "novec"}}}}}}"#);
    let (status, text) = call("PutItem", &body).await;
    assert_eq!(
        status, 200,
        "a vectorless item must still be writable: {text}"
    );
    put_vector(&name, "hasvec", None, &[1.0, 0.0]).await;

    let response = search_until_pks(&name, &[1.0, 0.0], 10, None, &["hasvec"]).await;
    assert_eq!(hit_pks(&response), vec!["hasvec"], "response: {response}");

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// A scoped index searches one partition. This is the isolation property the whole
/// partition column exists for, so it is asserted from both sides: each tenant sees
/// its own item and not the other's.
#[tokio::test]
async fn a_scoped_search_sees_only_its_own_partition() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_scope");
    create_vector_table(&name, 2, "COSINE", true).await;

    put_vector(&name, "a_item", Some("tenant_a"), &[1.0, 0.0]).await;
    put_vector(&name, "b_item", Some("tenant_b"), &[1.0, 0.0]).await;

    let a = search_until_pks(&name, &[1.0, 0.0], 10, Some("tenant_a"), &["a_item"]).await;
    assert_eq!(hit_pks(&a), vec!["a_item"], "tenant_a response: {a}");

    let b = search_until_pks(&name, &[1.0, 0.0], 10, Some("tenant_b"), &["b_item"]).await;
    assert_eq!(hit_pks(&b), vec!["b_item"], "tenant_b response: {b}");

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// Moving an item between partitions must move its row, not duplicate it. A row
/// keyed by base item makes this work; keying by partition would leave the old row
/// and the item would be findable under both tenants.
#[tokio::test]
async fn changing_the_partition_attribute_moves_the_row() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_move");
    create_vector_table(&name, 2, "COSINE", true).await;

    put_vector(&name, "mover", Some("tenant_a"), &[1.0, 0.0]).await;
    put_vector(&name, "mover", Some("tenant_b"), &[1.0, 0.0]).await;

    // Wait for the destination partition first, then assert the source is empty.
    // That ordering is what makes the absence assertion sound: one apply performs
    // the delete and the insert in a single transaction, so the row appearing under
    // tenant_b proves the removal from tenant_a has already committed. Asserting the
    // absence first would pass while the move had simply not propagated.
    let b = search_until_pks(&name, &[1.0, 0.0], 10, Some("tenant_b"), &["mover"]).await;
    assert_eq!(hit_pks(&b), vec!["mover"], "tenant_b response: {b}");

    let a = search(&name, &[1.0, 0.0], 10, Some("tenant_a")).await;
    assert!(
        hit_pks(&a).is_empty(),
        "the old partition must no longer hold the row: {a}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// Dot product ranks the other way round from cosine and euclidean. A single
/// ordering would silently return the worst matches here, so the direction is
/// asserted over the wire rather than only in a unit test.
#[tokio::test]
async fn dot_product_ranks_larger_scores_first() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_dot");
    create_vector_table(&name, 2, "DOT_PRODUCT", false).await;

    put_vector(&name, "small", None, &[0.5, 0.0]).await;
    put_vector(&name, "large", None, &[4.0, 0.0]).await;
    put_vector(&name, "negative", None, &[-3.0, 0.0]).await;

    let response =
        search_until_pks(&name, &[1.0, 0.0], 3, None, &["large", "small", "negative"]).await;
    assert_eq!(
        hit_pks(&response),
        vec!["large", "small", "negative"],
        "response: {response}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// The vector attribute is withheld unless the caller names it.
///
/// Confirmed as the service's behaviour: it is returned only when explicitly asked
/// for in `ProjectionExpression`, even under a Projection of `ALL`. Asserted in both
/// directions, because either half alone would pass against a broken implementation:
/// "absent by default" passes if the attribute is never returned at all, and
/// "present when named" passes if it is always returned.
#[tokio::test]
async fn the_vector_attribute_is_returned_only_when_named() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_vecproj");
    create_vector_table(&name, 2, "COSINE", false).await;
    put_vector(&name, "a", None, &[1.0, 0.0]).await;

    // Default: withheld, but the rest of the item is still there, so this is not
    // passing merely because nothing was returned.
    let response = search_until_count(&name, &[1.0, 0.0], 5, None, 1).await;
    let item = response
        .pointer("/SearchResults/0/Item")
        .unwrap_or_else(|| panic!("no item in: {response}"));
    assert!(
        item.get("emb").is_none(),
        "the vector must be withheld by default: {item}"
    );
    assert!(
        item.get("pk").is_some(),
        "the rest of the item must still be returned: {item}"
    );

    // Named explicitly: returned.
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "IndexName": "vidx",
        "SearchVector": [{{"N": "1"}}, {{"N": "0"}}],
        "TopK": 5,
        "ProjectionExpression": "pk, emb"
    }}"#
    );
    let (status, text) = call("SearchVectors", &body).await;
    assert_eq!(
        status, 200,
        "SearchVectors with a projection failed: {text}"
    );
    let json: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    let item = json
        .pointer("/SearchResults/0/Item")
        .unwrap_or_else(|| panic!("no item in: {text}"));
    assert!(
        item.get("emb").is_some(),
        "the vector must be returned when named in ProjectionExpression: {item}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// A vector index holds 32-bit floats, so a client writing more precision than an
/// `f32` carries reads the narrowed value back from a search, while the base item
/// keeps exactly what was written.
///
/// The service's own validation is the evidence for the width: it rejects a
/// component outside `[-3.4028235E38, 3.4028235E38]`, which is exactly `f32::MAX`,
/// and names the expected type "32-bit floating point number". Reading the narrowed
/// value back could not be measured directly, because `SearchVectors` is not served
/// by the standard DynamoDB endpoint.
///
/// Both halves are asserted deliberately. Without the base-table half this would
/// also pass against an implementation that had simply corrupted the item on write.
#[tokio::test]
async fn the_index_narrows_a_vector_to_f32_but_the_item_keeps_its_own_precision() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_vecf32");
    create_vector_table(&name, 2, "COSINE", false).await;

    // More decimal places than an f32 carries. Nearest f32 is 0.12345679. No
    // trailing zero, because `N` normalisation trims one and that would confound
    // the base-item assertion below with a second, unrelated effect.
    let written = "0.1234567890123456789";
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "Item": {{"pk": {{"S": "a"}}, "emb": {{"L": [{{"N": "{written}"}}, {{"N": "0"}}]}}}}
    }}"#
    );
    let (status, text) = call("PutItem", &body).await;
    assert_eq!(status, 200, "PutItem failed: {text}");

    // The base item is untouched: narrowing belongs to the index, not the write.
    let (status, text) = call(
        "GetItem",
        &format!(
            r#"{{"TableName": "{name}", "Key": {{"pk": {{"S": "a"}}}}, "ConsistentRead": true}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {text}");
    let json: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    assert_eq!(
        json.pointer("/Item/emb/L/0/N").and_then(|v| v.as_str()),
        Some(written),
        "the base item must keep the client's own precision: {text}"
    );

    // The search returns what the index stored, which is the f32.
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "IndexName": "vidx",
        "SearchVector": [{{"N": "1"}}, {{"N": "0"}}],
        "TopK": 5,
        "ProjectionExpression": "pk, emb"
    }}"#
    );
    let json = search_body_until(&body, "the written item to be indexed", |r| {
        r.pointer("/SearchResults/0/Item/emb/L/0/N").is_some()
    })
    .await;
    assert_eq!(
        json.pointer("/SearchResults/0/Item/emb/L/0/N")
            .and_then(|v| v.as_str()),
        Some("0.12345679"),
        "the index must return the narrowed f32: {json}"
    );
    // The exactly-representable component must not acquire a decimal point.
    assert_eq!(
        json.pointer("/SearchResults/0/Item/emb/L/1/N")
            .and_then(|v| v.as_str()),
        Some("0"),
        "an exact component must round-trip unchanged: {json}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// A two-HASH search schema is rejected with the service's measured message.
///
/// This mattered because the contract accepted it and then could not honour it: the
/// query side requires a condition for every declared HASH, while the backend
/// resolves the scope from the first and demotes the rest to filters. Measured
/// against the live service on 2026-08-06.
#[tokio::test]
async fn a_two_hash_search_schema_is_rejected() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("twohash");
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [
            {{"AttributeName": "pk", "AttributeType": "S"}},
            {{"AttributeName": "a", "AttributeType": "S"}},
            {{"AttributeName": "b", "AttributeType": "S"}}
        ],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{{
            "IndexName": "vidx",
            "Dimensions": 4,
            "DistanceFunction": "COSINE",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "SearchSchema": [
                {{"AttributeName": "a", "SearchSchemaElementType": "HASH"}},
                {{"AttributeName": "b", "SearchSchemaElementType": "HASH"}}
            ],
            "Projection": {{"ProjectionType": "ALL"}}
        }}]
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    assert_eq!(status, 400, "two HASH elements must be rejected: {text}");
    assert!(
        text.contains("Member must have HASH count less than or equal to 1"),
        "expected the measured HASH-count message, got: {text}"
    );
}

/// An unknown distance function is rejected, and the enum value set is listed in
/// the service's order.
///
/// The order is measured and is neither alphabetical nor the enum's declaration
/// order, so it is asserted explicitly: an earlier version guessed alphabetical.
#[tokio::test]
async fn an_unknown_distance_function_lists_the_measured_enum_order() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("baddf");
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{{
            "IndexName": "vidx",
            "Dimensions": 4,
            "DistanceFunction": "MANHATTAN",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "Projection": {{"ProjectionType": "ALL"}}
        }}]
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    assert_eq!(
        status, 400,
        "an unknown distance function must be rejected: {text}"
    );
    assert!(
        text.contains("[DOT_PRODUCT, COSINE, EUCLIDEAN]"),
        "the enum value set must be listed in the service's measured order, got: {text}"
    );
}

/// Adding a vector index to a table that already holds items backfills them, so a
/// search finds data written before the index existed.
///
/// This is the whole point of the `UpdateTable` create path, and it is what makes
/// the difference between an index and a filter over new writes. Items are written
/// first, deliberately, so a backfill that did nothing would fail here rather than
/// pass because the write path happened to cover it.
#[tokio::test]
async fn adding_a_vector_index_backfills_the_items_already_there() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_backfill");
    // A plain table: no vector index at creation.
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST"
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable failed: {text}");
    wait_for_active(&name).await;

    // Three items with vectors, plus one without, which must be skipped rather
    // than break the scan.
    put_vector(&name, "near", None, &[1.0, 0.0]).await;
    put_vector(&name, "far", None, &[-1.0, 0.0]).await;
    put_vector(&name, "mid", None, &[0.0, 1.0]).await;
    let (status, text) = call(
        "PutItem",
        &format!(r#"{{"TableName": "{name}", "Item": {{"pk": {{"S": "novec"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "PutItem without a vector failed: {text}");

    let (status, text) = call(
        "UpdateTable",
        &format!(
            r#"{{
        "TableName": "{name}",
        "VectorIndexUpdates": [{{"Create": {{
            "IndexName": "vidx",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "Dimensions": 2,
            "DistanceFunction": "COSINE",
            "Projection": {{"ProjectionType": "ALL"}}
        }}}}]
    }}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "UpdateTable create failed: {text}");

    // The backfill is asynchronous: UpdateTable returns with the index CREATING, and
    // a search is refused until it is ACTIVE. Waiting is not test scaffolding, it is
    // what a real client has to do, and the service takes minutes over it.
    wait_for_vector_index_active(&name, "vidx").await;

    let response = search_until_count(&name, &[1.0, 0.0], 10, None, 3).await;
    let results = response
        .get("SearchResults")
        .and_then(|r| r.as_array())
        .unwrap_or_else(|| panic!("no results in: {response}"));
    assert_eq!(
        results.len(),
        3,
        "the three vector-carrying items must be backfilled, and the fourth skipped: {response}"
    );
    assert_eq!(
        results[0].pointer("/Item/pk/S").and_then(|v| v.as_str()),
        Some("near"),
        "backfilled rows must be searchable in distance order: {response}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// A backfilled index keeps working for writes that arrive afterwards, which is
/// what proves the write-path hook and the backfill agree on the row shape.
#[tokio::test]
async fn an_index_added_by_update_table_indexes_later_writes_too() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_bf_then_write");
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST"
    }}"#
    );
    call("CreateTable", &body).await;
    wait_for_active(&name).await;
    put_vector(&name, "before", None, &[1.0, 0.0]).await;

    let (status, text) = call(
        "UpdateTable",
        &format!(
            r#"{{
        "TableName": "{name}",
        "VectorIndexUpdates": [{{"Create": {{
            "IndexName": "vidx",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "Dimensions": 2,
            "DistanceFunction": "COSINE",
            "Projection": {{"ProjectionType": "ALL"}}
        }}}}]
    }}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "UpdateTable create failed: {text}");

    // Written while the index is still CREATING, deliberately. The queue does not
    // claim rows for a table whose vector index is building, so this write
    // accumulates and is applied once the index goes ACTIVE. Moving it after the
    // wait would still pass but would stop covering that path.
    put_vector(&name, "after", None, &[0.9, 0.1]).await;

    wait_for_vector_index_active(&name, "vidx").await;

    // "before" arrives via the backfill, "after" via the propagation queue once the
    // index is ACTIVE, so this converges on both being present.
    let response = search_until(
        &name,
        &[1.0, 0.0],
        10,
        None,
        "both the backfilled and the later-written item to be present",
        |r| {
            let pks = hit_pks(r);
            pks.iter().any(|k| k == "before") && pks.iter().any(|k| k == "after")
        },
    )
    .await;
    let results = response
        .get("SearchResults")
        .and_then(|r| r.as_array())
        .unwrap_or_else(|| panic!("no results in: {response}"));
    let keys: Vec<&str> = results
        .iter()
        .filter_map(|r| r.pointer("/Item/pk/S").and_then(|v| v.as_str()))
        .collect();
    assert!(
        keys.contains(&"before") && keys.contains(&"after"),
        "both the backfilled and the later-written item must be found: {keys:?}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// Deleting a vector index stops it serving, and `DescribeTable` stops reporting it.
///
/// Measured against this backend before the path existed: the delete returned 200
/// while the index stayed ACTIVE and kept returning hits, which is why both halves
/// are asserted here rather than just the status code.
#[tokio::test]
async fn deleting_a_vector_index_stops_it_serving_and_reporting() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_idxdelete");
    create_vector_table(&name, 2, "COSINE", false).await;
    put_vector(&name, "a", None, &[1.0, 0.0]).await;

    // It serves before the delete, so the assertions below cannot pass vacuously.
    // This has to converge: the write is propagated asynchronously, so a single
    // search here could find nothing and make the precondition assert falsely.
    let response = search_until_count(&name, &[1.0, 0.0], 5, None, 1).await;
    assert_eq!(
        response
            .get("SearchResults")
            .and_then(|r| r.as_array())
            .map(Vec::len),
        Some(1),
        "the index must serve before it is deleted: {response}"
    );

    let (status, text) = call(
        "UpdateTable",
        &format!(
            r#"{{"TableName": "{name}", "VectorIndexUpdates": [{{"Delete": {{"IndexName": "vidx"}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "UpdateTable delete failed: {text}");

    let (status, text) = call("DescribeTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
    assert_eq!(status, 200, "DescribeTable failed: {text}");
    let json: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    let still_there = json
        .pointer("/Table/VectorIndexes")
        .and_then(|v| v.as_array())
        .is_some_and(|a| {
            a.iter()
                .any(|i| i.pointer("/IndexName").and_then(|n| n.as_str()) == Some("vidx"))
        });
    assert!(
        !still_there,
        "a deleted index must not still be reported: {text}"
    );

    let body = format!(
        r#"{{"TableName": "{name}", "IndexName": "vidx", "SearchVector": {}, "TopK": 5}}"#,
        vector_json(&[1.0, 0.0])
    );
    let (status, text) = call("SearchVectors", &body).await;
    assert_eq!(
        status, 400,
        "searching a deleted index must fail, not return stale hits: {text}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// The base items survive the index being deleted: only the index goes.
#[tokio::test]
async fn deleting_a_vector_index_leaves_the_items_alone() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_idxdel_items");
    create_vector_table(&name, 2, "COSINE", false).await;
    put_vector(&name, "a", None, &[1.0, 0.0]).await;

    call(
        "UpdateTable",
        &format!(
            r#"{{"TableName": "{name}", "VectorIndexUpdates": [{{"Delete": {{"IndexName": "vidx"}}}}]}}"#
        ),
    )
    .await;

    let (status, text) = call(
        "GetItem",
        &format!(
            r#"{{"TableName": "{name}", "Key": {{"pk": {{"S": "a"}}}}, "ConsistentRead": true}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {text}");
    assert!(
        text.contains(r#""emb""#),
        "the item and its vector attribute must be untouched: {text}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// Creating an index that already exists, and deleting one that does not.
#[tokio::test]
async fn update_table_rejects_a_duplicate_create_and_a_missing_delete() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_idx_errs");
    create_vector_table(&name, 2, "COSINE", false).await;

    let (status, text) = call(
        "UpdateTable",
        &format!(
            r#"{{
        "TableName": "{name}",
        "VectorIndexUpdates": [{{"Create": {{
            "IndexName": "vidx",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "Dimensions": 2,
            "DistanceFunction": "COSINE",
            "Projection": {{"ProjectionType": "ALL"}}
        }}}}]
    }}"#
        ),
    )
    .await;
    assert_eq!(status, 400, "a duplicate create must be rejected: {text}");
    assert!(
        text.contains("already exists"),
        "unexpected duplicate-create message: {text}"
    );

    let (status, text) = call(
        "UpdateTable",
        &format!(
            r#"{{"TableName": "{name}", "VectorIndexUpdates": [{{"Delete": {{"IndexName": "nosuch"}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "deleting a missing index must be rejected: {text}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// DescribeTable reports the vector index, and reports it ACTIVE with no
/// `Backfilling` member, which is what the service does for an index created by
/// CreateTable.
#[tokio::test]
async fn describe_table_reports_the_vector_index() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("pos_describe");
    create_vector_table(&name, 4, "COSINE", false).await;

    let (status, text) = call("DescribeTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
    assert_eq!(status, 200, "DescribeTable failed: {text}");
    let json: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    let vidx = json
        .pointer("/Table/VectorIndexes/0")
        .unwrap_or_else(|| panic!("no vector index in description: {text}"));

    assert_eq!(
        vidx.pointer("/IndexName").and_then(|v| v.as_str()),
        Some("vidx")
    );
    assert_eq!(
        vidx.pointer("/Dimensions").and_then(|v| v.as_u64()),
        Some(4)
    );
    assert_eq!(
        vidx.pointer("/DistanceFunction").and_then(|v| v.as_str()),
        Some("COSINE")
    );
    assert_eq!(
        vidx.pointer("/IndexStatus").and_then(|v| v.as_str()),
        Some("ACTIVE")
    );
    assert!(
        vidx.get("Backfilling").is_none(),
        "an ACTIVE index must not carry Backfilling at all: {vidx}"
    );
    assert!(
        vidx.get("VectorAttribute").is_some(),
        "VectorAttribute must be reported: {vidx}"
    );
    // The ARN must name this table and this index in the service's shape. A
    // malformed or misattributed ARN would ship unnoticed with only the presence
    // check the members above get.
    let arn = vidx
        .pointer("/IndexArn")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("IndexArn must be reported: {vidx}"));
    assert!(
        arn.starts_with("arn:aws:dynamodb:") && arn.ends_with(&format!("table/{name}/index/vidx")),
        "IndexArn must have the service's table/<name>/index/<name> form: {arn}"
    );
    // Emitted as numbers, matching the service's members. Their VALUES are the
    // GSI convention in this backend (not live-maintained), so only shape is
    // asserted here; a missing member would break a generated client's model.
    assert!(
        vidx.pointer("/ItemCount").is_some_and(serde_json::Value::is_number),
        "ItemCount must be a number: {vidx}"
    );
    assert!(
        vidx.pointer("/IndexSizeBytes")
            .is_some_and(serde_json::Value::is_number),
        "IndexSizeBytes must be a number: {vidx}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// `SearchVectorsOutput` carries no `Count` field.
///
/// Measured against the live service in sandbox 964157134968 on 2026-08-10: the
/// response contains only `SearchResults` and, when asked for,
/// `ConsumedCapacity`. Five parameter variations were probed and none produced a
/// `Count`: no projection, `ReturnConsumedCapacity=INDEXES`, a
/// `ProjectionExpression`, a `TopK` larger than the item count, and a projection
/// naming a single non-key attribute. The botocore model agrees: `SearchVectorsOutput`
/// declares exactly those two members.
///
/// This is asserted rather than left to review because an extra top-level field is
/// invisible to a generated client (it ignores unknown members), so nothing else in
/// the suite would ever catch its return. The `TopK`-exceeds-matches case is
/// included deliberately: a `Count` field is most tempting to add exactly when the
/// result set is shorter than `TopK`.
#[tokio::test]
async fn the_search_response_carries_no_count_field() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("no_count");
    create_vector_table(&name, 2, "COSINE", false).await;
    put_vector(&name, "a", None, &[1.0, 0.0]).await;
    put_vector(&name, "b", None, &[0.0, 1.0]).await;

    // Case 1: plain search. Converge first, so "no Count field" is asserted against
    // a populated response rather than an empty one.
    let response = search_until_count(&name, &[1.0, 0.0], 2, None, 2).await;
    assert!(
        response.get("Count").is_none(),
        "SearchVectors must not return a Count field: {response}"
    );
    assert!(
        response.get("SearchResults").is_some(),
        "SearchResults must be present: {response}"
    );

    // Case 2: TopK larger than the number of matches, where a Count field is the
    // most tempting addition.
    let response = search_until_count(&name, &[1.0, 0.0], 50, None, 2).await;
    assert!(
        response.get("Count").is_none(),
        "SearchVectors must not return a Count field when TopK exceeds the match \
         count: {response}"
    );

    // Case 3: with ConsumedCapacity requested, so the only two legal top-level
    // members are both present and nothing else is.
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "IndexName": "vidx",
        "SearchVector": [{{"N": "1"}}, {{"N": "0"}}],
        "TopK": 2,
        "ReturnConsumedCapacity": "INDEXES"
    }}"#
    );
    let (status, text) = call("SearchVectors", &body).await;
    assert_eq!(status, 200, "SearchVectors failed: {text}");
    let json: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    let members: Vec<&str> = json
        .as_object()
        .unwrap_or_else(|| panic!("response is not an object: {text}"))
        .keys()
        .map(String::as_str)
        .collect();
    for m in &members {
        assert!(
            matches!(*m, "SearchResults" | "ConsumedCapacity"),
            "unexpected top-level member '{m}' in SearchVectorsOutput; the service \
             returns only SearchResults and ConsumedCapacity: {text}"
        );
    }
    assert!(
        members.contains(&"ConsumedCapacity"),
        "ConsumedCapacity must be present when requested: {text}"
    );
}

/// A `KEYS_ONLY` vector index still projects its search-schema attributes, so a
/// filtered search works and returns them.
///
/// The documented rule is specific and is NOT GSI `KEYS_ONLY` semantics: on a
/// vector index, `KEYS_ONLY` projects the base table primary key, the vector
/// attribute, AND any inline filter attributes declared in the SearchSchema
/// (developer guide, "Projections"). GSI `KEYS_ONLY` projects base keys plus the
/// index's own key attributes and nothing else.
///
/// This went unasserted because `create_vector_table` hardcodes
/// `ProjectionType: ALL`, so all 18 pre-existing search tests exercise the one
/// projection under which the distinction cannot appear. Under `KEYS_ONLY` the
/// inline filter attribute was dropped from the stored payload, and because the
/// filter is evaluated against that payload, `item.get(name)` returned `None` for
/// every row and a filtered search matched nothing at all.
///
/// Asserted three ways so a partial fix cannot pass: the filtered search must
/// find the item, the filter attribute must come back in the result, and a
/// non-projected attribute must NOT come back (otherwise this would also pass
/// against an implementation that quietly ignored the projection and behaved like
/// `ALL`).
#[tokio::test]
async fn keys_only_vector_index_still_filters_on_its_search_schema() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("keysonly_filter");
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [
            {{"AttributeName": "pk", "AttributeType": "S"}},
            {{"AttributeName": "category", "AttributeType": "S"}}
        ],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{{
            "IndexName": "vidx",
            "Dimensions": 2,
            "DistanceFunction": "COSINE",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "SearchSchema": [
                {{"AttributeName": "category", "SearchSchemaElementType": "INLINE_FILTER"}}
            ],
            "Projection": {{"ProjectionType": "KEYS_ONLY"}}
        }}]
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable with KEYS_ONLY failed: {text}");
    wait_for_active(&name).await;

    // `note` is deliberately non-projected under KEYS_ONLY.
    let put = format!(
        r#"{{
        "TableName": "{name}",
        "Item": {{
            "pk": {{"S": "a"}},
            "category": {{"S": "books"}},
            "note": {{"S": "not projected"}},
            "emb": {{"L": [{{"N": "1"}}, {{"N": "0"}}]}}
        }}
    }}"#
    );
    let (status, text) = call("PutItem", &put).await;
    assert_eq!(status, 200, "PutItem failed: {text}");

    let search = format!(
        r#"{{
        "TableName": "{name}",
        "IndexName": "vidx",
        "SearchVector": [{{"N": "1"}}, {{"N": "0"}}],
        "TopK": 5,
        "SearchConditionExpression": "category = :c",
        "ExpressionAttributeValues": {{":c": {{"S": "books"}}}}
    }}"#
    );
    // Converge on the filtered query itself. Before the projection fix this returned
    // an empty result set permanently, so a timeout here is the defect reappearing
    // rather than slow propagation.
    let json = search_body_until(
        &search,
        "the KEYS_ONLY index to match its inline filter",
        |r| {
            r.get("SearchResults")
                .and_then(|v| v.as_array())
                .is_some_and(|a| !a.is_empty())
        },
    )
    .await;
    let text = json.to_string();
    let results = json
        .get("SearchResults")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("no results array in: {text}"));
    assert_eq!(
        results.len(),
        1,
        "a KEYS_ONLY index must still match its inline filter; the filter \
         attribute has to be projected for the filter to be evaluable: {text}"
    );

    let item = results[0]
        .get("Item")
        .unwrap_or_else(|| panic!("no item in: {text}"));
    assert!(
        item.get("category").is_some(),
        "KEYS_ONLY must project inline filter attributes, so 'category' must be \
         returned: {item}"
    );
    assert!(
        item.get("pk").is_some(),
        "KEYS_ONLY must project the base table primary key: {item}"
    );
    assert!(
        item.get("note").is_none(),
        "KEYS_ONLY must NOT project an unrelated non-key attribute; returning it \
         would mean the projection was ignored entirely: {item}"
    );
}

/// A search against a HASH-scoped index must be REFUSED without a condition, not
/// answered with an empty result set.
///
/// This is the divergence that mattered most of everything found by differential
/// testing against the live service, because it was a silent wrong answer rather
/// than an error. The index scopes to one partition via its HASH element, and with
/// no condition there was no partition to scope to, so the search ran unscoped and
/// returned HTTP 200 with zero results. A caller who forgot the condition was told
/// "no matches" for a request the service rejects outright, which is
/// indistinguishable from an empty table.
///
/// The code carried a comment asserting that "validation upstream guarantees it is
/// present here". No such validation existed.
///
/// Data is inserted first, and the positive case asserts a non-empty result, so
/// "zero results" cannot be mistaken for correct behaviour on an empty index: the
/// test would pass against the broken build if it only checked the refusal.
///
/// Message measured against DynamoDB in us-east-1 on 2026-08-11.
#[tokio::test]
async fn a_hash_scoped_index_requires_a_search_condition() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vi_hash_required");
    create_vector_table(&name, 2, "COSINE", true).await;

    for (pk, tenant, emb) in [("a", "t1", "1.0"), ("b", "t1", "0.9"), ("c", "t2", "0.1")] {
        let (status, text) = call(
            "PutItem",
            &format!(
                r#"{{"TableName": "{name}", "Item": {{
                    "pk": {{"S": "{pk}"}},
                    "tenant": {{"S": "{tenant}"}},
                    "emb": {{"L": [{{"N": "{emb}"}}, {{"N": "0.1"}}]}}
                }}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem failed: {text}");
    }

    // The scoped search works and finds data, which is what makes the refusal
    // below meaningful rather than vacuous.
    let scoped = search_until_count(&name, &[1.0, 0.1], 10, Some("t1"), 2).await;
    assert_eq!(
        hit_pks(&scoped).len(),
        2,
        "the scoped search must find both t1 items before the refusal is meaningful: {scoped}"
    );

    // No SearchConditionExpression at all.
    let (status, text) = call(
        "SearchVectors",
        &format!(
            r#"{{"TableName": "{name}", "IndexName": "vidx",
                "SearchVector": [{{"N": "1.0"}}, {{"N": "0.1"}}], "TopK": 10}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "an omitted SearchConditionExpression must be refused, not answered with an \
         empty result set: {text}"
    );
    assert!(
        text.contains("SearchConditionExpression must be provided when SearchSchema has a HASH key"),
        "expected the measured service message: {text}"
    );

    // An expression that IS supplied, references only IN-SCHEMA attributes, and
    // still omits the HASH leaves the search equally unscoped. The attribute must
    // be in the schema for this to test the intended branch: an out-of-schema
    // attribute (an earlier version used the base `pk`) is refused by the
    // "not in SearchSchema" validation first, so the omitted-HASH rule was never
    // exercised. That needs a schema with more than the HASH element, so this
    // sub-case builds its own.
    let name2 = table_name("vi_hash_omitted");
    let body = format!(
        r#"{{
        "TableName": "{name2}",
        "AttributeDefinitions": [
            {{"AttributeName": "pk", "AttributeType": "S"}},
            {{"AttributeName": "tenant", "AttributeType": "S"}},
            {{"AttributeName": "category", "AttributeType": "S"}}
        ],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{{
            "IndexName": "vidx",
            "Dimensions": 2,
            "DistanceFunction": "COSINE",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "SearchSchema": [
                {{"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}},
                {{"AttributeName": "category", "SearchSchemaElementType": "INLINE_FILTER"}}
            ],
            "Projection": {{"ProjectionType": "ALL"}}
        }}]
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable failed: {text}");
    wait_for_active(&name2).await;

    // The wording of this one is not measured against the service, so only the
    // refusal is asserted, not the text.
    let (status, text) = call(
        "SearchVectors",
        &format!(
            r#"{{"TableName": "{name2}", "IndexName": "vidx",
                "SearchVector": [{{"N": "1.0"}}, {{"N": "0.1"}}], "TopK": 10,
                "SearchConditionExpression": "category = :c",
                "ExpressionAttributeValues": {{":c": {{"S": "x"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "an in-schema expression that omits the HASH attribute must be refused: {text}"
    );
    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name2}"}}"#)).await;
}

/// An index that is still building must be reported by DescribeTable and refused by
/// SearchVectors, and must serve once it is ACTIVE.
///
/// The backfill is detached and commits per batch, so a search can now arrive while
/// the index holds only part of the data. The service refuses that, and reports it by
/// saying the table does not have the index at all rather than naming the status:
/// measured four times against DynamoDB on 2026-08-11 against an index in CREATING.
///
/// Both halves matter. Asserting only the refusal would also pass if the index never
/// became usable, and asserting only the success would also pass if it were searchable
/// while incomplete.
#[tokio::test]
async fn a_building_vector_index_is_visible_but_not_searchable() {
    if skip_unless_supported().await {
        return;
    }
    set_backfill_delay(3000).await;
    let name = table_name("vi_building");
    let (status, text) = call(
        "CreateTable",
        &format!(
            r#"{{"TableName": "{name}",
                "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
                "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
                "BillingMode": "PAY_PER_REQUEST"}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {text}");
    wait_for_active(&name).await;

    // More than one batch, so the delay is actually reached.
    for i in 0..600 {
        put_vector(&name, &format!("k{i:06}"), None, &[1.0, 0.0]).await;
    }

    let (status, text) = call(
        "UpdateTable",
        &format!(
            r#"{{"TableName": "{name}", "VectorIndexUpdates": [{{"Create": {{
                "IndexName": "vidx", "Dimensions": 2, "DistanceFunction": "COSINE",
                "VectorAttribute": {{"AttributeName": "emb"}},
                "Projection": {{"ProjectionType": "ALL"}}}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "UpdateTable failed: {text}");
    let described: serde_json::Value = serde_json::from_str(&text).expect("json");
    let vi = &described["TableDescription"]["VectorIndexes"][0];
    assert_eq!(
        vi["IndexStatus"], "CREATING",
        "UpdateTable must return while the index is still building: {text}"
    );

    // DescribeTable must still report it, with its status.
    let (_, text) = call("DescribeTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
    let d: serde_json::Value = serde_json::from_str(&text).expect("json");
    assert_eq!(
        d["Table"]["VectorIndexes"][0]["IndexStatus"], "CREATING",
        "a building index must remain visible in DescribeTable: {text}"
    );

    // But it must not serve.
    let (status, text) = call(
        "SearchVectors",
        &format!(
            r#"{{"TableName": "{name}", "IndexName": "vidx",
                "SearchVector": [{{"N": "1.0"}}, {{"N": "0.0"}}], "TopK": 5}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "a search against a building index must be refused, not answered from partial \
         data: {text}"
    );
    assert!(
        text.contains("The table does not have the specified index"),
        "expected the measured service message: {text}"
    );

    // And it must serve once ACTIVE, or the refusal above is worthless.
    wait_for_vector_index_active(&name, "vidx").await;
    let hits = search_until_count(&name, &[1.0, 0.0], 5, None, 5).await;
    assert_eq!(
        hit_pks(&hits).len(),
        5,
        "the index must serve once ACTIVE: {hits}"
    );
    set_backfill_delay(0).await;
}

/// Removing a row during a backfill must not cause another row to be skipped.
///
/// The backfill pages through the base table. Anchoring those pages on an OFFSET meant
/// that removing any already-scanned row shifted every later position by one, so the
/// next batch skipped a row, which was then missing from the index permanently: no
/// queue entry can repair it, because the skipped row was never written to. Only the
/// removed one was. Pagination is anchored on the key instead.
///
/// The probe row sits at the batch boundary (index 500, with BATCH = 500) and is the
/// only row pointing in its direction, so its absence is unambiguous rather than being
/// masked by its neighbours.
#[tokio::test]
async fn removing_a_row_during_a_backfill_does_not_skip_another() {
    if skip_unless_supported().await {
        return;
    }
    set_backfill_delay(3000).await;
    let name = table_name("vi_noskip");
    let (status, text) = call(
        "CreateTable",
        &format!(
            r#"{{"TableName": "{name}",
                "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
                "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
                "BillingMode": "PAY_PER_REQUEST"}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {text}");
    wait_for_active(&name).await;

    for i in 0..600 {
        // Only the boundary row points along y.
        let v = if i == 500 { [0.0, 1.0] } else { [1.0, 0.0] };
        put_vector(&name, &format!("k{i:06}"), None, &v).await;
    }

    let (status, text) = call(
        "UpdateTable",
        &format!(
            r#"{{"TableName": "{name}", "VectorIndexUpdates": [{{"Create": {{
                "IndexName": "vidx", "Dimensions": 2, "DistanceFunction": "COSINE",
                "VectorAttribute": {{"AttributeName": "emb"}},
                "Projection": {{"ProjectionType": "ALL"}}}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "UpdateTable failed: {text}");

    // Remove a row from the range the first batch has already scanned.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let (status, text) = call(
        "DeleteItem",
        &format!(r#"{{"TableName": "{name}", "Key": {{"pk": {{"S": "k000010"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "DeleteItem failed: {text}");

    wait_for_vector_index_active(&name, "vidx").await;
    let hits = search_until_pks(&name, &[0.0, 1.0], 1, None, &["k000500"]).await;
    assert_eq!(
        hit_pks(&hits),
        vec!["k000500"],
        "the row at the batch boundary must still be indexed after a removal shifted \
         the scan: {hits}"
    );
    set_backfill_delay(0).await;
}

/// Set the vector backfill batch delay via the management API.
///
/// A backfill over a test-sized table otherwise finishes faster than a client can
/// issue its next request, so a test that wanted a write to land mid-backfill would be
/// racing it and would pass whether or not the behaviour under test is correct.
///
/// A missing admin password is a hard failure when `EXTENDDB_EXPECT_VECTORS=1`, rather
/// than a skip. The vector suites exist to be run, and the repo already had a suite
/// that skipped itself and reported green when this variable was absent, which is the
/// exact failure mode `EXTENDDB_EXPECT_VECTORS` was introduced to close.
async fn set_backfill_delay(ms: u64) {
    let user = std::env::var("EXTENDDB_ADMIN_USER").unwrap_or_else(|_| "admin".into());
    let Ok(pass) = std::env::var("EXTENDDB_ADMIN_PASSWORD") else {
        assert!(
            std::env::var("EXTENDDB_EXPECT_VECTORS").as_deref() != Ok("1"),
            "EXTENDDB_EXPECT_VECTORS=1 but EXTENDDB_ADMIN_PASSWORD is unset, so the \
             backfill tests cannot set the batch delay and would silently pass without \
             testing anything. Run via devtools/run-tests --extenddb \
             --rust-integration, which provisions it."
        );
        eprintln!("SKIP: EXTENDDB_ADMIN_PASSWORD unset; backfill delay left alone");
        return;
    };
    let ep = std::env::var("EXTENDDB_TEST_ENDPOINT")
        .unwrap_or_else(|_| "https://127.0.0.1:18443".to_owned());
    let base = format!("{}/management", ep.trim_end_matches('/'));
    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // localhost self-signed; test only
        .build()
        .expect("reqwest build");
    let r = http
        .put(format!("{base}/settings/vector_backfill_batch_delay_ms"))
        .basic_auth(&user, Some(&pass))
        .json(&serde_json::json!({ "value": ms.to_string() }))
        .send()
        .await
        .expect("settings send");
    let status = r.status();
    let body = r.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "setting the backfill delay failed ({status}): {body}"
    );
}

/// Poll until a named vector index reports `ACTIVE`.
///
/// [`wait_for_active`] only waits on `TableStatus`, which is ACTIVE throughout an
/// index build, so it returns immediately and would let a test search a building
/// index and misread the refusal as a failure.
pub(crate) async fn wait_for_vector_index_active(table: &str, index: &str) {
    for _ in 0..600 {
        let (status, text) = call("DescribeTable", &format!(r#"{{"TableName": "{table}"}}"#)).await;
        if status == 200 {
            let d: serde_json::Value = serde_json::from_str(&text).expect("json");
            let found = d["Table"]["VectorIndexes"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|vi| vi["IndexName"] == index && vi["IndexStatus"] == "ACTIVE");
            if found {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("vector index {index} on {table} never became ACTIVE");
}

/// A vector index requires on-demand billing, and there is a documented cap of
/// five vector indexes per table.
///
/// Both are documented service constraints that the emulator previously accepted:
/// "Vector indexes are supported only on tables that use on-demand capacity mode"
/// under Requirements and limitations, and "Vector indexes per table: 5" in the
/// quota table. Accepting a combination the service refuses is the more dangerous
/// direction of divergence, because code written against the emulator then fails
/// on first contact with DynamoDB.
///
/// The PROVISIONED case is asserted twice, once explicitly and once with
/// `BillingMode` omitted, because the field defaults to PROVISIONED when absent
/// and an implementation that only checked the explicit value would pass the first
/// and fail the second. The five-index case asserts the boundary in both
/// directions so an off-by-one cannot hide: five is accepted, six is refused.
#[tokio::test]
async fn vector_indexes_require_on_demand_and_cap_at_five() {
    if skip_unless_supported().await {
        return;
    }

    fn one_index(n: usize) -> String {
        format!(
            r#"{{
            "IndexName": "vidx{n}",
            "Dimensions": 2,
            "DistanceFunction": "COSINE",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "Projection": {{"ProjectionType": "ALL"}}
        }}"#
        )
    }
    fn body(name: &str, billing: Option<&str>, count: usize) -> String {
        let billing_line = match billing {
            Some(b) => format!(r#""BillingMode": "{b}","#),
            None => String::new(),
        };
        let indexes = (0..count).map(one_index).collect::<Vec<_>>().join(", ");
        format!(
            r#"{{
            "TableName": "{name}",
            "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
            "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
            {billing_line}
            "VectorIndexes": [{indexes}]
        }}"#
        )
    }

    // PROVISIONED, stated explicitly. ProvisionedThroughput is supplied so the
    // rejection cannot be attributed to a missing-throughput error instead.
    let name = table_name("vi_prov");
    let mut prov = body(&name, Some("PROVISIONED"), 1);
    prov = prov.replace(
        r#""VectorIndexes""#,
        r#""ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}, "VectorIndexes""#,
    );
    let (status, text) = call("CreateTable", &prov).await;
    assert_eq!(
        status, 400,
        "a PROVISIONED table must not accept a vector index: {text}"
    );
    assert!(
        text.contains("PAY_PER_REQUEST"),
        "the error should name the required billing mode: {text}"
    );

    // BillingMode omitted, which defaults to PROVISIONED.
    let name = table_name("vi_default");
    let (status, text) = call("CreateTable", &body(&name, None, 1)).await;
    assert_eq!(
        status, 400,
        "an omitted BillingMode defaults to PROVISIONED and must be refused: {text}"
    );

    // Six indexes: over the documented cap of five.
    let name = table_name("vi_six");
    let (status, text) = call("CreateTable", &body(&name, Some("PAY_PER_REQUEST"), 6)).await;
    assert_eq!(
        status, 400,
        "six vector indexes exceeds the documented cap of five: {text}"
    );

    // Five indexes: exactly at the cap, must be accepted. This is what makes the
    // check above an off-by-one test rather than a blanket refusal.
    let name = table_name("vi_five");
    let (status, text) = call("CreateTable", &body(&name, Some("PAY_PER_REQUEST"), 5)).await;
    assert_eq!(
        status, 200,
        "five vector indexes is at the documented cap and must be accepted: {text}"
    );
    wait_for_active(&name).await;
}

/// BatchWriteItem maintains the vector index exactly as PutItem and DeleteItem do.
///
/// The engine routes batch entries through the same `.put_item` / `.delete_item`
/// paths, so this held in code from the start, but it had no wire coverage: a
/// refactor that gave BatchWriteItem its own storage path could silently stop
/// maintaining the index and every existing test would stay green. Both halves are
/// exercised: puts must appear in the index, and a batched delete must remove one
/// while leaving the other, so a converged-but-empty index cannot pass as "deleted".
#[tokio::test]
async fn batch_write_item_maintains_the_index() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vi_batch_write");
    create_vector_table(&name, 2, "COSINE", false).await;

    let (status, text) = call(
        "BatchWriteItem",
        &format!(
            r#"{{"RequestItems": {{"{name}": [
                {{"PutRequest": {{"Item": {{"pk": {{"S": "b1"}},
                    "emb": {{"L": [{{"N": "1.0"}}, {{"N": "0.0"}}]}}}}}}}},
                {{"PutRequest": {{"Item": {{"pk": {{"S": "b2"}},
                    "emb": {{"L": [{{"N": "0.9"}}, {{"N": "0.1"}}]}}}}}}}}
            ]}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "BatchWriteItem failed: {text}");
    search_until_pks(&name, &[1.0, 0.0], 10, None, &["b1", "b2"]).await;

    let (status, text) = call(
        "BatchWriteItem",
        &format!(
            r#"{{"RequestItems": {{"{name}": [
                {{"DeleteRequest": {{"Key": {{"pk": {{"S": "b1"}}}}}}}}
            ]}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "BatchWriteItem delete failed: {text}");
    // Converging on the survivor rather than on absence: "b2 alone" is ordered
    // behind the delete in the same queue, so it cannot be satisfied by a window
    // where the delete simply has not applied yet.
    search_until_pks(&name, &[1.0, 0.0], 10, None, &["b2"]).await;

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// TransactWriteItems maintains the vector index for both a transactional Put and
/// a transactional Delete.
///
/// The transactional path enqueues index maintenance in its own storage code
/// (`transactions.rs`), a sibling of the ordinary write paths rather than a caller
/// of them, so wire coverage here guards a genuinely separate implementation. The
/// unit tests cover the enqueue; this proves the queue rows drain into search
/// results over the wire.
#[tokio::test]
async fn transact_write_items_maintains_the_index() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vi_transact_write");
    create_vector_table(&name, 2, "COSINE", false).await;
    put_vector(&name, "keep", None, &[0.5, 0.5]).await;
    search_until_pks(&name, &[0.5, 0.5], 10, None, &["keep"]).await;

    let (status, text) = call(
        "TransactWriteItems",
        &format!(
            r#"{{"TransactItems": [
                {{"Put": {{"TableName": "{name}", "Item": {{"pk": {{"S": "txput"}},
                    "emb": {{"L": [{{"N": "1.0"}}, {{"N": "0.0"}}]}}}}}}}},
                {{"Delete": {{"TableName": "{name}", "Key": {{"pk": {{"S": "keep"}}}}}}}}
            ]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "TransactWriteItems failed: {text}");
    // One converged assertion covers both halves: the put must appear AND the
    // delete's target must be gone, and since both rode one transaction their
    // queue rows drain together.
    search_until_pks(&name, &[1.0, 0.0], 10, None, &["txput"]).await;

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// An UpdateItem whose UpdateExpression REMOVEs the vector attribute takes the item
/// out of the index, and one that SETs it back re-admits it.
///
/// The storage unit tests prove the queue applies a vectorless image as a removal;
/// this proves the whole wire path agrees: expression parsing, the update write,
/// the enqueue, and the drain. The re-admission half exists so the removal cannot
/// pass by accident of a broken index that returns nothing for any query.
#[tokio::test]
async fn update_item_removing_the_vector_attribute_leaves_the_index() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vi_update_remove");
    create_vector_table(&name, 2, "COSINE", false).await;
    put_vector(&name, "a", None, &[1.0, 0.0]).await;
    put_vector(&name, "b", None, &[0.9, 0.1]).await;
    search_until_pks(&name, &[1.0, 0.0], 10, None, &["a", "b"]).await;

    let (status, text) = call(
        "UpdateItem",
        &format!(
            r#"{{"TableName": "{name}", "Key": {{"pk": {{"S": "a"}}}},
                "UpdateExpression": "REMOVE emb"}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "UpdateItem REMOVE failed: {text}");
    // "b alone" is ordered behind the REMOVE in the queue, so it cannot pass in
    // the window before the REMOVE applies.
    search_until_pks(&name, &[1.0, 0.0], 10, None, &["b"]).await;

    let (status, text) = call(
        "UpdateItem",
        &format!(
            r#"{{"TableName": "{name}", "Key": {{"pk": {{"S": "a"}}}},
                "UpdateExpression": "SET emb = :v",
                "ExpressionAttributeValues": {{":v": {{"L": [{{"N": "1.0"}}, {{"N": "0.0"}}]}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "UpdateItem SET failed: {text}");
    search_until_pks(&name, &[1.0, 0.0], 10, None, &["a", "b"]).await;

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// A malformed vector cannot slip past write validation inside `if_not_exists`.
///
/// `SET emb = if_not_exists(emb, :v)` persists `:v` verbatim whenever the
/// attribute is absent, which is exactly the first write of a vector, yet an
/// earlier extraction only surfaced bare `SET emb = :v` for validation. The
/// bypass was concrete: a wrong-dimension vector through the fallback form
/// returned 200 and the item was silently omitted from the index, where the
/// service refuses the write. Both forms must now be refused identically, and
/// the valid fallback form must still work end to end.
#[tokio::test]
async fn if_not_exists_cannot_smuggle_a_malformed_vector() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vi_ine_validate");
    create_vector_table(&name, 2, "COSINE", false).await;

    // Wrong dimensions through the fallback form must be refused.
    let (status, text) = call(
        "UpdateItem",
        &format!(
            r#"{{"TableName": "{name}", "Key": {{"pk": {{"S": "a"}}}},
                "UpdateExpression": "SET emb = if_not_exists(emb, :v)",
                "ExpressionAttributeValues": {{":v": {{"L": [{{"N": "1.0"}}, {{"N": "2.0"}}, {{"N": "3.0"}}]}}}}}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "a wrong-dimension vector through if_not_exists must be refused, \
         not accepted and silently omitted from the index: {text}"
    );

    // The valid fallback form must still work and reach the index.
    let (status, text) = call(
        "UpdateItem",
        &format!(
            r#"{{"TableName": "{name}", "Key": {{"pk": {{"S": "a"}}}},
                "UpdateExpression": "SET emb = if_not_exists(emb, :v)",
                "ExpressionAttributeValues": {{":v": {{"L": [{{"N": "1.0"}}, {{"N": "0.0"}}]}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "a valid vector through if_not_exists must work: {text}");
    search_until_pks(&name, &[1.0, 0.0], 10, None, &["a"]).await;

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// Assert a 400 `ValidationException` whose message is the whole measured string.
///
/// Whole-string, because every wording quirk in this family is load-bearing and a
/// `contains` assertion is exactly what let three of them drift.
fn assert_validation_message(status: u16, body: &str, expected: &str) {
    assert_eq!(status, 400, "expected HTTP 400, body: {body}");
    let json: serde_json::Value = serde_json::from_str(body).expect("body is JSON");
    let type_field = json
        .get("__type")
        .and_then(|t| t.as_str())
        .unwrap_or_else(|| panic!("no __type in body: {body}"));
    assert!(
        type_field.ends_with("ValidationException"),
        "expected ValidationException, got {type_field} (body: {body})"
    );
    let message = json
        .get("message")
        .or_else(|| json.get("Message"))
        .and_then(|m| m.as_str())
        .unwrap_or_else(|| panic!("no message in body: {body}"));
    assert_eq!(message, expected);
}

/// `SearchVectors` reports its charge under both measured member names.
///
/// Measured on 2026-08-19 (probe P8) against real Amazon DynamoDB: the response
/// carries `{"VectorSearchRequestBytes": N, "VectorSearchUnits": N}` with the two
/// always equal, under `INDEXES` and `TOTAL` alike, and carries neither
/// `TableName` nor `CapacityUnits`. ExtendDB emitted the bytes member alone, so a
/// client reading the units member got `null` where the service gives a number.
///
/// The absent members are asserted as well as the present ones: a `ConsumedCapacity`
/// built from the ordinary table-capacity shape would satisfy a present-members-only
/// check while returning two members no vector search ever returns.
#[tokio::test]
async fn the_search_charge_reports_both_measured_capacity_members() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vcap_shape");
    create_vector_table(&name, 4, "COSINE", false).await;
    put_vector(&name, "a", None, &[1.0, 0.0, 0.0, 0.0]).await;
    put_vector(&name, "b", None, &[0.0, 1.0, 0.0, 0.0]).await;
    // Converge first, so the charge is measured against a populated result set.
    search_until_count(&name, &[1.0, 0.0, 0.0, 0.0], 2, None, 2).await;

    for granularity in ["INDEXES", "TOTAL"] {
        let body = format!(
            r#"{{
            "TableName": "{name}",
            "IndexName": "vidx",
            "SearchVector": [{{"N": "1"}}, {{"N": "0"}}, {{"N": "0"}}, {{"N": "0"}}],
            "TopK": 2,
            "ReturnConsumedCapacity": "{granularity}"
        }}"#
        );
        let (status, text) = call("SearchVectors", &body).await;
        assert_eq!(status, 200, "SearchVectors failed: {text}");
        let json: serde_json::Value = serde_json::from_str(&text).expect("JSON");
        let capacity = json
            .get("ConsumedCapacity")
            .unwrap_or_else(|| panic!("no ConsumedCapacity at {granularity}: {text}"));
        let mut members: Vec<&str> = capacity
            .as_object()
            .unwrap_or_else(|| panic!("ConsumedCapacity is not an object: {text}"))
            .keys()
            .map(String::as_str)
            .collect();
        members.sort_unstable();
        assert_eq!(
            members,
            ["VectorSearchRequestBytes", "VectorSearchUnits"],
            "at {granularity}, the search charge must carry exactly the two \
             measured members: {text}"
        );
        let bytes = capacity["VectorSearchRequestBytes"]
            .as_f64()
            .unwrap_or_else(|| panic!("VectorSearchRequestBytes is not a number: {text}"));
        let units = capacity["VectorSearchUnits"]
            .as_f64()
            .unwrap_or_else(|| panic!("VectorSearchUnits is not a number: {text}"));
        assert!(
            (bytes - units).abs() < f64::EPSILON,
            "the two members must be equal, got {bytes} and {units}: {text}"
        );
        assert!(
            bytes >= 1024.0,
            "the measured floor is 1024, got {bytes}: {text}"
        );
    }

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// Every invalid search-vector component returns one measured whole string.
///
/// Probe P4, 2026-08-19: `NaN`, `Infinity` and a value outside the f32 range all
/// produce the identical message on the search path, so the caller cannot tell
/// the three causes apart. ExtendDB returned its first sentence only.
///
/// All three inputs are exercised rather than one, because they take different
/// code paths (parse failure, non-finite parse, out-of-range parse) and the
/// service collapses them deliberately.
#[tokio::test]
async fn an_invalid_search_vector_reports_the_measured_whole_string() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vsearch_invalid");
    create_vector_table(&name, 2, "COSINE", false).await;
    put_vector(&name, "a", None, &[1.0, 0.0]).await;

    for component in ["NaN", "Infinity", "3.5E38"] {
        let body = format!(
            r#"{{
            "TableName": "{name}",
            "IndexName": "vidx",
            "SearchVector": [{{"N": "{component}"}}, {{"N": "0"}}],
            "TopK": 1
        }}"#
        );
        let (status, text) = call("SearchVectors", &body).await;
        assert_validation_message(
            status,
            &text,
            "Search vector contains invalid values. All values in the search vector must be a \
             32-bit floating-point number attribute",
        );
    }

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// The write path's three vector rejections, each as a whole measured string.
///
/// Probes P4, P5, P9 and P10 on 2026-08-19, all against index `vidx` on
/// attribute `emb`, which is what this suite's fixture builds, so these compare
/// byte for byte against the captured wire responses rather than against a
/// re-templated form of them. PR 244 recorded wrong-dimension wire coverage as a
/// known gap; this closes it.
///
/// Both directions of the size error are asserted (too short and too long) and
/// UpdateItem as well as PutItem, because the service was measured to use one
/// template for all four and a per-path template is the natural way to get it
/// wrong.
#[tokio::test]
async fn an_invalid_written_vector_reports_the_measured_whole_strings() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vwrite_invalid");
    create_vector_table(&name, 4, "COSINE", false).await;
    put_vector(&name, "indexed1", None, &[0.6, 0.8, 0.0, 0.0]).await;

    let put = |pk: &str, value: &str| {
        let body = format!(
            r#"{{
            "TableName": "{name}",
            "Item": {{"pk": {{"S": "{pk}"}}, "emb": {value}}}
        }}"#
        );
        async move { call("PutItem", &body).await }
    };

    // Too short.
    let (status, text) = put(
        "short",
        r#"{"L": [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}]}"#,
    )
    .await;
    assert_validation_message(
        status,
        &text,
        "One or more parameter values are not valid. One or more parameter values were invalid . \
         Invalid size for parameter emb, Expected: 4, Actual: 3. IndexName: vidx",
    );

    // Too long: same template, different count.
    let (status, text) = put(
        "long",
        r#"{"L": [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}, {"N": "0.5"}]}"#,
    )
    .await;
    assert_validation_message(
        status,
        &text,
        "One or more parameter values are not valid. One or more parameter values were invalid . \
         Invalid size for parameter emb, Expected: 4, Actual: 5. IndexName: vidx",
    );

    // Wrong attribute type: a String where the index expects a list. Sparse
    // semantics cover a MISSING attribute only, so this is a refused write.
    let (status, text) = put("strattr", r#"{"S": "not-a-vector"}"#).await;
    assert_validation_message(
        status,
        &text,
        "One or more parameter values are not valid. One or more parameter values were invalid . \
         Invalid type for parameter emb, Expected: L, Actual: S. IndexName: vidx",
    );

    // A valid DynamoDB number that no f32 can hold. The service echoes it in its
    // own normalised form, 3.5E+38 for the submitted 3.5E38.
    let (status, text) = put(
        "overflow",
        r#"{"L": [{"N": "0"}, {"N": "3.5E38"}, {"N": "0"}, {"N": "0"}]}"#,
    )
    .await;
    assert_validation_message(
        status,
        &text,
        "One or more parameter values are not valid. One or more parameter values were invalid . \
         Invalid value for parameter emb[1], Value: 3.5E+38 is outside valid range \
         [-3.4028235E38, 3.4028235E38]. IndexName: vidx",
    );

    // UpdateItem shares PutItem's wording exactly, on an item already indexed.
    let (status, text) = call(
        "UpdateItem",
        &format!(
            r#"{{"TableName": "{name}", "Key": {{"pk": {{"S": "indexed1"}}}},
                "UpdateExpression": "SET emb = :v",
                "ExpressionAttributeValues": {{":v": {{"L": [{{"N": "0.1"}}, {{"N": "0.2"}}, {{"N": "0.3"}}]}}}}}}"#
        ),
    )
    .await;
    assert_validation_message(
        status,
        &text,
        "One or more parameter values are not valid. One or more parameter values were invalid . \
         Invalid size for parameter emb, Expected: 4, Actual: 3. IndexName: vidx",
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}
