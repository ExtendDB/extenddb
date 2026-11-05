// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// ListBulkOperations.

#[tokio::test]
async fn list_defaults_to_a_page_of_10() {
    // Measured: an absent MaxResults answers 10 rows; 25 stays the
    // ceiling an explicit value may name.
    let h = Harness::new().await;
    let base = 1_700_000_000_000_i64;
    let mut ids = Vec::new();
    for i in 0..12 {
        ids.push(raw_op(&h, ACCOUNT, i, base + i64::from(i) * 1000).await);
    }
    let response = call(&base_ctx(&h), "ListBulkOperations", json!({}))
        .await
        .expect("list");
    let arns = summary_arns(&response);
    assert_eq!(arns.len(), 10);
    assert_eq!(arns[0], arn_of(ACCOUNT, &ids[11]), "newest first");
    assert_eq!(arns[9], arn_of(ACCOUNT, &ids[2]));
    assert_token_shape(
        response["NextToken"]
            .as_str()
            .expect("an 11th row forces a NextToken"),
    );
}

#[tokio::test]
async fn list_accepts_an_explicit_25() {
    let h = Harness::new().await;
    let base = 1_700_000_000_000_i64;
    for i in 0..26 {
        raw_op(&h, ACCOUNT, i, base + i64::from(i) * 1000).await;
    }
    let response = call(
        &base_ctx(&h),
        "ListBulkOperations",
        json!({ "MaxResults": 25 }),
    )
    .await
    .expect("list");
    assert_eq!(summary_arns(&response).len(), 25, "25 stays the ceiling");
}

#[tokio::test]
async fn list_rejects_max_results_zero() {
    let h = Harness::new().await;
    let message = expect_validation(
        call(
            &base_ctx(&h),
            "ListBulkOperations",
            json!({"MaxResults": 0}),
        )
        .await,
    );
    assert_eq!(
        message,
        "1 validation error detected: Value '0' at 'maxResults' failed to satisfy \
         constraint: Member must have value greater than or equal to 1"
    );
}

#[tokio::test]
async fn list_rejects_max_results_26() {
    let h = Harness::new().await;
    let message = expect_validation(
        call(
            &base_ctx(&h),
            "ListBulkOperations",
            json!({"MaxResults": 26}),
        )
        .await,
    );
    assert_eq!(
        message,
        "1 validation error detected: Value '26' at 'maxResults' failed to satisfy \
         constraint: Member must have value less than or equal to 25"
    );
}

#[tokio::test]
async fn list_rejects_a_negative_max_results() {
    let h = Harness::new().await;
    let message = expect_validation(
        call(
            &base_ctx(&h),
            "ListBulkOperations",
            json!({"MaxResults": -3}),
        )
        .await,
    );
    assert_eq!(
        message,
        "1 validation error detected: Value '-3' at 'maxResults' failed to satisfy \
         constraint: Member must have value greater than or equal to 1"
    );
}

#[tokio::test]
async fn list_orders_newest_first_then_id_descending() {
    let h = Harness::new().await;
    let old = raw_op(&h, ACCOUNT, 3, 1000).await;
    let tied_low = raw_op(&h, ACCOUNT, 1, 2000).await;
    let tied_high = raw_op(&h, ACCOUNT, 2, 2000).await;
    let newest = raw_op(&h, ACCOUNT, 4, 3000).await;
    let response = call(&base_ctx(&h), "ListBulkOperations", json!({}))
        .await
        .expect("list");
    let arns = summary_arns(&response);
    assert_eq!(
        arns,
        vec![
            arn_of(ACCOUNT, &newest),
            arn_of(ACCOUNT, &tied_high),
            arn_of(ACCOUNT, &tied_low),
            arn_of(ACCOUNT, &old),
        ],
        "newest first, then operation id descending on equal start times"
    );
}

#[tokio::test]
async fn next_token_pages_without_duplicate_or_gap() {
    let h = Harness::new().await;
    let base = 1_700_000_000_000_i64;
    let mut ids = Vec::new();
    for i in 0..7 {
        ids.push(raw_op(&h, ACCOUNT, i, base + i64::from(i) * 1000).await);
    }
    let expected: Vec<String> = ids.iter().rev().map(|id| arn_of(ACCOUNT, id)).collect();

    let ctx = base_ctx(&h);
    let mut collected = Vec::new();
    let mut token: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut body = json!({"MaxResults": 3});
        if let Some(t) = &token {
            body["NextToken"] = json!(t);
        }
        let response = call(&ctx, "ListBulkOperations", body).await.expect("list");
        collected.extend(summary_arns(&response));
        pages += 1;
        match response["NextToken"].as_str() {
            Some(t) => {
                assert_token_shape(t);
                token = Some(t.to_owned());
            }
            None => break,
        }
        assert!(pages < 10, "the pagination must terminate");
    }
    assert_eq!(pages, 3, "seven rows at three per page are three pages");
    assert_eq!(collected, expected, "no duplicate, no gap, stable order");
}

#[tokio::test]
async fn a_foreign_token_repositions_only_within_the_callers_rows() {
    let h = Harness::new().await;
    h.catalog
        .create_account(OTHER_ACCOUNT, "handler-tests")
        .await
        .expect("create account");
    for i in 0..3 {
        raw_op(&h, ACCOUNT, i, 1000 + i64::from(i) * 1000).await;
    }
    for i in 10..13 {
        raw_op(&h, OTHER_ACCOUNT, i, 3500 + i64::from(i) * 1000).await;
    }
    // A token minted from the other account's list decodes but names a
    // position in that account's key space.
    let other = ctx_for_account(&h, OTHER_ACCOUNT);
    let foreign_page = call(&other, "ListBulkOperations", json!({"MaxResults": 1}))
        .await
        .expect("list");
    let token = foreign_page["NextToken"]
        .as_str()
        .expect("the other account has more rows")
        .to_owned();

    let response = call(
        &base_ctx(&h),
        "ListBulkOperations",
        json!({"NextToken": token}),
    )
    .await
    .expect("a foreign token is only a position");
    let arns = summary_arns(&response);
    assert!(
        !arns.is_empty(),
        "the caller's rows past the position appear"
    );
    for arn in &arns {
        assert!(
            arn.contains(&format!(":{ACCOUNT}:")),
            "only the caller's rows may appear: {arn}"
        );
    }
}

#[tokio::test]
async fn a_foreign_token_that_lands_mid_list_returns_only_the_callers_older_rows() {
    let h = Harness::new().await;
    h.catalog
        .create_account(OTHER_ACCOUNT, "handler-tests")
        .await
        .expect("create account");
    let oldest = raw_op(&h, ACCOUNT, 1, 1000).await;
    let middle = raw_op(&h, ACCOUNT, 2, 2000).await;
    raw_op(&h, ACCOUNT, 3, 3000).await;
    // The other account's newest row sits between the caller's rows at
    // 2000 and 3000, so its page token names a mid-list position in the
    // caller's ordering.
    raw_op(&h, OTHER_ACCOUNT, 10, 2500).await;
    raw_op(&h, OTHER_ACCOUNT, 11, 400).await;
    let other = ctx_for_account(&h, OTHER_ACCOUNT);
    let foreign_page = call(&other, "ListBulkOperations", json!({"MaxResults": 1}))
        .await
        .expect("list");
    let token = foreign_page["NextToken"]
        .as_str()
        .expect("the other account has a second row")
        .to_owned();

    // A dropped token would put the caller's newest row first; a token
    // applied to the other account's rows would surface them. Exactly the
    // caller's rows older than the cursor may appear, newest first.
    let response = call(
        &base_ctx(&h),
        "ListBulkOperations",
        json!({"NextToken": token}),
    )
    .await
    .expect("a foreign token is only a position");
    assert_eq!(
        summary_arns(&response),
        vec![arn_of(ACCOUNT, &middle), arn_of(ACCOUNT, &oldest)],
        "only the caller's rows older than the cursor appear"
    );
}

#[tokio::test]
async fn a_malformed_next_token_answers_its_class() {
    let h = Harness::new().await;
    let ctx = base_ctx(&h);
    // Pattern class: non-hex, and a hex length that is not a multiple of 16.
    for bad in ["zzzzzzzzzzzzzzzz", "abc"] {
        let message =
            expect_validation(call(&ctx, "ListBulkOperations", json!({"NextToken": bad})).await);
        assert_eq!(
            message,
            format!(
                "1 validation error detected: Value '{bad}' at 'nextToken' failed to satisfy \
                 constraint: Member must satisfy regular expression pattern: ([0-9a-f]{{16}})+"
            ),
            "pattern class: {bad}"
        );
    }
    // Service minimum: a well-formed hex token under 112 characters.
    let short = format!("{:016x}", 1_700_000_000_000_u64);
    let message =
        expect_validation(call(&ctx, "ListBulkOperations", json!({"NextToken": short})).await);
    assert_eq!(
        message,
        format!(
            "Invalid Request: 1 validation error detected: Value '{short}' at 'nextToken' \
             failed to satisfy constraint: Member must have length greater than or equal to 112"
        )
    );
    // Model length maximum: over 1024 characters.
    let over = "0".repeat(1040);
    let message =
        expect_validation(call(&ctx, "ListBulkOperations", json!({"NextToken": over})).await);
    assert_eq!(
        message,
        format!(
            "1 validation error detected: Value '{over}' at 'nextToken' failed to satisfy \
             constraint: Member must have length less than or equal to 1024"
        )
    );
    // A well-formed token of 112 or more that does not decode to a cursor.
    for bad in ["0".repeat(112), "f".repeat(112)] {
        let message =
            expect_validation(call(&ctx, "ListBulkOperations", json!({"NextToken": bad})).await);
        assert_eq!(
            message,
            format!("Invalid Request: Provided nextToken is invalid: {bad}"),
            "not-a-cursor class: {bad}"
        );
    }
}

#[tokio::test]
async fn only_the_callers_account_is_listed() {
    let h = Harness::new().await;
    h.catalog
        .create_account(OTHER_ACCOUNT, "handler-tests")
        .await
        .expect("create account");
    for i in 0..2 {
        raw_op(&h, ACCOUNT, i, 1000 + i64::from(i)).await;
    }
    for i in 10..13 {
        raw_op(&h, OTHER_ACCOUNT, i, 2000 + i64::from(i)).await;
    }
    let response = call(&base_ctx(&h), "ListBulkOperations", json!({}))
        .await
        .expect("list");
    let arns = summary_arns(&response);
    assert_eq!(arns.len(), 2);
    for arn in &arns {
        assert!(arn.contains(&format!(":{ACCOUNT}:")), "foreign row: {arn}");
    }
}

#[tokio::test]
async fn a_terminal_summary_carries_end_time() {
    let h = Harness::new().await;
    let running = raw_op(&h, ACCOUNT, 1, 1000).await;
    let finished = raw_op(&h, ACCOUNT, 2, 2000).await;
    let claimed = h
        .storage
        .claim_runnable("finisher", Duration::from_secs(60), 100, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 2);
    finish_op(&h, ACCOUNT, "finisher", &finished, "COMPLETED").await;

    let response = call(&base_ctx(&h), "ListBulkOperations", json!({}))
        .await
        .expect("list");
    let rows = summaries(&response);
    assert_eq!(rows.len(), 2);
    let finished_row = rows
        .iter()
        .find(|s| s["BulkOperationArn"] == json!(arn_of(ACCOUNT, &finished)))
        .expect("the finished row is listed");
    assert_eq!(finished_row["BulkOperationStatus"], json!("COMPLETED"));
    assert!(finished_row["EndTime"].is_f64(), "EndTime must be present");
    let running_row = rows
        .iter()
        .find(|s| s["BulkOperationArn"] == json!(arn_of(ACCOUNT, &running)))
        .expect("the running row is listed");
    assert_eq!(running_row["BulkOperationStatus"], json!("IN_PROGRESS"));
    assert!(
        !running_row.as_object().unwrap().contains_key("EndTime"),
        "EndTime must be absent while running"
    );
    assert!(running_row["StartTime"].is_f64(), "StartTime is carried");
}
