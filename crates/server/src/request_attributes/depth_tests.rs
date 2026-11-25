// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The names of an `UpdateItem` whose expressions nest past
//! `max_expression_depth`: they are read under the depth limit the engine
//! applies, on a thread with a small stack, instead of overflowing it.

use std::sync::mpsc;
use std::time::Duration;

use super::*;
use serde_json::json;

/// Far past the limit: before the update parser checked the depth, this
/// many levels took more than the stack below.
const FAR: usize = 50_000;

/// Half the stack of a server worker thread (2 MiB).
const SMALL_STACK: usize = 1 << 20;

/// Size and token limits raised so only the depth limit refuses the
/// expressions below.
fn raised() -> LimitsConfig {
    LimitsConfig {
        max_expression_length_bytes: 4 << 20,
        max_expression_tokens: 1 << 20,
        ..LimitsConfig::default()
    }
}

fn hash_table() -> TableKeyInfo {
    TableKeyInfo {
        table_name: "t".to_owned(),
        key_schema: vec![extenddb_core::types::KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: extenddb_core::types::KeyType::Hash,
        }],
        ..Default::default()
    }
}

/// The names of `input` as an `UpdateItem`, read on a thread with
/// [`SMALL_STACK`], failing the test after 10 seconds.
fn on_small_stack(input: Value) -> Option<Vec<String>> {
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::Builder::new()
        .stack_size(SMALL_STACK)
        .spawn(move || {
            let found =
                single_table_attributes("UpdateItem", &input, Some(&hash_table()), &raised());
            let _ = tx.send(found);
        })
        .expect("spawn the reading thread");
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(found) => found,
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            std::panic::resume_unwind(worker.join().expect_err("no answer and no panic"))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("reading the names ran for 10 s"),
    }
}

fn names(list: &[&str]) -> Option<Vec<String>> {
    Some(list.iter().map(|s| (*s).to_owned()).collect())
}

#[test]
fn an_update_expression_past_the_depth_limit_gives_no_names() {
    let at_limit = format!("SET qty = {}:v{}", "(qty + ".repeat(150), ")".repeat(150));
    let input = json!({"TableName": "t", "Key": {"pk": {"S": "a"}}, "UpdateExpression": at_limit});
    assert_eq!(on_small_stack(input), names(&["pk", "qty"]));

    for update in [
        format!("SET qty = {}:v{}", "(qty + ".repeat(FAR), ")".repeat(FAR)),
        format!(
            "SET qty = {}:v{}",
            "list_append(qty, ".repeat(FAR),
            ")".repeat(FAR)
        ),
    ] {
        let input =
            json!({"TableName": "t", "Key": {"pk": {"S": "a"}}, "UpdateExpression": update});
        assert_eq!(on_small_stack(input), names(&["pk"]));
    }
}

#[test]
fn a_condition_expression_past_the_depth_limit_gives_no_names() {
    let condition = format!("{}qty{} > :v", "size(".repeat(FAR), ")".repeat(FAR));
    let input = json!({
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "ConditionExpression": condition,
    });
    assert_eq!(on_small_stack(input), names(&["pk"]));
}
