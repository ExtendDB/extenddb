// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Native side of the shared parity harness.
//!
//! Runs the shared case corpus (`../parity/corpus.json`) through
//! [`extenddb_wasm_adapter::Engine`] built natively — the `ExtendDB` engine over
//! `sqlx` and the C `SQLite` library. The wasm side (`../parity/run.mjs`) runs
//! the same corpus through the browser wasm module under Node and applies the
//! same assertions: each case's expected error name and its `checks` on chosen
//! response members. Neither runner compares the full responses of the two
//! builds with each other; a member a case does not check can differ between
//! them without either run failing.
//!
//! Keep this test and the Node runner reading the one corpus file so the two
//! apply the same assertions.
//!
//! Native only: it drives the engine on a tokio runtime, a native-only
//! dev-dependency, so the file compiles to nothing on `wasm32` and
//! `cargo clippy --target wasm32-unknown-unknown --all-targets` can build.
#![cfg(not(target_arch = "wasm32"))]

use serde_json::Value;

/// The shared corpus, embedded so the test is hermetic.
const CORPUS: &str = include_str!("../parity/corpus.json");

/// The short `DynamoDB` error name (the segment after `#`, then after `.`) of a
/// response, if the response is an error document.
fn error_short_name(resp: &Value) -> Option<String> {
    resp.get("__type").and_then(Value::as_str).map(|t| {
        t.rsplit('#')
            .next()
            .unwrap_or(t)
            .rsplit('.')
            .next()
            .unwrap_or(t)
            .to_owned()
    })
}

/// Apply one case's assertions to a parsed response, panicking with context on
/// any mismatch.
fn assert_case(name: &str, case: &Value, resp: &Value) {
    let expected_error = case.get("error").and_then(Value::as_str);
    let got_error = error_short_name(resp);
    match expected_error {
        Some(expected) => assert_eq!(
            got_error.as_deref(),
            Some(expected),
            "case '{name}': expected error {expected}, got response {resp}"
        ),
        None => assert!(
            got_error.is_none(),
            "case '{name}': expected success, got error document {resp}"
        ),
    }

    if let Some(checks) = case.get("checks").and_then(Value::as_array) {
        for check in checks {
            let path = check
                .get("path")
                .and_then(Value::as_str)
                .expect("check.path is a string");
            if check.get("absent").and_then(Value::as_bool) == Some(true) {
                assert!(
                    resp.pointer(path).is_none(),
                    "case '{name}': expected {path} to be absent, response {resp}"
                );
            } else if check.get("present").and_then(Value::as_bool) == Some(true) {
                assert!(
                    resp.pointer(path).is_some(),
                    "case '{name}': expected {path} to be present, response {resp}"
                );
            } else if let Some(contains) = check.get("contains").and_then(Value::as_str) {
                let got = resp
                    .pointer(path)
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| {
                        panic!("case '{name}': string path {path} not found in {resp}")
                    });
                assert!(
                    got.contains(contains),
                    "case '{name}': at {path} expected a string containing {contains:?}, got {got:?}"
                );
            } else {
                let expected = check
                    .get("equals")
                    .expect("check has equals, contains, or absent");
                let got = resp
                    .pointer(path)
                    .unwrap_or_else(|| panic!("case '{name}': path {path} not found in {resp}"));
                assert_eq!(
                    got, expected,
                    "case '{name}': at {path} expected {expected}, got {got}"
                );
            }
        }
    }
}

/// Run the whole corpus through the native engine in order, against one fresh
/// in-memory database (state carries between cases, as the wasm runner drives
/// it).
#[tokio::test]
async fn parity_corpus_passes_on_the_native_build() {
    let corpus: Value = serde_json::from_str(CORPUS).expect("corpus.json parses");
    let cases = corpus["cases"]
        .as_array()
        .expect("corpus has a cases array");
    assert!(!cases.is_empty(), "corpus must have cases");

    let engine = extenddb_wasm_adapter::Engine::new()
        .await
        .expect("in-memory engine initializes");

    for case in cases {
        let name = case["name"].as_str().unwrap_or("<unnamed>");
        let op = case["op"].as_str().expect("case.op is a string");
        let body_json = match case.get("rawBody").and_then(Value::as_str) {
            Some(raw) => raw.to_owned(),
            None => serde_json::to_string(&case["body"]).expect("case.body serializes"),
        };
        let out = engine.submit(op, &body_json).await;
        let resp: Value = serde_json::from_str(&out)
            .unwrap_or_else(|e| panic!("case '{name}': response is not JSON ({e}): {out}"));
        assert_case(name, case, &resp);
    }
}
