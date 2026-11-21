// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Native side of the shared parity harness.
//!
//! Runs the shared case corpus (`../parity/corpus.json`) through
//! [`extenddb_wasm_adapter::Engine`] built natively — the `ExtendDB` engine over
//! `sqlx` and the C `SQLite` library. The wasm side (`../parity/run.mjs`) runs
//! the same corpus through the wasm module under Node and applies the same
//! assertions: each case's expected error name and its `checks` on chosen
//! response members. Both runners also judge the rows of
//! `../parity/runner-selftest.json` and fail if a row's outcome differs from
//! the one it records, which holds the two to the same judging rules. Neither
//! runner compares the full responses of the two builds with each other; a
//! member a case does not check can differ between them without either run
//! failing.
//!
//! Keep this test and the Node runner reading the one corpus file so the two
//! apply the same assertions.
//!
//! Native only: it drives the engine on a tokio runtime, a native-only
//! dev-dependency, so the file compiles to nothing on `wasm32` and
//! `cargo clippy --target wasm32-unknown-unknown --all-targets` can build.
#![cfg(not(target_arch = "wasm32"))]

use std::collections::HashMap;

use serde_json::Value;

/// The shared corpus, embedded so the test is hermetic.
const CORPUS: &str = include_str!("../parity/corpus.json");

/// The runners' shared self-test rows: a case, a response, and the outcome
/// (`pass`, `fail` or `reject`) both runners must give the pair.
const SELFTEST: &str = include_str!("../parity/runner-selftest.json");

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

/// Why a case did not hold against a response.
enum Verdict {
    /// The case is well formed and the response does not satisfy it.
    Fail(String),
    /// The case itself is malformed: an unknown check shape, a member that is
    /// not part of a check, or an `error` that is neither null nor a non-empty
    /// string.
    Reject(String),
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fail(why) => write!(f, "fail: {why}"),
            Self::Reject(why) => write!(f, "malformed case: {why}"),
        }
    }
}

/// The kind of a corpus check, read from its one member besides `path`.
enum Check<'a> {
    Equals(&'a Value),
    Contains(&'a str),
    Absent,
    Present,
}

/// Read one check: an object with a string `path` and exactly one of `equals`
/// (any JSON value), `contains` (a string), `absent: true` or
/// `present: true`, and no other member.
fn parse_check(check: &Value) -> Result<(&str, Check<'_>), String> {
    let shape = "must have a string path and exactly one of equals, contains (a string), \
                 absent: true or present: true, and no other member";
    let members = check
        .as_object()
        .ok_or_else(|| format!("check {check} {shape}"))?;
    let path = members
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("check {check} {shape}"))?;
    if members.len() != 2 {
        return Err(format!("check {check} {shape}"));
    }
    let kind = members
        .iter()
        .find(|(key, _)| key.as_str() != "path")
        .and_then(|(key, value)| match (key.as_str(), value) {
            ("equals", expected) => Some(Check::Equals(expected)),
            ("contains", Value::String(needle)) => Some(Check::Contains(needle)),
            ("absent", Value::Bool(true)) => Some(Check::Absent),
            ("present", Value::Bool(true)) => Some(Check::Present),
            _ => None,
        })
        .ok_or_else(|| format!("check {check} {shape}"))?;
    Ok((path, kind))
}

/// JSON equality as the corpus defines it: object members compare regardless
/// of their order, array elements in order, and numbers by their value as an
/// `f64` (so `1` equals `1.0`, as it does once JavaScript has parsed both).
fn json_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::Array(xs), Value::Array(ys)) => {
            xs.len() == ys.len() && xs.iter().zip(ys).all(|(x, y)| json_equal(x, y))
        }
        (Value::Object(xs), Value::Object(ys)) => {
            xs.len() == ys.len()
                && xs
                    .iter()
                    .all(|(key, x)| ys.get(key).is_some_and(|y| json_equal(x, y)))
        }
        _ => a == b,
    }
}

/// Judge one case's assertions against a parsed response.
///
/// `../parity/run.mjs` applies the same rules in the Node runner; the rows of
/// `../parity/runner-selftest.json` hold both runners to the same outcomes.
fn judge(name: &str, case: &Value, resp: &Value) -> Result<(), Verdict> {
    let expected_error = match case.get("error") {
        None | Some(Value::Null) => None,
        Some(Value::String(error)) if !error.is_empty() => Some(error.as_str()),
        Some(other) => {
            return Err(Verdict::Reject(format!(
                "case '{name}': error {other} is neither null nor a non-empty string"
            )));
        }
    };
    let checks = match case.get("checks") {
        None => &[][..],
        Some(Value::Array(checks)) => checks.as_slice(),
        Some(other) => {
            return Err(Verdict::Reject(format!(
                "case '{name}': checks {other} is not an array"
            )));
        }
    };
    let checks = checks
        .iter()
        .map(parse_check)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Verdict::Reject(format!("case '{name}': {e}")))?;

    let got_error = error_short_name(resp);
    match expected_error {
        Some(expected) if got_error.as_deref() != Some(expected) => {
            return Err(Verdict::Fail(format!(
                "case '{name}': expected error {expected}, got response {resp}"
            )));
        }
        None if got_error.is_some() => {
            return Err(Verdict::Fail(format!(
                "case '{name}': expected success, got error document {resp}"
            )));
        }
        _ => {}
    }

    for (path, kind) in checks {
        let got = resp.pointer(path);
        let holds = match (&kind, got) {
            (Check::Absent, got) => got.is_none(),
            (Check::Present, got) => got.is_some(),
            (Check::Contains(needle), Some(Value::String(got))) => got.contains(needle),
            (Check::Equals(expected), Some(got)) => json_equal(got, expected),
            (Check::Contains(_) | Check::Equals(_), _) => false,
        };
        if !holds {
            let wanted = match kind {
                Check::Equals(expected) => format!("a value equal to {expected}"),
                Check::Contains(needle) => format!("a string containing {needle:?}"),
                Check::Absent => "no member".to_owned(),
                Check::Present => "a member".to_owned(),
            };
            return Err(Verdict::Fail(format!(
                "case '{name}': at {path} expected {wanted}, response {resp}"
            )));
        }
    }
    Ok(())
}

/// Replace every string in `body` of the form `${name}` with the value an
/// earlier case saved under `name`, panicking if no case saved it.
fn substitute(name: &str, body: &mut Value, saved: &HashMap<String, String>) {
    match body {
        Value::String(s) => {
            if let Some(key) = s.strip_prefix("${").and_then(|r| r.strip_suffix('}')) {
                let value = saved.get(key).unwrap_or_else(|| {
                    panic!("case '{name}': no earlier case saved a value named {key:?}")
                });
                value.clone_into(s);
            }
        }
        Value::Array(items) => {
            for item in items {
                substitute(name, item, saved);
            }
        }
        Value::Object(members) => {
            for member in members.values_mut() {
                substitute(name, member, saved);
            }
        }
        _ => {}
    }
}

/// Record the string members a case's `save` names, for later cases' bodies.
fn save(name: &str, case: &Value, resp: &Value, saved: &mut HashMap<String, String>) {
    if let Some(members) = case.get("save").and_then(Value::as_object) {
        for (key, path) in members {
            let path = path.as_str().expect("save values are JSON Pointers");
            let value = resp
                .pointer(path)
                .and_then(Value::as_str)
                .unwrap_or_else(|| {
                    panic!("case '{name}': save path {path} is not a string in {resp}")
                });
            saved.insert(key.clone(), value.to_owned());
        }
    }
}

/// Judge every row of the shared runner self-test and compare the outcome with
/// the row's `expect`. `../parity/run.mjs` judges the same rows before its
/// corpus run, so a judging rule that differs between the two runners fails
/// one of them.
#[test]
fn the_runner_judges_each_self_test_row_as_expected() {
    let fixture: Value = serde_json::from_str(SELFTEST).expect("runner-selftest.json parses");
    let rows = fixture["rows"]
        .as_array()
        .expect("fixture has a rows array");
    assert!(!rows.is_empty(), "fixture must have rows");
    let disagreements: Vec<String> = rows
        .iter()
        .filter_map(|row| {
            let name = row["name"].as_str().expect("row.name is a string");
            let expect = row["expect"].as_str().expect("row.expect is a string");
            let got = match judge(name, &row["case"], &row["response"]) {
                Ok(()) => "pass",
                Err(Verdict::Fail(_)) => "fail",
                Err(Verdict::Reject(_)) => "reject",
            };
            (got != expect).then(|| format!("{name}: expected {expect}, got {got}"))
        })
        .collect();
    assert!(
        disagreements.is_empty(),
        "self-test rows judged differently than expected:\n{}",
        disagreements.join("\n")
    );
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

    let mut saved = HashMap::new();
    for case in cases {
        let name = case["name"].as_str().unwrap_or("<unnamed>");
        let op = case["op"].as_str().expect("case.op is a string");
        let body_json = if let Some(raw) = case.get("rawBody").and_then(Value::as_str) {
            raw.to_owned()
        } else {
            let mut body = case["body"].clone();
            substitute(name, &mut body, &saved);
            serde_json::to_string(&body).expect("case.body serializes")
        };
        let out = engine.submit(op, &body_json).await;
        let resp: Value = serde_json::from_str(&out)
            .unwrap_or_else(|e| panic!("case '{name}': response is not JSON ({e}): {out}"));
        if let Err(verdict) = judge(name, case, &resp) {
            panic!("{verdict}");
        }
        save(name, case, &resp, &mut saved);
    }
}
