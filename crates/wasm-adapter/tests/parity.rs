// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The parity corpus (`../parity/corpus.json`), run on both targets.
//!
//! Natively the test runs the cases through [`extenddb_wasm_adapter::Engine`]:
//! the `ExtendDB` engine over `sqlx` and the C `SQLite` library, on a tokio
//! runtime. On `wasm32` it runs them through `ExtendDb`, the surface the
//! browser module exports (the engine over the crate's synchronous `SQLite`
//! shim and the bundled `SQLite`), under Node with `wasm-bindgen-test`. One set
//! of rules serves both runs:
//!
//! - the case schema ([`read_case`]): the members a case may have, the type of
//!   each, and that every case checks something;
//! - the request a case sends ([`request_text`]): its `rawBody` as written, or
//!   its `body` as canonical JSON ([`canonical_json`]) after each reference
//!   `${name}` in its strings is replaced by the value an earlier case saved;
//! - the judging of a response against the case's error name and checks
//!   ([`judge`]).
//!
//! A case may carry an `onWasm` expectation, which the `wasm32` run uses in
//! place of the case's `error` and `checks`; the native run checks its shape
//! and uses the case's own. Neither run compares the full responses of the two
//! builds with each other; a member a case does not check can differ between
//! them without either run failing.
//!
//! On wasm32 this test calls the Rust `ExtendDb::submit` directly, with string
//! arguments, and does not load the module `wasm-pack build --target web`
//! emits. `parity/run.mjs` is the only check of that module and of the
//! `ExtendDb` class wasm-bindgen generates: it calls `submit` with arguments
//! that are not strings, then a `ListTables`.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use serde_json::{Map, Value};

/// The corpus, embedded so the test is hermetic.
const CORPUS: &str = include_str!("../parity/corpus.json");

/// Whether this run judges a case against its `onWasm` expectation, when it
/// has one.
const ON_WASM: bool = cfg!(target_arch = "wasm32");

/// The members a case may have.
const CASE_MEMBERS: [&str; 8] = [
    "name", "op", "body", "rawBody", "error", "checks", "save", "onWasm",
];

/// The members of a case's `onWasm` expectation, all required.
const ON_WASM_MEMBERS: [&str; 3] = ["error", "checks", "reason"];

/// What a case expects of a response: the error it names, or `None` for
/// success, and the checks on its members.
struct Expectation<'a> {
    error: Option<&'a str>,
    checks: Vec<Check<'a>>,
}

/// One check: the JSON Pointer of a response member and what must hold there.
struct Check<'a> {
    path: &'a str,
    kind: CheckKind<'a>,
}

/// What a check asserts about the member its pointer addresses.
enum CheckKind<'a> {
    /// The member equals this value (see [`json_equal`]).
    Equals(&'a Value),
    /// The member is a string containing this one.
    Contains(&'a str),
    /// The pointer addresses nothing.
    Absent,
    /// The pointer addresses a member.
    Present,
}

/// What a case submits.
enum Request<'a> {
    /// A body, written as canonical JSON after substitution.
    Body(&'a Map<String, Value>),
    /// A request text, submitted as written.
    Raw(&'a str),
}

/// A case that follows the schema.
struct Case<'a> {
    name: &'a str,
    op: &'a str,
    request: Request<'a>,
    expect: Expectation<'a>,
    /// The case's `onWasm` expectation, if it has one.
    on_wasm: Option<Expectation<'a>>,
    /// The values the case saves for later cases: a name and the JSON Pointer
    /// of the string in the response that it saves.
    save: Vec<(&'a str, &'a str)>,
}

/// Whether `path` is a JSON Pointer (RFC 6901): empty, or `/`-separated tokens
/// in which every `~` begins the escape `~0` or `~1`.
fn is_json_pointer(path: &str) -> bool {
    if !path.is_empty() && !path.starts_with('/') {
        return false;
    }
    let mut chars = path.chars();
    while let Some(c) = chars.next() {
        if c == '~' && !matches!(chars.next(), Some('0' | '1')) {
            return false;
        }
    }
    true
}

/// Whether `key` can name a saved value: an ASCII letter or `_`, then ASCII
/// letters, digits and `_`.
fn is_save_name(key: &str) -> bool {
    let mut chars = key.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// One piece of a body string: text sent as written, or a reference `${name}`
/// to a saved value.
enum Piece<'a> {
    Text(&'a str),
    Reference(&'a str),
}

/// Split a body string into text and references. Every `${` begins a
/// reference: `${`, a name ([`is_save_name`]), then `}`. `Err` for a `${` that
/// does not begin one.
fn pieces(s: &str) -> Result<Vec<Piece<'_>>, String> {
    let mut pieces = Vec::new();
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        if start > 0 {
            pieces.push(Piece::Text(&rest[..start]));
        }
        let after = &rest[start + 2..];
        let name = after
            .find('}')
            .map(|end| &after[..end])
            .filter(|name| is_save_name(name))
            .ok_or_else(|| {
                format!(
                    "the body string {s:?} has a ${{ that does not begin a reference ${{name}} \
                     to a saved value"
                )
            })?;
        pieces.push(Piece::Reference(name));
        rest = &after[name.len() + 1..];
    }
    if !rest.is_empty() {
        pieces.push(Piece::Text(rest));
    }
    Ok(pieces)
}

/// Refuse a body string whose `${` does not begin a reference, or whose
/// reference names a value no earlier case saves.
fn check_references(value: &Value, saved: &HashSet<String>) -> Result<(), String> {
    match value {
        Value::String(s) => pieces(s)?.iter().try_for_each(|piece| match piece {
            Piece::Reference(name) if !saved.contains(*name) => Err(format!(
                "the body string {s:?} names {name:?}, a value no earlier case saves"
            )),
            _ => Ok(()),
        }),
        Value::Array(items) => items.iter().try_for_each(|v| check_references(v, saved)),
        Value::Object(members) => members
            .values()
            .try_for_each(|v| check_references(v, saved)),
        _ => Ok(()),
    }
}

/// A non-empty string, or `None`.
fn non_empty_str(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str).filter(|s| !s.is_empty())
}

/// Read one check: an object with a `path` that is a JSON Pointer and exactly
/// one of `equals` (any JSON value), `contains` (a string), `absent: true` or
/// `present: true`, and no other member.
fn read_check(check: &Value) -> Result<Check<'_>, String> {
    let malformed = || {
        format!(
            "check {check} must have a path that is a JSON Pointer and exactly one of \
             equals, contains (a string), absent: true or present: true, and no other member"
        )
    };
    let members = check.as_object().ok_or_else(malformed)?;
    let path = members
        .get("path")
        .and_then(Value::as_str)
        .filter(|path| is_json_pointer(path))
        .ok_or_else(malformed)?;
    if members.len() != 2 {
        return Err(malformed());
    }
    let kind = members
        .iter()
        .find(|(key, _)| key.as_str() != "path")
        .and_then(|(key, value)| match (key.as_str(), value) {
            ("equals", expected) => Some(CheckKind::Equals(expected)),
            ("contains", Value::String(needle)) => Some(CheckKind::Contains(needle)),
            ("absent", Value::Bool(true)) => Some(CheckKind::Absent),
            ("present", Value::Bool(true)) => Some(CheckKind::Present),
            _ => None,
        })
        .ok_or_else(malformed)?;
    Ok(Check { path, kind })
}

/// Read an expectation from its `error` and `checks` members.
///
/// `error` is required: `null` for success or the error's short name. `checks`
/// is required and must check something beyond the outcome: a success case
/// needs a check whose path is not `/__type`, and an error case needs an
/// `equals`, `contains` or `absent` check on the message, `/message` or
/// `/Message` (the key the error document gives it).
fn read_expectation<'a>(
    error: Option<&'a Value>,
    checks: Option<&'a Value>,
) -> Result<Expectation<'a>, String> {
    let error = match error {
        Some(Value::Null) => None,
        Some(Value::String(error)) if !error.is_empty() => Some(error.as_str()),
        _ => return Err("error must be null or a non-empty string".to_owned()),
    };
    let checks = match checks {
        Some(Value::Array(checks)) => checks
            .iter()
            .map(read_check)
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err("checks must be an array".to_owned()),
    };
    let checks_something = match error {
        None => checks.iter().any(|check| check.path != "/__type"),
        Some(_) => checks.iter().any(|check| {
            matches!(check.path, "/message" | "/Message")
                && !matches!(check.kind, CheckKind::Present)
        }),
    };
    if !checks_something {
        return Err(match error {
            None => "a success case needs a check on the response body".to_owned(),
            Some(_) => {
                "an error case needs an equals, contains or absent check on /message or /Message"
                    .to_owned()
            }
        });
    }
    Ok(Expectation { error, checks })
}

/// Read a case's `onWasm` expectation: an object with `error`, `checks` and a
/// non-empty `reason`, and no other member.
fn read_on_wasm(value: &Value) -> Result<Expectation<'_>, String> {
    let members = value
        .as_object()
        .ok_or("onWasm must be an object with error, checks and reason")?;
    if let Some(key) = members
        .keys()
        .find(|key| !ON_WASM_MEMBERS.contains(&key.as_str()))
    {
        return Err(format!("onWasm has an unknown member {key:?}"));
    }
    if non_empty_str(members.get("reason")).is_none() {
        return Err("onWasm needs a reason, a non-empty string".to_owned());
    }
    read_expectation(members.get("error"), members.get("checks"))
        .map_err(|e| format!("onWasm: {e}"))
}

/// Read one case and check it against the corpus schema.
///
/// A case has a non-empty string `name` and `op`; exactly one of `body` (an
/// object) and `rawBody` (a string); an `error` and `checks`
/// ([`read_expectation`]); optionally `save`, an object whose keys are names
/// ([`is_save_name`]) and whose values are JSON Pointers; optionally `onWasm`
/// ([`read_on_wasm`]), but not together with `save`; and no other member.
/// In its body strings every `${` begins a reference
/// `${name}` ([`pieces`]) to a value in `saved`, the names earlier cases save.
fn read_case<'a>(value: &'a Value, saved: &HashSet<String>) -> Result<Case<'a>, String> {
    let members = value.as_object().ok_or("a case must be an object")?;
    let name = non_empty_str(members.get("name")).ok_or("name must be a non-empty string")?;
    let within = |e: String| format!("case '{name}': {e}");
    if let Some(key) = members
        .keys()
        .find(|key| !CASE_MEMBERS.contains(&key.as_str()))
    {
        return Err(within(format!("unknown member {key:?}")));
    }
    let op = non_empty_str(members.get("op"))
        .ok_or_else(|| within("op must be a non-empty string".to_owned()))?;
    let request = match (members.get("body"), members.get("rawBody")) {
        (Some(Value::Object(body)), None) => {
            body.values()
                .try_for_each(|v| check_references(v, saved))
                .map_err(within)?;
            Request::Body(body)
        }
        (None, Some(Value::String(raw))) => Request::Raw(raw),
        _ => {
            return Err(within(
                "a case needs exactly one of body (an object) and rawBody (a string)".to_owned(),
            ));
        }
    };
    let expect = read_expectation(members.get("error"), members.get("checks")).map_err(within)?;
    let save = match members.get("save") {
        None => Vec::new(),
        Some(Value::Object(save)) => save
            .iter()
            .map(|(key, path)| match path.as_str() {
                Some(path) if is_save_name(key) && is_json_pointer(path) => {
                    Ok((key.as_str(), path))
                }
                _ => Err(within(format!(
                    "save member {key:?}: {path} must be a JSON Pointer string under a name \
                     of ASCII letters, digits and _ that does not start with a digit"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(other) => return Err(within(format!("save {other} is not an object"))),
    };
    let on_wasm = match members.get("onWasm") {
        None => None,
        Some(on_wasm) => {
            let on_wasm = read_on_wasm(on_wasm).map_err(within)?;
            if members.contains_key("save") {
                return Err(within(
                    "a case with onWasm cannot save values, as its response differs by build"
                        .to_owned(),
                ));
            }
            Some(on_wasm)
        }
    };
    Ok(Case {
        name,
        op,
        request,
        expect,
        on_wasm,
        save,
    })
}

/// Read the whole corpus: an object with an optional `_comment` (an array of
/// strings) and a non-empty `cases` array, each case read in order so that a
/// case's body can refer only to values an earlier case saves.
fn read_corpus(corpus: &Value) -> Result<Vec<Case<'_>>, String> {
    let members = corpus.as_object().ok_or("the corpus must be an object")?;
    if let Some(key) = members
        .keys()
        .find(|key| !["_comment", "cases"].contains(&key.as_str()))
    {
        return Err(format!("unknown corpus member {key:?}"));
    }
    match members.get("_comment") {
        None => {}
        Some(Value::Array(lines)) if lines.iter().all(Value::is_string) => {}
        Some(_) => return Err("_comment must be an array of strings".to_owned()),
    }
    let cases = match members.get("cases") {
        Some(Value::Array(cases)) if !cases.is_empty() => cases,
        _ => return Err("cases must be a non-empty array".to_owned()),
    };
    let mut saved = HashSet::new();
    let mut read = Vec::with_capacity(cases.len());
    for (index, case) in cases.iter().enumerate() {
        let case = read_case(case, &saved).map_err(|e| format!("case {index}: {e}"))?;
        saved.extend(case.save.iter().map(|(key, _)| (*key).to_owned()));
        read.push(case);
    }
    Ok(read)
}

/// Write `s` as a JSON string: `"` and `\` escaped with a backslash, U+0008,
/// U+000C, U+000A, U+000D and U+0009 as `\b`, `\f`, `\n`, `\r` and `\t`, every
/// other code point below U+0020 as `\u00XX` with lower-case hex digits, and
/// every other code point as itself.
fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Write `value` as canonical JSON: no whitespace, object members sorted by
/// the code points of their names, numbers as `serde_json` writes them,
/// strings as [`write_json_string`] writes them. The request text so does not
/// depend on the order in which `serde_json` keeps an object's members, which
/// its `preserve_order` feature changes.
fn canonical_json(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            let _ = write!(out, "{n}");
        }
        Value::String(s) => write_json_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                canonical_json(item, out);
            }
            out.push(']');
        }
        Value::Object(members) => {
            // `str` orders by UTF-8 bytes, which is code point order.
            let mut names: Vec<&String> = members.keys().collect();
            names.sort();
            out.push('{');
            for (index, name) in names.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_json_string(name, out);
                out.push(':');
                canonical_json(&members[name], out);
            }
            out.push('}');
        }
    }
}

/// Replace every reference `${name}` in the strings of `value` with the value
/// saved under `name`, writing the rest of each string as it is. The schema
/// has checked that each reference is well formed and that an earlier case
/// saves its name.
fn substitute(value: &Value, saved: &HashMap<String, String>) -> Value {
    match value {
        Value::String(s) => Value::String(
            pieces(s)
                .unwrap_or_else(|e| panic!("{e}"))
                .iter()
                .map(|piece| match piece {
                    Piece::Text(text) => *text,
                    Piece::Reference(name) => saved
                        .get(*name)
                        .unwrap_or_else(|| panic!("no value was saved under {name:?}")),
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(|v| substitute(v, saved)).collect()),
        Value::Object(members) => Value::Object(
            members
                .iter()
                .map(|(key, v)| (key.clone(), substitute(v, saved)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

/// The request text a case submits: its `rawBody` as written, or its `body`
/// with the saved values substituted, as canonical JSON.
fn request_text(case: &Case<'_>, saved: &HashMap<String, String>) -> String {
    match case.request {
        Request::Raw(raw) => raw.to_owned(),
        Request::Body(body) => {
            let mut out = String::new();
            canonical_json(&substitute(&Value::Object(body.clone()), saved), &mut out);
            out
        }
    }
}

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

/// JSON equality as the corpus defines it: object members compare regardless
/// of their order, array elements in order, and numbers by their value as an
/// `f64` (so `1` equals `1.0`).
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

/// Judge a response against an expectation: `Err` names the first thing that
/// does not hold. A pointer is resolved as `serde_json`'s `Value::pointer`
/// resolves it.
fn judge(expect: &Expectation<'_>, resp: &Value) -> Result<(), String> {
    let got_error = error_short_name(resp);
    match expect.error {
        Some(expected) if got_error.as_deref() != Some(expected) => {
            return Err(format!("expected error {expected}, got response {resp}"));
        }
        None if got_error.is_some() => {
            return Err(format!("expected success, got error document {resp}"));
        }
        _ => {}
    }
    for Check { path, kind } in &expect.checks {
        let got = resp.pointer(path);
        let holds = match (kind, got) {
            (CheckKind::Absent, got) => got.is_none(),
            (CheckKind::Present, got) => got.is_some(),
            (CheckKind::Contains(needle), Some(Value::String(got))) => got.contains(needle),
            (CheckKind::Equals(expected), Some(got)) => json_equal(got, expected),
            (CheckKind::Contains(_) | CheckKind::Equals(_), _) => false,
        };
        if !holds {
            let wanted = match kind {
                CheckKind::Equals(expected) => format!("a value equal to {expected}"),
                CheckKind::Contains(needle) => format!("a string containing {needle:?}"),
                CheckKind::Absent => "no member".to_owned(),
                CheckKind::Present => "a member".to_owned(),
            };
            return Err(format!("at {path} expected {wanted}, response {resp}"));
        }
    }
    Ok(())
}

/// Record the strings a case's `save` names, for later cases' bodies: `Err`
/// when a pointer does not address a string in the response.
fn save(case: &Case<'_>, resp: &Value, saved: &mut HashMap<String, String>) -> Result<(), String> {
    for (key, path) in &case.save {
        let value = resp
            .pointer(path)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("save path {path} is not a string in {resp}"))?;
        saved.insert((*key).to_owned(), value.to_owned());
    }
    Ok(())
}

/// An instance of the build under test, over a fresh in-memory database.
#[cfg(not(target_arch = "wasm32"))]
async fn create() -> extenddb_wasm_adapter::Engine {
    extenddb_wasm_adapter::Engine::new()
        .await
        .expect("in-memory engine initializes")
}

/// An instance of the build under test, over a fresh in-memory database.
#[cfg(target_arch = "wasm32")]
async fn create() -> extenddb_wasm_adapter::ExtendDb {
    extenddb_wasm_adapter::ExtendDb::create()
        .await
        .unwrap_or_else(|e| panic!("in-memory engine initializes: {e:?}"))
}

/// The answer of `instance` to one request.
#[cfg(not(target_arch = "wasm32"))]
async fn submit(instance: &extenddb_wasm_adapter::Engine, op: &str, request: &str) -> String {
    instance.submit(op, request).await
}

/// The answer of `instance` to one request, the string its promise resolves
/// with.
#[cfg(target_arch = "wasm32")]
async fn submit(instance: &extenddb_wasm_adapter::ExtendDb, op: &str, request: &str) -> String {
    use wasm_bindgen::JsValue;
    let promise = instance.submit(&JsValue::from_str(op), &JsValue::from_str(request));
    wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .unwrap_or_else(|e| panic!("submit rejected: {e:?}"))
        .as_string()
        .expect("submit resolves with a string")
}

/// Run the whole corpus in order against one fresh in-memory database (state
/// carries between cases), judging each case against its `onWasm`
/// expectation on `wasm32` when it has one, else against its own.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn every_case_gives_its_expected_answer() {
    let corpus: Value = serde_json::from_str(CORPUS).expect("corpus.json parses");
    let cases = read_corpus(&corpus).unwrap_or_else(|e| panic!("corpus.json is malformed: {e}"));

    let instance = create().await;

    let mut saved = HashMap::new();
    let mut checks = 0;
    for case in &cases {
        let expect = match &case.on_wasm {
            Some(on_wasm) if ON_WASM => on_wasm,
            _ => &case.expect,
        };
        let request = request_text(case, &saved);
        let out = submit(&instance, case.op, &request).await;
        let resp: Value = serde_json::from_str(&out)
            .unwrap_or_else(|e| panic!("case '{}': response is not JSON ({e}): {out}", case.name));
        if let Err(why) = judge(expect, &resp) {
            panic!("case '{}': {why}", case.name);
        }
        checks += expect.checks.len();
        if let Err(why) = save(case, &resp, &mut saved) {
            panic!("case '{}': {why}", case.name);
        }
    }
    println!(
        "parity corpus: {} cases and {checks} checks passed",
        cases.len()
    );
}
