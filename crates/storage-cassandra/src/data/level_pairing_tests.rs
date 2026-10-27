// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The pairing of each data-path statement's kind with the level it states,
//! checked over the source of `src/data/*.rs` at test time. The clippy lint in
//! `clippy.toml` rejects a call that runs at the driver default; it cannot see
//! which constructor a call passes, so a plain write at `read_params`, a
//! `SELECT` at `write_params`, or an `IF` statement at `write_params` would
//! compile and lint clean. This test reads the statement sites whose CQL and
//! level it can resolve (the CQL from a literal or a `format!` in the call, or
//! the nearest `let` binding of the identifier it passes; the level from a
//! constructor named in the arguments or in the `let` binding of an identifier
//! argument), and asserts: a `SELECT` carries a read level, a write without
//! `IF` carries `write_params` or `execute_quorum`, a statement with `IF`
//! carries `lwt_params` or `query_lwt`, and no `BatchQueryBuilder::new`,
//! `BatchQueryBuilder::default`, `Default::default()` or `.with_consistency(`
//! appears under `data/`, so a batch is built by `batch_builder()` or not at
//! all. A site whose CQL resolves and whose level does not is a failure. The
//! schema statements in `ddl.rs` are outside the check. The walker follows a
//! level passed as an identifier to its `let`, not one called in the argument
//! or received as a parameter of the enclosing function; it does not follow
//! text appended with `push_str` or a statement a helper receives as a
//! parameter (those are checked at the call that builds them), and a `let`
//! with several branches is read as the union of its literals.

use std::fs;
use std::path::Path;

const WRITE_VERBS: [&str; 3] = ["INSERT", "UPDATE", "DELETE"];
const SCHEMA_VERBS: [&str; 4] = ["CREATE", "DROP", "ALTER", "TRUNCATE"];

/// Calls that run a statement: the name, the index of the argument holding the
/// statement text (`None` when the call has no text of its own), and the level
/// the call states by itself (`None` when the level is in its arguments).
const CALLS: &[(&str, Option<usize>, Option<Kind>)] = &[
    ("query_rows_at", Some(1), Some(Kind::Read)),
    ("query_optional_at", Some(1), Some(Kind::Read)),
    ("query_rows_quorum", Some(1), Some(Kind::Read)),
    ("execute_quorum", Some(1), Some(Kind::Write)),
    ("query_lwt", Some(1), Some(Kind::Lwt)),
    ("execute_at", Some(1), None),
    ("execute_bound", Some(1), None),
    ("query_with_params", Some(0), None),
    ("query_with_pk_sk", Some(1), None),
    ("query_with_pk_sk_txnid", Some(1), None),
    ("query_with_txnid_ts_pk_sk", Some(1), None),
    ("query_with_pk_sk_item_txnid_ts", Some(1), None),
    ("query_with_item_ts_pk_sk_txnid", Some(1), None),
    ("query_with_pk_sk_sk", Some(1), None),
    ("query_with_pk_sk_pk_sk", Some(1), None),
    ("query_with_pk_sk_pk", Some(1), None),
    ("query_with_pk_pk_sk", Some(1), None),
    ("query_with_pk_sk_sk_sk", Some(1), None),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Read,
    Write,
    Lwt,
}

/// The source of every file under `src/data/`, without its test module.
fn data_sources() -> Vec<(String, String)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/data");
    let mut out = Vec::new();
    let mut entries: Vec<_> = fs::read_dir(&dir)
        .expect("read src/data")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .filter(|p| p.file_name().is_some_and(|n| n != "level_pairing_tests.rs"))
        .collect();
    entries.sort();
    for path in entries {
        let src = fs::read_to_string(&path).expect("read source");
        // A file's own test module is left out; the `mod level_pairing_tests;`
        // declaration in `mod.rs` is not a test module and does not cut it.
        let live = match src.find("\n#[cfg(test)]\nmod tests") {
            Some(i) => src[..i].to_owned(),
            None => src,
        };
        out.push((
            path.file_name().unwrap().to_string_lossy().into_owned(),
            live,
        ));
    }
    assert!(out.len() > 5, "src/data holds the data path");
    out
}

/// The index of the `)` closing the `(` at `open`.
fn matching_paren(text: &str, open: usize) -> usize {
    let bytes = text.as_bytes();
    let (mut depth, mut i, mut in_str) = (0usize, open, false);
    while i < bytes.len() {
        let c = bytes[i];
        if in_str {
            if c == b'\\' {
                i += 2;
                continue;
            }
            if c == b'"' {
                in_str = false;
            }
        } else if c == b'"' {
            in_str = true;
        } else if c == b'(' {
            depth += 1;
        } else if c == b')' {
            depth -= 1;
            if depth == 0 {
                return i;
            }
        }
        i += 1;
    }
    bytes.len() - 1
}

/// Top-level comma split of an argument list.
fn split_args(text: &str) -> Vec<String> {
    let (mut args, mut depth, mut cur, mut in_str) = (Vec::new(), 0i32, String::new(), false);
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if in_str {
            cur.push(c);
            if c == '\\' {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
                continue;
            }
            if c == '"' {
                in_str = false;
            }
        } else if c == '"' {
            in_str = true;
            cur.push(c);
        } else if "([{".contains(c) {
            depth += 1;
            cur.push(c);
        } else if ")]}".contains(c) {
            depth -= 1;
            cur.push(c);
        } else if c == ',' && depth == 0 {
            args.push(cur.trim().to_owned());
            cur.clear();
        } else {
            cur.push(c);
        }
    }
    if !cur.trim().is_empty() {
        args.push(cur.trim().to_owned());
    }
    args
}

/// Every string literal in `text`, joined.
fn literals(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find('"') {
        let body = &rest[start + 1..];
        let mut end = None;
        let mut i = 0;
        let b = body.as_bytes();
        while i < b.len() {
            if b[i] == b'\\' {
                i += 2;
                continue;
            }
            if b[i] == b'"' {
                end = Some(i);
                break;
            }
            i += 1;
        }
        let Some(end) = end else { break };
        out.push_str(&body[..end]);
        out.push(' ');
        rest = &body[end + 1..];
    }
    out
}

fn has_verb(text: &str, verbs: &[&str]) -> bool {
    text.split(|c: char| !c.is_ascii_alphabetic())
        .any(|w| verbs.contains(&w))
}

/// The text of the nearest `let <ident> = ...;` (or `let (<ident>, ..) = ...;`)
/// binding before `call_pos`, from `let` to its `;` at nesting depth 0.
fn binding_body(src: &str, call_pos: usize, ident: &str) -> Option<(usize, String)> {
    let head = &src[..call_pos];
    let mut best: Option<usize> = None;
    for (i, _) in head.match_indices("let ") {
        let after = &head[i + 4..];
        let after = after.trim_start_matches("mut ").trim_start();
        let names: Vec<&str> = if let Some(stripped) = after.strip_prefix('(') {
            stripped
                .split(')')
                .next()
                .unwrap_or("")
                .split(',')
                .map(|s| s.trim().trim_start_matches("mut ").trim())
                .collect()
        } else {
            vec![
                after
                    .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                    .next()
                    .unwrap_or(""),
            ]
        };
        if names.contains(&ident) {
            best = Some(i);
        }
    }
    let start = best?;
    let bytes = head.as_bytes();
    let (mut depth, mut j, mut in_str) = (0i32, start, false);
    while j < bytes.len() {
        let c = bytes[j];
        if in_str {
            if c == b'\\' {
                j += 2;
                continue;
            }
            if c == b'"' {
                in_str = false;
            }
        } else if c == b'"' {
            in_str = true;
        } else if b"([{".contains(&c) {
            depth += 1;
        } else if b")]}".contains(&c) {
            depth -= 1;
        } else if c == b';' && depth <= 0 {
            break;
        }
        j += 1;
    }
    Some((start, head[start..j].to_owned()))
}

/// The CQL text the statement argument resolves to, or `None` for a value the
/// call receives as a parameter.
fn statement_text(src: &str, call_pos: usize, arg: &str, hops: u8) -> Option<String> {
    let a = arg.trim().trim_start_matches('&').trim();
    if a.starts_with("format!") || a.starts_with('"') {
        return Some(literals(a));
    }
    let ident: String = a
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if ident.is_empty() {
        let l = literals(a);
        return (!l.trim().is_empty()).then_some(l);
    }
    let (start, body) = binding_body(src, call_pos, &ident)?;
    let body = body.as_str();
    let text = literals(body);
    if !has_verb(&text, &WRITE_VERBS)
        && !text.contains("SELECT")
        && !has_verb(&text, &SCHEMA_VERBS)
        && hops > 0
    {
        // Built from another binding: `format!("{query} AND ..")` or
        // `format!("{} ..", query)`.
        let mut others: Vec<String> = Vec::new();
        let mut rest = text.as_str();
        while let Some(o) = rest.find('{') {
            let after = &rest[o + 1..];
            let name: String = after
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() && after[name.len()..].starts_with('}') {
                others.push(name);
            }
            rest = after;
        }
        for piece in body.split(',') {
            let p = piece.trim().trim_start_matches('&');
            let name: String = p
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() && name != ident && p.len() == name.len() {
                others.push(name);
            }
        }
        for other in others {
            if other == ident {
                continue;
            }
            if let Some(resolved) = statement_text(src, start, &other, hops - 1)
                && (resolved.contains("SELECT") || has_verb(&resolved, &WRITE_VERBS))
            {
                return Some(format!("{resolved} {text}"));
            }
        }
    }
    (!text.trim().is_empty()).then_some(text)
}

/// The level the call's arguments state, when the call itself does not.
fn level_in_args(src: &str, call_pos: usize, args: &[String]) -> Option<Kind> {
    let mut joined = args.join(" ");
    // A level passed through a variable: `let params = |v| read_params(..)`.
    for a in args {
        let a = a.trim();
        if !a.is_empty()
            && a.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !["true", "false", "consistent_read"].contains(&a)
            && let Some((_, body)) = binding_body(src, call_pos, a)
        {
            joined.push(' ');
            joined.push_str(&body);
        }
    }
    if joined.contains("lwt_params") {
        Some(Kind::Lwt)
    } else if joined.contains("write_params") {
        Some(Kind::Write)
    } else if joined.contains("read_params") {
        Some(Kind::Read)
    } else if args.iter().any(|a| {
        a == "true" || a == "false" || a == "consistent_read" || a.ends_with(".consistent_read")
    }) {
        // A read helper that takes the flag itself (`query_helpers.rs`).
        Some(Kind::Read)
    } else {
        None
    }
}

fn is_lwt(text: &str) -> bool {
    text.split_whitespace().any(|w| w == "IF") && !has_verb(text, &SCHEMA_VERBS)
}

#[test]
fn every_data_path_statement_pairs_its_kind_with_its_level() {
    let mut sites = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for (file, src) in data_sources() {
        for (name, qarg, own) in CALLS {
            for (pos, _) in src.match_indices(name) {
                // A whole identifier, as a call: not a longer name, not a
                // definition, not a method on a receiver.
                let before = src[..pos].chars().next_back();
                if before.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_') {
                    continue;
                }
                // The helpers are free functions; `query_with_params` is the
                // one `Session` method the data path calls with parameters.
                if before == Some('.') && *name != "query_with_params" {
                    continue;
                }
                let after = &src[pos + name.len()..];
                let after_trim = after.trim_start();
                let open_rel = if after_trim.starts_with("::<") {
                    after_trim.find('(')
                } else if after_trim.starts_with('(') {
                    Some(0)
                } else {
                    None
                };
                let Some(open_rel) = open_rel else { continue };
                if after_trim[..open_rel].contains('\n') {
                    continue;
                }
                let line_start = src[..pos].rfind('\n').map_or(0, |i| i + 1);
                let line = src[line_start..pos].trim_start();
                if line.starts_with("fn ")
                    || line.starts_with("pub")
                    || line.starts_with("async fn")
                    || line.starts_with("//")
                {
                    continue;
                }
                let open = pos + name.len() + (after.len() - after_trim.len()) + open_rel;
                let close = matching_paren(&src, open);
                let args = split_args(&src[open + 1..close]);
                let lineno = src[..pos].matches('\n').count() + 1;
                let site = format!("{file}:{lineno} {name}");
                let level = own.or_else(|| level_in_args(&src, pos, &args));
                let text = qarg
                    .and_then(|i| args.get(i))
                    .and_then(|a| statement_text(&src, pos, a, 2));
                let Some(level) = level else {
                    // A helper handed the caller's constructor (`params`) with
                    // a statement it also received: checked at the caller. A
                    // resolvable statement with no resolvable level is not.
                    if let Some(text) = text {
                        failures.push(format!("{site}: statement {text:?} states no level"));
                    }
                    continue;
                };
                let Some(text) = text else {
                    // The statement arrives as a parameter of the enclosing
                    // function; its own call site is checked where it is built.
                    continue;
                };
                sites += 1;
                let select = text.contains("SELECT");
                let write = has_verb(&text, &WRITE_VERBS);
                let lwt = is_lwt(&text);
                let expected = if lwt {
                    Kind::Lwt
                } else if write {
                    Kind::Write
                } else if select {
                    Kind::Read
                } else {
                    failures.push(format!("{site}: no CQL verb in {text:?}"));
                    continue;
                };
                if expected != level {
                    failures.push(format!(
                        "{site}: statement {text:?} is {expected:?} but the call states {level:?}"
                    ));
                }
            }
        }
        // A batch starts only from `batch_builder()`, which sets its level:
        // no `BatchQueryBuilder::new`, `::default` or `Default::default()`
        // under `data/`, and no `.with_consistency(` that could lower a level
        // after the fact.
        for needle in [
            "BatchQueryBuilder::new",
            "BatchQueryBuilder::default",
            "Default::default()",
            ".with_consistency(",
        ] {
            for (pos, _) in src.match_indices(needle) {
                let line_start = src[..pos].rfind('\n').map_or(0, |i| i + 1);
                if src[line_start..pos].trim_start().starts_with("//") {
                    continue;
                }
                let lineno = src[..pos].matches('\n').count() + 1;
                failures.push(format!(
                    "{file}:{lineno}: `{needle}` under data/; batches are built with cassandra_util::batch_builder()"
                ));
            }
        }
    }
    // 121 sites resolve on the tree this test was written against; the
    // statements the `query_helpers.rs` and `occ_delete_lwt` helpers receive
    // as parameters are checked where they are built.
    assert!(
        sites >= 110,
        "resolved {sites} statement sites; the walk lost its anchors"
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
