// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Every statement outside the consistency policy runs through it (#770).
//!
//! `cassandra_util` sets the consistency level of every statement: `Stmt` and
//! `params` for one statement, `batch` for a batch, and `PolicySession::run`
//! to send it. A driver call that builds its own parameters, or sends a
//! statement with the driver's default level (`ONE`), would bypass that. This
//! test reads the crate's non-test source and fails on each such call.
//!
//! Two escapes are deliberate. Files that set their own levels on purpose are
//! exempt by path, each with a reason. And one call may opt out with a
//! `// consistency-policy-ok: <reason>` comment on its line or the line above.

use std::path::{Path, PathBuf};

/// Calls that bypass the policy, and what to use instead.
const FORBIDDEN: &[(&str, &str)] = &[
    (".query(", "PolicySession::run"),
    (".query_with_values(", "PolicySession::run"),
    (".query_with_params(", "PolicySession::run"),
    (".exec(", "PolicySession::run"),
    (".exec_with_values(", "PolicySession::run"),
    (".exec_with_params(", "PolicySession::run"),
    ("StatementParamsBuilder::new(", "cassandra_util::params"),
    ("BatchQueryBuilder::new(", "cassandra_util::batch"),
    ("BatchQueryBuilder::default(", "cassandra_util::batch"),
    ("QueryBatch::new(", "cassandra_util::batch"),
    ("Consistency::", "a Stmt kind"),
];

/// Source paths, relative to the crate, that set their own levels.
///
/// Each entry states why, because an unexplained exemption is how a check
/// becomes decoration.
const EXEMPT: &[(&str, &str)] = &[
    (
        "src/cassandra_util.rs",
        "the policy itself: Stmt, params, batch and PolicySession::run set every level",
    ),
    (
        "src/migrations/",
        "migrations set their own levels: QUORUM, SERIAL, ALL and LOCAL_QUORUM, each at \
         least as strong as the policy's level, and ONE only for reads of node-local \
         system tables (system.local, system_schema, system_views)",
    ),
];

/// A trailing comment that lets one line keep a forbidden call.
const MARKER: &str = "// consistency-policy-ok:";

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// `source` with comments blanked out and each `#[cfg(test)] mod … { … }`
/// removed. Newlines stay, so line numbers do not move. String and char
/// literals are kept, and `//` inside a string is not a comment.
fn code_only(source: &str) -> String {
    let chars: Vec<char> = source.chars().collect();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if c == '/' && next == Some('/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && next == Some('*') {
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                if chars[i] == '\n' {
                    out.push('\n');
                }
                i += 1;
            }
            i += 2;
        } else if let Some(hashes) = raw_string_start(&chars, i) {
            // r"…" or r#"…"#: ends at a quote followed by the same hashes.
            let close: Vec<char> = std::iter::once('"')
                .chain(std::iter::repeat_n('#', hashes))
                .collect();
            let body = i + 2 + hashes;
            out.extend(&chars[i..body]);
            i = body;
            while i < chars.len() && !chars[i..].starts_with(&close) {
                out.push(chars[i]);
                i += 1;
            }
            out.extend(chars[i..].iter().take(close.len()));
            i += close.len();
        } else if c == '"' {
            out.push(c);
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    out.push(chars[i]);
                    i += 1;
                }
                if i < chars.len() {
                    out.push(chars[i]);
                    i += 1;
                }
            }
            if i < chars.len() {
                out.push('"');
                i += 1;
            }
        } else if c == '\'' && (chars.get(i + 2) == Some(&'\'') || next == Some('\\')) {
            // A char literal such as '"' or '\n', which must not open a string.
            out.push(c);
            i += 1;
            while i < chars.len() && chars[i] != '\'' {
                if chars[i] == '\\' {
                    out.push(chars[i]);
                    i += 1;
                }
                out.push(chars[i]);
                i += 1;
            }
            out.push('\'');
            i += 1;
        } else {
            out.push(c);
            i += 1;
        }
    }
    without_test_modules(&out)
}

/// The number of `#` in the raw string literal that starts at `i`, or `None`
/// when no raw string starts there. `r` must not end an identifier.
fn raw_string_start(chars: &[char], i: usize) -> Option<usize> {
    if chars[i] != 'r' || i > 0 && (chars[i - 1].is_alphanumeric() || chars[i - 1] == '_') {
        return None;
    }
    let hashes = chars[i + 1..].iter().take_while(|c| **c == '#').count();
    (chars.get(i + 1 + hashes) == Some(&'"')).then_some(hashes)
}

/// `code` with each `#[cfg(test)]` attribute followed by `mod NAME {` removed
/// up to its closing brace, keeping the newlines.
fn without_test_modules(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    let mut rest = code;
    while let Some(at) = rest.find("#[cfg(test)]") {
        let after = &rest[at + "#[cfg(test)]".len()..];
        let is_module = after.trim_start().starts_with("mod ")
            && after.find('{').is_some_and(|brace| {
                after[..brace].trim_start().starts_with("mod ") && !after[..brace].contains(';')
            });
        if !is_module {
            out.push_str(&rest[..at + "#[cfg(test)]".len()]);
            rest = after;
            continue;
        }
        out.push_str(&rest[..at]);
        let open = after.find('{').expect("a module body");
        let mut depth = 0_usize;
        let mut end = after.len();
        for (offset, c) in after[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = open + offset + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        let removed = &rest[at..at + "#[cfg(test)]".len() + end];
        out.extend(removed.chars().filter(|c| *c == '\n'));
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

/// Each forbidden call in `source`: its line number, the call, and what to
/// use instead. A call is skipped when [`MARKER`] is on its line or on the
/// line above it (where `cargo fmt` keeps a comment).
fn bypasses(source: &str) -> Vec<(usize, &'static str, &'static str)> {
    let originals: Vec<&str> = source.lines().collect();
    let marked = |index: usize| originals.get(index).is_some_and(|l| l.contains(MARKER));
    let mut found = Vec::new();
    for (index, line) in code_only(source).lines().enumerate() {
        if marked(index) || index > 0 && marked(index - 1) {
            continue;
        }
        for (call, instead) in FORBIDDEN {
            if line.contains(call) {
                found.push((index + 1, *call, *instead));
            }
        }
    }
    found
}

fn exempt(relative: &str) -> bool {
    EXEMPT.iter().any(|(path, _)| {
        if path.ends_with('/') {
            relative.starts_with(path)
        } else {
            relative == *path
        }
    })
}

#[test]
fn no_statement_bypasses_the_consistency_policy() {
    let root = crate_root();
    let mut files = Vec::new();
    rust_files(&root.join("src"), &mut files);
    files.sort();
    assert!(
        !files.is_empty(),
        "no source files under {}",
        root.display()
    );

    let mut problems = Vec::new();
    for file in &files {
        let relative = file
            .strip_prefix(&root)
            .expect("under the crate")
            .to_string_lossy()
            .replace('\\', "/");
        if exempt(&relative) {
            continue;
        }
        let source = std::fs::read_to_string(file).expect("readable source");
        for (line, call, instead) in bypasses(&source) {
            problems.push(format!("{relative}:{line}: `{call}`; use {instead}"));
        }
    }
    assert!(
        problems.is_empty(),
        "{} statement(s) bypass the consistency policy in cassandra_util:\n{}",
        problems.len(),
        problems.join("\n")
    );
}

#[test]
fn each_exempt_path_exists() {
    let root = crate_root();
    for (path, reason) in EXEMPT {
        assert!(!reason.is_empty(), "{path} has no reason");
        assert!(
            root.join(path.trim_end_matches('/')).exists(),
            "{path} is exempt but does not exist; remove the entry"
        );
    }
}

#[test]
fn the_scan_finds_each_forbidden_call() {
    for (call, _) in FORBIDDEN {
        let source = format!("fn f() {{\n    let x = y{call}a);\n}}\n");
        let found = bypasses(&source);
        assert!(
            found.iter().any(|(line, c, _)| *line == 2 && c == call),
            "{call} was not found: {found:?}"
        );
    }
}

#[test]
fn the_scan_skips_comments_test_modules_and_marked_lines() {
    let source = r##"
// session.query("x")
/* session.query_with_values("x", v) */
/// Use `BatchQueryBuilder::new()` here.
fn f() {
    let s = "http://example//not.a.comment";
    let c = '"';
    let raw = r#"a "b" // c"#; session.query("x");
    session.run(Stmt::Read, "SELECT a FROM t", v); // not .query(
    let b = QueryBatch::new(r, m); // consistency-policy-ok: on the same line
    // consistency-policy-ok: on the line above
    let b = QueryBatch::new(r, m);
}
#[cfg(test)]
mod tests {
    fn g() { session.query("x"); let b = BatchQueryBuilder::new(); }
}
fn h() {
    session.query_with_values("x", v);
}
"##;
    let found = bypasses(source);
    assert_eq!(
        found
            .iter()
            .map(|(line, call, _)| (*line, *call))
            .collect::<Vec<_>>(),
        [(8, ".query("), (19, ".query_with_values(")],
        "{found:?}"
    );
}
