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

/// Driver methods that send a statement or a batch, and what to use instead.
/// [`forbidden`] matches each one as a method call (`.m(`), a method call
/// with type arguments (`.m::<`), a path call (`Session::m(&s, …)`), and a
/// method taken as a value (`let send = Session::m;`).
const SEND_METHODS: &[(&str, &str)] = &[
    ("query", "PolicySession::run"),
    ("query_with_values", "PolicySession::run"),
    ("query_with_params", "PolicySession::run"),
    ("exec", "PolicySession::run"),
    ("exec_with_values", "PolicySession::run"),
    ("exec_with_params", "PolicySession::run"),
    ("batch", "PolicySession::run_batch"),
    ("batch_with_params", "PolicySession::run_batch"),
    // `session.paged(n)` returns a pager that sends at the default level.
    ("paged", "PolicySession::run"),
    ("query_with_pager_state", "PolicySession::run"),
    ("query_with_pager_state_params", "PolicySession::run"),
    ("exec_with_pager_state", "PolicySession::run"),
];

/// The characters that can follow a method taken as a value, such as
/// `Session::query_with_values;` or `.map(Session::query)`. The end of a line
/// ends it too: rustfmt can put the method path last on a line. Text such as
/// `cdrs_tokio::query::QueryBatch` continues with `::` and is not a method.
const VALUE_END_CHARS: &str = ";,)]} \t";

/// Other text that bypasses the policy, and what to use instead.
const FORBIDDEN_TEXT: &[(&str, &str)] = &[
    ("StatementParamsBuilder::new(", "cassandra_util::params"),
    ("BatchQueryBuilder::new(", "cassandra_util::batch"),
    ("BatchQueryBuilder::default(", "cassandra_util::batch"),
    ("QueryBatch::new(", "cassandra_util::batch"),
    ("Consistency::", "a Stmt kind"),
    // A raw request frame on a connection, as `migrations/agreement.rs` sends.
    ("Envelope::new_req_query(", "PolicySession::run"),
    ("Envelope::new_req_execute(", "PolicySession::run"),
    ("Envelope::new_req_batch(", "PolicySession::run_batch"),
    ("Envelope::new_query(", "PolicySession::run"),
    ("BodyReqQuery::new(", "PolicySession::run"),
    ("BodyReqBatch::new(", "PolicySession::run_batch"),
    // The driver functions that send a frame to the nodes of a query plan,
    // or to one connection.
    ("send_envelope(", "PolicySession::run"),
    ("write_envelope(", "PolicySession::run"),
    // The driver call that gives a raw connection to a node, and the
    // constructor of a request frame that has no level checked.
    ("persistent_connection(", "PolicySession::run"),
    ("Envelope::new(", "PolicySession::run"),
];

/// Text that contains a forbidden pattern but is the policy's own API. The
/// scan blanks it out before it matches.
const ALLOWED: &[&str] = &["cassandra_util::batch(", "cassandra_util::batch;"];

/// Each forbidden pattern, and what to use instead.
fn forbidden() -> Vec<(String, &'static str)> {
    let mut patterns = Vec::new();
    for (method, instead) in SEND_METHODS {
        for form in [".{m}(", ".{m}::<", "::{m}(", "::{m}::<"] {
            patterns.push((form.replace("{m}", method), *instead));
        }
        // A method taken as a value. The `$` ends the pattern: it matches
        // only when [`VALUE_END_CHARS`] or the end of the line follows.
        patterns.push((format!("::{method}$"), *instead));
    }
    for (text, instead) in FORBIDDEN_TEXT {
        patterns.push(((*text).to_owned(), *instead));
    }
    patterns
}

/// Calls that bypass the policy, each with the pattern that must find it.
/// A pattern removed from [`SEND_METHODS`], [`FORBIDDEN_TEXT`] or the call
/// forms in [`forbidden`] fails `the_scan_finds_each_known_bypass`.
const KNOWN_BYPASSES: &[(&str, &str)] = &[
    (r#"session.query("SELECT a FROM t").await"#, ".query("),
    (
        "session.query_with_values(q, values).await",
        ".query_with_values(",
    ),
    (
        "session.query_with_params(q, params).await",
        ".query_with_params(",
    ),
    ("session.exec(&prepared).await", ".exec("),
    (
        "session.exec_with_values(&prepared, values).await",
        ".exec_with_values(",
    ),
    (
        "session.exec_with_params(&prepared, &params).await",
        ".exec_with_params(",
    ),
    ("session.batch(built).await", ".batch("),
    (
        "session.batch_with_params(built, params).await",
        ".batch_with_params(",
    ),
    ("let pager = session.paged(100);", ".paged("),
    (
        "pager.query_with_pager_state(q, state)",
        ".query_with_pager_state(",
    ),
    (
        "pager.query_with_pager_state_params(q, state, params)",
        ".query_with_pager_state_params(",
    ),
    (
        "pager.exec_with_pager_state(&prepared, state)",
        ".exec_with_pager_state(",
    ),
    ("Session::query(&session, cql).await", "::query("),
    (
        "CassandraSession::query_with_values(&session, q, values).await",
        "::query_with_values(",
    ),
    ("Session::batch(&session, built).await", "::batch("),
    ("session.query::<String>(cql).await", ".query::<"),
    (
        "Session::query::<String>(&session, cql).await",
        "::query::<",
    ),
    (
        "let p = StatementParamsBuilder::new().build();",
        "StatementParamsBuilder::new(",
    ),
    (
        "let b = BatchQueryBuilder::new();",
        "BatchQueryBuilder::new(",
    ),
    (
        "let b = BatchQueryBuilder::default();",
        "BatchQueryBuilder::default(",
    ),
    ("let b = QueryBatch::new(r, m);", "QueryBatch::new("),
    ("let level = Consistency::One;", "Consistency::"),
    (
        "let e = Envelope::new_req_query(q, level, None, false, version);",
        "Envelope::new_req_query(",
    ),
    (
        "let e = Envelope::new_req_execute(id, None, &params, flags, version);",
        "Envelope::new_req_execute(",
    ),
    (
        "let e = Envelope::new_req_batch(built, flags, version);",
        "Envelope::new_req_batch(",
    ),
    (
        "let e = Envelope::new_query(body, flags, version);",
        "Envelope::new_query(",
    ),
    (
        "let body = BodyReqQuery::new(q, Default::default(), None, false, None, None, None, None, None, None);",
        "BodyReqQuery::new(",
    ),
    (
        "let body = BodyReqBatch::new(queries, kind, level, serial, None, None);",
        "BodyReqBatch::new(",
    ),
    (
        "send_envelope(plan.into_iter(), &envelope, true, retry).await",
        "send_envelope(",
    ),
    (
        "connection.write_envelope(&envelope, false).await",
        "write_envelope(",
    ),
    (
        "let connection = node.persistent_connection().await?;",
        "persistent_connection(",
    ),
    (
        "let e = Envelope::new(version, flags, opcode, stream, body, tracing_id);",
        "Envelope::new(",
    ),
    (
        "let send = CassandraSession::query_with_values;",
        "::query_with_values$",
    ),
    ("let send = Session::batch;", "::batch$"),
    ("run_all(rows, Session::query)", "::query$"),
    ("let f = (Session::exec, Session::paged);", "::exec$"),
    (
        "let f = [Session::query_with_params];",
        "::query_with_params$",
    ),
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

/// The index after the string, raw string or char literal that starts at
/// `chars[i]`, or `None` when no literal starts there. A `'` that starts a
/// lifetime is not a literal.
fn literal_end(chars: &[char], i: usize) -> Option<usize> {
    if let Some(hashes) = raw_string_start(chars, i) {
        let close: Vec<char> = std::iter::once('"')
            .chain(std::iter::repeat_n('#', hashes))
            .collect();
        let mut j = i + 2 + hashes;
        while j < chars.len() && !chars[j..].starts_with(&close) {
            j += 1;
        }
        return Some((j + close.len()).min(chars.len()));
    }
    let is_char_literal =
        chars[i] == '\'' && (chars.get(i + 2) == Some(&'\'') || chars.get(i + 1) == Some(&'\\'));
    if chars[i] == '"' || is_char_literal {
        let quote = chars[i];
        let mut j = i + 1;
        while j < chars.len() && chars[j] != quote {
            j += if chars[j] == '\\' { 2 } else { 1 };
        }
        return Some((j + 1).min(chars.len()));
    }
    None
}

/// `code` with each `#[cfg(test)]` attribute followed by `mod NAME {` removed
/// up to its closing brace, keeping the newlines. An attribute inside a
/// string literal is not an attribute.
fn without_test_modules(code: &str) -> String {
    let attribute: Vec<char> = "#[cfg(test)]".chars().collect();
    let chars: Vec<char> = code.chars().collect();
    let mut out = String::with_capacity(code.len());
    let mut i = 0;
    while i < chars.len() {
        if let Some(end) = literal_end(&chars, i) {
            out.extend(&chars[i..end]);
            i = end;
            continue;
        }
        if chars[i..].starts_with(&attribute) {
            let after = i + attribute.len();
            let rest: String = chars[after..].iter().collect();
            let brace = rest.find('{');
            let is_module = rest.trim_start().starts_with("mod ")
                && brace.is_some_and(|brace| {
                    rest[..brace].trim_start().starts_with("mod ") && !rest[..brace].contains(';')
                });
            if let (true, Some(brace)) = (is_module, brace) {
                let open = after + rest[..brace].chars().count();
                let end = open + block_len(&chars[open..]);
                out.extend(chars[i..end].iter().filter(|c| **c == '\n'));
                i = end;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// The length in chars of the block that `chars` starts with: up to and
/// including the `}` that closes its first `{`, or all of `chars` when no `}`
/// does. A brace inside a string, a raw string or a char literal does not
/// count. The text has no comments (see [`code_only`]).
fn block_len(chars: &[char]) -> usize {
    let mut depth = 0_usize;
    let mut i = 0;
    while i < chars.len() {
        if let Some(end) = literal_end(chars, i) {
            i = end;
            continue;
        }
        match chars[i] {
            '{' => depth += 1,
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return i + 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    chars.len()
}

/// Each forbidden call in `source`: its line number, the call, and what to
/// use instead. A call is skipped when [`MARKER`] is on its line or on the
/// line above it (where `cargo fmt` keeps a comment).
fn bypasses(source: &str) -> Vec<(usize, String, &'static str)> {
    let originals: Vec<&str> = source.lines().collect();
    let marked = |index: usize| originals.get(index).is_some_and(|l| l.contains(MARKER));
    let patterns = forbidden();
    let mut found = Vec::new();
    for (index, line) in code_only(source).lines().enumerate() {
        if marked(index) || index > 0 && marked(index - 1) {
            continue;
        }
        let mut line = line.to_owned();
        for allowed in ALLOWED {
            line = line.replace(allowed, &" ".repeat(allowed.len()));
        }
        for (pattern, instead) in &patterns {
            if matches_pattern(&line, pattern) {
                found.push((index + 1, pattern.clone(), *instead));
            }
        }
    }
    found
}

/// Whether `line` holds `pattern`. A pattern that ends in `$` must be
/// followed by one of [`VALUE_END_CHARS`] or by the end of the line.
fn matches_pattern(line: &str, pattern: &str) -> bool {
    match pattern.strip_suffix('$') {
        None => line.contains(pattern),
        Some(prefix) => line.match_indices(prefix).any(|(at, _)| {
            line[at + prefix.len()..]
                .chars()
                .next()
                .is_none_or(|c| VALUE_END_CHARS.contains(c))
        }),
    }
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
    for (call, _) in forbidden() {
        let source = match call.strip_suffix('$') {
            Some(prefix) => format!("fn f() {{\n    let x = y{prefix}\n}}\n"),
            None => format!("fn f() {{\n    let x = y{call}a);\n}}\n"),
        };
        let found = bypasses(&source);
        assert!(
            found.iter().any(|(line, c, _)| *line == 2 && *c == call),
            "{call} was not found: {found:?}"
        );
    }
}

#[test]
fn the_scan_finds_each_known_bypass() {
    for (bypass, pattern) in KNOWN_BYPASSES {
        let source = format!("fn f() {{\n    {bypass}\n}}\n");
        let found = bypasses(&source);
        assert!(
            found.iter().any(|(line, p, _)| *line == 2 && p == pattern),
            "`{pattern}` did not find `{bypass}`: {found:?}"
        );
    }
}

#[test]
fn the_scan_allows_the_policy_batch_builder() {
    let source = "use crate::cassandra_util::batch;\nfn f() {\n    \
                  let b = crate::cassandra_util::batch();\n    \
                  let c = cassandra_util::batch().add_query(q, v);\n}\n";
    assert_eq!(
        bypasses(source),
        [],
        "cassandra_util::batch() is the policy"
    );
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
            .map(|(line, call, _)| (*line, call.as_str()))
            .collect::<Vec<_>>(),
        [(8, ".query("), (19, ".query_with_values(")],
        "{found:?}"
    );
}

#[test]
fn a_brace_in_a_literal_does_not_hide_the_code_after_a_test_module() {
    let source = r##"
#[cfg(test)]
mod tests {
    fn g() { let a = "{"; let b = '{'; let c = r#"{ " {"#; let d = "\"{"; }
}
fn h() {
    session.query("x");
}
"##;
    let found = bypasses(source);
    assert_eq!(
        found
            .iter()
            .map(|(line, call, _)| (*line, call.as_str()))
            .collect::<Vec<_>>(),
        [(7, ".query(")],
        "{found:?}"
    );
}

#[test]
fn a_brace_in_a_literal_does_not_end_a_test_module_early() {
    let source = r##"
#[cfg(test)]
mod tests {
    fn g() { let a = "}"; let b = '}'; let c = r#"} " }"#; session.query("x"); }
}
"##;
    assert_eq!(bypasses(source), [], "the call is in the test module");
}

#[test]
fn the_scan_finds_a_method_taken_as_a_value_at_the_end_of_a_line() {
    // rustfmt writes a long `if` expression with the method on its own line.
    let source = "fn f() {\n    let send = if paged {\n        \
                  CassandraSession::query_with_values\n    } else {\n        \
                  CassandraSession::query\n    };\n    \
                  let all = [CassandraSession::batch];\n}\n";
    let found = bypasses(source);
    assert_eq!(
        found
            .iter()
            .map(|(line, call, _)| (*line, call.as_str()))
            .collect::<Vec<_>>(),
        [
            (3, "::query_with_values$"),
            (5, "::query$"),
            (7, "::batch$")
        ],
        "{found:?}"
    );
}

#[test]
fn a_test_module_attribute_inside_a_string_does_not_hide_the_code_after_it() {
    let source = r##"
const SAMPLE: &str = "#[cfg(test)] mod x {";
fn h() {
    session.query("x");
}
"##;
    let found = bypasses(source);
    assert_eq!(
        found
            .iter()
            .map(|(line, call, _)| (*line, call.as_str()))
            .collect::<Vec<_>>(),
        [(4, ".query(")],
        "{found:?}"
    );
}
