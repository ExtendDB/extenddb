// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Checksums of migration bodies, and the test every backend runs on its
//! table to keep released bodies frozen.

use sha2::{Digest, Sha256};

use super::registry::MigrationId;
use super::table::{Body, Kind, Migration, StoreBody, StoreKind};

/// SHA-256 of a file body with CRLF normalised to LF, as lower-case hex. The
/// checksum of a code body's source and of a `NotApplicable` reason are
/// computed the same way.
#[must_use]
pub fn file_checksum(text: &str) -> String {
    let normalized = text.replace("\r\n", "\n");
    Sha256::digest(normalized.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A released body's pinned checksum, kind and convergent flag, as a
/// backend's `RELEASED` table lists them. The first entry for an `(id, store)`
/// is the released body; each later one is a deliberate change and carries a
/// `rebless` reason. A file body is recorded with the first entry's checksum
/// and the runtime accepts any entry's, so every release reads the same
/// ledger row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Released {
    pub id: MigrationId,
    pub store: StoreKind,
    /// SHA-256 of the file, of the code body's source, or of the
    /// `NotApplicable` reason.
    pub checksum: &'static str,
    /// The fingerprint a code body records in the ledger; `None` for a file
    /// body and a `NotApplicable` entry. It never changes after release.
    pub fingerprint: Option<&'static str>,
    pub kind: Kind,
    pub convergent: bool,
    pub rebless: Option<&'static str>,
}

/// The checksum the test pins for a body: of the file, or of the code
/// body's source.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn pinned_checksum<H: 'static>(body: &Body<H>) -> String {
    match body {
        Body::File { text, .. } => file_checksum(text),
        Body::Code { source, .. } => file_checksum(source),
    }
}

/// The checksum this binary computes for a body's ledger row, before the
/// released entries are consulted: the file's, or the code body's
/// fingerprint.
pub(crate) fn computed_checksum<H: 'static>(body: &Body<H>) -> String {
    match body {
        Body::File { text, .. } => file_checksum(text),
        Body::Code { body, .. } => body.fingerprint().to_owned(),
    }
}

/// The released checksums of `id` in a `store` kind, first entry first.
fn released_checksums(
    released: &[Released],
    id: MigrationId,
    store: StoreKind,
) -> impl Iterator<Item = String> + '_ {
    released
        .iter()
        .filter(move |r| r.id == id && r.store == store)
        .map(|r| r.checksum.to_owned())
}

/// The body a row of an id in a `kind` store records: the store's own body,
/// or, for the catalog row of an id whose bodies all run in other stores,
/// the id's first body.
fn recorded_body<H: 'static>(bodies: &[StoreBody<H>], kind: StoreKind) -> Option<&StoreBody<H>> {
    bodies
        .iter()
        .find(|b| b.store == kind)
        .or_else(|| bodies.first().filter(|_| kind == StoreKind::Catalog))
}

/// The checksum this binary records for a row of `id` in a `kind` store. A
/// file body or a `NotApplicable` reason that has been released records its
/// first released checksum, so a reblessed body writes the value every
/// release accepts; a code body records its fingerprint.
pub(crate) fn record_checksum<H: 'static>(
    released: &[Released],
    id: MigrationId,
    migration: &Migration<H>,
    kind: StoreKind,
) -> Option<String> {
    match migration {
        Migration::NotApplicable(reason) => (kind == StoreKind::Catalog).then(|| {
            released_checksums(released, id, StoreKind::Catalog)
                .next()
                .unwrap_or_else(|| file_checksum(reason))
        }),
        Migration::Applies { bodies, .. } => recorded_body(bodies, kind).map(|b| match &b.body {
            Body::File { .. } => released_checksums(released, id, b.store)
                .next()
                .unwrap_or_else(|| computed_checksum(&b.body)),
            Body::Code { .. } => computed_checksum(&b.body),
        }),
    }
}

/// The checksums this binary accepts for a row of `id` in a `kind` store:
/// the one it records, and for a file body or a `NotApplicable` reason every
/// released one and the one its current text has.
pub(crate) fn accepted_checksums<H: 'static>(
    released: &[Released],
    id: MigrationId,
    migration: &Migration<H>,
    kind: StoreKind,
) -> Vec<String> {
    let Some(recorded) = record_checksum(released, id, migration, kind) else {
        return Vec::new();
    };
    let mut accepted = vec![recorded];
    match migration {
        Migration::NotApplicable(reason) => {
            accepted.extend(released_checksums(released, id, StoreKind::Catalog));
            accepted.push(file_checksum(reason));
        }
        Migration::Applies { bodies, .. } => {
            if let Some(b) = recorded_body(bodies, kind)
                && let Body::File { .. } = &b.body
            {
                accepted.extend(released_checksums(released, id, b.store));
                accepted.push(computed_checksum(&b.body));
            }
        }
    }
    accepted
}

/// The line a release PR pastes into a backend's `RELEASED` table.
#[cfg(any(test, feature = "test-util"))]
fn released_line(
    id: MigrationId,
    store: StoreKind,
    checksum: &str,
    fingerprint: Option<&str>,
    kind: Kind,
    convergent: bool,
) -> String {
    let fingerprint = fingerprint.map_or_else(|| "None".to_owned(), |f| format!("Some(\"{f}\")"));
    format!(
        "    Released {{ id: MigrationId::{id:?}, store: StoreKind::{store:?}, \
         checksum: \"{checksum}\", fingerprint: {fingerprint}, kind: Kind::{kind:?}, \
         convergent: {convergent}, rebless: None }},"
    )
}

/// One body as the checksum test sees it.
#[cfg(any(test, feature = "test-util"))]
struct Pinned {
    store: StoreKind,
    checksum: String,
    fingerprint: Option<&'static str>,
}

/// Check a backend's migration table against its released checksums.
///
/// - Every released body (`id <= released_through`) must match the last
///   `RELEASED` entry for its id and store, every entry after the first must
///   carry a `rebless` reason, and the kind and convergent flag must equal
///   the table's. A code body's fingerprint must equal every entry's,
///   because deployed ledgers record it. A `NotApplicable` reason counts as
///   a shipped body.
/// - No entry may name an id above `released_through`, or a store the
///   table does not declare. A `NotApplicable` reason must not be empty.
/// - A file's name must be `NNNN_<name>.sql` or `.cql` with `NNNN` the id.
///   No statement of a file may begin with `BEGIN`, `COMMIT`, `END`,
///   `ROLLBACK`, `SAVEPOINT` or `RELEASE` (the statements inside a
///   `CREATE TRIGGER` body are not statements of the file), the text may not
///   mention `catalog_version` outside comments, and a CQL comment may not
///   hold a `;`.
/// - A code body's fingerprint must start with `NNNN_<name>/`.
///
/// Returns the lines to paste for every id the table carries, for the
/// release PR that raises `RELEASED_THROUGH`; on failure the error lists
/// every problem followed by the same lines.
///
/// # Errors
///
/// One message naming every problem found.
#[cfg(any(test, feature = "test-util"))]
pub fn check_released<H: 'static>(
    backend: &str,
    table: impl Fn(MigrationId) -> Migration<H>,
    released: &[Released],
    released_through: u32,
) -> Result<String, String> {
    let mut problems = Vec::new();
    let mut lines = Vec::new();
    for id in MigrationId::ALL {
        let migration = table(id);
        let mut bodies: Vec<Pinned> = Vec::new();
        match &migration {
            Migration::NotApplicable(reason) => {
                if reason.trim().is_empty() {
                    problems.push(format!("{backend} {id}: a NotApplicable reason is empty"));
                }
                bodies.push(Pinned {
                    store: StoreKind::Catalog,
                    checksum: file_checksum(reason),
                    fingerprint: None,
                });
            }
            Migration::Applies { bodies: list, .. } => {
                if list.is_empty() {
                    problems.push(format!("{backend} {id}: declares no body"));
                }
                for store_body in list {
                    let fingerprint = match &store_body.body {
                        Body::File { name, text } => {
                            check_file(backend, id, name, text, &mut problems);
                            None
                        }
                        Body::Code { body, .. } => {
                            check_fingerprint(backend, id, body.fingerprint(), &mut problems);
                            Some(body.fingerprint())
                        }
                    };
                    bodies.push(Pinned {
                        store: store_body.store,
                        checksum: pinned_checksum(&store_body.body),
                        fingerprint,
                    });
                }
            }
        }
        for body in &bodies {
            lines.push(released_line(
                id,
                body.store,
                &body.checksum,
                body.fingerprint,
                migration.kind(),
                migration.convergent(),
            ));
            if id.number() <= released_through {
                check_pins(backend, id, &migration, body, released, &mut problems);
            }
        }
        for entry in released.iter().filter(|r| r.id == id) {
            if id.number() > released_through {
                problems.push(format!(
                    "{backend} {id}: pins a checksum but is above RELEASED_THROUGH \
                     ({released_through}); unreleased bodies carry no literal"
                ));
            } else if !bodies.iter().any(|b| b.store == entry.store) {
                problems.push(format!(
                    "{backend} {id}: pins a checksum for the {} store, which the table does not \
                     declare",
                    entry.store.as_str()
                ));
            }
        }
    }
    let paste = lines.join("\n");
    if problems.is_empty() {
        Ok(paste)
    } else {
        Err(format!(
            "{}\n\nLines for the release PR that raises RELEASED_THROUGH:\n{paste}",
            problems.join("\n")
        ))
    }
}

/// The rules for one released body and its `RELEASED` entries.
#[cfg(any(test, feature = "test-util"))]
fn check_pins<H: 'static>(
    backend: &str,
    id: MigrationId,
    migration: &Migration<H>,
    body: &Pinned,
    released: &[Released],
    problems: &mut Vec<String>,
) {
    let store = body.store.as_str();
    let entries: Vec<&Released> = released
        .iter()
        .filter(|r| r.id == id && r.store == body.store)
        .collect();
    let Some(last) = entries.last() else {
        problems.push(format!(
            "{backend} {id} ({store}): released but has no pinned checksum"
        ));
        return;
    };
    if last.checksum != body.checksum {
        problems.push(format!(
            "{backend} {id} ({store}): the body changed after release (pinned {}, now {}). A \
             released body is frozen; if the change is deliberate, add an entry with the new \
             checksum and a `rebless: Some(\"<reason>\")` note",
            last.checksum, body.checksum
        ));
    }
    for later in entries.iter().skip(1) {
        if later.rebless.is_none_or(|r| r.trim().is_empty()) {
            problems.push(format!(
                "{backend} {id} ({store}): checksum {} replaces a released one without a \
                 rebless reason",
                later.checksum
            ));
        }
    }
    for entry in &entries {
        if entry.kind != migration.kind() || entry.convergent != migration.convergent() {
            problems.push(format!(
                "{backend} {id} ({store}): released as {} convergent={}, now {} \
                 convergent={}; older binaries read the released values",
                entry.kind.as_str(),
                entry.convergent,
                migration.kind().as_str(),
                migration.convergent()
            ));
        }
        if entry.fingerprint != body.fingerprint {
            problems.push(format!(
                "{backend} {id} ({store}): released with fingerprint {:?}, now {:?}; deployed \
                 ledgers record the fingerprint, so a released code body keeps it",
                entry.fingerprint, body.fingerprint
            ));
        }
    }
}

/// A code body's fingerprint starts with its id's file stem.
#[cfg(any(test, feature = "test-util"))]
fn check_fingerprint(
    backend: &str,
    id: MigrationId,
    fingerprint: &str,
    problems: &mut Vec<String>,
) {
    let stem = format!("{:04}_{}/", id.number(), id.name());
    if !fingerprint.starts_with(&stem) {
        problems.push(format!(
            "{backend} {id}: code body fingerprint {fingerprint:?} should start with {stem:?}"
        ));
    }
}

/// Statements a file body may not hold, because the runner owns the
/// transaction: each of them commits it, ends it, or nests in it.
#[cfg(any(test, feature = "test-util"))]
const TRANSACTION_KEYWORDS: [&str; 6] =
    ["BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"];

/// The file rules of [`check_released`].
#[cfg(any(test, feature = "test-util"))]
fn check_file(backend: &str, id: MigrationId, name: &str, text: &str, problems: &mut Vec<String>) {
    let expected_stem = format!("{:04}_{}", id.number(), id.name());
    let (stem, extension) = name.rsplit_once('.').unwrap_or((name, ""));
    let number = stem.split('_').next().unwrap_or_default();
    if number.parse::<u32>().ok() != Some(id.number()) {
        problems.push(format!(
            "{backend} {id}: file {name} does not start with the id's number"
        ));
    } else if stem != expected_stem {
        problems.push(format!(
            "{backend} {id}: file {name} should be named {expected_stem}.{extension}"
        ));
    }
    let cql = match extension {
        "sql" => false,
        "cql" => true,
        _ => {
            problems.push(format!(
                "{backend} {id}: file {name} is neither .sql nor .cql"
            ));
            false
        }
    };
    let (code, comments) = split_comments(text, cql);
    for keyword in statement_keywords(&code) {
        if TRANSACTION_KEYWORDS.contains(&keyword.as_str()) {
            problems.push(format!(
                "{backend} {id}: file {name} has a {keyword} statement; the runner owns the \
                 transaction"
            ));
        }
    }
    if code.to_ascii_uppercase().contains("CATALOG_VERSION") {
        problems.push(format!(
            "{backend} {id}: file {name} names catalog_version; the runner writes the version"
        ));
    }
    if cql && comments.contains(';') {
        problems.push(format!(
            "{backend} {id}: file {name} has a ';' inside a comment, which splits a statement"
        ));
    }
}

/// The words of SQL or CQL with comments removed, upper-cased, and `;` as
/// its own token. A quoted string or identifier is one `''` token, so the
/// words inside it are not read as keywords.
#[cfg(any(test, feature = "test-util"))]
fn tokens(code: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = code.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' | '`' => {
                while let Some(d) = chars.next() {
                    if d == c {
                        if chars.peek() == Some(&c) {
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
                out.push("''".to_owned());
            }
            '[' => {
                for d in chars.by_ref() {
                    if d == ']' {
                        break;
                    }
                }
                out.push("''".to_owned());
            }
            // A PostgreSQL dollar-quoted string, `$tag$ ... $tag$` with an
            // empty or identifier tag, such as a `DO` block's body. `$1` is a
            // parameter, not a quote.
            '$' => {
                let mut probe = chars.clone();
                let mut tag = String::new();
                let mut opens = false;
                for d in probe.by_ref() {
                    if d == '$' {
                        opens = true;
                        break;
                    }
                    if !(d.is_ascii_alphanumeric() || d == '_')
                        || (tag.is_empty() && d.is_ascii_digit())
                    {
                        break;
                    }
                    tag.push(d);
                }
                if opens {
                    chars = probe;
                    let close = format!("${tag}$");
                    let mut body = String::new();
                    for d in chars.by_ref() {
                        body.push(d);
                        if body.ends_with(&close) {
                            break;
                        }
                    }
                    out.push("''".to_owned());
                }
            }
            ';' => out.push(";".to_owned()),
            c if c.is_ascii_alphanumeric() || c == '_' => {
                let mut word = c.to_ascii_uppercase().to_string();
                while let Some(&d) = chars.peek() {
                    if !(d.is_ascii_alphanumeric() || d == '_') {
                        break;
                    }
                    word.push(d.to_ascii_uppercase());
                    chars.next();
                }
                out.push(word);
            }
            _ => {}
        }
    }
    out
}

/// The first keyword of every statement. A `CREATE TRIGGER` statement runs
/// from `CREATE` to the `;` after the `END` that closes its `BEGIN` (a
/// `CASE` inside the body has its own `END`), so the statements of its body
/// are not counted.
#[cfg(any(test, feature = "test-util"))]
fn statement_keywords(code: &str) -> Vec<String> {
    let mut keywords = Vec::new();
    let mut statement: Vec<String> = Vec::new();
    let mut in_body = false;
    let mut case_depth = 0usize;
    for token in tokens(code) {
        if in_body {
            match token.as_str() {
                "CASE" => case_depth += 1,
                "END" if case_depth > 0 => case_depth -= 1,
                "END" => in_body = false,
                _ => {}
            }
            continue;
        }
        if token == ";" {
            if let Some(first) = statement.first() {
                keywords.push(first.clone());
            }
            statement.clear();
            continue;
        }
        if token == "BEGIN" && is_create_trigger(&statement) {
            in_body = true;
            case_depth = 0;
        }
        statement.push(token);
    }
    if let Some(first) = statement.first() {
        keywords.push(first.clone());
    }
    keywords
}

#[cfg(any(test, feature = "test-util"))]
fn is_create_trigger(words: &[String]) -> bool {
    let mut words = words.iter().map(String::as_str);
    words.next() == Some("CREATE")
        && match words.next() {
            Some("TEMP" | "TEMPORARY") => words.next() == Some("TRIGGER"),
            Some("TRIGGER") => true,
            _ => false,
        }
}

/// Split SQL or CQL into its text outside comments and the comments' text.
/// String literals (`'...'` with `''` escapes) and quoted identifiers are
/// kept whole in the first part.
#[must_use]
pub fn split_comments(text: &str, cql: bool) -> (String, String) {
    let mut code = String::with_capacity(text.len());
    let mut comments = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' => {
                code.push(c);
                while let Some(d) = chars.next() {
                    code.push(d);
                    if d == c {
                        if chars.peek() == Some(&c) {
                            code.push(c);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => line_comment(&mut chars, &mut comments, &mut code),
            '/' if cql && chars.peek() == Some(&'/') => {
                line_comment(&mut chars, &mut comments, &mut code);
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut last = ' ';
                for d in chars.by_ref() {
                    if last == '*' && d == '/' {
                        break;
                    }
                    comments.push(d);
                    last = d;
                }
                code.push(' ');
            }
            _ => code.push(c),
        }
    }
    (code, comments)
}

fn line_comment(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    comments: &mut String,
    code: &mut String,
) {
    for d in chars.by_ref() {
        if d == '\n' {
            code.push('\n');
            break;
        }
        comments.push(d);
    }
}
