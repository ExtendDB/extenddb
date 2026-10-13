// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Version literals in the documentation must match the compiled constants.
//!
//! The documentation carries sample output containing the binary version and the
//! catalog version. Those literals go stale silently: nothing fails, nothing warns,
//! and the next reader trusts them. This stage found five such blocks still showing
//! catalog `0.0.2` long after `0.0.3` shipped, and the binary version wrong in the
//! same lines, so the fix has to leave a check behind rather than five fresh
//! literals that expire at the next release.
//!
//! The same guard pattern already exists for the schema side: the migration runner
//! asserts the final migration writes the expected version, and the SQLite schema
//! asserts its seeded literal matches the constant. This is that pattern applied to
//! prose.
//!
//! Two escapes are deliberate. Files that document a past version, an upgrade
//! between versions, or another backend's version are exempt by path, each with a
//! reason. And a line may opt out with a trailing `<!-- version-literal-ok: why -->`
//! marker, so a genuinely historical example inside an otherwise live document does
//! not force the whole file onto the exemption list.

use std::path::{Path, PathBuf};

/// Documents whose version literals are historical rather than current.
///
/// Each entry states why, because an unexplained exemption is how a check becomes
/// decoration.
const EXEMPT: &[(&str, &str)] = &[
    (
        "docs/manuals/07-upgrade-manual.md",
        "documents the 0.0.2 to 0.0.3 upgrade, so both versions appear on purpose",
    ),
    (
        "docs/backlog.md",
        "records completed history, including the version current at the time",
    ),
    (
        "docs/design/13-storage-mongodb.md",
        "describes the MongoDB backend's own expected catalog version, which is 0.0.2",
    ),
    (
        "docs/design/01-requirements.md",
        "illustrative version numbers in a requirement example, not sample output",
    ),
];

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/storage-postgres.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/storage-postgres")
        .to_path_buf()
}

fn markdown_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            // `rendered` is build output, not source.
            if path.file_name().is_some_and(|n| n == "rendered") {
                continue;
            }
            markdown_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "md") {
            out.push(path);
        }
    }
}

/// How far past a keyword a version literal may sit and still be about it.
///
/// Wide enough for the real forms, `Catalog version 0.0.3` and
/// `catalog 0.0.3 (postgres)`, and narrow enough that an unrelated number later in a
/// sentence is not attributed to the keyword.
const LOOKAHEAD: usize = 24;

/// Pull version literals that belong to `keyword` out of one line.
///
/// Deliberately not a prefix match. The first version of this guard matched
/// `"catalog "` immediately followed by digits, which is one form the documentation
/// uses and not the one this stage had to fix: every stale literal it corrected read
/// `OK: Catalog version 0.0.3`, with a capital letter and a word in between, so the
/// check could not see the lines it existed for, and two files carried no other
/// literal at all, which made their coverage zero while the test passed. The lesson is
/// the one this suite applies elsewhere: ask whether the check would pass if the thing
/// it checks were broken.
///
/// So the match is case-insensitive and scans a short window after the keyword rather
/// than requiring adjacency.
fn versions_near(line: &str, keyword: &str) -> Vec<String> {
    let haystack = line.to_ascii_lowercase();
    let chars: Vec<char> = haystack.chars().collect();
    let key: Vec<char> = keyword.chars().collect();
    let mut found = Vec::new();

    for start in 0..chars.len() {
        if !chars[start..].starts_with(&key[..]) {
            continue;
        }
        // Whole word only. Without this, `crc-catalog | 2.4.0` in the dependency
        // table reads as a claim about the catalog version.
        let before_ok = start == 0 || !is_word_char(chars[start - 1]);
        let after = start + key.len();
        let after_ok = after >= chars.len() || !chars[after].is_ascii_alphanumeric();
        if !before_ok || !after_ok {
            continue;
        }

        // Find where a number starts, within the window, then read the WHOLE run of
        // digits and dots rather than the part of it that fits. Truncating the run is
        // how an IP address in `--bind-addr 10.0.1.5` became the version `10.0.1`.
        let limit = (after + LOOKAHEAD).min(chars.len());
        if let Some(digit_at) = (after..limit).find(|&i| chars[i].is_ascii_digit()) {
            let mut j = digit_at;
            while j < chars.len() && (chars[j].is_ascii_digit() || chars[j] == '.') {
                j += 1;
            }
            let run: String = chars[digit_at..j].iter().collect();
            if let Some(version) = exact_version(&run) {
                found.push(version);
            }
        }
    }
    found
}

/// True for characters that make a keyword part of a longer word.
fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

/// `text` as a version, or `None`. The whole run must be `X.Y.Z`.
///
/// Requiring the entire run rather than a prefix of it is what rejects an IP address:
/// `10.0.1.5` has four components and is not a version, where its first three
/// characters-worth would have passed.
fn exact_version(text: &str) -> Option<String> {
    let parts: Vec<&str> = text.split('.').collect();
    if parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
    {
        Some(text.to_owned())
    } else {
        None
    }
}

/// Which literal a finding is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keyword {
    Catalog,
    Binary,
}

/// Every stale literal in one document, as `(line number, which, value)`.
///
/// Extracted from the corpus test so the rules below can be fed the forms they claim
/// to cover. The reason is a finding against this file: it had been "verified failing
/// first" by mutating a sample of the one form the matcher could already see, which
/// could not reveal that four of the five documents it existed for had zero coverage.
/// **An instrument is only proven on the cases you feed it**, so the cases are
/// enumerated in `the_matcher_sees_every_form_it_claims_to_cover` and
/// `a_marker_above_a_fence_covers_the_block` rather than left to a corpus that happens
/// to contain them today.
fn stale_literals(
    text: &str,
    expected_catalog: &str,
    expected_binary: &str,
) -> Vec<(usize, Keyword, String)> {
    // A marker inside a fenced block would render as sample output rather than as a
    // comment, and one did: it shipped in a manual as part of an error message an
    // operator would try to match. So a marker on the line immediately before a fence
    // covers that whole block instead, and the fence state has to be tracked to know
    // where the block ends. Fences are not skipped in general, because most of the
    // literals worth checking are inside them.
    let mut out = Vec::new();
    let mut in_marked_fence = false;
    let mut marker_pending = false;
    for (index, line) in text.lines().enumerate() {
        let number = index + 1;
        if line.trim_start().starts_with("```") {
            if in_marked_fence {
                in_marked_fence = false;
            } else if marker_pending {
                in_marked_fence = true;
            }
            marker_pending = false;
            continue;
        }
        if line.contains("version-literal-ok:") {
            // A marker alone on its line covers the block that follows. One at the end
            // of a sentence covers that sentence only: otherwise inserting a code block
            // after such a sentence would silence it, with no marker visible above the
            // block and nothing to fail.
            marker_pending = line.trim_start().starts_with("<!--");
            continue;
        }
        marker_pending = false;
        if in_marked_fence {
            continue;
        }
        for found in versions_near(line, "catalog") {
            if found != expected_catalog {
                out.push((number, Keyword::Catalog, found));
            }
        }
        for found in versions_near(line, "extenddb") {
            if found != expected_binary {
                out.push((number, Keyword::Binary, found));
            }
        }
    }
    out
}

/// Every form the matcher claims to cover, and every hazard it claims to ignore.
///
/// This test exists because "watched it fail" was true of this guard and still left it
/// blind to four of five documents: the failing case fed to it was the one form it
/// could already see. Each line below is a real line from this repository.
#[test]
fn the_matcher_sees_every_form_it_claims_to_cover() {
    // Must be seen: a stale value on each of these lines has to be reported.
    let seen = [
        "  OK: Catalog version 0.0.2",
        "# extenddb 0.1.5 (catalog 0.0.2) starting on 127.0.0.1:18443",
        "# catalog 0.0.2 (postgres)",
        "The catalog version is 0.0.2 on PostgreSQL and SQLite, stored in the `settings` table",
    ];
    for line in seen {
        assert!(
            !stale_literals(line, "0.0.3", "0.1.6").is_empty(),
            "the matcher must see a stale literal in this form: {line}"
        );
    }

    // Must be ignored: none of these is a claim about a version of ours.
    let ignored = [
        "| crc-catalog | 2.4.0 | MIT OR Apache-2.0 | Yes |",
        "./target/release/extenddb init --bind-addr 10.0.1.5",
        "connection_string = \"postgresql://extenddb:***@localhost:5432/extenddb_catalog\"",
        "under the key `catalog_version` and checked at startup.",
    ];
    for line in ignored {
        assert_eq!(
            stale_literals(line, "0.0.3", "0.1.6"),
            vec![],
            "the matcher must ignore this line: {line}"
        );
    }
}

/// The fence rules, all three directions, with cases the matcher would otherwise
/// report.
///
/// The first version of this test fed the marked block the real admin-guide line,
/// `found 1.0.0, expected 0.0.3`, which the matcher ignores anyway because its digit
/// falls one character outside the window. So the assertion held whether the fence
/// rule worked or not: I proved the rule with a case that could not exercise it, which
/// is the same defect as the guard it was written to protect. Every literal below is
/// one the matcher does see, so breaking the fence rule breaks these assertions.
#[test]
fn a_marker_above_a_fence_covers_the_block() {
    let marked = "<!-- version-literal-ok: an example -->\n```\ncatalog 0.0.2\n```\n";
    assert_eq!(
        stale_literals(marked, "0.0.3", "0.1.6"),
        vec![],
        "a marker on the line before a fence must cover the whole block"
    );

    let unmarked = "```\ncatalog 0.0.2\n```\n";
    assert_eq!(
        stale_literals(unmarked, "0.0.3", "0.1.6"),
        vec![(2, Keyword::Catalog, "0.0.2".to_owned())],
        "an unmarked fence must still be checked, or the escape becomes a blanket"
    );

    // The marker must stop at the closing fence. Asserting the exact finding rather
    // than "not empty" is what makes this fail if the exemption leaks past the block.
    let leaky = "<!-- version-literal-ok: covers the block below -->\n```\ncatalog 0.0.2\n```\ncatalog 0.0.1\n";
    assert_eq!(
        stale_literals(leaky, "0.0.3", "0.1.6"),
        vec![(5, Keyword::Catalog, "0.0.1".to_owned())],
        "exactly the line after the fence is reported: the block is covered, the rest is not"
    );

    // An INLINE marker exempts its own line and must not arm the block scope. The
    // in-tree inline marker is followed by prose today, so nothing was exempted, but a
    // code block inserted after that sentence would have been silenced with no marker
    // visible above it and nothing to fail.
    let inline =
        "Some prose. <!-- version-literal-ok: about this sentence -->\n```\ncatalog 0.0.2\n```\n";
    assert_eq!(
        stale_literals(inline, "0.0.3", "0.1.6"),
        vec![(3, Keyword::Catalog, "0.0.2".to_owned())],
        "a marker at the end of a sentence covers that sentence, not the next block"
    );
}

#[test]
fn documentation_version_literals_match_the_compiled_constants() {
    let expected_catalog = extenddb_storage_postgres::CATALOG_VERSION.to_string();
    let expected_binary = env!("CARGO_PKG_VERSION"); // inherited from the workspace
    let root = repo_root();

    let mut files = Vec::new();
    markdown_files(&root.join("docs"), &mut files);
    let readme = root.join("README.md");
    if readme.exists() {
        files.push(readme);
    }
    assert!(
        files.len() > 10,
        "expected to find the documentation set, found {} files under {}",
        files.len(),
        root.display()
    );

    let mut stale = Vec::new();
    for file in files {
        let rel = file
            .strip_prefix(&root)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        if EXEMPT.iter().any(|(path, _)| *path == rel) {
            continue;
        }
        let text = std::fs::read_to_string(&file).expect("read a documentation file");
        for (number, kind, found) in stale_literals(&text, &expected_catalog, expected_binary) {
            let (label, expected) = match kind {
                Keyword::Catalog => ("catalog", expected_catalog.as_str()),
                Keyword::Binary => ("extenddb", expected_binary),
            };
            stale.push(format!(
                "{rel}:{number}: {label} {found}, compiled constant is {expected}"
            ));
        }
    }

    assert!(
        stale.is_empty(),
        "documentation version literals are stale. Update them, add a \
         `<!-- version-literal-ok: reason -->` marker to a deliberately historical \
         line, or exempt the file with a reason in EXEMPT:\n  {}",
        stale.join("\n  ")
    );
}
