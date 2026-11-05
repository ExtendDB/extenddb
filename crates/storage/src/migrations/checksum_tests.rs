// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checksum test helper on the fake table, with a non-zero released
//! bound.

use super::tests::{FakeCode, FakeStores, table};
use super::*;

fn pins(through: u32) -> Vec<Released> {
    let mut out = Vec::new();
    for id in MigrationId::ALL
        .into_iter()
        .filter(|id| id.number() <= through)
    {
        let m = table(id);
        let bodies: Vec<(StoreKind, String, Option<&'static str>)> = match &m {
            Migration::NotApplicable(r) => vec![(StoreKind::Catalog, file_checksum(r), None)],
            Migration::Applies { bodies, .. } => bodies
                .iter()
                .map(|b| match &b.body {
                    Body::File { text, .. } => (b.store, file_checksum(text), None),
                    Body::Code { source, body } => {
                        (b.store, file_checksum(source), Some(body.fingerprint()))
                    }
                })
                .collect(),
        };
        for (store, sum, fingerprint) in bodies {
            out.push(Released {
                id,
                store,
                checksum: Box::leak(sum.into_boxed_str()),
                fingerprint,
                kind: m.kind(),
                convergent: m.convergent(),
                rebless: None,
            });
        }
    }
    out
}

/// The fake table with one id replaced.
fn with(
    replaced: MigrationId,
    entry: impl Fn() -> Migration<FakeStores>,
) -> impl Fn(MigrationId) -> Migration<FakeStores> {
    move |id| if id == replaced { entry() } else { table(id) }
}

#[test]
fn the_checksum_helper_accepts_matching_pins_and_prints_the_lines() {
    let paste = check_released("fake", table, &pins(3), 3).unwrap();
    assert!(paste.contains("Released { id: MigrationId::Baseline, store: StoreKind::Data,"));
    assert!(paste.contains(
        "fingerprint: Some(\"0002_resource_tag_condition_keys/fake/1\"), kind: Kind::Breaking"
    ));
    let bodies: usize = MigrationId::ALL
        .into_iter()
        .map(|id| table(id).bodies().len().max(1))
        .sum();
    assert_eq!(paste.lines().count(), bodies);
    // Nothing released: nothing to compare.
    check_released("fake", table, &[], 0).unwrap();
}

#[test]
fn the_checksum_helper_refuses_a_changed_released_body_without_a_rebless_note() {
    let mut pinned = pins(3);
    pinned[0].checksum = "0000";
    let err = check_released("fake", table, &pinned, 3).unwrap_err();
    assert!(err.contains("changed after release"), "{err}");
    assert!(err.contains("Lines for the release PR"), "{err}");

    // A later entry with the current checksum and a reason is accepted.
    let current = pins(3)[0];
    let mut reblessed = pins(3);
    reblessed[0].checksum = "0000";
    reblessed.insert(
        1,
        Released {
            rebless: Some("trailing whitespace"),
            ..current
        },
    );
    check_released("fake", table, &reblessed, 3).unwrap();
    reblessed[1].rebless = None;
    let err = check_released("fake", table, &reblessed, 3).unwrap_err();
    assert!(err.contains("without a rebless reason"), "{err}");
}

static CODE_2_RENAMED: FakeCode = FakeCode("0002_resource_tag_condition_keys/fake/2");
static CODE_2_MISNAMED: FakeCode = FakeCode("0004_index_table_base_key/fake/1");

/// Deployed ledgers hold a code body's fingerprint, so a released body may
/// change its source with a rebless entry but never its fingerprint.
#[test]
fn the_checksum_helper_pins_code_body_fingerprints() {
    let changed = with(MigrationId::ResourceTagConditionKeys, || {
        Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &CODE_2_RENAMED,
                "source 2, edited",
            )],
        }
    });
    let mut pinned = pins(3);
    let mut reblessed = pinned[2];
    reblessed.checksum = Box::leak(file_checksum("source 2, edited").into_boxed_str());
    reblessed.rebless = Some("a driver upgrade");
    pinned.insert(3, reblessed);
    let err = check_released("fake", changed, &pinned, 3).unwrap_err();
    assert!(err.contains("released with fingerprint"), "{err}");
    assert!(err.contains("fake/2"), "{err}");

    // The same rebless with the fingerprint kept is accepted.
    let kept = with(MigrationId::ResourceTagConditionKeys, || {
        Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &super::tests::CODE_2,
                "source 2, edited",
            )],
        }
    });
    check_released("fake", kept, &pinned, 3).unwrap();

    // A fingerprint must name its own id.
    let misnamed = with(MigrationId::ResourceTagConditionKeys, || {
        Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &CODE_2_MISNAMED,
                "source 2",
            )],
        }
    });
    let err = check_released("fake", misnamed, &[], 0).unwrap_err();
    assert!(
        err.contains("should start with \"0002_resource_tag_condition_keys/\""),
        "{err}"
    );
}

#[test]
fn the_checksum_helper_pins_kind_and_convergent_and_the_bound() {
    let mut pinned = pins(3);
    pinned[1].kind = Kind::Additive;
    let err = check_released("fake", table, &pinned, 3).unwrap_err();
    assert!(err.contains("released as additive"), "{err}");

    let mut pinned = pins(3);
    pinned[3].convergent = false;
    assert!(check_released("fake", table, &pinned, 3).is_err());

    // Released bodies without pins, and pins above the bound.
    let err = check_released("fake", table, &pins(1), 3).unwrap_err();
    assert!(err.contains("has no pinned checksum"), "{err}");
    let err = check_released("fake", table, &pins(3), 1).unwrap_err();
    assert!(err.contains("above RELEASED_THROUGH"), "{err}");

    // A pin for a store the table does not declare.
    let mut pinned = pins(3);
    pinned.push(Released {
        store: StoreKind::Account,
        ..pinned[0]
    });
    let err = check_released("fake", table, &pinned, 3).unwrap_err();
    assert!(
        err.contains("for the account store, which the table does not declare"),
        "{err}"
    );

    // An empty NotApplicable reason.
    let empty = with(MigrationId::SortKeyDefinitions, || {
        Migration::NotApplicable("  ")
    });
    let err = check_released("fake", empty, &[], 0).unwrap_err();
    assert!(err.contains("a NotApplicable reason is empty"), "{err}");
}

fn file_problems(name: &'static str, text: &'static str) -> String {
    let one = with(MigrationId::Baseline, move || Migration::Applies {
        kind: Kind::Breaking,
        convergent: true,
        bodies: vec![StoreBody::file(StoreKind::Catalog, name, text)],
    });
    check_released("fake", one, &[], 0)
        .err()
        .unwrap_or_default()
}

#[test]
fn the_checksum_helper_checks_file_names_and_contents() {
    assert_eq!(
        file_problems("0001_baseline.sql", "CREATE TABLE a (b TEXT);"),
        ""
    );
    assert!(file_problems("0002_baseline.sql", "SELECT 1;").contains("does not start with"));
    assert!(file_problems("0001_other.sql", "SELECT 1;").contains("should be named"));
    assert!(
        file_problems(
            "0001_baseline.sql",
            "UPDATE settings SET value = '4' WHERE key = 'catalog_version';"
        )
        .contains("names catalog_version")
    );
    // Words and comments are not statements.
    assert_eq!(
        file_problems(
            "0001_baseline.sql",
            "-- BEGIN, COMMIT and catalog_version in a comment\nCREATE TABLE beginning (committed TEXT);"
        ),
        ""
    );
    assert!(
        file_problems("0001_baseline.cql", "-- one; two\nCREATE TABLE a (b text);")
            .contains("inside a comment")
    );
    assert!(
        file_problems("0001_baseline.cql", "// one; two\nCREATE TABLE a (b text);")
            .contains("inside a comment")
    );
    assert_eq!(
        file_problems(
            "0001_baseline.cql",
            "// BEGIN\nCREATE TABLE a (b text, c text);"
        ),
        ""
    );
}

/// Every statement that commits, ends or nests in the runner's transaction
/// is refused, wherever it stands, and a trigger body's statements are not
/// statements of the file.
#[test]
fn the_checksum_helper_refuses_transaction_statements_and_allows_triggers() {
    for (text, keyword) in [
        ("BEGIN;\nSELECT 1;", "BEGIN"),
        ("SELECT 1;\ncommit;", "COMMIT"),
        ("SELECT 1;\nEND;\nSELECT 2;", "END"),
        ("SELECT 1; ROLLBACK;", "ROLLBACK"),
        ("SAVEPOINT a; SELECT 1;", "SAVEPOINT"),
        ("SELECT 1; RELEASE a;", "RELEASE"),
        ("/* x */ BEGIN TRANSACTION;", "BEGIN"),
    ] {
        let problems = file_problems("0001_baseline.sql", text);
        assert!(
            problems.contains(&format!("has a {keyword} statement")),
            "{text}: {problems}"
        );
    }
    assert_eq!(
        file_problems(
            "0001_baseline.sql",
            "CREATE TRIGGER t AFTER INSERT ON a BEGIN \
             UPDATE a SET b = CASE WHEN new.b IS NULL THEN 0 ELSE new.b END; \
             SELECT RAISE(ROLLBACK, 'no'); END;\nCREATE TEMP TRIGGER u AFTER DELETE ON a BEGIN \
             DELETE FROM c; END;"
        ),
        ""
    );
    assert!(
        file_problems(
            "0001_baseline.sql",
            "CREATE TRIGGER t AFTER INSERT ON a BEGIN DELETE FROM c; END;\nCOMMIT;"
        )
        .contains("has a COMMIT statement")
    );
    // Inside a block comment or a string, nothing is a statement.
    assert_eq!(
        file_problems(
            "0001_baseline.sql",
            "/* BEGIN; COMMIT; catalog_version */ INSERT INTO a VALUES ('END; ROLLBACK;');"
        ),
        ""
    );
    // Nor inside a dollar-quoted body, with or without a tag; `$1` is a
    // parameter and quotes nothing.
    assert_eq!(
        file_problems(
            "0001_baseline.sql",
            "DO $$ BEGIN IF true THEN DELETE FROM a; END IF; END $$;\n\
             DO $body$ BEGIN PERFORM 1; END $body$;\nSELECT $1, 'x';"
        ),
        ""
    );
    assert!(
        file_problems("0001_baseline.sql", "DO $$ BEGIN END $$;\nBEGIN;")
            .contains("has a BEGIN statement")
    );
    assert!(
        file_problems("0001_baseline.sql", "SELECT $1;\nCOMMIT;")
            .contains("has a COMMIT statement")
    );
}

#[test]
fn file_checksums_ignore_line_endings() {
    assert_eq!(file_checksum("a\r\nb\n"), file_checksum("a\nb\n"));
    assert_ne!(file_checksum("a\nb\n"), file_checksum("a\nb"));
    assert_eq!(
        file_checksum(""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}
