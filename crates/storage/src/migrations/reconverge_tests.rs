// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `forget`, `reconverge` and reblessed released bodies on the fake backend.

use super::tests::{CATALOG_FILE, CODE_2, Fake, LATEST, current, runner, steps_after};
use super::*;

#[tokio::test]
async fn forget_refuses_released_ids() {
    let fake = Fake::new();
    current(&fake).await;
    let all: Vec<u32> = (1..=LATEST).collect();
    let err = runner(&fake)
        .with_released_through(2)
        .forget(2)
        .await
        .unwrap_err();
    assert!(matches!(err, MigrationError::Refused(_)), "{err:?}");
    assert_eq!(fake.rows("cat"), all);
    runner(&fake)
        .with_released_through(2)
        .forget(3)
        .await
        .unwrap();
    let without_3: Vec<u32> = all.iter().copied().filter(|id| *id != 3).collect();
    assert_eq!(fake.rows("cat"), without_3);
    assert_eq!(fake.version_row().as_deref(), Some("2"));
    runner(&fake).migrate().await.unwrap();
    assert_eq!(fake.rows("cat"), all);
}

#[tokio::test]
async fn forget_refuses_an_id_it_cannot_forget() {
    let fake = Fake::new();
    runner(&fake)
        .migrate_through(MigrationId::ResourceTagConditionKeys)
        .await
        .unwrap();
    let before = fake.rows("cat");
    // An id this binary does not carry.
    let err = runner(&fake).forget(LATEST + 3).await.unwrap_err();
    assert_eq!(err.exit_code(), exit::ERROR);
    assert!(err.to_string().contains("carries no migration"), "{err}");
    // An id no store has a row for.
    let err = runner(&fake).forget(LATEST).await.unwrap_err();
    assert_eq!(err.exit_code(), exit::ERROR);
    assert!(err.to_string().contains("nothing to forget"), "{err}");
    assert_eq!(fake.rows("cat"), before);
}

/// No migration may be declared additive before CI runs the previous
/// release against a catalog this binary migrated.
#[test]
fn an_additive_declaration_is_refused() {
    let table = |additive: MigrationId| {
        move |id: MigrationId| -> Migration<()> {
            Migration::Applies {
                kind: if id == additive {
                    Kind::Additive
                } else {
                    Kind::Breaking
                },
                convergent: true,
                bodies: Vec::new(),
            }
        }
    };
    let err = refuse_additive("fake", table(MigrationId::LATEST)).unwrap_err();
    assert!(
        err.contains(&format!(
            "migration {} is declared Kind::Additive on fake",
            MigrationId::LATEST
        )),
        "{err}"
    );
    assert!(err.contains("step 4"), "{err}");
    // A migration that does not apply has no kind to declare.
    assert_eq!(
        refuse_additive("fake", |_| Migration::<()>::NotApplicable("no store")),
        Ok(())
    );
}

/// After its row for migration 1 is forgotten the catalog has a ledger at
/// migration 0, which is not a catalog without a ledger.
#[tokio::test]
async fn a_ledger_without_migration_1_is_at_migration_0() {
    let fake = Fake::new();
    current(&fake).await;
    runner(&fake)
        .with_released_through(0)
        .forget(1)
        .await
        .unwrap();
    let CatalogStatus::Behind { version, .. } = runner(&fake).status().await.unwrap() else {
        panic!("expected Behind");
    };
    assert_eq!(version, Some(0));
    assert_eq!(fake.version_row().as_deref(), Some("0"));
    let report = runner(&fake).migrate().await.unwrap();
    assert_eq!((report.from, report.to), (Some(0), Some(LATEST)));
}

#[tokio::test]
async fn reconverge_refuses_a_non_convergent_id_and_repairs_otherwise() {
    let fake = Fake::new();
    runner(&fake)
        .migrate_through(MigrationId::SortKeyDefinitions)
        .await
        .unwrap();
    fake.with_state(|s| {
        s.traces.catalog_version = Some("0.0.3".into());
        s.traces.resource_tag_marker = true;
    });
    let report = runner(&fake).reconverge().await.unwrap();
    assert_eq!(
        report.applied.len(),
        steps_after(0),
        "every id ran again: {report:?}"
    );
    assert_eq!(
        runner(&fake).status().await.unwrap(),
        CatalogStatus::Current { version: LATEST }
    );
    // Id 4 is not convergent on the fake, and is now in the ledger.
    let err = runner(&fake).reconverge().await.unwrap_err();
    assert!(matches!(err, MigrationError::Refused(_)), "{err:?}");
}

/// A reconverge reruns the bodies and writes the version row again, and
/// leaves every ledger row as it was, so the first application's notes and
/// `applied_at` survive. The bodies' new notes are in the report.
#[tokio::test]
async fn reconverge_leaves_the_ledger_rows_as_they_are() {
    let fake = Fake::new();
    runner(&fake)
        .migrate_through(MigrationId::SortKeyDefinitions)
        .await
        .unwrap();
    fake.with_state(|s| {
        for rows in s.ledgers.values_mut() {
            for row in rows {
                row.applied_at = "2026-01-01T00:00:00.000Z".into();
                row.applied_by = "1.0.0".into();
            }
        }
        s.traces.catalog_version = Some("0.0.3".into());
        s.traces.resource_tag_marker = true;
    });
    let before: Vec<Vec<LedgerRow>> = ["cat", "data-a", "data-b"]
        .iter()
        .map(|s| fake.ledger(s))
        .collect();
    let report = runner(&fake).reconverge().await.unwrap();
    for (store, rows) in ["cat", "data-a", "data-b"].iter().zip(&before) {
        let after = fake.ledger(store);
        for row in rows {
            assert!(after.contains(row), "{store}: {row:?} changed: {after:?}");
        }
    }
    let two = report.applied.iter().find(|s| s.id == 2).expect("id 2 ran");
    assert_eq!(two.notes, [format!("{} ran", CODE_2.0)]);
    assert_eq!(fake.version_row(), Some(LATEST.to_string()));
    assert_eq!(
        runner(&fake).status().await.unwrap(),
        CatalogStatus::Current { version: LATEST }
    );
}

#[tokio::test]
async fn reconverge_refuses_an_id_this_binary_does_not_carry() {
    let fake = Fake::new();
    runner(&fake)
        .migrate_through(MigrationId::SortKeyDefinitions)
        .await
        .unwrap();
    fake.add_catalog_row(LATEST + 1, Kind::Additive);
    fake.with_state(|s| s.traces.resource_tag_marker = true);
    let err = runner(&fake).reconverge().await.unwrap_err();
    let MigrationError::Refused(message) = &err else {
        panic!("{err:?}");
    };
    assert!(message.contains("does not carry"), "{message}");
}

/// A released file body changed later with a rebless entry: a new catalog
/// records the first released checksum, so every release reads it, and the
/// runtime accepts the released checksum and the current one, and nothing
/// else.
#[tokio::test]
async fn a_reblessed_file_body_records_its_first_release_and_accepts_both() {
    let current_checksum = file_checksum(CATALOG_FILE);
    let released: &'static [Released] = Box::leak(Box::new([
        Released {
            id: MigrationId::Baseline,
            store: StoreKind::Catalog,
            checksum: "released-1",
            fingerprint: None,
            kind: Kind::Breaking,
            convergent: true,
            rebless: None,
        },
        Released {
            id: MigrationId::Baseline,
            store: StoreKind::Catalog,
            checksum: Box::leak(current_checksum.clone().into_boxed_str()),
            fingerprint: None,
            kind: Kind::Breaking,
            convergent: true,
            rebless: Some("a comment reworded"),
        },
    ]));
    let fake = Fake::released(released);
    current(&fake).await;
    let recorded = |fake: &Fake| fake.ledger("cat")[0].checksum.clone();
    assert_eq!(recorded(&fake), "released-1");
    assert_eq!(
        runner(&fake).status().await.unwrap(),
        CatalogStatus::Current { version: LATEST }
    );
    let set = |checksum: &str| {
        fake.with_state(|s| s.ledgers.get_mut("cat").unwrap()[0].checksum = checksum.into());
    };
    set(&current_checksum);
    assert_eq!(
        runner(&fake).status().await.unwrap(),
        CatalogStatus::Current { version: LATEST }
    );
    set("another");
    assert_eq!(runner(&fake).status().await.unwrap().label(), "Changed");
}
