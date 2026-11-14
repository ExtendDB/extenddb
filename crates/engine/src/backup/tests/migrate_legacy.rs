// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `backup migrate-legacy` (`super::super::migrate_legacy`) against the
//! `Fake`'s legacy reader and a `MemStore`, the `backup sync` skip of a
//! prefix an in-catalog backup holds, and ListBackups over a backup both
//! readers list.

use extenddb_core::types::AttributeValue;
use extenddb_core::validation::RestorableLimits;
use extenddb_storage::backup_definition::LegacyVectorState;

use super::super::migrate_legacy::{
    BackupOutcome, MigrateReport, Migration, Outcome, Selection, migrate,
};
use super::*;

/// The in-catalog backups of these tests: 14-digit ids, one per `n`, of
/// the `Music` table of the tests' account and region.
fn legacy_arn(n: u32) -> String {
    format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/0179197643{n:04}-1111{n:04}")
}

/// An item of the `source` fixture's key schema (`pk`, `sk`).
fn legacy_item(i: usize) -> Item {
    Item::from([
        ("pk".to_owned(), AttributeValue::S(format!("p{i:04}"))),
        ("sk".to_owned(), AttributeValue::S("s".to_owned())),
        ("n".to_owned(), AttributeValue::N(i.to_string())),
    ])
}

fn legacy_items(n: usize) -> Vec<Item> {
    (0..n).map(legacy_item).collect()
}

/// An AVAILABLE in-catalog backup of `arn` holding `items`, its count
/// recorded as theirs, with a pay-per-request definition (a GSI and an
/// LSI), its name `nightly`, listed by `list_all`.
fn seed_legacy(fake: &Fake, arn: &str, items: Vec<Item>) {
    let mut source = source(Some(definition("PAY_PER_REQUEST")));
    source.backup_arn = arn.to_owned();
    source.item_count = i64::try_from(items.len()).unwrap();
    // An id of the 13-digit shape (a 0.1.x backup's) keeps the fixture's
    // time.
    source.backup_creation_date_time =
        extenddb_storage::util::backup_creation_date_time(arn).unwrap_or(CREATED);
    fake.sources.lock().unwrap().insert(arn.to_owned(), source);
    fake.legacy_items.lock().unwrap().insert(
        arn.to_owned(),
        FakeLegacyItems {
            backup_name: "nightly".to_owned(),
            vector_state: LegacyVectorState::NoneRecorded,
            items,
        },
    );
    fake.legacy_keys.lock().unwrap().push(LegacyBackupKey {
        account_id: ACCOUNT.to_owned(),
        backup_arn: arn.to_owned(),
        status: BackupStatus::Available,
    });
}

/// The in-catalog backup of `arn` is still there: its source and its
/// `list_all` entry.
fn legacy_kept(fake: &Fake, arn: &str) -> bool {
    fake.sources.lock().unwrap().contains_key(arn)
        && fake
            .legacy_keys
            .lock()
            .unwrap()
            .iter()
            .any(|k| k.backup_arn == arn && k.status != BackupStatus::Deleted)
}

/// The objects under `arn`'s prefix.
fn copy_of(mem: &MemStore, arn: &str) -> Vec<String> {
    objects_under(mem, arn)
}

/// A healthy runtime on `mem`, and the claim and signal of a command.
struct Command {
    runtime: BackupRuntime,
    holder: Arc<FakeHolder>,
    claim: maintenance::HeldClaim,
    signal: tokio::sync::watch::Sender<Option<&'static str>>,
    interrupt: maintenance::Interrupt,
}

fn new_command(mem: &Arc<MemStore>) -> Command {
    let holder = Arc::new(FakeHolder::default());
    let claim = held(&holder);
    let (signal, interrupt) = maintenance::Interrupt::new();
    Command {
        runtime: runtime_on(mem, store()),
        holder,
        claim,
        signal,
        interrupt,
    }
}

/// Run the migration of `selection` as the command would, collecting each
/// backup's outcome as it is reported.
async fn run(
    fake: &Fake,
    command: &Command,
    selection: Selection<'_>,
) -> (MigrateReport, Vec<BackupOutcome>) {
    run_with(fake, command, selection, 64 * 1024 * 1024).await
}

/// [`run`] with another `data_file_target_bytes`.
async fn run_with(
    fake: &Fake,
    command: &Command,
    selection: Selection<'_>,
    data_file_target_bytes: u64,
) -> (MigrateReport, Vec<BackupOutcome>) {
    run_on(fake, command, selection, data_file_target_bytes, "sqlite").await
}

/// [`run_with`] as a command of the `source_backend` build.
async fn run_on(
    fake: &Fake,
    command: &Command,
    selection: Selection<'_>,
    data_file_target_bytes: u64,
    source_backend: &str,
) -> (MigrateReport, Vec<BackupOutcome>) {
    let identity = store();
    let migration = Migration {
        configured: command.runtime.configured_store().unwrap(),
        identity: &identity,
        claim: &command.claim,
        interrupt: &command.interrupt,
        limits: RestorableLimits::from(&LimitsConfig::default()),
        data_file_target_bytes,
        region: REGION,
        source_backend,
        source_extenddb_version: "9.9.9",
        metrics: None,
    };
    let mut seen = Vec::new();
    let report = migrate(fake, &migration, selection, |b| seen.push(b.clone())).await;
    (report, seen)
}

fn outcome_of<'a>(report: &'a MigrateReport, arn: &str) -> &'a Outcome {
    &report
        .backups
        .iter()
        .find(|b| b.backup_arn == arn)
        .unwrap_or_else(|| panic!("no outcome for {arn}: {report:?}"))
        .outcome
}

fn refusal(outcome: &Outcome) -> &str {
    match outcome {
        Outcome::Refused(why) => why,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// The items of every data file of `arn`'s copy, decoded and verified
/// against the manifest's entries.
fn copied_items(mem: &MemStore, arn: &str) -> Vec<Item> {
    let manifest = stored_manifest(mem, arn);
    let prefix = prefix_of(arn);
    let mut items = Vec::new();
    for file in &manifest.data_files {
        let bytes = mem.objects.lock().unwrap()[&format!("{prefix}{}", file.key)].clone();
        let mut decoder = file.decoder(&bytes[..]).unwrap();
        while let Some(item) = decoder.next_item().unwrap() {
            items.push(item);
        }
    }
    items
}

/// An AVAILABLE in-catalog backup is copied under its own ARN: the
/// manifest says it is a copy of an in-catalog backup (no marker,
/// unordered), names this binary as its writer, keeps the backup's name and
/// creation instant and records the table the legacy restore creates; its
/// data files hold every item; the prefix reads back as `backup sync`
/// reads one. The replacement then gets the row `backup sync` would write
/// (AVAILABLE, unowned, the configured store, the manifest's size, count
/// and `source_table`, the id's instant), and the in-catalog backup is
/// gone. The migrated backup is described from its row, and a second run
/// finds nothing to do.
#[tokio::test]
async fn an_in_catalog_backup_is_copied_under_its_arn_and_replaced_by_its_row() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(5));
    let command = new_command(&mem);
    let (report, seen) = run(&fake, &command, Selection::All).await;
    assert!(report.complete(), "{report:?}");
    assert_eq!(seen, report.backups);
    let size = match outcome_of(&report, &arn) {
        Outcome::Migrated {
            item_count,
            size_bytes,
        } => {
            assert_eq!(*item_count, 5);
            *size_bytes
        }
        other => panic!("{other:?}"),
    };

    let manifest = stored_manifest(&mem, &arn);
    assert_eq!(
        manifest.snapshot.kind,
        extenddb_core::backup_format::SnapshotDescriptor::KIND_LEGACY_IN_CATALOG
    );
    assert_eq!(manifest.snapshot.marker, "");
    assert_eq!(
        manifest.item_order,
        extenddb_core::backup_format::ItemOrder::Unordered
    );
    assert_eq!(manifest.source_backend, "sqlite");
    assert_eq!(manifest.source_extenddb_version, "9.9.9");
    assert_eq!(manifest.backup_name, "nightly");
    assert_eq!(manifest.backup_arn().unwrap().to_arn_string(), arn);
    let instant = extenddb_storage::util::backup_creation_date_time(&arn).unwrap();
    assert!((manifest.backup_creation_date_time - instant).abs() < 1e-9);
    let source = source(Some(definition("PAY_PER_REQUEST")));
    let expected = extenddb_storage::backup_definition::source_table_from_legacy(
        &BackupRestoreSource {
            item_count: 5,
            ..source
        },
        LegacyVectorState::NoneRecorded,
        REGION,
        ACCOUNT,
    )
    .unwrap();
    assert_eq!(manifest.source_table.key_schema, expected.key_schema);
    assert_eq!(
        manifest.source_table.global_secondary_indexes,
        expected.global_secondary_indexes
    );
    assert_eq!(manifest.source_table.vector_indexes, Some(Vec::new()));
    assert_eq!(manifest.source_table.item_count, 5);
    let mut items = copied_items(&mem, &arn);
    items.sort_by_key(|item| format!("{item:?}"));
    let mut expected_items = legacy_items(5);
    expected_items.sort_by_key(|item| format!("{item:?}"));
    assert_eq!(items, expected_items);

    let replaced = fake.replaces_seen.lock().unwrap().clone();
    assert_eq!(replaced.len(), 1);
    let row = &replaced[0];
    assert_eq!(row.backup_arn, arn);
    assert_eq!(row.status, BackupStatus::Available);
    assert_eq!(row.owner_generation, UNOWNED_GENERATION);
    assert_eq!(row.store, store());
    assert_eq!(row.item_count, 5);
    assert_eq!(row.size_bytes, size);
    assert_eq!(row.size_bytes, manifest.backup_size_bytes);
    assert_eq!(row.backup_name, "nightly");
    assert!((row.created_at - instant).abs() < 1e-9);
    assert_eq!(
        row.source_table_details_json,
        serde_json::to_value(&manifest.source_table).unwrap()
    );
    assert!(!legacy_kept(&fake, &arn));
    assert!(fake.rows.lock().unwrap().contains_key(&arn));

    let description = describe_backup(&fake, ACCOUNT, REGION, &own(&arn))
        .await
        .unwrap();
    assert_eq!(description.backup_details.backup_status, "AVAILABLE");
    assert_eq!(description.backup_details.backup_name, "nightly");

    let (again, _) = run(&fake, &command, Selection::All).await;
    assert!(again.backups.is_empty() && again.complete(), "{again:?}");
}

/// What a refusal case changes in the seeded backup.
type Prepare = Box<dyn Fn(&Fake, &str)>;

/// Every refusal that needs no copy keeps the in-catalog backup and writes
/// nothing to the store: an id of the 13-digit shape a 0.1.x release
/// issued (13 digits with a suffix, and bare), a table name the store-prefix rules refuse, a name the
/// manifest refuses, a definition whose vector state is unknown (a SQLite
/// backup from before catalog 0.0.4), vector indexes (the legacy restore's
/// own refusal), a backend that cannot read the backup (MongoDB's without
/// an item collection), and a row of another store under the ARN. The id
/// and table-name refusals come before the backup is opened.
#[tokio::test]
async fn each_refusal_before_the_copy_keeps_the_in_catalog_backup_and_writes_nothing() {
    let thirteen =
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/1793254932797-4116998c");
    let bare = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/1793254932797");
    let dots =
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/.../backup/01791976431001-11110001");
    let cases: Vec<(String, Prepare, &str, bool)> = vec![
        (
            thirteen,
            Box::new(|_, _| {}),
            "has the 13-digit shape a 0.1.x release issued",
            false,
        ),
        (
            bare,
            Box::new(|_, _| {}),
            "has the 13-digit shape a 0.1.x release issued",
            false,
        ),
        (
            dots,
            Box::new(|_, _| {}),
            "cannot name a store prefix",
            false,
        ),
        (
            legacy_arn(2),
            Box::new(|fake, arn| {
                fake.legacy_items
                    .lock()
                    .unwrap()
                    .get_mut(arn)
                    .unwrap()
                    .backup_name = "x".to_owned();
            }),
            "is not a backup name",
            true,
        ),
        (
            legacy_arn(3),
            Box::new(|fake, arn| {
                fake.legacy_items
                    .lock()
                    .unwrap()
                    .get_mut(arn)
                    .unwrap()
                    .vector_state = LegacyVectorState::Unknown;
            }),
            "no recorded table definition",
            true,
        ),
        (
            legacy_arn(4),
            Box::new(|fake, arn| {
                fake.sources
                    .lock()
                    .unwrap()
                    .get_mut(arn)
                    .unwrap()
                    .definition
                    .as_mut()
                    .unwrap()
                    .vector_index_names = vec!["v".to_owned()];
            }),
            "the legacy restore refuses it too",
            true,
        ),
        (
            legacy_arn(5),
            Box::new(|fake, _| {
                *fake.items_error.lock().unwrap() = Some(StorageError::Unsupported(
                    "the backup has no item collection to read".to_owned(),
                ));
            }),
            "no item collection",
            true,
        ),
        (
            legacy_arn(6),
            Box::new(|fake, arn| {
                let mut other = row(arn, BackupStatus::Available);
                other.store.store_root = "/srv/elsewhere".to_owned();
                fake.rows.lock().unwrap().insert(arn.to_owned(), other);
            }),
            "another store",
            false,
        ),
    ];
    for (arn, prepare, says, opened) in cases {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        seed_legacy(&fake, &arn, legacy_items(3));
        prepare(&fake, &arn);
        let command = new_command(&mem);
        let (report, _) = run(&fake, &command, Selection::All).await;
        let why = refusal(outcome_of(&report, &arn));
        assert!(why.contains(says), "{arn}: {why}");
        assert!(report.stopped.is_none() && !report.complete(), "{report:?}");
        assert!(legacy_kept(&fake, &arn), "{arn}");
        assert!(
            mem.puts.lock().unwrap().is_empty(),
            "{arn}: nothing written"
        );
        assert!(fake.replaces_seen.lock().unwrap().is_empty(), "{arn}");
        assert_eq!(
            !fake.items_opened.lock().unwrap().is_empty(),
            opened,
            "{arn}: opened"
        );
    }
}

/// An item the validator refuses (#618's number) fails the copy: the
/// prefix written so far is deleted, the in-catalog backup kept, and the
/// reason names the item and the way out. `--all` goes on: the next
/// backup is migrated, and the run is incomplete.
#[tokio::test]
async fn a_refused_item_deletes_the_copy_and_all_goes_on_to_the_next_backup() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let bad = legacy_arn(1);
    let good = legacy_arn(2);
    let mut items = legacy_items(3);
    items[1].insert("n".to_owned(), AttributeValue::N("1E126".to_owned()));
    seed_legacy(&fake, &bad, items);
    seed_legacy(&fake, &good, legacy_items(2));
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    let why = refusal(outcome_of(&report, &bad));
    assert!(why.contains("1 item(s) cannot be restored"), "{why}");
    assert!(
        why.contains("(#618)") && why.contains("correct the items"),
        "{why}"
    );
    assert!(copy_of(&mem, &bad).is_empty(), "{:?}", copy_of(&mem, &bad));
    assert!(legacy_kept(&fake, &bad));
    assert!(matches!(
        outcome_of(&report, &good),
        Outcome::Migrated { item_count: 2, .. }
    ));
    assert!(!report.complete() && report.stopped.is_none());
}

/// The count check: a read that gives fewer items than the in-catalog
/// backup records (a DeleteBackup landing during the read) refuses the
/// backup and deletes its copy; a read that fails part way fails it the
/// same way. The in-catalog backup is kept either way.
#[tokio::test]
async fn a_read_short_of_the_recorded_count_or_failing_deletes_the_copy() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let short = legacy_arn(1);
    seed_legacy(&fake, &short, legacy_items(3));
    fake.sources
        .lock()
        .unwrap()
        .get_mut(&short)
        .unwrap()
        .item_count = 4;
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    let why = refusal(outcome_of(&report, &short));
    assert!(why.contains("records 4 items and its read gave 3"), "{why}");
    assert!(copy_of(&mem, &short).is_empty());
    assert!(
        !mem.puts.lock().unwrap().is_empty(),
        "a data file was written first"
    );
    assert!(legacy_kept(&fake, &short));
    assert!(fake.replaces_seen.lock().unwrap().is_empty());

    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let failing = legacy_arn(2);
    seed_legacy(&fake, &failing, legacy_items(3));
    *fake.items_fail_after.lock().unwrap() = Some((
        2,
        StorageError::Internal("the cursor's collection was dropped".to_owned()),
    ));
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    match outcome_of(&report, &failing) {
        Outcome::Failed(why) => assert!(why.contains("collection was dropped"), "{why}"),
        other => panic!("{other:?}"),
    }
    assert!(copy_of(&mem, &failing).is_empty());
    assert!(legacy_kept(&fake, &failing));
}

/// The deletion fence before the first put: a root another catalog
/// adopted since the store's check gets nothing from this catalog. The run
/// stops (a store this catalog no longer owns is no place for the next
/// backup either), the store is marked unhealthy, and the in-catalog
/// backups are kept.
#[tokio::test]
async fn migrate_legacy_on_a_root_another_catalog_adopted_writes_nothing() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let first = legacy_arn(1);
    seed_legacy(&fake, &first, legacy_items(3));
    seed_legacy(&fake, &legacy_arn(2), legacy_items(3));
    let command = new_command(&mem);
    mem.insert(STORE_IDENTITY_KEY, &adopted_file());
    let (report, _) = run(&fake, &command, Selection::All).await;
    let stopped = report.stopped.clone().expect("the run stopped");
    assert!(
        stopped.contains("deletion fence refused the first write"),
        "{stopped}"
    );
    assert!(mem.puts.lock().unwrap().is_empty());
    assert!(legacy_kept(&fake, &first) && legacy_kept(&fake, &legacy_arn(2)));
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
    assert_fenced(&command.runtime);
}

/// The re-read: a manifest that does not read back as written (its bytes
/// changed after the put, so its sidecar no longer matches) fails the
/// backup before any row is written; the copy is deleted and the in-catalog
/// backup kept.
#[tokio::test]
async fn a_copy_that_does_not_read_back_is_deleted_before_any_row() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(3));
    let weak = Arc::downgrade(&mem);
    *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
        if key.ends_with("extenddb-manifest.json")
            && let Some(mem) = weak.upgrade()
        {
            mem.insert(key, b"{}");
        }
    }));
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    match outcome_of(&report, &arn) {
        Outcome::Failed(why) => {
            assert!(
                why.contains("does not read back") && why.contains("sidecar"),
                "{why}"
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(copy_of(&mem, &arn).is_empty(), "{:?}", copy_of(&mem, &arn));
    assert!(legacy_kept(&fake, &arn));
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
}

/// A copy an earlier run left (it stopped after some puts and before the
/// replacement; no row names the prefix) is deleted through the fence and
/// the backup is copied again whole: the stale object is gone.
#[tokio::test]
async fn a_copy_an_earlier_run_left_is_deleted_and_written_again() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(3));
    let stale = format!("{}data/00009.json.gz", prefix_of(&arn));
    mem.insert(&stale, b"left by a run that stopped");
    let command = new_command(&mem);
    let (report, seen) = run(&fake, &command, Selection::All).await;
    assert!(report.complete(), "{report:?}");
    assert!(!mem.keys().contains(&stale));
    assert!(
        seen[0]
            .notes
            .iter()
            .any(|n| n.starts_with("deleted the copy an earlier run left")),
        "{seen:?}"
    );
    assert_eq!(copied_items(&mem, &arn).len(), 3);
}

/// A catalog row of the configured store under the ARN (a `backup sync`
/// that adopted an earlier run's copy before its replacement, or a
/// MongoDB replacement that stopped before its drop, whose in-catalog
/// backup is DELETING) is finished: the replacement with that row removes
/// what is left of the in-catalog backup, and nothing is copied.
#[tokio::test]
async fn a_backup_whose_row_is_in_the_store_is_finished_without_a_copy() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let adopted = legacy_arn(1);
    let deleting = legacy_arn(2);
    plant(&mem, &adopted, a_copy_of(&adopted, legacy_items(3)).await);
    seed_legacy(&fake, &adopted, legacy_items(3));
    seed_legacy(&fake, &deleting, legacy_items(3));
    fake.legacy_keys.lock().unwrap()[1].status = BackupStatus::Deleting;
    for arn in [&adopted, &deleting] {
        let mut existing = row(arn, BackupStatus::Available);
        existing.owner_generation = UNOWNED_GENERATION;
        fake.rows.lock().unwrap().insert(arn.clone(), existing);
    }
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(report.complete(), "{report:?}");
    assert_eq!(outcome_of(&report, &adopted), &Outcome::Finished);
    assert_eq!(outcome_of(&report, &deleting), &Outcome::Finished);
    assert!(mem.puts.lock().unwrap().is_empty());
    assert!(fake.items_opened.lock().unwrap().is_empty());
    assert!(!legacy_kept(&fake, &adopted));
    let finished: Vec<String> = fake
        .replaces_seen
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.backup_arn.clone())
        .collect();
    assert_eq!(finished, [adopted, deleting]);
}

/// A replacement whose call fails (its outcome unknown) stops the run with
/// the copy left; the next run, finding no row, deletes the copy, writes it
/// again and replaces the backup.
#[tokio::test]
async fn a_replacement_that_fails_leaves_the_copy_for_the_next_run() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(3));
    seed_legacy(&fake, &legacy_arn(2), legacy_items(1));
    *fake.replace_error.lock().unwrap() = Some("the connection was reset".to_owned());
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    let stopped = report.stopped.clone().expect("stopped");
    assert!(
        stopped.contains("the connection was reset")
            && stopped.contains("the next run finishes the replacement or deletes the copy"),
        "{stopped}"
    );
    assert!(report.backups.is_empty(), "{report:?}");
    assert!(mem.keys().contains(&manifest_key(&arn)));
    assert!(legacy_kept(&fake, &arn));
    assert!(
        fake.items_opened.lock().unwrap().len() == 1,
        "the second backup was not reached"
    );

    // A rerun whose replacement fails too: its stop carries the note of the
    // copy it deleted, so the backup is reported, `failed`, with the
    // deletion counted as a write and the in-catalog backup not called
    // kept (the replacement's answer did not come).
    *fake.replace_error.lock().unwrap() = Some("the connection was reset again".to_owned());
    let (failed, seen) = run(&fake, &command, Selection::All).await;
    assert!(failed.stopped.is_some(), "{failed:?}");
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(
        matches!(&seen[0].outcome, Outcome::Failed(why) if why.contains("reset again")),
        "{seen:?}"
    );
    assert!(seen[0].notes[0].starts_with("deleted the copy an earlier run left"));
    assert!(seen[0].wrote && !seen[0].in_catalog_kept, "{seen:?}");

    let (again, seen) = run(&fake, &command, Selection::All).await;
    assert!(again.complete(), "{again:?}");
    assert!(matches!(outcome_of(&again, &arn), Outcome::Migrated { .. }));
    assert!(seen[0].notes[0].starts_with("deleted the copy an earlier run left"));
    assert!(!legacy_kept(&fake, &arn));
}

/// A race lost at the replacement: a DeleteBackup that came first
/// (`NotAvailable`) skips the backup, a restore whose target is still
/// CREATING (`InUse`) refuses it naming `remove-target`; either way its copy
/// is deleted and the next backup runs. The in-use backup keeps its
/// in-catalog rows.
#[tokio::test]
async fn a_backup_deleted_or_in_use_at_the_replacement_has_its_copy_deleted() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let gone = legacy_arn(1);
    let in_use = legacy_arn(2);
    let next = legacy_arn(3);
    seed_legacy(&fake, &gone, legacy_items(1));
    seed_legacy(&fake, &in_use, legacy_items(1));
    seed_legacy(&fake, &next, legacy_items(1));
    *fake.replace_answer.lock().unwrap() =
        Some(Replaced::NotAvailable(format!("Backup not found: {gone}")));
    fake.in_use
        .lock()
        .unwrap()
        .insert(in_use.clone(), "MusicRestored".to_owned());
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(
        matches!(outcome_of(&report, &gone), Outcome::Skipped(why) if why.contains("no longer AVAILABLE")),
        "{report:?}"
    );
    let why = refusal(outcome_of(&report, &in_use));
    assert!(
        why.contains("MusicRestored") && why.contains("remove-target"),
        "{why}"
    );
    assert!(copy_of(&mem, &gone).is_empty() && copy_of(&mem, &in_use).is_empty());
    assert!(legacy_kept(&fake, &in_use));
    assert!(matches!(
        outcome_of(&report, &next),
        Outcome::Migrated { .. }
    ));
}

/// A CREATING or DELETING in-catalog backup with no row is skipped and
/// named, unopened; the AVAILABLE one after them is migrated.
#[tokio::test]
async fn creating_and_deleting_in_catalog_backups_are_skipped() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    for n in 1..=3 {
        seed_legacy(&fake, &legacy_arn(n), legacy_items(1));
    }
    fake.legacy_keys.lock().unwrap()[0].status = BackupStatus::Creating;
    fake.legacy_keys.lock().unwrap()[1].status = BackupStatus::Deleting;
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(
        matches!(outcome_of(&report, &legacy_arn(1)), Outcome::Skipped(why) if why.contains("CREATING"))
    );
    assert!(
        matches!(outcome_of(&report, &legacy_arn(2)), Outcome::Skipped(why) if why.contains("DeleteBackup again"))
    );
    assert!(matches!(
        outcome_of(&report, &legacy_arn(3)),
        Outcome::Migrated { .. }
    ));
    assert_eq!(*fake.items_opened.lock().unwrap(), [legacy_arn(3)]);
    assert!(!report.complete());
}

/// A signal received before a backup stops the run there, and a claim a
/// heartbeat found lost stops it too; neither touches the store or the
/// in-catalog backups.
#[tokio::test]
async fn a_signal_or_a_lost_claim_stops_the_run_before_the_next_backup() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    seed_legacy(&fake, &legacy_arn(1), legacy_items(1));
    let command = new_command(&mem);
    command.signal.send(Some("SIGINT")).unwrap();
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert_eq!(report.stopped.as_deref(), Some("SIGINT received"));
    assert!(report.backups.is_empty());

    let command = new_command(&mem);
    let lost = maintenance::HeldClaim::start(
        Box::new(Forward(Arc::clone(&command.holder))),
        Duration::from_millis(10),
    );
    *command.holder.lost.lock().unwrap() = true;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while lost.lost().is_none() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let command = Command {
        claim: lost,
        ..command
    };
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(
        report
            .stopped
            .as_deref()
            .is_some_and(|s| s.contains("lost")),
        "{report:?}"
    );
    assert!(report.backups.is_empty());
    assert!(mem.puts.lock().unwrap().is_empty());
    assert!(legacy_kept(&fake, &legacy_arn(1)));
}

/// The maintenance claim as the writer reads it: once a heartbeat finds it
/// lost, the pre-put check fails, the loss token the writer selects on is
/// cancelled (so a copy stops at its next await, not at the next backup),
/// and the reason is the heartbeat's.
#[tokio::test]
async fn a_lost_maintenance_claim_cancels_the_writers_token() {
    use super::super::job::writer::ClaimWatch;
    let holder = Arc::new(FakeHolder::default());
    let claim = maintenance::HeldClaim::start(
        Box::new(Forward(Arc::clone(&holder))),
        Duration::from_millis(10),
    );
    assert!(ClaimWatch::confirms_the_claim(&claim));
    assert!(!ClaimWatch::lost(&claim).is_cancelled());
    *holder.lost.lock().unwrap() = true;
    tokio::time::timeout(
        Duration::from_secs(10),
        ClaimWatch::lost(&claim).cancelled(),
    )
    .await
    .expect("the loss token was cancelled");
    assert!(!ClaimWatch::confirms_the_claim(&claim));
    assert!(
        ClaimWatch::reason(&claim).contains("another holder took it over"),
        "{}",
        ClaimWatch::reason(&claim)
    );
}

/// `<arn>` names the backup as DescribeBackup resolves it: another region
/// written by the operator finds the backup this server issued. An ARN no
/// in-catalog backup has is refused; one a row of the configured store
/// holds is already in the store.
#[tokio::test]
async fn an_arn_names_the_backup_describe_backup_resolves() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(2));
    seed_legacy(&fake, &legacy_arn(2), legacy_items(2));
    let command = new_command(&mem);
    let written = arn.replace(REGION, "eu-west-1");
    let (report, _) = run(&fake, &command, Selection::Arn(&written)).await;
    assert!(report.complete(), "{report:?}");
    assert_eq!(report.backups.len(), 1);
    assert!(matches!(
        outcome_of(&report, &arn),
        Outcome::Migrated { .. }
    ));
    assert!(legacy_kept(&fake, &legacy_arn(2)), "only the named backup");

    let (again, _) = run(&fake, &command, Selection::Arn(&arn)).await;
    assert_eq!(outcome_of(&again, &arn), &Outcome::InStore);
    assert!(again.complete());

    // A backup stored under another region, named as stored.
    let elsewhere = legacy_arn(3).replace(REGION, "eu-west-1");
    seed_legacy(&fake, &elsewhere, legacy_items(1));
    let (report, _) = run(&fake, &command, Selection::Arn(&elsewhere)).await;
    assert!(
        matches!(outcome_of(&report, &elsewhere), Outcome::Migrated { .. }),
        "{report:?}"
    );

    let unknown = legacy_arn(9);
    let (none, _) = run(&fake, &command, Selection::Arn(&unknown)).await;
    assert!(
        matches!(outcome_of(&none, &unknown), Outcome::NotFound(why) if why.contains("no in-catalog backup")),
        "{none:?}"
    );
    assert!(!none.complete());
}

/// `backup sync` skips a prefix whose ARN an in-catalog backup holds, under
/// the manifest's spelling and under the one this server issues, naming
/// `migrate-legacy`; no row is inserted, so the backup is not in the
/// catalog twice.
#[tokio::test]
async fn sync_skips_a_prefix_whose_arn_an_in_catalog_backup_holds() {
    use super::super::sync::SkipReason;
    let identity = store();
    for (described, written) in [
        (sync_arn(1), sync_arn(1)),
        (sync_arn(2), sync_arn(2).replace(REGION, "eu-west-1")),
        // A backup taken under an earlier `server.region`: the in-catalog
        // backup and its copy's manifest both under that region.
        (
            sync_arn(3).replace(REGION, "eu-west-1"),
            sync_arn(3).replace(REGION, "eu-west-1"),
        ),
    ] {
        let mem = Arc::new(MemStore::default());
        let fake = Arc::new(Fake::default());
        let mut description = backup_description("AVAILABLE", "PAY_PER_REQUEST");
        description.backup_details.backup_arn = described.clone();
        fake.descriptions
            .lock()
            .unwrap()
            .insert(described.clone(), description);
        put_backup(&mem, &written);
        let report = super::super::sync::sync(&*fake, sync_target(&mem, &identity))
            .await
            .unwrap();
        assert!(report.adopted.is_empty(), "{report:?}");
        let reason = &skipped_by_prefix(&report)[&prefix_of(&written)];
        assert_eq!(
            reason,
            &SkipReason::InCatalog {
                backup_arn: described.clone(),
                status: "AVAILABLE".to_owned(),
            }
        );
        assert!(
            reason
                .to_string()
                .contains(&format!("extenddb backup migrate-legacy {described}")),
            "{reason}"
        );
        assert!(!reason.is_transient());
        assert!(fake.insert_attempts.lock().unwrap().is_empty());
    }
}

/// ListBackups lists a backup both readers hold (a copy `backup sync`
/// adopted before the replacement) once, with the catalog row's summary;
/// the other in-catalog backups are listed as before.
#[tokio::test]
async fn list_backups_lists_a_backup_both_readers_hold_once_with_its_row() {
    let both = summary(1, 1_791_976_431_001);
    let legacy_only = summary(2, 1_791_976_431_002);
    let fake = fake_with_summaries(vec![
        BackupSummary {
            backup_name: "legacy-name".to_owned(),
            ..both.clone()
        },
        legacy_only.clone(),
    ]);
    let mut indexed = row(&both.backup_arn, BackupStatus::Available);
    indexed.backup_name = "indexed-name".to_owned();
    fake.rows
        .lock()
        .unwrap()
        .insert(both.backup_arn.clone(), indexed);
    let page = list(&fake, json!({})).await.unwrap();
    let mut listed = arns(&page);
    listed.sort();
    assert_eq!(listed, [both.backup_arn.clone(), legacy_only.backup_arn]);
    let names: Vec<&str> = page["BackupSummaries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["BackupArn"] == both.backup_arn.as_str())
        .map(|s| s["BackupName"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["indexed-name"]);
}

/// The objects under `arn`'s prefix, keys relative to it.
fn relative_objects(mem: &MemStore, arn: &str) -> Vec<(String, Vec<u8>)> {
    let prefix = prefix_of(arn);
    mem.objects
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix(&prefix)
                .map(|rel| (rel.to_owned(), v.clone()))
        })
        .collect()
}

/// A whole copy of `arn` holding `items`, as a run of the migration writes
/// one into a store of its own: its objects, keys relative to the prefix.
async fn a_copy_of(arn: &str, items: Vec<Item>) -> Vec<(String, Vec<u8>)> {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    seed_legacy(&fake, arn, items);
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(report.complete(), "{report:?}");
    relative_objects(&mem, arn)
}

/// Once `mem`'s manifest of `arn` is put, the objects under `arn`'s prefix
/// are replaced by `objects` (relative keys): a store that serves another
/// backup's bytes there.
fn serve_after_the_manifest(mem: &Arc<MemStore>, arn: &str, objects: Vec<(String, Vec<u8>)>) {
    let weak = Arc::downgrade(mem);
    let prefix = prefix_of(arn);
    let manifest = manifest_key(arn);
    *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
        if key == manifest
            && let Some(mem) = weak.upgrade()
        {
            mem.objects
                .lock()
                .unwrap()
                .retain(|k, _| !k.starts_with(&prefix));
            for (rel, bytes) in &objects {
                mem.insert(&format!("{prefix}{rel}"), bytes);
            }
        }
    }));
}

/// Put `objects` (keys relative to the prefix) under `arn`'s prefix of
/// `mem`, as a run that wrote them would have left them.
fn plant(mem: &MemStore, arn: &str, objects: Vec<(String, Vec<u8>)>) {
    let prefix = prefix_of(arn);
    for (rel, bytes) in objects {
        mem.insert(&format!("{prefix}{rel}"), &bytes);
    }
}

/// `run` under a deadline of its own, so a run that never ends fails its
/// cell by name instead of exhausting the test process.
async fn within_twenty_seconds<T>(run: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("the run ends within twenty seconds")
}

/// A DeleteBackup of the in-catalog backup after a run stopped with its
/// whole copy written (the replacement's call failed, as a kill after the
/// manifest leaves it): `backup sync` adopts nothing, naming the deleted
/// backup and the command; `--all` deletes the copy and says so, and is
/// complete; a sync after it finds nothing. The deleted backup never comes
/// back.
#[tokio::test]
async fn a_backup_deleted_after_a_stopped_copy_never_comes_back() {
    use super::super::sync::SkipReason;
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(3));
    let mut description = backup_description("AVAILABLE", "PAY_PER_REQUEST");
    description.backup_details.backup_arn = arn.clone();
    fake.descriptions
        .lock()
        .unwrap()
        .insert(arn.clone(), description);
    *fake.replace_error.lock().unwrap() = Some("the process was killed".to_owned());
    let command = new_command(&mem);
    let (stopped, _) = run(&fake, &command, Selection::All).await;
    assert!(stopped.stopped.is_some(), "{stopped:?}");
    assert!(mem.keys().contains(&manifest_key(&arn)), "the whole copy");

    let deleted = delete_backup(&fake, &command.runtime, ACCOUNT, REGION, &own(&arn))
        .await
        .unwrap();
    assert_eq!(deleted.backup_details.backup_status, "DELETED");

    let identity = store();
    let synced = super::super::sync::sync(&fake, sync_target(&mem, &identity))
        .await
        .unwrap();
    assert!(synced.adopted.is_empty(), "{synced:?}");
    let reason = &skipped_by_prefix(&synced)[&prefix_of(&arn)];
    assert_eq!(
        reason,
        &SkipReason::InCatalog {
            backup_arn: arn.clone(),
            status: "DELETED".to_owned(),
        }
    );
    assert!(
        reason.to_string().contains("was deleted")
            && reason
                .to_string()
                .contains(&format!("extenddb backup migrate-legacy {arn} deletes")),
        "{reason}"
    );
    assert!(fake.insert_attempts.lock().unwrap().is_empty());

    let (cleaned, seen) = run(&fake, &command, Selection::All).await;
    assert!(cleaned.complete(), "{cleaned:?}");
    assert_eq!(outcome_of(&cleaned, &arn), &Outcome::Deleted);
    assert!(
        seen[0]
            .notes
            .iter()
            .any(|n| n.starts_with("deleted the copy a stopped run left")),
        "{seen:?}"
    );
    // The record goes after the copy, once nothing is left under the
    // prefix.
    assert!(
        seen[0].notes[1].starts_with("removed its in-catalog record"),
        "{seen:?}"
    );
    assert_eq!(
        *fake.removed_deleted.lock().unwrap(),
        std::slice::from_ref(&arn)
    );
    assert!(seen[0].wrote && !seen[0].in_catalog_kept, "{seen:?}");
    assert!(copy_of(&mem, &arn).is_empty(), "{:?}", copy_of(&mem, &arn));
    let synced = super::super::sync::sync(&fake, sync_target(&mem, &identity))
        .await
        .unwrap();
    assert!(synced.adopted.is_empty(), "{synced:?}");
    assert!(fake.rows.lock().unwrap().is_empty());

    // Nothing left: a deleted backup with no copy is not reported again.
    let (again, _) = run(&fake, &command, Selection::All).await;
    assert!(again.backups.is_empty() && again.complete(), "{again:?}");
}

/// A partial copy (a data file, no manifest) of a backup deleted since is
/// deleted by `<arn>`, which answers "deleted" (complete); the prefix
/// another row holds is left alone, and a deleted backup whose id has the
/// 13-digit shape reads nothing from the store.
#[tokio::test]
async fn arn_of_a_deleted_backup_deletes_a_partial_copy_and_nothing_a_row_holds() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let partial = legacy_arn(1);
    let held = legacy_arn(2);
    for arn in [&partial, &held] {
        seed_legacy(&fake, arn, legacy_items(1));
        fake.legacy_keys
            .lock()
            .unwrap()
            .iter_mut()
            .filter(|k| k.backup_arn == *arn)
            .for_each(|k| k.status = BackupStatus::Deleted);
        mem.insert(&format!("{}data/000001.json.gz", prefix_of(arn)), b"part");
    }
    fake.rows
        .lock()
        .unwrap()
        .insert(held.clone(), row(&held, BackupStatus::Available));
    let command = new_command(&mem);
    let (report, seen) = run(&fake, &command, Selection::Arn(&partial)).await;
    assert!(report.complete(), "{report:?}");
    assert_eq!(outcome_of(&report, &partial), &Outcome::Deleted);
    assert!(seen[0].notes[0].starts_with("deleted the copy a stopped run left"));
    assert!(seen[0].wrote && !seen[0].in_catalog_kept, "{seen:?}");
    assert!(copy_of(&mem, &partial).is_empty());

    // A row holding the ARN: the deleted backup is in the catalog again.
    // It is reported, with the choice, and nothing is deleted or written;
    // `--all` reports it too.
    for selection in [Selection::Arn(&held), Selection::All] {
        let (report, seen) = run(&fake, &command, selection).await;
        let why = refusal(outcome_of(&report, &held));
        assert!(
            why.contains("(AVAILABLE)") && why.contains("DeleteBackup the ARN again"),
            "{why}"
        );
        assert!(!report.complete());
        let line = seen.iter().find(|b| b.backup_arn == held).unwrap();
        assert!(!line.wrote && !line.in_catalog_kept, "{line:?}");
        assert_eq!(copy_of(&mem, &held).len(), 1, "the row's prefix is left");
        assert!(!fake.removed_deleted.lock().unwrap().contains(&held));
    }
    assert_eq!(
        *fake.removed_deleted.lock().unwrap(),
        std::slice::from_ref(&partial)
    );

    // The row gone, `--all` deletes the copy and removes the record; a
    // deleted backup whose id has the 13-digit shape is left as it is,
    // unreported.
    fake.rows.lock().unwrap().remove(&held);
    let thirteen =
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/1793254932797-4116998c");
    fake.legacy_keys.lock().unwrap().push(LegacyBackupKey {
        account_id: ACCOUNT.to_owned(),
        backup_arn: thirteen.clone(),
        status: BackupStatus::Deleted,
    });
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(report.complete(), "{report:?}");
    assert_eq!(report.backups.len(), 1, "{report:?}");
    assert_eq!(outcome_of(&report, &held), &Outcome::Deleted);
    assert!(copy_of(&mem, &held).is_empty());
    assert_eq!(*fake.removed_deleted.lock().unwrap(), [partial, held]);
    assert!(
        fake.legacy_keys
            .lock()
            .unwrap()
            .iter()
            .any(|k| k.backup_arn == thirteen),
        "a 13-digit tombstone stays"
    );
    assert!(
        !fake
            .id_reads
            .lock()
            .unwrap()
            .iter()
            .any(|id| thirteen.ends_with(id.as_str()))
    );
}

/// The fence before the deletion of a deleted backup's copy: a root another
/// catalog adopted stops the run and deletes nothing.
#[tokio::test]
async fn a_deleted_backups_copy_on_a_fenced_root_is_left() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(1));
    fake.legacy_keys.lock().unwrap()[0].status = BackupStatus::Deleted;
    let left = format!("{}data/000001.json.gz", prefix_of(&arn));
    mem.insert(&left, b"part");
    let command = new_command(&mem);
    mem.insert(STORE_IDENTITY_KEY, &adopted_file());
    let (report, _) = run(&fake, &command, Selection::All).await;
    let stopped = report.stopped.clone().expect("stopped");
    assert!(stopped.contains("deletion fence refused"), "{stopped}");
    assert!(mem.keys().contains(&left));
    assert!(
        fake.removed_deleted.lock().unwrap().is_empty(),
        "the record is kept"
    );
    assert_fenced(&command.runtime);
}

/// MongoDB's replacement that committed and stopped before its drop is
/// finished whatever the catalog index row holds, and reported by it:
/// "finished" while the migrated backup's row is AVAILABLE, "deleted" once
/// a DeleteBackup of it removed the row. `--all` and `<arn>` (for which
/// `holder_status` answers nothing, as MongoDB answers for a marked
/// document) both finish it, nothing is copied, and each is a write.
#[tokio::test]
async fn a_replacement_stopped_before_its_end_is_finished_whatever_its_row_holds() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let all = legacy_arn(1);
    let named = legacy_arn(2);
    let kept_all = legacy_arn(3);
    let kept_named = legacy_arn(4);
    for arn in [&all, &named, &kept_all, &kept_named] {
        seed_legacy(&fake, arn, legacy_items(1));
        fake.migrated_deleting.lock().unwrap().insert(arn.clone());
    }
    for arn in [&kept_all, &kept_named] {
        fake.rows
            .lock()
            .unwrap()
            .insert(arn.clone(), row(arn, BackupStatus::Available));
    }
    for key in fake.legacy_keys.lock().unwrap().iter_mut() {
        key.status = BackupStatus::Deleting;
    }
    let command = new_command(&mem);
    for (arn, expected) in [(&named, Outcome::Deleted), (&kept_named, Outcome::Finished)] {
        let (report, seen) = run(&fake, &command, Selection::Arn(arn)).await;
        assert_eq!(outcome_of(&report, arn), &expected, "{report:?}");
        assert!(seen[0].wrote, "{seen:?}");
    }
    let (report, seen) = run(&fake, &command, Selection::All).await;
    assert!(report.complete(), "{report:?}");
    assert_eq!(outcome_of(&report, &all), &Outcome::Deleted);
    assert_eq!(outcome_of(&report, &kept_all), &Outcome::Finished);
    assert!(seen.iter().all(|b| b.wrote), "{seen:?}");
    assert_eq!(
        *fake.finished_replacements.lock().unwrap(),
        [named, kept_named, all, kept_all]
    );
    assert!(mem.puts.lock().unwrap().is_empty());
    assert!(fake.items_opened.lock().unwrap().is_empty());
}

/// What `<arn>` and the finish report follow the catalog row's status: an
/// AVAILABLE row is in the store; a DELETING or DELETED row (a DeleteBackup
/// of the migrated backup) is deleted, nothing to migrate, exit 0; a
/// CREATING row is refused without the replacement's call. The finish's
/// answers: `InUse` refuses naming the target, `NotAvailable` for an
/// AVAILABLE row skips.
#[tokio::test]
async fn arn_and_finish_report_by_the_rows_status() {
    let mem = Arc::new(MemStore::default());
    let command = new_command(&mem);
    for (status, expected) in [
        (BackupStatus::Available, Some(Outcome::InStore)),
        (BackupStatus::Deleting, Some(Outcome::Deleted)),
        (BackupStatus::Deleted, Some(Outcome::Deleted)),
        (BackupStatus::Creating, None),
    ] {
        let fake = Fake::default();
        let arn = legacy_arn(1);
        let mut held = row(&arn, status);
        held.owner_generation = UNOWNED_GENERATION;
        fake.rows.lock().unwrap().insert(arn.clone(), held);
        let (report, _) = run(&fake, &command, Selection::Arn(&arn)).await;
        match expected {
            Some(outcome) => {
                assert_eq!(outcome_of(&report, &arn), &outcome, "{status}");
                assert!(report.complete(), "{status}: {report:?}");
                assert_eq!(fake.replaces_seen.lock().unwrap().len(), 1, "{status}");
                assert!(
                    !report.backups[0].wrote,
                    "{status}: nothing in-catalog was left to remove"
                );
            }
            None => {
                assert!(
                    matches!(outcome_of(&report, &arn), Outcome::NotFound(why) if why.contains("CREATING")),
                    "{report:?}"
                );
                assert!(!report.complete());
                assert!(fake.replaces_seen.lock().unwrap().is_empty());
            }
        }
    }

    for (answer, check) in [
        (
            Replaced::InUse("MusicRestored".to_owned()),
            Box::new(
                |o: &Outcome| matches!(o, Outcome::Refused(why) if why.contains("MusicRestored")),
            ) as Box<dyn Fn(&Outcome) -> bool>,
        ),
        (
            Replaced::NotAvailable("the backup is DELETING in the store".to_owned()),
            Box::new(
                |o: &Outcome| matches!(o, Outcome::Skipped(why) if why.contains("no longer AVAILABLE")),
            ),
        ),
    ] {
        let fake = Fake::default();
        let arn = legacy_arn(1);
        seed_legacy(&fake, &arn, legacy_items(1));
        plant(&mem, &arn, a_copy_of(&arn, legacy_items(1)).await);
        fake.rows
            .lock()
            .unwrap()
            .insert(arn.clone(), row(&arn, BackupStatus::Available));
        *fake.replace_answer.lock().unwrap() = Some(answer.clone());
        let (report, _) = run(&fake, &command, Selection::All).await;
        assert!(check(outcome_of(&report, &arn)), "{answer:?}: {report:?}");
        assert!(!report.complete());
    }
}

/// A catalog row of the backup's id under the other spelling of its ARN
/// (the server's region; the in-catalog backup carries another) refuses the
/// backup before anything is deleted or written: the prefix, which is
/// spelling-free, may be that row's.
#[tokio::test]
async fn a_row_under_the_other_spelling_refuses_the_backup_and_deletes_nothing() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let canonical = legacy_arn(1);
    let stored = canonical.replace(REGION, "eu-west-1");
    seed_legacy(&fake, &stored, legacy_items(2));
    fake.rows
        .lock()
        .unwrap()
        .insert(canonical.clone(), row(&canonical, BackupStatus::Available));
    let object = format!("{}data/000001.json.gz", prefix_of(&stored));
    mem.insert(&object, b"the row's bytes");
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    let why = refusal(outcome_of(&report, &stored));
    assert!(
        why.contains(&canonical) && why.contains("holds its backup id"),
        "{why}"
    );
    assert!(mem.keys().contains(&object), "nothing deleted");
    assert!(mem.puts.lock().unwrap().is_empty());
    assert!(legacy_kept(&fake, &stored));
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
}

/// An `AlreadyExists` answer at the replacement (a row of the id written
/// after the migration read none) refuses the backup and keeps its copy:
/// the prefix may be that row's.
#[tokio::test]
async fn already_exists_at_the_replacement_keeps_the_copy() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(2));
    *fake.before_replace.lock().unwrap() = Some(Box::new(|fake: &Fake| {
        let other = legacy_arn(1).replace("table/Music/", "table/Other/");
        fake.rows
            .lock()
            .unwrap()
            .insert(other.clone(), row(&other, BackupStatus::Available));
    }));
    let command = new_command(&mem);
    let (report, seen) = run(&fake, &command, Selection::All).await;
    assert!(refusal(outcome_of(&report, &arn)).contains("under another ARN"));
    assert!(mem.keys().contains(&manifest_key(&arn)), "the copy is kept");
    assert!(seen[0].notes[0].contains("is left"), "{seen:?}");
    assert!(seen[0].in_catalog_kept);
    assert!(legacy_kept(&fake, &arn));
}

/// The fence before the deletion of a copy an earlier run left: a root
/// another catalog adopted stops the run with the copy left, and nothing
/// is written.
#[tokio::test]
async fn a_leftover_copy_on_a_fenced_root_is_left() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(1));
    let left = format!("{}data/000009.json.gz", prefix_of(&arn));
    mem.insert(&left, b"left by a run that stopped");
    let command = new_command(&mem);
    mem.insert(STORE_IDENTITY_KEY, &adopted_file());
    let (report, _) = run(&fake, &command, Selection::All).await;
    let stopped = report.stopped.clone().expect("stopped");
    assert!(
        stopped.contains("refused the deletion of the copy an earlier run left"),
        "{stopped}"
    );
    assert!(mem.keys().contains(&left));
    assert!(mem.puts.lock().unwrap().is_empty());
    assert!(legacy_kept(&fake, &arn));
}

/// The fence beside the re-checks before the two steps that cannot be
/// undone. A root another catalog adopted after the manifest's put, with a
/// replacement that would succeed: the run stops before the replacement,
/// with the copy and the in-catalog backup left and no replacement call.
/// Adopted at a data file's put, before the manifest: the run stops before
/// the manifest's put, with the data files left (no manifest, so nothing
/// reads the prefix as a backup) and the in-catalog backup kept. Nothing
/// is deleted either way.
#[tokio::test]
async fn a_root_fenced_before_the_manifest_or_the_replacement_stops_the_run_with_both_kept() {
    for (at, step) in [
        ("extenddb-manifest.json", "the replacement"),
        ("data/000001.json.gz", "the manifest's put"),
    ] {
        let mem = Arc::new(MemStore::default());
        mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
        let fake = Fake::default();
        let arn = legacy_arn(1);
        seed_legacy(&fake, &arn, legacy_items(2));
        let command = new_command(&mem);
        let weak = Arc::downgrade(&mem);
        let key = format!("{}{at}", prefix_of(&arn));
        *mem.on_put.lock().unwrap() = Some(Box::new(move |put: &str| {
            if put == key
                && let Some(mem) = weak.upgrade()
            {
                mem.insert(STORE_IDENTITY_KEY, &adopted_file());
            }
        }));
        let (report, _) = run(&fake, &command, Selection::All).await;
        let stopped = report.stopped.clone().expect("the run stops");
        assert!(
            stopped.contains(&format!("the deletion fence refused {step} of {arn}"))
                && stopped.contains("is left, its in-catalog backup is kept"),
            "{at}: {stopped}"
        );
        assert!(
            !report
                .backups
                .iter()
                .any(|b| matches!(b.outcome, Outcome::Migrated { .. })),
            "{at}: {report:?}"
        );
        assert!(fake.replaces_seen.lock().unwrap().is_empty(), "{at}");
        assert!(legacy_kept(&fake, &arn), "{at}");
        assert!(mem.deletes.lock().unwrap().is_empty(), "{at}");
        assert_eq!(
            mem.keys().contains(&manifest_key(&arn)),
            step == "the replacement",
            "{at}"
        );
        assert!(
            mem.keys()
                .contains(&format!("{}data/000001.json.gz", prefix_of(&arn))),
            "{at}: the data file is left"
        );
    }
}

/// The fence before the deletion of a copy at the backup's end: a root
/// another catalog adopted after the copy was read back, with a
/// replacement that finds the backup deleted, stops the run with the copy
/// left and a note of it.
#[tokio::test]
async fn a_copy_on_a_root_fenced_before_its_deletion_is_left_with_a_note() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Arc::new(Fake::default());
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(2));
    *fake.replace_answer.lock().unwrap() = Some(Replaced::NotAvailable(
        "the in-catalog backup is DELETED".to_owned(),
    ));
    let weak = Arc::downgrade(&mem);
    *fake.before_replace.lock().unwrap() = Some(Box::new(move |_: &Fake| {
        if let Some(mem) = weak.upgrade() {
            mem.insert(STORE_IDENTITY_KEY, &adopted_file());
        }
    }));
    let command = new_command(&mem);
    let (report, seen) = run(&fake, &command, Selection::All).await;
    let stopped = report.stopped.clone().expect("the run stops");
    assert!(
        stopped.contains("the deletion fence refused the deletion of the copy"),
        "{stopped}"
    );
    assert!(mem.keys().contains(&manifest_key(&arn)), "the copy is left");
    assert!(
        seen[0].notes.iter().any(|n| n.contains("was not deleted")),
        "{seen:?}"
    );
    assert!(
        seen[0]
            .notes
            .iter()
            .any(|n| n.contains("before the stop: skipped")),
        "{seen:?}"
    );
}

/// The second count check: a store that serves a whole, valid backup of
/// another count under the prefix once the manifest is put (the items
/// written match the recorded count, so only the re-read's total differs)
/// refuses the backup before any row.
#[tokio::test]
async fn a_copy_that_reads_back_with_another_count_is_refused() {
    let arn = legacy_arn(1);
    let three = a_copy_of(&arn, legacy_items(3)).await;
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    seed_legacy(&fake, &arn, legacy_items(4));
    let command = new_command(&mem);
    serve_after_the_manifest(&mem, &arn, three);
    let (report, _) = run(&fake, &command, Selection::All).await;
    let why = refusal(outcome_of(&report, &arn));
    assert!(why.contains("manifest counts 3 items"), "{why}");
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
    assert!(legacy_kept(&fake, &arn));
}

/// The manifest's ARN check: a store that serves another backup's whole
/// copy under the prefix fails the backup before any row.
#[tokio::test]
async fn a_copy_that_reads_back_as_another_backup_fails() {
    let arn = legacy_arn(1);
    let other = a_copy_of(&legacy_arn(2), legacy_items(2)).await;
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    seed_legacy(&fake, &arn, legacy_items(2));
    let command = new_command(&mem);
    serve_after_the_manifest(&mem, &arn, other);
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(
        matches!(outcome_of(&report, &arn), Outcome::Failed(why) if why.contains("names another backup")),
        "{report:?}"
    );
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
}

/// The manifest's `source_table.table_size_bytes` is the copied items'
/// DynamoDB sizes, summed; the region and creation-time notes name a
/// backup of another region and an in-catalog creation time off its id's
/// instant.
#[tokio::test]
async fn the_copy_records_its_items_size_and_notes_the_region_and_the_time() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1).replace(REGION, "eu-west-1");
    let items = legacy_items(4);
    let sum: i64 = items
        .iter()
        .map(|i| i64::try_from(extenddb_core::types::item_size_bytes(i)).unwrap())
        .sum();
    seed_legacy(&fake, &arn, items);
    fake.sources
        .lock()
        .unwrap()
        .get_mut(&arn)
        .unwrap()
        .backup_creation_date_time += 5.0;
    let command = new_command(&mem);
    let (report, seen) = run(&fake, &command, Selection::All).await;
    assert!(report.complete(), "{report:?}");
    assert_eq!(
        stored_manifest(&mem, &arn).source_table.table_size_bytes,
        sum
    );
    let notes = &seen[0].notes;
    assert!(
        notes
            .iter()
            .any(|n| n.contains("its region eu-west-1 is not this server's")),
        "{notes:?}"
    );
    assert!(
        notes.iter().any(|n| n.contains("is not its id's instant")),
        "{notes:?}"
    );
}

/// A claim whose heartbeat hangs once `stall` is set (a half-open
/// connection to the catalog).
#[derive(Default)]
struct StallingHolder {
    stall: std::sync::atomic::AtomicBool,
}

impl extenddb_storage::MaintenanceClaim for StallingHolder {
    fn generation(&self) -> u64 {
        13
    }
    fn heartbeat(
        &self,
    ) -> BoxedFuture<'_, Result<extenddb_storage::backup_catalog::Heartbeat, StorageError>> {
        let stall = self.stall.load(std::sync::atomic::Ordering::Acquire);
        Box::pin(async move {
            if stall {
                std::future::pending::<()>().await;
            }
            Ok(extenddb_storage::backup_catalog::Heartbeat::Confirmed)
        })
    }
    fn release(&self) -> BoxedFuture<'_, Result<(), StorageError>> {
        Box::pin(async { Ok(()) })
    }
}

struct ForwardStalling(Arc<StallingHolder>);

impl extenddb_storage::MaintenanceClaim for ForwardStalling {
    fn generation(&self) -> u64 {
        self.0.generation()
    }
    fn heartbeat(
        &self,
    ) -> BoxedFuture<'_, Result<extenddb_storage::backup_catalog::Heartbeat, StorageError>> {
        self.0.heartbeat()
    }
    fn release(&self) -> BoxedFuture<'_, Result<(), StorageError>> {
        self.0.release()
    }
}

/// A heartbeat that hangs from the manifest's put on (the store answers
/// slowly meanwhile): the claim is no longer confirmed by the time the row
/// is to be written, so the run stops before the replacement, with the
/// copy left for the next run and the in-catalog backup kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_heartbeat_stops_the_copy_before_the_replacement() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(2));
    let holder = Arc::new(StallingHolder::default());
    let mut command = new_command(&mem);
    command.claim = maintenance::HeldClaim::start(
        Box::new(ForwardStalling(Arc::clone(&holder))),
        Duration::from_millis(50),
    );
    let manifest = manifest_key(&arn);
    let stalling = Arc::clone(&holder);
    *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
        if key == manifest {
            stalling
                .stall
                .store(true, std::sync::atomic::Ordering::Release);
            std::thread::sleep(Duration::from_millis(400));
        }
    }));
    let (report, _) = run(&fake, &command, Selection::All).await;
    let stopped = report.stopped.clone().expect("stopped");
    assert!(stopped.contains("maintenance claim"), "{stopped}");
    assert!(
        stopped.contains("writes and deletes nothing more"),
        "{stopped}"
    );
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
    assert!(legacy_kept(&fake, &arn));
}

/// A signal sent from another task while the stream is fed slowly, after
/// the first data file is in the store: the copy stops at the writer's
/// next step, is deleted, and the run stops naming the signal.
#[tokio::test]
async fn a_signal_during_a_slow_copy_deletes_the_copy() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(40));
    *fake.items_delay.lock().unwrap() = Some(Duration::from_millis(10));
    let command = new_command(&mem);
    let signal = command.signal.clone();
    let watched = Arc::clone(&mem);
    let sender = tokio::spawn(async move {
        while watched.puts.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        signal.send(Some("SIGTERM")).unwrap();
    });
    let (report, _) = run_with(&fake, &command, Selection::All, 1).await;
    sender.await.unwrap();
    assert_eq!(
        report.stopped.as_deref(),
        Some("SIGTERM received during the copy"),
        "{report:?}"
    );
    assert!(copy_of(&mem, &arn).is_empty(), "{:?}", copy_of(&mem, &arn));
    let named = report
        .backups
        .iter()
        .find(|b| b.backup_arn == arn)
        .expect("the stop names the backup whose copy was deleted");
    assert_eq!(
        named.notes,
        [format!(
            "deleted the objects this run recorded under {}; a put under way at the signal may \
             still have stored its object, which the next run deletes",
            prefix_of(&arn)
        )],
        "{report:?}"
    );
    assert!(named.in_catalog_kept);
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
    assert!(legacy_kept(&fake, &arn));
}

/// A signal received while the replacement runs lets it finish, then stops
/// the run, whether or not another backup follows.
#[tokio::test]
async fn a_signal_during_the_replacement_stops_the_run_after_it() {
    for backups in [1, 2] {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        for n in 1..=backups {
            seed_legacy(&fake, &legacy_arn(n), legacy_items(1));
        }
        let command = new_command(&mem);
        let signal = command.signal.clone();
        let manifest = manifest_key(&legacy_arn(1));
        *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
            if key == manifest {
                signal.send(Some("SIGINT")).unwrap();
            }
        }));
        let (report, _) = run(&fake, &command, Selection::All).await;
        assert!(
            matches!(
                outcome_of(&report, &legacy_arn(1)),
                Outcome::Migrated { .. }
            ),
            "{report:?}"
        );
        assert_eq!(report.backups.len(), 1, "{report:?}");
        assert_eq!(report.stopped.as_deref(), Some("SIGINT received"));
        assert!(!report.complete());
    }
}

/// A replacement that commits between the engine's read of the catalog
/// index and its read of the legacy reader: DescribeBackup, DeleteBackup
/// and RestoreTableFromBackup re-read the index once and answer from the
/// row, and ListBackups lists the backup once.
#[tokio::test]
async fn requests_racing_a_replacement_find_the_row() {
    let arn_text = arn(ACCOUNT);
    let replaced = |fake: &Fake| {
        let arn_text = arn(ACCOUNT);
        fake.rows
            .lock()
            .unwrap()
            .insert(arn_text.clone(), row(&arn_text, BackupStatus::Available));
        fake.descriptions.lock().unwrap().remove(&arn_text);
        fake.sources.lock().unwrap().remove(&arn_text);
        fake.summaries
            .lock()
            .unwrap()
            .retain(|s| s.backup_arn != arn_text);
    };
    let legacy = || {
        let mut src = source(Some(definition("PROVISIONED")));
        src.backup_arn = arn(ACCOUNT);
        let fake = Fake::with_source(src);
        let mut description = backup_description("AVAILABLE", "PROVISIONED");
        description.backup_details.backup_arn = arn(ACCOUNT);
        fake.descriptions
            .lock()
            .unwrap()
            .insert(arn(ACCOUNT), description);
        *fake.before_legacy_read.lock().unwrap() = Some(Box::new(replaced));
        fake
    };

    let fake = legacy();
    let described = describe_backup(&*fake, ACCOUNT, REGION, &own(&arn_text))
        .await
        .unwrap();
    assert_eq!(described.backup_details.backup_name, "music-nightly");

    let fake = legacy();
    let mem = Arc::new(MemStore::default());
    put_backup(&mem, &arn_text);
    let deleted = delete_backup(
        &*fake,
        &runtime_on(&mem, store()),
        ACCOUNT,
        REGION,
        &own(&arn_text),
    )
    .await
    .unwrap();
    assert_eq!(deleted.backup_details.backup_status, "DELETED");
    assert!(
        !fake.delete_starts_seen.lock().unwrap().is_empty(),
        "the row's delete"
    );

    let fake = legacy();
    let configured = BackupRuntime::new(1)
        .with_store(ConfiguredStore::new(
            store().store_kind,
            store().store_root,
            Arc::new(MemStore::default()) as Arc<dyn extenddb_backup_store::BackupStore>,
            None,
            Ok(store()),
        ))
        .with_permit_wait(Duration::ZERO);
    let _held = JobPermit::try_acquire(configured.slots()).unwrap();
    let err = restore_table_from_backup(
        &fake,
        &configured,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        &restore_body(json!({})),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::LimitExceededException(_)),
        "the row's path, up to the permit: {err:?}"
    );
    assert!(fake.restores().is_empty());

    let both = BackupSummary {
        backup_arn: arn_text.clone(),
        ..summary(1, 1_791_976_431_438)
    };
    let fake = fake_with_summaries(vec![both]);
    *fake.before_legacy_read.lock().unwrap() = Some(Box::new(replaced));
    let page = list(&fake, json!({})).await.unwrap();
    assert_eq!(arns(&page), [arn_text]);
}

/// A claim another command took over since its last beat (the holder's
/// next beat would answer `Lost`, and the timer has not run it yet): each
/// step that cannot be undone runs only after a live re-check, so nothing
/// is deleted, replaced, finished or removed, and the run stops naming the
/// loss. The cases: a copy an earlier run left (the leftover's deletion),
/// a row of the configured store (the finish's replacement), MongoDB's
/// stopped replacement (its finish), a deleted backup with a copy (its
/// deletion) and one with nothing under its prefix (its record's removal).
#[tokio::test]
async fn a_claim_taken_over_since_its_last_beat_stops_every_step_that_cannot_be_undone() {
    let arn = legacy_arn(1);
    let left = format!("{}data/000009.json.gz", prefix_of(&arn));
    let whole = a_copy_of(&arn, legacy_items(1)).await;
    type Setup = (&'static str, Box<dyn Fn(&Fake, &MemStore)>);
    let setups: [Setup; 5] = [
        (
            "leftover",
            Box::new(|fake: &Fake, mem: &MemStore| {
                seed_legacy(fake, &legacy_arn(1), legacy_items(1));
                mem.insert(
                    &format!("{}data/000009.json.gz", prefix_of(&legacy_arn(1))),
                    b"left",
                );
            }),
        ),
        (
            "finish",
            Box::new(move |fake: &Fake, mem: &MemStore| {
                seed_legacy(fake, &legacy_arn(1), legacy_items(1));
                plant(mem, &legacy_arn(1), whole.clone());
                fake.rows
                    .lock()
                    .unwrap()
                    .insert(legacy_arn(1), row(&legacy_arn(1), BackupStatus::Available));
            }),
        ),
        (
            "stopped replacement",
            Box::new(|fake: &Fake, _: &MemStore| {
                seed_legacy(fake, &legacy_arn(1), legacy_items(1));
                fake.legacy_keys.lock().unwrap()[0].status = BackupStatus::Deleting;
                fake.migrated_deleting.lock().unwrap().insert(legacy_arn(1));
            }),
        ),
        (
            "deleted with a copy",
            Box::new(|fake: &Fake, mem: &MemStore| {
                seed_legacy(fake, &legacy_arn(1), legacy_items(1));
                fake.legacy_keys.lock().unwrap()[0].status = BackupStatus::Deleted;
                mem.insert(
                    &format!("{}data/000009.json.gz", prefix_of(&legacy_arn(1))),
                    b"left",
                );
            }),
        ),
        (
            "deleted with nothing left",
            Box::new(|fake: &Fake, _: &MemStore| {
                seed_legacy(fake, &legacy_arn(1), legacy_items(1));
                fake.legacy_keys.lock().unwrap()[0].status = BackupStatus::Deleted;
            }),
        ),
    ];
    for (case, setup) in setups {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        setup(&fake, &mem);
        let command = new_command(&mem);
        let before = mem.keys();
        *command.holder.lost.lock().unwrap() = true;
        let (report, _) = run(&fake, &command, Selection::All).await;
        let stopped = report.stopped.clone().expect(case);
        assert!(
            stopped.contains("another holder took it over"),
            "{case}: {stopped}"
        );
        assert_eq!(mem.keys(), before, "{case}: nothing deleted or put");
        assert!(mem.deletes.lock().unwrap().is_empty(), "{case}");
        assert!(fake.replaces_seen.lock().unwrap().is_empty(), "{case}");
        assert!(
            fake.finished_replacements.lock().unwrap().is_empty(),
            "{case}"
        );
        assert!(fake.removed_deleted.lock().unwrap().is_empty(), "{case}");
        if case == "leftover" {
            assert!(mem.keys().contains(&left), "{case}");
        }
    }
}

/// A claim taken over during the copy, seen by no beat yet: the re-check
/// before the manifest stops the run with the data files left (no
/// manifest, so nothing reads the prefix as a backup); taken over at the
/// manifest's put, the re-check before the replacement stops it with the
/// copy left and no replacement call; taken over while the stream fails,
/// the copy's deletion is skipped and noted.
#[tokio::test]
async fn a_claim_taken_over_during_the_copy_is_caught_before_the_next_irreversible_step() {
    // Before the manifest.
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(4));
    let command = new_command(&mem);
    let holder = Arc::clone(&command.holder);
    *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
        if key.contains("/data/") {
            *holder.lost.lock().unwrap() = true;
        }
    }));
    let (report, _) = run(&fake, &command, Selection::All).await;
    let stopped = report.stopped.clone().expect("stopped");
    assert!(
        stopped.contains("another holder took it over")
            && stopped.contains("writes and deletes nothing more"),
        "{stopped}"
    );
    assert!(!mem.keys().contains(&manifest_key(&arn)), "no manifest");
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
    assert!(legacy_kept(&fake, &arn));

    // Before the replacement.
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    seed_legacy(&fake, &arn, legacy_items(4));
    let command = new_command(&mem);
    let holder = Arc::clone(&command.holder);
    let manifest = manifest_key(&arn);
    *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
        if key == manifest {
            *holder.lost.lock().unwrap() = true;
        }
    }));
    let (report, _) = run(&fake, &command, Selection::All).await;
    let stopped = report.stopped.clone().expect("stopped");
    assert!(
        stopped.contains("another holder took it over")
            && stopped.contains("writes and deletes nothing more"),
        "{stopped}"
    );
    assert!(mem.keys().contains(&manifest_key(&arn)), "the copy is left");
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
    assert!(legacy_kept(&fake, &arn));

    // Before the deletion of this run's copy.
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    seed_legacy(&fake, &arn, legacy_items(4));
    *fake.items_fail_after.lock().unwrap() = Some((
        2,
        StorageError::Internal("the connection was reset".to_owned()),
    ));
    let command = new_command(&mem);
    let holder = Arc::clone(&command.holder);
    *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
        if key.contains("/data/") {
            *holder.lost.lock().unwrap() = true;
        }
    }));
    let (report, seen) = run_with(&fake, &command, Selection::All, 1).await;
    assert!(
        seen[0]
            .notes
            .iter()
            .any(|n| n.contains("was not deleted") && n.contains("another holder")),
        "{seen:?}"
    );
    assert!(!copy_of(&mem, &arn).is_empty(), "the copy is left");
    assert!(mem.deletes.lock().unwrap().is_empty());
    assert!(
        matches!(outcome_of(&report, &arn), Outcome::Failed(_)),
        "{report:?}"
    );
    let stopped = report
        .stopped
        .clone()
        .expect("the failed re-check stops the run");
    assert!(
        stopped.contains("another holder took it over")
            && stopped.contains("writes and deletes nothing more"),
        "{stopped}"
    );
    assert!(
        seen[0]
            .notes
            .iter()
            .any(|n| n.starts_with("before the stop: failed")),
        "{seen:?}"
    );
    assert!(command.claim.lost().is_some(), "the loss is recorded");
}

/// A loss the timer recorded before the run reaches a backup stops the run
/// there, before the backup's first catalog read.
#[tokio::test]
async fn a_lost_claim_is_read_before_each_backup() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let held = legacy_arn(1);
    seed_legacy(&fake, &held, legacy_items(1));
    fake.rows
        .lock()
        .unwrap()
        .insert(held.clone(), row(&held, BackupStatus::Available));
    let deleted = legacy_arn(2);
    seed_legacy(&fake, &deleted, legacy_items(1));
    fake.legacy_keys.lock().unwrap()[1].status = BackupStatus::Deleted;
    mem.insert(
        &format!("{}data/000001.json.gz", prefix_of(&deleted)),
        b"left",
    );
    let mut command = new_command(&mem);
    command.claim = maintenance::HeldClaim::start(
        Box::new(Forward(Arc::clone(&command.holder))),
        Duration::from_millis(20),
    );
    *command.holder.lost.lock().unwrap() = true;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert_eq!(
        report.stopped.as_deref(),
        Some("the maintenance claim was lost (another holder took it over)"),
        "{report:?}"
    );
    assert!(fake.row_reads.lock().unwrap().is_empty());
    assert!(fake.id_reads.lock().unwrap().is_empty());
    assert!(report.backups.is_empty());
}

/// A catalog row of the backup's id under a third spelling (neither the
/// stored ARN nor the server's region; the prefix carries no region):
/// the in-catalog backup is refused before anything under its prefix is
/// deleted or written, and so is a deleted one, whose record is kept.
#[tokio::test]
async fn a_row_of_its_id_under_any_arn_keeps_the_prefix() {
    for status in [BackupStatus::Available, BackupStatus::Deleted] {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        let arn = legacy_arn(1);
        seed_legacy(&fake, &arn, legacy_items(2));
        fake.legacy_keys.lock().unwrap()[0].status = status;
        let third = arn.replace(REGION, "eu-west-3");
        fake.rows
            .lock()
            .unwrap()
            .insert(third.clone(), row(&third, BackupStatus::Available));
        let object = format!("{}data/000001.json.gz", prefix_of(&arn));
        mem.insert(&object, b"the row's bytes");
        let command = new_command(&mem);
        let (report, seen) = run(&fake, &command, Selection::All).await;
        let why = refusal(outcome_of(&report, &arn));
        assert!(
            why.contains(&third) && why.contains("holds its backup id"),
            "{status}: {why}"
        );
        assert_eq!(
            mem.objects.lock().unwrap().get(&object).cloned(),
            Some(b"the row's bytes".to_vec())
        );
        assert!(mem.puts.lock().unwrap().is_empty(), "{status}");
        assert!(mem.deletes.lock().unwrap().is_empty(), "{status}");
        assert!(fake.replaces_seen.lock().unwrap().is_empty(), "{status}");
        assert!(fake.removed_deleted.lock().unwrap().is_empty(), "{status}");
        assert_eq!(
            seen[0].in_catalog_kept,
            status == BackupStatus::Available,
            "{status}"
        );
    }
}

/// Where no row can be read by its id (Cassandra's case), the manifest
/// under the prefix decides: one that names another backup, or does not
/// read, refuses the backup (in-catalog or deleted) with the prefix left;
/// one that names this backup is an earlier run's copy, deleted.
#[tokio::test]
async fn without_an_id_index_the_manifest_under_the_prefix_keeps_another_backups_prefix() {
    let arn = legacy_arn(1);
    let third = arn.replace(REGION, "eu-west-3");
    for (status, manifest, kept) in [
        (
            BackupStatus::Available,
            serde_json::to_vec(&json!({ "backup_arn": third })).unwrap(),
            true,
        ),
        (BackupStatus::Available, b"not json".to_vec(), true),
        (
            BackupStatus::Deleted,
            serde_json::to_vec(&json!({ "backup_arn": third })).unwrap(),
            true,
        ),
        (
            BackupStatus::Available,
            serde_json::to_vec(&json!({ "backup_arn": arn })).unwrap(),
            false,
        ),
    ] {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        fake.no_id_index
            .store(true, std::sync::atomic::Ordering::Release);
        seed_legacy(&fake, &arn, legacy_items(2));
        fake.legacy_keys.lock().unwrap()[0].status = status;
        if kept {
            fake.rows
                .lock()
                .unwrap()
                .insert(third.clone(), row(&third, BackupStatus::Available));
        }
        mem.insert(&manifest_key(&arn), &manifest);
        let command = new_command(&mem);
        let (report, _) = run(&fake, &command, Selection::All).await;
        if kept {
            let why = refusal(outcome_of(&report, &arn));
            assert!(why.contains("the manifest under"), "{status}: {why}");
            assert_eq!(
                mem.objects
                    .lock()
                    .unwrap()
                    .get(&manifest_key(&arn))
                    .cloned(),
                Some(manifest),
                "{status}"
            );
            assert!(mem.deletes.lock().unwrap().is_empty(), "{status}");
        } else {
            assert!(
                matches!(outcome_of(&report, &arn), Outcome::Migrated { .. }),
                "{report:?}"
            );
        }
    }
}

/// A deleted backup's record is removed only once nothing is left under
/// its prefix, behind the fence: a deletion that fails, or a store that
/// still lists objects after it, keeps the record (and the line does not
/// say an in-catalog backup is kept); with nothing there, the record goes
/// and `list_deleted` no longer lists it.
#[tokio::test]
async fn a_deleted_backups_record_is_removed_once_its_prefix_is_clean() {
    let arn = legacy_arn(1);
    let left = format!("{}data/000001.json.gz", prefix_of(&arn));
    let deleted = |fake: &Fake| {
        seed_legacy(fake, &arn, legacy_items(1));
        fake.legacy_keys.lock().unwrap()[0].status = BackupStatus::Deleted;
    };

    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    deleted(&fake);
    mem.insert(&left, b"part");
    *mem.fail_deletes_of.lock().unwrap() = Some(prefix_of(&arn));
    let command = new_command(&mem);
    let (report, seen) = run(&fake, &command, Selection::All).await;
    assert!(
        matches!(outcome_of(&report, &arn), Outcome::Failed(why) if why.contains("could not be deleted")),
        "{report:?}"
    );
    assert!(!seen[0].in_catalog_kept);
    assert!(fake.removed_deleted.lock().unwrap().is_empty());

    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    deleted(&fake);
    mem.insert(&left, b"part");
    *mem.late_put_after_delete.lock().unwrap() = Some("data/000002.json.gz".to_owned());
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(
        matches!(outcome_of(&report, &arn), Outcome::Failed(why) if why.contains("still lists")),
        "{report:?}"
    );
    assert!(fake.removed_deleted.lock().unwrap().is_empty());

    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    deleted(&fake);
    let command = new_command(&mem);
    let (report, seen) = run(&fake, &command, Selection::All).await;
    assert!(report.complete(), "{report:?}");
    assert_eq!(outcome_of(&report, &arn), &Outcome::Deleted);
    assert!(seen[0].wrote, "{seen:?}");
    assert!(
        seen[0].notes[0].starts_with("removed its in-catalog record"),
        "{seen:?}"
    );
    assert_eq!(
        *fake.removed_deleted.lock().unwrap(),
        std::slice::from_ref(&arn)
    );
    assert!(fake.list_deleted().await.unwrap().is_empty());
    let (again, _) = run(&fake, &command, Selection::All).await;
    assert!(again.backups.is_empty() && again.complete(), "{again:?}");
}

/// A read that fails or ends short because a DeleteBackup landed during
/// the copy is reported "skipped ... deleted during the copy" (not a
/// failure of a backup that is kept); with the backup still AVAILABLE the
/// same read is a failure, or the count's refusal.
#[tokio::test]
async fn a_read_a_delete_ended_is_skipped_as_deleted_during_the_copy() {
    for (deleted, short) in [(true, false), (true, true), (false, false), (false, true)] {
        let mem = Arc::new(MemStore::default());
        let fake = Arc::new(Fake::default());
        let arn = legacy_arn(1);
        seed_legacy(&fake, &arn, legacy_items(4));
        if short {
            fake.legacy_items
                .lock()
                .unwrap()
                .get_mut(&arn)
                .unwrap()
                .items
                .truncate(2);
        } else {
            *fake.items_fail_after.lock().unwrap() =
                Some((2, StorageError::Internal("collection dropped".to_owned())));
        }
        let command = new_command(&mem);
        if deleted {
            let weak = Arc::downgrade(&fake);
            *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
                if key.contains("/data/")
                    && let Some(fake) = weak.upgrade()
                {
                    fake.legacy_keys.lock().unwrap()[0].status = BackupStatus::Deleted;
                }
            }));
        }
        let (report, seen) = run_with(&fake, &command, Selection::All, 1).await;
        let outcome = outcome_of(&report, &arn);
        match (deleted, short) {
            (true, _) => assert!(
                matches!(outcome, Outcome::Skipped(why) if why.contains("deleted during the copy") && why.contains("DELETED")),
                "{outcome:?}"
            ),
            (false, false) => {
                assert!(matches!(outcome, Outcome::Failed(_)), "{outcome:?}");
                assert!(seen[0].in_catalog_kept);
            }
            (false, true) => assert!(
                refusal(outcome).contains("records 4 items and its read gave 2"),
                "{outcome:?}"
            ),
        }
        assert!(copy_of(&mem, &arn).is_empty(), "{deleted} {short}");
    }
}

/// A backup deleted between `--all`'s listing and its open is skipped, and
/// the run goes on to the next backup.
#[tokio::test]
async fn a_backup_deleted_before_its_open_is_skipped_and_all_goes_on() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    seed_legacy(&fake, &legacy_arn(1), legacy_items(1));
    seed_legacy(&fake, &legacy_arn(2), legacy_items(1));
    *fake.items_error.lock().unwrap() = Some(StorageError::Validation(format!(
        "Backup not found: {}",
        legacy_arn(1)
    )));
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(report.stopped.is_none(), "{report:?}");
    assert!(
        matches!(outcome_of(&report, &legacy_arn(1)), Outcome::Skipped(why) if why.contains("deleted since it was listed")),
        "{report:?}"
    );
    assert!(matches!(
        outcome_of(&report, &legacy_arn(2)),
        Outcome::Migrated { .. }
    ));
}

/// A replacement that commits between RestoreTableFromBackup's source read
/// and the backend's registration (which answers not found and removes its
/// target): the restore re-reads the catalog row and takes the row's path,
/// here up to its in-use rule, instead of answering
/// `BackupNotFoundException`.
#[tokio::test]
async fn a_restore_whose_registration_meets_a_replacement_takes_the_rows_path() {
    let arn_text = arn(ACCOUNT);
    let mut src = source(Some(definition("PROVISIONED")));
    src.backup_arn = arn_text.clone();
    let fake = Fake::with_source(src);
    *fake.restore_error.lock().unwrap() = Some(StorageError::Validation(format!(
        "Backup not found: {arn_text}"
    )));
    *fake.before_legacy_restore.lock().unwrap() = Some(Box::new(|fake: &Fake| {
        let arn_text = arn(ACCOUNT);
        fake.rows
            .lock()
            .unwrap()
            .insert(arn_text.clone(), row(&arn_text, BackupStatus::Available));
        fake.sources.lock().unwrap().remove(&arn_text);
        fake.in_use
            .lock()
            .unwrap()
            .insert(arn_text, "MusicRestoring".to_owned());
    }));
    let runtime = BackupRuntime::new(1)
        .with_store(ConfiguredStore::new(
            store().store_kind,
            store().store_root,
            Arc::new(MemStore::default()) as Arc<dyn extenddb_backup_store::BackupStore>,
            None,
            Ok(store()),
        ))
        .with_permit_wait(Duration::ZERO);
    let err = restore_table_from_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        &restore_body(json!({})),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::BackupInUseException(_)),
        "the row's path: {err:?}"
    );
    assert_eq!(fake.restores().len(), 1, "the legacy registration ran once");
}

/// An object another run wrote under a key the copy writes (here the second
/// data file, put by another writer after this run's first) stops the run
/// at that put: the other object is left byte for byte, nothing is deleted
/// (this run's first file stays for a later run), no replacement is made,
/// and the in-catalog backup is kept. Every object of the copy is written
/// with `put_if_absent`, so no run replaces an object it did not write.
#[tokio::test]
async fn an_object_another_run_wrote_under_the_prefix_stops_the_copy_and_is_left() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(3));
    let theirs = format!("{}data/000002.json.gz", prefix_of(&arn));
    let first = format!("{}data/000001.json.gz", prefix_of(&arn));
    let watched = Arc::clone(&mem);
    let (planted, mine) = (theirs.clone(), first.clone());
    *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
        if key == mine {
            watched.insert(&planted, b"another run's data file");
        }
    }));
    let command = new_command(&mem);
    let (report, _) = run_with(&fake, &command, Selection::All, 1).await;
    let stopped = report.stopped.clone().expect("the run stops");
    assert!(
        stopped.contains("data/000002.json.gz is already under")
            && stopped.contains("writes and deletes nothing more"),
        "{stopped}"
    );
    assert_eq!(
        mem.objects.lock().unwrap().get(&theirs).cloned(),
        Some(b"another run's data file".to_vec())
    );
    assert!(mem.keys().contains(&first), "this run's file is left");
    assert!(mem.deletes.lock().unwrap().is_empty());
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
    assert!(legacy_kept(&fake, &arn));
}

/// A copy that fails after another writer put an object under its prefix
/// (a key this run does not write) deletes the objects this run wrote, one
/// by one by their keys, and leaves the other: no deletion of the prefix.
#[tokio::test]
async fn a_failed_copy_deletes_only_the_objects_it_wrote() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(3));
    *fake.items_fail_after.lock().unwrap() = Some((
        2,
        StorageError::Internal("the connection was reset".to_owned()),
    ));
    let other = format!("{}data/000009.json.gz", prefix_of(&arn));
    let watched = Arc::clone(&mem);
    let planted = other.clone();
    *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
        if key.ends_with("data/000001.json.gz") {
            watched.insert(&planted, b"another writer's object");
        }
    }));
    let command = new_command(&mem);
    let (report, _) = run_with(&fake, &command, Selection::All, 1).await;
    assert!(
        matches!(outcome_of(&report, &arn), Outcome::Failed(why) if why.contains("reset")),
        "{report:?}"
    );
    assert_eq!(
        copy_of(&mem, &arn),
        vec![other.clone()],
        "only the other object is left"
    );
    let deletes = mem.deletes.lock().unwrap().clone();
    let written = mem.puts.lock().unwrap().clone();
    assert!(!written.is_empty());
    let mut expected = written;
    expected.reverse();
    assert_eq!(deletes, expected, "this run's keys, the last written first");
    assert!(legacy_kept(&fake, &arn));
}

/// The claim is read before each object of the copy's deletion: a loss the
/// timer records part way (the holder taken over during the second
/// deletion, seen by the beat that falls in it) leaves the objects not yet
/// deleted, with a note of how many.
#[tokio::test]
async fn a_lost_claim_during_the_copys_deletion_leaves_the_rest() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(3));
    *fake.items_fail_after.lock().unwrap() = Some((
        3,
        StorageError::Internal("the connection was reset".to_owned()),
    ));
    let command = new_command(&mem);
    *mem.delete_delay.lock().unwrap() = Some(Duration::from_millis(1_500));
    let holder = Arc::clone(&command.holder);
    *mem.on_delete.lock().unwrap() = Some(Box::new(move |_: &str| {
        *holder.lost.lock().unwrap() = true;
    }));
    let (report, seen) = run_with(&fake, &command, Selection::All, 1).await;
    assert!(
        matches!(outcome_of(&report, &arn), Outcome::Failed(_)),
        "{report:?}"
    );
    assert_eq!(mem.deletes.lock().unwrap().len(), 2, "two of three deleted");
    assert_eq!(
        copy_of(&mem, &arn),
        vec![format!("{}data/000001.json.gz", prefix_of(&arn))],
        "the first written is left"
    );
    assert!(
        seen[0]
            .notes
            .iter()
            .any(|n| n.contains("was deleted in part")
                && n.contains("another holder took it over")
                && n.contains("1 of its 3 objects are left")),
        "{seen:?}"
    );
    assert!(
        !seen[0].in_catalog_kept,
        "with the claim lost, the in-catalog backup is not known kept"
    );
}

/// A copy an earlier run left is deleted by the prefix walk under the
/// claim's deadline: every pass of `delete_prefix_before` gets the instant
/// two periods after the beat just before it, so a process stopped
/// mid-walk removes nothing past it; a walk that makes no progress before
/// its deadline fails the backup with the copy left.
#[tokio::test]
async fn a_leftover_walk_runs_under_the_claims_deadline() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(1));
    let left = format!("{}data/000009.json.gz", prefix_of(&arn));
    mem.insert(&left, b"left");
    let command = new_command(&mem);
    let started = tokio::time::Instant::now();
    let (report, seen) = within_twenty_seconds(run(&fake, &command, Selection::All)).await;
    let ended = tokio::time::Instant::now();
    assert!(
        matches!(outcome_of(&report, &arn), Outcome::Migrated { .. }),
        "{report:?}"
    );
    assert!(
        seen[0]
            .notes
            .iter()
            .any(|n| n.contains("deleted the copy an earlier run left"))
    );
    let deadlines = mem.deadlines.lock().unwrap().clone();
    assert_eq!(deadlines.len(), 1, "{deadlines:?}");
    let (prefix, deadline) = &deadlines[0];
    assert_eq!(*prefix, prefix_of(&arn));
    let period = Duration::from_secs(1);
    assert!(
        *deadline >= started + 2 * period && *deadline <= ended + 2 * period,
        "the deadline is two periods after a beat taken during the run"
    );
    assert!(!mem.keys().contains(&left));

    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    seed_legacy(&fake, &arn, legacy_items(1));
    mem.insert(&left, b"left");
    mem.deadline_reached
        .store(true, std::sync::atomic::Ordering::Release);
    let command = new_command(&mem);
    let (report, _) = within_twenty_seconds(run(&fake, &command, Selection::All)).await;
    match outcome_of(&report, &arn) {
        Outcome::Failed(why) => assert!(
            why.contains("removed nothing within two heartbeat periods"),
            "{why}"
        ),
        other => panic!("{other:?}"),
    }
    assert!(mem.keys().contains(&left), "the copy is left");
    assert!(legacy_kept(&fake, &arn));
}

/// A read that ends short of the recorded count, of a backup that is gone
/// by the re-read (MongoDB's marked document answers no status), is skipped
/// as deleted during the copy, not refused for a wrong count.
#[tokio::test]
async fn a_short_read_of_a_backup_gone_since_is_skipped_as_deleted() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(3));
    fake.sources
        .lock()
        .unwrap()
        .get_mut(&arn)
        .unwrap()
        .item_count = 5;
    fake.migrated_deleting.lock().unwrap().insert(arn.clone());
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    match outcome_of(&report, &arn) {
        Outcome::Skipped(why) => assert_eq!(
            why,
            "it was deleted during the copy (the in-catalog backup is gone)"
        ),
        other => panic!("{other:?}"),
    }
    assert!(copy_of(&mem, &arn).is_empty());
}

/// Where no row is read by its id (Cassandra's case), a DELETED in-catalog
/// backup stored under another region is refused when a row holds its id
/// under the server's spelling of its ARN, and the object under its prefix
/// (no manifest, so the manifest check cannot tell) is kept.
#[tokio::test]
async fn a_deleted_backup_whose_other_spelling_a_row_holds_is_refused_without_an_id_index() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    fake.no_id_index
        .store(true, std::sync::atomic::Ordering::Release);
    let stored = legacy_arn(1).replace(REGION, "eu-west-1");
    let served = legacy_arn(1);
    seed_legacy(&fake, &stored, legacy_items(1));
    fake.legacy_keys.lock().unwrap()[0].status = BackupStatus::Deleted;
    fake.rows
        .lock()
        .unwrap()
        .insert(served.clone(), row(&served, BackupStatus::Available));
    let object = format!("{}data/000001.json.gz", prefix_of(&stored));
    mem.insert(&object, b"the row's bytes");
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    let why = refusal(outcome_of(&report, &stored));
    assert!(
        why.contains(&served) && why.contains("holds the ARN of this in-catalog backup"),
        "{why}"
    );
    assert_eq!(
        mem.objects.lock().unwrap().get(&object).cloned(),
        Some(b"the row's bytes".to_vec())
    );
    assert!(mem.deletes.lock().unwrap().is_empty());
    assert!(fake.removed_deleted.lock().unwrap().is_empty());
}

/// The `Fake`'s replacement, as PostgreSQL's and SQLite's: `AlreadyReplaced`
/// with a row of the ARN removes what is left of an AVAILABLE in-catalog
/// backup, and keeps any other: a DeleteBackup's DELETED record, which
/// `list_deleted` still lists, and a CREATING or DELETING one.
#[tokio::test]
async fn already_replaced_keeps_a_deleted_in_catalog_record() {
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(1));
    fake.legacy_keys.lock().unwrap()[0].status = BackupStatus::Deleted;
    let held = row(&arn, BackupStatus::Available);
    fake.rows.lock().unwrap().insert(arn.clone(), held.clone());
    let answer = fake.replace_with_index_row(&held).await.unwrap();
    assert_eq!(answer, Replaced::AlreadyReplaced);
    let deleted = fake.list_deleted().await.unwrap();
    assert_eq!(deleted.len(), 1, "{deleted:?}");
    assert_eq!(deleted[0].backup_arn, arn);

    for status in [BackupStatus::Creating, BackupStatus::Deleting] {
        let fake = Fake::default();
        seed_legacy(&fake, &arn, legacy_items(1));
        fake.legacy_keys.lock().unwrap()[0].status = status;
        fake.rows.lock().unwrap().insert(arn.clone(), held.clone());
        assert_eq!(
            fake.replace_with_index_row(&held).await.unwrap(),
            Replaced::AlreadyReplaced
        );
        assert!(legacy_kept(&fake, &arn), "a {status} one is kept");
    }

    let fake = Fake::default();
    seed_legacy(&fake, &arn, legacy_items(1));
    fake.rows.lock().unwrap().insert(arn.clone(), held.clone());
    assert_eq!(
        fake.replace_with_index_row(&held).await.unwrap(),
        Replaced::AlreadyReplaced
    );
    assert!(!legacy_kept(&fake, &arn), "an AVAILABLE one is removed");
}

/// On Cassandra the count refusal names a repair before the rerun: the
/// create wrote each item at ONE and the read was at ALL, and the legacy
/// restore, which reads at ONE, is not offered as the way out. Elsewhere
/// the refusal names the restore.
#[tokio::test]
async fn the_count_refusal_on_cassandra_names_a_repair() {
    for backend in ["cassandra", "postgres"] {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        let arn = legacy_arn(1);
        seed_legacy(&fake, &arn, legacy_items(3));
        fake.sources
            .lock()
            .unwrap()
            .get_mut(&arn)
            .unwrap()
            .item_count = 4;
        let command = new_command(&mem);
        let (report, _) = run_on(&fake, &command, Selection::All, 64 * 1024 * 1024, backend).await;
        let why = refusal(outcome_of(&report, &arn));
        assert!(why.contains("records 4 items"), "{backend}: {why}");
        assert_eq!(
            why.contains("Run nodetool repair on the account's keyspace"),
            backend == "cassandra",
            "{backend}: {why}"
        );
        assert_eq!(
            why.contains("still restores the backup"),
            backend != "cassandra",
            "{backend}: {why}"
        );
        assert!(copy_of(&mem, &arn).is_empty());
        assert!(legacy_kept(&fake, &arn));
    }
}

/// Before `finish` removes an AVAILABLE in-catalog backup for an AVAILABLE
/// catalog row of its ARN, it reads the row's prefix as the main path reads
/// its own copy. A prefix with nothing under it, one holding a copy of
/// fewer items than the in-catalog backup records, and one holding another
/// backup's copy each refuse the backup naming the row, with the
/// in-catalog backup kept and no replacement call. The first two say the
/// next run copies the backup again; the third says to look at the prefix
/// first, since the next run refuses the backup on that manifest and
/// copies nothing.
#[tokio::test]
async fn finish_reads_the_rows_prefix_before_it_removes_the_in_catalog_backup() {
    let arn = legacy_arn(1);
    let short = a_copy_of(&arn, legacy_items(2)).await;
    let other = a_copy_of(&legacy_arn(2), legacy_items(3)).await;
    for (case, objects, says, then) in [
        (
            "gone",
            vec![],
            "does not read back as a complete backup",
            ") and run the command again, which copies the backup again",
        ),
        (
            "short",
            short,
            "holds a copy of 2 items where the in-catalog backup records 3",
            ") and run the command again, which copies the backup again",
        ),
        (
            "another",
            other,
            "holds a manifest naming another backup",
            "), look at what the prefix holds and delete it if it is not a backup, then run the \
             command again, which copies the backup again; while that manifest is there the \
             command refuses the backup on it and copies nothing",
        ),
    ] {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        seed_legacy(&fake, &arn, legacy_items(3));
        let mut held = row(&arn, BackupStatus::Available);
        held.owner_generation = UNOWNED_GENERATION;
        fake.rows.lock().unwrap().insert(arn.clone(), held);
        plant(&mem, &arn, objects);
        let command = new_command(&mem);
        let (report, seen) = run(&fake, &command, Selection::All).await;
        let why = refusal(outcome_of(&report, &arn));
        assert!(
            why.contains(says)
                && why.contains(&format!("the catalog row (AVAILABLE) under {arn}"))
                && why.contains("the in-catalog backup is the whole copy")
                && why.ends_with(then)
                && !why.contains("is kept"),
            "{case}: {why}"
        );
        // The line's "Its in-catalog backup is kept" says it, once.
        assert!(report.backups[0].in_catalog_kept, "{case}: {report:?}");
        assert!(report.stopped.is_none(), "{case}: {report:?}");
        assert!(fake.replaces_seen.lock().unwrap().is_empty(), "{case}");
        assert!(legacy_kept(&fake, &arn), "{case}");
        assert!(seen[0].in_catalog_kept, "{case}");
        assert!(!seen[0].wrote, "{case}");
        assert!(mem.puts.lock().unwrap().is_empty(), "{case}");
        assert!(mem.deletes.lock().unwrap().is_empty(), "{case}");
        if case == "another" {
            // What the text says of the run after the row is forgotten.
            fake.rows.lock().unwrap().remove(&arn);
            let (report, _) = run(&fake, &command, Selection::All).await;
            let why = refusal(outcome_of(&report, &arn));
            assert!(
                why.contains("the prefix may hold another backup"),
                "{case}: {why}"
            );
            assert!(mem.puts.lock().unwrap().is_empty(), "{case}");
            assert!(legacy_kept(&fake, &arn), "{case}");
        }
    }
}

/// A status re-read that fails after a read that ended short (or failed)
/// cannot tell a DeleteBackup from the read's own end: the backup fails,
/// saying the status could not be read, with its copy deleted and the
/// in-catalog backup said kept; it is not skipped as deleted.
#[tokio::test]
async fn a_status_reread_that_fails_after_a_short_read_fails_the_backup() {
    for short in [true, false] {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        let arn = legacy_arn(1);
        seed_legacy(&fake, &arn, legacy_items(4));
        if short {
            fake.legacy_items
                .lock()
                .unwrap()
                .get_mut(&arn)
                .unwrap()
                .items
                .truncate(2);
        } else {
            *fake.items_fail_after.lock().unwrap() =
                Some((2, StorageError::Internal("collection dropped".to_owned())));
        }
        *fake.holder_status_error.lock().unwrap() = Some("the catalog is down".to_owned());
        let command = new_command(&mem);
        let (report, seen) = run_with(&fake, &command, Selection::All, 1).await;
        match outcome_of(&report, &arn) {
            Outcome::Failed(why) => assert!(
                why.contains("could not be read") && why.contains("the catalog is down"),
                "short {short}: {why}"
            ),
            other => panic!("short {short}: {other:?}"),
        }
        assert!(seen[0].in_catalog_kept, "short {short}");
        assert!(legacy_kept(&fake, &arn), "short {short}");
        assert!(copy_of(&mem, &arn).is_empty(), "short {short}");
        assert!(report.stopped.is_none(), "short {short}");
    }
}

/// A walk of a copy an earlier run left stops at the loss the timer
/// records: the store removes one object per step, the holder is taken over
/// after the second removal, and once the beat records the loss no object
/// is removed; the run stops naming the loss, with the rest left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loss_the_timer_records_mid_walk_ends_the_walk() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(1));
    let prefix = prefix_of(&arn);
    for n in 1..=20 {
        mem.insert(&format!("{prefix}data/{n:06}.json.gz"), b"left");
    }
    *mem.delete_one_at_a_time.lock().unwrap() = Some(Duration::from_millis(40));
    let mut command = new_command(&mem);
    command.claim = maintenance::HeldClaim::start(
        Box::new(Forward(Arc::clone(&command.holder))),
        Duration::from_millis(100),
    );
    let watched = Arc::clone(&mem);
    let holder = Arc::clone(&command.holder);
    let taker = tokio::spawn(async move {
        while watched.deletes.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        *holder.lost.lock().unwrap() = true;
    });
    let lost = command.claim.lost_signal().clone();
    let counted = Arc::clone(&mem);
    let at_the_loss = tokio::spawn(async move {
        lost.cancelled().await;
        counted.deletes.lock().unwrap().len()
    });
    let (report, _) = within_twenty_seconds(run(&fake, &command, Selection::All)).await;
    taker.await.unwrap();
    let at_the_loss = at_the_loss.await.unwrap();
    let stopped = report.stopped.clone().expect("the run stops");
    assert!(stopped.contains("another holder took it over"), "{stopped}");
    let removed = mem.deletes.lock().unwrap().len();
    assert_eq!(removed, at_the_loss, "no removal after the recorded loss");
    assert!(removed < 20, "the walk ended part way: {removed}");
    assert_eq!(copy_of(&mem, &arn).len(), 20 - removed);
    assert!(fake.replaces_seen.lock().unwrap().is_empty());
}

/// A put whose answer was lost after the store kept its object (the data
/// file's put fails, its object stored): the key is read back with `head`
/// and the object found is recorded with the store's metadata, so the
/// copy's deletion reaches it with the size the store reports, not a size
/// the put never answered. When the head fails too, the key is left to the
/// next run (no deletion without a condition) and the reason says so.
#[tokio::test]
async fn a_put_whose_answer_was_lost_is_read_back_before_it_is_recorded() {
    for head_fails in [false, true] {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        let arn = legacy_arn(1);
        seed_legacy(&fake, &arn, legacy_items(3));
        let lost = format!("{}data/000002.json.gz", prefix_of(&arn));
        let first = format!("{}data/000001.json.gz", prefix_of(&arn));
        *mem.store_then_fail_puts_of.lock().unwrap() = Some("data/000002.json.gz".to_owned());
        if head_fails {
            *mem.fail_heads_of.lock().unwrap() = Some(lost.clone());
        }
        let command = new_command(&mem);
        let (report, _) = run_with(&fake, &command, Selection::All, 1).await;
        let why = match outcome_of(&report, &arn) {
            Outcome::Failed(why) => why.clone(),
            other => panic!("head fails {head_fails}: {other:?}"),
        };
        assert!(
            why.contains("writing data/000002.json.gz"),
            "head fails {head_fails}: {why}"
        );
        assert!(legacy_kept(&fake, &arn));
        let deleted: Vec<extenddb_backup_store::ObjectMeta> =
            mem.written_deletes.lock().unwrap().clone();
        assert!(deleted.iter().any(|o| o.key == first), "{deleted:?}");
        assert!(!mem.keys().contains(&first), "head fails {head_fails}");
        if head_fails {
            assert!(
                why.contains("could not be read") && why.contains("left to the next run"),
                "{why}"
            );
            assert!(
                !deleted.iter().any(|o| o.key == lost),
                "no deletion of a key the head could not read: {deleted:?}"
            );
            assert!(mem.keys().contains(&lost), "the key is left");
        } else {
            let size = mem
                .put_sizes
                .lock()
                .unwrap()
                .iter()
                .find(|(k, _)| *k == lost)
                .map(|(_, size)| *size)
                .expect("the lost put stored its object");
            let recorded = deleted
                .iter()
                .find(|o| o.key == lost)
                .unwrap_or_else(|| panic!("the stored object is deleted: {deleted:?}"));
            assert_eq!(recorded.size, size, "the head's size, not the put's");
            assert!(!mem.keys().contains(&lost));
        }
    }
}

/// A conversion that did not apply at any attempt (`NotApplied`, a
/// Cassandra LWT whose condition failed each time) fails the backup with
/// its copy deleted and the in-catalog backup kept; the run goes on. In
/// `finish` the same answer fails the backup with the row and the
/// in-catalog backup left.
#[tokio::test]
async fn a_replacement_that_did_not_apply_fails_the_backup_and_keeps_it() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(2));
    seed_legacy(&fake, &legacy_arn(2), legacy_items(1));
    *fake.replace_answer.lock().unwrap() = Some(Replaced::NotApplied(
        "the conversion did not apply after 3 attempts".to_owned(),
    ));
    let command = new_command(&mem);
    let (report, seen) = run_on(&fake, &command, Selection::All, 1 << 26, "cassandra").await;
    match outcome_of(&report, &arn) {
        Outcome::Failed(why) => assert!(
            why.contains("did not apply") && why.contains("the in-catalog backup is unchanged"),
            "{why}"
        ),
        other => panic!("{other:?}"),
    }
    assert!(seen[0].in_catalog_kept);
    assert!(legacy_kept(&fake, &arn));
    assert!(copy_of(&mem, &arn).is_empty(), "the copy is deleted");
    assert!(report.stopped.is_none(), "{report:?}");
    assert!(matches!(
        outcome_of(&report, &legacy_arn(2)),
        Outcome::Migrated { .. }
    ));

    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    seed_legacy(&fake, &arn, legacy_items(2));
    plant(&mem, &arn, a_copy_of(&arn, legacy_items(2)).await);
    fake.rows
        .lock()
        .unwrap()
        .insert(arn.clone(), row(&arn, BackupStatus::Available));
    *fake.replace_answer.lock().unwrap() = Some(Replaced::NotApplied(
        "the conversion did not apply after 3 attempts".to_owned(),
    ));
    let command = new_command(&mem);
    let (report, _) = run_on(&fake, &command, Selection::All, 1 << 26, "cassandra").await;
    match outcome_of(&report, &arn) {
        Outcome::Failed(why) => assert!(
            why.contains("did not apply") && why.contains("the catalog row is left"),
            "{why}"
        ),
        other => panic!("{other:?}"),
    }
    assert!(legacy_kept(&fake, &arn));
    assert!(
        mem.keys().contains(&manifest_key(&arn)),
        "the row's copy is left"
    );
}

/// On Cassandra every `finish` call that finds the row converted writes
/// (its list and status entries, and the item buckets of a conversion that
/// stopped after its transaction), so the backup's outcome carries an
/// audit line; elsewhere a finish with no in-catalog backup left writes
/// nothing.
#[tokio::test]
async fn a_cassandra_finish_counts_as_a_write() {
    for (backend, wrote) in [("cassandra", true), ("sqlite", false)] {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        let arn = legacy_arn(1);
        let mut held = row(&arn, BackupStatus::Available);
        held.owner_generation = UNOWNED_GENERATION;
        fake.rows.lock().unwrap().insert(arn.clone(), held);
        let command = new_command(&mem);
        let (report, _) = run_on(&fake, &command, Selection::Arn(&arn), 1 << 26, backend).await;
        assert_eq!(outcome_of(&report, &arn), &Outcome::InStore, "{backend}");
        assert_eq!(report.backups[0].wrote, wrote, "{backend}");
    }
}

/// The copy's SSE description names the key ARN in the server's region,
/// as the legacy DescribeBackup builds it, whatever region the backup's ARN
/// carries.
#[tokio::test]
async fn the_copys_sse_key_arn_is_in_the_servers_region() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1).replace(REGION, "eu-west-1");
    seed_legacy(&fake, &arn, legacy_items(1));
    fake.sources
        .lock()
        .unwrap()
        .get_mut(&arn)
        .unwrap()
        .definition
        .as_mut()
        .unwrap()
        .sse_specification = Some(json!({"Enabled": true, "SSEType": "KMS"}));
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(report.complete(), "{report:?}");
    let sse = stored_manifest(&mem, &arn)
        .source_table
        .sse_description
        .expect("an SSE description");
    assert_eq!(
        sse.kms_master_key_arn.as_deref(),
        Some(format!("arn:aws:kms:{REGION}:{ACCOUNT}:key/default").as_str())
    );
}

/// DescribeBackup answers the same before and after the migration, but for
/// the two sizes and, for a source that recorded no index, stream, TTL or
/// SSE, `SourceTableFeatureDetails`: omitted by the in-catalog answer, `{}`
/// by the row's (the store-backed shape, which the manifest cannot tell
/// apart). The throughput is the recorded one on both sides: a provisioned
/// GSI that recorded none or 0/0 keeps it, and a backup with no definition
/// and no recorded throughput reports 0/0 rather than the 5/5 its restore
/// falls back to.
#[tokio::test]
async fn describe_backup_answers_the_same_before_and_after_the_migration() {
    let provisioned = |gsi: Option<Option<ProvisionedThroughput>>| {
        let mut d = definition("PROVISIONED");
        if let Some(t) = gsi {
            d.global_secondary_indexes[0].provisioned_throughput = t;
        }
        Some(d)
    };
    let plain = {
        let mut d = definition("PROVISIONED");
        d.global_secondary_indexes.clear();
        d.local_secondary_indexes.clear();
        Some(d)
    };
    let cases: [(
        &str,
        Option<BackupTableDefinition>,
        &str,
        Option<ProvisionedThroughput>,
    ); 8] = [
        (
            "a provisioned GSI with none",
            provisioned(Some(None)),
            "PROVISIONED",
            None,
        ),
        (
            "a provisioned GSI with 0/0",
            provisioned(Some(Some(throughput(0, 0)))),
            "PROVISIONED",
            None,
        ),
        (
            "a provisioned GSI with 3/4",
            provisioned(None),
            "PROVISIONED",
            None,
        ),
        (
            "on demand",
            Some(definition("PAY_PER_REQUEST")),
            "PAY_PER_REQUEST",
            None,
        ),
        ("no index, stream, TTL or SSE", plain, "PROVISIONED", None),
        ("no definition, no throughput", None, "PROVISIONED", None),
        (
            "no definition, 6/7",
            None,
            "PROVISIONED",
            Some(throughput(6, 7)),
        ),
        ("no definition, on demand", None, "PAY_PER_REQUEST", None),
    ];
    for (case, definition, billing, column) in cases {
        let mem = Arc::new(MemStore::default());
        let fake = Fake::default();
        let arn = legacy_arn(1);
        seed_legacy(&fake, &arn, legacy_items(2));
        let empty_features = {
            let mut sources = fake.sources.lock().unwrap();
            let s = sources.get_mut(&arn).unwrap();
            s.billing_mode = billing.to_owned();
            s.provisioned_throughput = match &definition {
                Some(d) => d.provisioned_throughput.clone(),
                None => column,
            };
            s.attribute_definitions = vec![attr("pk"), attr("sk")];
            if definition
                .as_ref()
                .is_some_and(|d| !d.global_secondary_indexes.is_empty())
            {
                s.attribute_definitions.extend([attr("gpk"), attr("lsk")]);
            }
            let empty = definition
                .as_ref()
                .is_none_or(|d| d.global_secondary_indexes.is_empty());
            s.definition = definition;
            let mut desc = backup_description("AVAILABLE", billing);
            desc.backup_details.backup_arn.clone_from(&arn);
            desc.backup_details.backup_name = "nightly".to_owned();
            desc.backup_details.backup_creation_date_time = s.backup_creation_date_time;
            let details = &mut desc.source_table_details;
            details.key_schema.clone_from(&s.key_schema);
            details.item_count = s.item_count;
            details.table_creation_date_time = s.table_creation_date_time;
            fake.descriptions.lock().unwrap().insert(arn.clone(), desc);
            empty
        };
        let describe = || async {
            let mut v = serde_json::to_value(
                describe_backup(&fake, ACCOUNT, REGION, &own(&arn))
                    .await
                    .unwrap(),
            )
            .unwrap();
            // What the migration changes, as the backup manual states.
            v["BackupDetails"]
                .as_object_mut()
                .unwrap()
                .remove("BackupSizeBytes");
            v["SourceTableDetails"]
                .as_object_mut()
                .unwrap()
                .remove("TableSizeBytes");
            v
        };
        let before = describe().await;
        let command = new_command(&mem);
        let (report, _) = run(&fake, &command, Selection::All).await;
        assert!(report.complete(), "{case}: {report:?}");
        assert!(!legacy_kept(&fake, &arn), "{case}");
        let mut after = describe().await;
        if empty_features {
            assert!(
                before.get("SourceTableFeatureDetails").is_none(),
                "{case}: {before}"
            );
            assert_eq!(after["SourceTableFeatureDetails"], json!({}), "{case}");
            after
                .as_object_mut()
                .unwrap()
                .remove("SourceTableFeatureDetails");
        }
        assert_eq!(after, before, "{case}");
        let reported = &after["SourceTableDetails"]["ProvisionedThroughput"];
        let expected = match case {
            "no definition, 6/7" => json!({"ReadCapacityUnits": 6, "WriteCapacityUnits": 7}),
            _ if billing == "PROVISIONED" && !case.starts_with("no definition") => {
                json!({"ReadCapacityUnits": 7, "WriteCapacityUnits": 8})
            }
            _ => json!({"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}),
        };
        assert_eq!(reported, &expected, "{case}");
    }
}

/// `finish` beside an in-catalog backup the replacement leaves (CREATING,
/// or DELETING outside MongoDB, as PostgreSQL's and SQLite's
/// `AlreadyReplaced` leave it): the in-catalog backup is kept and the
/// backup's outcome records no write, so no audit line names a removal that
/// did not happen. An AVAILABLE one is removed and recorded, and on MongoDB
/// a DELETING one too (its `AlreadyReplaced` finishes a replacement that
/// stopped before the drop), so that removal gets its audit line.
#[tokio::test]
async fn a_finish_beside_an_in_catalog_backup_it_leaves_records_no_write() {
    for (status, kept_outside_mongodb) in [
        (BackupStatus::Creating, true),
        (BackupStatus::Deleting, true),
        (BackupStatus::Available, false),
    ] {
        for backend in ["postgres", "sqlite", "mongodb"] {
            let mongodb = backend == "mongodb";
            let kept = kept_outside_mongodb && !(mongodb && status == BackupStatus::Deleting);
            let mem = Arc::new(MemStore::default());
            let fake = Fake::default();
            fake.finishes_deleting
                .store(mongodb, std::sync::atomic::Ordering::Release);
            let arn = legacy_arn(1);
            seed_legacy(&fake, &arn, legacy_items(2));
            fake.legacy_keys.lock().unwrap()[0].status = status;
            if status == BackupStatus::Available {
                plant(&mem, &arn, a_copy_of(&arn, legacy_items(2)).await);
            }
            let mut held = row(&arn, BackupStatus::Available);
            held.owner_generation = UNOWNED_GENERATION;
            fake.rows.lock().unwrap().insert(arn.clone(), held);
            let command = new_command(&mem);
            let (report, _) = run_on(&fake, &command, Selection::Arn(&arn), 1 << 26, backend).await;
            let case = format!("{status} on {backend}");
            assert_eq!(outcome_of(&report, &arn), &Outcome::Finished, "{case}");
            assert_eq!(legacy_kept(&fake, &arn), kept, "{case}: {report:?}");
            assert_eq!(report.backups[0].wrote, !kept, "{case}");
        }
    }
}

/// `BackupStore::delete_written`'s default, for a store with neither a
/// conditional nor a single-key delete: it deletes the key's object, and
/// answers `false` with nothing deleted when an object sits under `key/`,
/// which its `delete_prefix` would also remove.
#[tokio::test]
async fn the_default_delete_written_removes_only_the_key() {
    use extenddb_backup_store::BackupStore;
    struct Plain(MemStore);
    impl BackupStore for Plain {
        fn put<'a>(
            &'a self,
            key: &'a str,
            body: extenddb_backup_store::ByteStream,
        ) -> BoxedFuture<
            'a,
            Result<extenddb_backup_store::ObjectMeta, extenddb_backup_store::StoreError>,
        > {
            self.0.put(key, body)
        }
        fn put_if_absent<'a>(
            &'a self,
            key: &'a str,
            body: extenddb_backup_store::ByteStream,
        ) -> BoxedFuture<
            'a,
            Result<Option<extenddb_backup_store::ObjectMeta>, extenddb_backup_store::StoreError>,
        > {
            self.0.put_if_absent(key, body)
        }
        fn get<'a>(
            &'a self,
            key: &'a str,
        ) -> BoxedFuture<
            'a,
            Result<extenddb_backup_store::ByteStream, extenddb_backup_store::StoreError>,
        > {
            self.0.get(key)
        }
        fn head<'a>(
            &'a self,
            key: &'a str,
        ) -> BoxedFuture<
            'a,
            Result<Option<extenddb_backup_store::ObjectMeta>, extenddb_backup_store::StoreError>,
        > {
            self.0.head(key)
        }
        fn list<'a>(
            &'a self,
            prefix: &'a str,
        ) -> BoxStream<
            'a,
            Result<extenddb_backup_store::ObjectMeta, extenddb_backup_store::StoreError>,
        > {
            self.0.list(prefix)
        }
        fn delete_prefix<'a>(
            &'a self,
            prefix: &'a str,
        ) -> BoxedFuture<'a, Result<u64, extenddb_backup_store::StoreError>> {
            self.0.delete_prefix(prefix)
        }
        fn validate(&self) -> BoxedFuture<'_, Result<(), extenddb_backup_store::StoreError>> {
            self.0.validate()
        }
    }
    let store = Plain(MemStore::default());
    let written = |key: &str| extenddb_backup_store::ObjectMeta {
        key: key.to_owned(),
        size: 1,
        last_modified: None,
        etag: None,
    };
    store.0.insert("p/a", b"a");
    store.0.insert("p/b", b"b");
    store.0.insert("p/b/under", b"u");
    assert!(store.delete_written(&written("p/a")).await.unwrap());
    assert!(!store.delete_written(&written("p/b")).await.unwrap());
    assert!(store.delete_written(&written("p/gone")).await.unwrap());
    assert_eq!(
        store.0.keys(),
        vec!["p/b".to_owned(), "p/b/under".to_owned()]
    );
}
