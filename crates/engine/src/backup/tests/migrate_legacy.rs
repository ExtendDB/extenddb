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
    // An id with no instant (a 0.1.x backup's) keeps the fixture's time.
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
    let identity = store();
    let migration = Migration {
        configured: command.runtime.configured_store().unwrap(),
        identity: &identity,
        claim: &command.claim,
        interrupt: &command.interrupt,
        limits: RestorableLimits::from(&LimitsConfig::default()),
        data_file_target_bytes,
        region: REGION,
        source_backend: "sqlite",
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
/// nothing to the store: an id with no instant (13 digits with a suffix,
/// and bare), a table name the store-prefix rules refuse, a name the
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
        (thirteen, Box::new(|_, _| {}), "carries no instant", false),
        (bare, Box::new(|_, _| {}), "carries no instant", false),
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
/// another row holds is left alone, and a deleted backup whose id carries
/// no instant reads nothing from the store.
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
    assert!(copy_of(&mem, &partial).is_empty());

    let (report, seen) = run(&fake, &command, Selection::Arn(&held)).await;
    assert_eq!(outcome_of(&report, &held), &Outcome::Deleted);
    assert!(seen[0].notes.is_empty(), "{seen:?}");
    assert_eq!(copy_of(&mem, &held).len(), 1, "the row's prefix is left");

    let thirteen =
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/1793254932797-4116998c");
    fake.legacy_keys.lock().unwrap().push(LegacyBackupKey {
        account_id: ACCOUNT.to_owned(),
        backup_arn: thirteen.clone(),
        status: BackupStatus::Deleted,
    });
    let lists_before = mem.keys().len();
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(report.backups.is_empty() && report.complete(), "{report:?}");
    assert_eq!(mem.keys().len(), lists_before);
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
    assert_fenced(&command.runtime);
}

/// MongoDB's replacement that committed and stopped before its drop is
/// finished whatever the catalog index row holds: here the migrated
/// backup was deleted since and its row is gone. `--all` and `<arn>` both
/// finish it, and nothing is copied.
#[tokio::test]
async fn a_replacement_stopped_before_its_end_is_finished_whatever_its_row_holds() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let all = legacy_arn(1);
    let named = legacy_arn(2);
    for arn in [&all, &named] {
        seed_legacy(&fake, arn, legacy_items(1));
        fake.migrated_deleting.lock().unwrap().insert(arn.clone());
    }
    for key in fake.legacy_keys.lock().unwrap().iter_mut() {
        key.status = BackupStatus::Deleting;
    }
    let command = new_command(&mem);
    let (report, _) = run(&fake, &command, Selection::Arn(&named)).await;
    assert_eq!(outcome_of(&report, &named), &Outcome::Finished);
    let (report, _) = run(&fake, &command, Selection::All).await;
    assert!(report.complete(), "{report:?}");
    assert_eq!(outcome_of(&report, &all), &Outcome::Finished);
    assert_eq!(*fake.finished_replacements.lock().unwrap(), [named, all]);
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

/// An `AlreadyExists` answer at the replacement (another row holds the id)
/// refuses the backup and keeps its copy: the bytes may be that row's.
#[tokio::test]
async fn already_exists_at_the_replacement_keeps_the_copy() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(2));
    let other = arn.replace("table/Music/", "table/Other/");
    fake.rows
        .lock()
        .unwrap()
        .insert(other.clone(), row(&other, BackupStatus::Available));
    let command = new_command(&mem);
    let (report, seen) = run(&fake, &command, Selection::All).await;
    assert!(refusal(outcome_of(&report, &arn)).contains("under another ARN"));
    assert!(mem.keys().contains(&manifest_key(&arn)), "the copy is kept");
    assert!(seen[0].notes[0].contains("are left"), "{seen:?}");
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

/// The fence before the deletion of a copy at the backup's end: a root
/// another catalog adopted after the manifest's put, and a replacement
/// that finds the backup deleted, leave the copy with a note.
#[tokio::test]
async fn a_copy_on_a_root_fenced_after_its_manifest_is_left_with_a_note() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let arn = legacy_arn(1);
    seed_legacy(&fake, &arn, legacy_items(2));
    *fake.replace_answer.lock().unwrap() = Some(Replaced::NotAvailable(
        "the in-catalog backup is DELETED".to_owned(),
    ));
    let command = new_command(&mem);
    let weak = Arc::downgrade(&mem);
    let manifest = manifest_key(&arn);
    *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
        if key == manifest
            && let Some(mem) = weak.upgrade()
        {
            mem.insert(STORE_IDENTITY_KEY, &adopted_file());
        }
    }));
    let (report, seen) = run(&fake, &command, Selection::All).await;
    assert!(
        matches!(outcome_of(&report, &arn), Outcome::Skipped(_)),
        "{report:?}"
    );
    assert!(mem.keys().contains(&manifest_key(&arn)), "the copy is left");
    assert!(
        seen[0].notes.iter().any(|n| n.contains("was not deleted")),
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
    assert!(stopped.contains("the next run deletes it"), "{stopped}");
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
