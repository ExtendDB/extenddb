// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The runner, the status and the checksum rules on a fake backend with a
//! catalog store and two data stores. Expected sequences are built from
//! [`MigrationId::ALL`], so a new id needs no edit here beyond the fake
//! table's wildcard arm.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use super::*;

pub(super) const CATALOG_FILE: &str = "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);\n";
const DATA_FILE: &str = "CREATE TABLE items (pk TEXT PRIMARY KEY);\n";

/// What code bodies see: the items `detect` reports, and a log.
#[derive(Default)]
pub(super) struct FakeStores {
    attention: Mutex<Vec<String>>,
    runs: Mutex<Vec<String>>,
}

pub(super) struct FakeCode(pub(super) &'static str);

#[async_trait]
impl CodeBody<FakeStores> for FakeCode {
    fn fingerprint(&self) -> &'static str {
        self.0
    }
    async fn detect(&self, stores: &FakeStores) -> Result<Vec<String>, MigrationError> {
        Ok(stores.attention.lock().unwrap().clone())
    }
    async fn run(&self, stores: &FakeStores) -> Result<Done, MigrationError> {
        stores.runs.lock().unwrap().push(self.0.to_owned());
        Ok(Done {
            notes: vec![format!("{} ran", self.0)],
        })
    }
}

pub(super) static CODE_2: FakeCode = FakeCode("0002_resource_tag_condition_keys/fake/1");
static CODE_4: FakeCode = FakeCode("0004_index_table_base_key/fake/1");

/// 1: a file in the catalog and in each data store. 2: code in the catalog.
/// 3: not applicable. 4: code in each data store, not convergent. Every
/// later id: not applicable.
#[allow(unreachable_patterns)]
pub(super) fn table(id: MigrationId) -> Migration<FakeStores> {
    match id {
        MigrationId::Baseline => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![
                StoreBody::file(StoreKind::Catalog, "0001_baseline.sql", CATALOG_FILE),
                StoreBody::file(StoreKind::Data, "0001_baseline.sql", DATA_FILE),
            ],
        },
        MigrationId::ResourceTagConditionKeys => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(StoreKind::Catalog, &CODE_2, "source 2")],
        },
        MigrationId::SortKeyDefinitions => Migration::NotApplicable("the fake has no sort keys"),
        MigrationId::IndexTableBaseKey => Migration::Applies {
            kind: Kind::Breaking,
            convergent: false,
            bodies: vec![StoreBody::code(StoreKind::Data, &CODE_4, "source 4")],
        },
        _ => Migration::NotApplicable("a later id the fake does not model"),
    }
}

/// The fake's store instances of each kind, in the order the runner applies
/// them.
fn instances(kind: StoreKind) -> &'static [&'static str] {
    match kind {
        StoreKind::Catalog => &["cat"],
        StoreKind::Data => &["data-a", "data-b"],
        StoreKind::Account => &[],
    }
}

/// The writes a run makes for `id`: each store's body, catalog first, then
/// the version row when the id's last body ran outside the catalog.
fn writes_for(id: MigrationId) -> Vec<String> {
    let kinds = table(id).store_kinds();
    let mut out: Vec<String> = kinds
        .iter()
        .flat_map(|kind| instances(*kind))
        .map(|store| format!("{} {store}", id.number()))
        .collect();
    if kinds.last() != Some(&StoreKind::Catalog) {
        out.push(format!("version {}", id.number()));
    }
    out
}

/// The ids whose bodies touch `store`.
fn ids_in(store: &str) -> Vec<u32> {
    MigrationId::ALL
        .into_iter()
        .filter(|id| {
            table(*id)
                .store_kinds()
                .iter()
                .any(|kind| instances(*kind).contains(&store))
        })
        .map(MigrationId::number)
        .collect()
}

/// The number of steps a run applies for the ids after `after`.
pub(super) fn steps_after(after: u32) -> usize {
    MigrationId::ALL
        .into_iter()
        .filter(|id| id.number() > after)
        .flat_map(|id| table(id).store_kinds())
        .map(|kind| instances(kind).len())
        .sum()
}

pub(super) const LATEST: u32 = MigrationId::LATEST.number();

#[derive(Default)]
pub(super) struct State {
    pub(super) ledgers: BTreeMap<String, Vec<LedgerRow>>,
    pub(super) traces: CatalogTraces,
    /// Whether the catalog store has tables.
    tables: bool,
    writes: Vec<String>,
    fail_apply: Option<(u32, String)>,
    fail_read: bool,
}

pub(super) struct Fake {
    stores: FakeStores,
    state: Mutex<State>,
    released: &'static [Released],
}

impl Fake {
    pub(super) fn new() -> Self {
        Self::released(&[])
    }
    pub(super) fn released(released: &'static [Released]) -> Self {
        Self {
            stores: FakeStores::default(),
            state: Mutex::new(State::default()),
            released,
        }
    }
    fn writes(&self) -> Vec<String> {
        self.state.lock().unwrap().writes.clone()
    }
    pub(super) fn rows(&self, store: &str) -> Vec<u32> {
        let state = self.state.lock().unwrap();
        let mut ids: Vec<u32> = state
            .ledgers
            .get(store)
            .map(|rows| rows.iter().map(|r| r.id).collect())
            .unwrap_or_default();
        ids.sort_unstable();
        ids
    }
    pub(super) fn ledger(&self, store: &str) -> Vec<LedgerRow> {
        self.state.lock().unwrap().ledgers[store].clone()
    }
    pub(super) fn version_row(&self) -> Option<String> {
        self.state.lock().unwrap().traces.catalog_version.clone()
    }
    pub(super) fn with_state(&self, f: impl FnOnce(&mut State)) {
        f(&mut self.state.lock().unwrap());
    }
    pub(super) fn add_catalog_row(&self, id: u32, kind: Kind) {
        self.with_state(|s| {
            s.ledgers.entry("cat".into()).or_default().push(LedgerRow {
                id,
                name: format!("future_{id}"),
                kind,
                checksum: "x".into(),
                outcome: Outcome::Applied,
                notes: String::new(),
                applied_at: String::new(),
                applied_by: "9.9.9".into(),
            });
        });
    }
}

#[async_trait]
impl MigrationBackend for Fake {
    type Stores = FakeStores;

    fn backend_name(&self) -> &'static str {
        "fake"
    }
    fn migration(&self, id: MigrationId) -> Migration<FakeStores> {
        table(id)
    }
    fn released(&self) -> &'static [Released] {
        self.released
    }
    fn stores(&self) -> &FakeStores {
        &self.stores
    }
    async fn store_names(&self, kind: StoreKind) -> Result<Vec<String>, MigrationError> {
        let mut names: Vec<String> = instances(kind).iter().map(|s| (*s).to_owned()).collect();
        names.reverse();
        Ok(names)
    }
    async fn read_ledger(&self, store: &StoreRef) -> Result<Vec<LedgerRow>, MigrationError> {
        let state = self.state.lock().unwrap();
        if state.fail_read {
            return Err(MigrationError::Store("permission denied".into()));
        }
        Ok(state.ledgers.get(&store.name).cloned().unwrap_or_default())
    }
    async fn read_traces(&self) -> Result<CatalogTraces, MigrationError> {
        Ok(self.state.lock().unwrap().traces.clone())
    }
    async fn catalog_is_empty(&self) -> Result<bool, MigrationError> {
        Ok(!self.state.lock().unwrap().tables)
    }
    async fn before_first_write(
        &self,
        from: Option<u32>,
        to: u32,
    ) -> Result<Vec<String>, MigrationError> {
        let from = from.map_or("legacy".to_owned(), |v| v.to_string());
        let line = format!("copy {from}-{to}");
        self.with_state(|s| s.writes.push(line.clone()));
        Ok(vec![line])
    }
    async fn apply(
        &self,
        store: &StoreRef,
        id: MigrationId,
        step: Step<'_, FakeStores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, MigrationError> {
        let failing = self.state.lock().unwrap().fail_apply.clone();
        if failing == Some((id.number(), store.name.clone())) {
            return Err(MigrationError::Store("killed".into()));
        }
        let done = match step {
            Step::Code(body) => body.run(&self.stores).await?,
            Step::File { .. } | Step::Record => Done::default(),
        };
        self.with_state(|s| {
            if let Some(mut row) = row {
                row.notes = done.notes.join("\n");
                let rows = s.ledgers.entry(store.name.clone()).or_default();
                assert!(
                    rows.iter().all(|r| r.id != row.id),
                    "a row is written only where none exists"
                );
                rows.push(row);
            }
            s.writes.push(format!("{} {}", id.number(), store.name));
            if let Some(v) = catalog_version {
                s.traces.catalog_version = Some(v.to_string());
            }
            if store.kind == StoreKind::Catalog {
                s.tables = true;
                if id == MigrationId::Baseline {
                    s.traces.resource_tag_marker = false;
                }
            }
        });
        Ok(done)
    }
    async fn write_catalog_version(&self, version: u32) -> Result<(), MigrationError> {
        self.with_state(|s| {
            s.traces.catalog_version = Some(version.to_string());
            s.writes.push(format!("version {version}"));
        });
        Ok(())
    }
    async fn delete_ledger_row(&self, store: &StoreRef, id: u32) -> Result<(), MigrationError> {
        self.with_state(|s| {
            if let Some(rows) = s.ledgers.get_mut(&store.name) {
                rows.retain(|r| r.id != id);
            }
        });
        Ok(())
    }
}

pub(super) fn runner(fake: &Fake) -> Runner<'_, Fake> {
    Runner::new(fake, "1.0.0-test")
}

#[tokio::test]
async fn a_fresh_store_gets_every_id_in_order_catalog_first() {
    let fake = Fake::new();
    assert_eq!(
        runner(&fake).status().await.unwrap(),
        CatalogStatus::Uninitialized
    );
    let report = runner(&fake).migrate().await.unwrap();
    let mut expected = vec![format!("copy legacy-{LATEST}")];
    expected.extend(MigrationId::ALL.into_iter().flat_map(writes_for));
    assert_eq!(fake.writes(), expected);
    assert_eq!((report.from, report.to), (None, Some(LATEST)));
    assert_eq!(report.notes, [format!("copy legacy-{LATEST}")]);
    assert_eq!(fake.rows("cat"), (1..=LATEST).collect::<Vec<_>>());
    assert_eq!(fake.rows("data-a"), ids_in("data-a"));
    assert_eq!(fake.version_row(), Some(LATEST.to_string()));
    assert_eq!(
        runner(&fake).status().await.unwrap(),
        CatalogStatus::Current { version: LATEST }
    );
    let again = runner(&fake).migrate().await.unwrap();
    assert!(again.applied.is_empty());
    assert_eq!(
        fake.writes().len(),
        expected.len(),
        "a current catalog is not written"
    );
}

#[tokio::test]
async fn a_crash_between_stores_resumes_at_the_first_store_without_a_row() {
    let fake = Fake::new();
    fake.with_state(|s| s.fail_apply = Some((1, "data-b".into())));
    let err = runner(&fake).migrate().await.unwrap_err();
    assert_eq!(err.exit_code(), exit::ERROR);
    // The run had taken its copy and committed two steps, and says so.
    let MigrationError::Stopped { report, .. } = &err else {
        panic!("expected Stopped: {err:?}");
    };
    assert_eq!(report.applied.len(), 2);
    assert_eq!(report.notes, [format!("copy legacy-{LATEST}")]);
    assert_eq!(fake.rows("cat"), [1]);
    assert_eq!(fake.rows("data-a"), [1]);
    // The catalog row alone does not count id 1 as done: the ledger exists
    // and is at migration 0.
    assert_eq!(fake.version_row().as_deref(), Some("0"));
    let CatalogStatus::Behind { version, pending } = runner(&fake).status().await.unwrap() else {
        panic!("expected Behind");
    };
    assert_eq!(version, Some(0));
    assert_eq!(
        (pending[0].id, pending[0].store_name.as_str()),
        (1, "data-b")
    );

    fake.with_state(|s| s.fail_apply = None);
    runner(&fake).migrate().await.unwrap();
    let writes = fake.writes();
    let resumed: Vec<&str> = writes.iter().skip(3).map(String::as_str).collect();
    let mut expected = vec![
        format!("copy 0-{LATEST}"),
        "1 data-b".to_owned(),
        "version 1".to_owned(),
    ];
    expected.extend(MigrationId::ALL.into_iter().skip(1).flat_map(writes_for));
    assert_eq!(resumed, expected);
}

#[tokio::test]
async fn detect_runs_for_every_pending_code_body_before_any_write() {
    let fake = Fake::new();
    fake.with_state(|s| {
        s.traces.catalog_version = Some("0.0.3".into());
        s.tables = true;
    });
    *fake.stores.attention.lock().unwrap() = vec!["policy p collides".into()];
    let err = runner(&fake).migrate().await.unwrap_err();
    let MigrationError::Status(CatalogStatus::NeedsAttention { items }) = &err else {
        panic!("{err:?}");
    };
    // Both pending code bodies (migrations 2 and 4 of the fake) report, once
    // each, before anything runs.
    assert_eq!(
        items,
        &[
            "migration 2 resource_tag_condition_keys: policy p collides",
            "migration 4 index_table_base_key: policy p collides"
        ]
    );
    assert_eq!(err.exit_code(), exit::ATTENTION);
    assert!(
        fake.writes().is_empty(),
        "nothing is written, not even the copy"
    );
    assert!(fake.stores.runs.lock().unwrap().is_empty());
}

/// A store with tables but neither a ledger nor a version row (a version
/// row deleted by hand) is not a catalog by the status's rule, and `detect`
/// still runs before the first write, because the store has tables.
#[tokio::test]
async fn detect_runs_on_a_store_with_tables_and_no_version_row() {
    let fake = Fake::new();
    fake.with_state(|s| s.tables = true);
    *fake.stores.attention.lock().unwrap() = vec!["policy p collides".into()];
    assert_eq!(
        runner(&fake).status().await.unwrap(),
        CatalogStatus::Uninitialized
    );
    let err = runner(&fake).migrate().await.unwrap_err();
    assert_eq!(err.exit_code(), exit::ATTENTION, "{err:?}");
    assert!(fake.writes().is_empty());

    // With no tables at all there is nothing to find: `detect` is not run,
    // and the run applies every id.
    let fake = Fake::new();
    *fake.stores.attention.lock().unwrap() = vec!["policy p collides".into()];
    let report = runner(&fake).migrate().await.unwrap();
    assert_eq!(report.to, Some(LATEST));
}

#[tokio::test]
async fn the_version_row_follows_the_ledger_through_a_partial_run() {
    let fake = Fake::new();
    runner(&fake)
        .migrate_through(MigrationId::ResourceTagConditionKeys)
        .await
        .unwrap();
    assert_eq!(fake.version_row().as_deref(), Some("2"));
    let CatalogStatus::Behind { version, pending } = runner(&fake).status().await.unwrap() else {
        panic!("expected Behind");
    };
    assert_eq!(version, Some(2));
    let ids: Vec<u32> = pending.iter().map(|p| p.id).collect();
    let expected: Vec<u32> = MigrationId::ALL
        .into_iter()
        .filter(|id| id.number() > 2)
        .flat_map(|id| {
            let n = table(id)
                .store_kinds()
                .iter()
                .map(|kind| instances(*kind).len())
                .sum::<usize>();
            std::iter::repeat_n(id.number(), n)
        })
        .collect();
    assert_eq!(ids, expected);
    assert_eq!(pending[0].kind, Kind::Additive, "NotApplicable is additive");
}

#[tokio::test]
async fn a_ledger_read_error_stops_instead_of_counting_as_not_applied() {
    let fake = Fake::new();
    fake.with_state(|s| s.fail_read = true);
    assert!(runner(&fake).status().await.is_err());
    assert!(runner(&fake).migrate().await.is_err());
    assert!(fake.writes().is_empty());
}

pub(super) async fn current(fake: &Fake) {
    runner(fake).migrate().await.unwrap();
}

#[tokio::test]
async fn every_status_and_its_exit_code() {
    // Legacy: a dotted version and no ledger.
    let fake = Fake::new();
    fake.with_state(|s| s.traces.catalog_version = Some("0.0.3".into()));
    let status = runner(&fake).status().await.unwrap();
    assert!(
        matches!(status, CatalogStatus::Behind { version: None, .. }),
        "{status:?}"
    );
    assert_eq!(status.exit_code(), exit::PENDING);

    // A pending code body with items is NeedsAttention, ahead of Behind.
    *fake.stores.attention.lock().unwrap() = vec!["collision".into()];
    let status = runner(&fake).status().await.unwrap();
    assert_eq!(status.label(), "NeedsAttention");
    assert_eq!(status.exit_code(), exit::ATTENTION);

    // Current, then ahead by an additive id, then by a breaking one.
    let fake = Fake::new();
    current(&fake).await;
    fake.add_catalog_row(LATEST + 1, Kind::Additive);
    let status = runner(&fake).status().await.unwrap();
    assert_eq!(status.label(), "AheadCompatible");
    assert_eq!(status.exit_code(), exit::OK);
    assert!(runner(&fake).migrate().await.unwrap().applied.is_empty());
    fake.add_catalog_row(LATEST + 2, Kind::Breaking);
    let status = runner(&fake).status().await.unwrap();
    assert_eq!(status.label(), "AheadIncompatible");
    assert_eq!(status.exit_code(), exit::AHEAD_INCOMPATIBLE);
    assert_eq!(
        runner(&fake).migrate().await.unwrap_err().exit_code(),
        exit::AHEAD_INCOMPATIBLE
    );

    // Behind wins over AheadCompatible.
    let fake = Fake::new();
    current(&fake).await;
    fake.add_catalog_row(LATEST + 1, Kind::Additive);
    fake.with_state(|s| s.ledgers.get_mut("data-a").unwrap().retain(|r| r.id != 4));
    assert_eq!(runner(&fake).status().await.unwrap().label(), "Behind");

    // A release before 1.0 wrote to it: a dotted version, or the marker.
    let fake = Fake::new();
    current(&fake).await;
    fake.with_state(|s| s.traces.catalog_version = Some("0.0.3".into()));
    let status = runner(&fake).status().await.unwrap();
    assert_eq!(status.label(), "WrittenByOlderRelease");
    assert_eq!(status.exit_code(), exit::ATTENTION);
    fake.with_state(|s| {
        s.traces.catalog_version = Some(LATEST.to_string());
        s.traces.resource_tag_marker = true;
    });
    assert_eq!(
        runner(&fake).status().await.unwrap().label(),
        "WrittenByOlderRelease"
    );
    // ... which ranks above AheadIncompatible, and below Changed.
    fake.add_catalog_row(LATEST + 2, Kind::Breaking);
    assert_eq!(
        runner(&fake).status().await.unwrap().label(),
        "WrittenByOlderRelease"
    );
    assert!(runner(&fake).migrate().await.is_err());
    fake.with_state(|s| s.ledgers.get_mut("data-b").unwrap()[0].checksum = "edited".into());
    let status = runner(&fake).status().await.unwrap();
    let CatalogStatus::Changed { id, store_name, .. } = &status else {
        panic!("{status:?}");
    };
    assert_eq!((*id, store_name.as_str()), (1, "data-b"));
    assert_eq!(status.exit_code(), exit::ATTENTION);
    assert_eq!(CatalogStatus::Uninitialized.exit_code(), exit::ERROR);
}

/// A breaking id ahead of the binary ranks above a pending code body whose
/// `detect` reports an item.
#[tokio::test]
async fn ahead_incompatible_ranks_above_needs_attention() {
    let fake = Fake::new();
    current(&fake).await;
    fake.with_state(|s| s.ledgers.get_mut("data-a").unwrap().retain(|r| r.id != 4));
    *fake.stores.attention.lock().unwrap() = vec!["collision".into()];
    assert_eq!(
        runner(&fake).status().await.unwrap().label(),
        "NeedsAttention"
    );
    fake.add_catalog_row(LATEST + 1, Kind::Breaking);
    let status = runner(&fake).status().await.unwrap();
    assert_eq!(status.label(), "AheadIncompatible", "{status:?}");
    let err = runner(&fake).migrate().await.unwrap_err();
    assert_eq!(err.exit_code(), exit::AHEAD_INCOMPATIBLE, "{err:?}");
}

#[tokio::test]
async fn a_changed_checksum_stops_the_run() {
    let fake = Fake::new();
    current(&fake).await;
    fake.with_state(|s| s.ledgers.get_mut("cat").unwrap()[0].checksum = "old".into());
    let err = runner(&fake).migrate().await.unwrap_err();
    assert_eq!(err.exit_code(), exit::ATTENTION);
}
