// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The runner: applies a backend's pending bodies in order and computes the
//! catalog status from the ledgers.

use super::backend::{MigrationBackend, Step, StoreRef};
use super::checksum::{accepted_checksums, record_checksum};
use super::code::Done;
use super::ledger::{CatalogTraces, LedgerRow, Outcome};
use super::registry::{MigrationId, RELEASED_THROUGH};
use super::status::{AppliedStep, CatalogStatus, MigrationError, Pending, RunReport};
use super::table::{Body, Kind, Migration, StoreKind};

/// Every store's ledger, catalog first, and the catalog's pre-1.0 traces.
#[derive(Debug, Clone)]
struct Snapshot {
    ledgers: Vec<(StoreRef, Vec<LedgerRow>)>,
    traces: CatalogTraces,
}

impl Snapshot {
    fn catalog(&self) -> &StoreRef {
        &self.ledgers[0].0
    }

    fn catalog_rows(&self) -> &[LedgerRow] {
        &self.ledgers[0].1
    }

    fn has_row(&self, store: &StoreRef, id: u32) -> bool {
        self.ledgers
            .iter()
            .any(|(s, rows)| s == store && rows.iter().any(|r| r.id == id))
    }

    fn instances(&self, kind: StoreKind) -> impl Iterator<Item = &StoreRef> {
        self.ledgers
            .iter()
            .map(|(s, _)| s)
            .filter(move |s| s.kind == kind)
    }

    fn put_row(&mut self, store: &StoreRef, row: LedgerRow) {
        if let Some((_, rows)) = self.ledgers.iter_mut().find(|(s, _)| s == store) {
            rows.retain(|r| r.id != row.id);
            rows.push(row);
        }
    }

    /// Whether the store is a catalog: it has a ledger row, or a
    /// `catalog_version` row a release before 1.0 wrote.
    fn initialized(&self) -> bool {
        !self.catalog_rows().is_empty() || self.traces.catalog_version.is_some()
    }
}

/// A step the run will apply.
struct Planned {
    id: MigrationId,
    store: StoreRef,
    kind: Kind,
    /// The store already has a row for the id (a reconverge reruns it).
    recorded: bool,
}

/// Applies a backend's migrations. See the module documentation of
/// [`crate::migrations`].
pub struct Runner<'a, B: MigrationBackend> {
    backend: &'a B,
    applied_by: String,
    released_through: u32,
}

impl<'a, B: MigrationBackend> Runner<'a, B> {
    /// A runner that records `applied_by` (the release) in every row.
    pub fn new(backend: &'a B, applied_by: impl Into<String>) -> Self {
        Self {
            backend,
            applied_by: applied_by.into(),
            released_through: RELEASED_THROUGH,
        }
    }

    /// Use another released bound than [`RELEASED_THROUGH`].
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_released_through(mut self, released_through: u32) -> Self {
        self.released_through = released_through;
        self
    }

    /// The catalog status `serve`, `migrate` and `verify` act on. Read-only:
    /// runs `detect` for pending code bodies and writes nothing.
    ///
    /// # Errors
    ///
    /// A ledger or a detect could not be read.
    pub async fn status(&self) -> Result<CatalogStatus, MigrationError> {
        let snap = self.read().await?;
        self.status_of(&snap).await
    }

    /// Apply every pending migration this binary carries.
    ///
    /// # Errors
    ///
    /// The status forbids the run (`Changed`, `WrittenByOlderRelease`,
    /// `AheadIncompatible`, `NeedsAttention`), or a body failed. A failed body
    /// leaves the earlier ones committed with their rows; the next run
    /// resumes at the first store without a row.
    pub async fn migrate(&self) -> Result<RunReport, MigrationError> {
        self.run(MigrationId::LATEST, false).await
    }

    /// Apply pending migrations up to and including `id`, for tests that need
    /// a catalog at an intermediate migration.
    ///
    /// # Errors
    ///
    /// As [`Runner::migrate`].
    #[cfg(any(test, feature = "test-util"))]
    pub async fn migrate_through(&self, id: MigrationId) -> Result<RunReport, MigrationError> {
        self.run(id, false).await
    }

    /// Run every migration again, repairing a catalog a release before 1.0
    /// wrote to (`WrittenByOlderRelease`): the convergent bodies rerun, the
    /// `catalog_version` row is written again, and the ledger rows already
    /// there stay as they are, with their notes and `applied_at`. A store
    /// without a row for an id gets one, as in [`Runner::migrate`].
    ///
    /// # Errors
    ///
    /// The ledger holds an id that is not convergent or that this binary
    /// does not carry, or as [`Runner::migrate`].
    pub async fn reconverge(&self) -> Result<RunReport, MigrationError> {
        self.run(MigrationId::LATEST, true).await
    }

    /// Delete the ledger rows of an unreleased id, so a developer's database
    /// whose body changed during review runs it again.
    ///
    /// # Errors
    ///
    /// `id` is at or below the released bound, this binary does not carry
    /// it, no store has a row for it, or a store failed.
    pub async fn forget(&self, id: u32) -> Result<(), MigrationError> {
        if id <= self.released_through {
            return Err(MigrationError::Refused(format!(
                "migration {id} has been released; only migrations above {} can be forgotten",
                self.released_through
            )));
        }
        if MigrationId::from_number(id).is_none() {
            return Err(MigrationError::Refused(format!(
                "this binary carries no migration {id}; it carries migrations 1 to {}",
                MigrationId::LATEST.number()
            )));
        }
        let mut snap = self.read().await?;
        if !snap
            .ledgers
            .iter()
            .any(|(_, rows)| rows.iter().any(|r| r.id == id))
        {
            return Err(MigrationError::Refused(format!(
                "no ledger has a row for migration {id}, so there is nothing to forget"
            )));
        }
        for (store, rows) in &mut snap.ledgers {
            if rows.iter().any(|r| r.id == id) {
                self.backend.delete_ledger_row(store, id).await?;
                rows.retain(|r| r.id != id);
            }
        }
        if let Some(version) = self.version_of(&snap) {
            self.backend.write_catalog_version(version).await?;
        }
        Ok(())
    }

    /// The catalog store's ledger rows, in id order. Read-only.
    ///
    /// # Errors
    ///
    /// A ledger could not be read.
    pub async fn catalog_ledger(&self) -> Result<Vec<LedgerRow>, MigrationError> {
        let snap = self.read().await?;
        let mut rows = snap.catalog_rows().to_vec();
        rows.sort_by_key(|r| r.id);
        Ok(rows)
    }

    async fn read(&self) -> Result<Snapshot, MigrationError> {
        let mut kinds = vec![StoreKind::Catalog];
        for id in MigrationId::ALL {
            kinds.extend(self.backend.migration(id).store_kinds());
        }
        kinds.sort_unstable();
        kinds.dedup();
        let mut ledgers = Vec::new();
        for kind in kinds {
            let mut names = self.backend.store_names(kind).await?;
            names.sort_unstable();
            if kind == StoreKind::Catalog && names.len() != 1 {
                return Err(MigrationError::Store(format!(
                    "{} reported {} catalog stores; expected one",
                    self.backend.backend_name(),
                    names.len()
                )));
            }
            for name in names {
                let store = StoreRef { kind, name };
                let rows = self.backend.read_ledger(&store).await?;
                ledgers.push((store, rows));
            }
        }
        let traces = self.backend.read_traces().await?;
        Ok(Snapshot { ledgers, traces })
    }

    /// The highest id such that it and every lower id have a catalog row and
    /// a row in every store their bodies touch: `None` for a catalog store
    /// with no ledger rows at all, `Some(0)` for a ledger without migration 1.
    fn version_of(&self, snap: &Snapshot) -> Option<u32> {
        if snap.catalog_rows().is_empty() {
            return None;
        }
        let highest = snap.catalog_rows().iter().map(|r| r.id).max().unwrap_or(0);
        let mut version = Some(0);
        for number in 1..=highest {
            if !snap.has_row(snap.catalog(), number) {
                break;
            }
            if let Some(id) = MigrationId::from_number(number) {
                let complete = self
                    .backend
                    .migration(id)
                    .store_kinds()
                    .into_iter()
                    .all(|kind| {
                        snap.instances(kind)
                            .collect::<Vec<_>>()
                            .into_iter()
                            .all(|store| snap.has_row(store, number))
                    });
                if !complete {
                    break;
                }
            }
            version = Some(number);
        }
        version
    }

    /// Every store without a row for an id up to `target`, in order: by id,
    /// then catalog first. With `rerun`, every store, rows or not.
    fn planned(&self, snap: &Snapshot, target: MigrationId, rerun: bool) -> Vec<Planned> {
        let mut steps = Vec::new();
        for id in MigrationId::ALL.into_iter().filter(|id| *id <= target) {
            let migration = self.backend.migration(id);
            for kind in migration.store_kinds() {
                for store in snap.instances(kind) {
                    let recorded = snap.has_row(store, id.number());
                    if rerun || !recorded {
                        steps.push(Planned {
                            id,
                            store: store.clone(),
                            kind: migration.kind(),
                            recorded,
                        });
                    }
                }
            }
        }
        steps
    }

    fn changed(&self, snap: &Snapshot) -> Option<CatalogStatus> {
        for (store, rows) in &snap.ledgers {
            for row in rows {
                let Some(id) = MigrationId::from_number(row.id) else {
                    continue;
                };
                let accepted = accepted_checksums(
                    self.backend.released(),
                    id,
                    &self.backend.migration(id),
                    store.kind,
                );
                if !accepted.contains(&row.checksum) {
                    return Some(CatalogStatus::Changed {
                        id: row.id,
                        store_name: store.name.clone(),
                        recorded: row.checksum.clone(),
                        expected: accepted.first().cloned().unwrap_or_else(|| {
                            format!("no body for the {} store", store.kind.as_str())
                        }),
                    });
                }
            }
        }
        None
    }

    fn older_release_trace(&self, snap: &Snapshot) -> Option<String> {
        if !snap.has_row(snap.catalog(), MigrationId::Baseline.number()) {
            return None;
        }
        if snap.traces.dotted_version() {
            return Some(format!(
                "catalog_version is {}",
                snap.traces.catalog_version.as_deref().unwrap_or_default()
            ));
        }
        snap.traces
            .resource_tag_marker
            .then(|| "the resource_tag_migration settings row exists".to_owned())
    }

    fn unknown(&self, snap: &Snapshot) -> Vec<(u32, String, Kind)> {
        snap.catalog_rows()
            .iter()
            .filter(|r| r.id > MigrationId::LATEST.number())
            .map(|r| (r.id, r.name.clone(), r.kind))
            .collect()
    }

    /// The first status that forbids a run, in the order the status is
    /// decided: `Changed`, then `WrittenByOlderRelease` (which a reconverge
    /// repairs), then `AheadIncompatible`.
    fn forbidding(&self, snap: &Snapshot, reconverge: bool) -> Option<CatalogStatus> {
        if let Some(changed) = self.changed(snap) {
            return Some(changed);
        }
        if !reconverge && let Some(trace) = self.older_release_trace(snap) {
            return Some(CatalogStatus::WrittenByOlderRelease { trace });
        }
        let unknown = self.unknown(snap);
        unknown
            .iter()
            .any(|(_, _, kind)| *kind == Kind::Breaking)
            .then(|| CatalogStatus::AheadIncompatible {
                unknown: unknown
                    .into_iter()
                    .map(|(id, name, _)| (id, name))
                    .collect(),
            })
    }

    /// Run `detect` for every pending code body, once per id and kind of
    /// store, and collect the items.
    async fn detect(&self, steps: &[Planned]) -> Result<Vec<String>, MigrationError> {
        let mut seen: Vec<(MigrationId, StoreKind)> = Vec::new();
        let mut items = Vec::new();
        for step in steps {
            if seen.contains(&(step.id, step.store.kind)) {
                continue;
            }
            seen.push((step.id, step.store.kind));
            let migration = self.backend.migration(step.id);
            let Some(store_body) = migration
                .bodies()
                .iter()
                .find(|b| b.store == step.store.kind)
            else {
                continue;
            };
            if let Body::Code { body, .. } = &store_body.body {
                for item in body.detect(self.backend.stores()).await? {
                    items.push(format!("migration {}: {item}", step.id));
                }
            }
        }
        Ok(items)
    }

    /// The status, in this order: `Uninitialized`, then `Changed`,
    /// `WrittenByOlderRelease`, `AheadIncompatible`, `NeedsAttention`,
    /// `Behind`, `AheadCompatible` and `Current`.
    async fn status_of(&self, snap: &Snapshot) -> Result<CatalogStatus, MigrationError> {
        if !snap.initialized() {
            return Ok(CatalogStatus::Uninitialized);
        }
        if let Some(status) = self.forbidding(snap, false) {
            return Ok(status);
        }
        let steps = self.planned(snap, MigrationId::LATEST, false);
        let items = self.detect(&steps).await?;
        if !items.is_empty() {
            return Ok(CatalogStatus::NeedsAttention { items });
        }
        let version = self.version_of(snap);
        if !steps.is_empty() {
            let pending = steps
                .iter()
                .map(|s| Pending {
                    id: s.id.number(),
                    name: s.id.name(),
                    store: s.store.kind,
                    store_name: s.store.name.clone(),
                    kind: s.kind,
                })
                .collect();
            return Ok(CatalogStatus::Behind { version, pending });
        }
        let unknown = self.unknown(snap);
        if !unknown.is_empty() {
            return Ok(CatalogStatus::AheadCompatible {
                version: version.unwrap_or_default(),
                unknown: unknown
                    .into_iter()
                    .map(|(id, name, _)| (id, name))
                    .collect(),
            });
        }
        Ok(CatalogStatus::Current {
            version: version.unwrap_or_default(),
        })
    }

    async fn run(
        &self,
        target: MigrationId,
        reconverge: bool,
    ) -> Result<RunReport, MigrationError> {
        let mut snap = self.read().await?;
        let from = self.version_of(&snap);
        if let Some(status) = self.forbidding(&snap, reconverge) {
            return Err(MigrationError::Status(status));
        }
        if reconverge {
            self.refuse_non_convergent(&snap)?;
        }
        let steps = self.planned(&snap, target, reconverge);
        if steps.is_empty() {
            return Ok(RunReport {
                from,
                to: from,
                ..RunReport::default()
            });
        }
        // A catalog store with no tables at all holds nothing a body could
        // find, and the tables `detect` reads do not exist in it.
        let items = if self.backend.catalog_is_empty().await? {
            Vec::new()
        } else {
            self.detect(&steps).await?
        };
        if !items.is_empty() {
            return Err(MigrationError::Status(CatalogStatus::NeedsAttention {
                items,
            }));
        }
        let notes = self
            .backend
            .before_first_write(from, target.number())
            .await?;
        let mut applied = Vec::new();
        let result = self.apply_steps(&mut snap, &steps, &mut applied).await;
        let report = RunReport {
            from,
            to: self.version_of(&snap),
            applied,
            notes,
        };
        match result {
            Ok(()) => Ok(report),
            // Nothing was prepared or committed: the error says it all.
            Err(error) if report.applied.is_empty() && report.notes.is_empty() => Err(error),
            Err(error) => Err(MigrationError::Stopped {
                error: Box::new(error),
                report,
            }),
        }
    }

    /// Apply `steps` in order, writing the version row after an id whose
    /// last body ran outside the catalog store, and record each step applied.
    async fn apply_steps(
        &self,
        snap: &mut Snapshot,
        steps: &[Planned],
        applied: &mut Vec<AppliedStep>,
    ) -> Result<(), MigrationError> {
        for (i, step) in steps.iter().enumerate() {
            let done = self.apply_step(snap, step).await?;
            applied.push(AppliedStep {
                id: step.id.number(),
                name: step.id.name(),
                store_name: step.store.name.clone(),
                notes: done.notes,
            });
            let last_of_id = steps.get(i + 1).is_none_or(|next| next.id != step.id);
            if last_of_id
                && step.store.kind != StoreKind::Catalog
                && let Some(version) = self.version_of(snap)
            {
                self.backend.write_catalog_version(version).await?;
            }
        }
        Ok(())
    }

    fn refuse_non_convergent(&self, snap: &Snapshot) -> Result<(), MigrationError> {
        for row in snap.catalog_rows() {
            match MigrationId::from_number(row.id) {
                Some(id) if self.backend.migration(id).convergent() => {}
                Some(id) => {
                    return Err(MigrationError::Refused(format!(
                        "migration {id} is not convergent on {}, so the catalog cannot be \
                         reconverged; restore the backup taken before the upgrade",
                        self.backend.backend_name()
                    )));
                }
                None => {
                    return Err(MigrationError::Refused(format!(
                        "the catalog has migration {} {}, which this binary does not carry; \
                         reconverge with the binary that applied it",
                        row.id, row.name
                    )));
                }
            }
        }
        Ok(())
    }

    async fn apply_step(
        &self,
        snap: &mut Snapshot,
        step: &Planned,
    ) -> Result<Done, MigrationError> {
        let migration = self.backend.migration(step.id);
        let checksum = record_checksum(
            self.backend.released(),
            step.id,
            &migration,
            step.store.kind,
        )
        .ok_or_else(|| {
            MigrationError::Store(format!(
                "migration {} declares no body for the {} store",
                step.id,
                step.store.kind.as_str()
            ))
        })?;
        let (outcome, body) = match &migration {
            Migration::NotApplicable(_) => (Outcome::NotApplicable, None),
            Migration::Applies { bodies, .. } => (
                Outcome::Applied,
                bodies
                    .iter()
                    .find(|b| b.store == step.store.kind)
                    .map(|b| &b.body),
            ),
        };
        let row = (!step.recorded).then(|| LedgerRow {
            id: step.id.number(),
            name: step.id.name().to_owned(),
            kind: migration.kind(),
            checksum,
            outcome,
            notes: String::new(),
            applied_at: String::new(),
            applied_by: self.applied_by.clone(),
        });
        let mut after = snap.clone();
        if let Some(row) = &row {
            after.put_row(&step.store, row.clone());
        }
        let catalog_version = (step.store.kind == StoreKind::Catalog)
            .then(|| self.version_of(&after).unwrap_or_default());
        let action = match body {
            None => Step::Record,
            Some(Body::File { name, text }) => Step::File { name, text },
            Some(Body::Code { body, .. }) => Step::Code(*body),
        };
        let done = self
            .backend
            .apply(&step.store, step.id, action, row, catalog_version)
            .await?;
        *snap = after;
        Ok(done)
    }
}
