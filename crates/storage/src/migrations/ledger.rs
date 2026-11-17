// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Ledger rows, the same columns in every store (`extenddb_migrations`).

use super::status::MigrationError;
use super::table::Kind;

/// What happened when the id was recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Applied,
    NotApplicable,
}

impl Outcome {
    /// The value recorded in the ledger's `outcome` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::NotApplicable => "not_applicable",
        }
    }

    /// Parse a ledger value.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "applied" => Some(Self::Applied),
            "not_applicable" => Some(Self::NotApplicable),
            _ => None,
        }
    }
}

/// One row of a store's ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerRow {
    pub id: u32,
    pub name: String,
    pub kind: Kind,
    /// SHA-256 of a file body (CRLF normalised to LF), the fingerprint of a
    /// code body, or SHA-256 of a `NotApplicable` reason.
    pub checksum: String,
    pub outcome: Outcome,
    /// Notes a code body returned, one per line; empty for a file body.
    pub notes: String,
    /// Server time, set by the store when the row is written. Empty in a row
    /// the runner hands to a backend to write.
    pub applied_at: String,
    /// The ExtendDB release that wrote the row.
    pub applied_by: String,
}

/// A ledger row as a store returns it, before this binary reads it: `id`,
/// `name`, `kind`, `checksum`, `outcome`, `notes`, `applied_at` and
/// `applied_by`, in that order. `I` is the integer type the store's driver
/// returns for `id`.
pub type LedgerColumns<I> = (I, String, String, String, String, String, String, String);

impl LedgerRow {
    /// Read a row a store returned. `store` names the store in the error, for
    /// a backend with more than one.
    ///
    /// # Errors
    ///
    /// An `id`, `kind` or `outcome` this binary cannot read.
    pub fn from_columns<I: Into<i64>>(
        store: Option<&str>,
        columns: LedgerColumns<I>,
    ) -> Result<Self, MigrationError> {
        let (id, name, kind, checksum, outcome, notes, applied_at, applied_by) = columns;
        let id: i64 = id.into();
        let bad = |what: &str, value: &str| {
            MigrationError::Store(match store {
                Some(store) => format!(
                    "extenddb_migrations row {id} in {store} has {what} {value:?}, which this \
                     binary cannot read"
                ),
                None => format!(
                    "extenddb_migrations row {id} has {what} {value:?}, which this binary \
                     cannot read"
                ),
            })
        };
        Ok(Self {
            id: u32::try_from(id).map_err(|_| bad("id", &id.to_string()))?,
            name,
            kind: Kind::parse(&kind).ok_or_else(|| bad("kind", &kind))?,
            checksum,
            outcome: Outcome::parse(&outcome).ok_or_else(|| bad("outcome", &outcome))?,
            notes,
            applied_at,
            applied_by,
        })
    }
}

/// What a release before 1.0 left in the catalog's settings, read beside the
/// ledger.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogTraces {
    /// The `catalog_version` settings row. A dotted value (`0.0.3`) comes from
    /// a release before 1.0; the runner writes an integer.
    pub catalog_version: Option<String>,
    /// Whether the `resource_tag_migration` row exists. Every released
    /// baseline seeded it and migration 1 deletes it, so on a catalog with a
    /// ledger it means a release before 1.0 wrote to the catalog.
    pub resource_tag_marker: bool,
}

impl CatalogTraces {
    /// The traces in the catalog's `catalog_version` and
    /// `resource_tag_migration` settings rows, given as `(key, value)`.
    #[must_use]
    pub fn from_settings(rows: &[(String, String)]) -> Self {
        Self {
            catalog_version: rows
                .iter()
                .find(|(k, _)| k == "catalog_version")
                .map(|(_, v)| v.clone()),
            resource_tag_marker: rows.iter().any(|(k, _)| k == "resource_tag_migration"),
        }
    }

    /// The version row holds a dotted pre-1.0 version.
    #[must_use]
    pub fn dotted_version(&self) -> bool {
        self.catalog_version
            .as_deref()
            .is_some_and(|v| v.contains('.'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns<I>(id: I, kind: &str, outcome: &str) -> LedgerColumns<I> {
        (
            id,
            "baseline".to_owned(),
            kind.to_owned(),
            "c".to_owned(),
            outcome.to_owned(),
            "a note".to_owned(),
            "2026-10-29T14:00:00Z".to_owned(),
            "1.0.0".to_owned(),
        )
    }

    fn refusal(result: Result<LedgerRow, MigrationError>) -> String {
        match result {
            Err(MigrationError::Store(message)) => message,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_ledger_row_is_read_from_its_columns() {
        let row = LedgerRow::from_columns(Some("extenddb"), columns(4_i32, "breaking", "applied"))
            .expect("row");
        assert_eq!(
            row,
            LedgerRow {
                id: 4,
                name: "baseline".to_owned(),
                kind: Kind::Breaking,
                checksum: "c".to_owned(),
                outcome: Outcome::Applied,
                notes: "a note".to_owned(),
                applied_at: "2026-10-29T14:00:00Z".to_owned(),
                applied_by: "1.0.0".to_owned(),
            }
        );
        let row = LedgerRow::from_columns(None, columns(2_i64, "additive", "not_applicable"))
            .expect("row");
        assert_eq!(
            (row.kind, row.outcome),
            (Kind::Additive, Outcome::NotApplicable)
        );
    }

    #[test]
    fn a_value_this_binary_cannot_read_is_an_error_naming_it() {
        assert_eq!(
            refusal(LedgerRow::from_columns(
                None,
                columns(-1_i64, "breaking", "applied")
            )),
            "extenddb_migrations row -1 has id \"-1\", which this binary cannot read"
        );
        assert_eq!(
            refusal(LedgerRow::from_columns(
                None,
                columns(4_294_967_296_i64, "breaking", "applied")
            )),
            "extenddb_migrations row 4294967296 has id \"4294967296\", which this binary cannot \
             read"
        );
        assert_eq!(
            refusal(LedgerRow::from_columns(
                Some("extenddb_catalog"),
                columns(2_i32, "sideways", "applied")
            )),
            "extenddb_migrations row 2 in extenddb_catalog has kind \"sideways\", which this \
             binary cannot read"
        );
        assert_eq!(
            refusal(LedgerRow::from_columns(
                None,
                columns(3_i64, "breaking", "maybe")
            )),
            "extenddb_migrations row 3 has outcome \"maybe\", which this binary cannot read"
        );
    }

    #[test]
    fn the_traces_are_read_from_their_two_settings_rows() {
        let rows = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect()
        };
        assert_eq!(CatalogTraces::from_settings(&[]), CatalogTraces::default());
        let traces = CatalogTraces::from_settings(&rows(&[
            ("other", "x"),
            ("catalog_version", "0.0.3"),
            ("resource_tag_migration", "done"),
        ]));
        assert_eq!(traces.catalog_version.as_deref(), Some("0.0.3"));
        assert!(traces.resource_tag_marker && traces.dotted_version());
        let traces = CatalogTraces::from_settings(&rows(&[("catalog_version", "4")]));
        assert_eq!(traces.catalog_version.as_deref(), Some("4"));
        assert!(!traces.resource_tag_marker && !traces.dotted_version());
    }
}
