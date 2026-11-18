// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// ── The pass (section 8.4) ─────────────────────────────────────────────

/// A KEK file in memory. `gone_after` reads, it is gone.
pub(crate) struct FakeKek {
    pub(crate) path: &'static str,
    pub(crate) bytes: [u8; 32],
    pub(crate) gone_after: Option<usize>,
    pub(crate) reads: Mutex<usize>,
}

impl FakeKek {
    pub(crate) fn new(bytes: [u8; 32]) -> Self {
        Self {
            path: "/run/credentials/extenddb.service/extenddb-kek",
            bytes,
            gone_after: None,
            reads: Mutex::new(0),
        }
    }
}

impl crate::key_commands::KekFile for FakeKek {
    fn path(&self) -> &str {
        self.path
    }

    fn read(&self) -> Result<Zeroizing<[u8; 32]>, String> {
        let mut reads = self.reads.lock().unwrap();
        *reads += 1;
        if self.gone_after.is_some_and(|after| *reads > after) {
            return Err(format!("{}: no such file", self.path));
        }
        Ok(Zeroizing::new(self.bytes))
    }
}

/// Which key a row's secret is under, in the pass and command cases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Under {
    CatalogKey,
    Dek(u64),
    NoKey,
    /// The active DEK of the catalog's ring after the run, which a switch or
    /// a rotate draws at random.
    RingActive,
}

impl Under {
    pub(crate) fn seal(self, row: &SecretRow<'_>, secret: &str) -> Vec<u8> {
        match self {
            Self::CatalogKey => catalog_key().encrypt(row, secret).expect("encrypt"),
            Self::Dek(id) => dek(id).encrypt(row, secret).expect("encrypt"),
            Self::NoKey => SecretKeys::dek_for_tests([0xee; 32], 9, CATALOG_ID)
                .encrypt(row, secret)
                .expect("encrypt"),
            Self::RingActive => unreachable!("a case seeds no row under the ring after the run"),
        }
    }

    pub(crate) fn opens(self, row: &SecretRow<'_>, stored: &[u8]) -> bool {
        match self {
            Self::CatalogKey => catalog_key().decrypt(row, stored).is_ok(),
            Self::Dek(id) => dek(id).decrypt(row, stored).is_ok(),
            Self::NoKey | Self::RingActive => false,
        }
    }
}

const SESSION_EXPIRED: i64 = 1_000_000_000;

pub(crate) fn access_key_row(id: &str) -> SecretRow<'_> {
    SecretRow::access_key(id, "111122223333", "bob")
}

pub(crate) fn session_row(id: &str) -> SecretRow<'_> {
    SecretRow::session(id, "111122223333", "Admin", "s1", SESSION_EXPIRED)
}

/// A catalog with one row per `(access key ID, key)`, sessions for `ASIA`.
pub(crate) fn catalog_with(rows: &[(&str, Under)], settings: &[(&str, &str)]) -> FakeCatalog {
    let catalog = FakeCatalog::with_settings(settings);
    for (id, under) in rows {
        let row = if id.starts_with("ASIA") {
            session_row(id)
        } else {
            access_key_row(id)
        };
        catalog.add_stored(&row, under.seal(&row, &format!("secret-of-{id}")));
    }
    catalog
}

/// The key each row's secret opens under now.
pub(crate) fn openers(catalog: &FakeCatalog, ids: &[&str]) -> Vec<Under> {
    ids.iter()
        .map(|id| {
            let (table, row) = if id.starts_with("ASIA") {
                (SecretTable::IamSessions, session_row(id))
            } else {
                (SecretTable::AccessKeys, access_key_row(id))
            };
            let stored = catalog.stored(table, id).expect("row");
            [
                Under::CatalogKey,
                Under::Dek(1),
                Under::Dek(2),
                Under::Dek(3),
            ]
            .into_iter()
            .find(|under| under.opens(&row, &stored))
            .unwrap_or(Under::NoKey)
        })
        .collect()
}

#[tokio::test]
async fn pass_cases() {
    use crate::key_pass::{PassError, PassKeys, PassOptions, run_pass};
    use crate::secret_keys::DATA_CONNECTION_SETTING;

    const CONNECTION: &str = "postgres://extenddb:pw@db.internal:5432/extenddb_data";
    let target = dek(2);
    let old_dek = dek(1);
    let catalog = catalog_key();
    let under_old_dek = old_dek.encrypt_setting(CONNECTION).expect("seal");

    struct Case {
        name: &'static str,
        rows: Vec<(&'static str, Under)>,
        connection: Option<String>,
        options: PassOptions,
        lossy: bool,
        /// Ok(rows rewritten) or Err(the access key IDs the error lists, or
        /// the connection string error).
        expect: Result<u64, String>,
        /// The key each row is under after the run.
        after: Vec<Under>,
    }
    let keep = PassOptions {
        keep_unopenable: true,
        plaintext_data_connection: false,
    };
    let switching = PassOptions {
        keep_unopenable: false,
        plaintext_data_connection: true,
    };
    let cases = vec![
        Case {
            name: "a secret under the catalog key, and a session under the old DEK",
            rows: vec![
                ("AKIACATALOGKEY00001", Under::CatalogKey),
                ("ASIAOLDDEK000000001", Under::Dek(1)),
            ],
            connection: None,
            options: PassOptions::default(),
            lossy: false,
            expect: Ok(2),
            after: vec![Under::Dek(2), Under::Dek(2)],
        },
        Case {
            name: "an expired session is re-encrypted like any other",
            rows: vec![("ASIAEXPIRED00000001", Under::CatalogKey)],
            connection: None,
            options: PassOptions::default(),
            lossy: false,
            expect: Ok(1),
            after: vec![Under::Dek(2)],
        },
        Case {
            name: "a resumed run: one secret done, one not",
            rows: vec![
                ("AKIADONE00000000001", Under::Dek(2)),
                ("AKIANOTDONE00000001", Under::Dek(1)),
            ],
            connection: None,
            options: PassOptions::default(),
            lossy: false,
            expect: Ok(1),
            after: vec![Under::Dek(2), Under::Dek(2)],
        },
        Case {
            name: "nothing to do",
            rows: vec![("AKIADONE00000000001", Under::Dek(2))],
            connection: Some(target.encrypt_setting(CONNECTION).unwrap()),
            options: PassOptions::default(),
            lossy: false,
            expect: Ok(0),
            after: vec![Under::Dek(2)],
        },
        Case {
            name: "a secret under no key stops the run",
            rows: vec![("AKIANOKEY0000000001", Under::NoKey)],
            connection: None,
            options: PassOptions::default(),
            lossy: false,
            expect: Err("AKIANOKEY0000000001".to_owned()),
            after: vec![Under::NoKey],
        },
        Case {
            name: "a secret under no key is kept with --keep-unopenable",
            rows: vec![
                ("AKIANOKEY0000000001", Under::NoKey),
                ("AKIACATALOGKEY00001", Under::CatalogKey),
            ],
            connection: None,
            options: keep,
            lossy: false,
            expect: Ok(1),
            after: vec![Under::NoKey, Under::Dek(2)],
        },
        Case {
            name: "a write that changed nothing",
            rows: vec![
                ("AKIADONE00000000001", Under::Dek(2)),
                ("ASIALOST00000000001", Under::Dek(1)),
            ],
            connection: None,
            options: PassOptions::default(),
            lossy: true,
            expect: Err("ASIALOST00000000001".to_owned()),
            after: vec![Under::Dek(2), Under::Dek(1)],
        },
        Case {
            name: "the plaintext connection string row during a switch",
            rows: vec![],
            connection: Some(CONNECTION.to_owned()),
            options: switching,
            lossy: false,
            expect: Ok(0),
            after: vec![],
        },
        Case {
            name: "the plaintext connection string row outside a switch",
            rows: vec![],
            connection: Some(CONNECTION.to_owned()),
            options: PassOptions::default(),
            lossy: false,
            expect: Err("is not encrypted under a DEK of the key ring".to_owned()),
            after: vec![],
        },
        Case {
            name: "the connection string row under the old DEK",
            rows: vec![],
            connection: Some(under_old_dek.clone()),
            options: PassOptions::default(),
            lossy: false,
            expect: Ok(0),
            after: vec![],
        },
        Case {
            name: "a connection string row that no key opens",
            rows: vec![],
            connection: Some("bm90IGEgY2lwaGVydGV4dA==".to_owned()),
            options: switching,
            lossy: false,
            expect: Err("opens under no key of this run".to_owned()),
            after: vec![],
        },
    ];
    for case in cases {
        let store = catalog_with(&case.rows, &[]);
        if let Some(connection) = &case.connection {
            store.set(DATA_CONNECTION_SETTING, connection);
        }
        *store.lossy_writes.lock().unwrap() = case.lossy;
        let keys = PassKeys {
            target: &target,
            old: vec![&catalog, &old_dek],
        };
        let result = run_pass(&store, &store, &keys, case.options).await;
        match (&case.expect, result) {
            (Ok(rewritten), Ok(summary)) => {
                let total: u64 = summary.tables.iter().map(|(_, c)| c.rewritten).sum();
                assert_eq!(total, *rewritten, "{}", case.name);
                if case.connection.is_some() {
                    let stored = store.get(DATA_CONNECTION_SETTING).unwrap();
                    assert_eq!(
                        *target
                            .decrypt_setting(&stored)
                            .expect("the row opens under the target"),
                        *CONNECTION,
                        "{}",
                        case.name
                    );
                }
                assert_eq!(
                    summary.data_connection_rewritten,
                    case.connection
                        .as_ref()
                        .map(|before| target.decrypt_setting(before).is_err()),
                    "{}",
                    case.name
                );
            }
            (Err(listed), Err(PassError::Unopenable(l) | PassError::NotRewritten(l))) => {
                let ids: Vec<&str> = l.iter().map(|s| s.access_key_id.as_str()).collect();
                assert_eq!(ids, [listed.as_str()], "{}", case.name);
            }
            (Err(part), Err(PassError::DataConnection(reason))) => {
                assert!(reason.contains(part.as_str()), "{}: {reason}", case.name);
            }
            (expected, got) => panic!("{}: expected {expected:?}, got {got:?}", case.name),
        }
        let ids: Vec<&str> = case.rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(openers(&store, &ids), case.after, "{}", case.name);
    }
}

#[tokio::test]
async fn pass_reads_every_page() {
    use crate::key_pass::{PAGE_SIZE, PassKeys, PassOptions, run_pass};

    let ids: Vec<String> = (0..=PAGE_SIZE)
        .map(|n| format!("AKIAPAGE{n:011}"))
        .collect();
    let rows: Vec<(&str, Under)> = ids
        .iter()
        .map(|id| (id.as_str(), Under::CatalogKey))
        .collect();
    let store = catalog_with(&rows, &[]);
    let (target, catalog) = (dek(1), catalog_key());
    let summary = run_pass(
        &store,
        &store,
        &PassKeys {
            target: &target,
            old: vec![&catalog],
        },
        PassOptions::default(),
    )
    .await
    .expect("pass");
    assert_eq!(summary.tables[0].1.rewritten, (PAGE_SIZE + 1) as u64);
    // The summary gives the counts after the pass: every row is under the
    // target and none is under the catalog key.
    assert_eq!(summary.tables[0].1.target, (PAGE_SIZE + 1) as u64);
    assert_eq!(summary.tables[0].1.old, vec![0]);
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    assert!(
        openers(&store, &id_refs)
            .into_iter()
            .all(|under| under == Under::Dek(1))
    );
}
