// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::pass::{FakeKek, Under, access_key_row, catalog_with, openers, session_row};
use super::*;

// ── The commands (sections 8.1 to 8.3, 8.5, 8.6) ───────────────────────

/// The key of each row after a command: the active DEK of the ring that
/// `kek` opens, else the fixed keys of [`Under`].
pub(super) fn openers_after(catalog: &FakeCatalog, ids: &[&str], kek: &[u8; 32]) -> Vec<Under> {
    let ring = catalog
        .get(KEY_RING_SETTING)
        .and_then(|text| open_ring(&text, Some(CATALOG_ID), kek).ok());
    ids.iter()
        .map(|id| {
            let (table, row) = if id.starts_with("ASIA") {
                (SecretTable::IamSessions, session_row(id))
            } else {
                (SecretTable::AccessKeys, access_key_row(id))
            };
            let stored = catalog.stored(table, id).expect("row");
            if ring
                .as_ref()
                .is_some_and(|open| open.active().decrypt(&row, &stored).is_ok())
            {
                return Under::RingActive;
            }
            openers(catalog, &[id])[0]
        })
        .collect()
}

#[tokio::test]
async fn command_cases() {
    use crate::key_commands::{KeyStores, Outcome, RunFlags, reset, rewrap, rotate, switch};
    use crate::secret_keys::DATA_CONNECTION_SETTING;

    const CONNECTION: &str = "postgres://extenddb:pw@db.internal:5432/extenddb_data";
    let catalog_key_bytes: [u8; 32] = std::array::from_fn(|i| i as u8);
    let retiring = Some(catalog_key_id(&KEK, &catalog_key()));
    let ring = |kek: &[u8; 32], ids: &[u64], switch: bool, retiring: Option<KeyId>| {
        ring_text(kek, ids, switch, retiring)
    };

    #[derive(Clone, Copy)]
    enum Cmd {
        Switch,
        Rotate,
        Rewrap([u8; 32]),
        Reset { discard: bool },
    }
    #[derive(Debug)]
    enum Expect {
        NothingToDo,
        Preview,
        Done,
        Refused(&'static str),
    }
    struct Case {
        name: &'static str,
        catalog_key: bool,
        ring: Option<String>,
        connection: Option<&'static str>,
        rows: Vec<(&'static str, Under)>,
        cmd: Cmd,
        kek: FakeKek,
        flags: RunFlags,
        expect: Expect,
        /// The key of each row after the run; empty when the rows are gone.
        after: Vec<Under>,
        /// The DEK ids and the flag of the ring after the run.
        ring_after: Option<(Vec<u64>, bool)>,
        catalog_key_after: bool,
    }
    let yes = RunFlags {
        yes: true,
        ..RunFlags::default()
    };
    let kek = || FakeKek::new(KEK);
    let mixed = vec![
        ("AKIAKEY000000000001", Under::CatalogKey),
        ("ASIASESSION00000001", Under::CatalogKey),
    ];
    let cases = vec![
        Case {
            name: "switch: the preview writes nothing",
            catalog_key: true,
            ring: None,
            connection: None,
            rows: mixed.clone(),
            cmd: Cmd::Switch,
            kek: kek(),
            flags: RunFlags::default(),
            expect: Expect::Preview,
            after: vec![Under::CatalogKey, Under::CatalogKey],
            ring_after: None,
            catalog_key_after: true,
        },
        Case {
            name: "switch: a fresh run",
            catalog_key: true,
            ring: None,
            connection: Some(CONNECTION),
            rows: mixed.clone(),
            cmd: Cmd::Switch,
            kek: kek(),
            flags: yes,
            expect: Expect::Done,
            after: vec![Under::RingActive, Under::RingActive],
            ring_after: Some((vec![1], false)),
            catalog_key_after: false,
        },
        Case {
            name: "switch: a second run has nothing to do",
            catalog_key: false,
            ring: Some(ring(&KEK, &[1], false, None)),
            connection: None,
            rows: vec![("AKIAKEY000000000001", Under::Dek(1))],
            cmd: Cmd::Switch,
            kek: kek(),
            flags: yes,
            expect: Expect::NothingToDo,
            after: vec![Under::RingActive],
            ring_after: Some((vec![1], false)),
            catalog_key_after: false,
        },
        Case {
            name: "switch: refused while key_file is set",
            catalog_key: true,
            ring: None,
            connection: None,
            rows: mixed.clone(),
            cmd: Cmd::Switch,
            kek: kek(),
            flags: RunFlags {
                key_file_configured: true,
                ..yes
            },
            expect: Expect::Refused("Unset it for this run"),
            after: vec![Under::CatalogKey, Under::CatalogKey],
            ring_after: None,
            catalog_key_after: true,
        },
        Case {
            name: "switch: a KEK equal to the catalog key",
            catalog_key: true,
            ring: None,
            connection: None,
            rows: vec![],
            cmd: Cmd::Switch,
            kek: FakeKek::new(catalog_key_bytes),
            flags: yes,
            expect: Expect::Refused("the KEK equals the catalog key"),
            after: vec![],
            ring_after: None,
            catalog_key_after: true,
        },
        Case {
            name: "switch: a secret under no key stops the run before its first write",
            catalog_key: true,
            ring: None,
            connection: None,
            rows: vec![
                ("AKIAKEY000000000001", Under::CatalogKey),
                ("AKIANOKEY0000000001", Under::NoKey),
            ],
            cmd: Cmd::Switch,
            kek: kek(),
            flags: yes,
            expect: Expect::Refused(
                "1 stored secrets open under no key of this run: AKIANOKEY0000000001",
            ),
            after: vec![Under::CatalogKey, Under::NoKey],
            ring_after: None,
            catalog_key_after: true,
        },
        Case {
            name: "switch: --keep-unopenable keeps a secret under no key",
            catalog_key: true,
            ring: None,
            connection: None,
            rows: vec![
                ("AKIAKEY000000000001", Under::CatalogKey),
                ("AKIANOKEY0000000001", Under::NoKey),
            ],
            cmd: Cmd::Switch,
            kek: kek(),
            flags: RunFlags {
                keep_unopenable: true,
                ..yes
            },
            expect: Expect::Done,
            after: vec![Under::RingActive, Under::NoKey],
            ring_after: Some((vec![1], false)),
            catalog_key_after: false,
        },
        Case {
            name: "switch: the KEK file is gone before the catalog key goes",
            catalog_key: true,
            ring: None,
            connection: None,
            rows: mixed.clone(),
            cmd: Cmd::Switch,
            kek: FakeKek {
                gone_after: Some(1),
                ..kek()
            },
            flags: yes,
            expect: Expect::Refused("cannot be read again before the old key is deleted"),
            after: vec![Under::RingActive, Under::RingActive],
            ring_after: Some((vec![1], true)),
            catalog_key_after: true,
        },
        Case {
            name: "switch: a stopped run resumes",
            catalog_key: true,
            ring: Some(ring(&KEK, &[1], true, retiring.clone())),
            connection: None,
            rows: vec![
                ("AKIAKEY000000000001", Under::Dek(1)),
                ("ASIASESSION00000001", Under::CatalogKey),
            ],
            cmd: Cmd::Switch,
            kek: kek(),
            flags: yes,
            expect: Expect::Done,
            after: vec![Under::RingActive, Under::RingActive],
            ring_after: Some((vec![1], false)),
            catalog_key_after: false,
        },
        Case {
            name: "switch: stopped between the catalog key delete and the last ring write",
            catalog_key: false,
            ring: Some(ring(&KEK, &[1], true, retiring.clone())),
            connection: None,
            rows: vec![("AKIAKEY000000000001", Under::Dek(1))],
            cmd: Cmd::Switch,
            kek: kek(),
            flags: yes,
            expect: Expect::Done,
            after: vec![Under::RingActive],
            ring_after: Some((vec![1], false)),
            catalog_key_after: false,
        },
        Case {
            name: "switch: a catalog key that the unfinished switch does not retire",
            catalog_key: true,
            ring: Some(ring(
                &KEK,
                &[1],
                true,
                Some(KeyId::parse("00112233aabbccdd").unwrap()),
            )),
            connection: None,
            rows: vec![("AKIAKEY000000000001", Under::CatalogKey)],
            cmd: Cmd::Switch,
            kek: kek(),
            flags: yes,
            expect: Expect::Refused("not the catalog key that the unfinished switch retires"),
            after: vec![Under::CatalogKey],
            ring_after: Some((vec![1], true)),
            catalog_key_after: true,
        },
        Case {
            name: "switch: a catalog key beside a finished ring, without --accept-catalog-key",
            catalog_key: true,
            ring: Some(ring(&KEK, &[1], false, None)),
            connection: None,
            rows: vec![("AKIAFORGED000000001", Under::CatalogKey)],
            cmd: Cmd::Switch,
            kek: kek(),
            flags: yes,
            expect: Expect::Refused("--accept-catalog-key"),
            after: vec![Under::CatalogKey],
            ring_after: Some((vec![1], false)),
            catalog_key_after: true,
        },
        Case {
            name: "switch: a catalog key beside a finished ring, with --accept-catalog-key",
            catalog_key: true,
            ring: Some(ring(&KEK, &[1], false, None)),
            connection: None,
            rows: vec![
                ("AKIAFORGED000000001", Under::CatalogKey),
                ("AKIAKEY000000000001", Under::Dek(1)),
            ],
            cmd: Cmd::Switch,
            kek: kek(),
            flags: RunFlags {
                accept_catalog_key: true,
                ..yes
            },
            expect: Expect::Done,
            after: vec![Under::RingActive, Under::RingActive],
            ring_after: Some((vec![1], false)),
            catalog_key_after: false,
        },
        Case {
            name: "rotate: a fresh run",
            catalog_key: false,
            ring: Some(ring(&KEK, &[1], false, None)),
            connection: Some("under DEK 1"),
            rows: vec![
                ("AKIAKEY000000000001", Under::Dek(1)),
                ("ASIASESSION00000001", Under::Dek(1)),
            ],
            cmd: Cmd::Rotate,
            kek: kek(),
            flags: yes,
            expect: Expect::Done,
            after: vec![Under::RingActive, Under::RingActive],
            ring_after: Some((vec![2], false)),
            catalog_key_after: false,
        },
        Case {
            name: "rotate: a stopped run resumes, with the ring's entries in either order",
            catalog_key: false,
            ring: Some(edit_ring(&ring(&KEK, &[1, 2], false, None), |r| {
                r["keys"].as_array_mut().unwrap().reverse();
            })),
            connection: None,
            rows: vec![
                ("AKIAKEY000000000001", Under::Dek(2)),
                ("ASIASESSION00000001", Under::Dek(1)),
            ],
            cmd: Cmd::Rotate,
            kek: kek(),
            flags: yes,
            expect: Expect::Done,
            after: vec![Under::RingActive, Under::RingActive],
            ring_after: Some((vec![2], false)),
            catalog_key_after: false,
        },
        Case {
            name: "rotate: refused on a ring whose DEK id has no successor",
            catalog_key: false,
            ring: Some(ring(&KEK, &[u64::MAX], false, None)),
            connection: None,
            rows: vec![("AKIAKEY000000000001", Under::Dek(u64::MAX))],
            cmd: Cmd::Rotate,
            kek: kek(),
            flags: yes,
            expect: Expect::Refused("has no id for a new DEK"),
            after: vec![Under::RingActive],
            ring_after: Some((vec![u64::MAX], false)),
            catalog_key_after: false,
        },
        Case {
            name: "rotate: a catalog key beside the ring, with --accept-catalog-key",
            catalog_key: true,
            ring: Some(ring(&KEK, &[1], false, None)),
            connection: None,
            rows: vec![
                ("AKIAFORGED000000001", Under::CatalogKey),
                ("AKIAKEY000000000001", Under::Dek(1)),
            ],
            cmd: Cmd::Rotate,
            kek: kek(),
            flags: RunFlags {
                accept_catalog_key: true,
                ..yes
            },
            expect: Expect::Done,
            after: vec![Under::RingActive, Under::RingActive],
            ring_after: Some((vec![2], false)),
            catalog_key_after: false,
        },
        Case {
            name: "rotate: refused on the ring of an unfinished switch",
            catalog_key: true,
            ring: Some(ring(&KEK, &[1], true, retiring.clone())),
            connection: None,
            rows: vec![],
            cmd: Cmd::Rotate,
            kek: kek(),
            flags: yes,
            expect: Expect::Refused("a switch to a KEK file did not finish"),
            after: vec![],
            ring_after: Some((vec![1], true)),
            catalog_key_after: true,
        },
        Case {
            name: "rotate: refused under another KEK, naming both ids",
            catalog_key: false,
            ring: Some(ring(&OTHER_KEK, &[1], false, None)),
            connection: None,
            rows: vec![],
            cmd: Cmd::Rotate,
            kek: kek(),
            flags: yes,
            expect: Expect::Refused("is wrapped by the KEK with id"),
            after: vec![],
            ring_after: None,
            catalog_key_after: false,
        },
        Case {
            name: "rewrap: the DEK under a new KEK",
            catalog_key: false,
            ring: Some(ring(&KEK, &[1], false, None)),
            connection: None,
            rows: vec![("AKIAKEY000000000001", Under::Dek(1))],
            cmd: Cmd::Rewrap(OTHER_KEK),
            kek: kek(),
            flags: yes,
            expect: Expect::Done,
            after: vec![Under::RingActive],
            ring_after: Some((vec![1], false)),
            catalog_key_after: false,
        },
        Case {
            name: "rewrap: a second run has nothing to do",
            catalog_key: false,
            ring: Some(ring(&OTHER_KEK, &[1], false, None)),
            connection: None,
            rows: vec![],
            cmd: Cmd::Rewrap(OTHER_KEK),
            kek: kek(),
            flags: yes,
            expect: Expect::NothingToDo,
            after: vec![],
            ring_after: None,
            catalog_key_after: false,
        },
        Case {
            name: "reset: refused without --discard-secrets",
            catalog_key: false,
            ring: Some(ring(&OTHER_KEK, &[1], false, None)),
            connection: None,
            rows: vec![("AKIAKEY000000000001", Under::Dek(1))],
            cmd: Cmd::Reset { discard: false },
            kek: kek(),
            flags: yes,
            expect: Expect::Refused("Pass --discard-secrets"),
            after: vec![Under::Dek(1)],
            ring_after: None,
            catalog_key_after: false,
        },
        Case {
            name: "reset: a lost KEK",
            catalog_key: false,
            ring: Some(ring(&OTHER_KEK, &[1], false, None)),
            connection: Some(CONNECTION),
            rows: vec![
                ("AKIAKEY000000000001", Under::Dek(1)),
                ("ASIASESSION00000001", Under::Dek(1)),
            ],
            cmd: Cmd::Reset { discard: true },
            kek: kek(),
            flags: yes,
            expect: Expect::Done,
            after: vec![],
            ring_after: Some((vec![2], false)),
            catalog_key_after: false,
        },
        Case {
            name: "reset: run again after a stop before its ring write",
            catalog_key: false,
            ring: Some(ring(&OTHER_KEK, &[1], false, None)),
            connection: None,
            rows: vec![],
            cmd: Cmd::Reset { discard: true },
            kek: kek(),
            flags: yes,
            expect: Expect::Done,
            after: vec![],
            ring_after: Some((vec![2], false)),
            catalog_key_after: false,
        },
        Case {
            name: "reset: the KEK is not lost",
            catalog_key: false,
            ring: Some(ring(&KEK, &[1], false, None)),
            connection: None,
            rows: vec![("AKIAKEY000000000001", Under::Dek(1))],
            cmd: Cmd::Reset { discard: true },
            kek: kek(),
            flags: yes,
            expect: Expect::NothingToDo,
            after: vec![Under::RingActive],
            ring_after: Some((vec![1], false)),
            catalog_key_after: false,
        },
        Case {
            name: "reset: refused on a catalog that uses the catalog key",
            catalog_key: true,
            ring: None,
            connection: None,
            rows: vec![("AKIAKEY000000000001", Under::CatalogKey)],
            cmd: Cmd::Reset { discard: true },
            kek: kek(),
            flags: yes,
            expect: Expect::Refused("so no KEK is lost"),
            after: vec![Under::CatalogKey],
            ring_after: None,
            catalog_key_after: true,
        },
    ];

    for case in cases {
        let mut settings = vec![(CATALOG_ID_SETTING, CATALOG_ID.to_owned())];
        if case.catalog_key {
            settings.push((CATALOG_KEY_SETTING, CATALOG_KEY_B64.to_owned()));
        }
        if let Some(ring) = &case.ring {
            settings.push((KEY_RING_SETTING, ring.clone()));
        }
        let connection = case.connection.map(|connection| {
            if connection == "under DEK 1" {
                dek(1).encrypt_setting(CONNECTION).unwrap()
            } else {
                connection.to_owned()
            }
        });
        if let Some(connection) = &connection {
            settings.push((DATA_CONNECTION_SETTING, connection.clone()));
        }
        let settings: Vec<(&str, &str)> = settings.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let store = catalog_with(&case.rows, &settings);
        let stores = KeyStores {
            management: &store,
            settings: &store,
        };
        let new_kek = match case.cmd {
            Cmd::Rewrap(bytes) => FakeKek::new(bytes),
            _ => FakeKek::new(KEK),
        };
        let outcome = match case.cmd {
            Cmd::Switch => switch(stores, &case.kek, case.flags).await,
            Cmd::Rotate => rotate(stores, &case.kek, case.flags).await,
            Cmd::Rewrap(_) => rewrap(stores, &case.kek, &new_kek, case.flags.yes).await,
            Cmd::Reset { discard } => {
                reset(
                    stores,
                    &case.kek,
                    case.flags,
                    discard,
                    case.connection.map(|_| CONNECTION),
                )
                .await
            }
        };
        match (&case.expect, &outcome) {
            (Expect::NothingToDo, Ok(Outcome::NothingToDo { .. }))
            | (Expect::Preview, Ok(Outcome::Preview(_)))
            | (Expect::Done, Ok(Outcome::Done(_))) => {}
            (Expect::Refused(part), Err(e)) if e.to_string().contains(part) => {}
            (expected, got) => panic!("{}: expected {expected:?}, got {got:?}", case.name),
        }

        // The state after the run.
        let final_kek = match case.cmd {
            Cmd::Rewrap(bytes) if matches!(case.expect, Expect::Done) => bytes,
            Cmd::Rewrap(bytes) if matches!(case.expect, Expect::NothingToDo) => bytes,
            _ => KEK,
        };
        let ids: Vec<&str> = case.rows.iter().map(|(id, _)| *id).collect();
        if case.after.is_empty() {
            assert!(
                store.rows.lock().unwrap().is_empty() || ids.is_empty(),
                "{}: the rows are gone",
                case.name
            );
        } else {
            assert_eq!(
                openers_after(&store, &ids, &final_kek),
                case.after,
                "{}",
                case.name
            );
        }
        if let Some((dek_ids, switching)) = &case.ring_after {
            let open = open_ring(
                &store.get(KEY_RING_SETTING).expect("a ring"),
                Some(CATALOG_ID),
                &final_kek,
            )
            .unwrap_or_else(|r| panic!("{}: {r}", case.name));
            assert_eq!(open.ring.dek_ids(), *dek_ids, "{}", case.name);
            assert_eq!(
                open.ring.switch_from_catalog_key, *switching,
                "{}",
                case.name
            );
            if !switching {
                assert_eq!(open.ring.retiring_catalog_key_id, None, "{}", case.name);
            }
            if let (Some(_), Ok(Outcome::Done(summary))) = (&connection, &outcome) {
                let stored = store.get(DATA_CONNECTION_SETTING).unwrap();
                assert_eq!(
                    *open
                        .active()
                        .decrypt_setting(&stored)
                        .expect("the row opens"),
                    *CONNECTION,
                    "{}",
                    case.name
                );
                assert_eq!(
                    summary.data_connection_rewritten,
                    Some(true),
                    "{}",
                    case.name
                );
            }
        }
        assert_eq!(
            store.get(CATALOG_KEY_SETTING).is_some(),
            case.catalog_key_after,
            "{}",
            case.name
        );
    }
}
