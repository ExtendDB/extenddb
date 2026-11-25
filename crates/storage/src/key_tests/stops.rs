// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::commands::openers_after;
use super::pass::{FakeKek, Under, catalog_with};
use super::*;

// ── Runs that stop: a ring changed after the read, a delete that does not land ──

/// `rewrap` runs outside the migration lock, so a `rotate` can write the ring
/// between the read and the write of either command. Both ring writes are
/// conditional on the text read, and a ring that changed stops the run
/// before it writes anything.
#[tokio::test]
async fn a_ring_that_changed_after_the_read_stops_the_run() {
    use crate::key_commands::{KeyStores, RunFlags, rewrap, rotate};

    let read = ring_text(&KEK, &[1], false, None);
    // The ring that a concurrent rotate writes first.
    let other = ring_text(&KEK, &[1, 2], false, None);
    for command in ["rewrap", "rotate"] {
        let store = catalog_with(
            &[("AKIAKEY000000000001", Under::Dek(1))],
            &[(CATALOG_ID_SETTING, CATALOG_ID), (KEY_RING_SETTING, &read)],
        );
        store
            .written_before_replace
            .lock()
            .unwrap()
            .push_back(Some(other.clone()));
        let stores = KeyStores {
            management: &store,
            settings: &store,
        };
        let kek = FakeKek::new(KEK);
        let outcome = match command {
            "rewrap" => rewrap(stores, &kek, &FakeKek::new(OTHER_KEK), true).await,
            _ => {
                let yes = RunFlags {
                    yes: true,
                    ..RunFlags::default()
                };
                rotate(stores, &kek, yes).await
            }
        };
        let message = outcome
            .err()
            .unwrap_or_else(|| panic!("{command}: the run should stop"))
            .to_string();
        assert!(
            message.contains(&format!(
                "the key ring changed while `extenddb encryption-key {command}` ran, so nothing \
                 was written"
            )),
            "{command}: {message}"
        );
        assert_eq!(
            store.get(KEY_RING_SETTING),
            Some(other.clone()),
            "{command}"
        );
        assert_eq!(
            openers_after(&store, &["AKIAKEY000000000001"], &KEK),
            vec![Under::Dek(1)],
            "{command}: the secret is not re-encrypted"
        );
    }
}

/// The last ring write of `rotate` is conditional on its two-DEK ring too.
/// A ring that changed after the first write stops the run with the DEK
/// that the pass put the stored secrets under.
#[tokio::test]
async fn a_ring_that_changed_before_the_last_rotate_write_stops_the_run() {
    use crate::key_commands::{KeyStores, RunFlags, rotate};

    let read = ring_text(&KEK, &[1], false, None);
    let other = ring_text(&KEK, &[1], false, None);
    let store = catalog_with(
        &[("AKIAKEY000000000001", Under::Dek(1))],
        &[(CATALOG_ID_SETTING, CATALOG_ID), (KEY_RING_SETTING, &read)],
    );
    store
        .written_before_replace
        .lock()
        .unwrap()
        .extend([None, Some(other.clone())]);
    let stores = KeyStores {
        management: &store,
        settings: &store,
    };
    let yes = RunFlags {
        yes: true,
        ..RunFlags::default()
    };
    let message = rotate(stores, &FakeKek::new(KEK), yes)
        .await
        .expect_err("the run should stop")
        .to_string();
    assert!(
        message.contains(
            "the key ring changed after `extenddb encryption-key rotate` wrote its two DEKs, so \
             its last ring write did not happen. The pass put the stored secrets under DEK 2"
        ),
        "{message}"
    );
    assert_eq!(store.get(KEY_RING_SETTING), Some(other));
}

/// `reset` writes its ring only while the row holds the ring it read. A ring
/// that changed after the read stops the run before its ring write, and
/// names what the run left.
#[tokio::test]
async fn a_ring_that_changed_before_the_reset_write_stops_the_run() {
    use crate::key_commands::{KeyStores, RunFlags, reset};

    let lost = ring_text(&[0x5b; 32], &[1], false, None);
    let other = ring_text(&[0x5c; 32], &[1], false, None);
    let store = catalog_with(
        &[("AKIAKEY000000000001", Under::Dek(1))],
        &[(CATALOG_ID_SETTING, CATALOG_ID), (KEY_RING_SETTING, &lost)],
    );
    store
        .written_before_replace
        .lock()
        .unwrap()
        .push_back(Some(other.clone()));
    let stores = KeyStores {
        management: &store,
        settings: &store,
    };
    let yes = RunFlags {
        yes: true,
        ..RunFlags::default()
    };
    let message = reset(stores, &FakeKek::new(KEK), yes, true, None)
        .await
        .expect_err("the run should stop")
        .to_string();
    assert!(
        message.contains(
            "the key ring changed while `extenddb encryption-key reset` ran, so its ring write \
             did not happen"
        ),
        "{message}"
    );
    assert_eq!(store.get(KEY_RING_SETTING), Some(other));
}

/// A store's `Ok` from a delete does not prove the row is gone: on Cassandra
/// a `DELETE` whose timestamp is below the row's own write time leaves the
/// row. Each command that deletes the `encryption_key` row reads it back,
/// and `reset` reads a page of each secret table back, so a lost delete
/// stops the run instead of reporting a key that is still readable as gone.
#[tokio::test]
async fn a_delete_that_does_not_land_stops_the_run() {
    use crate::key_commands::{KeyStores, RunFlags, reset, rotate, switch};

    enum Cmd {
        Switch,
        Rotate,
        Reset,
    }
    struct Case {
        name: &'static str,
        cmd: Cmd,
        ring: Option<String>,
        rows: Vec<(&'static str, Under)>,
        flags: RunFlags,
        message: &'static str,
    }
    let yes = RunFlags {
        yes: true,
        ..RunFlags::default()
    };
    let cases = vec![
        Case {
            name: "switch: the encryption_key row stays",
            cmd: Cmd::Switch,
            ring: None,
            rows: vec![("AKIAKEY000000000001", Under::CatalogKey)],
            flags: yes,
            message: "encryption_key row is still there after the delete",
        },
        Case {
            name: "rotate: the encryption_key row stays",
            cmd: Cmd::Rotate,
            ring: Some(ring_text(&KEK, &[1], false, None)),
            rows: vec![("AKIAKEY000000000001", Under::CatalogKey)],
            flags: RunFlags {
                accept_catalog_key: true,
                ..yes
            },
            message: "encryption_key row is still there after the delete",
        },
        Case {
            name: "reset: a row of access_keys stays",
            cmd: Cmd::Reset,
            ring: Some(ring_text(&[0x5b; 32], &[1], false, None)),
            rows: vec![("AKIAKEY000000000001", Under::Dek(1))],
            flags: yes,
            message: "access_keys table still holds a row after the delete",
        },
        Case {
            name: "reset with no secret row: the encryption_key row stays",
            cmd: Cmd::Reset,
            ring: Some(ring_text(&[0x5b; 32], &[1], false, None)),
            rows: vec![],
            flags: yes,
            message: "encryption_key row is still there after the delete",
        },
    ];

    for case in cases {
        let mut settings = vec![
            (CATALOG_ID_SETTING, CATALOG_ID.to_owned()),
            (CATALOG_KEY_SETTING, CATALOG_KEY_B64.to_owned()),
        ];
        if let Some(ring) = &case.ring {
            settings.push((KEY_RING_SETTING, ring.clone()));
        }
        let settings: Vec<(&str, &str)> = settings.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let store = catalog_with(&case.rows, &settings);
        *store.lossy_deletes.lock().unwrap() = true;
        let stores = KeyStores {
            management: &store,
            settings: &store,
        };
        let kek = FakeKek::new(KEK);
        let outcome = match case.cmd {
            Cmd::Switch => switch(stores, &kek, case.flags).await,
            Cmd::Rotate => rotate(stores, &kek, case.flags).await,
            Cmd::Reset => reset(stores, &kek, case.flags, true, None).await,
        };
        let message = outcome
            .err()
            .unwrap_or_else(|| panic!("{}: the run should stop", case.name))
            .to_string();
        assert!(message.contains(case.message), "{}: {message}", case.name);
        assert!(
            store.get(CATALOG_KEY_SETTING).is_some(),
            "{}: the catalog key is kept",
            case.name
        );
        assert_eq!(
            store.rows.lock().unwrap().len(),
            case.rows.len(),
            "{}: every row is kept",
            case.name
        );
        // The ring the run would have written last is not there: a switch
        // leaves its unfinished ring, and a rotate its two-DEK ring, both of
        // which the command resumes.
        let finished = store
            .get(KEY_RING_SETTING)
            .map(|text| open_ring(&text, Some(CATALOG_ID), &KEK).map(|open| open.ring.dek_ids()));
        match (&case.cmd, finished) {
            (Cmd::Switch, Some(Ok(ids))) => {
                assert_eq!(ids, vec![1], "{}: the unfinished ring stays", case.name);
            }
            (Cmd::Rotate, Some(Ok(ids))) => {
                assert_eq!(ids, vec![1, 2], "{}: the two-DEK ring stays", case.name);
            }
            // `reset` runs with a KEK the old ring does not name, so the old
            // ring stays and does not open with this run's KEK.
            (Cmd::Reset, Some(Err(_))) => {}
            (_, other) => panic!("{}: the ring after the run is {other:?}", case.name),
        }
    }
}

/// The rebuild that erases replaced ciphertexts (SQLite's `VACUUM`) runs
/// after the last write. A failed rebuild says that every row is written and
/// that a run of the same command again rebuilds the file, and that run,
/// with nothing else to do, rebuilds it; a preview does not.
#[tokio::test]
async fn a_failed_rebuild_is_named_and_a_run_again_rebuilds_the_file() {
    use crate::key_commands::{KeyStores, RunFlags, reset, rotate, switch};

    let yes = RunFlags {
        yes: true,
        ..RunFlags::default()
    };
    let finished = ring_text(&KEK, &[1], false, None);
    struct Case<'a> {
        name: &'a str,
        command: &'a str,
        catalog_key: bool,
        ring: Option<&'a str>,
        flags: RunFlags,
        fails: bool,
        /// The message of a run that stops, or `None` for a run that ends.
        message: Option<&'a str>,
        rebuilds: usize,
    }
    let nothing_to_do = |name, command, flags, rebuilds| Case {
        name,
        command,
        catalog_key: false,
        ring: Some(&finished),
        flags,
        fails: false,
        message: None,
        rebuilds,
    };
    let cases = [
        Case {
            name: "a fresh switch",
            command: "switch",
            catalog_key: true,
            ring: None,
            flags: yes,
            fails: true,
            message: Some("`extenddb encryption-key switch` wrote every row, and then the rebuild"),
            rebuilds: 1,
        },
        Case {
            name: "a rotate",
            command: "rotate",
            catalog_key: false,
            ring: Some(&finished),
            flags: yes,
            fails: true,
            message: Some("`extenddb encryption-key rotate` wrote every row, and then the rebuild"),
            rebuilds: 1,
        },
        nothing_to_do("a switch with nothing to do", "switch", yes, 1),
        nothing_to_do(
            "a switch preview with nothing to do",
            "switch",
            RunFlags::default(),
            0,
        ),
        nothing_to_do("a reset with nothing to do", "reset", yes, 1),
    ];
    for Case {
        name,
        command,
        catalog_key,
        ring,
        flags,
        fails,
        message,
        rebuilds,
    } in cases
    {
        let mut settings = vec![(CATALOG_ID_SETTING, CATALOG_ID)];
        if catalog_key {
            settings.push((CATALOG_KEY_SETTING, CATALOG_KEY_B64));
        }
        if let Some(ring) = ring {
            settings.push((KEY_RING_SETTING, ring));
        }
        let under = if catalog_key {
            Under::CatalogKey
        } else {
            Under::Dek(1)
        };
        let store = catalog_with(&[("AKIAKEY000000000001", under)], &settings);
        *store.erase_fails.lock().unwrap() = fails;
        let stores = KeyStores {
            management: &store,
            settings: &store,
        };
        let kek = FakeKek::new(KEK);
        let outcome = match command {
            "switch" => switch(stores, &kek, flags).await,
            "rotate" => rotate(stores, &kek, flags).await,
            _ => reset(stores, &kek, flags, true, None).await,
        };
        match message {
            Some(message) => {
                let error = outcome
                    .err()
                    .unwrap_or_else(|| panic!("{name}: the run should stop"))
                    .to_string();
                assert!(error.contains(message), "{name}: {error}");
                assert!(
                    error.contains("database or disk is full"),
                    "{name}: {error}"
                );
            }
            None => assert!(outcome.is_ok(), "{name}: {:?}", outcome.err()),
        }
        assert_eq!(*store.erased.lock().unwrap(), rebuilds, "{name}");
    }
}
