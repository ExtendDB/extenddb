// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::pass::{FakeKek, Under, catalog_with};
use super::*;

/// The backend check runs on the in-memory catalog, so a change to it is
/// seen here before a backend runs it.
#[cfg(feature = "test-util")]
#[tokio::test]
async fn the_backend_check_passes_on_the_fake_catalog() {
    use crate::key_conformance::{Expectations, run};
    use crate::secret_keys::DATA_CONNECTION_SETTING;

    let store = FakeCatalog::with_settings(&[
        (CATALOG_KEY_SETTING, CATALOG_KEY_B64),
        (CATALOG_ID_SETTING, CATALOG_ID),
        (
            DATA_CONNECTION_SETTING,
            "postgres://extenddb:pw@localhost:5432/extenddb_data",
        ),
    ]);
    run(
        &store,
        &store,
        Expectations {
            write_timestamps: false,
            data_connection: true,
        },
    )
    .await;
}

#[tokio::test]
async fn status_cases() {
    use crate::key_commands::{KeyStores, status};

    let finished = ring_text(&KEK, &[1], false, None);
    // (case, catalog key row, ring row, KEK given, failed DEK ids, the
    // access key IDs listed as under a key other than the target, or None
    // when no key opens the secrets)
    #[allow(clippy::type_complexity)]
    let cases: Vec<(
        &str,
        bool,
        Option<&str>,
        Option<[u8; 32]>,
        Vec<u64>,
        Option<Vec<&str>>,
    )> = vec![
        (
            "the catalog key alone",
            true,
            None,
            None,
            vec![],
            Some(vec!["AKIAUNDERDEK0000001"]),
        ),
        (
            "a ring and its KEK, and a catalog key beside it",
            true,
            Some(&finished),
            Some(KEK),
            vec![],
            Some(vec!["AKIACATALOGKEY00001"]),
        ),
        (
            "a ring under another KEK",
            false,
            Some(&finished),
            Some(OTHER_KEK),
            vec![1],
            None,
        ),
        (
            "a ring and no KEK",
            false,
            Some(&finished),
            None,
            vec![],
            None,
        ),
    ];
    for (case, with_catalog_key, ring, kek, failed, listed) in cases {
        let mut settings = vec![(CATALOG_ID_SETTING, CATALOG_ID)];
        if with_catalog_key {
            settings.push((CATALOG_KEY_SETTING, CATALOG_KEY_B64));
        }
        if let Some(ring) = ring {
            settings.push((KEY_RING_SETTING, ring));
        }
        let store = catalog_with(
            &[
                ("AKIACATALOGKEY00001", Under::CatalogKey),
                ("AKIAUNDERDEK0000001", Under::Dek(1)),
            ],
            &settings,
        );
        let kek_file = kek.map(FakeKek::new);
        let report = status(
            KeyStores {
                management: &store,
                settings: &store,
            },
            kek_file
                .as_ref()
                .map(|k| k as &dyn crate::key_commands::KekFile),
        )
        .await
        .unwrap_or_else(|e| panic!("{case}: {e}"));
        assert_eq!(report.catalog_key_present, with_catalog_key, "{case}");
        assert_eq!(
            report.ring.as_ref().map(|r| r.failed_dek_ids.clone()),
            ring.map(|_| failed.clone()),
            "{case}"
        );
        assert_eq!(
            report.survey.as_ref().map(|s| s
                .not_target
                .iter()
                .map(|l| l.access_key_id.as_str())
                .collect::<Vec<_>>()),
            listed,
            "{case}"
        );
    }
}

#[test]
fn data_connection_key_cases() {
    use crate::key_commands::data_connection_key;

    let finished = ring_text(&KEK, &[1], false, None);
    let switching = ring_text(&KEK, &[1], true, Some(catalog_key_id(&KEK, &catalog_key())));
    let rotating = ring_text(&KEK, &[1, 2], false, None);
    // (case, ring row, KEK, Ok(DEK id) or Err(part of the refusal))
    #[allow(clippy::type_complexity)]
    let cases: &[(&str, Option<&str>, [u8; 32], Result<u64, &str>)] = &[
        ("a finished ring", Some(&finished), KEK, Ok(1)),
        ("no ring", None, KEK, Err("has no key ring")),
        (
            "an unfinished switch",
            Some(&switching),
            KEK,
            Err("did not finish"),
        ),
        ("two DEKs", Some(&rotating), KEK, Err("holds two DEKs")),
        (
            "another KEK",
            Some(&finished),
            OTHER_KEK,
            Err("is wrapped by the KEK with id"),
        ),
    ];
    for (case, ring, kek, expected) in cases {
        let got = data_connection_key(*ring, kek, CATALOG_ID).map(|k| k.dek_id().unwrap());
        match (expected, got) {
            (Ok(id), Ok(got)) => assert_eq!(*id, got, "{case}"),
            (Err(part), Err(message)) => assert!(message.contains(part), "{case}: {message}"),
            (expected, got) => panic!("{case}: expected {expected:?}, got {got:?}"),
        }
    }
}
