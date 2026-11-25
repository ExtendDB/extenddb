// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// ── The startup loader (section 7) ─────────────────────────────────────

#[tokio::test]
async fn startup_loader() {
    let finished = ring_text(&KEK, &[1], false, None);
    let unfinished = ring_text(&KEK, &[1], true, Some(catalog_key_id(&KEK, &catalog_key())));
    let flipped = edit_ring(&finished, |r| r["switch_from_catalog_key"] = json!(true));
    let rotating = ring_text(&KEK, &[1, 2], false, None);
    let third = ring_text(&KEK, &[3], false, None);
    let refused = |r: KeyRefusal| Err(format!("Backend initialization failed: {r}"));
    let kek = || KeySource::Kek(Zeroizing::new(KEK).into());

    // (case, settings rows, key source, Ok(DEK id of the loaded key, None for
    // the catalog key) or Err(the BackendError text))
    #[allow(clippy::type_complexity)]
    let cases: Vec<(
        &str,
        Vec<(&str, &str)>,
        KeySource,
        Result<Option<u64>, String>,
    )> = vec![
        (
            "the catalog key, no ring, no key_file",
            vec![(CATALOG_KEY_SETTING, CATALOG_KEY_B64)],
            KeySource::CatalogKey,
            Ok(None),
        ),
        (
            "neither row, no key_file",
            vec![],
            KeySource::CatalogKey,
            Err(BackendError::MissingEncryptionKey.to_string()),
        ),
        (
            "a damaged catalog key",
            vec![(CATALOG_KEY_SETTING, "AAAA")],
            KeySource::CatalogKey,
            Err(
                BackendError::InvalidEncryptionKey("encryption key must be 32 bytes".to_owned())
                    .to_string(),
            ),
        ),
        (
            "an unfinished switch, no key_file",
            vec![
                (CATALOG_KEY_SETTING, CATALOG_KEY_B64),
                (KEY_RING_SETTING, &unfinished),
                (CATALOG_ID_SETTING, CATALOG_ID),
            ],
            KeySource::CatalogKey,
            refused(KeyRefusal::UnfinishedSwitchWithoutKeyFile),
        ),
        (
            "a finished ring, no key_file",
            vec![
                (KEY_RING_SETTING, &finished),
                (CATALOG_ID_SETTING, CATALOG_ID),
            ],
            KeySource::CatalogKey,
            refused(KeyRefusal::RingNeedsKeyFile {
                kek_id: kek_id(&KEK),
            }),
        ),
        (
            "a ring that does not parse, no key_file",
            vec![(KEY_RING_SETTING, "{}")],
            KeySource::CatalogKey,
            refused(KeyRefusal::DamagedRing {
                reason: "the ring does not parse: missing field `version` at line 1 column 2"
                    .to_owned(),
            }),
        ),
        (
            "the catalog key, no ring, key_file set",
            vec![(CATALOG_KEY_SETTING, CATALOG_KEY_B64)],
            kek(),
            refused(KeyRefusal::CatalogKeyWithoutRing),
        ),
        (
            "neither row, key_file set",
            vec![],
            kek(),
            refused(KeyRefusal::NoRing),
        ),
        (
            "a ring under another KEK",
            vec![
                (KEY_RING_SETTING, &finished),
                (CATALOG_ID_SETTING, CATALOG_ID),
            ],
            KeySource::Kek(Zeroizing::new(OTHER_KEK).into()),
            refused(KeyRefusal::KekMismatch {
                ring: kek_id(&KEK),
                configured: kek_id(&OTHER_KEK),
            }),
        ),
        (
            "a ring whose DEK fails to unwrap, catalog key present too",
            vec![
                (CATALOG_KEY_SETTING, CATALOG_KEY_B64),
                (KEY_RING_SETTING, &flipped),
                (CATALOG_ID_SETTING, CATALOG_ID),
            ],
            kek(),
            refused(KeyRefusal::DamagedRing {
                reason: "DEK 1 fails to unwrap".to_owned(),
            }),
        ),
        (
            "a ring and no catalog_id row",
            vec![(KEY_RING_SETTING, &finished)],
            kek(),
            refused(KeyRefusal::DamagedRing {
                reason: "the catalog has no catalog_id row, which the ring's AAD holds".to_owned(),
            }),
        ),
        (
            "an unfinished switch, key_file set",
            vec![
                (CATALOG_KEY_SETTING, CATALOG_KEY_B64),
                (KEY_RING_SETTING, &unfinished),
                (CATALOG_ID_SETTING, CATALOG_ID),
            ],
            kek(),
            refused(KeyRefusal::UnfinishedSwitchWithKeyFile),
        ),
        (
            "a catalog key beside a finished ring",
            vec![
                (CATALOG_KEY_SETTING, CATALOG_KEY_B64),
                (KEY_RING_SETTING, &finished),
                (CATALOG_ID_SETTING, CATALOG_ID),
            ],
            kek(),
            refused(KeyRefusal::CatalogKeyAfterSwitch),
        ),
        (
            "a ring with two DEKs",
            vec![
                (KEY_RING_SETTING, &rotating),
                (CATALOG_ID_SETTING, CATALOG_ID),
            ],
            kek(),
            refused(KeyRefusal::UnfinishedRotation),
        ),
        (
            "a ring with one DEK and no catalog key",
            vec![(KEY_RING_SETTING, &third), (CATALOG_ID_SETTING, CATALOG_ID)],
            kek(),
            Ok(Some(3)),
        ),
    ];
    for (case, rows, source, expected) in cases {
        let catalog = FakeCatalog::with_settings(&rows);
        let loaded = load_secret_keys(&catalog, source)
            .await
            .map(|keys| keys.expect("a key").dek_id())
            .map_err(|e| e.to_string());
        assert_eq!(loaded, expected, "{case}");
    }

    // No key: nothing is read, whatever the catalog holds.
    let catalog = FakeCatalog::with_settings(&[(KEY_RING_SETTING, "{}")]);
    assert!(
        load_secret_keys(&catalog, KeySource::None)
            .await
            .expect("no key")
            .is_none()
    );
}
