// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// ── AAD encoding ───────────────────────────────────────────────────────

#[test]
fn aad_encoding() {
    // (case, AAD under test, expected text)
    let session = SecretRow::session(
        "ASIAEXAMPLE|0000001",
        "111122223333",
        "Admin",
        "s1",
        1_763_294_400,
    );
    let access_key = SecretRow::access_key("AKIAEXAMPLE|0000001", "111122223333", "bob");
    let cases: &[(&str, String, &str)] = &[
        (
            "the RFC's wrapped DEK example, with a null retiring id",
            encode_aad(
                AadKind::Dek,
                &[
                    "1",
                    "0f3c2a1e-8b7d-4c6e-9f10-112233445566",
                    "file",
                    "3f9a0c51e2d47b86",
                    "2",
                    "2",
                    "false",
                    "",
                ],
            ),
            "extenddb-dek-v1|1:1|36:0f3c2a1e-8b7d-4c6e-9f10-112233445566|4:file|\
             16:3f9a0c51e2d47b86|1:2|1:2|5:false|0:",
        ),
        (
            "a session under a DEK: a | inside the access key ID",
            dek(1).aad(&session),
            "extenddb-secret-v1|36:0f3c2a1e-8b7d-4c6e-9f10-112233445566|12:iam_sessions|\
             19:ASIAEXAMPLE|0000001|12:111122223333|5:Admin|2:s1|10:1763294400",
        ),
        (
            "an access key under a DEK: empty session name and expiry",
            dek(1).aad(&access_key),
            "extenddb-secret-v1|36:0f3c2a1e-8b7d-4c6e-9f10-112233445566|11:access_keys|\
             19:AKIAEXAMPLE|0000001|12:111122223333|3:bob|0:|0:",
        ),
        (
            "an access key under the catalog key: the access key ID alone",
            catalog_key().aad(&access_key),
            "AKIAEXAMPLE|0000001",
        ),
        (
            "the connection string row",
            encode_aad(
                AadKind::Setting,
                &[CATALOG_ID, "data_database_connection_string"],
            ),
            "extenddb-setting-v1|36:0f3c2a1e-8b7d-4c6e-9f10-112233445566|\
             31:data_database_connection_string",
        ),
        (
            "every field empty",
            encode_aad(AadKind::Secret, &["", ""]),
            "extenddb-secret-v1|0:|0:",
        ),
    ];
    for (case, aad, expected) in cases {
        assert_eq!(aad, expected, "{case}");
    }

    // With a plain separator these two rows would share an AAD: a `|`
    // moves from the access key ID into the account ID.
    let left = SecretRow::access_key("AKIAAAAA|1", "2", "bob");
    let right = SecretRow::access_key("AKIAAAAA", "1|2", "bob");
    assert_ne!(dek(1).aad(&left), dek(1).aad(&right));
    let stored = dek(1).encrypt(&left, "secret").expect("encrypt");
    assert_eq!(
        dek(1).decrypt(&right, &stored).unwrap_err(),
        "decrypt: authentication failed"
    );
    assert_eq!(*dek(1).decrypt(&left, &stored).expect("decrypt"), "secret");
}

/// The AAD binds a stored secret to each field of its row: the same
/// ciphertext under the same DEK does not open for a row that differs in any
/// one field, and opens for the row it was sealed for.
#[test]
fn a_secret_does_not_open_for_a_row_moved_in_any_field() {
    let row = SecretRow {
        table: SecretTable::IamSessions,
        access_key_id: "ASIAEXAMPLE0000001",
        account_id: "111122223333",
        principal_name: "Admin",
        session_name: Some("s1"),
        expires_at: Some(1_763_294_400),
    };
    let key = dek(1);
    let stored = key.encrypt(&row, "secret").expect("encrypt");
    assert_eq!(*key.decrypt(&row, &stored).expect("decrypt"), "secret");
    // (field, the row with only that field changed)
    let moved: [(&str, SecretRow<'_>); 6] = [
        (
            "table",
            SecretRow {
                table: SecretTable::AccessKeys,
                ..row
            },
        ),
        (
            "access key ID",
            SecretRow {
                access_key_id: "ASIAEXAMPLE0000002",
                ..row
            },
        ),
        (
            "account",
            SecretRow {
                account_id: "444455556666",
                ..row
            },
        ),
        (
            "principal",
            SecretRow {
                principal_name: "Other",
                ..row
            },
        ),
        (
            "session name",
            SecretRow {
                session_name: Some("s2"),
                ..row
            },
        ),
        (
            "expiry",
            SecretRow {
                expires_at: Some(1_763_294_401),
                ..row
            },
        ),
    ];
    for (field, other) in moved {
        match key.decrypt(&other, &stored) {
            Ok(_) => panic!("{field}: the secret opens for the moved row"),
            Err(e) => assert_eq!(e, "decrypt: authentication failed", "{field}"),
        }
    }
    // The catalog id: the same DEK bytes and id in another catalog.
    let elsewhere = SecretKeys::dek_for_tests([1; 32], 1, "another-catalog");
    match elsewhere.decrypt(&row, &stored) {
        Ok(_) => panic!("catalog id: the secret opens in another catalog"),
        Err(e) => assert_eq!(e, "decrypt: authentication failed", "catalog id"),
    }
}

// ── The key ring ───────────────────────────────────────────────────────

#[test]
fn ring_tampering() {
    let one = ring_text(&KEK, &[2], false, None);
    let two = ring_text(&KEK, &[1, 2], false, None);
    let other_catalog = {
        let other = SecretKeys::dek_for_tests([2; 32], 2, "another-catalog");
        KeyRing::seal(&KEK, "another-catalog", &[&other], false, None)
            .expect("seal")
            .to_json()
    };
    let retiring = Some(KeyId::parse("00112233aabbccdd").expect("id"));
    let damaged = |reason: &str| {
        Expect::Refused(KeyRefusal::DamagedRing {
            reason: reason.to_owned(),
        })
    };
    enum Expect {
        /// The ring opens, and its active DEK is 2.
        Opens,
        /// The parser refuses it; the reason contains this text.
        Unparsed(&'static str),
        Refused(KeyRefusal),
    }
    use Expect::{Opens, Unparsed};

    let cases: Vec<(&str, String, Expect)> = vec![
        ("an untouched ring", one.clone(), Opens),
        (
            "a flipped switch_from_catalog_key",
            edit_ring(&one, |r| r["switch_from_catalog_key"] = json!(true)),
            damaged("DEK 2 fails to unwrap"),
        ),
        (
            "a changed retiring_catalog_key_id",
            edit_ring(&one, |r| {
                r["retiring_catalog_key_id"] = json!("00112233aabbccdd")
            }),
            damaged("DEK 2 fails to unwrap"),
        ),
        (
            "a retiring id removed from an unfinished switch",
            edit_ring(&ring_text(&KEK, &[2], true, retiring.clone()), |r| {
                r["retiring_catalog_key_id"] = Value::Null
            }),
            damaged("DEK 2 fails to unwrap"),
        ),
        (
            "an added DEK entry",
            edit_ring(&one, |r| {
                let mut entry = r["keys"][0].clone();
                entry["id"] = json!(3);
                r["keys"].as_array_mut().unwrap().push(entry);
            }),
            damaged("DEKs 2, 3 fail to unwrap"),
        ),
        (
            "a deleted DEK entry",
            edit_ring(&two, |r| {
                r["keys"].as_array_mut().unwrap().remove(0);
            }),
            damaged("DEK 2 fails to unwrap"),
        ),
        (
            "a wrapped DEK copied from another catalog's ring",
            edit_ring(&one, |r| {
                let other: Value = serde_json::from_str(&other_catalog).unwrap();
                r["keys"][0]["wrapped"] = other["keys"][0]["wrapped"].clone();
            }),
            damaged("DEK 2 fails to unwrap"),
        ),
        (
            "a changed DEK id",
            edit_ring(&one, |r| r["keys"][0]["id"] = json!(5)),
            damaged("DEK 5 fails to unwrap"),
        ),
        (
            "an unknown field",
            edit_ring(&one, |r| r["comment"] = json!("x")),
            Unparsed("unknown field `comment`"),
        ),
        (
            "an unknown field in a DEK entry",
            edit_ring(&one, |r| r["keys"][0]["created"] = json!(1)),
            Unparsed("unknown field `created`"),
        ),
        (
            "a version that is a string",
            edit_ring(&one, |r| r["version"] = json!("1")),
            Unparsed("invalid type: string \"1\", expected u64"),
        ),
        (
            "a DEK id that is a string",
            edit_ring(&one, |r| r["keys"][0]["id"] = json!("2")),
            Unparsed("invalid type: string \"2\", expected u64"),
        ),
        (
            "a DEK id that is a float",
            edit_ring(&one, |r| r["keys"][0]["id"] = json!(2.0)),
            Unparsed("invalid type: floating point `2.0`, expected u64"),
        ),
        (
            "a flag that is a string",
            edit_ring(&one, |r| r["switch_from_catalog_key"] = json!("false")),
            Unparsed("expected a boolean"),
        ),
        (
            "an empty-string retiring id",
            edit_ring(&one, |r| r["retiring_catalog_key_id"] = json!("")),
            Unparsed("a key id is 16 lowercase hex characters, not \"\""),
        ),
        (
            "a missing retiring id",
            edit_ring(&one, |r| {
                r.as_object_mut().unwrap().remove("retiring_catalog_key_id");
            }),
            Unparsed("missing field `retiring_catalog_key_id`"),
        ),
        (
            "an unknown KEK source",
            edit_ring(&one, |r| r["kek"]["source"] = json!("pkcs11")),
            Unparsed("unknown variant `pkcs11`"),
        ),
        (
            "a KEK id in upper case",
            edit_ring(&one, |r| {
                let id = r["kek"]["id"].as_str().unwrap().to_uppercase();
                r["kek"]["id"] = json!(id);
            }),
            Unparsed("a key id is 16 lowercase hex characters"),
        ),
        (
            "another version",
            edit_ring(&one, |r| r["version"] = json!(2)),
            Unparsed("version 2 is not the ring version 1 of this release"),
        ),
        (
            "two entries with one id",
            edit_ring(&one, |r| {
                let entry = r["keys"][0].clone();
                r["keys"].as_array_mut().unwrap().push(entry);
            }),
            Unparsed("DEK ids start at 1 and each appears once"),
        ),
        (
            "no DEK",
            edit_ring(&one, |r| r["keys"] = json!([])),
            Unparsed("a ring holds one or two DEKs, not 0"),
        ),
        (
            "a ring under another KEK",
            ring_text(&OTHER_KEK, &[2], false, None),
            Expect::Refused(KeyRefusal::KekMismatch {
                ring: kek_id(&OTHER_KEK),
                configured: kek_id(&KEK),
            }),
        ),
        ("a two-DEK ring", two.clone(), Opens),
        (
            "a two-DEK ring with its entries in the other order",
            edit_ring(&two, |r| r["keys"].as_array_mut().unwrap().reverse()),
            Opens,
        ),
    ];
    for (case, text, expected) in cases {
        let opened = open_ring(&text, Some(CATALOG_ID), &KEK);
        match (expected, opened) {
            (Opens, Ok(open)) => assert_eq!(open.active().dek_id(), Some(2), "{case}"),
            (Unparsed(part), Err(KeyRefusal::DamagedRing { reason })) => assert!(
                reason.starts_with("the ring does not parse: ") && reason.contains(part),
                "{case}: {reason}"
            ),
            (Expect::Refused(expected), Err(refusal)) => assert_eq!(refusal, expected, "{case}"),
            (_, opened) => panic!("{case}: {:?}", opened.map(|o| o.ring)),
        }
    }
}

/// Each section of the backup manual that a refusal names is a heading of
/// that manual. A ring under another KEK sends a ring row written back from
/// a copy to the restore steps, and a lost KEK to the steps that run `reset`.
#[test]
fn the_restore_sections_the_refusals_name_are_headings_of_the_manual() {
    let manual = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/manuals/14-backup-and-restore.md"),
    )
    .expect("read the backup manual");
    for pointer in [RESTORE_STEPS, LOST_KEK_STEPS] {
        let heading = pointer.split('"').nth(1).expect("a quoted heading");
        assert!(
            manual.lines().any(|line| line == format!("### {heading}")),
            "no heading {heading:?} in the backup manual"
        );
    }
    let mismatch = KeyRefusal::KekMismatch {
        ring: kek_id(&OTHER_KEK),
        configured: kek_id(&KEK),
    }
    .to_string();
    assert!(mismatch.contains(RESTORE_STEPS), "{mismatch}");
    assert!(mismatch.ends_with(LOST_KEK_STEPS), "{mismatch}");
}

#[test]
fn ring_round_trip_and_ids() {
    let text = ring_text(
        &KEK,
        &[1, 2],
        true,
        Some(catalog_key_id(&KEK, &catalog_key())),
    );
    let ring = KeyRing::parse(&text).expect("parse");
    assert_eq!(ring.to_json(), text);
    assert_eq!(ring.dek_ids(), [1, 2]);
    assert_eq!(ring.active_dek_id(), 2);
    assert_eq!(ring.next_dek_id().expect("a next id"), 3);
    assert!(text.starts_with(&format!(
        "{{\"version\":1,\"kek\":{{\"source\":\"file\",\"id\":\"{}\"}},\"keys\":[{{\"id\":1,",
        kek_id(&KEK)
    )));
    assert_ne!(kek_id(&KEK), kek_id(&OTHER_KEK));
    assert_ne!(
        catalog_key_id(&KEK, &catalog_key()),
        catalog_key_id(&OTHER_KEK, &catalog_key())
    );
    // The DEK bytes survive the wrap: a secret under the DEK opens under
    // the unwrapped one.
    let row = SecretRow::access_key("AKIAEXAMPLE0000001", "111122223333", "bob");
    let stored = dek(2).encrypt(&row, "secret").expect("encrypt");
    let open = open_ring(&text, Some(CATALOG_ID), &KEK).expect("open");
    assert_eq!(
        *open.active().decrypt(&row, &stored).expect("decrypt"),
        "secret"
    );
    assert_eq!(open.older().and_then(SecretKeys::dek_id), Some(1));
    // A new DEK differs from every other.
    let (a, b) = (
        new_dek(3, CATALOG_ID).unwrap(),
        new_dek(3, CATALOG_ID).unwrap(),
    );
    assert_eq!(
        a.decrypt(&row, &b.encrypt(&row, "s").unwrap()).unwrap_err(),
        "decrypt: authentication failed"
    );
}

#[test]
fn kek_file_format() {
    let good = "S0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0s=";
    // (contents, expected error, or None when the file parses to KEK)
    let cases: &[(String, Option<&str>)] = &[
        (good.to_owned(), None),
        (format!("{good}\n"), None),
        (
            format!("{good}\n\n"),
            Some(
                "a KEK file holds 44 characters of base64 and at most one trailing newline; this one holds 46 bytes",
            ),
        ),
        (
            format!(" {good}"),
            Some(
                "a KEK file holds 44 characters of base64 and at most one trailing newline; this one holds 45 bytes",
            ),
        ),
        (
            format!("{good}\r\n"),
            Some(
                "a KEK file holds 44 characters of base64 and at most one trailing newline; this one holds 46 bytes",
            ),
        ),
        (
            "S0tL-0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0s=".to_owned(),
            Some("decode encryption key: invalid symbol at offset 4"),
        ),
        (
            "S0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tL".to_owned(),
            Some(
                "a KEK file holds 44 characters of base64 and at most one trailing newline; this one holds 48 bytes",
            ),
        ),
        (
            // 31 bytes.
            "S0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLSw==".to_owned(),
            Some("encryption key must be 32 bytes"),
        ),
    ];
    for (contents, expected) in cases {
        let parsed = crate::key_ring::parse_kek_file(contents.as_bytes());
        match expected {
            None => assert_eq!(**parsed.expect("parse"), KEK, "{contents:?}"),
            Some(expected) => assert_eq!(&parsed.unwrap_err(), expected, "{contents:?}"),
        }
    }
}
