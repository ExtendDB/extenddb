// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use crate::key_pass::{PassKeys, PassOptions, run_pass, survey};
use crate::key_ring::{
    KeyRefusal, KeyRows, RESTORE_STEPS, catalog_key_id, kek_id, new_dek, open_ring,
};

use super::{
    KekFile, KeyCommandError, KeyStores, Outcome, Preview, RunFlags, Summary, catalog_id,
    catalog_key, catalog_key_is_retiring, catalog_key_not_accepted, check_before_write, dek_id,
    delete_catalog_key, erase_free_space, read_kek, refuse, refused, reread_kek, seal, untouched,
    write_ring,
};

/// `extenddb encryption-key switch --key-file <path>` (section 8.1), with the
/// resume of an earlier run and the `--accept-catalog-key` case of 8.5.
///
/// # Errors
///
/// [`KeyCommandError`]; a run that stops runs again with the same arguments.
pub async fn switch(
    stores: KeyStores<'_>,
    kek: &dyn KekFile,
    flags: RunFlags,
) -> Result<Outcome, KeyCommandError> {
    /// What the run does, decided from the key rows.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Plan {
        /// No ring: steps 2 to 7.
        Fresh,
        /// The ring's switch did not finish. With the catalog key row:
        /// steps 4 to 7; without it, steps 6 and 7.
        Resume { catalog_key: bool },
        /// A finished ring and `--accept-catalog-key`: steps 4, 5 and 7.
        Accept,
    }

    let kek_file = kek;
    let kek = read_kek(kek_file)?;
    let rows = KeyRows::read(stores.settings).await?;
    let open = rows
        .ring
        .as_deref()
        .map(|text| open_ring(text, rows.catalog_id.as_deref(), &kek))
        .transpose()
        .map_err(refused)?;
    if let Some(open) = &open
        && open.deks.len() == 1
        && !open.ring.switch_from_catalog_key
        && rows.catalog_key.is_none()
    {
        // A run that stopped at its rebuild rebuilds the file now.
        if flags.yes {
            erase_free_space(stores, "switch").await?;
        }
        return Ok(Outcome::NothingToDo {
            kek_id: open.ring.kek.id.clone(),
            dek_id: open.ring.active_dek_id(),
        });
    }
    if flags.key_file_configured {
        return Err(refuse(
            "the configuration sets [auth.encryption] key_file, which is set only after a \
             switch finishes. Unset it for this run (the line, or the \
             EXTENDDB__AUTH__ENCRYPTION__KEY_FILE override), then run the switch again",
        ));
    }
    let catalog_id = catalog_id(&rows)?.to_owned();
    let catalog_key = catalog_key(&rows)?;
    if catalog_key
        .as_ref()
        .is_some_and(|key| key.key_bytes().expose() == &*kek)
    {
        return Err(refuse(
            "the KEK equals the catalog key, which every catalog copy taken before the switch \
             holds. Write a KEK of new random bytes: head -c 32 /dev/urandom | base64",
        ));
    }
    if open.as_ref().is_some_and(|open| {
        open.deks
            .iter()
            .any(|dek| dek.key_bytes().expose() == &*kek)
    }) {
        return Err(refuse(
            "the KEK equals a DEK of the key ring. Write a KEK of new random bytes",
        ));
    }
    let not_finished_only =
        || refuse("--accept-catalog-key applies only to a catalog whose switch finished");
    let plan = match (&open, &catalog_key) {
        (None, None) => {
            return Err(refuse(
                "the catalog has no encryption_key row to switch from. Run `extenddb init` first",
            ));
        }
        (None, Some(_)) if flags.accept_catalog_key => return Err(not_finished_only()),
        (None, Some(_)) => Plan::Fresh,
        (Some(open), _) if open.ring.switch_from_catalog_key => {
            if flags.accept_catalog_key {
                return Err(not_finished_only());
            }
            if open.deks.len() != 1 {
                return Err(refused(KeyRefusal::DamagedRing {
                    reason: "a switch writes a ring with one DEK".to_owned(),
                }));
            }
            match &catalog_key {
                Some(key) if catalog_key_is_retiring(&open.ring, &kek, key) => {
                    Plan::Resume { catalog_key: true }
                }
                Some(_) => {
                    return Err(refuse(format!(
                        "the catalog's encryption_key row is not the catalog key that the \
                         unfinished switch retires: it appeared after the switch started, and \
                         no option accepts it. Run `extenddb encryption-key status`, and \
                         {RESTORE_STEPS}"
                    )));
                }
                None => Plan::Resume { catalog_key: false },
            }
        }
        (Some(open), Some(_)) if open.deks.len() == 1 => {
            if !flags.accept_catalog_key {
                return Err(catalog_key_not_accepted());
            }
            Plan::Accept
        }
        (Some(_), _) => return Err(refused(KeyRefusal::UnfinishedRotation)),
    };

    // Step 2, and the preview: check the stored secrets with the keys the
    // run holds before its first write.
    let check = match (&plan, &open, &catalog_key) {
        (Plan::Fresh, _, Some(key)) => PassKeys {
            target: key,
            old: Vec::new(),
        },
        (_, Some(open), key) => PassKeys {
            target: open.active(),
            old: key.iter().collect(),
        },
        _ => unreachable!("every plan but Fresh has a ring"),
    };
    let survey = survey(stores.management, stores.settings, &check).await?;
    // The plaintext connection string row is the form before a switch, and
    // only a run that still holds the catalog key encrypts it.
    let plaintext_data_connection =
        matches!(plan, Plan::Fresh | Plan::Resume { catalog_key: true });
    if !flags.yes {
        let mut steps = Vec::new();
        if plan == Plan::Fresh {
            steps.push(format!(
                "write the key ring with DEK 1 under the KEK {}",
                kek_id(&kek)
            ));
        }
        if plan != (Plan::Resume { catalog_key: false }) {
            steps.push("re-encrypt each stored secret under the catalog key under the DEK".into());
            steps.push("delete the encryption_key row".into());
        }
        if plan != Plan::Accept {
            steps.push("mark the switch finished in the key ring".into());
        }
        return Ok(Outcome::Preview(Preview {
            steps,
            survey: Some(survey),
        }));
    }
    check_before_write(&survey, flags, plaintext_data_connection)?;

    // Step 3: the ring of an unfinished switch.
    let fresh = match (&plan, &catalog_key) {
        (Plan::Fresh, Some(key)) => {
            let dek = new_dek(1, &catalog_id).map_err(refuse)?;
            let ring = seal(
                &kek,
                &catalog_id,
                &[&dek],
                true,
                Some(catalog_key_id(&kek, key)),
            )?;
            write_ring(stores.settings, &ring, "switch", None).await?;
            Some((ring, dek))
        }
        _ => None,
    };
    let (ring, target) = match (&fresh, &open) {
        (Some((ring, dek)), _) => (ring, dek),
        (None, Some(open)) => (&open.ring, open.active()),
        (None, None) => unreachable!("every plan but Fresh has a ring"),
    };

    // Steps 4 and 5: the pass, then the catalog key goes.
    let summary_parts = match &catalog_key {
        Some(key) if plan != (Plan::Resume { catalog_key: false }) => {
            let pass = run_pass(
                stores.management,
                stores.settings,
                &PassKeys {
                    target,
                    old: vec![key],
                },
                PassOptions {
                    keep_unopenable: flags.keep_unopenable,
                    plaintext_data_connection,
                },
            )
            .await?;
            reread_kek(kek_file, ring)?;
            delete_catalog_key(stores, "switch").await?;
            (
                pass.key_names,
                pass.tables,
                pass.unopenable,
                pass.data_connection_rewritten,
                true,
            )
        }
        _ => (
            survey.key_names.clone(),
            survey.tables.clone(),
            survey.unopenable().into_iter().cloned().collect(),
            untouched(survey.data_connection),
            false,
        ),
    };

    // Step 6: the switch is finished. A run with --accept-catalog-key writes
    // no new DEK and no ring.
    if plan != Plan::Accept {
        let finished = seal(&kek, &catalog_id, &[target], false, None)?;
        write_ring(stores.settings, &finished, "switch", None).await?;
    }
    erase_free_space(stores, "switch").await?;
    let (key_names, tables, unopenable, data_connection_rewritten, deleted_catalog_key) =
        summary_parts;
    Ok(Outcome::Done(Summary {
        kek_id: kek_id(&kek),
        dek_id: dek_id(target),
        key_names,
        tables,
        data_connection_rewritten,
        unopenable,
        deleted_access_key_ids: Vec::new(),
        deleted_catalog_key,
        key_file: kek_file.path().to_owned(),
    }))
}
