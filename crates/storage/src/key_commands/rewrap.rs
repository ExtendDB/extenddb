// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use crate::key_pass::{PassKeys, PassOptions, run_pass, survey};
use crate::key_ring::{KeyRefusal, KeyRows, new_dek, open_ring};

use super::{
    KekFile, KeyCommandError, KeyStores, Outcome, Preview, RunFlags, Summary, catalog_id,
    catalog_key, catalog_key_not_accepted, check_before_write, dek_id, delete_catalog_key,
    erase_free_space, read_kek, refuse, refused, reread_kek, ring_row, seal, untouched, write_ring,
};

/// `extenddb encryption-key rewrap --key-file <new path>` (section 8.2):
/// the same DEK wrapped under the new KEK, one ring write. Runs beside a
/// server.
///
/// # Errors
///
/// [`KeyCommandError`].
pub async fn rewrap(
    stores: KeyStores<'_>,
    current: &dyn KekFile,
    new: &dyn KekFile,
    yes: bool,
) -> Result<Outcome, KeyCommandError> {
    let current_kek = read_kek(current)?;
    let new_kek = read_kek(new)?;
    let rows = KeyRows::read(stores.settings).await?;
    let text = ring_row(&rows)?;
    // A rewrap to the file the ring already names has nothing to do.
    if let Ok(open) = open_ring(text, rows.catalog_id.as_deref(), &new_kek) {
        return Ok(Outcome::NothingToDo {
            kek_id: open.ring.kek.id.clone(),
            dek_id: open.ring.active_dek_id(),
        });
    }
    let open = open_ring(text, rows.catalog_id.as_deref(), &current_kek).map_err(refused)?;
    if open.ring.switch_from_catalog_key {
        return Err(refused(KeyRefusal::UnfinishedSwitchWithKeyFile));
    }
    if rows.catalog_key.is_some() {
        return Err(refused(KeyRefusal::CatalogKeyAfterSwitch));
    }
    if open.deks.len() > 1 {
        return Err(refused(KeyRefusal::UnfinishedRotation));
    }
    if open.active().key_bytes().expose() == &*new_kek {
        return Err(refuse(
            "the new KEK equals the DEK of the key ring. Write a KEK of new random bytes",
        ));
    }
    let catalog_id = catalog_id(&rows)?;
    let ring = seal(&new_kek, catalog_id, &[open.active()], false, None)?;
    let survey = survey(
        stores.management,
        stores.settings,
        &PassKeys {
            target: open.active(),
            old: Vec::new(),
        },
    )
    .await?;
    if !yes {
        return Ok(Outcome::Preview(Preview {
            steps: vec![format!(
                "write the key ring with DEK {} wrapped under the KEK {}",
                dek_id(open.active()),
                ring.kek.id
            )],
            survey: Some(survey),
        }));
    }
    write_ring(stores.settings, &ring, "rewrap", Some(text)).await?;
    Ok(Outcome::Done(Summary {
        kek_id: ring.kek.id.clone(),
        dek_id: dek_id(open.active()),
        unopenable: survey.unopenable().into_iter().cloned().collect(),
        key_names: survey.key_names,
        tables: survey.tables,
        data_connection_rewritten: untouched(survey.data_connection),
        deleted_access_key_ids: Vec::new(),
        deleted_catalog_key: false,
        key_file: new.path().to_owned(),
    }))
}

/// `extenddb encryption-key rotate` (section 8.3), with the resume of a ring
/// that holds two DEKs and the `--accept-catalog-key` case of 8.5.
///
/// # Errors
///
/// [`KeyCommandError`]; a run that stops runs again with the same arguments.
pub async fn rotate(
    stores: KeyStores<'_>,
    kek: &dyn KekFile,
    flags: RunFlags,
) -> Result<Outcome, KeyCommandError> {
    let kek_file = kek;
    let kek = read_kek(kek_file)?;
    let rows = KeyRows::read(stores.settings).await?;
    let text = ring_row(&rows)?;
    let open = open_ring(text, rows.catalog_id.as_deref(), &kek).map_err(refused)?;
    if open.ring.switch_from_catalog_key {
        return Err(refused(KeyRefusal::UnfinishedSwitchWithKeyFile));
    }
    let catalog_key = catalog_key(&rows)?;
    if catalog_key.is_some() && !flags.accept_catalog_key {
        return Err(catalog_key_not_accepted());
    }
    let catalog_id = catalog_id(&rows)?;

    // Check the stored secrets with each DEK of the ring, and the accepted
    // catalog key, before the first write.
    let check = PassKeys {
        target: open.active(),
        old: open
            .older()
            .into_iter()
            .chain(catalog_key.as_ref())
            .collect(),
    };
    let survey = survey(stores.management, stores.settings, &check).await?;
    let next_id = if open.older().is_some() {
        None
    } else {
        Some(open.ring.next_dek_id().map_err(refused)?)
    };
    if !flags.yes {
        let mut steps = Vec::new();
        if let Some(next_id) = next_id {
            steps.push(format!(
                "write the key ring with a new DEK {next_id} beside DEK {}",
                open.ring.active_dek_id()
            ));
        }
        steps.push(
            "re-encrypt each stored secret under the older keys under the new DEK".to_owned(),
        );
        if catalog_key.is_some() {
            steps.push("delete the encryption_key row".to_owned());
        }
        steps.push("write the key ring with the new DEK alone".to_owned());
        return Ok(Outcome::Preview(Preview {
            steps,
            survey: Some(survey),
        }));
    }
    check_before_write(&survey, flags, false)?;

    // Step 1: a fresh run adds the next DEK; a resumed run targets the
    // highest id. `two_deks` is the ring text the last write expects.
    let (fresh, two_deks) = if let Some(next_id) = next_id {
        let dek = new_dek(next_id, catalog_id).map_err(refuse)?;
        let ring = seal(&kek, catalog_id, &[open.active(), &dek], false, None)?;
        write_ring(stores.settings, &ring, "rotate", Some(text)).await?;
        (Some(dek), ring.to_json())
    } else {
        (None, text.to_owned())
    };
    let (target, older) = match &fresh {
        Some(dek) => (dek, open.active()),
        None => (
            open.active(),
            open.older().expect("a resumed rotate has two DEKs"),
        ),
    };

    // Steps 2 and 3.
    let pass = run_pass(
        stores.management,
        stores.settings,
        &PassKeys {
            target,
            old: std::iter::once(older).chain(catalog_key.as_ref()).collect(),
        },
        PassOptions {
            keep_unopenable: flags.keep_unopenable,
            plaintext_data_connection: false,
        },
    )
    .await?;
    reread_kek(kek_file, &open.ring)?;
    if catalog_key.is_some() {
        delete_catalog_key(stores, "rotate").await?;
    }
    let ring = seal(&kek, catalog_id, &[target], false, None)?;
    // The last write is conditional on the two-DEK ring too: on Cassandra a
    // plain write after a conditional one can lose to it by write time.
    // `write_ring` refuses only for a changed ring, and the pass has written
    // by now, so the refusal says what this run left.
    write_ring(stores.settings, &ring, "rotate", Some(&two_deks))
        .await
        .map_err(|e| match e {
            KeyCommandError::Refused(_) => refuse(format!(
                "the key ring changed after `extenddb encryption-key rotate` wrote its two \
                 DEKs, so its last ring write did not happen. The pass put the stored secrets \
                 under DEK {}; run the command again",
                dek_id(target)
            )),
            other => other,
        })?;
    erase_free_space(stores, "rotate").await?;
    Ok(Outcome::Done(Summary {
        kek_id: ring.kek.id,
        dek_id: dek_id(target),
        key_names: pass.key_names,
        tables: pass.tables,
        data_connection_rewritten: pass.data_connection_rewritten,
        unopenable: pass.unopenable,
        deleted_access_key_ids: Vec::new(),
        deleted_catalog_key: catalog_key.is_some(),
        key_file: kek_file.path().to_owned(),
    }))
}
