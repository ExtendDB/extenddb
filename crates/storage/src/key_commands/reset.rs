// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use crate::key_pass::{TableCounts, for_each_page};
use crate::key_ring::{KeyRefusal, KeyRing, KeyRows, kek_id, new_dek, open_ring};
use crate::secret_cipher::KEY_LEN;
use crate::secret_keys::{DATA_CONNECTION_SETTING, SecretKeys, SecretTable};

use super::{
    KekFile, KeyCommandError, KeyStores, Outcome, Preview, RunFlags, Summary, catalog_id,
    delete_catalog_key, delete_secret_rows, erase_free_space, read_kek, refuse, refused, seal,
    write_ring,
};

/// `extenddb encryption-key reset --key-file <path> --discard-secrets`
/// (section 8.6). `discard_secrets` is the flag; without it the command
/// refuses. `data_connection` is the PostgreSQL connection string that the
/// app read from standard input, which step 3 writes encrypted under the new
/// DEK; `None` on the other backends.
///
/// # Errors
///
/// [`KeyCommandError`]; a run that stops before its ring write runs again
/// from the start.
pub async fn reset(
    stores: KeyStores<'_>,
    kek: &dyn KekFile,
    flags: RunFlags,
    discard_secrets: bool,
    data_connection: Option<&str>,
) -> Result<Outcome, KeyCommandError> {
    if !discard_secrets {
        return Err(refuse(
            "reset deletes every access key and every session, and keeps every user, role, \
             policy, group, account and table. Pass --discard-secrets to run it",
        ));
    }
    let kek_file = kek;
    let kek = read_kek(kek_file)?;
    let rows = KeyRows::read(stores.settings).await?;
    let old_ring = rows.ring.as_deref().map(KeyRing::parse);
    if old_ring.is_none() && rows.catalog_key.is_some() {
        return Err(if flags.key_file_configured {
            refused(KeyRefusal::CatalogKeyWithoutRing)
        } else {
            refuse(
                "the catalog uses the catalog key of its encryption_key row and has no key \
                 ring, so no KEK is lost. `extenddb encryption-key switch --key-file <path>` \
                 moves it to a KEK file",
            )
        });
    }
    // A ring that names this KEK: the KEK is not lost.
    if let (Some(Ok(ring)), Some(text)) = (&old_ring, rows.ring.as_deref())
        && ring.kek.id == kek_id(&kek)
    {
        let open = open_ring(text, rows.catalog_id.as_deref(), &kek).map_err(refused)?;
        // A run that stopped at its rebuild rebuilds the file now.
        if flags.yes {
            erase_free_space(stores, "reset").await?;
        }
        return Ok(Outcome::NothingToDo {
            kek_id: open.ring.kek.id.clone(),
            dek_id: open.ring.active_dek_id(),
        });
    }
    let catalog_id = catalog_id(&rows)?;
    if data_connection.is_none()
        && stores
            .settings
            .get_setting(DATA_CONNECTION_SETTING)
            .await?
            .is_some()
    {
        return Err(refuse(
            "reset on PostgreSQL reads the data database connection string on standard input",
        ));
    }

    // Every row it deletes, by table.
    let mut doomed: Vec<(SecretTable, Vec<String>)> = Vec::new();
    let mut deleted_access_key_ids = Vec::new();
    for table in SecretTable::ALL {
        let mut keys = Vec::new();
        for_each_page(stores.management, table, |rows| {
            for row in rows {
                deleted_access_key_ids.push(row.access_key_id);
                keys.push(row.key);
            }
            Ok(Vec::new())
        })
        .await?;
        doomed.push((table, keys));
    }
    let next_id = match &old_ring {
        Some(Ok(ring)) => ring.next_dek_id().map_err(refused)?,
        _ => 1,
    };
    if !flags.yes {
        let mut steps: Vec<String> = doomed
            .iter()
            .map(|(table, keys)| format!("delete the {} rows of {}", keys.len(), table.name()))
            .collect();
        if rows.catalog_key.is_some() {
            steps.push("delete the encryption_key row".to_owned());
        }
        if data_connection.is_some() {
            steps
                .push("write the data_database_connection_string row under the new DEK".to_owned());
        }
        steps.push(format!(
            "write a new key ring with DEK {next_id} under the KEK {}",
            kek_id(&kek)
        ));
        if !deleted_access_key_ids.is_empty() {
            steps.push(format!(
                "the access keys and sessions deleted: {}",
                deleted_access_key_ids.join(", ")
            ));
        }
        return Ok(Outcome::Preview(Preview {
            steps,
            survey: None,
        }));
    }

    // Steps 2 to 4. The old ring stays until the last write.
    for (table, keys) in &doomed {
        delete_secret_rows(stores, *table, keys).await?;
    }
    if rows.catalog_key.is_some() {
        delete_catalog_key(stores, "reset").await?;
    }
    let dek = new_dek(next_id, catalog_id).map_err(refuse)?;
    if let Some(connection) = data_connection {
        let sealed = dek
            .encrypt_setting(connection)
            .map_err(|reason| refuse(format!("encrypt the connection string: {reason}")))?;
        stores
            .settings
            .set_setting(DATA_CONNECTION_SETTING, &sealed)
            .await?;
    }
    let ring = seal(&kek, catalog_id, &[&dek], false, None)?;
    // Conditional on the ring that this run read, when it read one: on
    // Cassandra a plain write after a conditional one can lose to it by
    // write time, and a `rewrap` can write the ring between the read and
    // this write. `write_ring` refuses only for a changed ring; the deletes
    // are done by now, so the refusal says what this run left.
    write_ring(stores.settings, &ring, "reset", rows.ring.as_deref())
        .await
        .map_err(|e| match e {
            KeyCommandError::Refused(_) => refuse(format!(
                "the key ring changed while `extenddb encryption-key reset` ran, so its ring \
                 write did not happen. The access_keys and iam_sessions rows are deleted{}. The \
                 key ring row holds the ring that the other command wrote.{} Run the command \
                 again",
                if rows.catalog_key.is_some() {
                    ", and so is the encryption_key row"
                } else {
                    ""
                },
                if data_connection.is_some() {
                    " The data_database_connection_string row is encrypted under a DEK that no \
                     ring holds until a run finishes."
                } else {
                    ""
                },
            )),
            other => other,
        })?;
    erase_free_space(stores, "reset").await?;
    Ok(Outcome::Done(Summary {
        kek_id: ring.kek.id,
        dek_id: next_id,
        key_names: vec![crate::key_pass::key_name(&dek)],
        tables: SecretTable::ALL.map(|table| (table, TableCounts::default())),
        data_connection_rewritten: data_connection.map(|_| true),
        unopenable: Vec::new(),
        deleted_access_key_ids,
        deleted_catalog_key: rows.catalog_key.is_some(),
        key_file: kek_file.path().to_owned(),
    }))
}

/// The DEK that `set-data-connection` (section 8.7) encrypts the connection
/// string under, from the ring text that the PostgreSQL method read with
/// `SELECT ... FOR SHARE`. Refused: no ring, a ring with
/// `switch_from_catalog_key` true, and a ring with two DEKs; and a ring that
/// the KEK does not open, in the order of section 7.
///
/// # Errors
///
/// The refusal's message.
pub fn data_connection_key(
    ring_text: Option<&str>,
    kek: &[u8; KEY_LEN],
    catalog_id: &str,
) -> Result<SecretKeys, String> {
    let text = ring_text.ok_or_else(|| {
        "the catalog has no key ring, so its data_database_connection_string row stays in \
         plaintext: edit it as the admin guide says"
            .to_owned()
    })?;
    let mut open = open_ring(text, Some(catalog_id), kek).map_err(|r| r.to_string())?;
    if open.ring.switch_from_catalog_key {
        return Err(KeyRefusal::UnfinishedSwitchWithKeyFile.to_string());
    }
    if open.deks.len() > 1 {
        return Err(KeyRefusal::UnfinishedRotation.to_string());
    }
    Ok(open.deks.pop().expect("an open ring holds a DEK"))
}
