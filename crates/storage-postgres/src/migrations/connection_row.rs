// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use extenddb_storage::key_ring::{KEY_RING_SETTING, open_ring};
use extenddb_storage::migrations::MigrationError;
use extenddb_storage::secret_keys::{CATALOG_ID_SETTING, DATA_CONNECTION_SETTING};
use sqlx::PgPool;
use zeroize::Zeroizing;

use super::connect::{relation_exists, sql_error};
use super::{DataKey, PostgresTarget};

/// The catalog's `data_database_connection_string` row as the server
/// connects with it; `None` when the catalog has no settings table, or no
/// such row and no key ring.
///
/// A catalog without a key ring keeps the row in plaintext. In a catalog
/// with a ring the row is encrypted under the ring's DEK, and `target`'s key
/// opens it before anything parses it (RFC-0000 section 12): a missing row,
/// a row that no DEK of the ring opens, and a plaintext row are refused,
/// except the plaintext row of an unfinished switch, which only the backend
/// that `encryption-key switch` opens accepts. A connection string that
/// `encryption-key reset` read from standard input takes the place of the
/// row, whatever the catalog holds.
pub(super) async fn connection_row(
    target: &PostgresTarget,
    catalog: &PgPool,
) -> Result<Option<Zeroizing<String>>, MigrationError> {
    if let DataKey::Given(given) = &target.data_key {
        return Ok(Some((**given).clone()));
    }
    if !relation_exists(catalog, "settings").await? {
        return Ok(None);
    }
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT key, value FROM settings WHERE key IN ($1, $2, $3)")
            .bind(DATA_CONNECTION_SETTING)
            .bind(KEY_RING_SETTING)
            .bind(CATALOG_ID_SETTING)
            .fetch_all(catalog)
            .await
            .map_err(|e| sql_error("read data_database_connection_string", &e))?;
    let value = |key: &str| rows.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
    let row = value(DATA_CONNECTION_SETTING);
    match value(KEY_RING_SETTING) {
        None => Ok(row.map(|r| Zeroizing::new(r.to_owned()))),
        Some(ring) => open_connection_row(&target.data_key, row, ring, value(CATALOG_ID_SETTING))
            .map(Some)
            .map_err(MigrationError::Refused),
    }
}

/// [`connection_row`] for a catalog with the key ring `ring`.
fn open_connection_row(
    key: &DataKey,
    row: Option<&str>,
    ring: &str,
    catalog_id: Option<&str>,
) -> Result<Zeroizing<String>, String> {
    const UNOPENED: &str = "the catalog's data_database_connection_string row does not open \
         under the DEK of its key ring, so the data database's password cannot be read; run \
         extenddb encryption-key status, and write the connection string with extenddb \
         encryption-key set-data-connection";
    let Some(row) = row.filter(|r| !r.is_empty()) else {
        return Err(
            "the catalog has a key ring and no data_database_connection_string row; \
             write the data database's connection string with extenddb encryption-key \
             set-data-connection"
                .to_owned(),
        );
    };
    match key {
        DataKey::None => Err(
            "the catalog has a key ring, so its data_database_connection_string row is \
             encrypted; run the command with the configuration whose [auth.encryption] \
             key_file names the KEK of the ring"
                .to_owned(),
        ),
        DataKey::Loaded(keys) => keys.decrypt_setting(row).map_err(|_| UNOPENED.to_owned()),
        // `connection_row` returns the given string before it reads a row.
        DataKey::Given(given) => Ok((**given).clone()),
        DataKey::Kek {
            kek,
            plaintext_during_switch,
        } => {
            let open = open_ring(ring, catalog_id, kek).map_err(|refusal| refusal.to_string())?;
            if let Some(opened) = open
                .deks
                .iter()
                .find_map(|dek| dek.decrypt_setting(row).ok())
            {
                return Ok(opened);
            }
            if *plaintext_during_switch && open.ring.switch_from_catalog_key {
                return Ok(Zeroizing::new(row.to_owned()));
            }
            Err(UNOPENED.to_owned())
        }
    }
}
