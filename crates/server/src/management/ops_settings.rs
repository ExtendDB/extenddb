// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Settings validation and write operations.
//!
//! Validation logic lives here in the server layer. The actual database
//! write is delegated to the `SettingsStore` trait implementation.

use extenddb_storage::management_store::{OpError, OpResult};

/// Validator function for a setting value.
pub type Validator = fn(&str) -> Result<(), &'static str>;

/// Known writable setting keys and their validators.
pub const KNOWN_KEYS: &[(&str, Validator)] = &[
    ("allow_credential_import", validate_bool),
    ("control_plane_delay_seconds", validate_delay_seconds),
    (
        extenddb_core::settings_keys::INDEX_PROPAGATION_DELAY_MS,
        validate_index_propagation_delay_ms,
    ),
    // Deprecated alias, still writable so an existing script or runbook keeps
    // working. `set_setting` canonicalises it, so it updates the same row rather
    // than creating a second one that the read path would ignore.
    (
        extenddb_core::settings_keys::LEGACY_GSI_PROPAGATION_DELAY_MS,
        validate_index_propagation_delay_ms,
    ),
    ("log_level", validate_log_level),
    ("sqlx_log_level", validate_log_level),
    ("throttling_enabled", validate_bool),
    // A test lever, writable for the same reason the propagation delay is: the
    // ordering property it exists to expose (a write landing mid-backfill must not
    // be overwritten by the backfill's older snapshot) cannot be observed unless a
    // test can slow the backfill down from outside the process.
    (
        extenddb_core::settings_keys::VECTOR_BACKFILL_BATCH_DELAY_MS,
        validate_backfill_batch_delay_ms,
    ),
];

/// Read-only keys that cannot be changed via the settings API.
pub const READONLY_KEYS: &[&str] = &[
    "catalog_version",
    "data_database_connection_string",
    "data_database_name",
];

fn validate_log_level(value: &str) -> Result<(), &'static str> {
    match value {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err("must be one of: trace, debug, info, warn, error"),
    }
}

/// Milliseconds, bounded so a mistyped value cannot wedge a backfill indefinitely.
///
/// The cap is generous next to any legitimate test need and small enough that the
/// worst case is a slow backfill rather than one that never finishes.
fn validate_backfill_batch_delay_ms(value: &str) -> Result<(), &'static str> {
    match value.parse::<u64>() {
        Ok(ms) if ms <= 60_000 => Ok(()),
        Ok(_) => Err("must be between 0 and 60000 milliseconds"),
        Err(_) => Err("must be a non-negative integer number of milliseconds"),
    }
}

fn validate_bool(value: &str) -> Result<(), &'static str> {
    match value {
        "true" | "false" => Ok(()),
        _ => Err("must be 'true' or 'false'"),
    }
}

fn validate_delay_seconds(value: &str) -> Result<(), &'static str> {
    match value.parse::<f64>() {
        Ok(v) if (0.0..=300.0).contains(&v) => Ok(()),
        Ok(_) => Err("must be between 0 and 300"),
        Err(_) => Err("must be a non-negative number"),
    }
}

fn validate_index_propagation_delay_ms(value: &str) -> Result<(), &'static str> {
    match value.parse::<u32>() {
        Ok(0..=10000) => Ok(()),
        Ok(_) => Err("must be between 0 and 10000"),
        Err(_) => Err("must be a non-negative integer"),
    }
}

/// Set a runtime setting with validation.
///
/// Validates the key and value, then delegates the write to the
/// `SettingsStore` implementation. Validation stays in the server layer;
/// the storage layer trusts validated input.
///
/// # Errors
///
/// Returns `OpError::Validation` if the key is read-only, unknown, or the value
/// fails validation. Returns `OpError::Internal` on database errors.
pub async fn set_setting(
    store: &dyn extenddb_storage::management_store::SettingsStore,
    key: &str,
    value: &str,
) -> OpResult<()> {
    if READONLY_KEYS.contains(&key) {
        return Err(OpError::Validation(format!("Setting '{key}' is read-only")));
    }

    let known = KNOWN_KEYS.iter().find(|(k, _)| *k == key);
    if let Some((_, validator)) = known {
        validator(value).map_err(|reason| {
            OpError::Validation(format!("Invalid value for '{key}': {reason}"))
        })?;
    } else {
        return Err(OpError::Validation(format!(
            "Unknown setting '{key}'. Known writable keys: {}",
            KNOWN_KEYS
                .iter()
                .map(|(k, _)| *k)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    // Write under the canonical name, so setting the deprecated alias updates the row
    // the read path actually consults instead of adding a second, ignored one.
    let key = extenddb_core::settings_keys::canonical_key(key);
    store.set_setting(key, value).await?;

    tracing::warn!(
        target: "extenddb::audit::settings",
        "settings-set: key={key}, value={value}",
    );
    Ok(())
}
