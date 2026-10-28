// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb settings` — read and write runtime settings (D-23).
//!
//! This is an infrastructure command for direct DB access when the server is
//! down. Validation logic is shared with the management API via `ops::KNOWN_KEYS`
//! and `ops::READONLY_KEYS`. Secret values (the encryption key, connection
//! strings carrying a password) are masked the same way the management API
//! masks them: `extenddb_config::should_redact` picks the keys and
//! `extenddb_config::MASK` is the text printed; `get --show-secret` prints the
//! stored value.

use clap::{Args, Subcommand};

use extenddb_storage::management_store::SettingsStore;

use extenddb_config::{self as config, MASK};

// Re-use validation constants from the ops layer.
use extenddb_server::management::ops_settings::{KNOWN_KEYS, READONLY_KEYS};

#[derive(Args)]
pub struct SettingsArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    #[command(subcommand)]
    action: SettingsAction,
}

#[derive(Subcommand)]
enum SettingsAction {
    /// List all settings. Secret values (encryption key, connection strings,
    /// passwords, tokens) are masked.
    List,
    /// Get a single setting. A secret value is masked unless --show-secret is
    /// given.
    Get {
        key: String,
        /// Print the stored value of a secret setting instead of the mask.
        #[arg(long)]
        show_secret: bool,
    },
    /// Set a setting value
    Set { key: String, value: String },
}

/// Run the settings subcommand.
pub async fn run(args: SettingsArgs) -> anyhow::Result<()> {
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found. Run 'extenddb init' to set up a deployment, \
             or use --config <path> to specify a different location.",
            args.config,
        );
    }
    let app_config = config::load(&args.config)?;
    let store = extenddb_storage::settings_store::create_settings_store(
        app_config.storage.connection_config(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create settings store: {e}"))?;

    match args.action {
        SettingsAction::List => list(store.as_ref()).await,
        SettingsAction::Get { key, show_secret } => get(store.as_ref(), &key, show_secret).await,
        SettingsAction::Set { key, value } => set(store.as_ref(), &key, &value).await,
    }
}

/// The value to print for `key`: the mask when the key is a secret and the
/// caller did not ask for it, else the stored value.
fn display_value<'a>(key: &str, value: &'a str, show_secret: bool) -> &'a str {
    if !show_secret && config::should_redact(key) {
        MASK
    } else {
        value
    }
}

/// Format the `settings list` output, one `key = value` line per row with
/// secrets masked.
fn format_list(rows: &[(String, String)]) -> String {
    rows.iter()
        .map(|(k, v)| format!("{k} = {}\n", display_value(k, v, false)))
        .collect()
}

async fn list(store: &dyn SettingsStore) -> anyhow::Result<()> {
    let rows = store
        .list_settings()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list settings: {e:?}"))?;

    if rows.is_empty() {
        println!("No settings found.");
    } else {
        print!("{}", format_list(&rows));
    }
    Ok(())
}

async fn get(store: &dyn SettingsStore, key: &str, show_secret: bool) -> anyhow::Result<()> {
    let value = store
        .get_setting(key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get setting: {e:?}"))?;

    if let Some(v) = value {
        println!("{}", display_value(key, &v, show_secret));
    } else {
        eprintln!("Setting '{key}' not found.");
        std::process::exit(1);
    }
    Ok(())
}

async fn set(store: &dyn SettingsStore, key: &str, value: &str) -> anyhow::Result<()> {
    if READONLY_KEYS.contains(&key) {
        anyhow::bail!("Setting '{key}' is read-only and cannot be changed via this command.");
    }

    // Validate against known keys.
    let known = KNOWN_KEYS.iter().find(|(k, _)| *k == key);
    if let Some((_, validator)) = known {
        validator(value)
            .map_err(|reason| anyhow::anyhow!("Invalid value for '{key}': {reason}"))?;
    } else {
        // Unknown key — reject.
        anyhow::bail!(
            "Unknown setting '{key}'. Known writable keys: {}",
            KNOWN_KEYS
                .iter()
                .map(|(k, _)| *k)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Write under the canonical name so the deprecated alias updates the row the read
    // path consults, rather than adding a second one that is silently ignored.
    let key = extenddb_core::settings_keys::canonical_key(key);
    store
        .set_setting(key, value)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to set setting: {e:?}"))?;

    tracing::warn!(
        target: "extenddb::audit::settings",
        "settings-set: key={key}, value={value}",
    );
    println!("{key} = {value}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    const CONN: &str = "postgresql://extenddb:apppassword@127.0.0.1:5432/extenddb";

    #[test]
    fn list_masks_encryption_key_and_connection_string() {
        let rows = vec![
            ("catalog_version".to_owned(), "42".to_owned()),
            ("encryption_key".to_owned(), KEY.to_owned()),
            (
                "data_database_connection_string".to_owned(),
                CONN.to_owned(),
            ),
            ("log_level".to_owned(), "info".to_owned()),
        ];
        let out = format_list(&rows);
        assert_eq!(
            out,
            format!(
                "catalog_version = 42\nencryption_key = {MASK}\n\
                 data_database_connection_string = {MASK}\nlog_level = info\n"
            )
        );
        assert!(!out.contains(KEY));
        assert!(!out.contains("apppassword"));
    }

    #[test]
    fn get_masks_secret_by_default() {
        assert_eq!(display_value("encryption_key", KEY, false), MASK);
        assert_eq!(
            display_value("data_database_connection_string", CONN, false),
            MASK
        );
        assert_eq!(display_value("ENCRYPTION_KEY", KEY, false), MASK);
    }

    #[test]
    fn get_show_secret_prints_stored_value() {
        assert_eq!(display_value("encryption_key", KEY, true), KEY);
        assert_eq!(
            display_value("data_database_connection_string", CONN, true),
            CONN
        );
    }

    #[test]
    fn non_secret_values_print_unchanged() {
        assert_eq!(display_value("log_level", "info", false), "info");
        assert_eq!(display_value("log_level", "info", true), "info");
        assert_eq!(display_value("catalog_version", "42", false), "42");
    }

    #[test]
    fn secret_prints_the_shared_mask() {
        // The CLI prints the constant the management API and the console
        // print, and that constant is the eight U+2022 bullets the admin guide
        // and the wire tests expect.
        assert_eq!(
            display_value("encryption_key", KEY, false),
            extenddb_config::MASK
        );
        assert_eq!(extenddb_config::MASK, "\u{2022}".repeat(8));
    }
}
