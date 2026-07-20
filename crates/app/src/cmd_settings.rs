// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb settings` — read and write runtime settings (D-23).
//!
//! This is an infrastructure command for direct DB access when the server is
//! down. Validation logic is shared with the management API via `ops::KNOWN_KEYS`
//! and `ops::READONLY_KEYS`.

use clap::{Args, Subcommand};

use extenddb_storage::management_store::SettingsStore;

use extenddb_config as config;

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
    /// List all settings
    List,
    /// Get a single setting
    Get { key: String },
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
    let backend = &app_config.storage.backend;
    let store = extenddb_storage::settings_store::create_settings_store(
        backend,
        app_config.storage.connection_config(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create settings store: {e}"))?;

    match args.action {
        SettingsAction::List => list(store.as_ref()).await,
        SettingsAction::Get { key } => get(store.as_ref(), &key).await,
        SettingsAction::Set { key, value } => set(store.as_ref(), &key, &value).await,
    }
}

async fn list(store: &dyn SettingsStore) -> anyhow::Result<()> {
    let rows = store
        .list_settings()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list settings: {e:?}"))?;

    if rows.is_empty() {
        println!("No settings found.");
    } else {
        for (k, v) in &rows {
            println!("{k} = {v}");
        }
    }
    Ok(())
}

async fn get(store: &dyn SettingsStore, key: &str) -> anyhow::Result<()> {
    let value = store
        .get_setting(key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get setting: {e:?}"))?;

    if let Some(v) = value {
        println!("{v}");
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
