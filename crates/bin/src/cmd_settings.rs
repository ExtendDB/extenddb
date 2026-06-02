// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb settings` — read and write runtime settings (D-23).
//!
//! This is an infrastructure command for direct DB access when the server is
//! down. Validation logic is shared with the management API via
//! `ops_settings::validate_setting`.

use clap::{Args, Subcommand};

use extenddb_storage::management_store::SettingsStore;

use crate::config;

// Re-use validation constants from the ops layer.
use extenddb_server::management::ops_settings::{
    READONLY_KEYS, RuntimeSettingContext, validate_setting,
};

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
    let setting_context = RuntimeSettingContext::from_storage_config(app_config.storage.as_trait());
    if let SettingsAction::Set { key, value } = &args.action {
        validate_setting(setting_context, key, value)
            .map_err(|e| anyhow::anyhow!(format_management_setting_error(e)))?;
    }

    let backend = &app_config.storage._backend;
    let store = extenddb_storage::settings_store::create_settings_store(
        backend,
        app_config.storage.connection_config(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create settings store: {e}"))?;

    match args.action {
        SettingsAction::List => list(store.as_ref()).await,
        SettingsAction::Get { key } => get(store.as_ref(), &key).await,
        SettingsAction::Set { key, value } => {
            set(store.as_ref(), setting_context, &key, &value).await
        }
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

async fn set(
    store: &dyn SettingsStore,
    context: RuntimeSettingContext,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    if READONLY_KEYS.contains(&key) {
        anyhow::bail!("Setting '{key}' is read-only and cannot be changed via this command.");
    }

    validate_setting(context, key, value)
        .map_err(|e| anyhow::anyhow!(format_management_setting_error(e)))?;

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

fn format_management_setting_error(e: extenddb_storage::management_store::OpError) -> String {
    match e {
        extenddb_storage::management_store::OpError::Validation(message) => message,
        other => format!("{other:?}"),
    }
}
