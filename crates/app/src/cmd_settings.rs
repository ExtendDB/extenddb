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
//! stored value. A key the catalog has no row for prints the storage
//! engine's default (`extenddb_storage::operations::setting_default`), as the
//! console does, so a fresh install, which seeds no settings rows, lists the
//! values the engine uses.

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

/// The other name of a renamed key (its deprecated alias, or the current
/// name of an alias), whose row the engine reads when the key has none.
fn other_name(key: &str) -> Option<&'static str> {
    use extenddb_core::settings_keys::{
        INDEX_PROPAGATION_DELAY_MS, LEGACY_GSI_PROPAGATION_DELAY_MS,
    };
    match key {
        INDEX_PROPAGATION_DELAY_MS => Some(LEGACY_GSI_PROPAGATION_DELAY_MS),
        LEGACY_GSI_PROPAGATION_DELAY_MS => Some(INDEX_PROPAGATION_DELAY_MS),
        _ => None,
    }
}

/// `rows` with a row for each writable key that has no row under either
/// of its names and that `default` gives a value for, ordered by key.
fn with_defaults(
    mut rows: Vec<(String, String)>,
    default: impl Fn(&str) -> Option<String>,
) -> Vec<(String, String)> {
    for (key, _) in KNOWN_KEYS {
        let names = [Some(*key), other_name(key)];
        if rows.iter().all(|(k, _)| !names.contains(&Some(k.as_str())))
            && let Some(value) = default(key)
        {
            rows.push(((*key).to_owned(), value));
        }
    }
    rows.sort();
    rows
}

/// The value `get` prints for `key`: its row, else the row of its other
/// name, which the engine reads in its place, else `default`'s value.
async fn stored_or_default(
    store: &dyn SettingsStore,
    key: &str,
    default: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<Option<String>> {
    for name in [Some(key), other_name(key)].into_iter().flatten() {
        let stored = store
            .get_setting(name)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get setting: {e:?}"))?;
        if stored.is_some() {
            return Ok(stored);
        }
    }
    Ok(default(extenddb_core::settings_keys::canonical_key(key)))
}

async fn list(store: &dyn SettingsStore) -> anyhow::Result<()> {
    let rows = store
        .list_settings()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list settings: {e:?}"))?;
    let rows = with_defaults(rows, extenddb_storage::operations::setting_default);

    if rows.is_empty() {
        println!("No settings found.");
    } else {
        print!("{}", format_list(&rows));
    }
    Ok(())
}

async fn get(store: &dyn SettingsStore, key: &str, show_secret: bool) -> anyhow::Result<()> {
    let value =
        stored_or_default(store, key, extenddb_storage::operations::setting_default).await?;

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

    /// The storage engine's defaults on SQLite and PostgreSQL, which a fresh
    /// install no longer seeds as rows.
    fn engine_default(key: &str) -> Option<String> {
        match key {
            "control_plane_delay_seconds" => Some("0.25".to_owned()),
            "index_propagation_delay_ms" => Some("10".to_owned()),
            _ => None,
        }
    }

    /// A settings store's answer, as its trait returns it.
    type Answer<'a, T> = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = extenddb_storage::management_store::OpResult<T>>
                + Send
                + 'a,
        >,
    >;

    /// A settings store holding `rows`.
    struct Rows(Vec<(String, String)>);

    impl SettingsStore for Rows {
        fn get_setting(&self, key: &str) -> Answer<'_, Option<String>> {
            let value = self
                .0
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone());
            Box::pin(async move { Ok(value) })
        }
        fn set_setting(&self, _key: &str, _value: &str) -> Answer<'_, ()> {
            Box::pin(async { Ok(()) })
        }
        fn list_settings(&self) -> Answer<'_, Vec<(String, String)>> {
            let rows = self.0.clone();
            Box::pin(async move { Ok(rows) })
        }
    }

    fn rows(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    /// A key a fresh install has no row for prints the engine's default,
    /// as on a catalog that seeded it, and a stored row wins over it.
    #[tokio::test]
    async fn get_falls_back_to_the_engine_default() {
        let fresh = Rows(rows(&[("catalog_version", "4")]));
        let value = stored_or_default(&fresh, "control_plane_delay_seconds", engine_default)
            .await
            .expect("get");
        assert_eq!(value.as_deref(), Some("0.25"));
        let value = stored_or_default(&fresh, "no_such_key", engine_default)
            .await
            .expect("get");
        assert_eq!(value, None);
        let set = Rows(rows(&[("control_plane_delay_seconds", "2")]));
        let value = stored_or_default(&set, "control_plane_delay_seconds", engine_default)
            .await
            .expect("get");
        assert_eq!(value.as_deref(), Some("2"));
        // A row under the deprecated name is the one the engine reads.
        let legacy = Rows(rows(&[("gsi_propagation_delay_ms", "0")]));
        for key in ["index_propagation_delay_ms", "gsi_propagation_delay_ms"] {
            let value = stored_or_default(&legacy, key, engine_default)
                .await
                .expect("get");
            assert_eq!(value.as_deref(), Some("0"), "{key}");
        }
        let value = stored_or_default(&fresh, "gsi_propagation_delay_ms", engine_default)
            .await
            .expect("get");
        assert_eq!(value.as_deref(), Some("10"));
    }

    /// `list` adds a row for each writable key with an engine default and
    /// no row, in key order, and keeps a stored value over the default.
    #[test]
    fn list_adds_the_engine_defaults_in_key_order() {
        let listed = with_defaults(
            rows(&[("catalog_version", "4"), ("log_level", "debug")]),
            engine_default,
        );
        assert_eq!(
            format_list(&listed),
            "catalog_version = 4\ncontrol_plane_delay_seconds = 0.25\n\
             index_propagation_delay_ms = 10\nlog_level = debug\n"
        );
        let listed = with_defaults(rows(&[("index_propagation_delay_ms", "0")]), engine_default);
        assert_eq!(
            listed,
            rows(&[
                ("control_plane_delay_seconds", "0.25"),
                ("index_propagation_delay_ms", "0"),
            ])
        );
        // No default beside a row under the deprecated name.
        let listed = with_defaults(rows(&[("gsi_propagation_delay_ms", "0")]), engine_default);
        assert_eq!(
            listed,
            rows(&[
                ("control_plane_delay_seconds", "0.25"),
                ("gsi_propagation_delay_ms", "0"),
            ])
        );
    }

    #[test]
    fn no_known_key_matches_a_redaction_pattern() {
        // `set` prints the value it wrote, and both `set` and the management
        // API write it to the `extenddb::audit::settings` log, unmasked. That
        // is safe only while no key in `KNOWN_KEYS` is one `should_redact`
        // matches.
        for (key, _) in KNOWN_KEYS {
            assert!(
                !config::should_redact(key),
                "writable key {key} matches a redaction pattern"
            );
        }
    }
}
