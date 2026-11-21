// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `[server] catalog_upgrade` and `catalog_upgrade_lock_timeout`: what
//! `serve` does with a catalog that is behind this binary.

use serde::Deserialize;

/// `[server] catalog_upgrade`: what `serve` does with a catalog that is
/// behind this binary. Every other status a server cannot serve is refused
/// under both values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogUpgrade {
    /// Exit 3, printing the pending migrations and the command that
    /// applies them.
    Refuse,
    /// Apply the pending migrations under the migration lock, as
    /// `migrate --yes` does, then serve.
    Apply,
}

impl CatalogUpgrade {
    /// The value as the config file spells it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Refuse => "refuse",
            Self::Apply => "apply",
        }
    }
}

/// The longest `catalog_upgrade_lock_timeout`, a day, as `migrate`'s
/// `--lock-timeout` allows.
pub const MAX_CATALOG_UPGRADE_LOCK_TIMEOUT: u64 = 86_400;

/// A dev-mode build migrates its database at startup, since the dev image
/// and the npm launcher run no `migrate`; a production build refuses.
pub(crate) fn default_catalog_upgrade() -> CatalogUpgrade {
    if cfg!(feature = "dev-mode") {
        CatalogUpgrade::Apply
    } else {
        CatalogUpgrade::Refuse
    }
}

pub(crate) fn default_catalog_upgrade_lock_timeout() -> u64 {
    60
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ServerConfig;

    /// `[server]` as `load` reads it: the file's table, then the
    /// `EXTENDDB__` variables in `env` over it. The variables are given to
    /// the source instead of set in the process, which other tests share.
    fn server_from(file: &str, env: &[(&str, &str)]) -> Result<ServerConfig, String> {
        #[derive(Deserialize)]
        struct File {
            #[serde(default)]
            server: ServerConfig,
        }
        let vars = env
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        config::Config::builder()
            .add_source(config::File::from_str(file, config::FileFormat::Toml))
            .add_source(
                config::Environment::with_prefix("EXTENDDB")
                    .separator("__")
                    .source(Some(vars)),
            )
            .build()
            .and_then(config::Config::try_deserialize::<File>)
            .map(|f| f.server)
            .map_err(|e| e.to_string())
    }

    /// A production build refuses a catalog behind it unless told to apply,
    /// and waits 60 s for the lock under `apply`; a dev-mode build applies.
    #[test]
    fn catalog_upgrade_defaults_to_refuse_and_a_60_s_lock_timeout() {
        let server = server_from("", &[]).expect("defaults");
        let expected = if cfg!(feature = "dev-mode") {
            CatalogUpgrade::Apply
        } else {
            CatalogUpgrade::Refuse
        };
        assert_eq!(server.catalog_upgrade, expected);
        assert_eq!(server.catalog_upgrade_lock_timeout, 60);
        assert_eq!(ServerConfig::default().catalog_upgrade, expected);
        assert_eq!(ServerConfig::default().catalog_upgrade_lock_timeout, 60);
    }

    #[test]
    fn catalog_upgrade_is_read_from_the_file_and_the_environment() {
        let server = server_from(
            "[server]\ncatalog_upgrade = \"apply\"\ncatalog_upgrade_lock_timeout = 5\n",
            &[],
        )
        .expect("file");
        assert_eq!(server.catalog_upgrade, CatalogUpgrade::Apply);
        assert_eq!(server.catalog_upgrade_lock_timeout, 5);
        // The environment wins over the file, as for every other key.
        let server = server_from(
            "[server]\ncatalog_upgrade = \"apply\"\n",
            &[
                ("EXTENDDB__SERVER__CATALOG_UPGRADE", "refuse"),
                ("EXTENDDB__SERVER__CATALOG_UPGRADE_LOCK_TIMEOUT", "120"),
            ],
        )
        .expect("environment");
        assert_eq!(server.catalog_upgrade, CatalogUpgrade::Refuse);
        assert_eq!(server.catalog_upgrade_lock_timeout, 120);
        let server = server_from("", &[("EXTENDDB__SERVER__CATALOG_UPGRADE", "apply")])
            .expect("environment alone");
        assert_eq!(server.catalog_upgrade, CatalogUpgrade::Apply);
    }

    #[test]
    fn catalog_upgrade_refuses_any_other_value() {
        let error =
            server_from("[server]\ncatalog_upgrade = \"ignore\"\n", &[]).expect_err("not a value");
        assert!(error.contains("ignore"), "{error}");
        let error = server_from("", &[("EXTENDDB__SERVER__CATALOG_UPGRADE", "always")])
            .expect_err("not a value");
        assert!(error.contains("always"), "{error}");
        assert_eq!(CatalogUpgrade::Refuse.as_str(), "refuse");
        assert_eq!(CatalogUpgrade::Apply.as_str(), "apply");
    }
}
