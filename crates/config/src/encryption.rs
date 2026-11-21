// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `[auth.encryption]` section.

use serde::{Deserialize, Deserializer};

use crate::expand_tilde;

/// The `[auth.encryption]` section: the key-encryption key (KEK) file and the
/// locked memory rule.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptionConfig {
    /// Path of the KEK file. Not set: the server uses the catalog key in the
    /// `encryption_key` settings row. Set: the server unwraps the data
    /// encryption key in the `encryption_key_ring` row with the KEK in this
    /// file. A leading `~` expands as in the TLS key path
    /// ([`EncryptionConfig::key_file_path`]), and the path must then be
    /// absolute: the parse refuses a relative one.
    #[serde(default, deserialize_with = "absolute_key_file")]
    pub key_file: Option<String>,
    /// With `key_file` set, refuse to start when the locked key store cannot
    /// lock its memory. `false` turns the refusal into a warning. Without
    /// `key_file` the server warns and starts either way.
    #[serde(default = "default_require_locked_memory")]
    pub require_locked_memory: bool,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            key_file: None,
            require_locked_memory: default_require_locked_memory(),
        }
    }
}

impl EncryptionConfig {
    /// The KEK file path with a leading `~` expanded ([`expand_tilde`]), or
    /// `None` when `key_file` is not set.
    #[must_use]
    pub fn key_file_path(&self) -> Option<String> {
        self.key_file.as_deref().map(expand_tilde)
    }
}

/// `key_file` as written, refused when it is relative after the `~`
/// expansion of [`EncryptionConfig::key_file_path`]: `serve` detaches into
/// `/` before it reads the file, so a relative path names another file.
fn absolute_key_file<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    let path = Option::<String>::deserialize(d)?;
    if let Some(path) = &path
        && !std::path::Path::new(&expand_tilde(path)).is_absolute()
    {
        return Err(serde::de::Error::custom(format!(
            "[auth.encryption] key_file = {path:?} is a relative path; the path must be absolute \
             (a leading `~/` expands to the home directory), because `extenddb serve` changes \
             its working directory to `/` when it detaches"
        )));
    }
    Ok(path)
}

fn default_require_locked_memory() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthConfig, environment};

    /// `[auth.encryption]` as `load` reads it: the file's text, then the
    /// `EXTENDDB__` variables in `vars`. `Ok` holds `key_file_path()` and
    /// `require_locked_memory`; `Err` holds a part of the error.
    #[test]
    fn auth_encryption_parses_with_its_defaults_and_overrides() {
        type Expected = Result<(Option<String>, bool), &'static str>;
        /// The file's text, the `EXTENDDB__` variables, and the outcome.
        type Case = (
            &'static str,
            &'static [(&'static str, &'static str)],
            Expected,
        );
        let home = std::env::var("HOME").unwrap();
        let kek = |path: &str| Some(path.to_owned());
        let cases: [Case; 11] = [
            ("", &[], Ok((None, true))),
            ("[auth]\nprovider = \"builtin\"\n", &[], Ok((None, true))),
            (
                "[auth.encryption]\nkey_file = \"/etc/extenddb/kek\"\n",
                &[],
                Ok((kek("/etc/extenddb/kek"), true)),
            ),
            (
                "[auth.encryption]\nkey_file = \"~/kek\"\n",
                &[],
                Ok((Some(format!("{home}/kek")), true)),
            ),
            (
                "[auth.encryption]\nrequire_locked_memory = false\n",
                &[],
                Ok((None, false)),
            ),
            (
                "[auth.encryption]\nkey_file = \"/etc/extenddb/kek\"\n",
                &[(
                    "EXTENDDB__AUTH__ENCRYPTION__KEY_FILE",
                    "/run/credentials/extenddb.service/extenddb-kek",
                )],
                Ok((kek("/run/credentials/extenddb.service/extenddb-kek"), true)),
            ),
            (
                "",
                &[("EXTENDDB__AUTH__ENCRYPTION__REQUIRE_LOCKED_MEMORY", "false")],
                Ok((None, false)),
            ),
            (
                "[auth.encryption]\nkeyfile = \"/etc/extenddb/kek\"\n",
                &[],
                Err("unknown field `keyfile`"),
            ),
            (
                "[auth.encryption]\nkey_file = \"/a\"\npassphrase = \"x\"\n",
                &[],
                Err("unknown field `passphrase`"),
            ),
            (
                "[auth.encryption]\nkey_file = \"kek\"\n",
                &[],
                Err("[auth.encryption] key_file = \"kek\" is a relative path; the path must be"),
            ),
            (
                "",
                &[("EXTENDDB__AUTH__ENCRYPTION__KEY_FILE", "etc/kek")],
                Err("[auth.encryption] key_file = \"etc/kek\" is a relative path"),
            ),
        ];
        /// The one section this test reads.
        #[derive(Deserialize)]
        struct Auth {
            #[serde(default)]
            auth: AuthConfig,
        }
        for (text, vars, expected) in cases {
            let vars = vars
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect();
            let parsed = config::Config::builder()
                .add_source(config::File::from_str(text, config::FileFormat::Toml))
                .add_source(environment(vars))
                .build()
                .and_then(|c| c.try_deserialize::<Auth>())
                .map(|Auth { auth }| {
                    (
                        auth.encryption.key_file_path(),
                        auth.encryption.require_locked_memory,
                    )
                })
                .map_err(|e| e.to_string());
            match (&parsed, &expected) {
                (Ok(got), Ok(want)) => assert_eq!(got, want, "{text:?}"),
                (Err(got), Err(part)) => assert!(got.contains(part), "{text:?}: {got}"),
                _ => panic!("{text:?}: got {parsed:?}, want {expected:?}"),
            }
        }
    }
}
