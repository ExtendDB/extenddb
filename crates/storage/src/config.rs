// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage configuration trait and registry for storage backends.

/// Operator-facing warning emitted during server startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupWarning {
    /// Short warning headline shown prominently in the startup banner.
    pub title: String,
    /// Supporting lines describing the impact and remediation.
    pub details: Vec<String>,
}

impl StartupWarning {
    /// Render the warning as a single line suitable for structured logs.
    #[must_use]
    pub fn log_message(&self) -> String {
        std::iter::once(self.title.as_str())
            .chain(self.details.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Configuration interface for storage backends.
///
/// Each backend implements this trait to expose connection parameters
/// in a backend-agnostic way. The bin crate uses these methods without
/// knowing the concrete backend type.
pub trait StorageConfig: Send + Sync + std::fmt::Debug {
    /// Backend-specific connection configuration as a string.
    ///
    /// For `PostgreSQL`: connection string (postgresql://...)
    fn connection_config(&self) -> &str;

    /// Maximum concurrent connections for data operations.
    fn max_connections(&self) -> u32;

    /// Maximum concurrent connections for catalog/management operations.
    fn max_catalog_connections(&self) -> u32;

    /// Explicit data-pool override, if the backend distinguishes an omitted
    /// setting from its effective default.
    fn max_connections_override(&self) -> Option<u32> {
        Some(self.max_connections())
    }

    /// Explicit catalog-pool override, if the backend distinguishes an
    /// omitted setting from its effective default.
    fn max_catalog_connections_override(&self) -> Option<u32> {
        Some(self.max_catalog_connections())
    }

    /// Operator-facing warnings for non-default or compatibility settings.
    ///
    /// The default keeps existing and third-party backends source-compatible.
    fn startup_warnings(&self) -> Vec<StartupWarning> {
        Vec::new()
    }

    /// Clone this config into a boxed trait object.
    fn clone_box(&self) -> Box<dyn StorageConfig>;

    /// Enable downcasting to specific storage engine config types to allow
    /// access to engine-specific configuration (e.g. `keyspace_prefix` for
    /// the Cassandra backend).
    fn as_any(&self) -> &dyn std::any::Any
    where
        Self: 'static;

    /// Set a per-node instance identifier used to derive backend-specific
    /// node identities (e.g. HLC node IDs for stream sequence numbers).
    /// Backends that do not need a node identity may ignore this.
    fn set_instance_id(&mut self, _instance_id: &str) {}

    /// Return the instance identifier previously set via [`set_instance_id`],
    /// if any.
    fn instance_id(&self) -> Option<&str> {
        None
    }
}

impl Clone for Box<dyn StorageConfig> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// The warning for a `server.region` outside `[a-z0-9-]`, or `None` when the
/// region is in that class.
///
/// The region is baked into every backup ARN the server issues
/// (`arn:aws:dynamodb:<region>:<account>:table/<name>/backup/<id>`), and the
/// `BackupArn` parser accepts a region in `[a-z0-9-]` only, as the service
/// does. A deployment configured outside that class therefore issues ARNs
/// it refuses on `DescribeBackup`, `DeleteBackup`, and
/// `RestoreTableFromBackup` with `ValidationException` "Invalid Request:
/// BackupArn is not valid". The server starts anyway (the setting is
/// pre-existing and the data plane does not depend on it); the operator is
/// told at startup.
#[must_use]
pub fn server_region_warning(region: &str) -> Option<StartupWarning> {
    let well_formed = !region.is_empty()
        && region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if well_formed {
        return None;
    }
    Some(StartupWarning {
        title: format!(
            "server.region {region:?} is outside [a-z0-9-]; the backup ARNs this server \
             issues will not resolve."
        ),
        details: vec![
            "Every backup ARN carries the configured region, and BackupArn is accepted \
             only with a region in [a-z0-9-], so DescribeBackup, DeleteBackup, and \
             RestoreTableFromBackup answer ValidationException \"Invalid Request: \
             BackupArn is not valid\" for every backup this server creates, and \
             ListBackups refuses the cursor it hands out, so a listing stops at its \
             first page."
                .to_owned(),
            "Set server.region to a value in [a-z0-9-] (for example us-east-1). Backups \
             already created keep the old region in their stored ARN; see the upgrade \
             manual on changing server.region."
                .to_owned(),
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::server_region_warning;

    #[test]
    fn a_region_the_backup_arn_parser_accepts_draws_no_warning() {
        for region in ["us-east-1", "local", "eu-west-2", "a", "0-9"] {
            assert!(server_region_warning(region).is_none(), "{region}");
        }
    }

    #[test]
    fn a_region_the_backup_arn_parser_refuses_is_warned_about_with_the_consequence() {
        for region in [
            "LOCAL",
            "us_east_1",
            "dev.local",
            "",
            "us east",
            "Us-East-1",
            "ü",
        ] {
            let warning = server_region_warning(region).unwrap_or_else(|| panic!("{region:?}"));
            assert!(warning.title.contains("server.region"), "{region:?}");
            assert!(warning.title.contains(&format!("{region:?}")), "{region:?}");
            let text = warning.log_message();
            assert!(
                text.contains("Invalid Request: BackupArn is not valid"),
                "{region:?}: {text}"
            );
            assert!(text.contains("DescribeBackup"), "{region:?}: {text}");
            // The warning agrees with the parser it speaks for.
            let arn =
                crate::util::backup_arn(region, "123456789012", "Music", "01489602797149-73d8d5bc");
            assert!(crate::util::parse_backup_arn(&arn).is_none(), "{region:?}");
        }
        let arn = crate::util::backup_arn(
            "us-east-1",
            "123456789012",
            "Music",
            "01489602797149-73d8d5bc",
        );
        assert!(crate::util::parse_backup_arn(&arn).is_some());
    }
}

/// Serde helpers for typed backend-config fields that may arrive as strings.
///
/// Environment-variable overrides (`EXTENDDB__STORAGE__<BACKEND>__...`) always
/// enter the config as strings; the top-level `AppConfig` fields are coerced by
/// the `config` crate, but a backend's storage subtree is re-deserialized from
/// a raw `toml::Table`, which is strict about types (issue #222). These helpers
/// accept either the native type or its string form, so
/// `EXTENDDB__STORAGE__POSTGRES__POOL_SIZE=10` deserializes like
/// `pool_size = 10`. String fields are untouched — coercion applies only where
/// a backend explicitly opts a numeric or boolean field in with
/// `#[serde(deserialize_with = ...)]`, so a password of `"12345"` is never
/// reinterpreted.
pub mod string_coerce {
    use serde::{Deserialize, Deserializer, de::Error};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum MaybeString<T> {
        Native(T),
        Text(String),
    }

    fn coerce<'de, T, D>(deserializer: D, what: &str) -> Result<T, D::Error>
    where
        T: Deserialize<'de> + std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Display,
        D: Deserializer<'de>,
    {
        match MaybeString::<T>::deserialize(deserializer)? {
            MaybeString::Native(v) => Ok(v),
            MaybeString::Text(s) => s
                .trim()
                .parse()
                .map_err(|e| D::Error::custom(format!("invalid {what} value \"{s}\": {e}"))),
        }
    }

    /// Deserialize a `u32` from either a number or its string form.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is neither a `u32` nor a string that
    /// parses as one.
    pub fn u32<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
        coerce(deserializer, "u32")
    }

    /// Deserialize an `Option<u32>` from a number, its string form, or absent.
    ///
    /// # Errors
    ///
    /// Returns an error when a present value is neither a `u32` nor a string
    /// that parses as one.
    pub fn opt_u32<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u32>, D::Error> {
        Option::<MaybeString<u32>>::deserialize(deserializer)?
            .map(|v| match v {
                MaybeString::Native(n) => Ok(n),
                MaybeString::Text(s) => s.trim().parse().map_err(|e| {
                    serde::de::Error::custom(format!("invalid u32 value \"{s}\": {e}"))
                }),
            })
            .transpose()
    }

    /// Deserialize an optional positive `u32` from a number, its string form,
    /// or absence. A present value must be at least one.
    pub fn positive_opt_u32<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u32>, D::Error> {
        let value = opt_u32(deserializer)?;
        if value == Some(0) {
            return Err(D::Error::custom("value must be at least 1"));
        }
        Ok(value)
    }

    /// Deserialize a `bool` from either a boolean or its string form
    /// (`"true"`/`"false"`).
    ///
    /// # Errors
    ///
    /// Returns an error when the value is neither a `bool` nor a string that
    /// parses as one.
    pub fn bool<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
        coerce(deserializer, "bool")
    }

    /// Deserialize a `u16` from either a number or its string form.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is neither a `u16` nor a string that
    /// parses as one.
    pub fn u16<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u16, D::Error> {
        coerce(deserializer, "u16")
    }
}

/// Deserializer function type for storage configurations.
///
/// Takes a TOML table and returns a boxed `StorageConfig` trait object.
pub type StorageConfigDeserializer = fn(&toml::Table) -> Result<Box<dyn StorageConfig>, String>;

/// Deserialize a storage configuration from a TOML table.
///
/// Uses the deserializer of the [`Backend`](crate::Backend) installed via
/// [`set_backend`](crate::set_backend), invoking it with the provided TOML
/// table.
pub fn deserialize_storage_config(table: &toml::Table) -> Result<Box<dyn StorageConfig>, String> {
    let backend = crate::backend::try_backend()
        .ok_or_else(|| "no storage backend installed (set_backend was not called)".to_owned())?;
    (backend.storage_config)(table)
}

#[cfg(test)]
mod string_coerce_tests {
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct Sample {
        #[serde(deserialize_with = "super::string_coerce::u32")]
        count: u32,
        #[serde(default, deserialize_with = "super::string_coerce::opt_u32")]
        optional: Option<u32>,
        #[serde(deserialize_with = "super::string_coerce::bool")]
        flag: bool,
        #[serde(deserialize_with = "super::string_coerce::u16")]
        port: u16,
        // Deliberately NOT coerced: a numeric-looking string field must stay a string.
        password: String,
    }

    #[test]
    fn native_types_deserialize_unchanged() {
        let s: Sample = toml::from_str(
            r#"count = 7
optional = 9
flag = true
port = 8443
password = "12345""#,
        )
        .expect("native types must deserialize");
        assert_eq!(s.count, 7);
        assert_eq!(s.optional, Some(9));
        assert!(s.flag);
        assert_eq!(s.port, 8443);
        assert_eq!(s.password, "12345");
    }

    /// Issue #222: env-var overrides arrive as strings.
    #[test]
    fn string_forms_coerce_to_the_native_type() {
        let s: Sample = toml::from_str(
            r#"count = "7"
optional = "9"
flag = "true"
port = " 8443 "
password = "12345""#,
        )
        .expect("string forms must coerce");
        assert_eq!(s.count, 7);
        assert_eq!(s.optional, Some(9));
        assert!(s.flag);
        assert_eq!(s.port, 8443);
        // The plain String field is never reinterpreted.
        assert_eq!(s.password, "12345");
    }

    #[test]
    fn absent_optional_stays_none() {
        let s: Sample = toml::from_str(
            r#"count = 1
flag = false
port = 1
password = "x""#,
        )
        .expect("absent optional must deserialize");
        assert_eq!(s.optional, None);
    }

    #[test]
    fn garbage_strings_are_rejected_with_the_value_in_the_error() {
        let err = toml::from_str::<Sample>(
            r#"count = "seven"
flag = false
port = 1
password = "x""#,
        )
        .expect_err("non-numeric string must be rejected");
        assert!(
            err.to_string().contains("seven"),
            "error should quote the bad value, got: {err}"
        );
    }

    #[test]
    fn negative_and_overflow_values_are_rejected() {
        assert!(
            toml::from_str::<Sample>(
                r#"count = "-1"
flag = false
port = 1
password = "x""#
            )
            .is_err()
        );
        assert!(
            toml::from_str::<Sample>(
                r#"count = 1
flag = false
port = "70000"
password = "x""#
            )
            .is_err()
        );
    }
}
