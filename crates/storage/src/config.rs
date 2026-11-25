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

    /// The local file the database lives in, for a backend whose database is
    /// one file on this host (SQLite with a file path); `None` for a server
    /// database and for an in-memory database. The backup configuration
    /// derives its per-backend defaults from it: a store root and a restore
    /// spool beside the file, and the rule that neither may sit in the
    /// file's directory. The default is `None`.
    fn database_file(&self) -> Option<std::path::PathBuf> {
        None
    }

    /// [`Self::database_file`] with the reason a configured location names
    /// no usable file: `Ok(None)` for a server database and for an
    /// in-memory one, `Err` for a location the backend cannot parse (which
    /// [`Self::database_file`] also answers `None` for). The default is
    /// `Ok` of [`Self::database_file`].
    ///
    /// # Errors
    ///
    /// The backend's reason the location is not usable.
    fn database_location(&self) -> Result<Option<std::path::PathBuf>, String> {
        Ok(self.database_file())
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
/// region is in that class (the `BackupArn` parser's predicate,
/// [`extenddb_core::backup_format::is_region`], so the warning and the parser
/// cannot disagree).
///
/// The region is baked into every backup ARN the server issues
/// (`arn:aws:dynamodb:<region>:<account>:table/<name>/backup/<id>`), and the
/// `BackupArn` parser accepts a region in `[a-z0-9-]` only, as the service
/// does. The engine checks the region before forming a backup ARN, so on a
/// deployment configured outside that class `CreateBackup` is refused with a
/// `ValidationException` naming `server.region`; backups taken before the
/// upgrade under such a region keep their stored ARN, which `DescribeBackup`,
/// `DeleteBackup` and `RestoreTableFromBackup` refuse as "Invalid Request:
/// BackupArn is not valid" as before. The server starts anyway (the setting
/// is pre-existing and the data plane does not depend on it); the operator
/// is told at startup.
#[must_use]
pub fn server_region_warning(region: &str) -> Option<StartupWarning> {
    if extenddb_core::backup_format::is_region(region) {
        return None;
    }
    Some(StartupWarning {
        title: format!(
            "server.region {region:?} is outside [a-z0-9-]; CreateBackup is refused on this \
             server."
        ),
        details: vec![
            "Every backup ARN carries the configured region, and BackupArn is accepted \
             only with a region in [a-z0-9-], so CreateBackup answers ValidationException \
             naming server.region (no backup ARN can be formed), and the backups created \
             under this region before the upgrade keep an ARN that DescribeBackup, \
             DeleteBackup, and RestoreTableFromBackup refuse with ValidationException \
             \"Invalid Request: BackupArn is not valid\"; ListBackups refuses a cursor that is \
             such an ARN (a listing of backups created under a valid region pages)."
                .to_owned(),
            "Set server.region to a value in [a-z0-9-] (for example us-east-1). Backups \
             already created keep the old region in their stored ARN; see the upgrade \
             manual on changing server.region."
                .to_owned(),
        ],
    })
}

#[cfg(test)]
mod tests;

/// Serde helpers for typed backend-config fields that may arrive as strings.
///
/// Environment-variable overrides (`EXTENDDB__STORAGE__<BACKEND>__...`) always
/// enter the config as strings; the top-level `AppConfig` fields are coerced by
/// the `config` crate, but a backend's storage subtree is re-deserialized from
/// a raw `toml::Table`, which is strict about types (issue #222). These helpers
/// accept either the native type or its string form, so
/// `EXTENDDB__STORAGE__POSTGRES__POOL_SIZE=10` deserializes like
/// `pool_size = 10`, and a list key accepts one comma-delimited string, so
/// `EXTENDDB__STORAGE__CASSANDRA__CONTACT_POINTS=a:9042,b:9042` deserializes
/// like `contact_points = ["a:9042", "b:9042"]`. String fields are untouched:
/// coercion applies only where a field explicitly opts in with
/// `#[serde(deserialize_with = ...)]`, so a password of `"12345"` is never
/// reinterpreted.
pub mod string_coerce {
    use std::marker::PhantomData;

    use serde::{Deserialize, Deserializer, de::Error, de::Unexpected, de::Visitor};

    /// Parses one scalar from its string form; the error quotes the value and
    /// names the type, and [`super::parse_table`] prefixes the key.
    fn parse<T, E>(s: &str, what: &str) -> Result<T, E>
    where
        T: std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Display,
        E: Error,
    {
        s.trim()
            .parse()
            .map_err(|e| E::custom(format!("invalid {what} value {s:?}: {e}")))
    }

    /// Accepts a scalar as its native type (integer or boolean) or as a string
    /// that parses to it. A native value goes through `FromStr` as well, so
    /// `bool` accepts a native boolean and `u32` a native integer; one that
    /// does not fit fails with serde's own value or type error (``invalid
    /// value: integer `-1`, expected a u32 or its string form``), and a bad
    /// string with the value quoted (`invalid u32 value "four": ...`). A
    /// float, array or table is a type error naming what was found and what
    /// is accepted.
    struct Scalar<T> {
        what: &'static str,
        _target: PhantomData<T>,
    }

    impl<T> Visitor<'_> for Scalar<T>
    where
        T: std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Display,
    {
        type Value = T;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a {} or its string form", self.what)
        }

        fn visit_str<E: Error>(self, s: &str) -> Result<T, E> {
            parse(s, self.what)
        }

        fn visit_u64<E: Error>(self, v: u64) -> Result<T, E> {
            v.to_string()
                .parse()
                .map_err(|_| E::invalid_value(Unexpected::Unsigned(v), &self))
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<T, E> {
            v.to_string()
                .parse()
                .map_err(|_| E::invalid_value(Unexpected::Signed(v), &self))
        }

        fn visit_bool<E: Error>(self, v: bool) -> Result<T, E> {
            v.to_string()
                .parse()
                .map_err(|_| E::invalid_type(Unexpected::Bool(v), &self))
        }
    }

    fn coerce<'de, T, D>(deserializer: D, what: &'static str) -> Result<T, D::Error>
    where
        T: std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Display,
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(Scalar {
            what,
            _target: PhantomData,
        })
    }

    /// `Option` over [`Scalar`]: absent or null is `None`, anything present
    /// is coerced.
    struct OptScalar<T> {
        what: &'static str,
        _target: PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for OptScalar<T>
    where
        T: std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Display,
    {
        type Value = Option<T>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a {} or its string form, or nothing", self.what)
        }

        fn visit_none<E: Error>(self) -> Result<Option<T>, E> {
            Ok(None)
        }

        fn visit_unit<E: Error>(self) -> Result<Option<T>, E> {
            Ok(None)
        }

        fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Option<T>, D::Error> {
            coerce(d, self.what).map(Some)
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
        deserializer.deserialize_option(OptScalar {
            what: "u32",
            _target: PhantomData,
        })
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

    /// Deserialize a `Vec<String>` from an array or from one comma-delimited
    /// string, the only shape an environment variable can carry
    /// (`EXTENDDB__IMPORT__PATHS="/data/in,/data/shared"`). Items in the
    /// string form are trimmed and empty items are dropped, so an empty
    /// variable is an empty list. An array is taken as written. The string
    /// form has no escape, so an item that itself contains a comma can only
    /// be written as an array in the file.
    ///
    /// # Errors
    ///
    /// Returns an error naming both accepted shapes when the value is neither
    /// an array nor a string; an array element that is not a string fails
    /// with serde's own type error.
    pub fn string_list<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<String>, D::Error> {
        struct ListVisitor;

        impl<'de> Visitor<'de> for ListVisitor {
            type Value = Vec<String>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an array of strings or one comma-delimited string")
            }

            fn visit_str<E: Error>(self, s: &str) -> Result<Self::Value, E> {
                Ok(s.split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .map(str::to_owned)
                    .collect())
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                seq: A,
            ) -> Result<Self::Value, A::Error> {
                Vec::<String>::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))
            }
        }

        deserializer.deserialize_any(ListVisitor)
    }
}

/// Deserialize a backend's `[storage.<backend>]` table into its config struct.
///
/// The table arrives as a `toml::Table`, whose deserializer has no source text
/// to point at; its error puts the key on a second line (``in `pool_size` ``).
/// This helper names the key in front of the message on one line instead:
/// `Failed to parse sqlite config: pool_size: invalid u32 value "-1": invalid
/// digit found in string`. An unknown-field or missing-field message already
/// names its key and is passed through.
///
/// # Errors
///
/// Returns the deserialization error as text: an unknown key, a value of the
/// wrong type, or a missing field.
pub fn parse_table<T: serde::de::DeserializeOwned>(
    table: &toml::Table,
    backend: &str,
) -> Result<T, String> {
    serde_path_to_error::deserialize(toml::Value::Table(table.clone())).map_err(|e| {
        let path = e.path().to_string();
        let inner = e.into_inner();
        let message = inner.message();
        if path == "." || message.starts_with("unknown field") {
            format!("Failed to parse {backend} config: {message}")
        } else {
            format!("Failed to parse {backend} config: {path}: {message}")
        }
    })
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
mod string_coerce_tests;
