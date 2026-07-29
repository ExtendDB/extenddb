// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage configuration trait and registry for storage backends.

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

    /// Clone this config into a boxed trait object.
    fn clone_box(&self) -> Box<dyn StorageConfig>;

    /// Enable downcasting to specific storage engine config types to allow
    /// access to engine-specific configuration (e.g. `keyspace_prefix` for
    /// the Cassandra backend).
    fn as_any(&self) -> &dyn std::any::Any
    where
        Self: 'static;
}

impl Clone for Box<dyn StorageConfig> {
    fn clone(&self) -> Self {
        self.clone_box()
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

/// Registration entry for a storage config deserializer.
pub struct StorageConfigRegistration {
    pub backend: &'static str,
    pub deserializer: StorageConfigDeserializer,
}

inventory::collect!(StorageConfigRegistration);

/// Deserialize a storage configuration from a TOML table.
///
/// Looks up the registered deserializer for the given backend name
/// and invokes it with the provided TOML table.
pub fn deserialize_storage_config(
    backend: &str,
    table: &toml::Table,
) -> Result<Box<dyn StorageConfig>, String> {
    for reg in inventory::iter::<StorageConfigRegistration> {
        if reg.backend == backend {
            return (reg.deserializer)(table);
        }
    }
    Err(format!("Unknown backend: {backend}"))
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
