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
    /// names the type, and serde appends the key.
    fn parse<T, E>(s: &str, what: &str) -> Result<T, E>
    where
        T: std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Display,
        E: Error,
    {
        s.trim()
            .parse()
            .map_err(|e| E::custom(format!("invalid {what} value \"{s}\": {e}")))
    }

    /// Accepts a scalar as its native type (integer or boolean) or as a string
    /// that parses to it. Every native value goes through `FromStr` as well, so
    /// an out-of-range or negative integer fails with the same message shape
    /// as a bad string (`invalid u32 value "-1": ...`). A float, array or
    /// table is a type error naming what was found and what is accepted.
    struct Scalar<T> {
        what: &'static str,
        _target: PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for Scalar<T>
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
            parse(&v.to_string(), self.what)
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<T, E> {
            parse(&v.to_string(), self.what)
        }

        fn visit_bool<E: Error>(self, v: bool) -> Result<T, E> {
            parse(&v.to_string(), self.what)
        }

        fn visit_f64<E: Error>(self, v: f64) -> Result<T, E> {
            Err(E::invalid_type(Unexpected::Float(v), &self))
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

    /// A native value of the wrong type or range is refused with the value,
    /// the accepted type and the key in the message, so `pool_size = -1` in a
    /// file reads the same as it did before the string form was accepted.
    #[test]
    fn native_wrong_type_errors_name_the_value_type_and_key() {
        let cases: [(&str, &[&str]); 5] = [
            ("count = -1", &["-1", "u32", "count"]),
            ("count = 4294967296", &["4294967296", "u32", "count"]),
            ("count = 4.0", &["floating point", "u32", "count"]),
            ("count = true", &["true", "u32", "count"]),
            ("count = [4]", &["sequence", "u32", "count"]),
        ];
        for (line, expected) in cases {
            let doc = format!("{line}\nflag = false\nport = 1\npassword = \"x\"");
            let err = toml::from_str::<Sample>(&doc).expect_err(line);
            let text = err.to_string();
            for needle in expected {
                assert!(
                    text.contains(needle),
                    "{line}: missing {needle:?} in: {text}"
                );
            }
        }
    }

    /// Native integers are accepted as before: a float that happens to be
    /// whole is not.
    #[test]
    fn native_integers_are_accepted_and_whole_floats_are_not() {
        let s: Sample =
            toml::from_str("count = 7\nflag = false\nport = 1\npassword = \"x\"").expect("int");
        assert_eq!(s.count, 7);
        assert!(
            toml::from_str::<Sample>("count = 7.0\nflag = false\nport = 1\npassword = \"x\"")
                .is_err()
        );
    }

    #[derive(Debug, Deserialize)]
    struct ListSample {
        #[serde(default, deserialize_with = "super::string_coerce::string_list")]
        items: Vec<String>,
    }

    #[test]
    fn a_list_key_takes_an_array_as_written() {
        let s: ListSample = toml::from_str(r#"items = ["a:9042", " b:9042 ", ""]"#).expect("array");
        assert_eq!(s.items, vec!["a:9042", " b:9042 ", ""]);
    }

    /// The environment form: one string, split on commas, each item trimmed,
    /// empty items dropped.
    #[test]
    fn a_list_key_takes_one_comma_delimited_string() {
        let s: ListSample = toml::from_str(r#"items = "a:9042, b:9042,,c:9042 ""#).expect("string");
        assert_eq!(s.items, vec!["a:9042", "b:9042", "c:9042"]);
        let one: ListSample = toml::from_str(r#"items = "/data/in""#).expect("single item");
        assert_eq!(one.items, vec!["/data/in"]);
        let none: ListSample = toml::from_str(r#"items = """#).expect("empty string");
        assert!(none.items.is_empty());
    }

    /// A wrong type is refused with both accepted shapes named, so a startup
    /// error tells the operator what the key takes.
    #[test]
    fn a_list_key_rejects_other_types() {
        let err = toml::from_str::<ListSample>("items = 7").expect_err("integer");
        assert!(
            err.to_string()
                .contains("an array of strings or one comma-delimited string"),
            "got: {err}"
        );
        assert!(toml::from_str::<ListSample>("items = [1, 2]").is_err());
        assert!(toml::from_str::<ListSample>("items = true").is_err());
    }
}
