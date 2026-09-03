// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for `MongoDB` storage backend.

use serde::Deserialize;

/// `MongoDB` storage backend configuration.
///
/// Deliberately does **not** derive `Serialize`: `connection_string` may carry
/// `user:pass@` credentials, and a `Serialize` impl would let them leave the
/// process on any serialize path. Matches the postgres backend, which derives
/// only `Debug, Clone, Deserialize`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MongoStorageConfig {
    /// `MongoDB` connection string (mongodb://...)
    pub connection_string: String,
    /// Maximum concurrent connections for data operations
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Maximum concurrent connections for catalog/management operations
    #[serde(default = "default_max_catalog_connections")]
    pub max_catalog_connections: u32,
    /// Read concern used for the multi-document transactions that back
    /// conditional writes, `TransactWriteItems`, `TransactGetItems`, and the
    /// idempotency-token path (RFC-0003). Defaults to `"snapshot"`, matching
    /// real MongoDB's strongest isolation level.
    ///
    /// Some MongoDB-wire-compatible backends (e.g. DocumentDB) do not
    /// implement `readConcern: snapshot` and reject transactions that
    /// request it with `CommandNotSupported` (error code 115). Setting this
    /// to `"majority"` or `"local"` lets the backend run against such
    /// targets, at the cost of the stronger isolation snapshot reads
    /// provide: concurrent transactions may observe a slightly different
    /// view of the data than they would under snapshot isolation. Only
    /// change this if the target deployment's MongoDB-compatible server
    /// does not support snapshot reads.
    #[serde(default = "default_transaction_read_concern")]
    pub transaction_read_concern: String,
}

fn default_max_connections() -> u32 {
    50
}

fn default_max_catalog_connections() -> u32 {
    20
}

fn default_transaction_read_concern() -> String {
    "snapshot".to_owned()
}

/// Parse [`MongoStorageConfig::transaction_read_concern`] into a driver
/// [`mongodb::options::ReadConcern`].
///
/// Accepts the MongoDB read concern levels valid inside a multi-document
/// transaction (`snapshot`, `majority`, `local`), case-insensitively. Any
/// other value is rejected at startup rather than silently passed through to
/// the driver, so a typo surfaces as a clear configuration error instead of
/// an opaque runtime failure.
pub(crate) fn parse_transaction_read_concern(
    value: &str,
) -> Result<mongodb::options::ReadConcern, String> {
    match value.to_ascii_lowercase().as_str() {
        "snapshot" => Ok(mongodb::options::ReadConcern::snapshot()),
        "majority" => Ok(mongodb::options::ReadConcern::majority()),
        "local" => Ok(mongodb::options::ReadConcern::local()),
        other => Err(format!(
            "invalid storage.mongodb.transaction_read_concern {other:?}: expected one of \
             \"snapshot\", \"majority\", \"local\""
        )),
    }
}

impl extenddb_storage::config::StorageConfig for MongoStorageConfig {
    fn connection_config(&self) -> &str {
        &self.connection_string
    }

    fn max_connections(&self) -> u32 {
        self.max_connections
    }

    fn max_catalog_connections(&self) -> u32 {
        self.max_catalog_connections
    }

    fn startup_warnings(&self) -> Vec<extenddb_storage::config::StartupWarning> {
        let concern = self.transaction_read_concern.to_ascii_lowercase();
        match concern.as_str() {
            "snapshot" => return Vec::new(),
            "majority" | "local" => {}
            _ => return Vec::new(),
        }

        let mut details = vec![
            "Transactions run below DynamoDB isolation: reads inside a transaction".to_owned(),
            "are not point-in-time consistent, and TransactGetItems may not return".to_owned(),
            "a consistent snapshot. Writes remain atomic and durable. Remove the".to_owned(),
            "setting to restore full DynamoDB transaction semantics.".to_owned(),
        ];
        if concern == "local" {
            details.insert(
                3,
                "With local reads, observed data may later roll back during failover.".to_owned(),
            );
        }

        vec![extenddb_storage::config::StartupWarning {
            title: format!("transaction_read_concern = {concern:?} (default: \"snapshot\")."),
            details,
        }]
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TryFrom<toml::Table> for MongoStorageConfig {
    type Error = toml::de::Error;

    fn try_from(table: toml::Table) -> Result<Self, Self::Error> {
        let value = toml::Value::Table(table);
        value.try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_storage::config::StorageConfig as _;

    #[test]
    fn deserialization_applies_transaction_defaults() {
        let config: MongoStorageConfig =
            toml::from_str(r#"connection_string = "mongodb://localhost:27017""#)
                .expect("minimal MongoDB config must deserialize");

        assert_eq!(config.max_connections, 50);
        assert_eq!(config.max_catalog_connections, 20);
        assert_eq!(config.transaction_read_concern, "snapshot");
    }

    #[test]
    fn transaction_read_concern_accepts_supported_values_case_insensitively() {
        for (value, expected) in [
            ("snapshot", mongodb::options::ReadConcern::snapshot()),
            ("MAJORITY", mongodb::options::ReadConcern::majority()),
            ("LoCaL", mongodb::options::ReadConcern::local()),
        ] {
            assert_eq!(
                parse_transaction_read_concern(value),
                Ok(expected),
                "{value} must be accepted"
            );
        }
    }

    #[test]
    fn transaction_read_concern_rejects_values_invalid_for_transactions() {
        for value in ["linearizable", "available", "garbage"] {
            assert_eq!(
                parse_transaction_read_concern(value),
                Err(format!(
                    "invalid storage.mongodb.transaction_read_concern {value:?}: expected one of \
                     \"snapshot\", \"majority\", \"local\""
                ))
            );
        }
    }

    #[test]
    fn deserialization_rejects_unknown_fields() {
        let error = toml::from_str::<MongoStorageConfig>(
            r#"connection_string = "mongodb://localhost:27017"
transaction_read_concerm = "majority""#,
        )
        .expect_err("a typoed MongoDB config field must be rejected");

        assert!(error.to_string().contains("unknown field"));
        assert!(error.to_string().contains("transaction_read_concerm"));
    }

    #[test]
    fn snapshot_read_concern_has_no_startup_warning() {
        let config: MongoStorageConfig =
            toml::from_str(r#"connection_string = "mongodb://localhost:27017""#)
                .expect("minimal MongoDB config must deserialize");

        assert!(config.startup_warnings().is_empty());
    }

    #[test]
    fn weaker_read_concerns_have_startup_warnings() {
        for concern in ["MAJORITY", "local"] {
            let config: MongoStorageConfig = toml::from_str(&format!(
                r#"connection_string = "mongodb://localhost:27017"
transaction_read_concern = "{concern}""#
            ))
            .expect("supported MongoDB config must deserialize");

            let warnings = config.startup_warnings();
            assert_eq!(warnings.len(), 1);
            let message = warnings[0].log_message();
            assert!(message.contains(&concern.to_ascii_lowercase()));
            assert!(message.contains("not point-in-time consistent"));
            assert!(message.contains("Writes remain atomic and durable"));
            assert!(message.contains("restore full DynamoDB transaction semantics"));
            assert_eq!(
                message.contains("roll back during failover"),
                concern == "local"
            );
        }
    }
}
