// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra storage backend configuration.

use serde::{Deserialize, Deserializer};

/// Cassandra storage backend configuration.
///
/// Unknown keys are refused like every other section (issue #540), so a
/// misspelled key fails at startup instead of silently taking the default.
/// Environment overrides (`EXTENDDB__STORAGE__CASSANDRA__<KEY>`) reach this
/// deserializer as strings, so the numeric fields accept their string form and
/// `contact_points` accepts one comma-delimited string (issue #539).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CassandraStorageConfig {
    /// Cassandra contact points (host:port)
    #[serde(deserialize_with = "extenddb_storage::config::string_coerce::string_list")]
    pub contact_points: Vec<String>,

    /// Username for authentication
    pub username: Option<String>,

    /// Password for authentication
    pub password: Option<String>,

    /// Keyspace prefix (default: "extenddb"), in lower case. A prefix with an
    /// upper-case letter is lower-cased when the config is read: see
    /// [`fold_keyspace_prefix`].
    #[serde(
        default = "default_keyspace_prefix",
        deserialize_with = "deserialize_keyspace_prefix"
    )]
    pub keyspace_prefix: String,

    /// Replication factor for keyspaces (default: 3, for a section with no
    /// `replication_factor` line; `init` always writes the line)
    #[serde(
        default = "default_replication_factor",
        deserialize_with = "extenddb_storage::config::string_coerce::u32"
    )]
    pub replication_factor: u32,

    /// Datacenter name for `NetworkTopologyStrategy` (default: "datacenter1")
    #[serde(default = "default_datacenter")]
    pub datacenter: String,

    /// Maximum connections per host (default: 10)
    #[serde(
        default = "default_max_connections",
        deserialize_with = "extenddb_storage::config::string_coerce::u32"
    )]
    pub max_connections: u32,

    /// Cached connection string (JDBC-style: `host1,host2/keyspace_prefix`)
    /// This is computed after deserialization and cached for `connection_config()`
    #[serde(skip)]
    pub cached_connection_string: Option<String>,

    /// Stable identifier for this server instance, set by the server before
    /// constructing the engine. Used to derive the HLC node ID.
    #[serde(skip)]
    pub instance_id: Option<String>,
}

fn default_keyspace_prefix() -> String {
    "extenddb".to_string()
}

/// The keyspace prefix in the form Cassandra stores it.
///
/// ExtendDB writes keyspace names into CQL without quotes, and Cassandra
/// stores an unquoted name in lower case: `CREATE KEYSPACE Acme_catalog`
/// makes `acme_catalog`. The code compares the names it builds with the
/// names in `system_schema`, which are lower case, so a prefix with an
/// upper-case letter would match none of them. The three places that read a
/// prefix from outside (the config file, the connection string and `init
/// --keyspace-prefix`) call this function, so the rest of the crate sees the
/// lower-case form. Code that builds a [`CassandraStorageConfig`] itself must
/// pass a lower-case prefix: `CassandraEngine::new` asserts it in debug
/// builds.
#[must_use]
pub(crate) fn fold_keyspace_prefix(prefix: &str) -> String {
    prefix.to_ascii_lowercase()
}

fn deserialize_keyspace_prefix<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    String::deserialize(deserializer).map(|prefix| fold_keyspace_prefix(&prefix))
}

fn default_replication_factor() -> u32 {
    3
}

fn default_datacenter() -> String {
    "datacenter1".to_string()
}

fn default_max_connections() -> u32 {
    10
}

/// The parts of a Cassandra connection string.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectionParts {
    pub contact_points: Vec<String>,
    pub keyspace_prefix: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl std::fmt::Debug for ConnectionParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionParts")
            .field("contact_points", &self.contact_points)
            .field("keyspace_prefix", &self.keyspace_prefix)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Every byte outside ASCII letters, digits and `-._~` as `%XX`, so a role or
/// password holding `@`, `:`, `/` or `,` cannot be read as another part.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// The inverse of [`percent_encode`]; a `%` not followed by two hex digits
/// stays as it is.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (bytes[i], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl CassandraStorageConfig {
    /// Build the connection string: `[user[:password]@]host1,host2/keyspace_prefix`.
    ///
    /// The role and password are the config's, percent-encoded, so a command
    /// that receives only the connection string (`verify`, `settings`,
    /// `migrate`) connects with the role the server uses.
    /// [`Self::redact_connection_string`] hides the password wherever the
    /// string is shown.
    fn build_connection_string(&self) -> String {
        let hosts = self.contact_points.join(",");
        let userinfo = match (&self.username, &self.password) {
            (Some(user), Some(password)) => {
                format!("{}:{}@", percent_encode(user), percent_encode(password))
            }
            (Some(user), None) => format!("{}@", percent_encode(user)),
            (None, _) => String::new(),
        };
        format!("{userinfo}{hosts}/{}", self.keyspace_prefix)
    }

    /// Ensure the cached connection string is populated.
    /// Call this after deserialization or construction.
    pub fn ensure_cached_connection_string(&mut self) {
        if self.cached_connection_string.is_none() {
            self.cached_connection_string = Some(self.build_connection_string());
        }
    }

    /// Rebuild the cached connection string.
    /// Use this after modifying `contact_points` or `keyspace_prefix`.
    pub fn rebuild_cached_connection_string(&mut self) {
        self.cached_connection_string = Some(self.build_connection_string());
    }

    /// Parse a JDBC-style connection string into contact points and keyspace prefix.
    /// Format: "`[user[:password]@]host1:port1,host2:port2/keyspace_prefix`"
    /// Returns (`contact_points`, `keyspace_prefix`)
    #[must_use]
    pub fn parse_connection_string(conn_str: &str) -> (Vec<String>, String) {
        let parts = Self::connection_parts(conn_str);
        (parts.contact_points, parts.keyspace_prefix)
    }

    /// Every part of a connection string [`Self::build_connection_string`]
    /// wrote, the role and password decoded.
    #[must_use]
    pub fn connection_parts(conn_str: &str) -> ConnectionParts {
        let (userinfo, rest) = match conn_str.rsplit_once('@') {
            Some((userinfo, rest)) => (Some(userinfo), rest),
            None => (None, conn_str),
        };
        let (username, password) = match userinfo {
            Some(info) => match info.split_once(':') {
                Some((user, password)) => {
                    (Some(percent_decode(user)), Some(percent_decode(password)))
                }
                None => (Some(percent_decode(info)), None),
            },
            None => (None, None),
        };
        let (hosts, keyspace_prefix) = match rest.split_once('/') {
            Some((hosts, keyspace)) => (hosts, fold_keyspace_prefix(keyspace.trim())),
            None => (rest, default_keyspace_prefix()),
        };
        ConnectionParts {
            contact_points: hosts.split(',').map(|s| s.trim().to_string()).collect(),
            keyspace_prefix,
            username,
            password,
        }
    }

    /// The connection string with its password replaced by `***`.
    #[must_use]
    pub fn redact_connection_string(conn_str: &str) -> String {
        match conn_str.rsplit_once('@') {
            Some((userinfo, rest)) => match userinfo.split_once(':') {
                Some((user, _)) => format!("{user}:***@{rest}"),
                None => conn_str.to_owned(),
            },
            None => conn_str.to_owned(),
        }
    }

    /// Create default config programmatically
    #[must_use]
    pub fn new(contact_points: Vec<String>) -> Self {
        let mut config = Self {
            contact_points,
            username: None,
            password: None,
            keyspace_prefix: default_keyspace_prefix(),
            replication_factor: default_replication_factor(),
            datacenter: default_datacenter(),
            max_connections: default_max_connections(),
            cached_connection_string: None,
            instance_id: None,
        };
        config.ensure_cached_connection_string();
        config
    }
}

impl extenddb_storage::config::StorageConfig for CassandraStorageConfig {
    fn connection_config(&self) -> &str {
        // Return JDBC-style connection string: host1,host2/keyspace_prefix
        // This allows factories to parse both contact points and keyspace
        self.cached_connection_string
            .as_deref()
            .unwrap_or("(no connection string)")
    }

    fn max_connections(&self) -> u32 {
        self.max_connections
    }

    fn max_catalog_connections(&self) -> u32 {
        // Cassandra uses same connection pool for catalog and data
        self.max_connections
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(self.clone())
    }

    fn set_instance_id(&mut self, instance_id: &str) {
        self.instance_id = Some(instance_id.to_string());
    }

    fn instance_id(&self) -> Option<&str> {
        self.instance_id.as_deref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Deserialize a `[storage.cassandra]` TOML table into a boxed `StorageConfig`.
///
/// This is the entry point the installed backend hands to `extenddb-config`,
/// so what it accepts is what `extenddb serve` accepts.
///
/// # Errors
///
/// Returns the deserialization error as text: an unknown key, a value of the
/// wrong type, or a missing `contact_points`.
pub fn deserialize_config(
    table: &toml::Table,
) -> Result<Box<dyn extenddb_storage::config::StorageConfig>, String> {
    let mut config: CassandraStorageConfig =
        extenddb_storage::config::parse_table(table, "cassandra")?;
    config.ensure_cached_connection_string();
    Ok(Box::new(config))
}

#[cfg(test)]
mod tests;
