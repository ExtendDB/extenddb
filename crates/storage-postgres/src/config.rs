// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL` connection configuration.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresStorageConfig {
    #[serde(default = "default_connection_string")]
    pub connection_string: String,
    // string_coerce: environment-variable overrides
    // (EXTENDDB__STORAGE__POSTGRES__POOL_SIZE=10) arrive as strings; accept
    // both forms (issue #222).
    #[serde(
        default = "default_pool_size",
        deserialize_with = "extenddb_storage::config::string_coerce::u32"
    )]
    pub pool_size: u32,
    /// Maximum connections for the management/catalog pool (authz, IAM, console).
    /// Defaults to `pool_size` if not set.
    #[serde(
        default,
        deserialize_with = "extenddb_storage::config::string_coerce::opt_u32"
    )]
    pub catalog_pool_size: Option<u32>,
}

impl Default for PostgresStorageConfig {
    fn default() -> Self {
        Self {
            connection_string: default_connection_string(),
            pool_size: default_pool_size(),
            catalog_pool_size: None,
        }
    }
}

fn default_connection_string() -> String {
    "postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog".to_owned()
}

fn default_pool_size() -> u32 {
    20
}

/// Parsed components of a `PostgreSQL` connection string.
pub struct ConnParts {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub database: String,
}

/// Parse host, port, user, password, and database from a `PostgreSQL` connection string.
///
/// Handles the standard `postgresql://user:pass@host:port/db` format.
///
/// Percent-decodes every component, mirroring the encoding applied by
/// `extenddb init` when it writes the connection string (and the decoding
/// sqlx applies when it reads one on the `serve` path). Without the decode, a
/// Unix socket host such as `/run/postgresql` — written as
/// `%2Frun%2Fpostgresql` — is treated as a DNS hostname and fails resolution,
/// so a config that `serve` accepts is rejected by `migrate` and `verify`
/// (issue #223). Decoding is lenient: a literal `%` that does not form a
/// valid escape passes through unchanged. This matches sqlx's parser (libpq
/// is stricter and rejects malformed escapes); since sqlx is the parser the
/// `serve` path uses, a strict parser here would recreate the #223 split in
/// the other direction — a hand-written config with a raw `%` in a password
/// would work under `serve` but fail under `migrate` and `verify`.
///
/// # Errors
///
/// Returns an error if the connection string doesn't match the expected format
/// or a component decodes to invalid UTF-8.
pub fn parse_connection_string(conn: &str) -> anyhow::Result<ConnParts> {
    let rest = conn
        .strip_prefix("postgresql://")
        .or_else(|| conn.strip_prefix("postgres://"))
        .ok_or_else(|| {
            anyhow::anyhow!("Connection string must start with postgresql:// or postgres://")
        })?;

    let (userpass, hostdb) = rest
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("Connection string missing '@' separator"))?;

    let (user, password) = userpass.split_once(':').map_or_else(
        || (userpass.to_owned(), String::new()),
        |(u, p)| (u.to_owned(), p.to_owned()),
    );

    let (hostport, database) = hostdb
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("Connection string missing /database"))?;

    let (host, port_str) = hostport
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("Connection string missing :port"))?;

    let port: u16 = port_str
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid port: {port_str}"))?;

    let decode = |component: &str, what: &str| -> anyhow::Result<String> {
        Ok(urlencoding::decode(component)
            .map_err(|e| anyhow::anyhow!("{what} decodes to invalid UTF-8: {e}"))?
            .into_owned())
    };

    Ok(ConnParts {
        user: decode(&user, "user")?,
        password: decode(&password, "password")?,
        host: decode(host, "host")?,
        port,
        database: decode(database, "database")?,
    })
}

// ── StorageConfig trait implementation ────────────────────────────────

impl extenddb_storage::config::StorageConfig for PostgresStorageConfig {
    fn connection_config(&self) -> &str {
        &self.connection_string
    }

    fn max_connections(&self) -> u32 {
        self.pool_size
    }

    fn max_catalog_connections(&self) -> u32 {
        self.catalog_pool_size.unwrap_or(self.pool_size)
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod env_override_tests {
    use super::PostgresStorageConfig;

    /// Issue #222: `EXTENDDB__STORAGE__POSTGRES__CATALOG_POOL_SIZE=10` reaches
    /// this deserializer as the string "10". The typed fields must accept it.
    #[test]
    fn string_valued_numeric_fields_deserialize() {
        let cfg: PostgresStorageConfig = toml::from_str(
            r#"connection_string = "postgresql://u:p@localhost:5432/db"
pool_size = "25"
catalog_pool_size = "10""#,
        )
        .expect("string-valued numeric fields must deserialize");
        assert_eq!(cfg.pool_size, 25);
        assert_eq!(cfg.catalog_pool_size, Some(10));
    }

    #[test]
    fn native_numeric_fields_still_deserialize() {
        let cfg: PostgresStorageConfig = toml::from_str(
            r#"connection_string = "postgresql://u:p@localhost:5432/db"
pool_size = 25
catalog_pool_size = 10"#,
        )
        .expect("native numeric fields must deserialize");
        assert_eq!(cfg.pool_size, 25);
        assert_eq!(cfg.catalog_pool_size, Some(10));
    }
}

#[cfg(test)]
mod tests {
    use super::parse_connection_string;

    #[test]
    fn parses_a_plain_tcp_connection_string() {
        let parts =
            parse_connection_string("postgresql://extenddb:secret@localhost:5432/extenddb_catalog")
                .expect("plain connection string must parse");
        assert_eq!(parts.user, "extenddb");
        assert_eq!(parts.password, "secret");
        assert_eq!(parts.host, "localhost");
        assert_eq!(parts.port, 5432);
        assert_eq!(parts.database, "extenddb_catalog");
    }

    /// Issue #223: `init --pg-host /run/postgresql` writes the host
    /// percent-encoded. The parser must decode it back to the socket path
    /// instead of handing `%2Frun%2Fpostgresql` to DNS resolution.
    #[test]
    fn decodes_a_percent_encoded_unix_socket_host() {
        let parts = parse_connection_string(
            "postgresql://extenddb:secret@%2Frun%2Fpostgresql:5432/extenddb_catalog",
        )
        .expect("socket connection string must parse");
        assert_eq!(parts.host, "/run/postgresql");
    }

    /// Round trip: whatever `init`'s encoder writes, this parser must read.
    #[test]
    fn round_trips_the_encoding_init_uses() {
        let host = "/var/run/postgresql";
        let user = "app user";
        let pass = "p@ss:w/rd%100";
        let conn = format!(
            "postgresql://{}:{}@{}:5432/extenddb_catalog",
            urlencoding::encode(user),
            urlencoding::encode(pass),
            urlencoding::encode(host),
        );
        let parts = parse_connection_string(&conn).expect("encoded connection string must parse");
        assert_eq!(parts.user, user);
        assert_eq!(parts.password, pass);
        assert_eq!(parts.host, host);
    }

    /// A hand-written config with a literal `%` not forming a valid escape
    /// must keep working: the decoder is lenient and passes malformed escapes
    /// through unchanged, matching sqlx's parser on the `serve` path (libpq is
    /// stricter, but sqlx is what the rest of the product uses), so
    /// pre-existing configs with raw `%` in a password are not broken by the
    /// decode step.
    #[test]
    fn passes_through_a_literal_percent_that_is_not_an_escape() {
        let parts = parse_connection_string("postgresql://extenddb:se%ZZcret@localhost:5432/db")
            .expect("literal % must not break parsing");
        assert_eq!(parts.password, "se%ZZcret");
    }

    /// `%20` decodes to a space in any component.
    #[test]
    fn decodes_a_percent_encoded_space() {
        let parts = parse_connection_string("postgresql://app%20user:secret@localhost:5432/db")
            .expect("encoded space must parse");
        assert_eq!(parts.user, "app user");
    }

    /// A literal `+` stays a `+`. Plus-as-space is form encoding
    /// (application/x-www-form-urlencoded), not URI percent-encoding; neither
    /// libpq nor `urlencoding::encode` treats `+` as a space in connection
    /// URIs, so a password containing `+` must survive unchanged.
    #[test]
    fn keeps_a_literal_plus_as_a_plus() {
        let parts = parse_connection_string("postgresql://extenddb:a+b@localhost:5432/db")
            .expect("literal plus must parse");
        assert_eq!(parts.password, "a+b");
    }

    #[test]
    fn missing_scheme_is_rejected() {
        assert!(parse_connection_string("mysql://u:p@h:5432/db").is_err());
    }
}
