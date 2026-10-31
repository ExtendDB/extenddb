// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How the runner reaches a deployment: one session built the way the server
//! builds its own, with the credentials the command was given, and every
//! request bounded by [`REQUEST_TIMEOUT`], except a schema statement, which
//! gets [`LOCKED_DDL_TIMEOUT`] under the lock and [`UNLOCKED_DDL_TIMEOUT`]
//! outside it.

use std::sync::Arc;
use std::time::Duration;

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::frame::Envelope;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::statement::StatementParamsBuilder;
use cdrs_tokio::types::rows::Row;
use extenddb_storage::migrations::{MigrationError, Target};

use crate::cassandra_util::CassandraSession;
use crate::config::CassandraStorageConfig;
use crate::engine::CassandraEngine;

/// The bound on every request the runner, the lock and the registration
/// send, apart from a schema statement. It is shorter than the lock's margin
/// (25 s), so a request sent before the holder's deadline has finished
/// before the lock can expire.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The bound on a schema statement sent under the migration lock. A schema
/// statement waits behind every other schema change on its coordinator:
/// measured through cqlsh on one node while ten migration tests ran on it in
/// parallel, 90 table creations, alterations and drops took a median of
/// 1.9 s, 5 of them 10 s or more (10.6, 11.3, 14.8, 24.4 and 76.8 s), where
/// the same statements took about 1 s on the idle node. The bound covers
/// three of those five and is the largest round value inside the lease: it
/// is under the margin (25 s), so a statement sent before the holder's
/// deadline has finished before its row can expire, and under the stall
/// (30 s), so a statement that takes the whole bound does not stop the
/// renewal, since the runner records progress when it completes. The two
/// statements beyond it are beyond the lease too, and a run that meets one
/// fails on it and is run again.
pub(crate) const LOCKED_DDL_TIMEOUT: Duration = Duration::from_secs(20);

/// The bound on a schema statement sent outside the migration lock:
/// CreateAccount's account migrations, which hold no lease and run while
/// the server serves, and the `settings` table that `init` and `migrate`
/// create before the lock exists. A CREATE TABLE waits behind every other
/// schema change on its coordinator, and in the direct integration suite and
/// the pytest suite, run in parallel on one node, such a statement took
/// longer than [`REQUEST_TIMEOUT`].
pub(crate) const UNLOCKED_DDL_TIMEOUT: Duration = Duration::from_secs(60);

/// A deployment: where it is and the role a command connects as.
#[derive(Clone, PartialEq, Eq)]
pub struct Deployment {
    pub contact_points: Vec<String>,
    pub keyspace_prefix: String,
    pub username: Option<String>,
    pub password: Option<String>,
    /// The role the config names, which the server serves as: `username`
    /// unless the command connects as an admin role (`--cassandra-user`).
    pub app_role: Option<String>,
}

impl std::fmt::Debug for Deployment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deployment")
            .field("contact_points", &self.contact_points)
            .field("keyspace_prefix", &self.keyspace_prefix)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("app_role", &self.app_role)
            .finish()
    }
}

impl Deployment {
    /// The deployment a server's config names, with the role it serves as.
    #[must_use]
    pub fn from_config(config: &CassandraStorageConfig) -> Self {
        Self {
            contact_points: config.contact_points.clone(),
            keyspace_prefix: config.keyspace_prefix.clone(),
            username: config.username.clone(),
            password: config.password.clone(),
            app_role: config.username.clone(),
        }
    }

    /// The deployment a command's target names. The location is the config
    /// file's connection string, which carries the role the config names;
    /// an admin role the command was given (`--cassandra-user`) replaces it.
    ///
    /// # Errors
    ///
    /// The location names no contact point.
    pub fn from_target(target: &Target) -> Result<Self, MigrationError> {
        let parts = CassandraStorageConfig::connection_parts(&target.location);
        if parts.contact_points.iter().all(|c| c.trim().is_empty()) {
            return Err(MigrationError::Refused(format!(
                "the Cassandra connection string {:?} names no contact point",
                CassandraStorageConfig::redact_connection_string(&target.location)
            )));
        }
        let (username, password) = match &target.admin {
            Some(admin) => (Some(admin.user.clone()), admin.password.clone()),
            None => (parts.username.clone(), parts.password),
        };
        Ok(Self {
            contact_points: parts.contact_points,
            keyspace_prefix: parts.keyspace_prefix,
            username,
            password,
            app_role: parts.username,
        })
    }

    /// The application role when the command connects as another role, so
    /// a keyspace it creates belongs to that other role and the application
    /// role must be granted its permissions on it; `None` otherwise.
    #[must_use]
    pub fn app_role_if_other(&self) -> Option<String> {
        self.app_role
            .clone()
            .filter(|role| Some(role) != self.username.as_ref())
    }

    /// The catalog keyspace.
    #[must_use]
    pub fn catalog_keyspace(&self) -> String {
        format!("{}_catalog", self.keyspace_prefix)
    }

    /// The keyspace of account `account_id`.
    #[must_use]
    pub fn account_keyspace(&self, account_id: &str) -> String {
        format!("{}_account_{account_id}", self.keyspace_prefix)
    }

    /// A session, built as the engine builds its own: the same credential
    /// path and a 10 s bound on the connect.
    ///
    /// # Errors
    ///
    /// The connect fails or does not finish within 10 s.
    pub async fn connect(&self) -> Result<Arc<CassandraSession>, MigrationError> {
        let mut config = CassandraStorageConfig::new(self.contact_points.clone());
        config.keyspace_prefix.clone_from(&self.keyspace_prefix);
        config.username.clone_from(&self.username);
        config.password.clone_from(&self.password);
        CassandraEngine::create_session(&config)
            .await
            .map(Arc::new)
            .map_err(|e| {
                MigrationError::store(
                    format!("connect to Cassandra at {}", self.contact_points.join(",")),
                    e,
                )
            })
    }
}

/// Send `cql` at `consistency` (and `serial` for a lightweight transaction),
/// bounded by [`REQUEST_TIMEOUT`].
///
/// # Errors
///
/// The request fails or times out; `context` names it.
pub(crate) async fn send(
    session: &CassandraSession,
    cql: &str,
    values: QueryValues,
    consistency: Consistency,
    serial: Option<Consistency>,
    context: &str,
) -> Result<Envelope, MigrationError> {
    send_within(
        session,
        cql,
        values,
        consistency,
        serial,
        context,
        REQUEST_TIMEOUT,
    )
    .await
}

/// [`send`] bounded by `timeout` instead of [`REQUEST_TIMEOUT`].
///
/// # Errors
///
/// The request fails or times out; `context` names it.
pub(crate) async fn send_within(
    session: &CassandraSession,
    cql: &str,
    values: QueryValues,
    consistency: Consistency,
    serial: Option<Consistency>,
    context: &str,
    timeout: Duration,
) -> Result<Envelope, MigrationError> {
    let mut params = StatementParamsBuilder::new()
        .with_consistency(consistency)
        .with_values(values);
    if let Some(serial) = serial {
        params = params.with_serial_consistency(serial);
    }
    match tokio::time::timeout(timeout, session.query_with_params(cql, params.build())).await {
        Ok(Ok(envelope)) => Ok(envelope),
        Ok(Err(e)) => {
            let code = match &e {
                cdrs_tokio::error::Error::Server { body, .. } => {
                    Some(format!("{:#06x}", body.ty.to_error_code()))
                }
                _ => None,
            };
            Err(MigrationError::coded(context, e, code))
        }
        Err(_) => Err(MigrationError::Store(format!(
            "{context}: no answer within {} s",
            timeout.as_secs()
        ))),
    }
}

/// The error codes of a request that may succeed when sent again:
/// Unavailable, Overloaded, IsBootstrapping, WriteTimeout and ReadTimeout.
const TRANSIENT_CODES: [&str; 5] = ["0x1000", "0x1001", "0x1002", "0x1100", "0x1200"];

/// Whether a request that failed with `error` may succeed when sent again:
/// no answer, a connection that failed, or a server error whose code is in
/// [`TRANSIENT_CODES`]. Decided by the code, never by the message.
pub(crate) fn retryable(error: &MigrationError) -> bool {
    error
        .sqlstate()
        .is_none_or(|code| TRANSIENT_CODES.contains(&code))
}

/// The rows of a read sent with [`send`].
///
/// # Errors
///
/// As [`send`], or the answer holds no rows.
pub(crate) async fn rows(
    session: &CassandraSession,
    cql: &str,
    values: QueryValues,
    consistency: Consistency,
    context: &str,
) -> Result<Vec<Row>, MigrationError> {
    let envelope = send(session, cql, values, consistency, None, context).await?;
    envelope
        .response_body()
        .map_err(|e| MigrationError::store(context, e))?
        .into_rows()
        .ok_or_else(|| MigrationError::Store(format!("{context}: the answer holds no rows")))
}

/// A column of a row, or an error naming it.
///
/// # Errors
///
/// The column is missing or has another type.
pub(crate) fn column<T>(row: &Row, name: &str, context: &str) -> Result<T, MigrationError>
where
    Row: cdrs_tokio::types::IntoRustByName<T>,
{
    use cdrs_tokio::types::IntoRustByName;
    row.get_r_by_name(name)
        .map_err(|e| MigrationError::store(format!("{context}: column {name}"), e))
}

/// A column that may be null.
///
/// # Errors
///
/// The column is missing or has another type.
pub(crate) fn nullable<T>(row: &Row, name: &str, context: &str) -> Result<Option<T>, MigrationError>
where
    Row: cdrs_tokio::types::IntoRustByName<T>,
{
    use cdrs_tokio::types::IntoRustByName;
    row.get_by_name(name)
        .map_err(|e| MigrationError::store(format!("{context}: column {name}"), e))
}

/// A `map<text, text>` column, empty when null.
///
/// # Errors
///
/// The column is missing or has another type.
pub(crate) fn text_map(
    row: &Row,
    name: &str,
    context: &str,
) -> Result<std::collections::HashMap<String, String>, MigrationError> {
    use cdrs_tokio::types::AsRust;
    let Some(map) = nullable::<cdrs_tokio::types::map::Map>(row, name, context)? else {
        return Ok(std::collections::HashMap::new());
    };
    let map = map
        .as_rust::<std::collections::HashMap<String, String>>()
        .map_err(|e| MigrationError::store(format!("{context}: column {name}"), e))?;
    Ok(map.unwrap_or_default())
}

/// Whether `table` exists in `keyspace`, read from `system_schema`, which
/// every node holds the same once the schema agrees.
///
/// # Errors
///
/// The read fails.
pub(crate) async fn table_exists(
    session: &CassandraSession,
    keyspace: &str,
    table: &str,
) -> Result<bool, MigrationError> {
    let found = rows(
        session,
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
        cdrs_tokio::query_values!(keyspace, table),
        Consistency::One,
        &format!("read whether {keyspace}.{table} exists"),
    )
    .await?;
    Ok(!found.is_empty())
}

/// Whether `table` exists in `keyspace` with every column of `columns`.
///
/// # Errors
///
/// The read fails.
pub(crate) async fn has_columns(
    session: &CassandraSession,
    keyspace: &str,
    table: &str,
    columns: &[&str],
) -> Result<bool, MigrationError> {
    let found = rows(
        session,
        "SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
        cdrs_tokio::query_values!(keyspace, table),
        Consistency::One,
        &format!("read the columns of {keyspace}.{table}"),
    )
    .await?;
    let names = found
        .iter()
        .map(|row| column::<String>(row, "column_name", "read columns"))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().all(|c| names.iter().any(|n| n == c)))
}

/// The tables of `keyspace`, empty when the keyspace does not exist.
///
/// # Errors
///
/// The read fails.
pub(crate) async fn tables_of(
    session: &CassandraSession,
    keyspace: &str,
) -> Result<Vec<String>, MigrationError> {
    let found = rows(
        session,
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
        cdrs_tokio::query_values!(keyspace),
        Consistency::One,
        &format!("list the tables of {keyspace}"),
    )
    .await?;
    let mut names = found
        .iter()
        .map(|row| column::<String>(row, "table_name", "list tables"))
        .collect::<Result<Vec<_>, _>>()?;
    names.sort_unstable();
    Ok(names)
}

/// The replication of `keyspace` as `system_schema` records it, written as
/// the map literal a `CREATE KEYSPACE` takes; `None` when the keyspace does
/// not exist.
///
/// # Errors
///
/// The read fails.
pub(crate) async fn replication_of(
    session: &CassandraSession,
    keyspace: &str,
) -> Result<Option<String>, MigrationError> {
    let found = rows(
        session,
        "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?",
        cdrs_tokio::query_values!(keyspace),
        Consistency::One,
        &format!("read the replication of {keyspace}"),
    )
    .await?;
    let Some(row) = found.first() else {
        return Ok(None);
    };
    let map = text_map(row, "replication", "read the replication")?;
    Ok(Some(replication_literal(&map)))
}

/// A replication map as a CQL map literal, in a stable order.
pub(crate) fn replication_literal(map: &std::collections::HashMap<String, String>) -> String {
    let mut entries: Vec<(&String, &String)> = map.iter().collect();
    entries.sort();
    let quote = |s: &str| format!("'{}'", s.replace('\'', "''"));
    let body: Vec<String> = entries
        .into_iter()
        .map(|(k, v)| format!("{}: {}", quote(k), quote(v)))
        .collect();
    format!("{{{}}}", body.join(", "))
}

#[cfg(test)]
mod tests {
    use extenddb_storage::migrations::{AdminRole, MigrationError, Target};

    use super::{Deployment, TRANSIENT_CODES, replication_literal, retryable};

    /// The errors a request may be sent again after, decided by the code
    /// (finding 4): no answer or a failed connection, which carry no code,
    /// and the five transient server codes; a server error with any other
    /// code is final.
    #[test]
    fn a_request_is_sent_again_after_a_transient_code_and_never_after_another() {
        let coded =
            |code: &str| MigrationError::coded("send", "server error", Some(code.to_owned()));
        assert!(retryable(&MigrationError::Store(
            "send: no answer within 10 s".to_owned()
        )));
        assert!(retryable(&MigrationError::coded(
            "send",
            "connection reset",
            None
        )));
        assert_eq!(
            TRANSIENT_CODES,
            ["0x1000", "0x1001", "0x1002", "0x1100", "0x1200"],
            "Unavailable, Overloaded, IsBootstrapping, WriteTimeout, ReadTimeout"
        );
        for code in TRANSIENT_CODES {
            assert!(retryable(&coded(code)), "{code}");
        }
        for code in [
            "0x0000", "0x000a", "0x1101", "0x1201", "0x1300", "0x1400", "0x1500", "0x2000",
            "0x2100", "0x2200", "0x2300", "0x2400", "0x2500",
        ] {
            assert!(!retryable(&coded(code)), "{code}");
        }
    }

    #[test]
    fn a_command_connects_as_the_config_role_unless_given_an_admin() {
        let target = Target::at("app:s%40cret@10.0.0.1:9042,10.0.0.2:9042/acme");
        let deployment = Deployment::from_target(&target).expect("deployment");
        assert_eq!(
            deployment.contact_points,
            ["10.0.0.1:9042", "10.0.0.2:9042"]
        );
        assert_eq!(deployment.catalog_keyspace(), "acme_catalog");
        assert_eq!(deployment.account_keyspace("123"), "acme_account_123");
        assert_eq!(deployment.username.as_deref(), Some("app"));
        assert_eq!(deployment.password.as_deref(), Some("s@cret"));
        let shown = format!("{deployment:?}");
        assert!(!shown.contains("s@cret"), "{shown}");

        let target = Target {
            admin: Some(AdminRole {
                user: "dba".to_owned(),
                password: Some("pw".to_owned()),
            }),
            ..target
        };
        let deployment = Deployment::from_target(&target).expect("deployment");
        assert_eq!(deployment.username.as_deref(), Some("dba"));
        assert_eq!(deployment.password.as_deref(), Some("pw"));

        let none = Deployment::from_target(&Target::at("/acme")).expect_err("no contact point");
        assert!(none.to_string().contains("no contact point"), "{none}");
    }

    #[test]
    fn a_replication_map_is_written_back_as_it_was_read() {
        let map = [
            (
                "class",
                "org.apache.cassandra.locator.NetworkTopologyStrategy",
            ),
            ("dc1", "3"),
            ("dc2", "1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
        assert_eq!(
            replication_literal(&map),
            "{'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'dc1': '3', \
             'dc2': '1'}"
        );
    }
}
