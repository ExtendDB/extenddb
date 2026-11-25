// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the Cassandra storage configuration.

use super::*;
use extenddb_storage::config::StorageConfig;

#[test]
fn test_config_defaults() {
    let config = CassandraStorageConfig::new(vec!["localhost:9042".to_string()]);

    assert_eq!(config.keyspace_prefix, "extenddb");
    assert_eq!(config.replication_factor, 3);
    assert_eq!(config.datacenter, "datacenter1");
    assert_eq!(config.max_connections, 10);
    assert_eq!(config.username, None);
    assert_eq!(config.password, None);
}

#[test]
fn test_connection_config() {
    let config =
        CassandraStorageConfig::new(vec!["host1:9042".to_string(), "host2:9042".to_string()]);

    // Should return JDBC-style connection string with keyspace
    assert_eq!(config.connection_config(), "host1:9042,host2:9042/extenddb");
}

#[test]
fn test_connection_config_empty() {
    let config = CassandraStorageConfig::new(vec![]);
    // Empty contact points should still include keyspace
    assert_eq!(config.connection_config(), "/extenddb");
}

#[test]
fn the_connection_string_carries_the_role_and_hides_the_password_when_shown() {
    let mut config = CassandraStorageConfig::new(vec!["h1:9042".to_string()]);
    config.username = Some("app:role".to_string());
    config.password = Some("p@ss/w,rd%".to_string());
    config.rebuild_cached_connection_string();
    let conn = config.connection_config();
    assert_eq!(conn, "app%3Arole:p%40ss%2Fw%2Crd%25@h1:9042/extenddb");
    let parts = CassandraStorageConfig::connection_parts(conn);
    assert_eq!(parts.username.as_deref(), Some("app:role"));
    assert_eq!(parts.password.as_deref(), Some("p@ss/w,rd%"));
    assert_eq!(parts.contact_points, ["h1:9042"]);
    assert_eq!(parts.keyspace_prefix, "extenddb");
    assert_eq!(
        CassandraStorageConfig::parse_connection_string(conn),
        (vec!["h1:9042".to_string()], "extenddb".to_string())
    );
    let shown = CassandraStorageConfig::redact_connection_string(conn);
    assert_eq!(shown, "app%3Arole:***@h1:9042/extenddb");
    assert!(!format!("{parts:?}").contains("p@ss"));
    // Without a password nothing is hidden, and without a role nothing
    // is added.
    config.password = None;
    config.rebuild_cached_connection_string();
    assert_eq!(config.connection_config(), "app%3Arole@h1:9042/extenddb");
    config.username = None;
    config.rebuild_cached_connection_string();
    assert_eq!(config.connection_config(), "h1:9042/extenddb");
    assert_eq!(
        CassandraStorageConfig::redact_connection_string("h1:9042/extenddb"),
        "h1:9042/extenddb"
    );
}

#[test]
fn test_toml_deserialization() {
    let toml_str = r#"
        contact_points = ["host1:9042", "host2:9042"]
        username = "cassandra"
        password = "secret"
        keyspace_prefix = "myapp"
        replication_factor = 5
        datacenter = "dc1"
        max_connections = 20
    "#;

    let config: CassandraStorageConfig = toml::from_str(toml_str).unwrap();

    assert_eq!(config.contact_points, vec!["host1:9042", "host2:9042"]);
    assert_eq!(config.username, Some("cassandra".to_string()));
    assert_eq!(config.password, Some("secret".to_string()));
    assert_eq!(config.keyspace_prefix, "myapp");
    assert_eq!(config.replication_factor, 5);
    assert_eq!(config.datacenter, "dc1");
    assert_eq!(config.max_connections, 20);
}

#[test]
fn test_toml_deserialization_minimal() {
    let toml_str = r#"
        contact_points = ["localhost:9042"]
    "#;

    let config: CassandraStorageConfig = toml::from_str(toml_str).unwrap();

    // Verify defaults are applied
    assert_eq!(config.keyspace_prefix, "extenddb");
    assert_eq!(config.replication_factor, 3);
    assert_eq!(config.datacenter, "datacenter1");
    assert_eq!(config.max_connections, 10);
    assert_eq!(config.username, None);
    assert_eq!(config.password, None);
}

fn table(toml_str: &str) -> toml::Table {
    toml::from_str(toml_str).expect("test TOML parses")
}

/// The refusals of `deserialize_config`, each with the key named. Issue
/// #540: a misspelled key, or a key from another backend's section, fails
/// at startup instead of silently leaving the default in force. A string
/// field is never reinterpreted, so a numeric password is still a type
/// error, and a numeric field in its string form must parse.
#[test]
fn deserialize_refuses_with_the_key_named() {
    const KNOWN: &str = "expected one of `contact_points`, `username`, `password`, \
                         `keyspace_prefix`, `replication_factor`, `datacenter`, `max_connections`";
    let rows = [
        (
            "replication_factr = 3",
            format!("Failed to parse cassandra config: unknown field `replication_factr`, {KNOWN}"),
        ),
        (
            "pool_size = 10",
            format!("Failed to parse cassandra config: unknown field `pool_size`, {KNOWN}"),
        ),
        (
            "replication_factor = \"three\"",
            "Failed to parse cassandra config: replication_factor: invalid u32 value \"three\": \
             invalid digit found in string"
                .to_owned(),
        ),
        (
            "password = 12345",
            "Failed to parse cassandra config: password: invalid type: integer `12345`, expected a \
             string"
                .to_owned(),
        ),
    ];
    for (line, expected) in rows {
        let doc = format!("contact_points = [\"localhost:9042\"]\n{line}");
        let err = deserialize_config(&table(&doc)).expect_err(line);
        assert_eq!(err, expected, "{line}");
    }
}

/// Issue #539: `EXTENDDB__STORAGE__CASSANDRA__REPLICATION_FACTOR=5` and
/// `..__MAX_CONNECTIONS=20` arrive in the table as strings.
#[test]
fn deserialize_accepts_numeric_fields_in_string_form() {
    let boxed = deserialize_config(&table(
        r#"
        contact_points = ["localhost:9042"]
        replication_factor = "5"
        max_connections = "20"
    "#,
    ))
    .expect("string-valued numeric fields must parse");
    assert_eq!(boxed.max_connections(), 20);
    let config = boxed
        .as_any()
        .downcast_ref::<CassandraStorageConfig>()
        .expect("cassandra config");
    assert_eq!(config.replication_factor, 5);
}

/// Issue #539: `EXTENDDB__STORAGE__CASSANDRA__CONTACT_POINTS=a:9042,b:9042`
/// arrives as one string; it splits into the list the array form gives.
#[test]
fn deserialize_accepts_contact_points_as_a_comma_delimited_string() {
    let boxed = deserialize_config(&table(
        r#"
        contact_points = "host1:9042, host2:9042"
    "#,
    ))
    .expect("string-valued contact_points must parse");
    assert_eq!(boxed.connection_config(), "host1:9042,host2:9042/extenddb");
    let config = boxed
        .as_any()
        .downcast_ref::<CassandraStorageConfig>()
        .expect("cassandra config");
    assert_eq!(config.contact_points, vec!["host1:9042", "host2:9042"]);
}

/// String fields are never reinterpreted: a numeric-looking password
/// stays a string (`deserialize_refuses_with_the_key_named` has the numeric
/// password).
#[test]
fn deserialize_leaves_string_fields_alone() {
    let boxed = deserialize_config(&table(
        r#"
        contact_points = ["localhost:9042"]
        password = "12345"
    "#,
    ))
    .expect("string password must parse");
    let config = boxed
        .as_any()
        .downcast_ref::<CassandraStorageConfig>()
        .expect("cassandra config");
    assert_eq!(config.password.as_deref(), Some("12345"));
}

#[test]
fn test_build_connection_string() {
    let mut config = CassandraStorageConfig::new(vec![
        "127.0.0.1:9042".to_string(),
        "127.0.0.2:9042".to_string(),
    ]);
    config.keyspace_prefix = "my_keyspace".to_string();
    config.rebuild_cached_connection_string();

    assert_eq!(
        config.cached_connection_string.as_ref().unwrap(),
        "127.0.0.1:9042,127.0.0.2:9042/my_keyspace"
    );
}

#[test]
fn test_build_connection_string_default_keyspace() {
    let config = CassandraStorageConfig::new(vec!["localhost:9042".to_string()]);

    assert_eq!(
        config.cached_connection_string.as_ref().unwrap(),
        "localhost:9042/extenddb"
    );
}

#[test]
fn test_parse_connection_string_with_keyspace() {
    let (contact_points, keyspace) = CassandraStorageConfig::parse_connection_string(
        "127.0.0.1:9042,127.0.0.2:9042/my_keyspace",
    );

    assert_eq!(contact_points.len(), 2);
    assert_eq!(contact_points[0], "127.0.0.1:9042");
    assert_eq!(contact_points[1], "127.0.0.2:9042");
    assert_eq!(keyspace, "my_keyspace");
}

#[test]
fn test_parse_connection_string_without_keyspace() {
    let (contact_points, keyspace) =
        CassandraStorageConfig::parse_connection_string("127.0.0.1:9042,127.0.0.2:9042");

    assert_eq!(contact_points.len(), 2);
    assert_eq!(contact_points[0], "127.0.0.1:9042");
    assert_eq!(contact_points[1], "127.0.0.2:9042");
    assert_eq!(keyspace, "extenddb"); // Default
}

#[test]
fn test_parse_connection_string_single_host() {
    let (contact_points, keyspace) =
        CassandraStorageConfig::parse_connection_string("localhost:9042/test_keyspace");

    assert_eq!(contact_points.len(), 1);
    assert_eq!(contact_points[0], "localhost:9042");
    assert_eq!(keyspace, "test_keyspace");
}

#[test]
fn test_parse_connection_string_with_whitespace() {
    let (contact_points, keyspace) = CassandraStorageConfig::parse_connection_string(
        "  127.0.0.1:9042  ,  127.0.0.2:9042  /  my_keyspace  ",
    );

    assert_eq!(contact_points.len(), 2);
    assert_eq!(contact_points[0], "127.0.0.1:9042");
    assert_eq!(contact_points[1], "127.0.0.2:9042");
    assert_eq!(keyspace, "my_keyspace");
}

#[test]
fn test_roundtrip_connection_string() {
    // Build a connection string
    let mut config =
        CassandraStorageConfig::new(vec!["host1:9042".to_string(), "host2:9042".to_string()]);
    config.keyspace_prefix = "test_ks".to_string();
    config.rebuild_cached_connection_string();

    let conn_str = config.cached_connection_string.unwrap();

    // Parse it back
    let (contact_points, keyspace) = CassandraStorageConfig::parse_connection_string(&conn_str);

    assert_eq!(contact_points.len(), 2);
    assert_eq!(contact_points[0], "host1:9042");
    assert_eq!(contact_points[1], "host2:9042");
    assert_eq!(keyspace, "test_ks");
}
