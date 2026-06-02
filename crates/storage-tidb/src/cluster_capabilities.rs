// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB server capability validation.

use std::fmt;

use extenddb_storage::error::StorageError;
use sqlx::MySqlPool;

const MIN_GLOBAL_NON_UNIQUE_INDEX_VERSION: TidbVersion = TidbVersion {
    major: 8,
    minor: 5,
    patch: 4,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TidbVersion {
    major: u16,
    minor: u16,
    patch: u16,
}

impl fmt::Display for TidbVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}.{}.{}", self.major, self.minor, self.patch)
    }
}

pub(crate) async fn validate_tidb_capabilities(pool: &MySqlPool) -> Result<(), StorageError> {
    let version_info: String = sqlx::query_scalar("SELECT tidb_version()")
        .fetch_one(pool)
        .await
        .map_err(|e| StorageError::Connection(format!("read TiDB version: {e}")))?;

    let version = parse_tidb_release_version(&version_info).ok_or_else(|| {
        StorageError::Configuration(format!(
            "cannot parse TiDB release version from tidb_version(): {}",
            version_info.lines().next().unwrap_or("<empty>")
        ))
    })?;

    validate_version(version)
}

fn validate_version(version: TidbVersion) -> Result<(), StorageError> {
    if version < MIN_GLOBAL_NON_UNIQUE_INDEX_VERSION {
        return Err(unsupported_version_error(version));
    }
    Ok(())
}

fn unsupported_version_error(version: TidbVersion) -> StorageError {
    StorageError::Configuration(format!(
        "TiDB backend requires TiDB >= {MIN_GLOBAL_NON_UNIQUE_INDEX_VERSION} because partitioned DynamoDB data tables use non-unique GLOBAL indexes for native secondary-index reads; detected {version}"
    ))
}

fn parse_tidb_release_version(version_info: &str) -> Option<TidbVersion> {
    version_info
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("Release Version:")
                .and_then(|value| parse_version_token(value.trim()))
        })
        .or_else(|| {
            let trimmed = version_info.trim();
            if trimmed.lines().count() == 1 && trimmed.starts_with('v') {
                parse_version_token(trimmed)
            } else {
                None
            }
        })
}

fn parse_version_token(token: &str) -> Option<TidbVersion> {
    let token = token.trim().trim_start_matches('v');
    let mut parts = token.split(['.', '-', '+']);

    Some(TidbVersion {
        major: parts.next()?.parse().ok()?,
        minor: parts.next()?.parse().ok()?,
        patch: parts.next()?.parse().ok()?,
    })
}

#[cfg(test)]
mod tests {
    use extenddb_storage::error::StorageError;

    use super::{
        MIN_GLOBAL_NON_UNIQUE_INDEX_VERSION, TidbVersion, parse_tidb_release_version,
        validate_version,
    };

    #[test]
    fn parses_tidb_version_function_output() {
        let version =
            parse_tidb_release_version("Release Version: v8.5.6\nEdition: Community\nStore: tikv")
                .expect("version");

        assert_eq!(
            version,
            TidbVersion {
                major: 8,
                minor: 5,
                patch: 6,
            }
        );
    }

    #[test]
    fn parses_single_release_token() {
        assert_eq!(
            parse_tidb_release_version("v8.5.4-dirty").expect("version"),
            MIN_GLOBAL_NON_UNIQUE_INDEX_VERSION
        );
    }

    #[test]
    fn rejects_version_before_non_unique_global_index_support() {
        let err = validate_version(TidbVersion {
            major: 8,
            minor: 5,
            patch: 3,
        })
        .unwrap_err();

        assert!(matches!(err, StorageError::Configuration(_)));
        assert!(err.to_string().contains("requires TiDB >= v8.5.4"));
    }

    #[test]
    fn accepts_minimum_supported_version() {
        validate_version(MIN_GLOBAL_NON_UNIQUE_INDEX_VERSION).expect("minimum version");
    }

    #[test]
    fn does_not_parse_mysql_compatibility_version_as_tidb_release() {
        assert!(parse_tidb_release_version("8.0.11-TiDB-v8.5.6").is_none());
    }
}
