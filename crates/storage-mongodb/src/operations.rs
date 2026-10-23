// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MongoDB` implementation of `OperationsEngine`.

use extenddb_storage::error::StorageError;
use extenddb_storage::operations::{ConnectionParts, OperationsEngine};

/// `MongoDB` operations engine for extenddb CLI commands.
pub struct MongoOperationsEngine;

impl OperationsEngine for MongoOperationsEngine {
    fn parse_connection_string(&self, s: &str) -> Result<ConnectionParts, StorageError> {
        // Best-effort parse of `mongodb[+srv]://[user[:pass]@]host[:port][/db][?...]`.
        // The mongo driver owns full URI validation at connect time; this is only
        // for display and CLI-side identifier extraction.
        let scheme_end = s
            .find("://")
            .ok_or_else(|| StorageError::Internal("connection string has no scheme".to_owned()))?
            + 3;
        let rest = &s[scheme_end..];

        // Split at first `?` to drop query string.
        let (authority_path, _) = rest.split_once('?').unwrap_or((rest, ""));

        // Split userinfo from host by the last `@` before the first `/`.
        let path_start = authority_path.find('/').unwrap_or(authority_path.len());
        let authority = &authority_path[..path_start];
        let path = authority_path[path_start..].trim_start_matches('/');

        let (user, password, hostport) = if let Some(at) = authority.rfind('@') {
            let (userinfo, hp) = authority.split_at(at);
            let hp = &hp[1..];
            let (u, p) = userinfo
                .split_once(':')
                .map_or((userinfo, ""), |(u, p)| (u, p));
            (u.to_owned(), p.to_owned(), hp)
        } else {
            (String::new(), String::new(), authority)
        };

        // hostport may be a comma-separated list for replica sets — take the first
        // seed. Port defaults to 27017 (matches mongo driver default).
        let first_host = hostport.split(',').next().unwrap_or(hostport);
        let (host, port) = if let Some((h, p)) = first_host.rsplit_once(':') {
            (h.to_owned(), p.parse::<u16>().unwrap_or(27017))
        } else {
            (first_host.to_owned(), 27017)
        };

        Ok(ConnectionParts {
            host,
            port,
            user,
            password,
            database: path.to_owned(),
        })
    }

    fn redact_connection_string(&self, s: &str) -> String {
        // Redact password from mongodb[+srv]://user:password@host[:port]/...
        let Some(scheme_end) = s.find("://") else {
            return s.to_owned();
        };
        let after_scheme = scheme_end + 3;
        // Only consider `@` before the first `?`.
        let query_start = s[after_scheme..]
            .find('?')
            .map_or(s.len(), |q| after_scheme + q);
        let Some(at) = s[after_scheme..query_start].rfind('@') else {
            return s.to_owned();
        };
        let at_idx = after_scheme + at;
        let userinfo = &s[after_scheme..at_idx];
        let Some(colon) = userinfo.find(':') else {
            return s.to_owned();
        };
        let user = &userinfo[..colon];
        format!("{}{user}:***{}", &s[..after_scheme], &s[at_idx..])
    }

    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError> {
        // MongoDB collection/database identifier constraints:
        //   - no `$` prefix reserved for operators
        //   - no `.` (used as path separator inside documents)
        //   - no `\0` (null byte)
        //   - no non-ASCII
        if name.contains('$') {
            return Err(StorageError::Internal(format!(
                "{label} must not contain '$'"
            )));
        }
        if name.contains('.') {
            return Err(StorageError::Internal(format!(
                "{label} must not contain '.'"
            )));
        }
        if name.contains('\0') {
            return Err(StorageError::Internal(format!(
                "{label} must not contain null bytes"
            )));
        }
        if !name.is_ascii() {
            return Err(StorageError::Internal(format!(
                "{label} must contain only ASCII characters"
            )));
        }
        Ok(())
    }

    fn catalog_version(&self) -> String {
        // Mongo catalog version — matches the constant enforced by
        // MongoBootstrapper::expected_catalog_version.
        "0.0.2".to_owned()
    }

    fn is_sensitive_key(&self, key: &str) -> bool {
        let lower = key.to_lowercase();
        [
            "connection_string",
            "password",
            "secret",
            "token",
            "encryption_key",
        ]
        .iter()
        .any(|pattern| lower.contains(pattern))
    }
}
