// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query execution helpers to reduce code repetition.
//!
//! Every helper reads at
//! [`read_consistency`](crate::cassandra_util::read_consistency)`(consistent_read)`.

use crate::cassandra_util;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::rows::Row;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::SortKeyValue;
use std::sync::Arc;

/// Execute query with PK and single SK parameter.
pub(super) async fn query_with_pk_sk(
    session: &Arc<crate::cassandra_util::CassandraSession>,
    query: &str,
    pk: &str,
    sk: &SortKeyValue,
    consistent_read: bool,
    label: &str,
) -> Result<Vec<Row>, StorageError> {
    match sk {
        SortKeyValue::S(s) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, s.as_str()),
                consistent_read,
                label,
            )
            .await
        }
        SortKeyValue::N(n) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, super::decimal_to_value(n)),
                consistent_read,
                label,
            )
            .await
        }
        SortKeyValue::B(b) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, Blob::new(b.clone())),
                consistent_read,
                label,
            )
            .await
        }
    }
}

/// Execute query with PK and two SK parameters.
pub(super) async fn query_with_pk_sk_sk(
    session: &Arc<crate::cassandra_util::CassandraSession>,
    query: &str,
    pk: &str,
    sk1: &SortKeyValue,
    sk2: &SortKeyValue,
    consistent_read: bool,
    label: &str,
) -> Result<Vec<Row>, StorageError> {
    match (sk1, sk2) {
        (SortKeyValue::S(s1), SortKeyValue::S(s2)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, s1.as_str(), s2.as_str()),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::N(n1), SortKeyValue::N(n2)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(
                    pk,
                    super::decimal_to_value(n1),
                    super::decimal_to_value(n2)
                ),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::B(b1), SortKeyValue::B(b2)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, Blob::new(b1.clone()), Blob::new(b2.clone())),
                consistent_read,
                label,
            )
            .await
        }
        _ => Err(StorageError::Internal(
            "Type mismatch in query parameters".to_owned(),
        )),
    }
}

/// Execute query with index PK, index SK, base PK, and base SK parameters.
/// Used for LSI pagination: `WHERE pk=? AND sk_*=? AND base_pk=? AND base_sk_*>?`
#[allow(clippy::too_many_arguments)]
pub(super) async fn query_with_pk_sk_pk_sk(
    session: &Arc<crate::cassandra_util::CassandraSession>,
    query: &str,
    pk: &str,
    idx_sk: &SortKeyValue,
    base_pk: &str,
    base_sk: &SortKeyValue,
    consistent_read: bool,
    label: &str,
) -> Result<Vec<Row>, StorageError> {
    use cdrs_tokio::query_values;
    match (idx_sk, base_sk) {
        (SortKeyValue::S(isk), SortKeyValue::S(bsk)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                query_values!(pk, isk.as_str(), base_pk, bsk.as_str()),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::S(isk), SortKeyValue::N(bsk)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                query_values!(pk, isk.as_str(), base_pk, super::decimal_to_value(bsk)),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::S(isk), SortKeyValue::B(bsk)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                query_values!(pk, isk.as_str(), base_pk, Blob::new(bsk.clone())),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::N(isk), SortKeyValue::S(bsk)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                query_values!(pk, super::decimal_to_value(isk), base_pk, bsk.as_str()),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::N(isk), SortKeyValue::N(bsk)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                query_values!(
                    pk,
                    super::decimal_to_value(isk),
                    base_pk,
                    super::decimal_to_value(bsk)
                ),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::N(isk), SortKeyValue::B(bsk)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                query_values!(
                    pk,
                    super::decimal_to_value(isk),
                    base_pk,
                    Blob::new(bsk.clone())
                ),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::B(isk), SortKeyValue::S(bsk)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                query_values!(pk, Blob::new(isk.clone()), base_pk, bsk.as_str()),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::B(isk), SortKeyValue::N(bsk)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                query_values!(
                    pk,
                    Blob::new(isk.clone()),
                    base_pk,
                    super::decimal_to_value(bsk)
                ),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::B(isk), SortKeyValue::B(bsk)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                query_values!(pk, Blob::new(isk.clone()), base_pk, Blob::new(bsk.clone())),
                consistent_read,
                label,
            )
            .await
        }
    }
}

/// Execute query with index PK, index SK, and base PK parameters.
/// Used for GSI pagination: `WHERE pk=? AND sk_*=? AND base_pk>?`
pub(super) async fn query_with_pk_sk_pk(
    session: &Arc<crate::cassandra_util::CassandraSession>,
    query: &str,
    pk: &str,
    sk: &SortKeyValue,
    base_pk: &str,
    consistent_read: bool,
    label: &str,
) -> Result<Vec<Row>, StorageError> {
    match sk {
        SortKeyValue::S(s) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, s.as_str(), base_pk),
                consistent_read,
                label,
            )
            .await
        }
        SortKeyValue::N(n) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, super::decimal_to_value(n), base_pk),
                consistent_read,
                label,
            )
            .await
        }
        SortKeyValue::B(b) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, Blob::new(b.clone()), base_pk),
                consistent_read,
                label,
            )
            .await
        }
    }
}

/// Execute query with index PK, base table pk, and an SK parameter.
pub(super) async fn query_with_pk_pk_sk(
    session: &Arc<crate::cassandra_util::CassandraSession>,
    query: &str,
    pk: &str,
    base_pk: &str,
    sk: &SortKeyValue,
    consistent_read: bool,
    label: &str,
) -> Result<Vec<Row>, StorageError> {
    match sk {
        SortKeyValue::S(s) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, base_pk, s.as_str()),
                consistent_read,
                label,
            )
            .await
        }
        SortKeyValue::N(n) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, base_pk, super::decimal_to_value(n)),
                consistent_read,
                label,
            )
            .await
        }
        SortKeyValue::B(b) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, base_pk, Blob::new(b.clone())),
                consistent_read,
                label,
            )
            .await
        }
    }
}

/// Execute query with PK and three SK parameters.
#[allow(clippy::too_many_arguments)]
pub(super) async fn query_with_pk_sk_sk_sk(
    session: &Arc<crate::cassandra_util::CassandraSession>,
    query: &str,
    pk: &str,
    sk1: &SortKeyValue,
    sk2: &SortKeyValue,
    sk3: &SortKeyValue,
    consistent_read: bool,
    label: &str,
) -> Result<Vec<Row>, StorageError> {
    match (sk1, sk2, sk3) {
        (SortKeyValue::S(s1), SortKeyValue::S(s2), SortKeyValue::S(s3)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(pk, s1.as_str(), s2.as_str(), s3.as_str()),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::N(n1), SortKeyValue::N(n2), SortKeyValue::N(n3)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(
                    pk,
                    super::decimal_to_value(n1),
                    super::decimal_to_value(n2),
                    super::decimal_to_value(n3)
                ),
                consistent_read,
                label,
            )
            .await
        }
        (SortKeyValue::B(b1), SortKeyValue::B(b2), SortKeyValue::B(b3)) => {
            cassandra_util::query_rows_at(
                session,
                query,
                cdrs_tokio::query_values!(
                    pk,
                    Blob::new(b1.clone()),
                    Blob::new(b2.clone()),
                    Blob::new(b3.clone())
                ),
                consistent_read,
                label,
            )
            .await
        }
        _ => Err(StorageError::Internal(
            "Type mismatch in query parameters".to_owned(),
        )),
    }
}
