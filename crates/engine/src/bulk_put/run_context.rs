// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The run context the Start handler records on an operation for its
//! runner, in the record's `run_context` column: the kind of store the
//! operation started on and the caller it acts for. No wire response
//! renders it.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use extenddb_storage::BulkOperationRecord;

use crate::bulk_put::authorize::BulkPrincipal;
use crate::bulk_put::store::BulkStore;

/// The store kinds, spelled as the `[bulk] store` setting names them.
pub(crate) const STORE_FILESYSTEM: &str = "filesystem";
pub(crate) const STORE_S3: &str = "s3";

/// The kind of `store`: a store with a directory tree is the filesystem
/// store; every other store reaches S3 or an S3-compatible endpoint.
pub(crate) fn store_kind(store: &BulkStore) -> &'static str {
    if store.filesystem_root().is_some() {
        STORE_FILESYSTEM
    } else {
        STORE_S3
    }
}

/// The stored shape of the column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecordedRunContext {
    /// The kind of store the operation started on.
    pub(crate) store: String,
    /// The caller, as the server captured it at Start; `None` in developer
    /// mode and on an operation started before callers were recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) principal: Option<BulkPrincipal>,
}

impl RecordedRunContext {
    /// What Start records for an operation on `store` started by
    /// `principal`.
    pub(crate) fn at_start(store: &BulkStore, principal: Option<BulkPrincipal>) -> Self {
        Self {
            store: store_kind(store).to_owned(),
            principal,
        }
    }

    /// The column value to store.
    pub(crate) fn to_value(&self) -> Value {
        // Strings, string maps and a bool cannot fail to serialize; Null
        // appears only if that invariant broke, and the runner refuses a
        // Null.
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// The run context of `record`. A record with no value in the column
    /// was started before the column existed, on the filesystem store, the
    /// only store there was. A value that does not parse is corruption and
    /// is refused with its detail.
    pub(crate) fn of(record: &BulkOperationRecord) -> Result<Self, String> {
        match &record.run_context {
            None => Ok(Self {
                store: STORE_FILESYSTEM.to_owned(),
                principal: None,
            }),
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|error| format!("stored run context does not parse: {error}")),
        }
    }
}
