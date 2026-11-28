// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The storage engine and the [`OperationContext`] that the engine's unit
//! tests build: an empty in-memory SQLite database, and a context on a given
//! engine with no item authorizer and no authorized keys.

use std::sync::Arc;

use extenddb_core::limits::LimitsConfig;
use extenddb_storage::StorageEngine;
use extenddb_storage_sqlite::SqliteEngine;

use crate::{AuthorizedKeyInfo, BackupRuntime, OperationContext};

/// A storage engine on an empty in-memory SQLite database.
pub(crate) async fn memory_engine() -> Arc<dyn StorageEngine> {
    Arc::new(
        SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("an in-memory database opens"),
    )
}

/// A context on `storage` for `account_id` in us-east-1, with the default
/// limits, no item authorizer and no authorized keys.
pub(crate) fn context_on(storage: Arc<dyn StorageEngine>, account_id: &str) -> OperationContext {
    OperationContext {
        storage,
        backup: Arc::new(BackupRuntime::new(1)),
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from("us-east-1"),
        account_id: Arc::from(account_id),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(Vec::new()),
        authorized_key_info: AuthorizedKeyInfo::new(),
        auth_cache: extenddb_auth::AuthCacheRegistry::default(),
        table_key_info_lookup: None,
        item_authorizer: None,
    }
}
