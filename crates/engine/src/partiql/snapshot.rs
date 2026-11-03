// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog snapshot the members of a batch or a transaction run against.

use std::future::Future;
use std::sync::Arc;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::TableKeyInfo;
use extenddb_storage::error::StorageError;

use crate::create_table::storage_err_to_dynamo;
use crate::{AuthorizedKeyInfo, OperationContext};

/// `ctx` with `authorized_key_info` as its catalog snapshot.
pub(super) fn context_with_key_info(
    ctx: &OperationContext,
    authorized_key_info: AuthorizedKeyInfo,
) -> OperationContext {
    OperationContext {
        storage: Arc::clone(&ctx.storage),
        limits: Arc::clone(&ctx.limits),
        region: Arc::clone(&ctx.region),
        account_id: Arc::clone(&ctx.account_id),
        import_paths: Arc::clone(&ctx.import_paths),
        export_paths: Arc::clone(&ctx.export_paths),
        authorized_key_info,
        auth_cache: ctx.auth_cache.clone(),
        table_key_info_lookup: ctx.table_key_info_lookup.clone(),
    }
}

/// The context the members of a batch or a transaction run in: `ctx` with
/// every table of `tables` read once and added to its catalog snapshot.
/// Every later lookup of those tables, in the PartiQL layer and in the
/// classic requests it builds, then resolves from that one read, as the
/// catalog rule on [`OperationContext`] asks. A table is read as the classic
/// batch and transaction handlers read it, with `table_key_info_uncached`,
/// and a missing one stays missing for the rest of the request.
///
/// # Errors
///
/// Returns a failed catalog read other than a missing table, mapped as the
/// classic handlers map it.
pub(super) async fn snapshot_context(
    ctx: &OperationContext,
    tables: &[&str],
) -> Result<OperationContext, DynamoDbError> {
    let snapshot = resolve_tables(&ctx.authorized_key_info, tables, |table| {
        ctx.table_key_info_uncached(table)
    })
    .await?;
    Ok(context_with_key_info(ctx, snapshot))
}

/// `known` with every table of `tables` it does not hold read once with
/// `read`: its key info, or `None` when the table is missing.
async fn resolve_tables<'t, F, R>(
    known: &AuthorizedKeyInfo,
    tables: &[&'t str],
    mut read: F,
) -> Result<AuthorizedKeyInfo, DynamoDbError>
where
    F: FnMut(&'t str) -> R,
    R: Future<Output = Result<TableKeyInfo, StorageError>>,
{
    let mut snapshot = known.clone();
    for &table in tables {
        if snapshot.contains_key(table) {
            continue;
        }
        let entry = match read(table).await {
            Ok(info) => Some(info),
            Err(StorageError::TableNotFound(_)) => None,
            Err(e) => return Err(storage_err_to_dynamo(e)),
        };
        snapshot.insert(table.to_owned(), entry);
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    fn info(table: &str) -> TableKeyInfo {
        TableKeyInfo {
            table_name: table.to_owned(),
            ..TableKeyInfo::default()
        }
    }

    #[test]
    fn each_table_is_read_once_and_a_missing_one_is_recorded() {
        let reads = RefCell::new(Vec::new());
        let mut known = AuthorizedKeyInfo::new();
        known.insert("authorized".to_owned(), Some(info("authorized")));
        let tables = ["a", "b", "a", "missing", "authorized", "b", "missing"];
        let snapshot = block_on(resolve_tables(&known, &tables, |table| {
            reads.borrow_mut().push(table.to_owned());
            async move {
                match table {
                    "missing" => Err(StorageError::TableNotFound(table.to_owned())),
                    _ => Ok(info(table)),
                }
            }
        }))
        .unwrap();
        assert_eq!(*reads.borrow(), ["a", "b", "missing"]);
        assert_eq!(snapshot.len(), 4);
        assert_eq!(snapshot["a"].as_ref().unwrap().table_name, "a");
        assert_eq!(
            snapshot["authorized"].as_ref().unwrap().table_name,
            "authorized"
        );
        assert!(snapshot["missing"].is_none());
    }

    #[test]
    fn a_failed_read_fails_the_request() {
        // A lost connection is a retryable fault, not a missing table.
        let result = block_on(resolve_tables(
            &AuthorizedKeyInfo::new(),
            &["a"],
            |_| async { Err(StorageError::Connection("connection lost".to_owned())) },
        ));
        assert!(matches!(result, Err(DynamoDbError::ServiceUnavailable(_))));
    }
}
