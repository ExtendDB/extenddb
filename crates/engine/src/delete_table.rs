// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{DeleteTableInput, DeleteTableOutput};
use extenddb_core::validation::validate_table_name;

use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;
use crate::serialize_output;

pub async fn handle_delete_table(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: DeleteTableInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    validate_table_name(&input.table_name, &ctx.limits)?;

    let table_name = input.table_name.clone();
    // Drop the cached TableKeyInfo before the storage call as well as after
    // it. The storage call can outlive the request: MongoDB finishes a delete
    // in a task of its own when the caller gives up, and the invalidation
    // after the call then never runs. A write with an entry cached before the
    // delete would create the dropped data collection again.
    ctx.auth_cache
        .invalidate_table_key_info(&ctx.account_id, &table_name)
        .await;
    let mut table_desc = ctx
        .storage
        .delete_table(&ctx.account_id, input)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Drop the cached TableKeyInfo so subsequent requests see the deletion
    // (or get a fresh negative-cache entry) immediately.
    ctx.auth_cache
        .invalidate_table_key_info(&ctx.account_id, &table_name)
        .await;

    // The deleted table's tags rows are gone; if a table with the same name
    // is recreated, ABAC must not see the prior tag map.
    let arn = format!(
        "arn:aws:dynamodb:{}:{}:table/{}",
        ctx.region, ctx.account_id, table_name
    );
    ctx.auth_cache.invalidate_resource_tags(&arn).await;

    table_desc.prepare_for_response();
    let output = DeleteTableOutput {
        table_description: table_desc,
    };
    serialize_output(&output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_storage::error::StorageError;

    /// A second DeleteTable during the DELETING window: the measured sentence
    /// (2026-10-24), byte for byte. The other non-ACTIVE statuses keep theirs.
    #[test]
    fn a_deleting_table_has_its_own_sentence() {
        match storage_err_to_dynamo(StorageError::TableBeingDeleted("t".to_owned())) {
            DynamoDbError::ResourceInUseException(msg) => assert_eq!(
                msg,
                "Attempt to change a resource which is still in use: Table is being deleted: t"
            ),
            other => panic!("expected ResourceInUseException, got {other:?}"),
        }
        match storage_err_to_dynamo(StorageError::TableNotActive("t".to_owned())) {
            DynamoDbError::ResourceInUseException(msg) => {
                assert_eq!(msg, "Table is being created, updated, or deleted: t");
            }
            other => panic!("expected ResourceInUseException, got {other:?}"),
        }
    }

    /// Records the key-info invalidations a handler asks for.
    #[derive(Default)]
    struct Invalidations(std::sync::Mutex<Vec<(String, String)>>);

    impl extenddb_auth::TableKeyInfoCacheInvalidator for Invalidations {
        fn invalidate<'a>(
            &'a self,
            account_id: &'a str,
            table_name: &'a str,
        ) -> futures::future::BoxFuture<'a, ()> {
            self.0
                .lock()
                .unwrap()
                .push((account_id.to_owned(), table_name.to_owned()));
            Box::pin(async {})
        }

        fn invalidate_all(&self) {}
    }

    /// The table's key-info entry is dropped before the storage call, so a
    /// DeleteTable that does not get back to the handler (a caller that gives
    /// up while MongoDB finishes the delete, or a call that fails, as here:
    /// the test context's catalog has no tables) leaves no entry cached from
    /// before it.
    #[tokio::test]
    async fn the_key_info_entry_is_dropped_before_the_storage_call() {
        let invalidations = std::sync::Arc::new(Invalidations::default());
        let mut ctx = crate::partiql::test_context::context(None).await;
        ctx.auth_cache = extenddb_auth::AuthCacheRegistry::default()
            .with_table_key_info_invalidator(invalidations.clone());

        let err = handle_delete_table(serde_json::json!({"TableName": "tbl"}), &ctx)
            .await
            .expect_err("a catalog without tables fails the storage call");
        match err {
            DynamoDbError::InternalServerError(msg) => assert_eq!(msg, "Internal server error"),
            other => panic!("expected InternalServerError, got {other:?}"),
        }
        assert_eq!(
            *invalidations.0.lock().unwrap(),
            [("000000000000".to_owned(), "tbl".to_owned())]
        );
    }
}
