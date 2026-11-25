// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The tag path while a table is `DELETING`, on the window the parent module
//! opens. The handlers read the table's status from its description, because
//! the key-info read on this backend reads a `DELETING` row as found.

use extenddb_core::error::DynamoDbError;
use extenddb_storage::MetadataEngine;
use serde_json::json;

use super::ttl::engine_context;
use super::{ACCOUNT, TABLE, base_conn, open_window, scratch, skip};

/// A failed status read answers a tag write as a fault, as the TTL handlers
/// answer it, and writes no tag. The key-info read after it would read the
/// `DELETING` row as found and accept the write. The description reads
/// `table_restores` and the key-info read does not, so the read fails here
/// once that table is renamed.
#[tokio::test]
async fn a_failed_status_read_refuses_a_tag_write() {
    if base_conn().is_none() {
        return skip("a_failed_status_read_refuses_a_tag_write");
    }
    let s = scratch().await;
    open_window(&s).await;
    sqlx::query("ALTER TABLE table_restores RENAME TO table_restores_hidden")
        .execute(&s.catalog)
        .await
        .expect("rename table_restores");

    let ctx = engine_context(&s);
    let arn = format!("arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/{TABLE}");
    let tagged = extenddb_engine::handle_tag_resource(
        json!({"ResourceArn": arn, "Tags": [{"Key": "k", "Value": "v"}]}),
        &ctx,
    )
    .await;
    assert!(
        matches!(&tagged, Err(DynamoDbError::InternalServerError(m)) if m == "Internal server error"),
        "TagResource after a failed status read: {tagged:?}"
    );
    drop(ctx);

    sqlx::query("ALTER TABLE table_restores_hidden RENAME TO table_restores")
        .execute(&s.catalog)
        .await
        .expect("rename table_restores back");
    assert_eq!(
        s.engine.list_tags(&arn).await.expect("read the tag rows"),
        Vec::new(),
        "no tag row was written"
    );

    s.cleanup().await;
}
