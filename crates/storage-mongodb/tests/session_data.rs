// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Same-name sessions of one role each read their own session data by their
//! access key (`extenddb_storage::session_data_conformance`).
//!
//! A direct integration test: it calls the catalog store's trait
//! methods against a live replica set named by `EXTENDDB_TEST_MONGODB_URI`,
//! as `tests/iam_leftovers.rs` does. Without the URI it skips, unless
//! `EXTENDDB_TEST_MONGODB_REQUIRED` is `1`. It works in a deployment of its
//! own, created with every migration as `init` creates it, and drops it when
//! the check passes.

mod migrations_support;

use extenddb_storage::session_data_conformance::{Expectations, run};
use extenddb_storage_mongodb::MongoCatalogStore;
use migrations_support::{Deployment, server};

#[tokio::test]
async fn a_session_reads_its_own_data_by_its_access_key() {
    let Some(uri) = server() else {
        return;
    };
    let deployment = Deployment::new(&uri, "session_data").await;
    deployment.create().await;
    let store =
        MongoCatalogStore::on_database_for_tests(deployment.client.clone(), deployment.catalog());
    run(
        &store,
        "123456789012",
        Expectations {
            // The TTL index on `iam_sessions.expires_at` deletes an expired
            // session in the background, so the expired session may read
            // `None`. The check then cannot tell this read from one that
            // skips expired sessions.
            deletes_expired_sessions: true,
        },
    )
    .await;
    deployment.drop_databases().await;
}
