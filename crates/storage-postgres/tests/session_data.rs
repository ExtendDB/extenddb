// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Same-name sessions of one role each read their own session data by their
//! access key (`extenddb_storage::session_data_conformance`), on a catalog
//! that has run every migration. The catalog is one of the two scratch
//! databases of a `migrations_support::Scratch`, which drops both when the
//! test ends, also after a failed check. See `migrations_support` for the
//! server this test needs.

mod migrations_support;

use std::sync::Arc;

use extenddb_storage::migrations::MigrationId;
use extenddb_storage::secret_keys::SecretKeys;
use extenddb_storage::session_data_conformance::{Expectations, run};
use extenddb_storage_postgres::PostgresCatalogStore;
use migrations_support::{ACCOUNT, Scratch, base_conn, skip};

#[tokio::test]
async fn a_session_reads_its_own_data_by_its_access_key() {
    let Some(base) = base_conn() else {
        return skip("session data");
    };
    let s = Scratch::new(&base, "pgm_session_data").await;
    s.migrate_through(MigrationId::LATEST).await;
    let catalog = s.catalog().await;
    // The store writes no session without a key holder.
    let key =
        extenddb_storage::bootstrapper::helpers::generate_encryption_key().expect("a catalog key");
    let keys = SecretKeys::catalog_key(&key).expect("key holder");
    let store = PostgresCatalogStore::with_secret_keys(catalog.clone(), Some(Arc::new(keys)));
    run(&store, ACCOUNT, Expectations::default()).await;
    catalog.close().await;
}
