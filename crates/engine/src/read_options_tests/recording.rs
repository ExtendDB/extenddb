// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The storage double of the read-options tests: the backup tests' [`Fake`],
//! holding the fixture tables and indexes. It records every `get_item`,
//! `query` and `scan` with the [`ReadOptions`](extenddb_storage::ReadOptions)
//! it received, finds no item on `get_item`, and answers `query` and `scan`
//! from `read_pages` (an empty last page once those are used up).
//! `table_key_info`, `index_info` and `describe_table` (the export handler
//! describes the table before it scans it) answer from the fixtures, and
//! `update_item` fails with `update_error` when a test sets one; every other
//! method is unreachable from the handlers under test and fails as unused.

use std::sync::Arc;

use extenddb_core::types::TableDescription;

use super::fixtures::{EVENTS, ORDERS, USERS, gsi_info, lsi_info, table_info};
pub(super) use crate::backup::tests::{Fake, ReadCall};

/// A fresh double holding the three fixture tables and their two indexes.
pub(super) fn storage() -> Arc<Fake> {
    let fake = Fake::default();
    for name in [ORDERS, USERS, EVENTS] {
        fake.key_infos
            .lock()
            .unwrap()
            .insert(name.to_owned(), table_info(name));
        fake.tables.lock().unwrap().insert(
            name.to_owned(),
            TableDescription {
                table_name: name.to_owned(),
                ..TableDescription::default()
            },
        );
    }
    *fake.indexes.lock().unwrap() = vec![gsi_info(), lsi_info()];
    Arc::new(fake)
}
