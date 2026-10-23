// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct storage-trait integration tests, ported from the original
//! extenddb-cassandra-plugin repository (its tests/rust suite). They exercise
//! the Cassandra engine's trait implementations directly against a live node
//! at 127.0.0.1:9042 — no HTTP server in between — and skip themselves when
//! no Cassandra is reachable (see helpers::skip_without_cassandra).
//!
//! One binary, one module per area, sharing tests/common/mod.rs, which is the
//! in-tree descendant of the plug-in repo's helpers.rs.

#[path = "common/mod.rs"]
mod helpers;

#[path = "direct/access_keys.rs"]
mod access_keys;
#[path = "direct/accounts.rs"]
mod accounts;
#[path = "direct/admin_store.rs"]
mod admin_store;
#[path = "direct/authorization_store.rs"]
mod authorization_store;
#[path = "direct/backup_engine.rs"]
mod backup_engine;
#[path = "direct/cassandra_engine.rs"]
mod cassandra_engine;
#[path = "direct/delete_item.rs"]
mod delete_item;
#[path = "direct/groups.rs"]
mod groups;
#[path = "direct/index.rs"]
mod index;
#[path = "direct/metadata_engine.rs"]
mod metadata_engine;
#[path = "direct/policies.rs"]
mod policies;
#[path = "direct/put_get_item.rs"]
mod put_get_item;
#[path = "direct/query.rs"]
mod query;
#[path = "direct/roles.rs"]
mod roles;
#[path = "direct/scan.rs"]
mod scan;
#[path = "direct/settings_store.rs"]
mod settings_store;
#[path = "direct/streams.rs"]
mod streams;
#[path = "direct/table_engine.rs"]
mod table_engine;
#[path = "direct/transact_get_items.rs"]
mod transact_get_items;
#[path = "direct/transact_write_items.rs"]
mod transact_write_items;
#[path = "direct/transaction_ledger.rs"]
mod transaction_ledger;
#[path = "direct/users.rs"]
mod users;
