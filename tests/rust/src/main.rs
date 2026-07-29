// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Rust integration tests for extenddb — mirrors the external Java test suite.
//!
//! Run via `cargo test` from `tests/rust/`.
//! Requires a running extenddb instance with `EXTENDDB_TEST_ENDPOINT` and credentials set.

mod helpers;
mod test_base;

#[cfg(test)]
mod authorization;
#[cfg(test)]
mod backup_arn_scoping;
#[cfg(test)]
mod backup_restore;
#[cfg(test)]
mod batch_get_item;
#[cfg(test)]
mod batch_transact_authz;
#[cfg(test)]
mod batch_write_item;
#[cfg(test)]
mod binary_sort_key;
#[cfg(test)]
mod capacity_throttling;
#[cfg(test)]
mod composite_keys;
#[cfg(test)]
mod concurrency;
#[cfg(test)]
mod condition_expressions;
#[cfg(test)]
mod condition_expressions_more;
#[cfg(test)]
mod consumed_capacity_shape;
#[cfg(test)]
mod create_table_validation;
#[cfg(test)]
mod data_types;
#[cfg(test)]
mod delete_item;
#[cfg(test)]
mod delete_item_more;
#[cfg(test)]
mod empty_values;
#[cfg(test)]
mod error_handling;
#[cfg(test)]
mod get_item;
#[cfg(test)]
mod gsi;
#[cfg(test)]
mod gsi_more;
#[cfg(test)]
mod gsi_on_hash_only_table;
#[cfg(test)]
mod gsi_update_table_attr_defs;
#[cfg(test)]
mod index_key_validation;
#[cfg(test)]
mod lsi;
#[cfg(test)]
mod misc_control_plane;
#[cfg(test)]
mod number_validation;
#[cfg(test)]
mod put_item;
#[cfg(test)]
mod put_item_edge;
#[cfg(test)]
mod query;
#[cfg(test)]
mod query_more;
#[cfg(test)]
mod raw_http;
#[cfg(test)]
mod restore_active_completeness;
#[cfg(test)]
mod scan;
#[cfg(test)]
mod select_projection_validation;
#[cfg(test)]
mod table_operations;
#[cfg(test)]
mod table_operations_more;
#[cfg(test)]
mod tagging;
#[cfg(test)]
mod transact_get_items;
#[cfg(test)]
mod transact_key_validation;
#[cfg(test)]
mod transact_write_items;
#[cfg(test)]
mod transact_write_items_more;
#[cfg(test)]
mod transaction_key_size_validation;
#[cfg(test)]
mod transaction_validation;
#[cfg(test)]
mod ttl;
#[cfg(test)]
mod unicode;
#[cfg(test)]
mod update_expressions;
#[cfg(test)]
mod update_expressions_more;
#[cfg(test)]
mod update_item;
#[cfg(test)]
mod update_item_edge;
#[cfg(test)]
mod update_item_more;
#[cfg(test)]
mod update_item_number_validation;
#[cfg(test)]
mod update_table_billing_validation;
#[cfg(test)]
mod wording_parity_validation;

fn main() {
    eprintln!("Run with `cargo test` to execute integration tests.");
}
