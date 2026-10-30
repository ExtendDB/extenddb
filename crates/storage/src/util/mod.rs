// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Utility functions for storage backends.
//!
//! Support generic functionality like ARN handling, key handling, item serialization, and
//! id validation.

mod arn;
mod backup_id;
mod catalog_version;
mod key;
mod prefix;
mod shard_stream;

pub use arn::{
    ARN_PARTITION, STREAM_DOES_NOT_EXIST, format_stream_label, index_arn, new_stream_label,
    parse_stream_arn, shard_not_in_stream, stream_arn, stream_does_not_exist, stream_not_found,
    table_arn,
};
pub use backup_id::{
    BackupArnError, BackupArnParts, MAX_BACKUP_ARN_LEN, backup_arn, backup_creation_date_time,
    backup_id_epoch_millis, check_backup_arn, format_backup_id, is_backup_id,
    is_backup_resource_arn, new_backup_id, parse_backup_arn,
};
pub use catalog_version::check_stored_catalog_version;
pub use key::SortKeyValue;
pub use key::{
    composite_pk_to_text, effective_attribute_definitions, encode_netstring_composite,
    merge_attribute_definitions, parse_sk, pk_to_text, recover_sort_key_definitions, sk_column,
    sk_column_n, sk_info,
};
pub use prefix::string_prefix_upper_bound;
pub use shard_stream::shard_in_stream;

/// Whether an UpdateTable's GSI actions must be refused because the GSIs named
/// in `building` are `CREATING`: any Create, or a Delete of a GSI that is not
/// one of them. The service answers both with LimitExceededException ("Only 1
/// online index can be created or deleted simultaneously per table").
#[must_use]
pub fn building_refuses(
    building: &[String],
    updates: &[extenddb_core::types::GlobalSecondaryIndexUpdate],
) -> bool {
    if building.is_empty() {
        return false;
    }
    updates.iter().any(|u| {
        u.create.is_some()
            || u.delete
                .as_ref()
                .is_some_and(|d| !building.contains(&d.index_name))
    })
}

#[cfg(test)]
mod building_refuses_tests {
    use super::building_refuses;
    use extenddb_core::types::GlobalSecondaryIndexUpdate;

    fn updates(json: serde_json::Value) -> Vec<GlobalSecondaryIndexUpdate> {
        serde_json::from_value(json).expect("updates")
    }

    #[test]
    fn nothing_building_refuses_nothing() {
        let u = updates(serde_json::json!([{"Delete": {"IndexName": "a"}}]));
        assert!(!building_refuses(&[], &u));
    }

    #[test]
    fn a_create_or_a_delete_of_another_index_is_refused() {
        let building = vec!["b".to_owned()];
        let create = updates(serde_json::json!([{"Create": {
            "IndexName": "c",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }}]));
        assert!(building_refuses(&building, &create));
        let other = updates(serde_json::json!([{"Delete": {"IndexName": "a"}}]));
        assert!(building_refuses(&building, &other));
    }

    #[test]
    fn a_delete_of_the_building_index_is_not_refused_here() {
        let building = vec!["b".to_owned()];
        let own = updates(serde_json::json!([{"Delete": {"IndexName": "b"}}]));
        assert!(!building_refuses(&building, &own));
    }
}
