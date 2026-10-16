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

pub use arn::{
    format_stream_label, index_arn, new_stream_label, parse_stream_arn, stream_arn, table_arn,
};
pub use backup_id::{
    BackupArnParts, backup_arn, backup_creation_date_time, backup_id_epoch_millis,
    format_backup_id, new_backup_id, parse_backup_arn,
};
pub use catalog_version::check_stored_catalog_version;
pub use key::SortKeyValue;
pub use key::{
    composite_pk_to_text, effective_attribute_definitions, encode_netstring_composite,
    merge_attribute_definitions, parse_sk, pk_to_text, recover_sort_key_definitions, sk_column,
    sk_column_n, sk_info,
};
