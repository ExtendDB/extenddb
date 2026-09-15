// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Utility functions for storage backends.
//!
//! Support generic functionality like ARN handling, key handling, item serialization, and
//! id validation.

mod arn;
mod key;

pub use arn::{index_arn, parse_stream_arn, stream_arn, table_arn};
pub use key::SortKeyValue;
pub use key::{
    composite_pk_to_text, effective_attribute_definitions, encode_netstring_composite,
    merge_attribute_definitions, parse_sk, pk_to_text, recover_sort_key_definitions, sk_column,
    sk_column_n, sk_info,
};
