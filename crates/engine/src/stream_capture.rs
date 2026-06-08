// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stream record capture helpers for write operations.
//!
//! When a table has streams enabled, write operations (`PutItem`, `DeleteItem`,
//! `UpdateItem`) generate stream records. This module provides the
//! `stream_view_type` helper to check whether a table has streams enabled
//! and determine the view type.

use extenddb_core::types::{StreamViewType, TableKeyInfo};

/// Return the stream view type if the table has streams enabled.
///
/// Reads the stream specification from the cached `TableKeyInfo` — no extra
/// SQL round-trip required.
#[must_use]
pub fn stream_view_type(key_info: &TableKeyInfo) -> Option<StreamViewType> {
    let spec = key_info.stream_specification.as_ref()?;
    if !spec.stream_enabled {
        return None;
    }
    spec.stream_view_type
}
