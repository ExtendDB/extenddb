// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The outcome the resume bound's ERROR line names is the one the store
//! records: `finish` ends a `CANCELLING` row `CANCELLED`.

use super::super::resume::outcome_after_the_bound;

#[test]
fn the_bound_names_the_outcome_the_store_records() {
    assert_eq!(outcome_after_the_bound("CANCELLING"), "CANCELLED");
    assert_eq!(outcome_after_the_bound("IN_PROGRESS"), "FAILED");
}
