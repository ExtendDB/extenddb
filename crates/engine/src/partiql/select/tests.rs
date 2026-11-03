// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the SELECT executor, and the fixtures the tests of its modules share.

use super::*;
use extenddb_core::partiql::ast::Statement;
use extenddb_core::partiql::parse;

pub(super) fn select(sql: &str) -> SelectStatement {
    match parse(sql).unwrap() {
        Statement::Select(s) => s,
        _ => panic!("not a select"),
    }
}

pub(super) const KEYS: KeyNames<'static> = KeyNames {
    partition: "pk",
    sort: Some("sk"),
};

pub(super) const HASH_ONLY: KeyNames<'static> = KeyNames {
    partition: "pk",
    sort: None,
};
