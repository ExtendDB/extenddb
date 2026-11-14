// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers and module wiring for the parser's unit tests.

use super::*;
use crate::partiql::ast::{Operand, Returning, TypeName, WhereExpr};
use crate::types::AttributeValue;

fn err(sql: &str) -> String {
    parse(sql).unwrap_err().message
}

const WF: &str = "Statement wasn't well formed, can't be processed: ";

fn wf(reason: &str) -> String {
    format!("{WF}{reason}")
}

fn where_of(sql: &str) -> WhereExpr {
    match parse(sql).unwrap() {
        Statement::Select(s) => s.where_clause.unwrap(),
        other => panic!("expected select, got {other:?}"),
    }
}

mod brackets;
mod conditions;
mod statements;
mod values;
