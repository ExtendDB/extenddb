// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL grammar, AST and query planning.
//!
//! This module is compiled only with the `partiql` feature. It is a pure
//! front end: it turns a statement string into an [`ast::Statement`] and then
//! (in [`plan`]) into a plan expressed in terms of the operations the engine
//! already has. It introduces no storage concept and nothing else in `core`
//! depends on it, so the feature can be removed by deleting this directory.
//!
//! Every grammar rule and every error string is justified by behaviour measured
//! against the live service; see the project's PartiQL behavioural spec.

pub mod ast;
pub mod lexer;
pub mod parser;
pub mod plan;

pub use ast::Statement;
pub use parser::{ParseError, parse};

/// The prefix the service puts on a PartiQL parse failure. The full
/// `ValidationException` message is this prefix followed by a specific reason.
pub const PARSE_ERROR_PREFIX: &str = "Statement wasn't well formed, can't be processed: ";

/// Build the service's parse-error message from a reason.
#[must_use]
pub fn parse_error(reason: &str) -> String {
    format!("{PARSE_ERROR_PREFIX}{reason}")
}
