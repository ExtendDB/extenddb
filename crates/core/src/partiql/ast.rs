// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL abstract syntax tree.
//!
//! One `Statement` per `ExecuteStatement`. The grammar and every node here is
//! justified by behaviour measured against the live service; see the project's
//! PartiQL behavioural spec. The planner (`super::plan`) turns a `Statement`
//! into a plan expressed in terms of the operations the engine already has, so
//! nothing here introduces a new storage concept.

use crate::types::AttributeValue;

/// A parsed PartiQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

impl Statement {
    /// Whether the statement reads (`SELECT`) or writes. Batch and transaction
    /// requests must be all-read or all-write, so this classifies each member.
    #[must_use]
    pub fn is_read(&self) -> bool {
        matches!(self, Statement::Select(_))
    }
}

/// `SELECT <projection> FROM <source> [WHERE <expr>] [ORDER BY <attr> ASC|DESC]`.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub projection: Projection,
    pub from: FromSource,
    pub where_clause: Option<WhereExpr>,
    pub order_by: Option<OrderBy>,
}

/// `INSERT INTO <table> VALUE { ... }`.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub value: Vec<(String, Operand)>,
}

/// `UPDATE <table> SET ... REMOVE ... WHERE <full key> [AND <condition>] [RETURNING <mode>]`.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub sets: Vec<SetAction>,
    pub removes: Vec<Path>,
    pub where_clause: WhereExpr,
    pub returning: Option<Returning>,
}

/// `DELETE FROM <table> WHERE <full key> [AND <condition>] [RETURNING ALL OLD *]`.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: WhereExpr,
    pub returning: Option<Returning>,
}

/// The attributes a `SELECT` returns.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// `SELECT *`.
    All,
    /// `SELECT a, b.c, d[0]`. The projected key naming follows the service:
    /// a plain or nested attribute keys on its leaf, a list-index path keys on
    /// the literal path text.
    Paths(Vec<Path>),
}

/// A `FROM` target: a table, optionally an index segment (`"table"."index"`).
#[derive(Debug, Clone, PartialEq)]
pub struct FromSource {
    pub table: String,
    pub index: Option<String>,
}

/// A document path: a first attribute then zero or more attribute or index steps.
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    pub root: String,
    pub steps: Vec<PathStep>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathStep {
    Attribute(String),
    Index(usize),
}

impl Path {
    /// The single attribute name when the path has no steps, else `None`.
    #[must_use]
    pub fn as_simple(&self) -> Option<&str> {
        self.steps.is_empty().then_some(self.root.as_str())
    }
}

/// A `SET` target and its value expression.
#[derive(Debug, Clone, PartialEq)]
pub struct SetAction {
    pub path: Path,
    pub value: Operand,
}

/// An operand in a value or condition position.
#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    /// A literal attribute value (`'s'`, `1`, `true`, `null`, `{...}`, `[...]`, `<<...>>`).
    Literal(AttributeValue),
    /// A positional `?` parameter, resolved from the request `Parameters` by index.
    Parameter(usize),
    /// A document path (an attribute reference).
    Path(Path),
    /// `left + right` or `left - right` (numbers only).
    Arith(Box<Operand>, ArithOp, Box<Operand>),
    /// A function call: `if_not_exists(path, operand)`, `list_append(a, b)`, etc.
    Function(String, Vec<Operand>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
}

/// The `WHERE` expression: a boolean tree over comparisons and functions. Used
/// both for `SELECT` planning and as the condition on a write (where any term
/// beyond the key equalities becomes a condition expression on the item).
#[derive(Debug, Clone, PartialEq)]
pub enum WhereExpr {
    Compare(Operand, CompareOp, Operand),
    Between(Operand, Operand, Operand),
    In(Operand, Vec<Operand>),
    BeginsWith(Operand, Operand),
    Contains(Operand, Operand),
    AttributeType(Operand, Operand),
    Function(String, Vec<Operand>),
    IsMissing(Path, bool),
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
    Not(Box<WhereExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub path: Path,
    pub descending: bool,
}

/// The `RETURNING` clause of a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Returning {
    AllOld,
    ModifiedOld,
    AllNew,
    ModifiedNew,
}
