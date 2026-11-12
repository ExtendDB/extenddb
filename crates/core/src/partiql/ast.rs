// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL abstract syntax tree.
//!
//! One `Statement` per `ExecuteStatement`. The grammar and every node here is
//! justified by behaviour measured against the live service, cited where it
//! is implemented. The planner (`super::plan`) turns a `Statement`
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
    /// `EXISTS(SELECT ...)`: a condition check on one item, accepted as an
    /// `ExecuteTransaction` member.
    Exists(SelectStatement),
}

impl Statement {
    /// Whether the statement reads (`SELECT`) or writes. Batch and transaction
    /// requests must be all-read or all-write, so this classifies each member.
    /// An `EXISTS` condition check counts with the writes, as a
    /// `ConditionCheck` does in `TransactWriteItems`.
    #[must_use]
    pub fn is_read(&self) -> bool {
        matches!(self, Statement::Select(_))
    }

    /// How many `?` parameters the statement reads: one past the highest
    /// index the parser gave a placeholder, since it numbers them in the
    /// order they are written.
    #[must_use]
    pub fn parameter_count(&self) -> usize {
        let mut count = 0;
        match self {
            Statement::Select(select) | Statement::Exists(select) => {
                if let Some(where_clause) = &select.where_clause {
                    where_parameters(where_clause, &mut count);
                }
            }
            Statement::Insert(insert) => {
                for (_, value) in &insert.value {
                    operand_parameters(value, &mut count);
                }
            }
            Statement::Update(update) => {
                for action in &update.sets {
                    operand_parameters(&action.value, &mut count);
                }
                for action in &update.set_members {
                    operand_parameters(&action.members, &mut count);
                }
                if let Some(where_clause) = &update.where_clause {
                    where_parameters(where_clause, &mut count);
                }
            }
            Statement::Delete(delete) => where_parameters(&delete.where_clause, &mut count),
        }
        count
    }
}

fn where_parameters(expr: &WhereExpr, count: &mut usize) {
    match expr {
        WhereExpr::Compare(left, _, right)
        | WhereExpr::BeginsWith(left, right)
        | WhereExpr::Contains(left, right)
        | WhereExpr::AttributeType(left, right) => {
            operand_parameters(left, count);
            operand_parameters(right, count);
        }
        WhereExpr::Between(value, low, high) => {
            for operand in [value, low, high] {
                operand_parameters(operand, count);
            }
        }
        WhereExpr::In(value, items) => {
            operand_parameters(value, count);
            for item in items {
                operand_parameters(item, count);
            }
        }
        WhereExpr::Function(_, args) => {
            for arg in args {
                operand_parameters(arg, count);
            }
        }
        WhereExpr::IsType(operand, ..) => operand_parameters(operand, count),
        WhereExpr::IsMissing(..) => {}
        WhereExpr::And(left, right) | WhereExpr::Or(left, right) => {
            where_parameters(left, count);
            where_parameters(right, count);
        }
        WhereExpr::Not(inner) | WhereExpr::ComparedWithBoolean(inner, _) => {
            where_parameters(inner, count);
        }
    }
}

fn operand_parameters(operand: &Operand, count: &mut usize) {
    match operand {
        Operand::Parameter(index) => *count = (*count).max(index + 1),
        Operand::Arith(left, _, right) => {
            operand_parameters(left, count);
            operand_parameters(right, count);
        }
        Operand::Function(_, args) => {
            for arg in args {
                operand_parameters(arg, count);
            }
        }
        Operand::Literal(_) | Operand::Path(_) => {}
    }
}

/// `SELECT <projection> FROM <source> [WHERE <expr>] [ORDER BY <path> [ASC|DESC], ...]`.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub projection: Projection,
    pub from: FromSource,
    pub where_clause: Option<WhereExpr>,
    pub order_by: Option<OrderBy>,
    /// The refusal for a projection item that is a call (`COUNT(*)`), when
    /// there is one. The statement parses; the service reports the refusal
    /// once it has resolved the target, so a multi-attribute index key is
    /// reported first, and in place of a missing table (measured 2026-11-02).
    pub projection_error: Option<String>,
}

/// `INSERT INTO <table> VALUE { ... }`.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub value: Vec<(String, Operand)>,
}

/// `UPDATE <table> SET ... REMOVE ... WHERE <full key> [AND <condition>] [RETURNING <mode>]`,
/// or the FROM-first form `FROM <table> [WHERE ...] SET ... REMOVE ... [RETURNING <mode>]`.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub sets: Vec<SetAction>,
    pub removes: Vec<Path>,
    /// `SET a = set_add(a, ...)` and `SET a = set_delete(a, ...)` assignments.
    pub set_members: Vec<SetMembersAction>,
    /// `None` only in the FROM-first form, whose grammar makes WHERE optional;
    /// execution refuses it with the full-key message.
    pub where_clause: Option<WhereExpr>,
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

/// `SET a = set_add(a, <set>)` or `SET a = set_delete(a, <set>)`: add the
/// members of `<set>` to the set `a` holds, or remove them from it. An add to
/// a missing attribute creates the set, a delete from one leaves it missing,
/// and removing every member removes the attribute.
#[derive(Debug, Clone, PartialEq)]
pub struct SetMembersAction {
    pub path: Path,
    pub op: SetMembersOp,
    /// The members: a set literal, or a `?` parameter that must bind to a set.
    pub members: Operand,
    /// `line:column:length` of the function's first argument, which the
    /// refusal of a value that is not a set names.
    pub first_argument_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetMembersOp {
    Add,
    Delete,
}

impl SetMembersOp {
    /// The function name as the service writes it in messages.
    #[must_use]
    pub fn function_name(self) -> &'static str {
        match self {
            SetMembersOp::Add => "SET_ADD",
            SetMembersOp::Delete => "SET_DELETE",
        }
    }

    /// The refusal of a second argument that is not a set, positioned at the
    /// first argument (`line:column:length`).
    #[must_use]
    pub fn not_a_set_message(self, first_argument_at: &str) -> String {
        format!(
            "The second argument to {} must be a value with type SET at {first_argument_at}",
            self.function_name()
        )
    }
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
    /// `operand IS [NOT] <type>` for any type but MISSING; the flag is true for
    /// `IS NOT`.
    IsType(Operand, TypeName, bool),
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
    Not(Box<WhereExpr>),
    /// `<condition> = true`, `<> true`, `= false` or `<> false`. As a
    /// condition it holds where the condition does, or where it does not for
    /// `= false` and `<> true` (see [`BooleanComparison::negates`]). It is
    /// kept as it was written for the key reading, where a key condition
    /// compared with a boolean is read by key and the literal alone decides
    /// which items it returns (see `plan`).
    ComparedWithBoolean(Box<WhereExpr>, BooleanComparison),
}

/// How a condition is compared with a boolean literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BooleanComparison {
    /// `=` rather than `<>`.
    pub equal: bool,
    /// The literal, `true` or `false`.
    pub literal: bool,
}

impl BooleanComparison {
    /// Whether the comparison holds where the condition does not: `= false`
    /// and `<> true`.
    #[must_use]
    pub fn negates(self) -> bool {
        self.equal != self.literal
    }
}

impl WhereExpr {
    /// The condition under a run of `NOT`s and comparisons with a boolean,
    /// and whether the run negates it (an odd number of `NOT`s, `= false`s
    /// and `<> true`s). A condition is true or false, so two negations
    /// cancel: a translation can write the run as one `NOT` or none, which
    /// keeps a WHERE of 299 `NOT`s (which runs, measured 2026-11-02) within
    /// the classic parser's nesting limit.
    #[must_use]
    pub fn without_nots(&self) -> (bool, &WhereExpr) {
        let mut negated = false;
        let mut expr = self;
        loop {
            expr = match expr {
                WhereExpr::Not(inner) => {
                    negated = !negated;
                    inner
                }
                WhereExpr::ComparedWithBoolean(inner, comparison) => {
                    negated ^= comparison.negates();
                    inner
                }
                _ => return (negated, expr),
            };
        }
    }

    /// The condition under a run of comparisons with a boolean that keep it
    /// (`= true` and `<> false`).
    #[must_use]
    pub fn without_kept_comparisons(&self) -> &WhereExpr {
        let mut expr = self;
        while let WhereExpr::ComparedWithBoolean(inner, comparison) = expr
            && !comparison.negates()
        {
            expr = inner;
        }
        expr
    }
}

/// A type name the service's parser accepts after `IS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeName {
    Missing,
    Null,
    Boolean,
    Smallint,
    Integer4,
    Integer8,
    Integer,
    Float,
    Real,
    DoublePrecision,
    Decimal,
    Numeric,
    Timestamp,
    Date,
    Time,
    Character,
    CharacterVarying,
    String,
    Symbol,
    Clob,
    Blob,
    Struct,
    Tuple,
    List,
    Sexp,
    Bag,
    Any,
}

impl TypeName {
    /// The type named by a one-word type keyword (lower case), aliases
    /// included (`bool`, `int`, `int4`, `bigint`, `varchar`, ...).
    #[must_use]
    pub fn from_keyword(word: &str) -> Option<Self> {
        Some(match word {
            "missing" => Self::Missing,
            "null" => Self::Null,
            "bool" | "boolean" => Self::Boolean,
            "smallint" | "int2" | "integer2" => Self::Smallint,
            "int4" | "integer4" => Self::Integer4,
            "int8" | "integer8" | "bigint" => Self::Integer8,
            "int" | "integer" => Self::Integer,
            "float" => Self::Float,
            "real" => Self::Real,
            "decimal" | "dec" => Self::Decimal,
            "numeric" => Self::Numeric,
            "timestamp" => Self::Timestamp,
            "date" => Self::Date,
            "time" => Self::Time,
            "character" | "char" => Self::Character,
            "varchar" => Self::CharacterVarying,
            "string" => Self::String,
            "symbol" => Self::Symbol,
            "clob" => Self::Clob,
            "blob" => Self::Blob,
            "struct" => Self::Struct,
            "tuple" => Self::Tuple,
            "list" => Self::List,
            "sexp" => Self::Sexp,
            "bag" => Self::Bag,
            "any" => Self::Any,
            _ => return None,
        })
    }

    /// The `attribute_type` code a filter on this type tests for, or `None`
    /// for a type the service does not filter on (MISSING and NULL have
    /// filters of their own).
    #[must_use]
    pub fn attribute_type(self) -> Option<&'static str> {
        match self {
            Self::Boolean => Some("BOOL"),
            Self::Numeric => Some("N"),
            Self::String => Some("S"),
            Self::Blob => Some("B"),
            Self::Struct | Self::Tuple => Some("M"),
            Self::List => Some("L"),
            _ => None,
        }
    }

    /// The refusal of an `IS` filter on a type with no filter.
    #[must_use]
    pub fn unsupported_filter_message(self) -> String {
        format!(
            "Unsupported data type for IS operator: {}. Supported data types are [MISSING, \
             NULL, BOOLEAN, BLOB, STRING, NUMERIC, STRUCT, TUPLE, LIST]",
            self.service_name()
        )
    }

    /// The name the service uses for the type in its refusals.
    #[must_use]
    pub fn service_name(self) -> &'static str {
        match self {
            Self::Missing => "MissingType",
            Self::Null => "NullType",
            Self::Boolean => "BooleanType",
            Self::Smallint => "SmallintType",
            Self::Integer4 => "Integer4Type",
            Self::Integer8 => "Integer8Type",
            Self::Integer => "IntegerType",
            Self::Float => "FloatType",
            Self::Real => "RealType",
            Self::DoublePrecision => "DoublePrecisionType",
            Self::Decimal => "DecimalType",
            Self::Numeric => "NumericType",
            Self::Timestamp => "TimestampType",
            Self::Date => "DateType",
            Self::Time => "TimeType",
            Self::Character => "CharacterType",
            Self::CharacterVarying => "CharacterVaryingType",
            Self::String => "StringType",
            Self::Symbol => "SymbolType",
            Self::Clob => "ClobType",
            Self::Blob => "BlobType",
            Self::Struct => "StructType",
            Self::Tuple => "TupleType",
            Self::List => "ListType",
            Self::Sexp => "SexpType",
            Self::Bag => "BagType",
            Self::Any => "AnyType",
        }
    }
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

/// `ORDER BY <path> [ASC | DESC], ...`: one or more paths, each with its
/// direction.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    /// The paths in statement order; never empty.
    pub terms: Vec<OrderTerm>,
}

/// One path of an `ORDER BY` list and its direction.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderTerm {
    pub path: Path,
    pub descending: bool,
    /// The `line:column:length` span of the path's first token, where the
    /// refusal of a path with steps names it.
    pub at: String,
}

impl OrderBy {
    /// The first term that names the attribute `name` by itself.
    #[must_use]
    pub fn term_for(&self, name: &str) -> Option<&OrderTerm> {
        self.terms
            .iter()
            .find(|term| term.path.as_simple() == Some(name))
    }
}

/// The `RETURNING` clause of a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Returning {
    AllOld,
    ModifiedOld,
    AllNew,
    ModifiedNew,
}

#[cfg(test)]
mod tests;
