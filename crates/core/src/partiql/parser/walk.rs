// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The refusals of a statement that parses, and the order they are reported
//! in.
//!
//! The parser reads on after it finds a refusal, so that a syntax error
//! anywhere in the statement still wins, and reports the refusal once the
//! statement has been read. The service's order is (measured 2026-11-02):
//!
//! 1. a syntax error anywhere in the statement;
//! 2. a refusal of the statement's target, found before the WHERE condition
//!    (an alias on the table, a FROM path);
//! 3. in ExecuteStatement, an `EXISTS(SELECT ...)` statement;
//! 4. for a request, a table name outside the name rule;
//! 5. an alias in the select list, and an UPDATE whose target is an index;
//! 6. a LIMIT clause;
//! 7. the first problem the walk of the WHERE condition meets (below);
//! 8. the IN list limit and the operator count of the condition, whichever
//!    the condition meets first;
//! 9. the refusals in an UPDATE's SET and REMOVE clauses and in an INSERT's
//!    value, wherever they are written, then those after the condition.
//!
//! Measured: an alias on the table before a FROM path refusal and before the
//! name rule, and an empty FROM path component and a path of three
//! components before the name rule; EXISTS in ExecuteStatement before the
//! name rule and `+`; the name rule before an alias in the select list, an
//! UPDATE of an index, a LIMIT clause, `+`, a number that breaks an item
//! number rule with 301 operators, 301 operators and an IN list of 101
//! members; LIMIT before a number, a missing parameter, `+`, 301 operators
//! and an IN list of 101 members in the condition; the walk before the IN
//! limit and the count; the count before a SET value, a SET parameter, a
//! unary plus, an IS test and a parameter inside a list in a SET clause, in
//! both UPDATE forms; the walk of the condition before a SET clause written
//! before it. Not measured: an alias on the table against EXISTS, an
//! INSERT's value against the name rule, and a DELETE of an index against
//! the name rule (it keeps its place in 2).
//!
//! In the SET clause, a sign before a value that is not a number comes after
//! the number literals of the clause that break an item number rule, in
//! either order (measured 2026-11-02 with a unary plus). Those numbers are
//! otherwise left to the checks made once the statement parses.
//!
//! The walk goes over the condition as a tree, while the parser reads it in
//! the order it is written. The walk meets an operator before its operands
//! (`b = 1e126 + 1` is refused for the `+`, `? IS NULL` with no parameter
//! for the left side of the IS), the operands in the order they are written,
//! and the arguments of a call before its name (`foo(1e126)` is refused for
//! the number, `foo([?])` for the parameter). It meets the refusals found
//! while the condition is read (an operator a condition does not take, an
//! unknown function, MISSING as a value, a literal nested too deep, a
//! parameter inside a literal), a number literal that breaks an item number
//! rule, also inside a list or map literal, and, for a request, a `?` with no
//! parameter left in it. The order is the same within the operator limit and
//! past it. Some checks are met after the operands they apply to (measured
//! 2026-11-02): the refusal of `IS INT4` after its left side (`1e126 IS INT4`
//! is refused for the number), an operand that is not a condition, as an
//! argument of AND or OR, after both arguments, and an operand that is the
//! whole condition after the rest of the walk.
//!
//! A SELECT whose select list holds a call (`COUNT(*)`) is refused for the
//! call ahead of every number literal and operand type in its condition
//! (measured 2026-11-02), and the engine reports the call once it has
//! resolved the target. So the parser leaves the condition of such a SELECT
//! to the engine. The call comes ahead of an IN with no list after it too
//! (measured 2026-11-02 with `a IN 1`), which stops the parse: the parser
//! reports the call itself then. That a refusal found while reading the
//! condition, the IN limit and the operator count come after the call is
//! not measured.
//!
//! A refusal the parse stops at comes after those found before it that come
//! ahead of its part of the statement in this order.

use super::limits::{MAX_IN_OPERANDS, too_many_in_operands, too_many_operators};
use super::{
    EXISTS_OUTSIDE_TRANSACTION, IN_OPERANDS, PARAMETER_COUNT_MISMATCH, PResult, ParseError, Parser,
    Statement,
};
use crate::partiql::ast::Operand;
use crate::partiql::from_path;

/// The refusals found while a statement is read, by the part of the
/// statement they were found in.
#[derive(Debug, Default)]
pub(super) struct Findings {
    /// The first refusal found before the WHERE condition, outside the SET,
    /// REMOVE and VALUE parts, that comes ahead of the table name rule.
    before_where: Option<String>,
    /// The first refusal found before the WHERE condition that comes after
    /// the table name rule: an alias in the select list, an UPDATE of an
    /// index.
    after_name_rule: Option<String>,
    /// The refusal of the LIMIT clause a statement ends with.
    clause: Option<String>,
    /// The first problem the walk of the WHERE condition has met in the part
    /// of the condition being read (see [`Parser::walk_part`]).
    walk: Option<String>,
    /// The refusal of the first IN list with too many members, when the
    /// condition meets it before its 301st operator.
    in_operands: Option<String>,
    /// The first refusal in an UPDATE's SET and REMOVE clauses or an INSERT's
    /// value.
    values: Option<String>,
    /// Whether [`Findings::values`] is the refusal of a sign before a SET
    /// value.
    values_is_sign: bool,
    /// The first number literal in a SET value that breaks an item number
    /// rule.
    set_number: Option<String>,
    /// The first refusal found after the WHERE condition.
    after_where: Option<String>,
}

/// The parts of a statement, in the order their refusals are reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Part {
    BeforeWhere,
    Where,
    Values,
    AfterWhere,
}

fn keep_first(slot: &mut Option<String>, message: &str) {
    if slot.is_none() {
        *slot = Some(message.to_owned());
    }
}

impl Parser {
    /// The part of the statement being read.
    fn part(&self) -> Part {
        if self.in_where {
            Part::Where
        } else if self.in_values {
            Part::Values
        } else if self.after_where {
            Part::AfterWhere
        } else {
            Part::BeforeWhere
        }
    }

    /// Keep a refusal found while the statement is still being read.
    pub(super) fn defer(&mut self, message: &str) {
        let slot = match self.part() {
            Part::BeforeWhere => &mut self.found.before_where,
            Part::Where => &mut self.found.walk,
            Part::Values => &mut self.found.values,
            Part::AfterWhere => &mut self.found.after_where,
        };
        keep_first(slot, message);
    }

    /// Keep a refusal found before the WHERE condition that comes after the
    /// table name rule.
    pub(super) fn defer_after_name_rule(&mut self, message: &str) {
        keep_first(&mut self.found.after_name_rule, message);
    }

    /// Keep the refusal of a sign before a value that is not a number. In a
    /// SET clause it comes after the clause's number literals that break an
    /// item number rule (see [`Parser::note_set_number`]).
    pub(super) fn defer_sign(&mut self, message: &str) {
        if self.part() == Part::Values && self.found.values.is_none() {
            self.found.values = Some(message.to_owned());
            self.found.values_is_sign = true;
        } else {
            self.defer(message);
        }
    }

    /// Note the value of a SET assignment: the first of its number literals
    /// that breaks an item number rule, if any.
    pub(super) fn note_set_number(&mut self, value: &Operand) {
        if self.found.set_number.is_none() {
            self.found.set_number = crate::partiql::number::operand_number_refusal(value);
        }
    }

    /// Keep the refusal of a clause the statement ends with.
    pub(super) fn refuse_clause(&mut self, message: String) {
        if self.found.clause.is_none() {
            self.found.clause = Some(message);
        }
    }

    /// Start a part of the condition, an operand or a predicate: what the
    /// walk met before it is set aside, and handed back to
    /// [`Parser::end_walk_part`], which puts it ahead of what the part meets.
    /// Within the part, an operator read after its first operand can then
    /// be put ahead of what that operand met (see [`Parser::walk_operator`]).
    pub(super) fn walk_part(&mut self) -> Option<String> {
        self.found.walk.take()
    }

    /// End a part of the condition started with [`Parser::walk_part`].
    pub(super) fn end_walk_part(&mut self, before: Option<String>) {
        if before.is_some() {
            self.found.walk = before;
        }
    }

    /// The refusal of an operator of the condition, read after the operands
    /// the current part has read so far: the walk meets an operator before
    /// its operands.
    pub(super) fn walk_operator(&mut self, refusal: String) {
        if self.in_where {
            self.found.walk = Some(refusal);
        }
    }

    /// The refusal of a check of the condition that the walk meets after the
    /// operands it applies to.
    pub(super) fn walk_after_operands(&mut self, refusal: &str) {
        if self.in_where {
            keep_first(&mut self.found.walk, refusal);
        }
    }

    /// Note a number literal read in the WHERE condition at `path` inside its
    /// value (`root`, `root[0]`, `root.k`), for the walk.
    pub(super) fn walk_number(&mut self, text: &str, path: &str) {
        if self.in_where
            && self.found.walk.is_none()
            && let Some(refusal) = crate::partiql::number::literal_number_refusal(text, path)
        {
            self.found.walk = Some(refusal);
        }
    }

    /// Note the `?` with index `index` read in the WHERE condition, for the
    /// walk: the request may give no parameter for it.
    pub(super) fn walk_parameter(&mut self, index: usize) {
        if self.in_where && self.parameters.is_some_and(|given| index >= given) {
            keep_first(&mut self.found.walk, PARAMETER_COUNT_MISMATCH);
        }
    }

    /// Note an IN list of `members` members, read with the operators counted
    /// so far.
    pub(super) fn count_in_operands(&mut self, members: usize) {
        if members > MAX_IN_OPERANDS && self.within_operator_limit() {
            let refusal = too_many_in_operands(members);
            keep_first(&mut self.found.in_operands, &refusal);
        }
    }

    /// Forget the refusals found so far, for a statement refused by its form.
    pub(super) fn forget_findings(&mut self) {
        self.found = Findings::default();
    }

    /// For a request, the refusal of a table name outside the name rule.
    fn table_name_refusal(&self) -> Option<String> {
        let table = self
            .table
            .as_deref()
            .filter(|_| self.parameters.is_some())?;
        from_path::breaks_name_rule(table).then(|| from_path::NAME_RULE.to_owned())
    }

    /// The answer for a statement once it has been read, in the order the
    /// module documents. `result` is what the parse returned.
    pub(super) fn resolve(&mut self, result: PResult<Statement>) -> PResult<Statement> {
        let found = std::mem::take(&mut self.found);
        let call = self.select_list_call.take();
        // The parts read in full: all of them, or those before the part a
        // refusal stopped the parse in.
        let reached = match &result {
            Err(e) if e.is_syntax() => return result,
            Err(_) => self.part(),
            Ok(_) => Part::AfterWhere,
        };
        let stopped_at_in = matches!(&result, Err(e) if e.message == IN_OPERANDS);
        let condition = match &result {
            Ok(Statement::Select(s)) => s.projection_error.is_none(),
            Err(_) if stopped_at_in => call.is_none(),
            _ => true,
        };
        let exists = (self.refuse_exists && matches!(&result, Ok(Statement::Exists(_))))
            .then(|| EXISTS_OUTSIDE_TRANSACTION.to_owned());
        let mut refusal = found
            .before_where
            .or(exists)
            .or_else(|| self.table_name_refusal())
            .or(found.after_name_rule)
            .or(found.clause);
        if reached >= Part::Where && condition {
            refusal = refusal.or(found.walk);
        }
        if reached > Part::Where && condition {
            refusal = refusal
                .or(found.in_operands)
                .or_else(|| (!self.within_operator_limit()).then(too_many_operators));
        }
        if reached >= Part::Values {
            let values = match found.values {
                Some(sign) if found.values_is_sign => Some(found.set_number.unwrap_or(sign)),
                values => values,
            };
            refusal = refusal.or(values);
        }
        if reached >= Part::AfterWhere {
            refusal = refusal.or(found.after_where);
        }
        // A statement that parses leaves its call to the engine.
        if stopped_at_in {
            refusal = refusal.or(call);
        }
        match (result, refusal) {
            (_, Some(refusal)) => Err(ParseError::new(refusal)),
            (result, None) => result,
        }
    }
}

#[cfg(test)]
mod tests;
