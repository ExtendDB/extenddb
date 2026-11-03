// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Statement forms and functions beyond the base productions in the parent
//! module: the FROM-first UPDATE, SET and REMOVE clauses in any number and
//! order, `set_add` and `set_delete` in a SET, `EXISTS(SELECT ...)`, the
//! operand checks on the condition functions, and a condition function
//! compared with a boolean.
//!
//! Every message is the service's (measured 2026-11-02).

use super::{PResult, ParseError, Parser};
use crate::partiql::ast::{
    BooleanComparison, Operand, Path, SelectStatement, SetAction, SetMembersAction, SetMembersOp,
    UpdateStatement, WhereExpr,
};
use crate::partiql::lexer::TokenKind;
use crate::types::AttributeValue;

/// The functions a WHERE condition recognizes.
const CONDITION_FUNCTIONS: [&str; 6] = [
    "attribute_exists",
    "attribute_not_exists",
    "attribute_type",
    "begins_with",
    "contains",
    "size",
];

/// The assignments of one or more SET and REMOVE clauses.
#[derive(Default)]
pub(super) struct DmlClauses {
    pub sets: Vec<SetAction>,
    pub removes: Vec<Path>,
    pub set_members: Vec<SetMembersAction>,
}

impl Parser {
    /// The FROM-first UPDATE, after `FROM`:
    /// `"table" [WHERE <condition>] (SET ... | REMOVE ...)+ [RETURNING ...]`.
    ///
    /// The service runs it as the UPDATE it spells out. WHERE is optional in
    /// this grammar, so a statement without one parses to its end, where a
    /// leftover token (a WHERE written after SET, say) is reported first, and
    /// execution then refuses it with the full-key message (measured
    /// 2026-11-02).
    pub(super) fn update_led_by_from(&mut self) -> PResult<UpdateStatement> {
        let table = self.table_name()?;
        // The target path and an alias get the refusals of an UPDATE's
        // (measured 2026-11-02: an index, a short index name, three
        // components, an empty component, `t` and `AS t`).
        self.dml_target_rest(&table, true)?;
        self.optional_alias()?;
        let where_clause = if self.eat_keyword("WHERE") {
            Some(self.where_expr()?)
        } else {
            None
        };
        let clauses = self.dml_clauses()?;
        let returning = self.returning(false)?;
        Ok(UpdateStatement {
            table,
            sets: clauses.sets,
            removes: clauses.removes,
            set_members: clauses.set_members,
            where_clause,
            returning,
        })
    }

    /// One or more SET and REMOVE clauses, in any order: an UPDATE may carry
    /// two SET clauses, and SET and REMOVE may follow each other (measured
    /// 2026-11-02). A SET takes assignments separated by commas, and a REMOVE
    /// takes one path: a comma after it is a token left over after the
    /// statement (measured 2026-11-02: `SET a = ..., b = ...` runs and
    /// `REMOVE m.a, m` is "Unexpected token after expression").
    pub(super) fn dml_clauses(&mut self) -> PResult<DmlClauses> {
        let mut clauses = DmlClauses::default();
        let mut saw_clause = false;
        // A refusal that stops the parse leaves the flag set (see `walk`).
        self.in_values = true;
        loop {
            if self.eat_keyword("SET") {
                loop {
                    self.set_assignment(&mut clauses)?;
                    if !self.eat(&TokenKind::Comma) {
                        break;
                    }
                }
            } else if self.eat_keyword("REMOVE") {
                clauses.removes.push(self.simple_path()?);
            } else {
                break;
            }
            saw_clause = true;
        }
        if !saw_clause {
            return Err(ParseError::well_formed("Expected data manipulation"));
        }
        self.in_values = false;
        Ok(clauses)
    }

    /// One `<path> = <value>` of a SET clause.
    fn set_assignment(&mut self, clauses: &mut DmlClauses) -> PResult<()> {
        let path = self.simple_path()?;
        if !self.eat(&TokenKind::Eq) {
            return Err(ParseError::well_formed("Expected = in SET clause"));
        }
        match self.set_function_ahead() {
            Some(op) => {
                let action = self.set_members_call(path, op)?;
                clauses.set_members.push(action);
            }
            None => {
                let value = self.operand()?;
                self.note_set_number(&value);
                // An IS test as the value parses and is refused (measured
                // 2026-11-02 with `SET a = b IS NULL`).
                if self.eat_keyword("IS") {
                    self.eat_keyword("NOT");
                    self.type_name()?;
                    self.defer("IS operator is not supported in update expressions");
                }
                clauses.sets.push(SetAction { path, value });
            }
        }
        Ok(())
    }

    /// Whether the current token is an identifier opening a call.
    fn at_call(&self) -> bool {
        matches!(self.peek().kind, TokenKind::Ident(_))
            && matches!(
                self.tokens.get(self.pos + 1).map(|t| &t.kind),
                Some(TokenKind::LParen)
            )
    }

    /// `set_add(` or `set_delete(` at the current token, in any case.
    fn set_function_ahead(&self) -> Option<SetMembersOp> {
        if !self.at_call() {
            return None;
        }
        match &self.peek().kind {
            TokenKind::Ident(name) if name.eq_ignore_ascii_case("set_add") => {
                Some(SetMembersOp::Add)
            }
            TokenKind::Ident(name) if name.eq_ignore_ascii_case("set_delete") => {
                Some(SetMembersOp::Delete)
            }
            _ => None,
        }
    }

    /// `set_add(<target>, <set>)` or `set_delete(<target>, <set>)` as the value
    /// of `SET <target> = ...`, with the current token on the function name.
    ///
    /// The service's checks, in order: exactly two arguments, the first one
    /// the assignment target, the second a set; positions name the function,
    /// except the set check, which names the first argument. An empty set
    /// literal is refused on its own (all measured 2026-11-02).
    fn set_members_call(&mut self, target: Path, op: SetMembersOp) -> PResult<SetMembersAction> {
        let function = op.function_name();
        let at = self.span();
        self.bump();
        self.bump();
        let mut args: Vec<(Operand, String)> = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RParen) {
            loop {
                let arg_at = self.span();
                args.push((self.operand()?, arg_at));
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&TokenKind::RParen) {
            return Err(ParseError::well_formed("Expected ) to close function call"));
        }
        let [(first, first_argument_at), (members, _)]: [(Operand, String); 2] =
            args.try_into().map_err(|_| {
                ParseError::new(format!(
                    "{function} must have exactly two arguments at {at}"
                ))
            })?;
        if !matches!(&first, Operand::Path(path) if *path == target) {
            return Err(ParseError::new(format!(
                "The first argument to {function} must equal the assignment value at {at}"
            )));
        }
        match &members {
            Operand::Literal(AttributeValue::SS(set)) if set.is_empty() => {
                return Err(ParseError::new("Empty bags are not supported"));
            }
            Operand::Literal(
                AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_),
            )
            | Operand::Parameter(_) => {}
            _ => {
                return Err(ParseError::new(op.not_a_set_message(&first_argument_at)));
            }
        }
        Ok(SetMembersAction {
            path: target,
            op,
            members,
            first_argument_at,
        })
    }

    /// Whether the statement is `EXISTS(SELECT ...)`.
    pub(super) fn at_exists_statement(&self) -> bool {
        self.is_keyword("EXISTS")
            && self.at_call()
            && matches!(
                self.tokens.get(self.pos + 2).map(|t| &t.kind),
                Some(TokenKind::Ident(s)) if s.eq_ignore_ascii_case("SELECT")
            )
    }

    /// `EXISTS(SELECT ...)`, with the current token on `EXISTS`.
    pub(super) fn exists_statement(&mut self) -> PResult<SelectStatement> {
        self.bump();
        self.bump();
        self.bump();
        let select = self.select(false)?;
        if !self.eat(&TokenKind::RParen) {
            return Err(ParseError::well_formed("Expected ) to close function call"));
        }
        Ok(select)
    }

    /// A predicate compared with a boolean literal, as in
    /// `attribute_exists(a) = true`, any number of times, as in `a = 1 = true
    /// = true` (measured 2026-11-02): as a condition, `= true` and `<> false`
    /// keep it and `= false` and `<> true` negate it. The comparison is kept
    /// as written (see [`WhereExpr::ComparedWithBoolean`]), since a key
    /// condition compared with a boolean is read by key (see `plan`). A
    /// comparison compared with a number with `=` holds for no item
    /// (measured 2026-11-02: `a = 1 = 1` runs and selects nothing). Past the
    /// operator limit the statement is refused, and the condition is not
    /// built on.
    pub(super) fn compared_with_boolean(&mut self, condition: WhereExpr) -> WhereExpr {
        let mut condition = condition;
        loop {
            let equal = match self.peek().kind {
                TokenKind::Eq => true,
                TokenKind::Ne => false,
                _ => return condition,
            };
            let literal = match self.tokens.get(self.pos + 1).map(|t| &t.kind) {
                Some(TokenKind::Ident(s)) if s.eq_ignore_ascii_case("true") => true,
                Some(TokenKind::Ident(s)) if s.eq_ignore_ascii_case("false") => false,
                Some(TokenKind::Number(n))
                    if equal
                        && matches!(
                            condition.without_kept_comparisons(),
                            WhereExpr::Compare(..)
                        ) =>
                {
                    let n = n.clone();
                    self.count_operator();
                    self.bump();
                    self.bump();
                    self.walk_number(&n, super::LITERAL_ROOT);
                    let never = WhereExpr::Not(Box::new(condition.clone()));
                    condition = WhereExpr::And(Box::new(condition), Box::new(never));
                    continue;
                }
                _ => return condition,
            };
            self.count_operator();
            self.bump();
            self.bump();
            if self.within_operator_limit() {
                condition = WhereExpr::ComparedWithBoolean(
                    Box::new(condition),
                    BooleanComparison { equal, literal },
                );
            }
        }
    }

    /// A function call as a top-level INSERT value: once the call parses, it is
    /// refused naming the function and its position (measured 2026-11-02 for
    /// `set_add`). `None` when the current token does not open a call.
    pub(super) fn function_as_item_value(&mut self) -> Option<ParseError> {
        if !self.at_call() {
            return None;
        }
        let at = self.span();
        let TokenKind::Ident(name) = self.bump().kind else {
            return None;
        };
        if let Err(e) = self.call_args() {
            return Some(e);
        }
        Some(ParseError::new(format!(
            "Unsupported data type: {} under key root at {at}",
            name.to_ascii_lowercase()
        )))
    }
}

/// The operand checks on `attribute_exists` and `attribute_not_exists`: exactly
/// one operand, and that operand a document path rather than a literal or a
/// parameter (measured 2026-11-02).
pub(super) fn check_presence_operands(name: &str, args: &[Operand]) -> PResult<()> {
    if args.len() != 1 {
        return Err(ParseError::new(format!(
            "Incorrect number of operands for operator or function; operator or function: {name}, number of operands: {}",
            args.len()
        )));
    }
    if !matches!(args[0], Operand::Path(_)) {
        return Err(ParseError::new(format!(
            "Operator or function requires a document path; operator or function: {name}"
        )));
    }
    Ok(())
}

/// A call inside a WHERE condition to anything but a condition function, such
/// as `EXISTS(a)` or `set_add(...)`, is refused naming the function in lower
/// case and its position (measured 2026-11-02). `None` for a condition
/// function.
pub(super) fn where_function_refusal(name: &str, at: &str) -> Option<String> {
    (!CONDITION_FUNCTIONS.contains(&name)).then(|| format!("Unrecognized function: {name} at {at}"))
}

#[cfg(test)]
mod tests;
