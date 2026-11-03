// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The predicates that follow an operand in a condition: `IN`, `BETWEEN`,
//! `LIKE` and their `NOT` forms, and `IS`; and what may follow a predicate.

use super::{IN_OPERANDS, IS_LEFT_SIDE, LITERAL_ROOT, Nest, PResult, ParseError, Parser};
use crate::partiql::ast::{CompareOp, Operand, TypeName, WhereExpr};
use crate::partiql::lexer::{TokenKind, WordClass, is_function_keyword, word_class};
use crate::types::AttributeValue;

impl Parser {
    /// `<operand> NOT IN ...` and `NOT BETWEEN ...`, with the current token on
    /// the NOT: the predicate after the NOT, negated. The NOT counts as an
    /// operator of its own (measured 2026-11-02: `a NOT IN [1]` and `a NOT
    /// BETWEEN 1 AND 2` count two operators and run wherever a condition
    /// does). `NOT LIKE` gets the answers LIKE gets (measured 2026-11-02).
    ///
    /// Before anything else a NOT ends the expression at the operand, and
    /// what encloses it reports the NOT (measured 2026-11-02: `a NOT = 1`,
    /// `a NOT IS MISSING` and `a NOT NOT IN [1]` are a token left over after
    /// the statement, `(a NOT = 1)` a group without its right parenthesis).
    pub(super) fn negated_predicate(&mut self, left: Operand) -> PResult<WhereExpr> {
        let word = match self.tokens.get(self.pos + 1).map(|t| &t.kind) {
            Some(TokenKind::Ident(word)) => word.to_ascii_uppercase(),
            _ => String::new(),
        };
        if !matches!(word.as_str(), "IN" | "BETWEEN" | "LIKE") {
            return Err(ParseError::well_formed(if self.nesting.in_group() {
                "Expected RIGHT_PAREN"
            } else {
                "Unexpected token after expression"
            }));
        }
        self.bump();
        self.count_operator();
        self.bump();
        let predicate = match word.as_str() {
            "IN" => self.in_list(left)?,
            "BETWEEN" => self.between(left)?,
            _ => self.like(left)?,
        };
        self.after_predicate(WhereExpr::Not(Box::new(predicate)))
    }

    /// The rest of `<operand> BETWEEN <low> AND <high>` after BETWEEN
    /// (measured 2026-11-02: nothing after BETWEEN, after its low bound and
    /// after its AND each have their own message, with and without NOT).
    pub(super) fn between(&mut self, value: Operand) -> PResult<WhereExpr> {
        self.count_operator();
        self.require_rhs()?;
        let low = self.operand()?;
        if !self.eat_keyword("AND") {
            return Err(ParseError::well_formed("Expected AND keyword"));
        }
        if matches!(self.peek().kind, TokenKind::Eof) {
            return Err(ParseError::well_formed("Expected expression after AND"));
        }
        let high = self.operand()?;
        Ok(WhereExpr::Between(value, low, high))
    }

    /// The rest of `<operand> LIKE <pattern> [ESCAPE <escape>]` after LIKE.
    /// The statement parses, and LIKE is refused as an operator a condition
    /// does not take (see [`Parser::like_pattern`]). The comparison returned
    /// stands for it and never runs.
    pub(super) fn like(&mut self, value: Operand) -> PResult<WhereExpr> {
        let pattern = self.like_pattern()?;
        Ok(WhereExpr::Compare(value, CompareOp::Eq, pattern))
    }

    /// The pattern of a LIKE and its ESCAPE, after LIKE. LIKE is refused as
    /// an operator a condition does not take, with or without NOT and with or
    /// without ESCAPE (measured 2026-11-02: `a LIKE 'x'`, `a NOT LIKE 'x'`
    /// and `a LIKE 'x' ESCAPE 'y'` name the operator `like`). Nothing after
    /// LIKE is refused like any infix operator, and nothing after ESCAPE has
    /// a message of its own (measured 2026-11-02).
    fn like_pattern(&mut self) -> PResult<Operand> {
        self.count_operator();
        self.require_rhs()?;
        self.refuse_condition_operator("like");
        let pattern = self.operand()?;
        if self.eat_keyword("ESCAPE") {
            if matches!(self.peek().kind, TokenKind::Eof) {
                return Err(ParseError::well_formed("Expected expression after ESCAPE"));
            }
            self.operand()?;
        }
        Ok(pattern)
    }

    /// An argument of a call read as an operand of a condition: an operand,
    /// or an operand and a LIKE, which is refused there like anywhere in the
    /// condition (measured 2026-11-02: `foo(a LIKE 'x')` names the operator
    /// `like`). The argument is one part of the walk, so the LIKE comes
    /// after what the arguments before it met. The operand stands for the
    /// LIKE and never runs. NOT LIKE there, and LIKE in the arguments of a
    /// condition function that starts a predicate, are not measured, and keep
    /// the syntax error of a call left open.
    pub(super) fn argument_with_like(&mut self) -> PResult<Operand> {
        let before = self.walk_part();
        let argument = self.operand().and_then(|argument| {
            if self.eat_keyword("LIKE") {
                self.like_pattern()?;
            }
            Ok(argument)
        });
        self.end_walk_part(before);
        argument
    }

    /// The rest of `<operand> IS [NOT] <type>` after IS, with `left_span`
    /// the position of the operand. The left side must be a name, a literal
    /// or a path (measured 2026-11-02 with a parameter), and a type with no
    /// filter is refused at the left side's position (measured 2026-11-02:
    /// `a IS INT4` names the `a`). The walk meets the IS before its left
    /// side (measured 2026-11-02: `? IS NULL` with no parameter is refused
    /// for its left side), and the refusal of `INT4` after the left side
    /// (measured 2026-11-02: `1e126 IS INT4` is refused for the number). The
    /// other types with no filter are not measured against their left side,
    /// and keep the place of an operator, before it.
    pub(super) fn is_test(&mut self, left: Operand, left_span: &str) -> PResult<WhereExpr> {
        self.count_operator();
        let negated = self.eat_keyword("NOT");
        if negated {
            self.count_operator();
        }
        let ty = self.type_name()?;
        if matches!(
            left,
            Operand::Parameter(_) | Operand::Function(..) | Operand::Arith(..)
        ) {
            self.walk_operator(IS_LEFT_SIDE.to_owned());
            // It stands for the test and never runs.
            return Ok(WhereExpr::Function("is".to_owned(), vec![left]));
        }
        if ty == TypeName::Missing {
            return match left {
                Operand::Path(path) => Ok(WhereExpr::IsMissing(path, !negated)),
                // (measured 2026-11-02: `1 IS MISSING` and `'x' IS NOT
                // MISSING`). Other literals are not measured.
                Operand::Literal(AttributeValue::N(_) | AttributeValue::S(_)) => {
                    Err(ParseError::new(
                        "Operator or function requires a document path; operator or function: \
                         attribute_not_exists",
                    ))
                }
                _ => Err(ParseError::new("IS MISSING requires an attribute path")),
            };
        }
        if ty != TypeName::Null && ty.attribute_type().is_none() {
            let refusal = format!("{} at {left_span}", ty.unsupported_filter_message());
            if ty == TypeName::Integer4 {
                self.walk_after_operands(&refusal);
            } else {
                self.walk_operator(refusal);
            }
        }
        Ok(WhereExpr::IsType(left, ty, negated))
    }

    /// The rest of `<operand> IN <list>` after IN: the members in brackets,
    /// in parentheses or in a bag (measured 2026-11-02: `a IN [1, 2]`, `a IN
    /// (1, 2)` and `a IN <<1, 5>>` run, with and without NOT). A bag there is
    /// not checked as a set literal (measured 2026-11-02: `a IN <<1, 1>>` and
    /// `a IN <<'x', 1>>` run). Anything else after IN, an empty list and an
    /// empty bag are refused naming the operands IN takes (measured
    /// 2026-11-02 with `IN 1`, `IN []` and `IN <<>>`); in parentheses a member
    /// must follow (measured 2026-11-02: `IN ()` and `IN (1,)` are
    /// "Unexpected term").
    pub(super) fn in_list(&mut self, value: Operand) -> PResult<WhereExpr> {
        self.count_operator();
        self.require_rhs()?;
        let items = match self.peek().kind {
            TokenKind::LBracket => {
                self.bump();
                let items = self.in_members(&TokenKind::RBracket)?;
                if !self.eat(&TokenKind::RBracket) {
                    return Err(ParseError::new("Expected ] to close IN list"));
                }
                items
            }
            TokenKind::LParen => {
                self.bump();
                let mut items = vec![self.in_member()?];
                while self.eat(&TokenKind::Comma) {
                    items.push(self.in_member()?);
                }
                if !self.eat(&TokenKind::RParen) {
                    return Err(ParseError::well_formed("Expected RIGHT_PAREN"));
                }
                items
            }
            TokenKind::LDoubleAngle => self.nested(Nest::Literal, Self::in_bag)?,
            _ => return Err(ParseError::new(IN_OPERANDS)),
        };
        if items.is_empty() {
            return Err(ParseError::new(IN_OPERANDS));
        }
        self.count_in_operands(items.len());
        Ok(WhereExpr::In(value, items))
    }

    /// The members of an IN list up to the token that closes it, which is
    /// left unread.
    fn in_members(&mut self, close: &TokenKind) -> PResult<Vec<Operand>> {
        let mut items = Vec::new();
        if &self.peek().kind != close {
            loop {
                items.push(self.operand()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        Ok(items)
    }

    /// One member of an IN list in parentheses. A member in parentheses of
    /// its own is a list when a comma separates what they hold, and what they
    /// hold otherwise (measured 2026-11-02: in `a IN ((1, 2))` the member is
    /// the list `[1, 2]`, and in `a IN ((5))` the number 5).
    fn in_member(&mut self) -> PResult<Operand> {
        if !matches!(self.peek().kind, TokenKind::LParen) {
            return self.operand();
        }
        if self.list_in_parentheses_ahead() {
            let list = self.list_in_parentheses(LITERAL_ROOT)?;
            return Ok(Operand::Literal(AttributeValue::L(list)));
        }
        self.bump();
        let member = self.nested(Nest::Group, Self::in_member)?;
        if !self.eat(&TokenKind::RParen) {
            return Err(ParseError::well_formed("Expected RIGHT_PAREN"));
        }
        Ok(member)
    }

    /// Whether the parentheses opening at the current token hold a comma of
    /// their own, outside any bracket, brace or parenthesis inside them.
    fn list_in_parentheses_ahead(&self) -> bool {
        let mut depth = 0usize;
        for token in &self.tokens[self.pos..] {
            match token.kind {
                TokenKind::LParen
                | TokenKind::LBracket
                | TokenKind::LBrace
                | TokenKind::LDoubleAngle => depth += 1,
                TokenKind::RParen
                | TokenKind::RBracket
                | TokenKind::RBrace
                | TokenKind::RDoubleAngle => {
                    depth -= 1;
                    if depth == 0 {
                        return false;
                    }
                }
                TokenKind::Comma if depth == 1 => return true,
                TokenKind::Eof => return false,
                _ => {}
            }
        }
        false
    }

    /// A list literal written in parentheses, at `path` inside the value
    /// being read: its elements are read as those of a list in brackets.
    fn list_in_parentheses(&mut self, path: &str) -> PResult<Vec<AttributeValue>> {
        self.nested(Nest::Literal, |p| {
            p.nesting.containers += 1;
            let list = p.elements_in_parentheses(path);
            p.nesting.containers -= 1;
            list
        })
    }

    fn elements_in_parentheses(&mut self, path: &str) -> PResult<Vec<AttributeValue>> {
        self.bump();
        let mut items = Vec::new();
        loop {
            let element = format!("{path}[{}]", items.len());
            if self.term_in_parentheses(&element)? {
                // It stands for the term and is never used.
                items.push(AttributeValue::Null);
            } else {
                items.push(self.literal_value(&element)?);
            }
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        if !self.eat(&TokenKind::RParen) {
            return Err(ParseError::well_formed("Expected RIGHT_PAREN"));
        }
        Ok(items)
    }

    /// A quoted name, a path or a call as an element of a list in
    /// parentheses, at `path` inside the list: it parses and is refused at
    /// its position, named by what it is (measured 2026-11-02: in `a IN
    /// (("pk", 1))`, `a IN ((m.b, 1))` and `a IN ((foo(b), 1))` the first
    /// member is refused as an Id, a Path and a Call under root[0], at the
    /// first token). Returns false, having read nothing, for any other
    /// element; a name alone is refused by [`Parser::literal_value`]. In a
    /// list in brackets these are not measured.
    fn term_in_parentheses(&mut self, path: &str) -> PResult<bool> {
        let next = self.tokens.get(self.pos + 1).map(|t| &t.kind);
        let name = match &self.peek().kind {
            TokenKind::Ident(w) => word_class(w) == WordClass::Name,
            TokenKind::QuotedIdent(_) => true,
            _ => false,
        };
        let callable = matches!(
            &self.peek().kind,
            TokenKind::Ident(w) if word_class(w) == WordClass::Name || is_function_keyword(w)
        );
        let kind = match next {
            Some(TokenKind::Dot | TokenKind::LBracket) if name => "Path",
            Some(TokenKind::LParen) if callable => "Call",
            Some(TokenKind::LParen) => return Ok(false),
            _ if matches!(self.peek().kind, TokenKind::QuotedIdent(_)) => "Id",
            _ => return Ok(false),
        };
        self.defer(&format!(
            "Unsupported data type: {kind} under key {path} at {}",
            self.span()
        ));
        match kind {
            "Path" => {
                self.path()?;
            }
            "Call" => {
                // What the arguments meet comes after the refusal.
                self.bump();
                let before = self.walk_part();
                let args = self.call_args();
                self.end_walk_part(before);
                args?;
            }
            _ => {
                self.bump();
            }
        }
        Ok(true)
    }

    /// The members of a bag after IN, with the current token on its `<<`:
    /// the operands of the IN in the order they are written, each a value, a
    /// parameter or a name (measured 2026-11-02: `a IN <<?>>` and `a IN
    /// <<pk>>` run). A member is named as the value itself in a refusal
    /// (measured 2026-11-02: `a IN <<1e126>>` is refused under root).
    fn in_bag(&mut self) -> PResult<Vec<Operand>> {
        self.bump();
        let mut members = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RDoubleAngle) {
            loop {
                members.push(if matches!(self.peek().kind, TokenKind::Param) {
                    self.bump();
                    let index = self.take_param_index();
                    self.walk_parameter(index);
                    Operand::Parameter(index)
                } else if let TokenKind::Ident(w) = &self.peek().kind
                    && self.at_name_in_literal(w)
                {
                    Operand::Path(self.path()?)
                } else {
                    Operand::Literal(self.literal_value(LITERAL_ROOT)?)
                });
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&TokenKind::RDoubleAngle) {
            return Err(ParseError::well_formed(
                "Expected RIGHT_DOUBLE_ANGLE_BRACKET",
            ));
        }
        Ok(members)
    }

    /// What may follow a predicate: a comparison with a boolean (measured
    /// 2026-11-02: `a NOT IN [1] = true`, `(a = 1) = false` and `pk = 'a' =
    /// true` run and match what the predicate does or does not), or an IS
    /// test (see [`Parser::trailing_is`]).
    pub(super) fn after_predicate(&mut self, predicate: WhereExpr) -> PResult<WhereExpr> {
        if self.trailing_is()? {
            return Ok(predicate);
        }
        Ok(self.compared_with_boolean(predicate))
    }

    /// An IS test after a predicate, which parses and is refused for its
    /// left side (measured 2026-11-02 after IN, NOT IN, BETWEEN, `=` and an
    /// IS test). The walk meets the IS before the predicate. Returns whether
    /// there was one.
    pub(super) fn trailing_is(&mut self) -> PResult<bool> {
        if !self.eat_keyword("IS") {
            return Ok(false);
        }
        self.count_operator();
        if self.eat_keyword("NOT") {
            self.count_operator();
        }
        self.type_name()?;
        self.walk_operator(IS_LEFT_SIDE.to_owned());
        Ok(true)
    }
}

#[cfg(test)]
mod tests;
