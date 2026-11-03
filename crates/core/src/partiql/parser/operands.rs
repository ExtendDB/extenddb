// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Paths and operands: the signs and arithmetic of an operand and the term it starts with.

use super::limits::Nest;
use super::{LITERAL_ROOT, PResult, ParseError, Parser, forms, terms};
use crate::partiql::ast::{ArithOp, Operand, Path, PathStep};
use crate::partiql::lexer::{TokenKind, WordClass, is_function_keyword, word_class};
use crate::types::AttributeValue;

impl Parser {
    // --- paths ---

    /// An empty quoted name in a path is refused once the statement parses
    /// (measured 2026-11-02 in a condition, as the path and as a step).
    fn refuse_empty_component(&mut self, name: &str) {
        if name.is_empty() {
            self.defer("Path component cannot be an empty string");
        }
    }

    pub(super) fn path(&mut self) -> PResult<Path> {
        let root = match &self.peek().kind {
            TokenKind::Ident(s)
                if matches!(word_class(s), WordClass::Name | WordClass::Literal) =>
            {
                let s = s.clone();
                self.bump();
                s
            }
            TokenKind::QuotedIdent(s) => {
                let s = s.clone();
                self.bump();
                self.refuse_empty_component(&s);
                s
            }
            _ => return Err(self.term_error()),
        };
        let mut steps = Vec::new();
        loop {
            if self.eat(&TokenKind::Dot) {
                match &self.peek().kind {
                    TokenKind::QuotedIdent(s) => {
                        let s = s.clone();
                        self.bump();
                        self.refuse_empty_component(&s);
                        steps.push(PathStep::Attribute(s));
                    }
                    TokenKind::Ident(s) if word_class(s) == WordClass::Name => {
                        let s = s.clone();
                        self.bump();
                        steps.push(PathStep::Attribute(s));
                    }
                    TokenKind::Star => {
                        return Err(ParseError::new(format!(
                            "Unexpected path component at {}",
                            self.span()
                        )));
                    }
                    // A reserved word (measured 2026-11-02 with `inner`), or
                    // anything else that is not a name, after a dot.
                    _ => return Err(ParseError::well_formed("Invalid path dot component")),
                }
            } else if self.eat(&TokenKind::LBracket) {
                let idx = match &self.peek().kind {
                    TokenKind::Number(n) => n.parse::<usize>().map_err(|_| {
                        ParseError::new(format!("Unexpected path component at {}", self.span()))
                    })?,
                    _ => {
                        return Err(ParseError::new(format!(
                            "Unexpected path component at {}",
                            self.span()
                        )));
                    }
                };
                self.bump();
                if !self.eat(&TokenKind::RBracket) {
                    return Err(ParseError::new("Expected ] after list index"));
                }
                steps.push(PathStep::Index(idx));
            } else {
                break;
            }
        }
        Ok(Path { root, steps })
    }

    // --- operands ---

    /// A condition takes no arithmetic: the service refuses any operator in
    /// it once the statement parses (measured 2026-11-02 with `+`, `-`, `%`,
    /// `/` and `||`, between numbers and on a path). The walk meets the
    /// operator before the operands the current part has read (see [`walk`]).
    pub(super) fn refuse_condition_operator(&mut self, op: &str) {
        if self.in_where {
            self.operator_refusals += 1;
            self.walk_operator(format!(
                "Unsupported operator in Condition Expression. Operator: {op}"
            ));
        }
    }

    /// An operand, one part of the walk of a condition (see [`walk`]).
    pub(super) fn operand(&mut self) -> PResult<Operand> {
        let before = self.walk_part();
        let operand = self.nested(Nest::Operand, Self::operand_unnested);
        self.end_walk_part(before);
        operand
    }

    fn operand_unnested(&mut self) -> PResult<Operand> {
        let left = self.operand_atom()?;
        if matches!(self.peek().kind, TokenKind::Plus | TokenKind::Minus) {
            let op = if matches!(self.peek().kind, TokenKind::Plus) {
                ArithOp::Add
            } else {
                ArithOp::Sub
            };
            self.refuse_condition_operator(if op == ArithOp::Add { "+" } else { "-" });
            self.bump();
            self.require_rhs()?;
            let right = self.operand_atom()?;
            return Ok(Operand::Arith(Box::new(left), op, Box::new(right)));
        }
        // `*`, `/`, `%` and `||` in a condition read like `+` and `-`.
        // Elsewhere they are not measured: `/`, `%` and `||` keep the answer
        // they had when they did not lex, and `*` is left for the caller.
        let op = match self.peek().kind {
            TokenKind::Star if self.in_where => "*",
            TokenKind::Slash => "/",
            TokenKind::Percent => "%",
            TokenKind::Concat => "||",
            _ => return Ok(left),
        };
        if !self.in_where {
            return Err(ParseError::well_formed(""));
        }
        self.refuse_condition_operator(op);
        self.bump();
        self.require_rhs()?;
        self.operand_atom()?;
        Ok(left)
    }

    /// A number literal with any number of leading signs (`-7.3`, `+5`,
    /// `- .5`, `- -7`): the service folds the signs written before a number
    /// into the number. Returns `None`, consuming nothing, when no number
    /// follows the signs. The signs are read in a loop, so a long run costs no
    /// stack.
    pub(super) fn signed_number(&mut self) -> Option<String> {
        let start = self.pos;
        let mut negative = false;
        loop {
            match self.peek().kind {
                TokenKind::Minus => negative = !negative,
                TokenKind::Plus => {}
                TokenKind::Number(_) => break,
                _ => {
                    self.pos = start;
                    return None;
                }
            }
            self.bump();
        }
        let TokenKind::Number(n) = self.bump().kind else {
            unreachable!("the loop stops on a number");
        };
        if !negative {
            return Some(n);
        }
        Some(match n.strip_prefix('-') {
            Some(positive) => positive.to_owned(),
            None => format!("-{n}"),
        })
    }

    fn operand_atom(&mut self) -> PResult<Operand> {
        let at = self.span();
        if let Some(n) = self.signed_number() {
            self.walk_number(&n, LITERAL_ROOT);
            return self.literal_steps(Operand::Literal(AttributeValue::N(n)), &at);
        }
        // A sign before anything but a number is an operator with one
        // operand, refused once the statement parses (measured 2026-11-02:
        // `-n` in a condition names the operator, as a SET value it counts
        // the operands, and 4000 `+` before a name get the answer one does).
        // In a run of signs the one next to the operand is named.
        let mut sign = None;
        while let TokenKind::Plus | TokenKind::Minus = self.peek().kind {
            sign = Some(if matches!(self.peek().kind, TokenKind::Plus) {
                "+"
            } else {
                "-"
            });
            self.bump();
        }
        let Some(op) = sign else {
            return self.unsigned_atom(&at);
        };
        let at = self.span();
        // The sign is an operator of its own in the walk, met before its
        // operand.
        let before = self.walk_part();
        let inner = self.unsigned_atom(&at);
        if inner.is_ok() {
            if self.in_where {
                self.refuse_condition_operator(op);
            } else {
                self.defer_sign(&format!(
                    "Incorrect number of operands for operator or function; operator or function: {op}, number of operands: 1"
                ));
            }
        }
        self.end_walk_part(before);
        inner
    }

    /// An operand that does not start with a sign, at the span `at`.
    fn unsigned_atom(&mut self, at: &str) -> PResult<Operand> {
        match self.peek().kind.clone() {
            // A backtick literal parses and is refused at its position, by
            // its Ion type (measured 2026-11-02 with an integer).
            TokenKind::Ion(text) => {
                self.bump();
                self.defer(&format!(
                    "Ion Literals are not supported: {} under key root at {at}",
                    terms::ion_type_name(&text)
                ));
                Ok(Operand::Literal(AttributeValue::Null))
            }
            TokenKind::Param => {
                self.bump();
                let index = self.take_param_index();
                self.walk_parameter(index);
                Ok(Operand::Parameter(index))
            }
            TokenKind::String(s) => {
                self.bump();
                self.literal_steps(Operand::Literal(AttributeValue::S(s)), at)
            }
            TokenKind::LBrace => Ok(Operand::Literal(AttributeValue::M(
                self.map_literal(LITERAL_ROOT)?,
            ))),
            TokenKind::LBracket => Ok(Operand::Literal(AttributeValue::L(
                self.list_literal(LITERAL_ROOT)?,
            ))),
            TokenKind::LDoubleAngle => Ok(Operand::Literal(self.bag_literal(LITERAL_ROOT)?)),
            TokenKind::Ident(name) => {
                let lname = name.to_ascii_lowercase();
                let literal = match lname.as_str() {
                    "true" => Some(AttributeValue::Bool(true)),
                    "false" => Some(AttributeValue::Bool(false)),
                    "null" => Some(AttributeValue::Null),
                    _ => None,
                };
                if let Some(value) = literal {
                    self.bump();
                    return self.literal_steps(Operand::Literal(value), at);
                }
                // (measured 2026-11-02: as a comparison operand and as the
                // argument of attribute_exists, ahead of the table lookup).
                // The rest is read as a name, so a syntax error after it wins.
                if lname == "missing" && self.in_where {
                    self.defer("MISSING must be compared using IS operator");
                    return Ok(Operand::Path(self.path()?));
                }
                // function call vs path
                let call = matches!(
                    self.tokens.get(self.pos + 1).map(|t| &t.kind),
                    Some(TokenKind::LParen)
                );
                let class = word_class(&name);
                let callable = class == WordClass::Name
                    || (class == WordClass::Keyword && is_function_keyword(&name));
                if call && callable {
                    self.count_operator();
                    let at = self.span();
                    let refusal = if self.in_where {
                        forms::where_function_refusal(&lname, &at)
                    } else {
                        None
                    };
                    self.bump();
                    // In a condition an argument may hold a LIKE (see
                    // [`Parser::argument_with_like`]).
                    let argument: fn(&mut Self) -> PResult<Operand> = if self.in_where {
                        Self::argument_with_like
                    } else {
                        Self::operand
                    };
                    let args = self.arguments(argument)?;
                    // The walk meets a call's arguments before its name
                    // (measured 2026-11-02: `foo(1e126)` is refused for the
                    // number, `foo(1 + 1)` for the `+`).
                    if let Some(refusal) = refusal {
                        self.defer(&refusal);
                    }
                    Ok(Operand::Function(lname, args))
                } else if matches!(class, WordClass::Name | WordClass::Literal) {
                    // `missing` outside a condition is read as a name.
                    Ok(Operand::Path(self.path()?))
                } else {
                    self.reserved_word_term()
                }
            }
            TokenKind::QuotedIdent(_) => Ok(Operand::Path(self.path()?)),
            _ => Err(self.term_error()),
        }
    }

    /// Path steps after a literal: the statement parses, and the path is
    /// refused at the literal (measured 2026-11-02: `pk = 1.2.e3` is
    /// "Unexpected path component at" the `1.2`, and `1.2.` and `1.2..3` are
    /// "Invalid path dot component").
    fn literal_steps(&mut self, literal: Operand, at: &str) -> PResult<Operand> {
        if !matches!(self.peek().kind, TokenKind::Dot) {
            return Ok(literal);
        }
        while self.eat(&TokenKind::Dot) {
            match &self.peek().kind {
                TokenKind::QuotedIdent(_) => {}
                TokenKind::Ident(s) if word_class(s) == WordClass::Name => {}
                _ => return Err(ParseError::well_formed("Invalid path dot component")),
            }
            self.bump();
        }
        self.defer(&format!("Unexpected path component at {at}"));
        Ok(literal)
    }
}
