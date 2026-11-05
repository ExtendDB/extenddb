// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Paths and operands: the signs and arithmetic of an operand and the term it starts with.

use super::limits::Nest;
use super::{LITERAL_ROOT, PResult, ParseError, Parser, forms, terms};
use crate::partiql::ast::{ArithOp, Operand, Path, PathStep};
use crate::partiql::lexer::{TokenKind, WordClass, is_function_keyword, word_class};
use crate::types::AttributeValue;

/// A number literal with the signs written before it folded in (see
/// [`Parser::signed_number`]).
pub(super) struct SignedNumber {
    /// The number's text, with a `-` when the signs negate it.
    pub text: String,
    /// The `line:column:length` span of the number itself.
    pub at: String,
    /// Whether the number is a zero written with a decimal point or an
    /// exponent and a minus sign is among the signs before it (see
    /// `number::is_zero_decimal`).
    pub negative_zero: bool,
}

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
        self.path_steps(Path {
            root,
            steps: Vec::new(),
        })
    }

    /// The steps after `path`: `.name` and what brackets hold (see
    /// [`Parser::bracket_step`]), as many as follow.
    pub(super) fn path_steps(&mut self, mut path: Path) -> PResult<Path> {
        loop {
            if self.eat(&TokenKind::Dot) {
                match &self.peek().kind {
                    TokenKind::QuotedIdent(s) => {
                        let s = s.clone();
                        self.bump();
                        self.refuse_empty_component(&s);
                        path.steps.push(PathStep::Attribute(s));
                    }
                    TokenKind::Ident(s) if word_class(s) == WordClass::Name => {
                        let s = s.clone();
                        self.bump();
                        path.steps.push(PathStep::Attribute(s));
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
                let step = self.bracket_step()?;
                // (measured 2026-11-05: `m['k'`, `m[k`, `m['k' 'j']` and `m[k
                // j]`; a bracket left open wins over a refusal of what it
                // holds, as `m[-1` and `m[k.j` show)
                if !self.eat(&TokenKind::RBracket) {
                    return Err(ParseError::well_formed("Expected RIGHT_BRACKET"));
                }
                path.steps.push(step);
            } else {
                break;
            }
        }
        Ok(path)
    }

    /// What a pair of brackets after a path holds, with the current token
    /// after the `[`. A number is a list index. A string, a quoted name or
    /// a name is a map member, as after a dot (measured 2026-11-05: `m['k']`,
    /// `m["k"]` and `m[k]` read as `m.k` in a condition, a SET target, a
    /// REMOVE, a SET value and the select list, and `m['my key']` names a
    /// member a dot cannot). A reserved word there is a syntax error. Anything
    /// else parses and is refused once the statement parses, at its first
    /// token (measured 2026-11-05: `m[?]`, `m[1.5]`, `m[true]`, `m[k.j]`,
    /// `m['k'.x]` and `m[-1.5]`, at the number); a minus before a whole
    /// number is refused as an index out of range. The step returned for a
    /// refused content stands for it and is never used.
    fn bracket_step(&mut self) -> PResult<PathStep> {
        let at = self.span();
        let refuse = |p: &mut Self, at: &str| {
            p.defer(&format!("Unexpected path component at {at}"));
            Ok(PathStep::Index(0))
        };
        match self.peek().kind.clone() {
            TokenKind::Number(n) => {
                self.bump();
                match n.parse::<usize>() {
                    Ok(index) => Ok(PathStep::Index(index)),
                    Err(_) => refuse(self, &at),
                }
            }
            TokenKind::String(name) | TokenKind::QuotedIdent(name) => {
                if matches!(self.nth_kind(1), Some(TokenKind::Dot | TokenKind::LBracket)) {
                    self.bump();
                    self.path_steps(Path {
                        root: name,
                        steps: Vec::new(),
                    })?;
                    return refuse(self, &at);
                }
                self.bump();
                self.refuse_empty_component(&name);
                Ok(PathStep::Attribute(name))
            }
            TokenKind::Ident(word) => match word_class(&word) {
                WordClass::Name => {
                    if matches!(self.nth_kind(1), Some(TokenKind::Dot | TokenKind::LBracket)) {
                        self.path()?;
                        return refuse(self, &at);
                    }
                    self.bump();
                    Ok(PathStep::Attribute(word))
                }
                WordClass::Literal => {
                    self.bump();
                    refuse(self, &at)
                }
                WordClass::Operator => Err(ParseError::well_formed("Unexpected operator")),
                WordClass::Keyword | WordClass::Special => {
                    Err(ParseError::well_formed("Unexpected term"))
                }
            },
            TokenKind::Minus if matches!(self.nth_kind(1), Some(TokenKind::Number(_))) => {
                self.bump();
                let at = self.span();
                let TokenKind::Number(n) = self.bump().kind else {
                    unreachable!("a number follows the minus");
                };
                if n.bytes().all(|b| b.is_ascii_digit()) {
                    self.defer(&format!(
                        "List index is not within the allowable range; index: [-{n}] at {at}"
                    ));
                    Ok(PathStep::Index(0))
                } else {
                    refuse(self, &at)
                }
            }
            TokenKind::Param => {
                self.bump();
                self.take_param_index();
                refuse(self, &at)
            }
            _ => Err(ParseError::new(format!(
                "Unexpected path component at {at}"
            ))),
        }
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
    /// `- .5`, `- -7`), and in any number of parentheses with signs before
    /// and inside them (measured 2026-11-05: `-(5)` and `- (5)` are -5,
    /// `-(-(5))` is 5, `(-0)` is 0): the service folds the signs into the
    /// number. Returns `None`, consuming nothing, when no number follows the
    /// signs and parentheses or the parentheses do not close right after it.
    /// The run is read in a loop, so its length costs no stack.
    pub(super) fn signed_number(&mut self) -> Option<SignedNumber> {
        let start = self.pos;
        let mut negative = false;
        let mut minus = false;
        let mut open = 0usize;
        loop {
            match self.peek().kind {
                TokenKind::Minus => {
                    negative = !negative;
                    minus = true;
                }
                TokenKind::Plus => {}
                TokenKind::LParen => open += 1,
                TokenKind::Number(_) => break,
                _ => {
                    self.pos = start;
                    return None;
                }
            }
            self.bump();
        }
        let at = self.span();
        let TokenKind::Number(n) = self.bump().kind else {
            unreachable!("the loop stops on a number");
        };
        for _ in 0..open {
            if !self.eat(&TokenKind::RParen) {
                self.pos = start;
                return None;
            }
        }
        let negative_zero = minus && crate::partiql::number::is_zero_decimal(&n);
        let text = if negative {
            match n.strip_prefix('-') {
                Some(positive) => positive.to_owned(),
                None => format!("-{n}"),
            }
        } else {
            n
        };
        Some(SignedNumber {
            text,
            at,
            negative_zero,
        })
    }

    fn operand_atom(&mut self) -> PResult<Operand> {
        if let Some(number) = self.signed_number() {
            self.note_number(&number, LITERAL_ROOT);
            return self
                .literal_steps(Operand::Literal(AttributeValue::N(number.text)), &number.at);
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
        let at = self.span();
        let Some(op) = sign else {
            return self.unsigned_atom(&at);
        };
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

    /// An operand in parentheses, with the current token on the `(`. The
    /// parentheses only group what they hold (measured 2026-11-05: `pk =
    /// ('a')`, `pk = (?)`, `SET x = (a + 1)`, `SET x = (a) + 1` and
    /// `begins_with((sk), 'b')` run as written without them), a path in them
    /// takes the steps after them (measured 2026-11-05: `(m).k = 1` and
    /// `(m)['k'] = 1` read `m.k`), and a literal followed by steps is
    /// refused at the literal. A comma of their own makes a list (measured
    /// 2026-11-05: `SET x = ('a', 'b')` stores a list).
    fn operand_in_parentheses(&mut self) -> PResult<Operand> {
        if self.list_in_parentheses_ahead() {
            let list = self.list_in_parentheses(LITERAL_ROOT)?;
            return Ok(Operand::Literal(AttributeValue::L(list)));
        }
        let open = self.pos;
        self.bump();
        let inner = self.nested(Nest::Group, Self::operand)?;
        // (measured 2026-11-05: `a = (1` and `pk = ('a'`)
        if !self.eat(&TokenKind::RParen) {
            return Err(ParseError::well_formed("Expected RIGHT_PAREN"));
        }
        self.steps_after_group(inner, open)
    }

    /// The `line:column:length` span of the first token inside the group
    /// that opens at token `open`, past any `(` of groups inside it.
    pub(super) fn inner_span(&self, open: usize) -> String {
        self.tokens[open..]
            .iter()
            .find(|t| t.kind != TokenKind::LParen)
            .map(|t| format!("{}:{}:{}", t.line, t.column, t.length))
            .unwrap_or_default()
    }

    /// The operand a group that opens at token `open` holds, with the path
    /// steps written after the group: a path takes them (measured
    /// 2026-11-05: `(m).k` and `(m)['k']` read `m.k`), and a literal is
    /// refused for them at its first token (measured 2026-11-05: `('x').y`
    /// names the `'x'`).
    pub(super) fn steps_after_group(&mut self, operand: Operand, open: usize) -> PResult<Operand> {
        match operand {
            Operand::Path(path) => Ok(Operand::Path(self.path_steps(path)?)),
            literal @ Operand::Literal(_) => {
                let at = self.inner_span(open);
                self.literal_steps(literal, &at)
            }
            other => Ok(other),
        }
    }

    /// An operand that does not start with a sign, at the span `at`.
    fn unsigned_atom(&mut self, at: &str) -> PResult<Operand> {
        match self.peek().kind.clone() {
            TokenKind::LParen => self.operand_in_parentheses(),
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
