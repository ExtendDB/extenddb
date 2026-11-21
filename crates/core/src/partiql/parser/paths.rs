// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Paths: the steps of a path and what brackets in a path hold.

use super::limits::Nest;
use super::{PResult, ParseError, Parser};
use crate::partiql::ast::{Path, PathStep};
use crate::partiql::lexer::{TokenKind, WordClass, word_class};

/// The syntax error of a SET or REMOVE target whose brackets hold a path or
/// a parameter (measured 2026-11-06).
const NOT_A_SIMPLE_PATH: &str = "Invalid path component for simple path";

/// How what brackets in a path hold is answered, by where the path is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Brackets {
    /// In a condition, a value, the select list and the arguments of a call:
    /// a content that is not a list index or a map member is refused once
    /// the statement parses, at its first token.
    Refused,
    /// In a SET or REMOVE target: as [`Brackets::Refused`], except that
    /// brackets that hold a path, a parameter or an expression are a syntax
    /// error (measured 2026-11-06: `SET m[k.j] = 1`, `m[k[0]]`, `m['k'.x]`,
    /// `m[?]`, `m[[1]]`, `m[{'a': 1}]`, `m[<<1>>]`, `m[*]`, `m[- k]`, `m[NOT
    /// a]`, `m[f(x)]`, `m[a = 1]` and `m[k + 1]`, and the same REMOVEs; see
    /// [`Parser::expression_in_a_target_here`]). The error comes once the
    /// brackets close, ahead of anything after them in the statement
    /// (measured 2026-11-06: with the SET value missing, with `=` missing,
    /// with a bracket left open in a later target and with a WHERE that
    /// ends in `=`), and ahead of a refusal found before the target (an
    /// alias on the table, a number in an earlier SET value).
    Target,
    /// In an ORDER BY term: nothing in brackets is refused, only read as an
    /// expression (see [`Parser::expression_in_brackets`]), and the engine
    /// refuses the path for its steps (measured 2026-11-06: `ORDER BY m[?]`,
    /// `m[1.5]`, `m['']`, `m[-1]`, `m[true]`, `m[MISSING]`, `m[(1)]`, `m[*]`,
    /// `m[f(x)]`, `m[k + 1]`, `m[[1]]`, `m[<<1>>]`, ``m[`1`]``, `m[{'a':
    /// 1}]`, `m[k.j]` and `m[k[0]]` get "Argument to ORDER BY must be of
    /// type VariableReference", and with no WHERE the refusal of that).
    OrderBy,
    /// Inside an inner bracket, that is a bracket of a path that is itself
    /// in brackets: the path in the outer brackets is refused first, so
    /// what an inner bracket holds is only read, as an expression (see
    /// [`Parser::expression_in_brackets`]).
    Inner,
}

impl Parser {
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
                let (step, in_a_target) = self.bracket_step()?;
                // (measured 2026-11-05: `m['k'`, `m[k`, `m['k' 'j']` and `m[k
                // j]`; a bracket left open wins over a refusal of what it
                // holds, as `m[-1` and `m[k.j` show)
                if !self.eat(&TokenKind::RBracket) {
                    return Err(ParseError::well_formed("Expected RIGHT_BRACKET"));
                }
                if !in_a_target && self.brackets == Brackets::Target {
                    return Err(ParseError::well_formed(NOT_A_SIMPLE_PATH));
                }
                path.steps.push(step);
            } else {
                break;
            }
        }
        Ok(path)
    }

    /// What a pair of brackets after a path holds, with the current token
    /// after the `[`, and whether a SET or REMOVE target may hold it (see
    /// [`Brackets::Target`]). A number is a list index. A string, a quoted
    /// name or a name is a map member, as after a dot (measured 2026-11-05:
    /// `m['k']`, `m["k"]` and `m[k]` read as `m.k` in a condition, a SET
    /// target, a REMOVE, a SET value and the select list, and `m['my key']`
    /// names a member a dot cannot). A path is refused at its first token,
    /// however deep its own brackets nest (see [`Parser::path_in_brackets`]).
    /// A reserved word there is a syntax error. Anything else parses and is
    /// refused once the statement parses, at its first token (measured
    /// 2026-11-05: `m[?]`, `m[1.5]`, `m[true]` and `m[-1.5]`, at the
    /// number); a minus before a whole number is refused as an index out of
    /// range, and empty brackets are a syntax error (measured 2026-11-06:
    /// `m[]` in a condition, a SET target and ORDER BY). The step returned
    /// for a refused content stands for it and is never used. In an inner
    /// bracket and in an ORDER BY term the content is only read (see
    /// [`Brackets`]).
    fn bracket_step(&mut self) -> PResult<(PathStep, bool)> {
        let at = self.span();
        if self.path_starts_here() {
            self.bump();
            self.path_in_brackets(&at)?;
            return Ok((PathStep::Index(0), false));
        }
        match self.brackets {
            Brackets::Inner | Brackets::OrderBy => {
                self.inner_bracket()?;
                return Ok((PathStep::Index(0), true));
            }
            Brackets::Target if self.expression_in_a_target_here() => {
                self.inner_bracket()?;
                return Ok((PathStep::Index(0), false));
            }
            Brackets::Refused | Brackets::Target => {}
        }
        match self.peek().kind.clone() {
            TokenKind::Number(n) => {
                self.bump();
                match n.parse::<usize>() {
                    Ok(index) => Ok((PathStep::Index(index), true)),
                    Err(_) => Ok(self.refuse_bracket(&at)),
                }
            }
            TokenKind::String(name) | TokenKind::QuotedIdent(name) => {
                self.bump();
                if name.is_empty() {
                    self.defer_bracket("Path component cannot be an empty string");
                }
                Ok((PathStep::Attribute(name), true))
            }
            TokenKind::Ident(word) => match word_class(&word) {
                WordClass::Name => {
                    self.bump();
                    Ok((PathStep::Attribute(word), true))
                }
                WordClass::Literal => {
                    self.bump();
                    Ok(self.refuse_bracket(&at))
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
                    self.defer_bracket(&format!(
                        "List index is not within the allowable range; index: [-{n}] at {at}"
                    ));
                    Ok((PathStep::Index(0), true))
                } else {
                    Ok(self.refuse_bracket(&at))
                }
            }
            TokenKind::Param => {
                self.bump();
                self.take_param_index();
                let (step, _) = self.refuse_bracket(&at);
                Ok((step, false))
            }
            TokenKind::RBracket => Err(ParseError::well_formed("Unexpected term")),
            _ => Err(ParseError::new(format!(
                "Unexpected path component at {at}"
            ))),
        }
    }

    /// Whether what the brackets of a SET or REMOVE target hold, from the
    /// current token, is an expression the target may not hold (see
    /// [`Brackets::Target`]): a list, a map, a bag, a wildcard, a sign or an
    /// operator word before a value, or a name, a string, a number, a
    /// literal word or a parameter with more of an expression after it
    /// (measured 2026-11-06: `m['k' + 1]` and `m[1 + 1]`).
    fn expression_in_a_target_here(&self) -> bool {
        let closes = matches!(self.nth_kind(1), Some(TokenKind::RBracket));
        match &self.peek().kind {
            TokenKind::LBracket | TokenKind::LBrace | TokenKind::LDoubleAngle | TokenKind::Star => {
                true
            }
            TokenKind::Minus => !matches!(self.nth_kind(1), Some(TokenKind::Number(_))),
            TokenKind::String(_)
            | TokenKind::QuotedIdent(_)
            | TokenKind::Number(_)
            | TokenKind::Param => !closes,
            TokenKind::Ident(word) => match word_class(word) {
                WordClass::Operator => true,
                WordClass::Name | WordClass::Literal => !closes,
                WordClass::Keyword | WordClass::Special => false,
            },
            _ => false,
        }
    }

    /// Refuse what brackets hold, at the span `at`, once the statement
    /// parses. The step returned stands for it and is never used.
    fn refuse_bracket(&mut self, at: &str) -> (PathStep, bool) {
        self.defer_bracket(&format!("Unexpected path component at {at}"));
        (PathStep::Index(0), true)
    }

    /// Keep the refusal of what brackets hold, except in an ORDER BY term
    /// (see [`Brackets::OrderBy`]).
    fn defer_bracket(&mut self, refusal: &str) {
        if self.brackets != Brackets::OrderBy {
            self.defer(refusal);
        }
    }

    /// Whether a path starts at the current token: a name, a string or a
    /// quoted name with a dot or a bracket after it.
    fn path_starts_here(&self) -> bool {
        let first = match &self.peek().kind {
            TokenKind::String(_) | TokenKind::QuotedIdent(_) => true,
            TokenKind::Ident(word) => word_class(word) == WordClass::Name,
            _ => false,
        };
        first && matches!(self.nth_kind(1), Some(TokenKind::Dot | TokenKind::LBracket))
    }

    /// A path in brackets, with its first component read, at the span
    /// `at`. The service refuses it at that component, whatever follows and
    /// however deep the brackets of the path nest (measured 2026-11-05 and
    /// 2026-11-06: `m[k.j]`, `m[k[0]]`, `m['k'.x]`, `m[k.*]`, `m[k['']]`,
    /// `m[k[-1]]`, `m[k[?]]`, `m[k[1 + 1]]`, `m[''[k]]` and `m[` followed by
    /// 1, 10, 1000 and 2700 levels of `k[` are refused at the first token in
    /// the outer brackets, in a condition, a SET value, the select list and
    /// the arguments of a call). So the refusal is kept first, and the rest
    /// of the path is read without being built (see
    /// [`Parser::rest_of_path_in_brackets`]).
    fn path_in_brackets(&mut self, at: &str) -> PResult<()> {
        self.refuse_bracket(at);
        self.rest_of_path_in_brackets()
    }

    /// The rest of a path in brackets after its first component: its steps,
    /// and those of the paths its own brackets hold, read in a loop rather
    /// than by recursion, so that how deep the brackets nest costs no stack.
    /// `paths` counts the brackets whose path is being read. What an inner
    /// bracket holds that is not a path, and what follows a path in an inner
    /// bracket, are read as an expression (see
    /// [`Parser::expression_in_brackets`]); so is what follows the path
    /// itself where brackets are only read (see [`Brackets`]).
    fn rest_of_path_in_brackets(&mut self) -> PResult<()> {
        let inner = self.brackets != Brackets::Refused;
        let mut paths = 0usize;
        loop {
            if self.eat(&TokenKind::Dot) {
                // (measured 2026-11-06: `m[k.*]` is refused at the `k`, and
                // `m[k.select]` is "Invalid path dot component")
                match &self.peek().kind {
                    TokenKind::QuotedIdent(_) | TokenKind::Star => {}
                    TokenKind::Ident(word) if word_class(word) == WordClass::Name => {}
                    _ => return Err(ParseError::well_formed("Invalid path dot component")),
                }
                self.bump();
            } else if self.eat(&TokenKind::LBracket) {
                if self.path_starts_here() {
                    self.bump();
                    paths += 1;
                    continue;
                }
                self.inner_bracket()?;
                if !self.eat(&TokenKind::RBracket) {
                    return Err(ParseError::well_formed("Expected RIGHT_BRACKET"));
                }
            } else if paths > 0 || inner {
                if !matches!(self.peek().kind, TokenKind::RBracket) {
                    self.expression_in_brackets(true)?;
                }
                if paths == 0 {
                    return Ok(());
                }
                if !self.eat(&TokenKind::RBracket) {
                    return Err(ParseError::well_formed("Expected RIGHT_BRACKET"));
                }
                paths -= 1;
            } else {
                return Ok(());
            }
        }
    }

    /// What an inner bracket holds that is not a path: a wildcard (measured
    /// 2026-11-06: `m[k[*]]` is refused at the `k`) or an expression.
    fn inner_bracket(&mut self) -> PResult<()> {
        if self.eat(&TokenKind::Star) {
            return Ok(());
        }
        self.expression_in_brackets(false)
    }

    /// What brackets hold, read as the service reads it, as an expression,
    /// which may be a condition, where nothing in it is refused: in an inner
    /// bracket, where the path around it is refused first, in an ORDER BY
    /// term and in a target that may not hold it (see [`Brackets`]).
    /// Measured 2026-11-06: in `m[k[...]]`, a call, a list, a bag, a map, a
    /// sign, `a = 1`, `a AND b`, `NOT a`, `a BETWEEN 1 AND 2`, `a IN [1]`, `a
    /// IS NULL`, `a LIKE 'x'`, `1 + 1`, `2 * 3`, `'a' || 'b'` and the same
    /// after a path with steps (`j.x = 1`, `j[0] + 1`, `j.x IS MISSING`) are
    /// refused at the `k`; `'a' 'b'`, `1 2`, `1, 2`, `j.x y` and `j.x (y)`
    /// are "Expected RIGHT_BRACKET"; `(`, `)`, `,`, `;`, CASE, FROM, `j.x =`
    /// and nothing are "Unexpected term". So only a syntax error in the
    /// expression is an answer: what the expression's walk finds is dropped,
    /// and the state the parse keeps for the statement is put back.
    /// `after_path` is set when the expression starts with a path that has
    /// been read (see [`Parser::condition_after_path`]). The expression
    /// nests as an operand does.
    fn expression_in_brackets(&mut self, after_path: bool) -> PResult<()> {
        let in_where = std::mem::replace(&mut self.in_where, true);
        let brackets = std::mem::replace(&mut self.brackets, Brackets::Inner);
        let groups = self.bracket_groups.replace(self.nesting.groups());
        let read = self.read_and_drop(Nest::Operand, |p| {
            if after_path {
                p.condition_after_path()
            } else {
                p.or_expr().map(drop)
            }
        });
        self.in_where = in_where;
        self.brackets = brackets;
        self.bracket_groups = groups;
        read
    }
}
