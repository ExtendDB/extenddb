// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The service's answers for a reserved word where a term starts, for the
//! first word of a select-list item, for a SET or REMOVE target that is not a
//! name, and for a statement that is a bare expression.
//!
//! The service reads a statement with a grammar wider than what DynamoDB
//! runs, so a reserved word where a term starts often begins a form of its own
//! (`CASE <expr>`, `DATE '...'`, `FROM <source> ... SET`, `EXEC <name> <args>`),
//! and the answer is the error that form meets at the next token: `WHERE case
//! = 1` is "Unexpected operator" for the `=`, `attribute_exists(case)` is
//! "Unexpected term" for the `)`. Every answer here was measured on
//! 2026-11-02 for 274 words in four positions: a condition (`WHERE <w> = 1`),
//! the argument of `attribute_exists(<w>)`, a SET target and a projection
//! (`SELECT <w> FROM ...`). Where such a form reads to its end, the answer is
//! not measured and the word keeps the term error of its class.

use super::{PResult, ParseError, Parser};
use crate::partiql::ast::{Operand, Path, Statement};
use crate::partiql::lexer::{TokenKind, WordClass, word_class};
use crate::partiql::parse_error;
use crate::types::AttributeValue;

/// The keywords that end a select list rather than start an item. In an item's
/// place they get a message of their own; every other reserved word gets the
/// term error.
const SELECT_LIST_KEYWORDS: [&str; 7] = [
    "delete", "from", "remove", "select", "set", "update", "where",
];

/// The calls that name the missing parenthesis in their message.
const PAREN_NAMED: [&str; 5] = [
    "can_cast",
    "can_lossless_cast",
    "cast",
    "coalesce",
    "nullif",
];

/// The calls whose missing parenthesis is "Expected LEFT_PAREN".
const PAREN_UNNAMED: [&str; 5] = ["date_add", "date_diff", "extract", "substring", "trim"];

const SIMPLE_PATH: &str = "Expected identifier for simple path";

/// How a bare expression statement is refused: by the kind of expression it is.
enum BareForm {
    /// An operator expression (`x = 1`, `NOT x`, `x IS NOT INT4`) or a call
    /// (`size(x)`): the service takes it for a condition check, which only a
    /// transaction runs.
    Operator,
    /// `x IS <type>`.
    IsType,
    /// A single term, named as the service names it (`Id`, `Lit`, ...).
    Term(&'static str),
}

impl Parser {
    pub(super) fn nth_kind(&self, n: usize) -> Option<&TokenKind> {
        self.tokens.get(self.pos + n).map(|t| &t.kind)
    }

    /// Whether the token after the current one is a name: an unquoted word
    /// that is not reserved, or a quoted identifier.
    pub(super) fn name_follows(&self) -> bool {
        match self.nth_kind(1) {
            Some(TokenKind::Ident(w)) => word_class(w) == WordClass::Name,
            Some(TokenKind::QuotedIdent(_)) => true,
            _ => false,
        }
    }

    /// The answer for a reserved word where a term starts, with the current
    /// token on the word. `Ok` only for `EXEC`, whose refusal is deferred to
    /// the end of the statement (see [`Parser::exec_term`]); the operand it
    /// returns stands in for the form and is never run.
    pub(super) fn reserved_word_term(&mut self) -> PResult<Operand> {
        let TokenKind::Ident(word) = &self.peek().kind else {
            return Err(self.term_error());
        };
        let word = word.to_ascii_lowercase();
        let fallback = self.term_error();
        let next = self.nth_kind(1).cloned();
        match word.as_str() {
            // A prefix and two forms that take an expression first.
            "not" | "case" | "pivot" => {
                self.bump();
                self.operand()?;
                Err(fallback)
            }
            // A query: its select list starts at the next token.
            "select" => {
                self.bump();
                if let Some(e) = self.select_list_keyword() {
                    return Err(e);
                }
                self.operand()?;
                Err(fallback)
            }
            // `FROM <source> [WHERE ...]` followed by a data manipulation.
            "from" => {
                self.bump();
                self.operand()?;
                if self.eat_keyword("WHERE") {
                    self.where_expr()?;
                }
                if self.is_keyword("SET") || self.is_keyword("REMOVE") {
                    return Err(fallback);
                }
                Err(ParseError::well_formed("Expected data manipulation"))
            }
            "exec" => self.exec_term(),
            "values" if next != Some(TokenKind::LParen) => Err(ParseError::well_formed(
                "Expected LEFT_PAREN for row value constructor",
            )),
            // These are calls when `(` follows; the call is read elsewhere.
            w if PAREN_NAMED.contains(&w) => Err(ParseError::well_formed(&format!(
                "Missing left parenthesis after {w}"
            ))),
            w if PAREN_UNNAMED.contains(&w) => Err(ParseError::well_formed("Expected LEFT_PAREN")),
            "date" => match next {
                Some(TokenKind::String(_)) => Err(fallback),
                _ => Err(ParseError::well_formed(
                    "Expected date string followed by the keyword DATE, found SYMBOL",
                )),
            },
            // A keyword after TIME must be WITH (`TIME WITH TIME ZONE '...'`);
            // anything else must be the time string.
            "time" => match next {
                Some(TokenKind::String(_)) => Err(fallback),
                Some(TokenKind::Ident(w)) if w.eq_ignore_ascii_case("with") => Err(fallback),
                Some(TokenKind::Ident(w)) if word_class(&w) == WordClass::Keyword => {
                    Err(ParseError::well_formed("Expected WITH keyword"))
                }
                _ => Err(ParseError::well_formed(
                    "Expected time string followed by the keyword TIME OR TIME WITH TIME ZONE, found SYMBOL",
                )),
            },
            "create" | "drop" => match next {
                Some(TokenKind::Ident(w))
                    if w.eq_ignore_ascii_case("table") || w.eq_ignore_ascii_case("index") =>
                {
                    Err(fallback)
                }
                _ => Err(ParseError::well_formed(&format!(
                    "Unexpected token following {}",
                    word.to_ascii_uppercase()
                ))),
            },
            "delete" => match next {
                Some(TokenKind::Ident(w)) if w.eq_ignore_ascii_case("from") => Err(fallback),
                _ => Err(ParseError::well_formed("Expected FROM after DELETE")),
            },
            "set" | "remove" | "update" if !self.name_follows() => {
                Err(ParseError::well_formed(SIMPLE_PATH))
            }
            _ => Err(fallback),
        }
    }

    /// `EXEC <name> [<argument>, ...]` where a term starts. Any token is read as
    /// the procedure's name, then the arguments while terms follow. The refusal
    /// names the token read as the name and waits for the end of the
    /// statement, so a syntax error after the form wins (measured 2026-11-02:
    /// `WHERE exec = 1` is "Keyword = only expected at the top level in the
    /// query", `attribute_exists(exec)` is "Expected RIGHT_PAREN" and
    /// `SELECT exec FROM "t"` is "Expected FROM after SELECT list").
    fn exec_term(&mut self) -> PResult<Operand> {
        self.bump();
        if matches!(self.peek().kind, TokenKind::Eof) {
            return Err(self.term_error());
        }
        let name = token_text(&self.bump().kind);
        if self.at_term_start() {
            loop {
                self.operand()?;
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        self.defer(&parse_error(&format!(
            "Keyword {name} only expected at the top level in the query"
        )));
        Ok(Operand::Literal(AttributeValue::Null))
    }

    /// Whether the current token can start a term.
    fn at_term_start(&self) -> bool {
        match &self.peek().kind {
            TokenKind::Ident(w) => word_class(w) == WordClass::Name,
            TokenKind::QuotedIdent(_)
            | TokenKind::String(_)
            | TokenKind::Number(_)
            | TokenKind::Ion(_)
            | TokenKind::Param
            | TokenKind::LParen
            | TokenKind::LBracket
            | TokenKind::LBrace
            | TokenKind::LDoubleAngle
            | TokenKind::Plus
            | TokenKind::Minus => true,
            _ => false,
        }
    }

    /// The refusal of a keyword that ends a select list where an item should
    /// start (measured 2026-11-02 with FROM, SELECT, DELETE, WHERE, SET, UPDATE
    /// and REMOVE, and with FROM after `SELECT ALL` and `SELECT DISTINCT`).
    pub(super) fn select_list_keyword(&self) -> Option<ParseError> {
        let TokenKind::Ident(word) = &self.peek().kind else {
            return None;
        };
        let word = word.to_ascii_lowercase();
        SELECT_LIST_KEYWORDS.contains(&word.as_str()).then(|| {
            ParseError::well_formed(&format!(
                "Expected identifier or an expression but found unexpected keyword '{word}' in a select list."
            ))
        })
    }

    /// A SET or REMOVE target, or the target of an UPDATE or DELETE: a path
    /// whose first step is a name. Anything else there, a reserved word or a
    /// literal included, is "Expected identifier for simple path" (measured
    /// 2026-11-02 for every reserved word as a SET target and as a DELETE
    /// table).
    pub(super) fn simple_path(&mut self) -> PResult<Path> {
        let is_name = match &self.peek().kind {
            TokenKind::Ident(w) => word_class(w) == WordClass::Name,
            TokenKind::QuotedIdent(_) => true,
            _ => false,
        };
        if !is_name {
            return Err(ParseError::well_formed(SIMPLE_PATH));
        }
        self.path()
    }

    /// A statement that is an expression rather than a query or a write. It
    /// parses, then is refused by what it is (measured 2026-11-02): `x` is
    /// "Unsupported operation: Id", `1` is "Unsupported operation: Lit",
    /// `x IS <type>` is "Unsupported operation: IsType", and an operator
    /// expression (`x = 1`, `NOT x`, `x IS NOT INT4`) or a call (`size(x)`) is
    /// taken for a condition check, which only a transaction runs. A trailing
    /// semicolon is accepted.
    pub(super) fn bare_statement(&mut self) -> PResult<Statement> {
        let form = self.bare_expression()?;
        self.eat_final_semicolon();
        if !matches!(self.peek().kind, TokenKind::Eof) {
            return Err(self.trailing_error());
        }
        // The checks of a statement that would run do not apply to one that
        // is refused by its form.
        self.forget_findings();
        Err(ParseError::new(match form {
            BareForm::Operator => super::EXISTS_OUTSIDE_TRANSACTION.to_owned(),
            BareForm::IsType => "Unsupported operation: IsType".to_owned(),
            BareForm::Term(name) => format!("Unsupported operation: {name}"),
        }))
    }

    fn bare_expression(&mut self) -> PResult<BareForm> {
        let mut form = self.bare_unit()?;
        while self.eat_keyword("AND") || self.eat_keyword("OR") {
            self.require_rhs()?;
            self.bare_unit()?;
            form = BareForm::Operator;
        }
        Ok(form)
    }

    fn bare_unit(&mut self) -> PResult<BareForm> {
        // A run of NOTs is read in a loop, so its length costs no stack.
        let mut negated = false;
        while self.eat_keyword("NOT") {
            negated = true;
        }
        let form = self.bare_term()?;
        Ok(if negated { BareForm::Operator } else { form })
    }

    fn bare_term(&mut self) -> PResult<BareForm> {
        let operand = self.operand()?;
        // `*` outside a condition is left to the caller (see `operand`).
        if self.eat(&TokenKind::Star) {
            self.require_rhs()?;
            self.operand()?;
            return Ok(BareForm::Operator);
        }
        if self.eat_keyword("IS") {
            let negated = self.eat_keyword("NOT");
            self.type_name()?;
            return Ok(if negated {
                BareForm::Operator
            } else {
                BareForm::IsType
            });
        }
        let comparison = matches!(
            self.peek().kind,
            TokenKind::Eq
                | TokenKind::Ne
                | TokenKind::Lt
                | TokenKind::Le
                | TokenKind::Gt
                | TokenKind::Ge
        );
        if comparison || self.is_keyword("LIKE") || self.is_keyword("IN") {
            self.bump();
            self.require_rhs()?;
            self.operand()?;
            return Ok(BareForm::Operator);
        }
        if self.eat_keyword("BETWEEN") {
            self.require_rhs()?;
            self.operand()?;
            if !self.eat_keyword("AND") {
                return Err(ParseError::well_formed("Expected AND keyword"));
            }
            self.operand()?;
            return Ok(BareForm::Operator);
        }
        Ok(match operand {
            Operand::Path(path) if path.steps.is_empty() => BareForm::Term("Id"),
            Operand::Path(_) => BareForm::Term("Path"),
            Operand::Literal(AttributeValue::M(_)) => BareForm::Term("Struct"),
            Operand::Literal(AttributeValue::L(_)) => BareForm::Term("List"),
            Operand::Literal(AttributeValue::SS(_) | AttributeValue::NS(_)) => {
                BareForm::Term("Bag")
            }
            Operand::Literal(_) => BareForm::Term("Lit"),
            Operand::Parameter(_) => BareForm::Term("Parameter"),
            // A call is taken for a condition check like an operator
            // expression (measured 2026-11-02 with `size(x)`).
            Operand::Function(..) | Operand::Arith(..) => BareForm::Operator,
        })
    }

    /// Consume a semicolon that ends the statement: the service accepts one
    /// (measured 2026-11-02 after a SELECT, an UPDATE and a bare `IS` test).
    pub(super) fn eat_final_semicolon(&mut self) {
        if matches!(self.peek().kind, TokenKind::Semicolon)
            && matches!(self.nth_kind(1), Some(TokenKind::Eof))
        {
            self.bump();
        }
    }
}

/// The text of a token as a message names it.
fn token_text(kind: &TokenKind) -> String {
    match kind {
        TokenKind::Ident(s)
        | TokenKind::QuotedIdent(s)
        | TokenKind::String(s)
        | TokenKind::Number(s)
        | TokenKind::Ion(s) => s.clone(),
        TokenKind::Param => "?".to_owned(),
        TokenKind::Comma => ",".to_owned(),
        TokenKind::Dot => ".".to_owned(),
        TokenKind::Star => "*".to_owned(),
        TokenKind::LParen => "(".to_owned(),
        TokenKind::RParen => ")".to_owned(),
        TokenKind::LBracket => "[".to_owned(),
        TokenKind::RBracket => "]".to_owned(),
        TokenKind::LDoubleAngle => "<<".to_owned(),
        TokenKind::RDoubleAngle => ">>".to_owned(),
        TokenKind::LBrace => "{".to_owned(),
        TokenKind::RBrace => "}".to_owned(),
        TokenKind::Colon => ":".to_owned(),
        TokenKind::Eq => "=".to_owned(),
        TokenKind::Ne => "<>".to_owned(),
        TokenKind::Lt => "<".to_owned(),
        TokenKind::Le => "<=".to_owned(),
        TokenKind::Gt => ">".to_owned(),
        TokenKind::Ge => ">=".to_owned(),
        TokenKind::Plus => "+".to_owned(),
        TokenKind::Minus => "-".to_owned(),
        TokenKind::Percent => "%".to_owned(),
        TokenKind::Slash => "/".to_owned(),
        TokenKind::Concat => "||".to_owned(),
        TokenKind::Semicolon => ";".to_owned(),
        TokenKind::Eof => String::new(),
    }
}

/// The Ion type name of a backtick literal's text, as the service names it
/// when it refuses the literal (measured 2026-11-02 for an integer, INT).
pub(super) fn ion_type_name(text: &str) -> &'static str {
    let t = text.trim();
    let numeric = |s: &str| {
        let s = s.strip_prefix('-').unwrap_or(s);
        !s.is_empty() && s.chars().all(|c| c.is_ascii_digit() || c == '_')
    };
    if numeric(t) {
        "INT"
    } else if t.starts_with("{{") {
        "BLOB"
    } else if t.starts_with('{') {
        "STRUCT"
    } else if t.starts_with('[') {
        "LIST"
    } else if t.starts_with('(') {
        "SEXP"
    } else if t.starts_with('"') {
        "STRING"
    } else if t == "true" || t == "false" {
        "BOOL"
    } else if t.starts_with("null") {
        "NULL"
    } else if t.contains(['e', 'E'])
        && t.chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit() || c == '-')
    {
        "FLOAT"
    } else if t
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_digit() || c == '-')
    {
        "DECIMAL"
    } else {
        "SYMBOL"
    }
}

#[cfg(test)]
mod tests;
