// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL parser: tokens -> [`ast::Statement`].
//!
//! Recursive descent. Error messages reproduce the service's
//! `ValidationException` strings verbatim, as measured; a parse failure
//! carries the full message the engine will surface.

use super::ast::Statement;
use super::lexer::{Token, TokenKind, WordClass, lex, word_class};
use super::{PARSE_ERROR_PREFIX, parse_error};

mod condition;
mod forms;
mod limits;
mod literals;
mod operands;
mod predicates;
mod statements;
mod terms;
mod walk;

pub use limits::{MAX_STATEMENT_LENGTH, statement_length_refusal};
use limits::{Nest, Nesting};
use walk::Findings;

/// A parse failure. `message` is the full `ValidationException` string the
/// service returns, ready to hand to `DynamoDbError::ValidationException`.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
}

impl ParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
    fn well_formed(reason: &str) -> Self {
        Self {
            message: parse_error(reason),
        }
    }

    /// Whether this is a syntax failure (the "wasn't well formed" envelope)
    /// rather than a refusal of a statement that parsed.
    fn is_syntax(&self) -> bool {
        self.message.starts_with(PARSE_ERROR_PREFIX)
    }
}

type PResult<T> = Result<T, ParseError>;

/// The refusal of a table alias (measured 2026-11-02 on SELECT, UPDATE and
/// DELETE, with and without `AS`).
const ALIASING: &str = "Aliasing is not supported";

const MISSING_RHS: &str = "Missing right-hand side expression of infix operator";

/// The refusal of a bag member that is neither a number nor a string.
const BAG_MEMBER_TYPES: &str =
    "Unsupported data type in Bag. DynamoDB only supports either numbers or strings in bags";

/// How the service names a literal value itself in a refusal of a part of it.
const LITERAL_ROOT: &str = "root";

/// The refusal of a statement with more `?` placeholders than the request
/// gives parameters.
pub const PARAMETER_COUNT_MISMATCH: &str =
    "Number of parameters in request and statement don't match.";

/// The refusal of a statement whose `IN` has no list after it, or an empty
/// one.
const IN_OPERANDS: &str = "IN operator must have a left hand argument of type Variable Reference \
     and right hand argument of type Seq with at least one member";

/// The refusal of an IS test whose left side is not a name, a literal or a
/// path.
const IS_LEFT_SIDE: &str =
    "The left hand side argument to IS must be a Variable Reference, Literal, or Path";

/// The refusal of a condition check outside an ExecuteTransaction: a bare
/// expression statement, and `EXISTS(SELECT ...)` in ExecuteStatement.
pub const EXISTS_OUTSIDE_TRANSACTION: &str =
    "EXISTS can only be used in ExecuteTransaction write requests.";

/// The refusal of an operand that is the whole WHERE condition (measured
/// 2026-11-02 with a name, a path, a number, a string, a list, `true` and a
/// parameter).
const NOT_A_CONDITION: &str = "WHERE clause must contain an expression that resolves to a boolean";

/// The refusal of an operand that is an argument of AND or OR (measured
/// 2026-11-02: `a AND pk = 'a'`).
const AND_OR_ARGUMENTS: &str = "The arguments to AND/OR must be Expressions";

/// The refusal of an operand that is the argument of a NOT (measured
/// 2026-11-05: `NOT a`, `NOT (a)` and `(NOT a)`).
const NOT_ARGUMENT: &str = "The argument to NOT must be a Type or Expression";

/// The name of the predicate that stands for an operand that is not a
/// condition. It never runs: the operand is refused (see
/// [`Parser::term_predicate`]). No call can have this name.
const BARE_OPERAND: &str = "<operand>";

/// The name of the predicate that stands for an operand that is all a group
/// holds, until the group is read as that operand (see `Parser::predicate`).
/// It never leaves the parser. No call can have this name.
const GROUPED_OPERAND: &str = "<grouped operand>";

/// The syntax error of an operand with no comparison after it, where the
/// service's answer is not measured.
const NO_COMPARISON: &str = "Expected a comparison operator";

/// Parse a single PartiQL statement.
///
/// # Errors
///
/// Returns a [`ParseError`] whose `message` is the service's
/// `ValidationException` text for the failure.
pub fn parse(input: &str) -> PResult<Statement> {
    limits::parse_on_a_fitting_stack(|inline| parse_attempt(input, None, false, inline))
}

/// [`parse`] for a request that gives `parameters` values for the statement's
/// `?` placeholders. A statement past the operator limit with more
/// placeholders than that is refused for the parameter count rather than the
/// operator count (measured 2026-11-02).
///
/// # Errors
///
/// As [`parse`].
pub fn parse_for_request(input: &str, parameters: usize) -> PResult<Statement> {
    limits::parse_on_a_fitting_stack(|inline| parse_attempt(input, Some(parameters), false, inline))
}

/// [`parse_for_request`] for an ExecuteStatement request, which refuses an
/// `EXISTS(SELECT ...)` statement ahead of the table name rule and of the
/// walk of its condition (measured 2026-11-02 with a table name of two
/// characters and a `+` in the condition).
///
/// # Errors
///
/// As [`parse`].
pub fn parse_for_execute_statement(input: &str, parameters: usize) -> PResult<Statement> {
    limits::parse_on_a_fitting_stack(|inline| parse_attempt(input, Some(parameters), true, inline))
}

/// [`parse`] with the full caps on the calling thread, for the tests that
/// measure the stack a deep statement needs.
#[cfg(test)]
pub(crate) fn parse_with_full_caps(input: &str) -> PResult<Statement> {
    parse_attempt(input, None, false, false).expect("only an inline parse runs out of budget")
}

/// One parse of `input`, for a request with `parameters` values when that is
/// known, refusing an EXISTS statement when `refuse_exists`. `inline` keeps
/// the parse within the depth budget of the caller's thread; `None` means it
/// ran out (see [`limits`]).
fn parse_attempt(
    input: &str,
    parameters: Option<usize>,
    refuse_exists: bool,
    inline: bool,
) -> Option<PResult<Statement>> {
    // A lexing failure carries nothing after the prefix (see the lexer).
    let Ok(tokens) = lex(input) else {
        return Some(Err(ParseError::well_formed("")));
    };
    let mut p = Parser {
        tokens,
        pos: 0,
        next_param: 0,
        parameters,
        refuse_exists,
        found: Findings::default(),
        select_list_call: None,
        table: None,
        in_where: false,
        in_values: false,
        after_where: false,
        after_not: false,
        bare_left: false,
        nesting: Nesting::new(inline),
        operators: 0,
        operator_refusals: 0,
    };

    // A number literal that cannot be converted fails the whole statement
    // before parsing starts.
    if let Err(e) = super::number::check_number_tokens(&p.tokens) {
        return Some(Err(e));
    }

    let result = p.statement().and_then(|stmt| p.finish(stmt));
    if p.nesting.out_of_budget {
        return None;
    }
    // The service reads the whole statement before it checks what the
    // statement names, so a syntax error anywhere wins over an alias, an
    // empty table name or a refused FROM path, and those win over the checks
    // made on a statement that parsed, the LIMIT clause check included
    // (measured 2026-11-02: `... t WHERE` is "Unexpected term",
    // `DELETE FROM "t" t WHERE pk = 'x'` is the alias refusal). See [`walk`]
    // for the order around the operator count.
    Some(p.resolve(result))
}

// Each flag records one independent fact about the request or the parse so far.
#[allow(clippy::struct_excessive_bools)]
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    next_param: usize,
    /// How many parameters the request gives, when the statement is parsed
    /// for a request.
    parameters: Option<usize>,
    /// Whether an `EXISTS(SELECT ...)` statement is refused (ExecuteStatement).
    refuse_exists: bool,
    /// The refusals found in a statement that is still being read (an alias,
    /// a refused `FROM` path). They are reported only once the statement
    /// parses; see [`walk`].
    found: Findings,
    /// The refusal of a call in the select list of a SELECT statement, once
    /// the select list has been read (see [`walk`]).
    select_list_call: Option<String>,
    /// The table the statement names, once it has been read.
    table: Option<String>,
    /// Set while a WHERE condition is parsed, where only the condition
    /// functions are recognized. A refusal that stops the parse there leaves
    /// it set.
    in_where: bool,
    /// Set while the SET and REMOVE clauses of an UPDATE or the value of an
    /// INSERT are parsed.
    in_values: bool,
    /// Set once the WHERE condition has been read.
    after_where: bool,
    /// Whether the predicate being read follows a NOT.
    after_not: bool,
    /// Set when an operand that is not a condition was read as the left
    /// argument of an AND or an OR, until that operator is read.
    bare_left: bool,
    /// How deep the parse has recursed (see [`limits`]).
    nesting: Nesting,
    /// The operators of the WHERE condition read so far.
    operators: usize,
    /// The operators of the WHERE condition refused so far (see
    /// [`Parser::refuse_condition_operator`]).
    operator_refusals: usize,
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn bump(&mut self) -> Token {
        let t = self.tokens[self.pos].clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        t
    }

    fn take_param_index(&mut self) -> usize {
        let i = self.next_param;
        self.next_param += 1;
        i
    }

    /// A case-insensitive keyword match on the current token, consuming it on hit.
    fn eat_keyword(&mut self, kw: &str) -> bool {
        if let TokenKind::Ident(s) = &self.peek().kind
            && s.eq_ignore_ascii_case(kw)
        {
            self.bump();
            return true;
        }
        false
    }

    fn is_keyword(&self, kw: &str) -> bool {
        matches!(&self.peek().kind, TokenKind::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn eat(&mut self, kind: &TokenKind) -> bool {
        if &self.peek().kind == kind {
            self.bump();
            true
        } else {
            false
        }
    }

    /// Span string `line:column:length` for the current token, for `... at <l:c:len>` messages.
    fn span(&self) -> String {
        let t = self.peek();
        format!("{}:{}:{}", t.line, t.column, t.length)
    }

    /// What may follow a complete statement: nothing, or a semicolon.
    fn finish(&mut self, stmt: Statement) -> PResult<Statement> {
        self.eat_final_semicolon();
        match &self.peek().kind {
            TokenKind::Eof => Ok(stmt),
            // A trailing unsupported clause keyword (e.g. LIMIT, which is a
            // request parameter rather than grammar) is refused by name, in
            // its place among the refusals of a statement that parses (see
            // [`walk`]). The position is the token after it (measured
            // 2026-11-02: `LIMIT 2` names the `2`, length 1). LIMIT at the
            // end of the statement is not measured and names itself.
            TokenKind::Ident(s) if s.eq_ignore_ascii_case("LIMIT") => {
                let t = self
                    .tokens
                    .get(self.pos + 1)
                    .filter(|t| !matches!(t.kind, TokenKind::Eof))
                    .unwrap_or_else(|| self.peek());
                let refusal = format!("Unsupported clause: LIMIT at {}:{}:1", t.line, t.column);
                self.refuse_clause(refusal);
                Ok(stmt)
            }
            _ => Err(self.trailing_error()),
        }
    }

    /// The error for a token left over after a complete statement or bare
    /// expression.
    fn trailing_error(&self) -> ParseError {
        match self.peek().kind {
            TokenKind::Semicolon => ParseError::well_formed(
                "Unexpected token after semicolon. (Only one query is allowed.)",
            ),
            _ => ParseError::well_formed("Unexpected token after expression"),
        }
    }

    /// The service's error for a token that cannot start a term, by what the
    /// token is (measured 2026-11-02): an operator is "Unexpected operator", a
    /// reserved keyword "Unexpected keyword", and anything else, the end of
    /// the statement included, "Unexpected term".
    fn term_error(&self) -> ParseError {
        let reason = match &self.peek().kind {
            TokenKind::Eq
            | TokenKind::Ne
            | TokenKind::Lt
            | TokenKind::Le
            | TokenKind::Gt
            | TokenKind::Ge
            | TokenKind::Plus
            | TokenKind::Minus => "Unexpected operator",
            TokenKind::Ident(word) => match word_class(word) {
                WordClass::Operator => "Unexpected operator",
                WordClass::Keyword => "Unexpected keyword",
                _ => "Unexpected term",
            },
            _ => "Unexpected term",
        };
        ParseError::well_formed(reason)
    }

    /// The service refuses an infix operator with nothing after it before it
    /// reads a right side (measured 2026-11-02 on a condition ending in `AND`
    /// and in `OR`). Call with the operator consumed.
    fn require_rhs(&self) -> PResult<()> {
        if matches!(self.peek().kind, TokenKind::Eof) {
            return Err(ParseError::well_formed(MISSING_RHS));
        }
        Ok(())
    }

    /// Whether the current token is an unquoted word usable as a name.
    fn at_name(&self) -> bool {
        matches!(&self.peek().kind, TokenKind::Ident(s) if word_class(s) == WordClass::Name)
    }

    /// An optional alias after a table. The service reads an alias and then
    /// refuses it, so the refusal is deferred until the statement parses.
    fn optional_alias(&mut self) -> PResult<()> {
        if self.read_alias()? {
            self.defer(ALIASING);
        }
        Ok(())
    }

    /// Read an optional alias after a table or a projected path: `AS` and a
    /// name, or a bare name. Returns whether there was one. `AS` with no name
    /// after it is a syntax error (measured 2026-11-02 in a projection and in
    /// FROM).
    fn read_alias(&mut self) -> PResult<bool> {
        let is_name = |p: &Self| p.at_name() || matches!(p.peek().kind, TokenKind::QuotedIdent(_));
        if self.eat_keyword("AS") {
            if !is_name(self) {
                return Err(ParseError::well_formed("Expected identifier for AS-alias"));
            }
        } else if !is_name(self) {
            return Ok(false);
        }
        self.bump();
        Ok(true)
    }

    /// An optional alias after a path in the select list. It is refused after
    /// the table name rule (measured 2026-11-02: `SELECT a AS b FROM "tt"` is
    /// refused for the name of the table).
    fn projection_alias(&mut self) -> PResult<()> {
        if self.read_alias()? {
            self.defer_after_name_rule(ALIASING);
        }
        Ok(())
    }

    /// The aliases a FROM source may carry: `AS` or a bare name, then `AT`
    /// and `BY` names, each refused like the first (measured 2026-11-02 with
    /// `AT x` and `BY x` after the table). An alias on the table comes ahead
    /// of the table name rule (measured 2026-11-02 with and without `AS`).
    fn source_aliases(&mut self) -> PResult<()> {
        self.optional_alias()?;
        for word in ["AT", "BY"] {
            if self.is_keyword(word)
                && matches!(
                    self.tokens.get(self.pos + 1).map(|t| &t.kind),
                    Some(TokenKind::Ident(s)) if word_class(s) == WordClass::Name
                )
            {
                self.bump();
                self.bump();
                self.defer(ALIASING);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
