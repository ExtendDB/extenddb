// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL parser: tokens -> [`ast::Statement`].
//!
//! Recursive descent. Error messages reproduce the service's
//! `ValidationException` strings verbatim (see the behavioural spec); a parse
//! failure carries the full message the engine will surface.

use std::collections::{BTreeMap, BTreeSet};

use super::ast::{
    ArithOp, CompareOp, DeleteStatement, FromSource, InsertStatement, Operand, OrderBy, Path,
    PathStep, Projection, Returning, SelectStatement, SetAction, Statement, UpdateStatement,
    WhereExpr,
};
use super::lexer::{Token, TokenKind, lex};
use super::parse_error;
use crate::types::AttributeValue;

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
}

type PResult<T> = Result<T, ParseError>;

/// Parse a single PartiQL statement.
///
/// # Errors
///
/// Returns a [`ParseError`] whose `message` is the service's
/// `ValidationException` text for the failure.
pub fn parse(input: &str) -> PResult<Statement> {
    let tokens = lex(input).map_err(|e| ParseError::well_formed(&e.message))?;
    let mut p = Parser {
        tokens,
        pos: 0,
        next_param: 0,
    };
    let stmt = p.statement()?;
    match &p.peek().kind {
        TokenKind::Eof => Ok(stmt),
        TokenKind::Semicolon => Err(ParseError::well_formed(
            "Unexpected token after semicolon. (Only one query is allowed.)",
        )),
        // A trailing unsupported clause keyword (e.g. LIMIT, which is a request
        // parameter rather than grammar) is reported by name with its span.
        TokenKind::Ident(s) if s.eq_ignore_ascii_case("LIMIT") => {
            let t = p.peek();
            Err(ParseError::new(format!(
                "Unsupported clause: LIMIT at {}:{}:1",
                t.line, t.column
            )))
        }
        _ => Err(ParseError::well_formed("Unexpected token after expression")),
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    next_param: usize,
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

    // --- statements ---

    fn statement(&mut self) -> PResult<Statement> {
        if self.is_keyword("SELECT") {
            self.bump();
            Ok(Statement::Select(self.select()?))
        } else if self.is_keyword("INSERT") {
            self.bump();
            Ok(Statement::Insert(self.insert()?))
        } else if self.is_keyword("UPDATE") {
            self.bump();
            Ok(Statement::Update(self.update()?))
        } else if self.is_keyword("DELETE") {
            self.bump();
            Ok(Statement::Delete(self.delete()?))
        } else {
            Err(ParseError::well_formed("Expected data manipulation"))
        }
    }

    fn select(&mut self) -> PResult<SelectStatement> {
        let projection = self.projection()?;
        if !self.eat_keyword("FROM") {
            return Err(ParseError::well_formed("Expected FROM after SELECT list"));
        }
        let from = self.parse_from_source()?;
        let where_clause = if self.eat_keyword("WHERE") {
            Some(self.where_expr()?)
        } else {
            None
        };
        let order_by = if self.eat_keyword("ORDER") {
            if !self.eat_keyword("BY") {
                return Err(ParseError::well_formed("Expected BY after ORDER"));
            }
            let path = self.path()?;
            let descending = if self.eat_keyword("DESC") {
                true
            } else {
                self.eat_keyword("ASC");
                false
            };
            Some(OrderBy { path, descending })
        } else {
            None
        };
        Ok(SelectStatement {
            projection,
            from,
            where_clause,
            order_by,
        })
    }

    fn projection(&mut self) -> PResult<Projection> {
        if self.eat(&TokenKind::Star) {
            return Ok(Projection::All);
        }
        let mut paths = vec![self.projection_path()?];
        while self.eat(&TokenKind::Comma) {
            paths.push(self.projection_path()?);
        }
        // `AS` aliasing is rejected by the service.
        if self.is_keyword("AS") {
            return Err(ParseError::new("Aliasing is not supported"));
        }
        Ok(Projection::Paths(paths))
    }

    /// A path in the SELECT list, with the two projection-only grammar limits the
    /// service enforces: a projected path carries at most one dot step (a
    /// two-dot path like `a.b.c` is an "Invalid path dot component"), and an
    /// identifier immediately followed by `(` (e.g. `COUNT(*)`) is an unsupported
    /// path component reported at the identifier's span.
    fn projection_path(&mut self) -> PResult<Path> {
        let (line, column) = {
            let t = self.peek();
            (t.line, t.column)
        };
        let root_len = match &self.peek().kind {
            TokenKind::Ident(s) | TokenKind::QuotedIdent(s) => s.len(),
            _ => 0,
        };
        let path = self.path()?;
        // `COUNT(*)` and the like: an identifier followed by a call parenthesis
        // is not a projectable path component.
        if matches!(self.peek().kind, TokenKind::LParen) {
            return Err(ParseError::new(format!(
                "Unexpected path component at {line}:{column}:{root_len}"
            )));
        }
        let dot_steps = path
            .steps
            .iter()
            .filter(|s| matches!(s, PathStep::Attribute(_)))
            .count();
        if dot_steps > 1 {
            return Err(ParseError::well_formed("Invalid path dot component"));
        }
        Ok(path)
    }

    fn parse_from_source(&mut self) -> PResult<FromSource> {
        let table = match &self.peek().kind {
            TokenKind::QuotedIdent(s) => {
                let s = s.clone();
                self.bump();
                s
            }
            // A bare, unquoted table name is not a valid from source.
            _ => return Err(ParseError::new("Unexpected from source")),
        };
        let index = if self.eat(&TokenKind::Dot) {
            match &self.peek().kind {
                TokenKind::QuotedIdent(s) | TokenKind::Ident(s) => {
                    let s = s.clone();
                    self.bump();
                    Some(s)
                }
                _ => return Err(ParseError::new("Unexpected from source")),
            }
        } else {
            None
        };
        Ok(FromSource { table, index })
    }

    fn insert(&mut self) -> PResult<InsertStatement> {
        if !self.eat_keyword("INTO") {
            return Err(ParseError::well_formed("Unexpected keyword"));
        }
        let table = self.table_name()?;
        if self.is_keyword("VALUES") {
            return Err(ParseError::well_formed(
                "Expected LEFT_PAREN for row value constructor",
            ));
        }
        if !self.eat_keyword("VALUE") {
            return Err(ParseError::well_formed("Expected VALUE"));
        }
        if !matches!(self.peek().kind, TokenKind::LBrace) {
            let span = self.span();
            return Err(ParseError::new(format!(
                "Unsupported format in item expression. Please use struct format {{...}} to express item values for insert statement. at {span}"
            )));
        }
        let value = self.item_map()?;
        Ok(InsertStatement { table, value })
    }

    fn update(&mut self) -> PResult<UpdateStatement> {
        let table = self.table_name()?;
        let mut sets = Vec::new();
        let mut removes = Vec::new();
        let mut saw_clause = false;
        if self.eat_keyword("SET") {
            saw_clause = true;
            loop {
                let path = self.path()?;
                if !self.eat(&TokenKind::Eq) {
                    return Err(ParseError::well_formed("Expected = in SET clause"));
                }
                let value = self.operand()?;
                sets.push(SetAction { path, value });
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if self.eat_keyword("REMOVE") {
            saw_clause = true;
            loop {
                removes.push(self.path()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !saw_clause {
            return Err(ParseError::well_formed("Expected data manipulation"));
        }
        if !self.eat_keyword("WHERE") {
            return Err(ParseError::new(
                "Where clause does not contain a mandatory equality on all key attributes",
            ));
        }
        let where_clause = self.where_expr()?;
        let returning = self.returning()?;
        Ok(UpdateStatement {
            table,
            sets,
            removes,
            where_clause,
            returning,
        })
    }

    fn delete(&mut self) -> PResult<DeleteStatement> {
        if !self.eat_keyword("FROM") {
            return Err(ParseError::well_formed("Expected FROM after DELETE"));
        }
        let table = self.table_name()?;
        if !self.eat_keyword("WHERE") {
            return Err(ParseError::new(
                "Where clause does not contain a mandatory equality on all key attributes",
            ));
        }
        let where_clause = self.where_expr()?;
        let returning = self.returning()?;
        Ok(DeleteStatement {
            table,
            where_clause,
            returning,
        })
    }

    fn returning(&mut self) -> PResult<Option<Returning>> {
        if !self.eat_keyword("RETURNING") {
            return Ok(None);
        }
        let old_new;
        let modified = self.eat_keyword("MODIFIED");
        if self.eat_keyword("ALL") {
            old_new = self.old_or_new()?;
            self.eat(&TokenKind::Star);
            Ok(Some(match (modified, old_new) {
                (false, false) => Returning::AllOld,
                (false, true) => Returning::AllNew,
                (true, false) => Returning::ModifiedOld,
                (true, true) => Returning::ModifiedNew,
            }))
        } else if modified {
            old_new = self.old_or_new()?;
            self.eat(&TokenKind::Star);
            Ok(Some(if old_new {
                Returning::ModifiedNew
            } else {
                Returning::ModifiedOld
            }))
        } else {
            Err(ParseError::new("Invalid returning clause"))
        }
    }

    /// Returns true for NEW, false for OLD.
    fn old_or_new(&mut self) -> PResult<bool> {
        if self.eat_keyword("OLD") {
            Ok(false)
        } else if self.eat_keyword("NEW") {
            Ok(true)
        } else {
            Err(ParseError::new("Invalid returning clause"))
        }
    }

    fn table_name(&mut self) -> PResult<String> {
        match &self.peek().kind {
            TokenKind::QuotedIdent(s) => {
                let s = s.clone();
                self.bump();
                Ok(s)
            }
            _ => Err(ParseError::well_formed("Expected FROM after DELETE")),
        }
    }

    // --- paths ---

    fn path(&mut self) -> PResult<Path> {
        let root = match &self.peek().kind {
            TokenKind::Ident(s) | TokenKind::QuotedIdent(s) => {
                let s = s.clone();
                self.bump();
                s
            }
            _ => return Err(ParseError::well_formed("Expected an attribute path")),
        };
        let mut steps = Vec::new();
        loop {
            if self.eat(&TokenKind::Dot) {
                match &self.peek().kind {
                    TokenKind::Ident(s) | TokenKind::QuotedIdent(s) => {
                        let s = s.clone();
                        self.bump();
                        steps.push(PathStep::Attribute(s));
                    }
                    _ => {
                        return Err(ParseError::new(format!(
                            "Unexpected path component at {}",
                            self.span()
                        )));
                    }
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

    // --- where expression ---

    fn where_expr(&mut self) -> PResult<WhereExpr> {
        self.or_expr()
    }

    fn or_expr(&mut self) -> PResult<WhereExpr> {
        let mut left = self.and_expr()?;
        while self.eat_keyword("OR") {
            let right = self.and_expr()?;
            left = WhereExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and_expr(&mut self) -> PResult<WhereExpr> {
        let mut left = self.not_expr()?;
        while self.eat_keyword("AND") {
            let right = self.not_expr()?;
            left = WhereExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn not_expr(&mut self) -> PResult<WhereExpr> {
        if self.eat_keyword("NOT") {
            let inner = self.not_expr()?;
            return Ok(WhereExpr::Not(Box::new(inner)));
        }
        self.predicate()
    }

    fn predicate(&mut self) -> PResult<WhereExpr> {
        if self.eat(&TokenKind::LParen) {
            let inner = self.where_expr()?;
            if !self.eat(&TokenKind::RParen) {
                return Err(ParseError::well_formed("Expected ) to close group"));
            }
            return Ok(inner);
        }
        // Function-style predicates: begins_with / contains / attribute_type.
        if let TokenKind::Ident(name) = &self.peek().kind {
            let lname = name.to_ascii_lowercase();
            if matches!(
                lname.as_str(),
                "begins_with" | "contains" | "attribute_type"
            ) && matches!(
                self.tokens.get(self.pos + 1).map(|t| &t.kind),
                Some(TokenKind::LParen)
            ) {
                self.bump();
                let args = self.call_args()?;
                if args.len() != 2 {
                    return Err(ParseError::new(format!("{lname} requires two arguments")));
                }
                let mut it = args.into_iter();
                let a = it.next().unwrap();
                let b = it.next().unwrap();
                return Ok(match lname.as_str() {
                    "begins_with" => WhereExpr::BeginsWith(a, b),
                    "contains" => WhereExpr::Contains(a, b),
                    _ => WhereExpr::AttributeType(a, b),
                });
            }
            if matches!(
                lname.as_str(),
                "attribute_exists" | "attribute_not_exists" | "size"
            ) && matches!(
                self.tokens.get(self.pos + 1).map(|t| &t.kind),
                Some(TokenKind::LParen)
            ) {
                self.bump();
                let args = self.call_args()?;
                // size(...) can appear as the left of a comparison; handle that below
                // by treating the function as an operand. Here with no comparison it is
                // a bare boolean function (attribute_exists / attribute_not_exists).
                if lname == "size" {
                    let left = Operand::Function(lname, args);
                    return self.finish_comparison(left);
                }
                return Ok(WhereExpr::Function(lname, args));
            }
        }
        let left = self.operand()?;
        // IS [NOT] MISSING
        if self.eat_keyword("IS") {
            let negated = self.eat_keyword("NOT");
            if !self.eat_keyword("MISSING") {
                return Err(ParseError::well_formed("Expected MISSING after IS"));
            }
            let path = match left {
                Operand::Path(p) => p,
                _ => return Err(ParseError::new("IS MISSING requires an attribute path")),
            };
            return Ok(WhereExpr::IsMissing(path, !negated));
        }
        // BETWEEN
        if self.eat_keyword("BETWEEN") {
            let low = self.operand()?;
            if !self.eat_keyword("AND") {
                return Err(ParseError::well_formed("Expected AND in BETWEEN"));
            }
            let high = self.operand()?;
            return Ok(WhereExpr::Between(left, low, high));
        }
        // IN
        if self.eat_keyword("IN") {
            if !self.eat(&TokenKind::LBracket) {
                return Err(ParseError::new(
                    "IN operator must have a left hand argument of type Variable Reference and right hand argument of type Seq with at least one member",
                ));
            }
            let mut items = Vec::new();
            if !matches!(self.peek().kind, TokenKind::RBracket) {
                loop {
                    items.push(self.operand()?);
                    if !self.eat(&TokenKind::Comma) {
                        break;
                    }
                }
            }
            if !self.eat(&TokenKind::RBracket) {
                return Err(ParseError::new("Expected ] to close IN list"));
            }
            if items.is_empty() {
                return Err(ParseError::new(
                    "IN operator must have a left hand argument of type Variable Reference and right hand argument of type Seq with at least one member",
                ));
            }
            return Ok(WhereExpr::In(left, items));
        }
        self.finish_comparison(left)
    }

    fn finish_comparison(&mut self, left: Operand) -> PResult<WhereExpr> {
        let op = match &self.peek().kind {
            TokenKind::Eq => CompareOp::Eq,
            TokenKind::Ne => CompareOp::Ne,
            TokenKind::Lt => CompareOp::Lt,
            TokenKind::Le => CompareOp::Le,
            TokenKind::Gt => CompareOp::Gt,
            TokenKind::Ge => CompareOp::Ge,
            _ => return Err(ParseError::well_formed("Expected a comparison operator")),
        };
        self.bump();
        let right = self.operand()?;
        Ok(WhereExpr::Compare(left, op, right))
    }

    fn call_args(&mut self) -> PResult<Vec<Operand>> {
        // assumes the current token is LParen
        self.eat(&TokenKind::LParen);
        let mut args = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RParen) {
            loop {
                args.push(self.operand()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&TokenKind::RParen) {
            return Err(ParseError::well_formed("Expected ) to close function call"));
        }
        Ok(args)
    }

    // --- operands ---

    fn operand(&mut self) -> PResult<Operand> {
        let left = self.operand_atom()?;
        if matches!(self.peek().kind, TokenKind::Plus | TokenKind::Minus) {
            let op = if matches!(self.peek().kind, TokenKind::Plus) {
                ArithOp::Add
            } else {
                ArithOp::Sub
            };
            self.bump();
            let right = self.operand_atom()?;
            return Ok(Operand::Arith(Box::new(left), op, Box::new(right)));
        }
        Ok(left)
    }

    fn operand_atom(&mut self) -> PResult<Operand> {
        match self.peek().kind.clone() {
            TokenKind::Param => {
                self.bump();
                Ok(Operand::Parameter(self.take_param_index()))
            }
            TokenKind::String(s) => {
                self.bump();
                Ok(Operand::Literal(AttributeValue::S(s)))
            }
            TokenKind::Number(n) => {
                self.bump();
                Ok(Operand::Literal(AttributeValue::N(n)))
            }
            TokenKind::LBrace => Ok(Operand::Literal(AttributeValue::M(self.map_literal()?))),
            TokenKind::LBracket => Ok(Operand::Literal(AttributeValue::L(self.list_literal()?))),
            TokenKind::LDoubleAngle => Ok(Operand::Literal(self.bag_literal()?)),
            TokenKind::Ident(name) => {
                let lname = name.to_ascii_lowercase();
                if lname == "true" {
                    self.bump();
                    return Ok(Operand::Literal(AttributeValue::Bool(true)));
                }
                if lname == "false" {
                    self.bump();
                    return Ok(Operand::Literal(AttributeValue::Bool(false)));
                }
                if lname == "null" {
                    self.bump();
                    return Ok(Operand::Literal(AttributeValue::Null));
                }
                // function call vs path
                if matches!(
                    self.tokens.get(self.pos + 1).map(|t| &t.kind),
                    Some(TokenKind::LParen)
                ) {
                    self.bump();
                    let args = self.call_args()?;
                    Ok(Operand::Function(lname, args))
                } else {
                    Ok(Operand::Path(self.path()?))
                }
            }
            TokenKind::QuotedIdent(_) => Ok(Operand::Path(self.path()?)),
            _ => Err(ParseError::well_formed("Expected an operand")),
        }
    }

    fn map_literal(&mut self) -> PResult<BTreeMap<String, AttributeValue>> {
        self.eat(&TokenKind::LBrace);
        let mut map = BTreeMap::new();
        if !matches!(self.peek().kind, TokenKind::RBrace) {
            loop {
                let key = match &self.peek().kind {
                    TokenKind::String(s) => s.clone(),
                    _ => {
                        return Err(ParseError::new(format!(
                            "Tuple keys must be of type String. Actual type: Id under root at {}",
                            self.span()
                        )));
                    }
                };
                self.bump();
                if !self.eat(&TokenKind::Colon) {
                    return Err(ParseError::well_formed("Expected : in map literal"));
                }
                let val = self.literal_value()?;
                map.insert(key, val);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&TokenKind::RBrace) {
            return Err(ParseError::well_formed("Expected } to close map literal"));
        }
        Ok(map)
    }

    /// The top-level `VALUE { ... }` item map, returned as ordered pairs.
    fn item_map(&mut self) -> PResult<Vec<(String, Operand)>> {
        self.eat(&TokenKind::LBrace);
        let mut pairs = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RBrace) {
            loop {
                let key = match &self.peek().kind {
                    TokenKind::String(s) => s.clone(),
                    _ => {
                        return Err(ParseError::new(format!(
                            "Tuple keys must be of type String. Actual type: Id under root at {}",
                            self.span()
                        )));
                    }
                };
                self.bump();
                if !self.eat(&TokenKind::Colon) {
                    return Err(ParseError::well_formed("Expected : in item value"));
                }
                // A VALUE field may be a literal or a ? parameter.
                let val = if matches!(self.peek().kind, TokenKind::Param) {
                    self.bump();
                    Operand::Parameter(self.take_param_index())
                } else {
                    Operand::Literal(self.literal_value()?)
                };
                pairs.push((key, val));
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&TokenKind::RBrace) {
            return Err(ParseError::well_formed("Expected } to close item value"));
        }
        Ok(pairs)
    }

    fn list_literal(&mut self) -> PResult<Vec<AttributeValue>> {
        self.eat(&TokenKind::LBracket);
        let mut items = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RBracket) {
            loop {
                items.push(self.literal_value()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&TokenKind::RBracket) {
            return Err(ParseError::well_formed("Expected ] to close list literal"));
        }
        Ok(items)
    }

    fn bag_literal(&mut self) -> PResult<AttributeValue> {
        self.eat(&TokenKind::LDoubleAngle);
        let mut strings: BTreeSet<String> = BTreeSet::new();
        let mut numbers: BTreeSet<String> = BTreeSet::new();
        let mut saw_string = false;
        let mut saw_number = false;
        // Members in source order, for the service's duplicate message which
        // echoes the whole collection ("Input collection [a, a] ...").
        let mut members: Vec<String> = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RDoubleAngle) {
            loop {
                match self.literal_value()? {
                    AttributeValue::S(s) => {
                        saw_string = true;
                        strings.insert(s.clone());
                        members.push(s);
                    }
                    AttributeValue::N(n) => {
                        saw_number = true;
                        numbers.insert(n.clone());
                        members.push(n);
                    }
                    _ => {
                        return Err(ParseError::new(
                            "Unsupported data type in Bag. DynamoDB only supports either numbers or strings in bags",
                        ));
                    }
                }
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&TokenKind::RDoubleAngle) {
            return Err(ParseError::well_formed("Expected >> to close bag literal"));
        }
        if saw_string && saw_number {
            return Err(ParseError::new(
                "Unsupported data type in Bag. DynamoDB only supports either numbers or strings in bags",
            ));
        }
        let distinct = if saw_number {
            numbers.len()
        } else {
            strings.len()
        };
        if distinct != members.len() {
            return Err(ParseError::new(duplicate_bag_message(&members)));
        }
        if saw_number {
            Ok(AttributeValue::NS(numbers))
        } else {
            Ok(AttributeValue::SS(strings))
        }
    }

    /// A literal value in a map/list/bag position (no paths, no parameters).
    fn literal_value(&mut self) -> PResult<AttributeValue> {
        match self.peek().kind.clone() {
            TokenKind::String(s) => {
                self.bump();
                Ok(AttributeValue::S(s))
            }
            TokenKind::Number(n) => {
                self.bump();
                Ok(AttributeValue::N(n))
            }
            TokenKind::LBrace => Ok(AttributeValue::M(self.map_literal()?)),
            TokenKind::LBracket => Ok(AttributeValue::L(self.list_literal()?)),
            TokenKind::LDoubleAngle => self.bag_literal(),
            TokenKind::Ident(name) => {
                let l = name.to_ascii_lowercase();
                self.bump();
                match l.as_str() {
                    "true" => Ok(AttributeValue::Bool(true)),
                    "false" => Ok(AttributeValue::Bool(false)),
                    "null" => Ok(AttributeValue::Null),
                    _ => Err(ParseError::well_formed("Expected a literal value")),
                }
            }
            _ => Err(ParseError::well_formed("Expected a literal value")),
        }
    }
}

fn duplicate_bag_message(members: &[String]) -> String {
    // The service echoes the whole collection in source order, e.g.
    // `Input collection [a, a] contains duplicates.`
    format!(
        "One or more parameter values were invalid: Input collection [{}] contains duplicates.",
        members.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(sql: &str) -> String {
        parse(sql).unwrap_err().message
    }

    #[test]
    fn select_missing_from() {
        assert_eq!(
            err("SELECT * WHERE pk='a'"),
            "Statement wasn't well formed, can't be processed: Expected FROM after SELECT list"
        );
    }

    #[test]
    fn non_dml_leading_token() {
        assert_eq!(
            err("FOO * FROM \"t\""),
            "Statement wasn't well formed, can't be processed: Expected data manipulation"
        );
    }

    #[test]
    fn unquoted_table_is_unexpected_from_source() {
        assert_eq!(
            err("SELECT * FROM t WHERE pk='a'"),
            "Unexpected from source"
        );
    }

    #[test]
    fn delete_missing_from() {
        assert_eq!(
            err("DELETE \"t\" WHERE pk='a' AND sk='b'"),
            "Statement wasn't well formed, can't be processed: Expected FROM after DELETE"
        );
    }

    #[test]
    fn update_without_set_or_remove() {
        assert_eq!(
            err("UPDATE \"t\" WHERE pk='a' AND sk='b'"),
            "Statement wasn't well formed, can't be processed: Expected data manipulation"
        );
    }

    #[test]
    fn insert_values_plural_is_rejected() {
        assert_eq!(
            err("INSERT INTO \"t\" VALUES {'pk':'a'}"),
            "Statement wasn't well formed, can't be processed: Expected LEFT_PAREN for row value constructor"
        );
    }

    #[test]
    fn insert_double_quoted_key_is_rejected() {
        let m = err("INSERT INTO \"t\" VALUE {\"pk\":'a'}");
        assert!(
            m.starts_with("Tuple keys must be of type String. Actual type: Id under root at "),
            "{m}"
        );
    }

    #[test]
    fn two_statements_separated_by_semicolon() {
        assert_eq!(
            err("SELECT * FROM \"t\" WHERE pk='a';SELECT * FROM \"t\" WHERE pk='b'"),
            "Statement wasn't well formed, can't be processed: Unexpected token after semicolon. (Only one query is allowed.)"
        );
    }

    #[test]
    fn aliasing_is_rejected() {
        assert_eq!(
            err("SELECT name AS n FROM \"t\" WHERE pk='a'"),
            "Aliasing is not supported"
        );
    }

    #[test]
    fn limit_clause_is_an_unsupported_clause() {
        // The column is where LIMIT begins; the service reports length 1.
        let m = err("SELECT * FROM \"t\" WHERE pk='a' LIMIT 2");
        assert!(m.starts_with("Unsupported clause: LIMIT at 1:"), "{m}");
        assert!(m.ends_with(":1"), "{m}");
    }

    #[test]
    fn multi_dot_projection_is_rejected() {
        assert_eq!(
            err("SELECT a.b.c FROM \"t\" WHERE pk='a'"),
            "Statement wasn't well formed, can't be processed: Invalid path dot component"
        );
    }

    #[test]
    fn single_dot_projection_is_allowed() {
        assert!(parse("SELECT a.b FROM \"t\" WHERE pk='a'").is_ok());
    }

    #[test]
    fn count_star_is_an_unexpected_path_component() {
        // COUNT starts at column 8 (after "SELECT ") and is 5 chars.
        assert_eq!(
            err("SELECT COUNT(*) FROM \"t\" WHERE pk='a'"),
            "Unexpected path component at 1:8:5"
        );
    }

    #[test]
    fn duplicate_bag_members_are_named() {
        assert_eq!(
            err("INSERT INTO \"t\" VALUE {'pk':'a','s':<<'a','a'>>}"),
            "One or more parameter values were invalid: Input collection [a, a] contains duplicates."
        );
    }

    #[test]
    fn empty_in_list_is_rejected() {
        let m = err("SELECT * FROM \"t\" WHERE pk='a' AND sk IN []");
        assert!(
            m.starts_with("IN operator must have a left hand argument"),
            "{m}"
        );
    }

    #[test]
    fn select_happy_path_parses() {
        let s = parse(
            "SELECT a, b.c, d[0] FROM \"t\".\"gsi1\" WHERE pk='a' AND sk > 'b' ORDER BY sk DESC",
        )
        .unwrap();
        match s {
            Statement::Select(sel) => {
                assert_eq!(sel.from.table, "t");
                assert_eq!(sel.from.index.as_deref(), Some("gsi1"));
                assert!(sel.order_by.unwrap().descending);
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn insert_rich_literal_parses() {
        let s = parse(
            "INSERT INTO \"t\" VALUE {'pk':'p','n':3.5,'b':true,'z':null,'m':{'x':1},'l':[1,'two'],'ss':<<'a','b'>>}",
        )
        .unwrap();
        match s {
            Statement::Insert(i) => assert_eq!(i.value.len(), 7),
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn update_with_params_assigns_positional_indices() {
        let s = parse("UPDATE \"t\" SET a = ? WHERE pk = ? AND sk = ?").unwrap();
        match s {
            Statement::Update(u) => {
                assert!(matches!(u.sets[0].value, Operand::Parameter(0)));
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn delete_returning_all_old_parses() {
        let s = parse("DELETE FROM \"t\" WHERE pk='a' AND sk='b' RETURNING ALL OLD *").unwrap();
        match s {
            Statement::Delete(d) => assert_eq!(d.returning, Some(super::Returning::AllOld)),
            _ => panic!("expected delete"),
        }
    }
}
