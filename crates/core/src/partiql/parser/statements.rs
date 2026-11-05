// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The statements: SELECT, INSERT, UPDATE and DELETE, their targets and their clauses.

use super::{PResult, ParseError, Parser};
use crate::partiql::ast::{
    DeleteStatement, FromSource, InsertStatement, OrderBy, OrderTerm, Path, Projection, Returning,
    SelectStatement, Statement, UpdateStatement,
};
use crate::partiql::from_path;
use crate::partiql::lexer::{TokenKind, WordClass, is_function_keyword, word_class};

impl Parser {
    // --- statements ---

    pub(super) fn statement(&mut self) -> PResult<Statement> {
        if self.is_keyword("SELECT") {
            self.bump();
            Ok(Statement::Select(self.select(true)?))
        } else if self.is_keyword("INSERT") {
            self.bump();
            Ok(Statement::Insert(self.insert()?))
        } else if self.is_keyword("UPDATE") {
            self.bump();
            Ok(Statement::Update(self.update()?))
        } else if self.is_keyword("DELETE") {
            self.bump();
            Ok(Statement::Delete(self.delete()?))
        } else if self.is_keyword("FROM") {
            self.bump();
            Ok(Statement::Update(self.update_led_by_from()?))
        } else if self.at_exists_statement() {
            Ok(Statement::Exists(self.exists_statement()?))
        } else {
            // A statement that starts with none of the statement keywords is
            // read as a bare expression and refused by what it is.
            self.bare_statement()
        }
    }

    /// A SELECT after its keyword. `statement` is true for a SELECT statement,
    /// false for the query of an EXISTS.
    pub(super) fn select(&mut self, statement: bool) -> PResult<SelectStatement> {
        let mut projection_error = None;
        let projection = self.projection(&mut projection_error)?;
        if statement {
            self.select_list_call.clone_from(&projection_error);
        }
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
            // A list of names, each with a direction or none (measured
            // 2026-11-03: `ORDER BY pk DESC, sk`, `pk, sk ASC` and `sk DESC,
            // pk` run). A comma that ends the statement is "Unexpected term"
            // (measured 2026-11-03). A comma before anything else but a name
            // stays a token left over after the statement; the service
            // answers `ORDER BY pk, 1` with an internal error.
            let mut terms = vec![self.order_term()?];
            while matches!(self.peek().kind, TokenKind::Comma) {
                if matches!(self.nth_kind(1), Some(TokenKind::Eof)) {
                    self.bump();
                    return Err(self.term_error());
                }
                if !self.name_follows() {
                    break;
                }
                self.bump();
                terms.push(self.order_term()?);
            }
            Some(OrderBy { terms })
        } else {
            None
        };
        Ok(SelectStatement {
            projection,
            from,
            where_clause,
            order_by,
            projection_error,
        })
    }

    /// One path of an ORDER BY list and its direction, ascending unless it
    /// says `DESC`.
    fn order_term(&mut self) -> PResult<OrderTerm> {
        let at = self.span();
        let path = self.path()?;
        let descending = self.eat_keyword("DESC");
        if !descending {
            self.eat_keyword("ASC");
        }
        Ok(OrderTerm {
            path,
            descending,
            at,
        })
    }

    fn projection(&mut self, deferred: &mut Option<String>) -> PResult<Projection> {
        // `SELECT ALL` and `SELECT DISTINCT` read a select list after the
        // quantifier, and `SELECT VALUE` an expression (measured 2026-11-02:
        // `SELECT DISTINCT FROM "t"` names FROM, `SELECT VALUE FROM "t"` is the
        // error of a FROM-led form). Past those errors the forms are not
        // measured, and the quantifier keeps its term error.
        if self.is_keyword("ALL") || self.is_keyword("DISTINCT") {
            let quantifier = self.term_error();
            self.bump();
            return Err(self.select_list_keyword().unwrap_or(quantifier));
        }
        if self.is_keyword("VALUE") {
            let value = self.term_error();
            self.bump();
            self.operand()?;
            return Err(value);
        }
        if self.eat(&TokenKind::Star) {
            return Ok(Projection::All);
        }
        let mut paths = vec![self.projection_path(deferred)?];
        self.projection_alias()?;
        while self.eat(&TokenKind::Comma) {
            paths.push(self.projection_path(deferred)?);
            self.projection_alias()?;
        }
        Ok(Projection::Paths(paths))
    }

    /// A path in the SELECT list. An identifier immediately followed by `(`
    /// (e.g. `COUNT(*)`) and a literal are unsupported path components
    /// reported at their span (measured 2026-11-02 with COUNT, null, true,
    /// false and missing). Neither is a parse failure: the item is skipped and
    /// its refusal goes in `deferred` (see [`SelectStatement::projection_error`]).
    /// A reserved word gets the answer it gets where any term starts, and a
    /// path may have any number of steps (measured 2026-11-02: `SELECT m.a.b`
    /// runs).
    fn projection_path(&mut self, deferred: &mut Option<String>) -> PResult<Path> {
        if let Some(e) = self.select_list_keyword() {
            return Err(e);
        }
        let (line, column) = {
            let t = self.peek();
            (t.line, t.column)
        };
        let root_len = match &self.peek().kind {
            TokenKind::Ident(s) | TokenKind::QuotedIdent(s) => s.len(),
            _ => 0,
        };
        // `COUNT(*)` and the like: a function call is not a projectable path
        // component. Checked before the path is read because a keyword such as
        // COUNT is not a name. An argument list that is never closed is
        // reported at once.
        let callee = match &self.peek().kind {
            TokenKind::Ident(s) if word_class(s) == WordClass::Name || is_function_keyword(s) => {
                Some(s.clone())
            }
            TokenKind::QuotedIdent(s) => Some(s.clone()),
            _ => None,
        };
        if let Some(root) = callee
            && matches!(
                self.tokens.get(self.pos + 1).map(|t| &t.kind),
                Some(TokenKind::LParen)
            )
        {
            let message = format!("Unexpected path component at {line}:{column}:{root_len}");
            self.bump();
            if !self.skip_parenthesized() {
                return Err(ParseError::new(message));
            }
            deferred.get_or_insert(message);
            return Ok(Path {
                root,
                steps: Vec::new(),
            });
        }
        let literal = match &self.peek().kind {
            TokenKind::Ident(s) => word_class(s) == WordClass::Literal,
            TokenKind::Number(_) | TokenKind::String(_) => true,
            _ => false,
        };
        if literal {
            deferred.get_or_insert(format!("Unexpected path component at {}", self.span()));
            self.bump();
            return Ok(Path {
                root: String::new(),
                steps: Vec::new(),
            });
        }
        if matches!(&self.peek().kind, TokenKind::Ident(s) if word_class(s) != WordClass::Name) {
            // Only a form whose refusal waits for the end of the statement
            // reads to its end here.
            self.operand()?;
            return Ok(Path {
                root: String::new(),
                steps: Vec::new(),
            });
        }
        self.path()
    }

    /// Consume a parenthesized group starting at the current `(`, nested groups
    /// included. Returns false, having consumed the rest of the statement,
    /// when the group is never closed.
    fn skip_parenthesized(&mut self) -> bool {
        let mut depth = 0usize;
        loop {
            match self.peek().kind {
                TokenKind::LParen => depth += 1,
                TokenKind::RParen => depth -= 1,
                TokenKind::Eof => return false,
                _ => {}
            }
            self.bump();
            if depth == 0 {
                return true;
            }
        }
    }

    fn parse_from_source(&mut self) -> PResult<FromSource> {
        let table = match &self.peek().kind {
            // A quoted or an unquoted name (measured 2026-11-02: `FROM t` runs).
            TokenKind::QuotedIdent(s) => {
                let s = s.clone();
                self.bump();
                s
            }
            TokenKind::Ident(s) if word_class(s) == WordClass::Name => {
                let s = s.clone();
                self.bump();
                s
            }
            // A reserved word gets the answer it gets where any term starts
            // (measured 2026-11-02: `FROM where` is "Unexpected keyword", `FROM
            // and` "Unexpected operator", `FROM set` "Expected identifier for
            // simple path"); a literal word is not a table.
            TokenKind::Ident(s) if word_class(s) != WordClass::Literal => {
                self.operand()?;
                return Err(ParseError::new("Unexpected from source"));
            }
            // Where no term can start (the end of the statement, an
            // operator), the service reports the term error (measured
            // 2026-11-02: `SELECT * FROM` is "Unexpected term").
            TokenKind::Eof
            | TokenKind::Semicolon
            | TokenKind::Comma
            | TokenKind::Dot
            | TokenKind::Star
            | TokenKind::Colon
            | TokenKind::RParen
            | TokenKind::RBracket
            | TokenKind::RBrace
            | TokenKind::RDoubleAngle
            | TokenKind::Eq
            | TokenKind::Ne
            | TokenKind::Lt
            | TokenKind::Le
            | TokenKind::Gt
            | TokenKind::Ge => return Err(self.term_error()),
            // Any other term (a bare name, a literal) is not a table.
            _ => return Err(ParseError::new("Unexpected from source")),
        };
        self.table = Some(table.clone());
        let mut rest = self.path_steps_after_table()?;
        let index = if rest.is_empty() {
            None
        } else {
            Some(rest.remove(0))
        };
        let mut components = vec![table.as_str()];
        components.extend(index.as_deref());
        components.extend(rest.iter().map(String::as_str));
        let refusal = from_path::refusal(&components);
        // An alias is refused ahead of the path (measured 2026-11-02: `FROM
        // "t"."" x` is the alias refusal).
        self.source_aliases()?;
        if let Some(refusal) = refusal {
            self.defer(&refusal);
        }
        Ok(FromSource { table, index })
    }

    /// The `.component` steps after the table name of a FROM path. A step is
    /// a name or a quoted name; anything else after the dot, a reserved word
    /// or the end of the path included, is "Invalid path dot component"
    /// (measured 2026-11-02 with `"t".index` and `"t". WHERE`).
    fn path_steps_after_table(&mut self) -> PResult<Vec<String>> {
        let mut rest = Vec::new();
        while self.eat(&TokenKind::Dot) {
            match &self.peek().kind {
                TokenKind::QuotedIdent(s) => {
                    rest.push(s.clone());
                    self.bump();
                }
                TokenKind::Ident(s) if word_class(s) == WordClass::Name => {
                    rest.push(s.clone());
                    self.bump();
                }
                _ => return Err(ParseError::well_formed("Invalid path dot component")),
            }
        }
        Ok(rest)
    }

    /// The rest of an `UPDATE` (`update`) or `DELETE` target after its table
    /// name. A path naming an index parses and is refused once the statement
    /// has parsed: an UPDATE's after the table name rule (measured
    /// 2026-11-02: `UPDATE "tt"."idx"` is refused for the name of the table).
    /// A DELETE's is not measured against the rule and keeps its place ahead
    /// of it.
    pub(super) fn dml_target_rest(&mut self, table: &str, update: bool) -> PResult<()> {
        let rest = self.path_steps_after_table()?;
        let mut components = vec![table];
        components.extend(rest.iter().map(String::as_str));
        if let Some(refusal) = from_path::refusal(&components) {
            self.defer(&refusal);
        } else if components.len() == 2 {
            if update {
                self.defer_after_name_rule(from_path::ON_INDEX);
            } else {
                self.defer(from_path::ON_INDEX);
            }
        }
        Ok(())
    }

    fn insert(&mut self) -> PResult<InsertStatement> {
        if !self.eat_keyword("INTO") {
            return Err(ParseError::well_formed("Unexpected keyword"));
        }
        let table_span = self.span();
        let table = self.table_name()?;
        if !self.path_steps_after_table()?.is_empty() {
            self.defer(&from_path::insert_refusal(&table_span));
        }
        if self.is_keyword("VALUES") {
            return Err(ParseError::well_formed(
                "Expected LEFT_PAREN for row value constructor",
            ));
        }
        if !self.eat_keyword("VALUE") {
            return Err(self.insert_without_value());
        }
        if !matches!(self.peek().kind, TokenKind::LBrace) {
            let span = self.span();
            return Err(ParseError::new(format!(
                "Unsupported format in item expression. Please use struct format {{...}} to express item values for insert statement. at {span}"
            )));
        }
        self.in_values = true;
        let value = self.item_map()?;
        self.in_values = false;
        Ok(InsertStatement { table, value })
    }

    /// `INSERT INTO <table>` followed by something other than VALUE: the
    /// service reads an expression there, the items to insert, and refuses a
    /// statement that ends after it (measured 2026-11-02 with a map, a name
    /// and a string). Tokens after the expression are a leftover token
    /// (`INSERT INTO "t" t VALUE {...}`), and no expression at all is the term
    /// error ("Unexpected term" at the end of the statement).
    fn insert_without_value(&mut self) -> ParseError {
        if let Err(e) = self.operand() {
            return e;
        }
        self.eat_final_semicolon();
        if !matches!(self.peek().kind, TokenKind::Eof) {
            return self.trailing_error();
        }
        ParseError::new(
            "Unsupported operation: Inserting multiple items in a single statement is not supported, use \"INSERT INTO tableName VALUE item\" instead",
        )
    }

    fn update(&mut self) -> PResult<UpdateStatement> {
        let table = self.table_name()?;
        self.dml_target_rest(&table, true)?;
        self.optional_alias()?;
        let clauses = self.dml_clauses()?;
        // An UPDATE without WHERE parses; execution refuses it with the
        // full-key message, and a transaction cancels at that member with a
        // ValidationError (measured 2026-11-02 in ExecuteTransaction).
        let where_clause = if self.eat_keyword("WHERE") {
            Some(self.where_expr()?)
        } else {
            None
        };
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

    fn delete(&mut self) -> PResult<DeleteStatement> {
        if !self.eat_keyword("FROM") {
            return Err(ParseError::well_formed("Expected FROM after DELETE"));
        }
        let table = self.table_name()?;
        self.dml_target_rest(&table, false)?;
        self.optional_alias()?;
        if !self.eat_keyword("WHERE") {
            return Err(ParseError::new(
                "Where clause does not contain a mandatory equality on all key attributes",
            ));
        }
        let where_clause = self.where_expr()?;
        let returning = self.returning(true)?;
        Ok(DeleteStatement {
            table,
            where_clause,
            returning,
        })
    }

    /// The RETURNING clause of an UPDATE, or of a DELETE when `delete`.
    pub(super) fn returning(&mut self, delete: bool) -> PResult<Option<Returning>> {
        if !self.eat_keyword("RETURNING") {
            return Ok(None);
        }
        let old_new;
        let modified = self.eat_keyword("MODIFIED");
        if self.eat_keyword("ALL") {
            old_new = match self.old_or_new() {
                // A syntax error (measured 2026-11-02 with `RETURNING ALL` at
                // the end of a DELETE). An UPDATE's is not measured.
                Err(_) if delete => {
                    return Err(ParseError::well_formed(
                        "Expected ( MODIFIED | ALL ) ( NEW | OLD ) in each returning element.",
                    ));
                }
                old_new => old_new?,
            };
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

    /// The table an INSERT, UPDATE or DELETE names, quoted or not (measured
    /// 2026-11-02: unquoted names run). Where no name starts (the end of the
    /// statement, a reserved word, a literal) the service reports "Expected
    /// identifier for simple path" (measured 2026-11-02 on `DELETE FROM` with
    /// nothing, a semicolon, WHERE and other reserved words after it).
    pub(super) fn table_name(&mut self) -> PResult<String> {
        let table = match &self.peek().kind {
            TokenKind::QuotedIdent(s) => s.clone(),
            TokenKind::Ident(s) if word_class(s) == WordClass::Name => s.clone(),
            _ => {
                return Err(ParseError::well_formed(
                    "Expected identifier for simple path",
                ));
            }
        };
        self.bump();
        self.table = Some(table.clone());
        Ok(table)
    }
}
