// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Literal values: maps, lists and bags, and the item map of an INSERT.

use std::collections::{BTreeMap, BTreeSet};

use super::limits::{MAX_LITERAL_NESTING, Nest, literal_nesting_refusal};
use super::{BAG_MEMBER_TYPES, LITERAL_ROOT, PResult, ParseError, Parser, terms};
use crate::partiql::ast::Operand;
use crate::partiql::lexer::{TokenKind, WordClass, word_class};
use crate::types::AttributeValue;

impl Parser {
    /// A map literal at `path` inside the value being read (`root`, `root.a`,
    /// `root[0]`, ...).
    pub(super) fn map_literal(&mut self, path: &str) -> PResult<BTreeMap<String, AttributeValue>> {
        self.nested(Nest::Literal, |p| {
            p.nesting.containers += 1;
            let map = p.map_members(path);
            p.nesting.containers -= 1;
            map
        })
    }

    fn map_members(&mut self, path: &str) -> PResult<BTreeMap<String, AttributeValue>> {
        // A name given twice is refused at the map's opening brace.
        let open = self.span();
        self.eat(&TokenKind::LBrace);
        let mut map = BTreeMap::new();
        if !matches!(self.peek().kind, TokenKind::RBrace) {
            loop {
                let key = self.map_key(path)?;
                if let Some(key) = &key
                    && map.contains_key(key)
                {
                    self.refuse_duplicate_key(key, path, &open);
                }
                if !self.eat(&TokenKind::Colon) {
                    return Err(ParseError::well_formed("Expected COLON"));
                }
                let val =
                    self.literal_value(&format!("{path}.{}", key.as_deref().unwrap_or_default()))?;
                if let Some(key) = key {
                    map.insert(key, val);
                }
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&TokenKind::RBrace) {
            return Err(ParseError::well_formed("Expected RIGHT_CURLY"));
        }
        Ok(map)
    }

    /// The key of a member of a map literal or of the item map, or `None`
    /// for a key that is refused. A string is the key, also in parentheses
    /// (measured 2026-11-23: the item `{('a'): 1}` is refused for the
    /// missing table, as `{'a': 1}` is), and a closing token is "Unexpected
    /// term" (measured 2026-11-06: `{]`, `{)`, `{,}`, `{>>` and `{'a': 1,}`,
    /// as an item, in a condition, a SET value and brackets). Another key
    /// parses and is refused once the statement parses, by its type, at its
    /// first token after any signs, parentheses and `@`; a list in
    /// parentheses at its `(`, a NOT or a `-` at the operator (measured
    /// 2026-11-23 as an item, in a map in it, in a SET value and in a
    /// condition: `{1: 1}` is "Tuple keys must be of type String. Actual
    /// type: INT under root at" the `1`, and `{1: 1, 'c': }` and `{{'a' 1}:
    /// 1}` are syntax errors). A backtick literal is refused as an operand is
    /// (see [`Parser::unsigned_atom`]). Any other token is refused at once,
    /// as an Id. The text names `path`, the place of the map in the value:
    /// `root` for the item and for a value's own map (measured 2026-11-23:
    /// `{'pk': 'q', 'm': {1: 1}}` is under root), and `root.n` or `root[0]`
    /// for a map inside it (measured 2026-11-23 with `{'m': {'n': {1: 1}}}`,
    /// `[{1: 1}]` and `SET a = {'x': {1: 1}}`).
    fn map_key(&mut self, path: &str) -> PResult<Option<String>> {
        if let Some(number) = self.signed_number() {
            let kind = if number.text.contains(['.', 'e', 'E']) {
                "DECIMAL"
            } else {
                "INT"
            };
            self.defer(&key_refusal(kind, path, &number.at));
            return Ok(None);
        }
        let at = self.span();
        match self.peek().kind.clone() {
            TokenKind::String(key) => {
                self.bump();
                return Ok(Some(key));
            }
            _ if self.at_closing_token() => return Err(self.term_error()),
            TokenKind::LParen if !self.list_in_parentheses_ahead() => {
                self.bump();
                let key = self.nested(Nest::Group, |p| p.map_key(path))?;
                if !self.eat(&TokenKind::RParen) {
                    return Err(ParseError::well_formed("Expected RIGHT_PAREN"));
                }
                return Ok(key);
            }
            open @ (TokenKind::LParen
            | TokenKind::LBracket
            | TokenKind::LBrace
            | TokenKind::LDoubleAngle) => {
                let kind = match open {
                    TokenKind::LBrace => "Struct",
                    TokenKind::LDoubleAngle => "Bag",
                    _ => "List",
                };
                self.defer(&key_refusal(kind, path, &at));
                self.literal_value(LITERAL_ROOT)?;
            }
            TokenKind::Param => {
                self.defer(&key_refusal("Parameter", path, &at));
                self.bump();
                self.take_param_index();
            }
            TokenKind::Ion(_) => {
                self.unsigned_atom(&at)?;
            }
            TokenKind::Minus => {
                self.defer(&key_refusal("Neg", path, &at));
                self.bump();
                self.operand()?;
            }
            _ if self.is_keyword("NOT") => {
                self.defer(&key_refusal("Not", path, &at));
                self.bump();
                self.operand()?;
            }
            _ if self.call_starts_here() => {
                self.defer(&key_refusal("Call", path, &at));
                self.refused_call()?;
            }
            _ => {
                // `@` needs a name before the key's type is read (measured
                // 2026-11-23: `{@1: 1}` is "Identifier must follow
                // @-operator").
                self.eat_at_sign()?;
                let kind = match &self.peek().kind {
                    _ if self.path_starts_here() => "Path",
                    TokenKind::QuotedIdent(_) => "Id",
                    _ if self.at_name() => "Id",
                    TokenKind::Ident(w) if word_class(w) == WordClass::Literal => {
                        match w.to_ascii_lowercase().as_str() {
                            "true" | "false" => "BOOL",
                            "null" => "NULL",
                            _ => "Missing",
                        }
                    }
                    _ => return Err(ParseError::new(key_refusal("Id", path, &at))),
                };
                self.defer(&key_refusal(kind, path, &at));
                self.path()?;
            }
        }
        Ok(None)
    }

    /// The top-level `VALUE { ... }` item map, returned as ordered pairs. A
    /// name given twice is refused at its second occurrence (see
    /// [`Parser::refuse_duplicate_key`]), so the pairs name each attribute
    /// once.
    pub(super) fn item_map(&mut self) -> PResult<Vec<(String, Operand)>> {
        self.eat(&TokenKind::LBrace);
        let mut pairs: Vec<(String, Operand)> = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RBrace) {
            loop {
                // (measured 2026-11-06: `VALUE {]`, `{)`, `{>>`, `{,}`,
                // `{'pk': 'a', ]` and `{'pk': 'a', 'sk': 's',}` are
                // "Unexpected term" in ExecuteStatement, a batch and a
                // transaction)
                // A string key in parentheses is refused as repeated at the
                // string (measured 2026-11-23: `{'pk': 'q', ('pk'): 'r'}` and
                // `(('pk'))` name the `'pk'`).
                let second = self.inner_span(self.pos);
                let key = self.map_key(LITERAL_ROOT)?;
                if let Some(key) = &key
                    && pairs.iter().any(|(name, _)| name == key)
                {
                    self.refuse_duplicate_key(key, LITERAL_ROOT, &second);
                }
                if !self.eat(&TokenKind::Colon) {
                    return Err(ParseError::well_formed("Expected COLON"));
                }
                // A VALUE field may be a literal or a ? parameter. A name
                // after `@` is never called (see `Parser::eat_at_sign`).
                let at_sign = self.eat_at_sign()?;
                let val = if matches!(self.peek().kind, TokenKind::Param) {
                    self.bump();
                    Operand::Parameter(self.take_param_index())
                } else if !at_sign && let Some(refusal) = self.function_as_item_value() {
                    return Err(refusal);
                } else if self.at_name_in_literal()
                    || (at_sign && self.at_name() && self.nth_kind(1) == Some(&TokenKind::LParen))
                {
                    // A name as the whole value is refused naming itself
                    // (measured 2026-11-02: `{'pk': pk}` is "Unsupported
                    // data type: pk under key root"), also with `(` after a
                    // name after `@`, which the map then refuses as a
                    // missing `}` (measured 2026-11-23 with `{'pk': 'q', 'a':
                    // @foo(b)}`). So are a quoted name and a path (see
                    // [`Parser::refuse_name_in_literal`]).
                    self.refuse_name_in_literal(LITERAL_ROOT, true)?;
                    Operand::Literal(AttributeValue::Null)
                } else {
                    Operand::Literal(self.literal_value(LITERAL_ROOT)?)
                };
                if let Some(key) = key {
                    pairs.push((key, val));
                }
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&TokenKind::RBrace) {
            return Err(ParseError::well_formed("Expected RIGHT_CURLY"));
        }
        Ok(pairs)
    }

    /// A list literal at `path` inside the value being read.
    pub(super) fn list_literal(&mut self, path: &str) -> PResult<Vec<AttributeValue>> {
        self.nested(Nest::Literal, |p| {
            p.nesting.containers += 1;
            let list = p.list_elements(path);
            p.nesting.containers -= 1;
            list
        })
    }

    fn list_elements(&mut self, path: &str) -> PResult<Vec<AttributeValue>> {
        self.eat(&TokenKind::LBracket);
        let mut items = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RBracket) {
            loop {
                let element = format!("{path}[{}]", items.len());
                items.push(self.literal_value(&element)?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        if !self.eat(&TokenKind::RBracket) {
            return Err(ParseError::well_formed("Expected RIGHT_BRACKET"));
        }
        Ok(items)
    }

    /// A bag (set) literal at `path` inside the value being read. A bag is
    /// not a container for the literal nesting rule: its members are numbers
    /// or strings.
    pub(super) fn bag_literal(&mut self, path: &str) -> PResult<AttributeValue> {
        self.nested(Nest::Literal, |p| p.bag_members(path))
    }

    fn bag_members(&mut self, path: &str) -> PResult<AttributeValue> {
        self.eat(&TokenKind::LDoubleAngle);
        let mut strings: BTreeSet<String> = BTreeSet::new();
        let mut numbers: BTreeSet<String> = BTreeSet::new();
        let mut saw_string = false;
        let mut saw_number = false;
        // Members in source order, for the service's duplicate message which
        // echoes the whole collection ("Input collection [a, a] ...").
        let mut members: Vec<String> = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RDoubleAngle) {
            let mut first = true;
            loop {
                // A member that is neither a number nor a string parses and
                // is refused at its first token after any `@`, once the
                // statement parses (measured 2026-11-02 with `<<?>>`,
                // 2026-11-13 with `<<true>>`, and 2026-11-23: `{'s':
                // <<true>>, 'c': }` is "Unexpected term"). As the first
                // member so is a name, a quoted name or a path (measured
                // 2026-11-23 with `<<a>>`, `<<@a>>`, `<<"a">>`, `<<a.b>>`,
                // `<<a, 1>>` and `<<?, 1>>` in INSERT values, SET values and
                // WHERE literals), ahead of what its steps hold. After a
                // number or a string, a name or a parameter gets its own
                // refusal from `literal_value` (measured 2026-11-23: `<<1,
                // a>>` is "Unsupported data type: Id under key root" and `<<1,
                // ?>>` the same with Parameter, at the member).
                let refusal = format!("{BAG_MEMBER_TYPES} at {}", self.span());
                self.eat_at_sign()?;
                if first && self.eat(&TokenKind::Param) {
                    self.take_param_index();
                    self.defer(&refusal);
                } else if first && self.at_name_in_literal() {
                    self.defer(&refusal);
                    self.path()?;
                } else if first
                    && matches!(
                        self.peek().kind,
                        TokenKind::LBrace | TokenKind::LBracket | TokenKind::LDoubleAngle
                    )
                {
                    // Ahead of what the map, list or bag holds (measured
                    // 2026-11-23: `<<{1: 1}>>` is the Bag text, not the key's).
                    self.defer(&refusal);
                    self.literal_value(path)?;
                } else {
                    match self.literal_value(path)? {
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
                        _ => self.defer(&refusal),
                    }
                }
                first = false;
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        // (measured 2026-11-02: `<<1.2.3>>`, a member followed by a second
        // number, in set_add and set_delete)
        if !self.eat(&TokenKind::RDoubleAngle) {
            return Err(ParseError::well_formed(
                "Expected RIGHT_DOUBLE_ANGLE_BRACKET",
            ));
        }
        if saw_string && saw_number {
            return Err(ParseError::new(BAG_MEMBER_TYPES));
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

    /// A literal value in a map/list/bag position, at `path` inside the value
    /// being read; a name there is refused (see
    /// [`Parser::at_name_in_literal`]). A value inside [`MAX_LITERAL_NESTING`]
    /// maps and lists is refused once the statement parses, naming its path;
    /// the first such value in the statement is named.
    pub(super) fn literal_value(&mut self, path: &str) -> PResult<AttributeValue> {
        if self.nesting.containers >= MAX_LITERAL_NESTING {
            self.defer(&literal_nesting_refusal(path));
        }
        if let Some(number) = self.signed_number() {
            self.note_number(&number, path);
            return Ok(AttributeValue::N(number.text));
        }
        self.eat_at_sign()?;
        match self.peek().kind.clone() {
            TokenKind::LParen => self.literal_in_parentheses(path),
            // A parameter inside a literal parses and is refused at its
            // position, naming where it sits in the value (measured
            // 2026-11-02 in INSERT values, SET values and WHERE literals:
            // `{'k':?}` is root.k, `[1,?]` is root[1]). The value stands in
            // for it and is never used.
            TokenKind::Param => {
                let at = self.span();
                self.bump();
                self.take_param_index();
                self.defer(&format!(
                    "Unsupported data type: Parameter under key {path} at {at}"
                ));
                Ok(AttributeValue::Null)
            }
            TokenKind::String(s) => {
                self.bump();
                Ok(AttributeValue::S(s))
            }
            TokenKind::LBrace => Ok(AttributeValue::M(self.map_literal(path)?)),
            TokenKind::LBracket => Ok(AttributeValue::L(self.list_literal(path)?)),
            TokenKind::LDoubleAngle => self.bag_literal(path),
            _ if self.at_name_in_literal() => {
                self.refuse_name_in_literal(path, false)?;
                Ok(AttributeValue::Null)
            }
            // (measured 2026-11-23 in INSERT values, SET values and WHERE
            // literals: `{'pk': }`, `[}`, `[1, , 2]`, `[)]`, `{'x': )}`,
            // `<<1,>>`, `{'x': >>}`, `[,]` and `()` are "Unexpected term")
            _ if self.at_closing_token() => Err(self.term_error()),
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

    /// A literal value in parentheses at `path`, with the current token on
    /// the `(`. The parentheses only group it, and a comma of their own
    /// makes a list (measured 2026-11-05: the INSERT value `{'pk': ('k'), 'x':
    /// (5), 'l': [(5)], 'p': (1, 2)}` stores k, 5, [5] and [1, 2]).
    fn literal_in_parentheses(&mut self, path: &str) -> PResult<AttributeValue> {
        if self.list_in_parentheses_ahead() {
            return Ok(AttributeValue::L(self.list_in_parentheses(path)?));
        }
        self.bump();
        let value = self.nested(Nest::Group, |p| p.literal_value(path))?;
        if !self.eat(&TokenKind::RParen) {
            return Err(ParseError::well_formed("Expected RIGHT_PAREN"));
        }
        Ok(value)
    }

    /// Whether the current token starts a name that a literal refuses (see
    /// [`Parser::refuse_name_in_literal`]): a name or a quoted name, standing
    /// alone or as the first step of a path (measured 2026-11-23 in INSERT
    /// values, SET values and WHERE literals). A call keeps the syntax error
    /// here, where DynamoDB names it (measured 2026-11-23: `[foo(1)]` is
    /// "Unsupported data type: Call under key root[0]").
    pub(super) fn at_name_in_literal(&self) -> bool {
        let named = match &self.peek().kind {
            TokenKind::Ident(word) => word_class(word) == WordClass::Name,
            TokenKind::QuotedIdent(_) => true,
            _ => false,
        };
        named && self.nth_kind(1) != Some(&TokenKind::LParen)
    }

    /// Refuse `key`, a key of the map at `path` that a member before it
    /// already names, at the position `at` (measured 2026-11-03, before the
    /// statement is authorized and with nothing written):
    ///
    /// * at the top level of an INSERT VALUE, the position of the second
    ///   occurrence, its length the quoted key's: `{'pk': 'd1', 'pk': 'd2',
    ///   'sk': 's'}` is "Duplicate keys in tuple. Key: pk under root at
    ///   1:47:4", for the partition key, the sort key and another attribute;
    /// * in any other map (one nested in a value, a map in a list, a SET
    ///   value, a WHERE literal), the position of that map's opening `{`,
    ///   length 1: in `'m': {'y': {'x': 1, 'x': 2}}` the inner `{` with
    ///   "Key: x under root.y".
    ///
    /// Names differing in case are different names. Like the other refusals
    /// of a literal, it is reported once the statement parses, in its place
    /// among them; of two maps that each repeat a name, the one whose second
    /// name comes first is reported (not measured).
    fn refuse_duplicate_key(&mut self, key: &str, path: &str, at: &str) {
        self.defer(&format!(
            "Duplicate keys in tuple. Key: {key} under {path} at {at}"
        ));
    }

    /// Read a name inside a literal (see [`Parser::at_name_in_literal`] and
    /// [`Parser::term_in_parentheses`]), at `path` inside the value: it
    /// parses and is refused at its first token (measured 2026-11-02: in `a
    /// IN ((pk, 1))` the `pk` is root[0]), as an Id, or as a Path when steps
    /// follow it. As the whole of an INSERT item value (`whole`) it is named
    /// by itself, or PATH (measured 2026-11-23: in `{'pk': 'q', 'a': "b"}`
    /// the `"b"` is b, and `b.c`, `b[0]` and `"b".c` are PATH). The refusal
    /// comes ahead of any in the steps, which are not measured. The value
    /// read stands in for it and is never used. Kept out of
    /// [`Parser::literal_value`], which nests.
    pub(super) fn refuse_name_in_literal(&mut self, path: &str, whole: bool) -> PResult<()> {
        let what = match (self.path_starts_here(), whole) {
            (true, true) => "PATH".to_owned(),
            (true, false) => "Path".to_owned(),
            (false, true) => terms::token_text(&self.peek().kind),
            (false, false) => "Id".to_owned(),
        };
        self.defer(&format!(
            "Unsupported data type: {what} under key {path} at {}",
            self.span()
        ));
        self.path().map(drop)
    }
}

/// The refusal of a key of type `kind` at `at`, of the map at `path` (see
/// [`Parser::map_key`]).
fn key_refusal(kind: &str, path: &str, at: &str) -> String {
    format!("Tuple keys must be of type String. Actual type: {kind} under {path} at {at}")
}

fn duplicate_bag_message(members: &[String]) -> String {
    // The service echoes the whole collection in source order, e.g.
    // `Input collection [a, a] contains duplicates.`
    format!(
        "One or more parameter values were invalid: Input collection [{}] contains duplicates.",
        members.join(", ")
    )
}
