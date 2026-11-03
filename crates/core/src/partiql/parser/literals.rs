// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Literal values: maps, lists and bags, and the item map of an INSERT.

use std::collections::{BTreeMap, BTreeSet};

use super::limits::{MAX_LITERAL_NESTING, Nest, literal_nesting_refusal};
use super::{BAG_MEMBER_TYPES, LITERAL_ROOT, PResult, ParseError, Parser};
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
                let val = self.literal_value(&format!("{path}.{key}"))?;
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
    pub(super) fn item_map(&mut self) -> PResult<Vec<(String, Operand)>> {
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
                } else if let Some(refusal) = self.function_as_item_value() {
                    return Err(refusal);
                } else if let TokenKind::Ident(name) = &self.peek().kind
                    && self.at_name_in_literal(name)
                {
                    // A name as the whole value is refused naming itself
                    // (measured 2026-11-02: `{'pk': pk}` is "Unsupported
                    // data type: pk under key root").
                    let refusal = format!(
                        "Unsupported data type: {name} under key {LITERAL_ROOT} at {}",
                        self.span()
                    );
                    self.bump();
                    self.defer(&refusal);
                    Operand::Literal(AttributeValue::Null)
                } else {
                    Operand::Literal(self.literal_value(LITERAL_ROOT)?)
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
            return Err(ParseError::well_formed("Expected ] to close list literal"));
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
            loop {
                // A parameter as a member parses and is refused at its
                // position (measured 2026-11-02 with `<<?>>`).
                if matches!(self.peek().kind, TokenKind::Param) {
                    let at = self.span();
                    self.bump();
                    self.take_param_index();
                    self.defer(&format!("{BAG_MEMBER_TYPES} at {at}"));
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
                        _ => return Err(ParseError::new(BAG_MEMBER_TYPES)),
                    }
                }
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

    /// A literal value in a map/list/bag position (no paths), at `path` inside
    /// the value being read. A value inside [`MAX_LITERAL_NESTING`] maps and
    /// lists is refused once the statement parses, naming its path; the
    /// first such value in the statement is named.
    pub(super) fn literal_value(&mut self, path: &str) -> PResult<AttributeValue> {
        if self.nesting.containers >= MAX_LITERAL_NESTING {
            self.defer(&literal_nesting_refusal(path));
        }
        if let Some(n) = self.signed_number() {
            self.walk_number(&n, path);
            return Ok(AttributeValue::N(n));
        }
        match self.peek().kind.clone() {
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
            TokenKind::Ident(name) if self.at_name_in_literal(&name) => {
                self.refuse_name_in_literal(path);
                Ok(AttributeValue::Null)
            }
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

    /// Whether the current token, the word `word`, is a name standing alone
    /// inside a literal. A name that starts a path or a call is not measured
    /// there, and keeps the syntax error.
    pub(super) fn at_name_in_literal(&self, word: &str) -> bool {
        word_class(word) == WordClass::Name
            && !matches!(
                self.tokens.get(self.pos + 1).map(|t| &t.kind),
                Some(TokenKind::Dot | TokenKind::LBracket | TokenKind::LParen)
            )
    }

    /// Read a name inside a literal, at `path` inside the value: it parses
    /// and is refused at its position (measured 2026-11-02: in `a IN ((pk,
    /// 1))` the `pk` is root[0]). The value read stands in for it and is
    /// never used. Kept out of [`Parser::literal_value`], which nests.
    fn refuse_name_in_literal(&mut self, path: &str) {
        let at = self.span();
        self.bump();
        self.defer(&format!(
            "Unsupported data type: Id under key {path} at {at}"
        ));
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
