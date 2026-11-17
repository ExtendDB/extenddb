// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The condition grammar a search condition shares with ConditionExpression,
//! parsed into a tree that keeps the operands as written.
//!
//! ```text
//! condition := and ( OR and )*
//! and       := not ( AND not )*
//! not       := NOT not | primary
//! primary   := '(' condition ')'
//!            | call                                 -- a function as a condition
//!            | operand comparator operand
//!            | operand BETWEEN operand AND operand
//!            | operand IN '(' operand ( ',' operand )* ')'
//! operand   := path | :value | call
//! call      := name '(' operand ( ',' operand )* ')'
//! path      := ( name | #name ) ( '.' ( name | #name ) | '[' digits ']' )*
//! ```
//!
//! Keywords are case-insensitive. A number is a token, but no rule takes
//! one outside a list index, so `a = 10` is a syntax error at `10`.

use super::lexer::{Kind, Tok, syntax_error};
use crate::expression::CompareOp;

/// One element of a document path, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Element {
    /// A bare name or a `#name` reference, with its `#`.
    Name(String),
    /// A list index, as its digits.
    Index(String),
}

/// An operand as written.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Operand {
    Path(Vec<Element>),
    /// A `:value` reference, with its colon.
    Value(String),
    Call(Call),
}

/// A function call.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Call {
    pub(super) name: String,
    pub(super) args: Vec<Operand>,
}

/// A condition node.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Node {
    Or(Box<Node>, Box<Node>),
    And(Box<Node>, Box<Node>),
    Not(Box<Node>),
    Compare {
        left: Operand,
        op: CompareOp,
        right: Operand,
    },
    Between {
        subject: Operand,
        low: Operand,
        high: Operand,
    },
    In {
        subject: Operand,
        list: Vec<Operand>,
    },
    Function(Call),
}

/// Parse the whole expression.
///
/// # Errors
///
/// Returns the syntax error body (see [`syntax_error`]) or the redundant
/// parentheses body, without the expression prefix.
pub(super) fn parse(src: &str, toks: &[Tok]) -> Result<Node, String> {
    if let Some(at) = toks.iter().position(|t| t.kind == Kind::Unknown) {
        return Err(syntax_error(src, toks, at));
    }
    check_redundant_parens(toks)?;
    let mut p = Parser { src, toks, pos: 0 };
    let node = p.condition()?;
    if p.kind() != &Kind::Eof {
        return Err(p.error());
    }
    Ok(node)
}

/// A parenthesised group whose whole content is another parenthesised group,
/// such as `((a = :v))`. The same rule as the other expression parsers.
fn check_redundant_parens(toks: &[Tok]) -> Result<(), String> {
    let mut stack: Vec<bool> = Vec::new();
    for (i, tok) in toks.iter().enumerate() {
        match tok.kind {
            Kind::LParen => stack.push(i > 0 && toks[i - 1].kind == Kind::LParen),
            Kind::RParen => {
                let doubled = stack.pop() == Some(true);
                if doubled && toks.get(i + 1).map(|t| &t.kind) == Some(&Kind::RParen) {
                    return Err("The expression has redundant parentheses;".to_owned());
                }
            }
            _ => {}
        }
    }
    Ok(())
}

struct Parser<'a> {
    src: &'a str,
    toks: &'a [Tok],
    pos: usize,
}

impl Parser<'_> {
    fn kind(&self) -> &Kind {
        &self.toks[self.pos].kind
    }

    fn peek_kind(&self, ahead: usize) -> &Kind {
        self.toks
            .get(self.pos + ahead)
            .map_or(&Kind::Eof, |t| &t.kind)
    }

    fn text(&self) -> &str {
        let t = &self.toks[self.pos];
        &self.src[t.start..t.end]
    }

    fn error(&self) -> String {
        syntax_error(self.src, self.toks, self.pos)
    }

    fn bump(&mut self) {
        if self.pos + 1 < self.toks.len() {
            self.pos += 1;
        }
    }

    fn expect(&mut self, kind: &Kind) -> Result<(), String> {
        if self.kind() == kind {
            self.bump();
            Ok(())
        } else {
            Err(self.error())
        }
    }

    fn condition(&mut self) -> Result<Node, String> {
        let mut left = self.and()?;
        while self.kind() == &Kind::Or {
            self.bump();
            let right = self.and()?;
            left = Node::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Node, String> {
        let mut left = self.not()?;
        while self.kind() == &Kind::And {
            self.bump();
            let right = self.not()?;
            left = Node::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn not(&mut self) -> Result<Node, String> {
        if self.kind() == &Kind::Not {
            self.bump();
            return Ok(Node::Not(Box::new(self.not()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Node, String> {
        if self.kind() == &Kind::LParen {
            self.bump();
            let inner = self.condition()?;
            self.expect(&Kind::RParen)?;
            return Ok(inner);
        }
        let subject = self.operand()?;
        let op = match self.kind() {
            Kind::Eq => CompareOp::Eq,
            Kind::Ne => CompareOp::Ne,
            Kind::Lt => CompareOp::Lt,
            Kind::Le => CompareOp::Le,
            Kind::Gt => CompareOp::Gt,
            Kind::Ge => CompareOp::Ge,
            Kind::Between => {
                self.bump();
                let low = self.operand()?;
                self.expect(&Kind::And)?;
                let high = self.operand()?;
                return Ok(Node::Between { subject, low, high });
            }
            Kind::In => {
                self.bump();
                self.expect(&Kind::LParen)?;
                let mut list = vec![self.operand()?];
                while self.kind() == &Kind::Comma {
                    self.bump();
                    list.push(self.operand()?);
                }
                self.expect(&Kind::RParen)?;
                return Ok(Node::In { subject, list });
            }
            // A function on its own is a condition: attribute_exists(a).
            _ => {
                return match subject {
                    Operand::Call(call) => Ok(Node::Function(call)),
                    Operand::Path(_) | Operand::Value(_) => Err(self.error()),
                };
            }
        };
        self.bump();
        let right = self.operand()?;
        Ok(Node::Compare {
            left: subject,
            op,
            right,
        })
    }

    fn operand(&mut self) -> Result<Operand, String> {
        match self.kind() {
            Kind::ValueRef => {
                let value = self.text().to_owned();
                self.bump();
                Ok(Operand::Value(value))
            }
            Kind::Name if self.peek_kind(1) == &Kind::LParen => {
                let name = self.text().to_owned();
                self.bump();
                self.bump();
                let mut args = vec![self.operand()?];
                while self.kind() == &Kind::Comma {
                    self.bump();
                    args.push(self.operand()?);
                }
                self.expect(&Kind::RParen)?;
                Ok(Operand::Call(Call { name, args }))
            }
            Kind::Name | Kind::NameRef => self.path(),
            _ => Err(self.error()),
        }
    }

    fn path(&mut self) -> Result<Operand, String> {
        let mut elements = vec![Element::Name(self.text().to_owned())];
        self.bump();
        loop {
            match self.kind() {
                Kind::Dot => {
                    self.bump();
                    match self.kind() {
                        Kind::Name | Kind::NameRef => {
                            elements.push(Element::Name(self.text().to_owned()));
                            self.bump();
                        }
                        _ => return Err(self.error()),
                    }
                }
                Kind::LBracket => {
                    self.bump();
                    if self.kind() != &Kind::Number {
                        return Err(self.error());
                    }
                    elements.push(Element::Index(self.text().to_owned()));
                    self.bump();
                    self.expect(&Kind::RBracket)?;
                }
                _ => return Ok(Operand::Path(elements)),
            }
        }
    }
}
