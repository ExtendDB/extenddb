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

// A tree can be as deep as the expression is long, and the drop glue of a
// `Box` chain recurses once per level. These drops move the children onto a
// heap stack instead, so dropping never recurses.

impl Drop for Node {
    fn drop(&mut self) {
        let mut pending = Vec::new();
        detach(self, &mut pending);
        while let Some(mut node) = pending.pop() {
            detach(&mut node, &mut pending);
        }
    }
}

/// Move the child nodes of `node` onto `pending`, leaving childless leaves.
fn detach(node: &mut Node, pending: &mut Vec<Node>) {
    let leaf = || {
        Node::Function(Call {
            name: String::new(),
            args: Vec::new(),
        })
    };
    match node {
        Node::Or(l, r) | Node::And(l, r) => {
            pending.push(std::mem::replace(&mut **l, leaf()));
            pending.push(std::mem::replace(&mut **r, leaf()));
        }
        Node::Not(inner) => pending.push(std::mem::replace(&mut **inner, leaf())),
        Node::Compare { .. } | Node::Between { .. } | Node::In { .. } | Node::Function(_) => {}
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        let mut pending = std::mem::take(&mut self.args);
        while let Some(operand) = pending.pop() {
            if let Operand::Call(mut call) = operand {
                pending.append(&mut call.args);
            }
        }
    }
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

/// One open group of [`Parser::condition`]: the OR and AND operands that
/// wait for their right side, and the NOTs that wait for their operand.
#[derive(Default)]
struct Group {
    or: Option<Node>,
    and: Option<Node>,
    nots: usize,
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

    /// `condition`, with an explicit stack in place of recursion, so the
    /// nesting depth of the input never reaches the thread stack. Each `(` that
    /// opens a group pushes a [`Group`]; the group's pending OR and AND
    /// operands and its pending NOT count live there. The tokens consumed and
    /// the token an error names are those of the recursive grammar in the
    /// module documentation.
    fn condition(&mut self) -> Result<Node, String> {
        let mut groups = vec![Group::default()];
        loop {
            // The start of a `not`: NOT* then a primary.
            let top = groups.last_mut().unwrap_or_else(|| unreachable!());
            while self.kind() == &Kind::Not {
                self.bump();
                top.nots += 1;
            }
            if self.kind() == &Kind::LParen {
                self.bump();
                groups.push(Group::default());
                continue;
            }
            let mut node = self.primary()?;
            // Fold the finished primary into the open groups. A group that
            // ends at `)` becomes a primary of the group around it.
            loop {
                let group = groups.last_mut().unwrap_or_else(|| unreachable!());
                for _ in 0..std::mem::take(&mut group.nots) {
                    node = Node::Not(Box::new(node));
                }
                if let Some(left) = group.and.take() {
                    node = Node::And(Box::new(left), Box::new(node));
                }
                if self.kind() == &Kind::And {
                    self.bump();
                    group.and = Some(node);
                    break;
                }
                if let Some(left) = group.or.take() {
                    node = Node::Or(Box::new(left), Box::new(node));
                }
                if self.kind() == &Kind::Or {
                    self.bump();
                    group.or = Some(node);
                    break;
                }
                if groups.len() == 1 {
                    return Ok(node);
                }
                self.expect(&Kind::RParen)?;
                groups.pop();
            }
        }
    }

    /// A primary that does not open a group.
    fn primary(&mut self) -> Result<Node, String> {
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

    /// `operand`, with an explicit stack of the calls that are open, so a
    /// nested call such as `f(f(f(x)))` never recurses.
    fn operand(&mut self) -> Result<Operand, String> {
        let mut calls: Vec<Call> = Vec::new();
        loop {
            let mut done = match self.kind() {
                Kind::ValueRef => {
                    let value = self.text().to_owned();
                    self.bump();
                    Operand::Value(value)
                }
                Kind::Name if self.peek_kind(1) == &Kind::LParen => {
                    let name = self.text().to_owned();
                    self.bump();
                    self.bump();
                    calls.push(Call {
                        name,
                        args: Vec::new(),
                    });
                    continue;
                }
                Kind::Name | Kind::NameRef => self.path()?,
                _ => return Err(self.error()),
            };
            loop {
                let Some(open) = calls.last_mut() else {
                    return Ok(done);
                };
                open.args.push(done);
                if self.kind() == &Kind::Comma {
                    self.bump();
                    break;
                }
                self.expect(&Kind::RParen)?;
                let call = calls.pop().unwrap_or_else(|| unreachable!());
                done = Operand::Call(call);
            }
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
