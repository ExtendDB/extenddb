// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The limits on a statement's size and shape: its length, the operators in
//! its WHERE condition, the nesting of a literal value, and the nesting depth
//! the parser recurses to.
//!
//! The parser recurses once per parenthesized group, literal container and
//! nested operand (a call's arguments, a reserved word that reads an
//! expression). Runs of `NOT` and of signs are read in a loop, so they cost no
//! depth. A statement is first parsed on the calling thread with a small depth
//! budget; one that needs more is parsed again on a thread of its own with a
//! large stack and the full caps, so the depth an accepted statement may reach
//! does not depend on the caller's stack. Past a cap the statement is refused
//! with a syntax error instead of overflowing a stack.
//!
//! Stack use per level on a debug build (measured 2026-11-02 by bisecting
//! the stack of a thread that parses one statement): 8.6 KB per
//! parenthesized group, 6.8 KB per nested operand and 2.5 to 2.8 KB per
//! literal container. The inline budget keeps a parse within 310 KB of the
//! caller's stack, and the worst parse the caps allow used 12.2 MB on its own
//! thread. On a release build the same parses used 2.3 KB, 1.8 KB and 0.5 to
//! 0.7 KB per level, 86 KB of the caller's stack and 3.1 MB at the caps.

use super::{PResult, ParseError, Parser, Statement};

/// The longest statement the service accepts, in characters: 8192 run and
/// 8193 are refused before the statement is read (measured 2026-11-02 with
/// ASCII text).
pub const MAX_STATEMENT_LENGTH: usize = 8192;

/// The most operators a WHERE condition may hold (measured 2026-11-02: 299
/// `NOT`s before `a = 1` run and 300 are refused).
pub const MAX_OPERATORS: usize = 300;

/// The most members an IN list may hold (measured 2026-11-02: 101 and 1000
/// members are refused, naming the count).
pub const MAX_IN_OPERANDS: usize = 100;

/// The containers (maps and lists) a literal value may sit inside. A value
/// inside this many is refused, naming its path (measured 2026-11-02 for a
/// list and a map literal 32, 33, 34, 40, 100 and 1000 levels deep, in an
/// INSERT value, a SET value and a WHERE literal).
pub const MAX_LITERAL_NESTING: usize = 32;

/// Parenthesized groups a condition may nest. The service runs 1000 levels
/// and fails at 2000 (measured 2026-11-02).
pub const MAX_GROUP_DEPTH: usize = 1000;

/// Literal containers (maps, lists and bags) a value may nest. The service
/// refuses a literal 1000 levels deep with the nesting message and fails at
/// 4000 (measured 2026-11-02).
pub const MAX_LITERAL_DEPTH: usize = 1000;

/// Operands nested inside operands: a call inside a call's arguments, or a
/// reserved word that reads an expression after it. The service's limit is
/// not measured.
pub const MAX_OPERAND_DEPTH: usize = 100;

/// The depth (all kinds together) a parse may reach on the calling thread
/// before it moves to a thread of its own.
const INLINE_DEPTH: usize = 32;

/// The stack of the thread a deep statement is parsed on: about five times
/// what the worst parse the caps allow (1000 groups around 100 nested
/// operands around a 1000-level literal) used on a debug build.
pub const LARGE_STACK_BYTES: usize = 64 << 20;

/// The reason given for a statement nested past a cap. The service fails
/// such a statement with an internal error instead (measured 2026-11-02 with
/// 2000 groups and 4000 nested lists).
pub const TOO_DEEP: &str = "expression nesting depth exceeded";

/// The refusal of a statement longer than [`MAX_STATEMENT_LENGTH`], echoing
/// the statement (measured 2026-11-02: 8192 characters run, 8193 are
/// refused). `None` when the statement is not too long.
#[must_use]
pub fn statement_length_refusal(statement: &str) -> Option<String> {
    (statement.chars().count() > MAX_STATEMENT_LENGTH).then(|| {
        format!(
            "1 validation error detected: Value '{statement}' at 'statement' failed to \
             satisfy constraint: Member must have length less than or equal to \
             {MAX_STATEMENT_LENGTH}"
        )
    })
}

/// The refusal of a WHERE condition with more than [`MAX_OPERATORS`]
/// operators. The service stops counting at the first one past the limit.
#[must_use]
pub fn too_many_operators() -> String {
    format!(
        "The expression contains too many operators; operator count: {}",
        MAX_OPERATORS + 1
    )
}

/// The refusal of an IN list with `members` members, more than
/// [`MAX_IN_OPERANDS`].
#[must_use]
pub fn too_many_in_operands(members: usize) -> String {
    format!("The IN operator is provided with too many operands; number of operands: {members}")
}

/// The refusal of a literal value at `path` that sits inside
/// [`MAX_LITERAL_NESTING`] containers.
#[must_use]
pub fn literal_nesting_refusal(path: &str) -> String {
    format!("Nesting Levels have exceeded supported limits under {path}")
}

/// A production that recurses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Nest {
    /// A parenthesized condition.
    Group,
    /// A map, list or bag literal.
    Literal,
    /// An operand read inside another one.
    Operand,
}

/// How deep the parser has recursed, by kind, and whether the inline budget
/// still applies.
#[derive(Debug, Default)]
pub(super) struct Nesting {
    group: usize,
    literal: usize,
    operand: usize,
    /// Open map and list literals, which the literal nesting rule counts.
    pub containers: usize,
    /// Whether this parse runs on the caller's thread, under [`INLINE_DEPTH`].
    inline: bool,
    /// Set when the inline budget ran out: the parse is abandoned and run
    /// again on a thread of its own.
    pub out_of_budget: bool,
}

impl Nesting {
    pub(super) fn new(inline: bool) -> Self {
        Self {
            inline,
            ..Self::default()
        }
    }

    fn enter(&mut self, kind: Nest) -> PResult<()> {
        let (depth, cap) = match kind {
            Nest::Group => (&mut self.group, MAX_GROUP_DEPTH),
            Nest::Literal => (&mut self.literal, MAX_LITERAL_DEPTH),
            Nest::Operand => (&mut self.operand, MAX_OPERAND_DEPTH),
        };
        if *depth >= cap {
            return Err(ParseError::well_formed(TOO_DEEP));
        }
        *depth += 1;
        if self.inline && self.group + self.literal + self.operand > INLINE_DEPTH {
            self.out_of_budget = true;
            return Err(ParseError::well_formed(TOO_DEEP));
        }
        Ok(())
    }

    /// Whether the parse is inside a parenthesized condition.
    pub(super) fn in_group(&self) -> bool {
        self.group > 0
    }

    fn leave(&mut self, kind: Nest) {
        let depth = match kind {
            Nest::Group => &mut self.group,
            Nest::Literal => &mut self.literal,
            Nest::Operand => &mut self.operand,
        };
        *depth -= 1;
    }
}

impl Parser {
    /// Run a production that recurses, one level deeper.
    pub(super) fn nested<T>(
        &mut self,
        kind: Nest,
        production: impl FnOnce(&mut Self) -> PResult<T>,
    ) -> PResult<T> {
        self.nesting.enter(kind)?;
        let result = production(self);
        self.nesting.leave(kind);
        result
    }

    /// Count one operator of the WHERE condition: AND, OR, NOT, a comparison,
    /// BETWEEN, IN, IS, or a function call.
    pub(super) fn count_operator(&mut self) {
        if self.in_where {
            self.operators += 1;
        }
    }

    /// Whether the condition is still within [`MAX_OPERATORS`]. Past it the
    /// statement is refused, so the parser reads the rest of the condition
    /// without building it, which keeps the tree it drops shallow.
    pub(super) fn within_operator_limit(&self) -> bool {
        self.operators <= MAX_OPERATORS
    }
}

/// Parse with `attempt`, on the calling thread when the statement fits the
/// inline budget and on a thread with [`LARGE_STACK_BYTES`] of stack when it
/// does not. `attempt` takes whether the parse is inline and returns `None`
/// when the inline budget ran out.
pub(super) fn parse_on_a_fitting_stack(
    attempt: impl Fn(bool) -> Option<PResult<Statement>> + Sync,
) -> PResult<Statement> {
    if let Some(result) = attempt(true) {
        return result;
    }
    std::thread::scope(|scope| {
        let spawned = std::thread::Builder::new()
            .name("partiql-parse".to_owned())
            .stack_size(LARGE_STACK_BYTES)
            .spawn_scoped(scope, || attempt(false));
        match spawned {
            Ok(handle) => match handle.join() {
                Ok(result) => result.unwrap_or_else(|| Err(ParseError::well_formed(TOO_DEEP))),
                Err(panic) => std::panic::resume_unwind(panic),
            },
            Err(_) => Err(ParseError::well_formed(TOO_DEEP)),
        }
    })
}

#[cfg(test)]
mod tests;
