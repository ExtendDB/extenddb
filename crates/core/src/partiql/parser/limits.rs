// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The limits on a statement's size and shape: its length, the operators in
//! its WHERE condition and in its SET values, the nesting of a literal value,
//! and the nesting depth the parser recurses to.
//!
//! The parser recurses once per parenthesized group, literal container and
//! nested operand (a call's arguments, a reserved word that reads an
//! expression, the expression an inner bracket of a path holds). An operand
//! in parentheses nests as a group only. Runs of `NOT` and of signs, and of
//! `NOT`, `CASE` and `PIVOT` where an operand starts, are read in a loop, so
//! they cost no depth, and so are the steps of a path, the brackets of a
//! path in brackets included. A statement is first parsed on the calling
//! thread with a small depth budget; one that needs more is parsed again on
//! a thread of its own with a large stack and the full caps, so the depth an
//! accepted statement may reach does not depend on the caller's stack. Past
//! a cap the statement is refused with a syntax error instead of overflowing
//! a stack.
//!
//! Stack use per level on a debug build (measured 2026-11-24 with mincore
//! over the stack of a thread that parses one statement): 9.4 KB per group
//! round a condition, 5.6 KB per group round an operand, 5.4 KB per nested
//! call, 21.2 KB per nested `FROM ... WHERE`, 27.7 KB per level of a call
//! whose argument holds a bracket (`f(j[` in a bracket), 2.3 KB per group
//! round a literal value, 1.0 KB per group round an IN member and 2.4 to
//! 2.8 KB per literal container. The inline budget keeps a parse within
//! 700 KB of the caller's stack (a chain of `FROM ... WHERE`; groups take
//! 324 KB), and the deepest statement found within the length limit (2000
//! groups round a condition, round a bracket in a bracket, round 131 levels
//! of `f(j[`, round 104 of `FROM a WHERE`, round 1000 lists, 8181
//! characters) used 26.9 MB on its own thread. On a release build the same
//! parses used 2.8 KB, 1.6 KB, 2.0 KB, 5.3 KB, 9.3 KB, 0.5 KB, 0.4 KB and
//! 0.7 to 1.0 KB per level, 176 KB of the caller's stack and 8.0 MB for the
//! deepest statement. An expression in brackets nests as an operand does.
//! The tests parse every nesting form at the length limit on a thread with
//! half the stack of a server worker thread (see `nesting_forms` in the
//! tests).

use super::{PResult, ParseError, Parser, Statement};

/// The longest statement the service accepts, in characters: 8192 run and
/// 8193 are refused before the statement is read (measured 2026-11-02 with
/// ASCII text).
pub const MAX_STATEMENT_LENGTH: usize = 8192;

/// The most operators a WHERE condition may hold (measured 2026-11-02: 299
/// `NOT`s before `a = 1` run and 300 are refused), and the most the SET
/// values of an UPDATE may hold, counted apart (see
/// `Parser::count_value_operator`).
pub const MAX_OPERATORS: usize = 300;

/// The most members an IN list may hold (measured 2026-11-02: 101 and 1000
/// members are refused, naming the count).
pub const MAX_IN_OPERANDS: usize = 100;

/// The containers (maps and lists) a literal value may sit inside. A value
/// inside this many is refused, naming its path (measured 2026-11-02 for a
/// list and a map literal 32, 33, 34, 40, 100 and 1000 levels deep, in an
/// INSERT value, a SET value and a WHERE literal).
pub const MAX_LITERAL_NESTING: usize = 32;

/// Parenthesized groups a condition, an operand, a value or an IN member may
/// nest. The service read 2000 levels round a name and round a path; the
/// depths it was measured at are in `measured_depths` in the tests.
pub const MAX_GROUP_DEPTH: usize = 2000;

/// Literal containers (maps, lists and bags) a value may nest. The service
/// refuses a literal 1000 levels deep with the nesting message and fails at
/// 4000 (measured 2026-11-02).
pub const MAX_LITERAL_DEPTH: usize = 1000;

/// Operands nested inside operands: a call inside a call's arguments, or a
/// reserved word that reads an expression after it. An operand in
/// parentheses nests as a group, not as an operand. In a SET value the
/// operator count bounds the calls first: the service read 300 nested
/// `if_not_exists` and `list_append` calls and refused 301 nested
/// `if_not_exists` calls for their operators. It failed 700 nested `size`
/// calls in parentheses (measured 2026-11-24).
pub const MAX_OPERAND_DEPTH: usize = 500;

/// The depth (all kinds together) a parse may reach on the calling thread
/// before it moves to a thread of its own.
const INLINE_DEPTH: usize = 32;

/// The stack of the thread a deep statement is parsed on: about five times
/// the 26.9 MB the deepest statement found within the length limit used on
/// a debug build (see the module comment).
pub const LARGE_STACK_BYTES: usize = 128 << 20;

/// The reason given for a statement nested past a cap. The service fails
/// such a statement with an internal error instead (measured 2026-11-02 with
/// 4000 nested lists, and 2026-11-24 with 700 nested `size` calls in
/// parentheses and 2000 nested bags), or reads it to its end (1160 nested
/// maps, measured 2026-11-24).
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

/// The refusal of a WHERE condition, or of the SET values, with more than
/// [`MAX_OPERATORS`] operators. The service stops counting at the first one
/// past the limit.
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

    /// How many parenthesized groups are open.
    pub(super) fn groups(&self) -> usize {
        self.group
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
    /// BETWEEN, IN, IS, or a function call. A call in a SET value is counted
    /// with the SET values' operators.
    pub(super) fn count_operator(&mut self) {
        if self.in_where {
            self.operators += 1;
        }
        self.count_value_operator();
    }

    /// Count one operator of the SET values, a call or a `+` or `-`. The
    /// service counts them apart from the condition's and refuses more than
    /// [`MAX_OPERATORS`] (measured 2026-11-24: 300 nested `if_not_exists`
    /// calls run and 301 are refused, on a table that exists with three
    /// operators in the condition and on one that does not). That `+` and
    /// `-` count is the service's documented rule, not measured.
    pub(super) fn count_value_operator(&mut self) {
        if self.in_values && !self.in_where {
            self.value_operators += 1;
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
