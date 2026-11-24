// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The WHERE condition: OR, AND and NOT over predicates, comparisons, type names and calls.

use super::limits::{MAX_OPERATORS, Nest};
use super::{
    AND_OR_ARGUMENTS, BARE_OPERAND, GROUPED_OPERAND, NOT_A_CONDITION, NOT_ARGUMENT, PResult,
    ParseError, Parser,
};
use crate::partiql::ast::{CompareOp, Operand, Path, TypeName, WhereExpr};
use crate::partiql::lexer::{Token, TokenKind};
use crate::partiql::operand_types::call_type;
use crate::partiql::values::{self, CONDITION_FUNCTIONS};

impl Parser {
    // --- where expression ---

    pub(super) fn where_expr(&mut self) -> PResult<WhereExpr> {
        let outer = std::mem::replace(&mut self.in_where, true);
        // A refusal that stops the parse leaves the flag set, so the
        // findings are reported in the order of the part it stopped in.
        let expr = self.or_expr()?;
        self.in_where = outer;
        if !outer {
            self.after_where = true;
        }
        Ok(expr)
    }

    pub(super) fn or_expr(&mut self) -> PResult<WhereExpr> {
        let mut left = self.and_expr()?;
        while self.eat_keyword("OR") {
            self.count_operator();
            let bare_left = std::mem::take(&mut self.bare_left);
            self.require_rhs()?;
            let right = self.and_expr()?;
            self.end_and_or(bare_left);
            if self.within_operator_limit() {
                left = WhereExpr::Or(Box::new(left), Box::new(right));
            }
        }
        Ok(left)
    }

    /// AND, any number of times, between predicates. An operand that is not
    /// a condition as the left argument of an AND or an OR is refused once
    /// the right argument has been read, after what the walk met in both
    /// (measured 2026-11-02: `a AND pk = 'a'` is refused for it, `a + 1 AND
    /// pk = 'a'` for the `+`). Only a flag is kept for it, so the frames each
    /// group nests stay small (see [`limits`]).
    fn and_expr(&mut self) -> PResult<WhereExpr> {
        let mut left = self.not_expr()?;
        while self.eat_keyword("AND") {
            self.count_operator();
            let bare_left = std::mem::take(&mut self.bare_left);
            self.require_rhs()?;
            let right = self.not_expr()?;
            self.end_and_or(bare_left);
            if self.within_operator_limit() {
                left = WhereExpr::And(Box::new(left), Box::new(right));
            }
        }
        Ok(left)
    }

    /// The rest of a condition whose first operand is a path the parser has
    /// read already, with the current token after the path: the rest of the
    /// operand, the rest of its predicate, then the ANDs and ORs after them,
    /// as [`Parser::or_expr`] reads them. Only an expression in brackets
    /// starts this way (see `Parser::rest_of_path_in_brackets`), and the
    /// condition is only checked for syntax, so nothing is built.
    pub(super) fn condition_after_path(&mut self) -> PResult<()> {
        let start = self.pos;
        let refused_operators = self.operator_refusals;
        self.after_not = false;
        let path = Operand::Path(Path {
            root: String::new(),
            steps: Vec::new(),
        });
        let left = self.operand_tail(path)?;
        self.after_operand(left, start, refused_operators)?;
        while self.eat_keyword("AND") {
            self.require_rhs()?;
            self.not_expr()?;
        }
        while self.eat_keyword("OR") {
            self.require_rhs()?;
            self.and_expr()?;
        }
        Ok(())
    }

    /// The right argument of an AND or an OR has been read; `bare_left` is
    /// whether the left one is an operand that is not a condition.
    fn end_and_or(&mut self, bare_left: bool) {
        if bare_left {
            self.walk_after_operands(AND_OR_ARGUMENTS);
        }
    }

    /// `NOT`, any number of times, before a predicate. The run is read in a
    /// loop, so its length costs no stack. The predicate is one part of the
    /// walk of the condition (see [`walk`]). The NOTs count as operators once
    /// the predicate is read, so a check of a call or of IS MISSING in it
    /// comes ahead of their count (measured 2026-11-24, each twice: after 300
    /// operators, `NOT attribute_exists(1)` and `NOT 1 IS MISSING` get the
    /// path refusal and `NOT contains(m)` the operand count).
    fn not_expr(&mut self) -> PResult<WhereExpr> {
        let mut nots = 0;
        while self.eat_keyword("NOT") {
            nots += 1;
        }
        self.after_not = nots > 0;
        let before = self.walk_part();
        let predicate = self.predicate();
        self.end_walk_part(before);
        for _ in 0..nots {
            self.count_operator();
        }
        let mut expr = predicate?;
        for _ in 0..nots.min(MAX_OPERATORS + 1) {
            expr = WhereExpr::Not(Box::new(expr));
        }
        Ok(expr)
    }

    /// A predicate: a condition in parentheses, and what may follow it, or a
    /// predicate of a term. Parentheses nest through here, so the rest is
    /// read in a function of its own, never inlined, and this frame stays
    /// small (see [`limits`]).
    ///
    /// A group that holds one operand and nothing else is that operand, and
    /// the predicate goes on from it (measured 2026-11-05: `(pk) = 'a'`
    /// reads by key, `((pk)) = 'a'`, `(a) BETWEEN 0 AND 2`, `(a) IN [1]`,
    /// `(m) IS NOT MISSING` and `NOT (a) = 2` run, and `(a)` alone is
    /// refused as `a` alone is).
    fn predicate(&mut self) -> PResult<WhereExpr> {
        if !matches!(self.peek().kind, TokenKind::LParen) {
            return self.term_predicate();
        }
        let open = self.pos;
        let after_not = self.after_not;
        let refused_operators = self.operator_refusals;
        self.bump();
        let inner = self.nested(Nest::Group, Self::where_expr)?;
        // (measured 2026-11-02: `(a = 1 b)`, `(a = 1` and `((a = 1)`)
        if !self.eat(&TokenKind::RParen) {
            return Err(ParseError::well_formed("Expected RIGHT_PAREN"));
        }
        match inner {
            WhereExpr::Function(name, mut operand) if name == GROUPED_OPERAND => {
                self.after_not = after_not;
                let left = operand.pop().expect("a grouped operand holds one operand");
                let left = self.steps_after_group(left, open)?;
                self.after_operand(left, open, refused_operators)
            }
            inner => self.after_predicate(inner),
        }
    }

    /// A predicate that is not in parentheses: an operand and what follows
    /// it. A call is an operand, its name quoted or not (measured
    /// 2026-11-24: `size(m) BETWEEN 1 AND 2` and `"Size"(m) BETWEEN 1 AND 2`
    /// run, and `size(m) + 1 = 2` and `contains(m LIKE 'x', 'a')` are
    /// refused for the operator, as their quoted forms are). A condition
    /// function with another number of operands than it takes (see
    /// [`Parser::check_operand_count`]) stands for its call and never runs.
    #[inline(never)]
    fn term_predicate(&mut self) -> PResult<WhereExpr> {
        let left_start = self.pos;
        let exec = self.is_keyword("EXEC");
        let refused_operators = self.operator_refusals;
        let left = self.operand()?;
        if exec {
            // `EXEC` reads to the end of its arguments and its refusal is
            // deferred; this condition stands in for it and never runs.
            return Ok(WhereExpr::Function("exec".to_owned(), vec![left]));
        }
        self.after_operand(left, left_start, refused_operators)
    }

    /// The rest of a predicate that starts with the operand `left`, read
    /// from token `start` on, the operand itself or a group around it. The
    /// condition operators read so far numbered `refused_operators` before
    /// the operand was read.
    #[inline(never)]
    fn after_operand(
        &mut self,
        left: Operand,
        start: usize,
        refused_operators: usize,
    ) -> PResult<WhereExpr> {
        // A refusal of the operand names its first token inside any group
        // around it (measured 2026-11-05: `(a) IS INT4` names the `a`).
        let left_span = self.inner_span(start);
        let left_start = start;
        if self.eat_keyword("IS") {
            // An IS after the test is refused for its left side (measured
            // 2026-11-02 with `a IS NULL IS NULL`). A comparison with a
            // boolean after it is not measured.
            let test = self.is_test(left, &left_span)?;
            self.trailing_is()?;
            return Ok(test);
        }
        if self.is_keyword("NOT") {
            return self.negated_predicate(left);
        }
        if self.eat_keyword("BETWEEN") {
            let between = self.between(left)?;
            return self.after_predicate(between);
        }
        if self.eat_keyword("LIKE") {
            let like = self.like(left)?;
            return self.after_predicate(like);
        }
        if self.eat_keyword("IN") {
            let in_list = self.in_list(left)?;
            return self.after_predicate(in_list);
        }
        let Some(op) = self.comparison_op() else {
            // The operand is all the group around it holds: the group is
            // the operand, and the predicate goes on after the group (see
            // [`Parser::predicate`]).
            if matches!(self.peek().kind, TokenKind::RParen)
                && self.nesting.in_group()
                && start
                    .checked_sub(1)
                    .is_some_and(|i| self.tokens[i].kind == TokenKind::LParen)
            {
                return Ok(WhereExpr::Function(GROUPED_OPERAND.to_owned(), vec![left]));
            }
            // A call to a function a condition does not take, and MISSING,
            // are refused on their own once the statement is read (measured
            // 2026-11-02: `... AND EXISTS(ss)` is "Unrecognized function"),
            // and so is an operand holding an operator a condition does not
            // take (measured 2026-11-02: `a + 1` is refused for the `+`).
            // Each stands for a predicate that never runs. A call of a
            // condition function is its predicate (see [`call_predicate`]).
            if let Operand::Function(name, args) = left {
                return Ok(call_predicate(name, args));
            }
            let missing = matches!(
                &self.tokens[left_start].kind,
                TokenKind::Ident(w) if w.eq_ignore_ascii_case("missing")
            );
            if missing {
                return Ok(WhereExpr::Function("missing".to_owned(), vec![left]));
            }
            if self.operator_refusals > refused_operators {
                return Ok(WhereExpr::Function("operator".to_owned(), vec![left]));
            }
            // Any other operand is refused, as the whole condition, as an
            // argument of AND or OR or as the argument of a NOT (measured
            // 2026-11-02 with names, paths, numbers, strings, lists, `true`
            // and a parameter as the condition, and `a AND pk = 'a'`;
            // measured 2026-11-05 in a group, `(a AND b = 2)` and `(b = 2 AND
            // a)`, and after a NOT, `NOT a`, `(NOT a)` and `NOT (a) AND pk =
            // 'a'`), after the walk (see [`walk`]). Any other token after the
            // operand ends it, and the part of the statement around the
            // operand reads that token, so a syntax error there comes first
            // (measured 2026-11-05: `a b` is "Unexpected token after
            // expression", `a LIMIT 1` is refused for the LIMIT clause and
            // `(a b)` is "Expected RIGHT_PAREN"). The check is made here, at
            // the end of the recursion, so the frames each group nests stay
            // small (see [`limits`]).
            self.refuse_bare_operand(left_start);
            return Ok(WhereExpr::Function(BARE_OPERAND.to_owned(), vec![left]));
        };
        // A call whose result is BOOL compared with a boolean is the
        // condition compared (see [`Parser::compared_with_boolean`]; measured
        // 2026-11-24: `begins_with(m, 'a') = true` reaches the table).
        let next = self.tokens.get(self.pos + 1);
        if matches!(op, CompareOp::Eq | CompareOp::Ne)
            && (is_word(next, "true") || is_word(next, "false"))
            && matches!(&left, Operand::Function(name, _) if call_type(name) == Some("BOOL"))
            && let Operand::Function(name, args) = left
        {
            return self.after_predicate(call_predicate(name, args));
        }
        let comparison = self.finish_comparison(left, op)?;
        self.after_predicate(comparison)
    }

    /// Refuse an operand that is not a condition, starting at token
    /// `start`, in its place in the walk: after everything the walk meets
    /// in the AND or OR it is an argument of, or in the whole condition.
    /// As the right argument the operand is the last thing that operator
    /// meets, so its refusal is kept now, after what the operand met and
    /// behind what the left argument met. As the left argument it is kept
    /// once the right argument has been read (see [`Parser::and_expr`]). An
    /// operand after OR and before AND is the left argument of the AND.
    ///
    /// After a NOT the operand is the NOT's argument, and its refusal is
    /// kept now (measured 2026-11-05: `NOT (a) AND pk = 'a'` is refused for
    /// the NOT's argument). A group around the operand counts as the
    /// operand: `start` is then the token of its `(`.
    fn refuse_bare_operand(&mut self, start: usize) {
        if self.after_not {
            self.walk_after_operands(NOT_ARGUMENT);
            return;
        }
        let before = start.checked_sub(1).and_then(|i| self.tokens.get(i));
        let (after_and, after_or) = (is_word(before, "AND"), is_word(before, "OR"));
        let (before_and, before_or) = (self.is_keyword("AND"), self.is_keyword("OR"));
        if after_and || (after_or && !before_and) {
            self.walk_after_operands(AND_OR_ARGUMENTS);
        } else if before_and || before_or {
            self.bare_left = true;
        } else {
            self.walk_after_operands(NOT_A_CONDITION);
        }
    }

    /// The comparison operator at the current token, if it is one.
    fn comparison_op(&self) -> Option<CompareOp> {
        Some(match self.peek().kind {
            TokenKind::Eq => CompareOp::Eq,
            TokenKind::Ne => CompareOp::Ne,
            TokenKind::Lt => CompareOp::Lt,
            TokenKind::Le => CompareOp::Le,
            TokenKind::Gt => CompareOp::Gt,
            TokenKind::Ge => CompareOp::Ge,
            _ => return None,
        })
    }

    /// The operand count check on a call in a condition, `name` as the code
    /// compares it (see [`values::call_name`]) and `args` its arguments:
    /// `attribute_exists`, `attribute_not_exists` and `size` take one
    /// operand, `attribute_type`, `begins_with` and `contains` two, and a
    /// condition function with another number is refused in its place among
    /// the operand refusals (see [`Parser::refuse_before_count`]). Measured
    /// 2026-11-23 and 2026-11-24 for each function, quoted or not, where a
    /// predicate starts; for `contains` after NOT and in parentheses; for
    /// `size`, `contains` and `attribute_exists` on the right of a
    /// comparison; and for `size` as a BETWEEN bound and as an IN member.
    /// Any other call takes any arguments. A call with the operands it takes
    /// gets the path check (see [`Parser::check_presence_operand`]). Both
    /// checks take `within`, whether the condition was within its operator
    /// limit before the call was counted. Returns whether the call has the
    /// operands it takes.
    pub(super) fn check_operand_count(
        &mut self,
        name: &str,
        args: &[Operand],
        within: bool,
    ) -> bool {
        if !CONDITION_FUNCTIONS.contains(&name) {
            return true;
        }
        let operands = match name {
            "attribute_exists" | "attribute_not_exists" | "size" => 1,
            _ => 2,
        };
        if args.len() != operands {
            self.refuse_before_count(&values::operand_count_refusal(name, args.len()), within);
            return false;
        }
        self.check_presence_operand(name, args, within);
        true
    }

    /// The operand check on `attribute_exists` and `attribute_not_exists`
    /// with one operand, `name` as the code compares it: the operand is a
    /// document path rather than a literal or a parameter, or the call is
    /// refused among the operand refusals, wherever it stands in the
    /// condition (measured 2026-11-02 where a predicate starts, and
    /// 2026-11-24 quoted or not there, on the right of a comparison, as an
    /// IN member, as a BETWEEN bound and as an operand of `size` and
    /// `contains`). The check comes ahead of the call's count as an operator
    /// (see [`Parser::refuse_before_count`]).
    fn check_presence_operand(&mut self, name: &str, args: &[Operand], within: bool) {
        if matches!(name, "attribute_exists" | "attribute_not_exists")
            && !matches!(args[0], Operand::Path(_))
        {
            self.refuse_before_count(&values::path_operand_refusal(name), within);
        }
    }

    /// The rest of a comparison of `left` by `op`, with the current token on
    /// the operator.
    fn finish_comparison(&mut self, left: Operand, op: CompareOp) -> PResult<WhereExpr> {
        self.count_operator();
        self.bump();
        self.require_rhs()?;
        let right = self.operand()?;
        Ok(WhereExpr::Compare(left, op, right))
    }

    /// The type name after `IS` / `IS NOT`: one of the service's type keywords
    /// (`DOUBLE PRECISION`, `CHARACTER VARYING` and `CHAR VARYING` are two
    /// words), else "Expected type name" (measured 2026-11-02, and 2026-11-13
    /// for `CHAR VARYING(5)`).
    pub(super) fn type_name(&mut self) -> PResult<TypeName> {
        let word = match &self.peek().kind {
            TokenKind::Ident(w) => w.to_ascii_lowercase(),
            _ => return Err(ParseError::well_formed("Expected type name")),
        };
        let second = match self.tokens.get(self.pos + 1).map(|t| &t.kind) {
            Some(TokenKind::Ident(w)) => w.to_ascii_lowercase(),
            _ => String::new(),
        };
        let (mut ty, words) = match (word.as_str(), second.as_str()) {
            ("double", "precision") => (TypeName::DoublePrecision, 2),
            ("character" | "char", "varying") => (TypeName::CharacterVarying, 2),
            (w, _) => match TypeName::from_keyword(w) {
                Some(ty) => (ty, 1),
                None => return Err(ParseError::well_formed("Expected type name")),
            },
        };
        for _ in 0..words {
            self.bump();
        }
        // A length or precision in parentheses is read and does not change
        // the type (measured 2026-11-02: `IS VARCHAR(10)` parses). A time
        // zone makes a time a type of its own (measured 2026-11-13: `IS
        // TIME(3) WITH TIME ZONE` is refused as TimeWithTimeZoneType); a
        // timestamp with one is not measured and keeps its type.
        if self.eat(&TokenKind::LParen) {
            loop {
                if !matches!(self.peek().kind, TokenKind::Number(_)) {
                    return Err(ParseError::well_formed("Expected RIGHT_PAREN"));
                }
                self.bump();
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
            if !self.eat(&TokenKind::RParen) {
                return Err(ParseError::well_formed("Expected RIGHT_PAREN"));
            }
        }
        if matches!(ty, TypeName::Time | TypeName::Timestamp) && self.eat_keyword("WITH") {
            if !(self.eat_keyword("TIME") && self.eat_keyword("ZONE")) {
                return Err(ParseError::well_formed("Expected type name"));
            }
            if ty == TypeName::Time {
                ty = TypeName::TimeWithTimeZone;
            }
        }
        Ok(ty)
    }

    pub(super) fn call_args(&mut self) -> PResult<Vec<Operand>> {
        self.arguments(Self::operand)
    }

    /// The arguments of a call, with the current token on its `(`, each read
    /// with `argument`.
    pub(super) fn arguments(
        &mut self,
        argument: fn(&mut Self) -> PResult<Operand>,
    ) -> PResult<Vec<Operand>> {
        self.eat(&TokenKind::LParen);
        let mut args = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RParen) {
            loop {
                args.push(argument(self)?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        // (measured 2026-11-02: a call left open at the end of the statement)
        if !self.eat(&TokenKind::RParen) {
            return Err(ParseError::well_formed("Expected RIGHT_PAREN"));
        }
        Ok(args)
    }
}

/// Whether `token` is the unquoted word `word`, in any case.
fn is_word(token: Option<&Token>, word: &str) -> bool {
    matches!(
        token.map(|t| &t.kind),
        Some(TokenKind::Ident(s)) if s.eq_ignore_ascii_case(word)
    )
}

/// The predicate a call stands for as a condition: `begins_with`,
/// `contains` and `attribute_type` with the two operands they take are
/// those predicates, and any other call stands for itself.
fn call_predicate(name: String, args: Vec<Operand>) -> WhereExpr {
    let predicate = match name.as_str() {
        "begins_with" => WhereExpr::BeginsWith,
        "contains" => WhereExpr::Contains,
        "attribute_type" => WhereExpr::AttributeType,
        _ => return WhereExpr::Function(name, args),
    };
    match <[Operand; 2]>::try_from(args) {
        Ok([a, b]) => predicate(a, b),
        Err(args) => WhereExpr::Function(name, args),
    }
}
