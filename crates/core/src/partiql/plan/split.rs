// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The split of a WHERE clause into the disjuncts read separately, and the key read of each.

use super::roles::{Role, bare_key, is_key_condition, is_value, key_comparison, role};
use super::{KeyNames, KeyRead, MAX_BRANCHES, SortCondition, same_value};
use crate::partiql::ast::{CompareOp, Operand, WhereExpr};
use crate::types::AttributeValue;

/// One condition of a disjunct, with its role. An IN on a key attribute
/// contributes one value per disjunct and no WHERE term of its own.
#[derive(Debug, Clone, Copy)]
pub(super) struct Conjunct<'a> {
    term: Option<&'a WhereExpr>,
    role: Role<'a>,
}

#[derive(Debug, Clone)]
pub(super) struct Branch<'a> {
    pub(super) conjuncts: Vec<Conjunct<'a>>,
    has_key: bool,
}

/// The first [`MAX_BRANCHES`] disjuncts of a WHERE, in statement order,
/// whether they are all of them, and the tally of all of them.
pub(super) struct Split<'a> {
    pub(super) branches: Vec<Branch<'a>>,
    pub(super) complete: bool,
    pub(super) tally: Tally<'a>,
}

impl<'a> Split<'a> {
    fn one(term: &'a WhereExpr, role: Role<'a>, has_key: bool) -> Self {
        Self {
            branches: vec![Branch {
                conjuncts: vec![Conjunct {
                    term: Some(term),
                    role,
                }],
                has_key,
            }],
            complete: true,
            tally: Tally::of(role),
        }
    }
}

/// What the key reading needs to know of every disjunct of a WHERE, those
/// past the first [`MAX_BRANCHES`] too: how many there are, and whether each
/// one reads by key (see [`key_read`]). It is kept without listing the
/// disjuncts, whose number AND multiplies.
#[derive(Debug, Clone, Copy)]
pub(super) struct Tally<'a> {
    /// How many disjuncts there are, at most `u64::MAX`.
    pub(super) count: u64,
    /// Whether some disjunct is a scan for what it holds: a predicate on a
    /// key attribute that is no key condition, two partition key values, or
    /// two sort key conditions.
    clash: bool,
    /// Whether some disjunct holds no equality of the partition key.
    unkeyed: bool,
    /// The partition key values the disjuncts hold.
    partitions: Held<&'a Operand>,
    /// The sort key conditions the disjuncts hold.
    sorts: Held<SortCondition<'a>>,
}

impl<'a> Tally<'a> {
    /// The tally of no disjunct, which an OR with any other leaves as it is.
    const NONE: Self = Self {
        count: 0,
        clash: false,
        unkeyed: false,
        partitions: Held::Nothing,
        sorts: Held::Nothing,
    };

    /// The tally of one disjunct of one condition.
    fn of(role: Role<'a>) -> Self {
        let mut tally = Self {
            count: 1,
            clash: false,
            unkeyed: true,
            partitions: Held::Nothing,
            sorts: Held::Nothing,
        };
        match role {
            Role::Partition(value) => {
                tally.unkeyed = false;
                tally.partitions = Held::One(value);
            }
            Role::Sort(condition) => tally.sorts = Held::One(condition),
            Role::OnKey => tally.clash = true,
            Role::Compared(_) | Role::KeyAndPath(_) | Role::IntoKey(_) | Role::Filter => {}
        }
        tally
    }

    /// The tally of the disjuncts of either side of an OR.
    fn or(self, other: Self, value_of: &dyn Fn(&Operand) -> Option<AttributeValue>) -> Self {
        Self {
            count: self.count.saturating_add(other.count),
            clash: self.clash || other.clash,
            unkeyed: self.unkeyed || other.unkeyed,
            partitions: self
                .partitions
                .either(other.partitions, |a, b| same_value(a, b, value_of)),
            sorts: self
                .sorts
                .either(other.sorts, |a, b| same_condition(a, b, value_of)),
        }
    }

    /// The tally of the disjuncts of an AND: each disjunct of one side with
    /// each disjunct of the other.
    fn and(self, other: Self, value_of: &dyn Fn(&Operand) -> Option<AttributeValue>) -> Self {
        let same_partition = |a: &&Operand, b: &&Operand| same_value(a, b, value_of);
        let same_sort =
            |a: &SortCondition<'_>, b: &SortCondition<'_>| same_condition(a, b, value_of);
        Self {
            count: self.count.saturating_mul(other.count),
            clash: self.clash
                || other.clash
                || self.partitions.clash(other.partitions, same_partition)
                || self.sorts.clash(other.sorts, same_sort),
            unkeyed: self.unkeyed && other.unkeyed,
            partitions: self.partitions.either(other.partitions, same_partition),
            sorts: self.sorts.either(other.sorts, same_sort),
        }
    }

    /// Whether every disjunct reads by key.
    pub(super) fn reads_by_key(&self) -> bool {
        !self.clash && !self.unkeyed
    }
}

/// The values of one key attribute that a set of disjuncts holds: none, one,
/// or more than one distinct value.
#[derive(Debug, Clone, Copy)]
enum Held<T> {
    Nothing,
    One(T),
    Several,
}

impl<T: Copy> Held<T> {
    /// The values the disjuncts of two sets hold together.
    fn either(self, other: Self, same: impl Fn(&T, &T) -> bool) -> Self {
        match (self, other) {
            (Self::Nothing, held) | (held, Self::Nothing) => held,
            (Self::One(a), Self::One(b)) if same(&a, &b) => Self::One(a),
            _ => Self::Several,
        }
    }

    /// Whether a disjunct of one set ANDed with a disjunct of the other can
    /// hold two distinct values.
    fn clash(self, other: Self, same: impl Fn(&T, &T) -> bool) -> bool {
        match (self, other) {
            (Self::Nothing, _) | (_, Self::Nothing) => false,
            (Self::One(a), Self::One(b)) => !same(&a, &b),
            _ => true,
        }
    }
}

/// Split a WHERE into the disjuncts the service reads separately. The
/// recursion follows the AND and OR nesting, which the operator limit keeps
/// within 300 levels; a run of NOTs is one condition. `value_of` resolves a
/// value as [`key_read`] does, for the tally.
pub(super) fn split<'a>(
    expr: &'a WhereExpr,
    keys: KeyNames<'_>,
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> Split<'a> {
    match expr {
        WhereExpr::Or(a, b) => {
            let left = split(a, keys, value_of);
            let right = split(b, keys, value_of);
            let splits = !left.complete
                || !right.complete
                || left
                    .branches
                    .iter()
                    .chain(&right.branches)
                    .any(|b| b.has_key);
            if !splits {
                return Split::one(expr, role(expr, keys), false);
            }
            let complete = left.complete
                && right.complete
                && left.branches.len() + right.branches.len() <= MAX_BRANCHES;
            let tally = left.tally.or(right.tally, value_of);
            let mut branches = left.branches;
            if left.complete {
                branches.extend(right.branches);
            }
            branches.truncate(MAX_BRANCHES);
            Split {
                branches,
                complete,
                tally,
            }
        }
        WhereExpr::And(a, b) => {
            let left = split(a, keys, value_of);
            let right = split(b, keys, value_of);
            let complete = left.complete
                && right.complete
                && left.branches.len().saturating_mul(right.branches.len()) <= MAX_BRANCHES;
            Split {
                branches: product(&left.branches, &right.branches),
                complete,
                tally: left.tally.and(right.tally, value_of),
            }
        }
        WhereExpr::In(left, items) => match bare_key(left, keys) {
            Some(name) if items.iter().all(is_value) => {
                let equality = |item| key_comparison(name, CompareOp::Eq, item, keys);
                let tally = items.iter().fold(Tally::NONE, |tally, item| {
                    tally.or(Tally::of(equality(item)), value_of)
                });
                Split {
                    branches: items
                        .iter()
                        .take(MAX_BRANCHES)
                        .map(|item| Branch {
                            conjuncts: vec![Conjunct {
                                term: None,
                                role: equality(item),
                            }],
                            has_key: true,
                        })
                        .collect(),
                    complete: items.len() <= MAX_BRANCHES,
                    tally,
                }
            }
            _ => {
                let role = role(expr, keys);
                Split::one(expr, role, role.splits())
            }
        },
        // A negated condition is never split (measured 2026-11-02: a branch
        // whose sort-key condition is negated makes the statement run).
        WhereExpr::Not(_) => Split::one(expr, role(expr, keys), false),
        // A key condition compared with a boolean is read by key, and the
        // comparison goes with its disjunct, in an OR as anywhere else
        // (measured 2026-11-03: `pk = 'a' <> true OR pk = 'b'` reads both
        // partitions). Any other condition compared with a boolean is the
        // condition when the comparison keeps it, and is read as a NOT when
        // it negates it.
        WhereExpr::ComparedWithBoolean(inner, comparison) => {
            if is_key_condition(inner, keys) {
                let mut split = split(inner, keys, value_of);
                for branch in &mut split.branches {
                    branch.conjuncts.push(Conjunct {
                        term: None,
                        role: Role::Compared(*comparison),
                    });
                }
                split
            } else if comparison.negates() {
                Split::one(expr, role(expr, keys), false)
            } else {
                split(inner, keys, value_of)
            }
        }
        other => {
            let role = role(other, keys);
            Split::one(other, role, role.splits())
        }
    }
}

/// The disjuncts of an AND: each disjunct of one side with each disjunct of
/// the other, in statement order, the first [`MAX_BRANCHES`] of them.
fn product<'a>(left: &[Branch<'a>], right: &[Branch<'a>]) -> Vec<Branch<'a>> {
    let mut branches = Vec::new();
    'product: for l in left {
        for r in right {
            if branches.len() == MAX_BRANCHES {
                break 'product;
            }
            let mut conjuncts = l.conjuncts.clone();
            conjuncts.extend(r.conjuncts.iter().copied());
            branches.push(Branch {
                conjuncts,
                has_key: l.has_key || r.has_key,
            });
        }
    }
    branches
}

/// The key read of one disjunct, or `None` when the disjunct is a scan.
pub(super) fn key_read<'a>(
    conjuncts: &[Conjunct<'a>],
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> Option<KeyRead<'a>> {
    let mut partition: Option<&'a Operand> = None;
    let mut sort: Option<SortCondition<'a>> = None;
    let mut filter = Vec::new();
    let mut comparisons = Vec::new();
    for conjunct in conjuncts {
        match conjunct.role {
            Role::Partition(value) => match partition {
                Some(existing) if !same_value(existing, value, value_of) => return None,
                Some(_) => {}
                None => partition = Some(value),
            },
            Role::Compared(comparison) => comparisons.push(comparison),
            Role::Sort(condition) => match &sort {
                Some(existing) if !same_condition(existing, &condition, value_of) => return None,
                Some(_) => {}
                None => sort = Some(condition),
            },
            Role::OnKey => return None,
            Role::KeyAndPath(_) | Role::IntoKey(_) | Role::Filter => {
                filter.extend(conjunct.term);
            }
        }
    }
    Some(KeyRead {
        partition: partition?,
        sort,
        filter,
        comparisons,
    })
}

fn same_condition(
    a: &SortCondition<'_>,
    b: &SortCondition<'_>,
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> bool {
    match (a, b) {
        (SortCondition::Compare(op1, v1), SortCondition::Compare(op2, v2)) => {
            op1 == op2 && same_value(v1, v2, value_of)
        }
        (SortCondition::Between(l1, h1), SortCondition::Between(l2, h2)) => {
            same_value(l1, l2, value_of) && same_value(h1, h2, value_of)
        }
        (SortCondition::BeginsWith(p), SortCondition::BeginsWith(q)) => same_value(p, q, value_of),
        _ => false,
    }
}
