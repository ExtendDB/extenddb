// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the overlap refusal, the bound on the disjuncts listed,
//! and the count of all of them.

use super::*;
use crate::partiql::ast::Path;

fn overlap(condition: &str) -> Result<(), &'static str> {
    let w = where_of(&select(condition));
    WhereAnalysis::new(&w, KEYS, &literal).check_overlap(&literal)
}

#[test]
fn a_non_key_branch_beside_a_sort_key_branch_on_one_partition_overlaps() {
    for cond in ["x = '1'", "x <> '1'"] {
        let condition = format!("pk = 'a' AND {cond} OR pk = 'a' AND sk = '2'");
        assert_eq!(overlap(&condition), Err(OVERLAP_MESSAGE), "{condition}");
    }
    assert_eq!(
        overlap("pk = 'a' AND (sk = a OR a = 'x')"),
        Err(OVERLAP_MESSAGE)
    );
    assert_eq!(
        overlap("pk = 'a' AND NOT (sk = 'x' AND a = 1) OR pk = 'a' AND sk = 'y'"),
        Err(OVERLAP_MESSAGE)
    );
}

#[test]
fn a_negated_or_not_equal_sort_key_branch_makes_a_scan() {
    for left in [
        "pk = 'p' AND NOT sk = '1'",
        "pk = 'p' AND sk <> '1'",
        "pk = 'p' AND NOT begins_with(sk, 'x')",
        "pk = 'p' AND NOT sk IN ['1', '3']",
        "pk = 'p' AND NOT sk > '5'",
        "pk = 'p' AND NOT (sk = '1')",
    ] {
        let condition = format!("{left} OR pk = 'p' AND sk = '2'");
        assert_eq!(overlap(&condition), Ok(()), "{condition}");
    }
    assert_eq!(overlap("pk = 'p' OR pk = 'p' AND NOT sk = '1'"), Ok(()));
}

#[test]
fn distinct_sort_key_values_and_partitions_do_not_overlap() {
    assert_eq!(overlap("pk = 'p' AND sk IN ['s1', 's3']"), Ok(()));
    assert_eq!(overlap("pk = 'a' OR pk = 'b'"), Ok(()));
    assert_eq!(
        overlap("pk = 'a' AND sk < '2' OR pk = 'a' AND sk > '2'"),
        Ok(())
    );
    assert_eq!(
        overlap("pk = 'a' AND sk <= '2' OR pk = 'a' AND '2' <= sk"),
        Err(OVERLAP_MESSAGE)
    );
}

/// `k` copies of `(<a> OR <b>)` joined with AND: 2^k branches.
fn anded_ors(k: usize, a: &str, b: &str) -> String {
    let pair = format!("({a} OR {b})");
    vec![pair; k].join(" AND ")
}

#[test]
fn forty_anded_ors_return_at_once() {
    let started = std::time::Instant::now();
    assert_eq!(overlap(&anded_ors(40, "pk = 'a'", "pk = 'b'")), Ok(()));
    // 2^40 disjuncts that read partition 'a', each one by key.
    assert_eq!(
        overlap(&anded_ors(40, "pk = 'a'", "pk = 'a'")),
        Err(OVERLAP_MESSAGE)
    );
    let values = |n: usize| {
        (0..n)
            .map(|i| format!("'v{i}'"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let ins = format!("pk IN [{}] AND sk IN [{}]", values(40), values(40));
    assert_eq!(overlap(&ins), Ok(()));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn the_branch_count_is_bounded() {
    // 2^10 branches are listed and compared; past MAX_BRANCHES the first
    // ones are compared, and the statement still reads by key when every
    // disjunct does.
    assert_eq!(1 << 10, MAX_BRANCHES);
    for k in [10, 11] {
        let w = where_of(&select(&anded_ors(k, "pk = 'a'", "pk = 'a'")));
        let analysis = WhereAnalysis::new(&w, KEYS, &literal);
        assert_eq!(analysis.check_overlap(&literal), Err(OVERLAP_MESSAGE));
        assert_eq!(analysis.reads().len(), MAX_BRANCHES, "{k}");
        assert_eq!(analysis.disjunct_count(), 1 << k);
    }
    // An IN list this long is refused by the parser (more than 100
    // members), so the condition is built here.
    let ins = |n: usize| pk_in(vec![AttributeValue::S("a".to_owned()); n]);
    let overlap_of = |w: &WhereExpr| WhereAnalysis::new(w, KEYS, &literal).check_overlap(&literal);
    assert_eq!(overlap_of(&ins(MAX_BRANCHES)), Err(OVERLAP_MESSAGE));
    assert_eq!(overlap_of(&ins(MAX_BRANCHES + 1)), Err(OVERLAP_MESSAGE));
}

/// `pk IN [<values>]`.
fn pk_in(values: Vec<AttributeValue>) -> WhereExpr {
    WhereExpr::In(
        Operand::Path(Path {
            root: "pk".to_owned(),
            steps: Vec::new(),
        }),
        values.into_iter().map(Operand::Literal).collect(),
    )
}

#[test]
fn past_the_branch_count_the_first_disjuncts_are_still_read() {
    let w = pk_in(
        (0..=MAX_BRANCHES)
            .map(|i| AttributeValue::N(i.to_string()))
            .collect(),
    );
    let analysis = WhereAnalysis::new(&w, KEYS, &literal);
    assert_eq!(analysis.reads().len(), MAX_BRANCHES);
    assert!(analysis.is_keyed());
    assert_eq!(analysis.disjunct_count(), 1025);
}

/// Whether a WHERE reads by key, and how many disjuncts it splits into.
fn tally(condition: &str, keys: KeyNames<'_>) -> (bool, u64) {
    let w = where_of(&select(condition));
    let analysis = WhereAnalysis::new(&w, keys, &literal);
    (analysis.is_keyed(), analysis.disjunct_count())
}

/// `<name> IN [<prefix>000, <prefix>001, ...]` with `n` values.
fn listed(name: &str, prefix: &str, n: usize) -> String {
    let values: Vec<String> = (0..n).map(|i| format!("'{prefix}{i:03}'")).collect();
    format!("{name} IN [{}]", values.join(", "))
}

#[test]
fn every_disjunct_is_counted_and_read_by_key_past_the_branch_count() {
    let products = format!("{} AND {}", listed("pk", "v", 100), listed("sk", "s", 100));
    for (condition, want) in [
        (listed("pk", "v", 51), (true, 51)),
        (
            format!("{} AND x = 1 AND y = 2", listed("pk", "v", 50)),
            (true, 50),
        ),
        (
            format!("{} AND {}", listed("pk", "v", 17), listed("sk", "s", 3)),
            (true, 51),
        ),
        (
            format!("{} AND (sk = 's' OR sk = 't')", listed("pk", "v", 26)),
            (true, 52),
        ),
        (
            format!("{} OR {}", listed("pk", "v", 26), listed("pk", "w", 25)),
            (true, 51),
        ),
        (products.clone(), (true, 10_000)),
        // A disjunct that is a scan makes the statement one, past the
        // listed disjuncts too.
        (format!("({products}) OR x = 1"), (false, 10_001)),
        (format!("x = 1 OR {}", listed("pk", "v", 51)), (false, 52)),
        (
            format!("{} AND {}", listed("pk", "v", 100), listed("pk", "w", 100)),
            (false, 10_000),
        ),
        (
            format!("({products}) AND (sk = 'a' OR pk = 'b')"),
            (false, 20_000),
        ),
        (
            format!("({products}) AND (n = 1 OR pk <> 'b')"),
            (false, 20_000),
        ),
        (
            format!("({products}) OR (pk = 'a' AND pk = 'b')"),
            (false, 10_001),
        ),
        (
            format!("({products}) OR (pk = 'a' AND sk = 'x')"),
            (true, 10_001),
        ),
        // The first 1024 of these read by key, the 1101st holds two sort
        // key conditions and no partition key.
        (
            format!(
                "({} OR sk = 'q') AND {}",
                listed("pk", "v", 11),
                listed("sk", "s", 100)
            ),
            (false, 1_200),
        ),
    ] {
        let shown: String = condition.chars().take(50).collect();
        assert_eq!(tally(&condition, KEYS), want, "{shown}");
    }
    assert_eq!(tally(&listed("pk", "v", 100), HASH_ONLY), (true, 100));
}

#[test]
fn a_branch_without_a_partition_equality_makes_a_scan() {
    assert_eq!(overlap("pk = 'a' OR x = 1"), Ok(()));
}
