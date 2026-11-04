// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which error a SearchConditionExpression gets when it has more than one
//! fault, or a fault past the condition limit.
//!
//! Every shape here was measured against the live service on 2026-10-29 or
//! 2026-10-30 in us-east-1, with an index declaring a HASH `h` and 18 inline
//! filters `f1` to `f18`, each shape sent twice with the same response both
//! times. Where ExtendDB's wording for a check differs from the service's, the
//! expectation is ExtendDB's existing message for that check, and the
//! assertion pins only that the check, not the count, answers.

use super::*;

const TOO_MANY: &str = "Invalid SearchConditionExpression: SearchConditionExpression cannot \
     have more than 1 partition key and more than 18 inline filter key comparisons";
const DUPLICATE: &str = "Invalid SearchConditionExpression: SearchConditionExpression must \
     only contain one condition per attribute";
const COMPARATOR: &str =
    "Invalid SearchConditionExpression: Invalid comparator used in SearchConditionExpression";
const RESERVED_BUCKET: &str = "Invalid SearchConditionExpression: Attribute name is a reserved \
     keyword; reserved keyword: bucket";
const NESTED: &str = "SearchConditionExpression cannot have conditions on nested attributes";
const OPERATOR: &str = "Invalid operator used in SearchConditionExpression";
const UNDEFINED_ALIAS_NOPE: &str =
    "An expression attribute name used in the expression is not defined: #nope";
/// ExtendDB's message for a syntax error and for a right side that is not a
/// value placeholder.
const BARE: &str = "Invalid SearchConditionExpression";
const UNUSED: &str = "Value provided in ExpressionAttributeValues unused in expressions";

fn undefined_value(placeholder: &str) -> String {
    format!(
        "Invalid SearchConditionExpression: An expression attribute value used in expression \
         is not defined; attribute value: {placeholder}"
    )
}

/// `h = :v0 AND f1 = :v1 ... AND f{n-1} = :v{n-1}`: the first `n` of the 19
/// schema attributes.
fn base(n: usize) -> String {
    std::iter::once("h = :v0".to_owned())
        .chain((1..n).map(|i| format!("f{i} = :v{i}")))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The 19-comparison base with `tail` appended as further comparisons.
fn past(tail: &str) -> String {
    format!("{} AND {tail}", base(19))
}

/// The 19-comparison base with its comparison at `position` (1-based) replaced.
fn replaced(n: usize, position: usize, comparison: &str) -> String {
    let mut parts: Vec<String> = base(n).split(" AND ").map(str::to_owned).collect();
    parts[position - 1] = comparison.to_owned();
    parts.join(" AND ")
}

/// A shape: the expression, its name aliases, the `:vN` placeholders left
/// undefined, and the placeholders supplied but unused.
struct Shape<'a> {
    label: &'a str,
    expr: String,
    names: &'a [(&'a str, &'a str)],
    undefined: &'a [&'a str],
    unused: &'a [&'a str],
}

fn shape<'a>(label: &'a str, expr: String) -> Shape<'a> {
    Shape {
        label,
        expr,
        names: &[],
        undefined: &[],
        unused: &[],
    }
}

/// Defines every `:vN` written in the expression except those in `undefined`.
fn run(s: &Shape<'_>) -> Result<Vec<SearchCondition>, DynamoDbError> {
    let mut values: HashMap<String, AttributeValue> = HashMap::new();
    for tok in s
        .expr
        .split(|c: char| c.is_whitespace() || "()<>=!,".contains(c))
    {
        if tok.starts_with(":v") && !s.undefined.contains(&tok) {
            values.insert(tok.to_owned(), AttributeValue::S(format!("x{}", &tok[2..])));
        }
    }
    for extra in s.unused {
        values.insert((*extra).to_owned(), AttributeValue::S("unused".to_owned()));
    }
    let names: HashMap<String, String> = s
        .names
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    validate_search_condition_expression(
        &s.expr,
        (!names.is_empty()).then_some(&names),
        Some(&values),
    )
}

fn assert_refused(shapes: &[(Shape<'_>, &str)]) {
    for (s, expected) in shapes {
        match run(s) {
            Err(DynamoDbError::ValidationException(m)) => {
                assert_eq!(m, *expected, "shape {}: {}", s.label, s.expr);
            }
            other => panic!(
                "shape {}: expected a ValidationException, got {other:?}",
                s.label
            ),
        }
    }
}

/// Live: the undefined-value and reserved-keyword checks answer wherever the
/// fault sits, including the 20th and 21st comparison.
#[test]
fn value_and_reserved_word_faults_past_the_limit_get_their_own_message() {
    let v19 = undefined_value(":v19");
    let v20 = undefined_value(":v20");
    let nope = undefined_value(":nope");
    assert_refused(&[
        (
            Shape {
                undefined: &[":v19"],
                ..shape("P4 20th x = :v19 undefined", past("x = :v19"))
            },
            &v19,
        ),
        (shape("D 20th x = :nope", past("x = :nope")), &nope),
        (
            Shape {
                undefined: &[":v20"],
                ..shape("F6 21st :v20 undefined", past("x = :v19 AND y = :v20"))
            },
            &v20,
        ),
        (
            Shape {
                undefined: &[":v19"],
                ..shape("M6f 20th x<:v19 unspaced, :v19 undefined", past("x<:v19"))
            },
            &v19,
        ),
        (
            shape("P7 20th bucket", past("bucket = :v19")),
            RESERVED_BUCKET,
        ),
        (
            shape("F7 21st bucket", past("x = :v19 AND bucket = :v20")),
            RESERVED_BUCKET,
        ),
        (
            shape("M11b 20th a.bucket", past("a.bucket = :v19")),
            RESERVED_BUCKET,
        ),
    ]);
}

/// Live: an undefined `#alias` past the limit gets the undefined-name message.
/// ExtendDB's wording for that message differs from the service's.
#[test]
fn an_undefined_alias_past_the_limit_gets_the_undefined_name_message() {
    assert_refused(&[
        (
            shape("P6 20th #nope", past("#nope = :v19")),
            UNDEFINED_ALIAS_NOPE,
        ),
        (
            shape("F9 21st #nope", past("x = :v19 AND #nope = :v20")),
            UNDEFINED_ALIAS_NOPE,
        ),
        (
            shape("M12b 20th a.#nope", past("a.#nope = :v19")),
            UNDEFINED_ALIAS_NOPE,
        ),
    ]);
}

/// Live: a syntax error after 19 or 20 comparisons gets the syntax error, not
/// the count. ExtendDB's syntax error does not name the token.
#[test]
fn a_syntax_error_past_the_limit_gets_the_syntax_error() {
    assert_refused(&[
        (
            shape("P9 19 then trailing AND", format!("{} AND", base(19))),
            BARE,
        ),
        (shape("F5 20 then trailing AND", past("x = :v19 AND")), BARE),
        (shape("P10 19 then AND x", past("x")), BARE),
        (shape("F 20th missing =", past("x :v19")), BARE),
        (shape("M9a 20th x =", past("x =")), BARE),
        (
            shape("M9b 20th trailing token", past("x = :v19 junk")),
            BARE,
        ),
        (shape("M9c 19 then AND AND", past("AND")), BARE),
        (
            shape("M10c 20th begins_with", past("begins_with(x, :v19)")),
            BARE,
        ),
        (shape("M6b 20th !=", past("x != :v19")), COMPARATOR),
    ]);
}

/// Live: OR, NOT and IN get the operator message wherever they sit.
#[test]
fn an_operator_past_the_limit_gets_the_operator_message() {
    assert_refused(&[
        (
            shape(
                "P24 OR before the 20th",
                format!("{} OR x = :v19", base(19)),
            ),
            OPERATOR,
        ),
        (
            shape(
                "F4 OR before the 2nd",
                base(19).replacen(" AND ", " OR ", 1) + " AND x = :v19",
            ),
            OPERATOR,
        ),
        (shape("M10a 20th NOT", past("NOT x = :v19")), OPERATOR),
        (shape("M10b 20th IN", past("x IN (:v19)")), OPERATOR),
    ]);
}

/// Live: nested path, comparator, value shape and repeat in the 20th
/// comparison all get the count message.
#[test]
fn per_comparison_faults_in_the_twentieth_get_the_count_message() {
    assert_refused(&[
        (shape("P8 20th a.b", past("a.b = :v19")), TOO_MANY),
        (
            Shape {
                names: &[("#nn", "a.b")],
                ..shape("F8 20th alias to a.b", past("#nn = :v19"))
            },
            TOO_MANY,
        ),
        (shape("I 20th x = v19", past("x = v19")), TOO_MANY),
        (shape("P23 20th <", past("x < :v19")), TOO_MANY),
        (shape("M6c 20th <>", past("x <> :v19")), TOO_MANY),
        (shape("M6d 20th <=", past("x <= :v19")), TOO_MANY),
        (shape("B20 20th repeats f1", past("f1 = :v19")), TOO_MANY),
        (shape("P29 20th reuses :v0", past("x = :v0")), TOO_MANY),
        (
            Shape {
                unused: &[":unused"],
                ..shape("P12 20 plus an unused value", past("x = :v19"))
            },
            TOO_MANY,
        ),
    ]);
}

/// Live: before the 20th, per-comparison faults answer in written order.
#[test]
fn per_comparison_faults_before_the_twentieth_get_their_own_message() {
    let twenty = |position, comparison| replaced(19, position, comparison) + " AND x = :v19";
    assert_refused(&[
        (
            shape("P3 repeat at 3 of 20", twenty(3, "f1 = :v2")),
            DUPLICATE,
        ),
        (
            shape("P28 repeat at 19 of 20", twenty(19, "f1 = :v18")),
            DUPLICATE,
        ),
        (
            shape("P20 nested at 2 of 20", twenty(2, "a.b = :v1")),
            NESTED,
        ),
        (shape("F1 < at 2 of 20", twenty(2, "f1 < :v1")), COMPARATOR),
        (
            shape("F1b < at 2 of 19", replaced(19, 2, "f1 < :v1")),
            COMPARATOR,
        ),
    ]);
}

/// Live: within one comparison the comparator is checked first, then the value
/// shape, then the nested path, then the repeat; across comparisons the first
/// fault in written order answers.
#[test]
fn per_comparison_checks_run_comparator_value_nested_duplicate() {
    let at3 = |second: &str, third: &str| {
        let mut parts: Vec<String> = base(19).split(" AND ").map(str::to_owned).collect();
        parts[1] = second.to_owned();
        parts[2] = third.to_owned();
        parts.join(" AND ")
    };
    assert_refused(&[
        (
            shape("M5a < at 2, nested at 3", at3("f1 < :v1", "a.b = :v2")),
            COMPARATOR,
        ),
        (
            shape("M5b nested at 2, < at 3", at3("a.b = :v1", "f2 < :v2")),
            NESTED,
        ),
        (
            shape("M5c a.b < :v1", replaced(19, 2, "a.b < :v1")),
            COMPARATOR,
        ),
        (
            shape("M5d repeat with <", replaced(19, 3, "f1 < :v2")),
            COMPARATOR,
        ),
        (
            shape("M5e < at 2, repeat at 3", at3("f1 < :v1", "h = :v2")),
            COMPARATOR,
        ),
        (shape("M5f f1 < v1", replaced(19, 2, "f1 < v1")), COMPARATOR),
        (shape("M5g a.b = v1", replaced(19, 2, "a.b = v1")), BARE),
        (
            shape("M5h repeat with v2", replaced(19, 3, "f1 = v2")),
            BARE,
        ),
        (
            shape("M5i v1 at 2, nested at 3", at3("f1 = v1", "a.b = :v2")),
            BARE,
        ),
        (
            shape("M5j nested at 2, repeat at 3", at3("a.b = :v1", "h = :v2")),
            NESTED,
        ),
        (shape("M7 f1 = v1", replaced(19, 2, "f1 = v1")), BARE),
    ]);
}

/// Live: the syntax is checked before any value, a name before its value, and
/// otherwise the first fault in written order answers.
#[test]
fn the_whole_expression_phase_runs_syntax_then_names_and_values_in_order() {
    let at3 = |second: &str, third: &str| {
        let mut parts: Vec<String> = base(19).split(" AND ").map(str::to_owned).collect();
        parts[1] = second.to_owned();
        parts[2] = third.to_owned();
        parts.join(" AND ")
    };
    let nope = undefined_value(":nope");
    let v1 = undefined_value(":v1");
    assert_refused(&[
        (
            Shape {
                undefined: &[":v1"],
                ..shape(
                    "M1 trailing AND with :v1 undefined",
                    format!("{} AND", base(18)),
                )
            },
            BARE,
        ),
        (
            shape("M2 bucket = :nope", replaced(19, 2, "bucket = :nope")),
            RESERVED_BUCKET,
        ),
        (
            shape("M3 #nope = :nope", replaced(19, 2, "#nope = :nope")),
            UNDEFINED_ALIAS_NOPE,
        ),
        (
            shape(
                "M4a bucket at 2, :nope at 3",
                at3("bucket = :v1", "f2 = :nope"),
            ),
            RESERVED_BUCKET,
        ),
        (
            shape(
                "M4b :nope at 2, bucket at 3",
                at3("f1 = :nope", "bucket = :v2"),
            ),
            &nope,
        ),
        (
            shape("M11a a.bucket at 2", replaced(19, 2, "a.bucket = :v1")),
            RESERVED_BUCKET,
        ),
        (
            shape("M12a a.#nope at 2", replaced(19, 2, "a.#nope = :v1")),
            UNDEFINED_ALIAS_NOPE,
        ),
        (
            Shape {
                undefined: &[":v1"],
                ..shape(
                    "P5 :v1 undefined at 2 of 20",
                    replaced(19, 2, "f1 = :v1") + " AND x = :v19",
                )
            },
            &v1,
        ),
        (
            shape(
                "P19 bucket at 2 of 20",
                replaced(19, 2, "bucket = :v1") + " AND x = :v19",
            ),
            RESERVED_BUCKET,
        ),
        (
            shape(
                "P21 #nope at 2 of 20",
                replaced(19, 2, "#nope = :v1") + " AND x = :v19",
            ),
            UNDEFINED_ALIAS_NOPE,
        ),
        (
            Shape {
                unused: &[":unused"],
                ..shape("P12b 19 plus an unused value", base(19))
            },
            UNUSED,
        ),
    ]);
}

/// Live: 19 comparisons are accepted with lowercase `and`, without spaces
/// around `=`, and with every name aliased; 20 get the count message.
#[test]
fn the_count_ignores_case_spacing_and_aliases() {
    let lower19 = base(19).replace(" AND ", " and ");
    let tight19 = base(19).replace(" = ", "=").replace(" AND ", "  AND  ");
    let aliased = |n: usize| {
        (0..n)
            .map(|i| format!("#n{i} = :v{i}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let names: Vec<(String, String)> = (0..20)
        .map(|i| {
            let target = match i {
                0 => "h".to_owned(),
                19 => "x".to_owned(),
                _ => format!("f{i}"),
            };
            (format!("#n{i}"), target)
        })
        .collect();
    let name_refs: Vec<(&str, &str)> = names
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    for (label, expr, count) in [
        ("P13b lowercase and, 19", lower19.clone(), 19),
        ("P14b no spaces, 19", tight19.clone(), 19),
        ("P15b aliased, 19", aliased(19), 19),
    ] {
        let s = Shape {
            names: &name_refs[..count],
            ..shape(label, expr)
        };
        assert_eq!(run(&s).map(|c| c.len()).ok(), Some(19), "shape {label}");
    }
    assert_refused(&[
        (
            shape("P13 lowercase and, 20", format!("{lower19} and x = :v19")),
            TOO_MANY,
        ),
        (
            shape("P14 no spaces, 20", format!("{tight19}  AND  x=:v19")),
            TOO_MANY,
        ),
        (
            Shape {
                names: &name_refs,
                ..shape("P15 aliased, 20", aliased(20))
            },
            TOO_MANY,
        ),
    ]);
}
