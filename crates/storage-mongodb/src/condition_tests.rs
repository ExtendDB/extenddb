// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::AttributeValue;
use std::collections::HashMap;

fn make_maps(names: Vec<(&str, &str)>, values: Vec<(&str, AttributeValue)>) -> ExpressionMaps {
    let names_map: HashMap<String, String> = names
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let values_map: HashMap<String, AttributeValue> = values
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    ExpressionMaps::new(names_map, values_map)
}

#[test]
fn test_attribute_exists() {
    let maps = make_maps(vec![], vec![]);
    let expr = Expr::Function {
        name: "attribute_exists".to_string(),
        args: vec![Expr::Path(vec![PathElement::Attribute("foo".to_string())])],
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(filter, doc! { "item_data.foo": { "$exists": true } });
}

#[test]
fn test_attribute_not_exists() {
    let maps = make_maps(vec![], vec![]);
    let expr = Expr::Function {
        name: "attribute_not_exists".to_string(),
        args: vec![Expr::Path(vec![PathElement::Attribute("bar".to_string())])],
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(filter, doc! { "item_data.bar": { "$exists": false } });
}

#[test]
fn test_equality_comparison_string() {
    let maps = make_maps(
        vec![],
        vec![(":val", AttributeValue::S("hello".to_string()))],
    );
    let expr = Expr::Compare {
        left: Box::new(Expr::Path(vec![PathElement::Attribute("name".to_string())])),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder(":val".to_string())),
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(filter, doc! { "item_data.name.S": "hello" });
}

#[test]
fn test_less_than_comparison_number() {
    let maps = make_maps(vec![], vec![(":min", AttributeValue::N("100".to_string()))]);
    let expr = Expr::Compare {
        left: Box::new(Expr::Path(vec![PathElement::Attribute(
            "price".to_string(),
        )])),
        op: CompareOp::Lt,
        right: Box::new(Expr::Placeholder(":min".to_string())),
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(filter, doc! { "item_data.price.N": { "$lt": "100" } });
}

#[test]
fn test_and_condition() {
    let maps = make_maps(
        vec![],
        vec![
            (":v1", AttributeValue::S("active".to_string())),
            (":v2", AttributeValue::N("5".to_string())),
        ],
    );
    let expr = Expr::And(
        Box::new(Expr::Compare {
            left: Box::new(Expr::Path(vec![PathElement::Attribute(
                "status".to_string(),
            )])),
            op: CompareOp::Eq,
            right: Box::new(Expr::Placeholder(":v1".to_string())),
        }),
        Box::new(Expr::Compare {
            left: Box::new(Expr::Path(vec![PathElement::Attribute(
                "count".to_string(),
            )])),
            op: CompareOp::Gt,
            right: Box::new(Expr::Placeholder(":v2".to_string())),
        }),
    );
    let filter = condition_to_filter(&expr, &maps).unwrap();
    let expected = doc! {
        "$and": [
            { "item_data.status.S": "active" },
            { "item_data.count.N": { "$gt": "5" } }
        ]
    };
    assert_eq!(filter, expected);
}

#[test]
fn test_or_condition() {
    let maps = make_maps(
        vec![],
        vec![
            (":a", AttributeValue::S("x".to_string())),
            (":b", AttributeValue::S("y".to_string())),
        ],
    );
    let expr = Expr::Or(
        Box::new(Expr::Compare {
            left: Box::new(Expr::Path(vec![PathElement::Attribute("f".to_string())])),
            op: CompareOp::Eq,
            right: Box::new(Expr::Placeholder(":a".to_string())),
        }),
        Box::new(Expr::Compare {
            left: Box::new(Expr::Path(vec![PathElement::Attribute("f".to_string())])),
            op: CompareOp::Eq,
            right: Box::new(Expr::Placeholder(":b".to_string())),
        }),
    );
    let filter = condition_to_filter(&expr, &maps).unwrap();
    let expected = doc! {
        "$or": [
            { "item_data.f.S": "x" },
            { "item_data.f.S": "y" }
        ]
    };
    assert_eq!(filter, expected);
}

#[test]
fn test_not_condition() {
    let maps = make_maps(vec![], vec![]);
    let expr = Expr::Not(Box::new(Expr::Function {
        name: "attribute_exists".to_string(),
        args: vec![Expr::Path(vec![PathElement::Attribute(
            "deleted".to_string(),
        )])],
    }));
    let filter = condition_to_filter(&expr, &maps).unwrap();
    let expected = doc! {
        "$nor": [{ "item_data.deleted": { "$exists": true } }]
    };
    assert_eq!(filter, expected);
}

#[test]
fn test_begins_with() {
    let maps = make_maps(
        vec![],
        vec![(":prefix", AttributeValue::S("user#".to_string()))],
    );
    let expr = Expr::Function {
        name: "begins_with".to_string(),
        args: vec![
            Expr::Path(vec![PathElement::Attribute("sk".to_string())]),
            Expr::Placeholder(":prefix".to_string()),
        ],
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(filter, doc! { "item_data.sk.S": { "$regex": "^user#" } });
}

#[test]
fn test_between() {
    let maps = make_maps(
        vec![],
        vec![
            (":lo", AttributeValue::N("10".to_string())),
            (":hi", AttributeValue::N("20".to_string())),
        ],
    );
    let expr = Expr::Between {
        operand: Box::new(Expr::Path(vec![PathElement::Attribute("age".to_string())])),
        low: Box::new(Expr::Placeholder(":lo".to_string())),
        high: Box::new(Expr::Placeholder(":hi".to_string())),
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(
        filter,
        doc! { "item_data.age.N": { "$gte": "10", "$lte": "20" } }
    );
}

#[test]
fn test_in_condition() {
    let maps = make_maps(
        vec![],
        vec![
            (":v1", AttributeValue::S("a".to_string())),
            (":v2", AttributeValue::S("b".to_string())),
            (":v3", AttributeValue::S("c".to_string())),
        ],
    );
    let expr = Expr::In {
        operand: Box::new(Expr::Path(vec![PathElement::Attribute("x".to_string())])),
        list: vec![
            Expr::Placeholder(":v1".to_string()),
            Expr::Placeholder(":v2".to_string()),
            Expr::Placeholder(":v3".to_string()),
        ],
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(filter, doc! { "item_data.x.S": { "$in": ["a", "b", "c"] } });
}

#[test]
fn test_name_ref_resolution() {
    let maps = make_maps(
        vec![("n", "status")],
        vec![(":v", AttributeValue::S("active".to_string()))],
    );
    let expr = Expr::Compare {
        left: Box::new(Expr::Path(vec![PathElement::Attribute("#n".to_string())])),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder(":v".to_string())),
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(filter, doc! { "item_data.status.S": "active" });
}

#[test]
fn test_ne_comparison() {
    let maps = make_maps(
        vec![],
        vec![(":v", AttributeValue::S("deleted".to_string()))],
    );
    let expr = Expr::Compare {
        left: Box::new(Expr::Path(vec![PathElement::Attribute(
            "status".to_string(),
        )])),
        op: CompareOp::Ne,
        right: Box::new(Expr::Placeholder(":v".to_string())),
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(filter, doc! { "item_data.status.S": { "$ne": "deleted" } });
}

#[test]
fn test_regex_escape() {
    assert_eq!(regex_escape("user.name"), "user\\.name");
    assert_eq!(regex_escape("a+b"), "a\\+b");
    assert_eq!(regex_escape("normal"), "normal");
}

#[test]
fn test_attribute_type() {
    let maps = make_maps(vec![], vec![(":t", AttributeValue::S("S".to_string()))]);
    let expr = Expr::Function {
        name: "attribute_type".to_string(),
        args: vec![
            Expr::Path(vec![PathElement::Attribute("field".to_string())]),
            Expr::Placeholder(":t".to_string()),
        ],
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(filter, doc! { "item_data.field.S": { "$exists": true } });
}

fn path(elements: &[&str]) -> Expr {
    Expr::Path(
        elements
            .iter()
            .map(|e| match e.parse::<usize>() {
                Ok(i) => PathElement::Index(i),
                Err(_) => PathElement::Attribute((*e).to_string()),
            })
            .collect(),
    )
}

fn exists(elements: &[&str]) -> Document {
    let maps = make_maps(vec![("m", "m"), ("k", "k")], vec![]);
    let expr = Expr::Function {
        name: "attribute_exists".to_string(),
        args: vec![path(elements)],
    };
    condition_to_filter(&expr, &maps).unwrap()
}

#[test]
fn map_members_and_list_elements_use_the_typed_fields() {
    // A map member sits under the map's `M` field and a list element under
    // the list's `L` field, as `item_to_document` stores them.
    assert_eq!(
        exists(&["#m", "#k"]),
        doc! { "item_data.m.M.k": { "$exists": true } }
    );
    assert_eq!(
        exists(&["m", "mm", "k"]),
        doc! { "item_data.m.M.mm.M.k": { "$exists": true } }
    );
    assert_eq!(
        exists(&["l", "0", "k"]),
        doc! { "item_data.l.L.0.M.k": { "$exists": true } }
    );
    assert_eq!(
        exists(&["m", "n", "1"]),
        doc! { "item_data.m.M.n.L.1": { "$exists": true } }
    );
    assert_eq!(
        exists(&["l", "0"]),
        doc! { "item_data.l.L.0": { "$exists": true } }
    );
    assert_eq!(exists(&["m"]), doc! { "item_data.m": { "$exists": true } });
}

#[test]
fn comparison_on_a_map_member_uses_the_typed_field() {
    let maps = make_maps(
        vec![("m", "m"), ("k", "k")],
        vec![(":v", AttributeValue::S("inner".to_string()))],
    );
    let expr = Expr::Compare {
        left: Box::new(path(&["#m", "#k"])),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder(":v".to_string())),
    };
    let filter = condition_to_filter(&expr, &maps).unwrap();
    assert_eq!(filter, doc! { "item_data.m.M.k.S": "inner" });
}
