// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The ExecuteStatement response of a SELECT and the projection of its items.

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{Path, PathStep, Projection};

use crate::DispatchResult;
use crate::partiql::SingleRequest;
use crate::partiql::next_token::encode_next_token;

/// Reshape a Query/Scan `DispatchResult` into the PartiQL `ExecuteStatement`
/// response: `Items` (projected), `ConsumedCapacity`, and, when the underlying
/// read was paged, both `LastEvaluatedKey` and an opaque `NextToken` bound to
/// the request's statement and parameter values.
pub(super) fn shape_response(
    result: DispatchResult,
    req: &SingleRequest,
    projection: &Projection,
) -> Result<DispatchResult, DynamoDbError> {
    let DispatchResult { body, metrics } = result;
    let mut source = match body {
        Value::Object(m) => m,
        _ => Map::new(),
    };

    let raw_items = match source.remove("Items") {
        Some(Value::Array(items)) => items,
        _ => Vec::new(),
    };
    let items: Vec<Value> = match projection {
        Projection::All => raw_items,
        Projection::Paths(paths) => raw_items
            .iter()
            .map(|item| project_item(item, paths))
            .collect(),
    };

    let mut out = Map::new();
    out.insert("Items".to_owned(), Value::Array(items));
    if let Some(cc) = source.remove("ConsumedCapacity") {
        out.insert("ConsumedCapacity".to_owned(), cc);
    }
    if let Some(lek) = source.remove("LastEvaluatedKey") {
        out.insert(
            "NextToken".to_owned(),
            Value::String(encode_next_token(&req.statement, &req.parameters, &lek)),
        );
        out.insert("LastEvaluatedKey".to_owned(), lek);
    }

    Ok(DispatchResult {
        body: Value::Object(out),
        metrics,
    })
}

/// Project one wire-form item (`{"attr": {typed value}}`) down to the selected
/// paths. A plain or nested attribute keys on its leaf name; any path with a
/// list-index step keys on the literal path text. A path that does not resolve
/// is omitted, as the service omits a missing projected attribute.
pub(crate) fn project_item(item: &Value, paths: &[Path]) -> Value {
    let mut out = Map::new();
    for path in paths {
        if let Some(value) = navigate(item, path) {
            out.insert(output_key(path), value);
        }
    }
    Value::Object(out)
}

/// Walk a wire-form item along a document path, descending into `M` for an
/// attribute step and `L` for a list-index step. Returns the leaf value, or
/// `None` if any step is absent or the wrong shape.
fn navigate(item: &Value, path: &Path) -> Option<Value> {
    let mut current = item.get(&path.root)?;
    for step in &path.steps {
        current = match step {
            PathStep::Attribute(name) => current.get("M")?.get(name)?,
            PathStep::Index(i) => current.get("L")?.get(*i)?,
        };
    }
    Some(current.clone())
}

/// The output map key for a projected path: the literal path text when it has a
/// list-index step, otherwise the leaf attribute name.
pub(super) fn output_key(path: &Path) -> String {
    let has_index = path.steps.iter().any(|s| matches!(s, PathStep::Index(_)));
    if has_index {
        let mut text = path.root.clone();
        for step in &path.steps {
            match step {
                PathStep::Attribute(name) => {
                    text.push('.');
                    text.push_str(name);
                }
                PathStep::Index(i) => text.push_str(&format!("[{i}]")),
            }
        }
        text
    } else {
        // The leaf is the last attribute step, or the root when there are none.
        match path.steps.last() {
            Some(PathStep::Attribute(name)) => name.clone(),
            _ => path.root.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::select;
    use super::*;

    #[test]
    fn projects_nested_path_on_leaf_and_index_on_literal_text() {
        let item = serde_json::json!({
            "addr": {"M": {"city": {"S": "NYC"}}},
            "tags": {"L": [{"S": "a"}, {"S": "b"}]},
            "name": {"S": "n"}
        });
        let s = select("SELECT addr.city, tags[0] FROM \"t\" WHERE pk='a' AND sk='b'");
        let Projection::Paths(paths) = &s.projection else {
            panic!("expected paths");
        };
        let projected = project_item(&item, paths);
        assert_eq!(projected["city"]["S"], "NYC");
        assert_eq!(projected["tags[0]"]["S"], "a");
        assert!(projected.get("addr").is_none());
    }
}
