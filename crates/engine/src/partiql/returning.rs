// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `MODIFIED` forms of a PartiQL `UPDATE ... RETURNING` clause.
//!
//! `RETURNING MODIFIED OLD *` and `RETURNING MODIFIED NEW *` project every
//! path the statement names (each `SET` target, including the attribute a
//! `set_add` or `set_delete` assigns, and each `REMOVE` path) against the item
//! as it was before the write (`OLD`) or after it (`NEW`). A path
//! that resolves contributes its value, a path that does not resolve
//! contributes nothing, and list elements picked by index come back packed
//! into a dense list in ascending index order, whatever order the statement
//! names them in (measured 2026-11-02). That is the rule a
//! `ProjectionExpression` follows, so the shared projection does the shaping:
//! the update handler is asked for the whole item on the matching side and the
//! statement's paths are projected over it here.

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{ExpressionMaps, PathElement, Projection};
use extenddb_core::partiql::ast::{Path, PathStep, Returning, UpdateStatement};
use extenddb_core::types::Item;

/// Replace the handler's whole-item `Attributes` with the statement's
/// `MODIFIED` projection. An empty projection removes `Attributes`, which
/// leaves the PartiQL `Items` list empty. The `ALL` forms and a statement with
/// no `RETURNING` clause leave the body unchanged.
///
/// # Errors
///
/// Returns `InternalServerError` when the handler's item does not decode.
pub(super) fn project_modified(
    body: &mut Value,
    update: &UpdateStatement,
) -> Result<(), DynamoDbError> {
    if !matches!(
        update.returning,
        Some(Returning::ModifiedOld | Returning::ModifiedNew)
    ) {
        return Ok(());
    }
    let Some(fields) = body.as_object_mut() else {
        return Ok(());
    };
    let Some(attributes) = fields.remove("Attributes") else {
        return Ok(());
    };
    let item: Item = serde_json::from_value(attributes).map_err(internal)?;
    let projected = modified_paths(&item, update)?;
    if !projected.is_empty() {
        fields.insert(
            "Attributes".to_owned(),
            serde_json::to_value(projected).map_err(internal)?,
        );
    }
    Ok(())
}

/// Project the paths an `UPDATE` names onto `item`.
fn modified_paths(item: &Item, update: &UpdateStatement) -> Result<Item, DynamoDbError> {
    let mut names = HashMap::new();
    let paths: Vec<Vec<PathElement>> = update
        .sets
        .iter()
        .map(|action| &action.path)
        .chain(&update.removes)
        .chain(update.set_members.iter().map(|action| &action.path))
        .map(|path| placeholder_path(path, &mut names))
        .collect();
    let maps = ExpressionMaps::new(names, HashMap::new());
    // A statement whose paths overlap was refused before it ran (see
    // `write::check_overlapping_paths`), so the projection does not check
    // them again.
    Ok(Projection::compile(&paths, &maps, false)?.apply(item))
}

/// A statement path in projection form, each attribute name carried through a
/// `#` placeholder so a name that itself starts with `#` is not read as one.
fn placeholder_path(path: &Path, names: &mut HashMap<String, String>) -> Vec<PathElement> {
    let mut elements = Vec::with_capacity(path.steps.len() + 1);
    elements.push(placeholder(&path.root, names));
    for step in &path.steps {
        elements.push(match step {
            PathStep::Attribute(name) => placeholder(name, names),
            PathStep::Index(index) => PathElement::Index(*index),
        });
    }
    elements
}

fn placeholder(name: &str, names: &mut HashMap<String, String>) -> PathElement {
    let key = format!("p{}", names.len());
    names.insert(key.clone(), name.to_owned());
    PathElement::Attribute(format!("#{key}"))
}

fn internal(e: serde_json::Error) -> DynamoDbError {
    tracing::error!(internal_error = %e, "failed to shape a PartiQL RETURNING item");
    DynamoDbError::InternalServerError("Internal server error".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::partiql::{Statement, parse};
    use serde_json::json;

    fn update(sql: &str) -> UpdateStatement {
        match parse(sql).unwrap() {
            Statement::Update(u) => u,
            other => panic!("not an update: {other:?}"),
        }
    }

    fn item(value: Value) -> Item {
        serde_json::from_value(value).unwrap()
    }

    fn shaped(sql: &str, attributes: Value) -> Value {
        let mut body = json!({ "Attributes": attributes });
        project_modified(&mut body, &update(sql)).unwrap();
        body
    }

    fn tags(letters: &[&str]) -> Value {
        let list: Vec<Value> = letters.iter().map(|s| json!({ "S": s })).collect();
        json!({ "L": list })
    }

    #[test]
    fn several_list_indices_pack_densely_in_ascending_index_order() {
        let new_item = json!({ "pk": {"S": "k"}, "tags": tags(&["x", "b", "y"]) });
        for sql in [
            "UPDATE \"t\" SET tags[0] = 'x', tags[2] = 'y' WHERE pk = 'k' RETURNING MODIFIED NEW *",
            "UPDATE \"t\" SET tags[2] = 'y', tags[0] = 'x' WHERE pk = 'k' RETURNING MODIFIED NEW *",
        ] {
            let body = shaped(sql, new_item.clone());
            assert_eq!(
                body,
                json!({ "Attributes": { "tags": tags(&["x", "y"]) } }),
                "{sql}"
            );
        }
    }

    #[test]
    fn modified_old_packs_the_prior_elements_the_same_way() {
        let old_item = json!({ "pk": {"S": "k"}, "tags": tags(&["a", "b", "c"]) });
        let body = shaped(
            "UPDATE \"t\" SET tags[2] = 'y', tags[0] = 'x' WHERE pk = 'k' RETURNING MODIFIED OLD *",
            old_item,
        );
        assert_eq!(body, json!({ "Attributes": { "tags": tags(&["a", "c"]) } }));
    }

    #[test]
    fn nested_paths_keep_only_the_named_leaves() {
        let new_item = json!({
            "pk": {"S": "k"},
            "m": {"M": {"a": {"N": "1"}, "b": {"N": "2"}, "keep": {"S": "k"}}}
        });
        let body = shaped(
            "UPDATE \"t\" SET m.a = 1, m.b = 2 WHERE pk = 'k' RETURNING MODIFIED NEW *",
            new_item,
        );
        assert_eq!(
            body,
            json!({ "Attributes": { "m": {"M": {"a": {"N": "1"}, "b": {"N": "2"}}} } })
        );
    }

    #[test]
    fn a_path_that_no_longer_resolves_projects_nothing() {
        // REMOVE data, then project the new item: data is gone, so there is no row.
        let body = shaped(
            "UPDATE \"t\" REMOVE data WHERE pk = 'k' RETURNING MODIFIED NEW *",
            json!({ "pk": {"S": "k"} }),
        );
        assert_eq!(body, json!({}));
    }

    #[test]
    fn set_and_remove_paths_are_both_projected() {
        let old_item = json!({
            "pk": {"S": "k"},
            "a": {"S": "old"},
            "gone": {"S": "g"},
            "other": {"S": "o"}
        });
        let body = shaped(
            "UPDATE \"t\" SET a = 'new' REMOVE gone WHERE pk = 'k' RETURNING MODIFIED OLD *",
            old_item,
        );
        assert_eq!(
            body,
            json!({ "Attributes": { "a": {"S": "old"}, "gone": {"S": "g"} } })
        );
    }

    #[test]
    fn set_add_and_set_delete_targets_are_projected() {
        let new_item = json!({
            "pk": {"S": "s"},
            "ss": {"SS": ["a", "b"]},
            "ns": {"NS": ["2"]},
            "other": {"N": "6"}
        });
        let body = shaped(
            "UPDATE \"t\" SET ss = set_add(ss, <<'b'>>) SET other = 6 WHERE pk = 's' \
             RETURNING MODIFIED NEW *",
            new_item.clone(),
        );
        assert_eq!(
            body,
            json!({ "Attributes": { "ss": {"SS": ["a", "b"]}, "other": {"N": "6"} } })
        );
        let body = shaped(
            "UPDATE \"t\" SET ns = set_delete(ns, <<1>>) WHERE pk = 's' RETURNING MODIFIED NEW *",
            new_item,
        );
        assert_eq!(body, json!({ "Attributes": { "ns": {"NS": ["2"]} } }));
    }

    #[test]
    fn a_set_the_update_empties_or_adds_projects_nothing_on_that_side() {
        // set_delete of every member removes the attribute from the new item.
        let body = shaped(
            "UPDATE \"t\" SET ss = set_delete(ss, <<'a'>>) WHERE pk = 's' RETURNING MODIFIED NEW *",
            json!({ "pk": {"S": "s"}, "other": {"N": "5"} }),
        );
        assert_eq!(body, json!({}));
        // set_add to an absent attribute: the old item does not have it.
        let body = shaped(
            "UPDATE \"t\" SET zz = set_add(zz, <<'q'>>) WHERE pk = 's' RETURNING MODIFIED OLD *",
            json!({ "pk": {"S": "s"}, "other": {"N": "5"} }),
        );
        assert_eq!(body, json!({}));
    }

    #[test]
    fn a_name_starting_with_a_hash_is_taken_literally() {
        let body = shaped(
            "UPDATE \"t\" SET \"#odd\" = 'v' WHERE pk = 'k' RETURNING MODIFIED NEW *",
            json!({ "pk": {"S": "k"}, "#odd": {"S": "v"} }),
        );
        assert_eq!(body, json!({ "Attributes": { "#odd": {"S": "v"} } }));
    }

    #[test]
    fn all_forms_pass_the_whole_item_through() {
        let whole = json!({ "pk": {"S": "k"}, "tags": tags(&["x", "b", "y"]) });
        let body = shaped(
            "UPDATE \"t\" SET tags[0] = 'x' WHERE pk = 'k' RETURNING ALL NEW *",
            whole.clone(),
        );
        assert_eq!(body, json!({ "Attributes": whole }));
    }

    #[test]
    fn projection_reads_the_item_it_is_given() {
        let projected = modified_paths(
            &item(json!({ "tags": tags(&["a", "c"]) })),
            &update("UPDATE \"t\" REMOVE tags[1] WHERE pk = 'k' RETURNING MODIFIED NEW *"),
        )
        .unwrap();
        // The removed index now holds the shifted tail element.
        assert_eq!(
            serde_json::to_value(projected).unwrap(),
            json!({ "tags": tags(&["c"]) })
        );
    }
}
