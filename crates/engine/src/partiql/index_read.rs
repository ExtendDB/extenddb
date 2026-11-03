// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `SELECT` through a secondary index that does not project every attribute
//! (`KEYS_ONLY` or `INCLUDE`).
//!
//! Three rules, all measured on the service 2026-11-02:
//!
//! * A keyed read (one the service runs as a Query of the index) whose filter
//!   names an attribute the index does not project is refused, on a GSI and an
//!   LSI alike. Unkeyed, the read is a scan of the index and the filter is
//!   evaluated against the index rows.
//! * A GSI read whose projection names such an attribute is refused, keyed or
//!   not. The filter refusal is checked first.
//! * An LSI read whose projection names such an attribute reads back every
//!   item it walks from the base table and serves the projection and the
//!   filter from that item (see `base_join`).

use std::collections::BTreeSet;

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{Operand, Projection, SelectStatement, WhereExpr};
use extenddb_core::types::{IndexInfo, IndexType, KeySchemaElement, ProjectionType};

/// Refuse a read of `info` that names an attribute the index does not
/// project, in the filter of a keyed read or in the projection of a GSI read.
///
/// `keyed_filter` is the filter a keyed read of the index carries (see
/// `WhereAnalysis::shared_filter`): the conditions beside the key conditions
/// at the top of the WHERE, so a condition inside one OR branch is not
/// refused. `None` is a scan of the index, whose filter is evaluated against
/// the index rows.
///
/// # Errors
///
/// Returns the service's `ValidationException` naming the index and the
/// attributes it does not project.
pub(super) fn check(
    stmt: &SelectStatement,
    info: &IndexInfo,
    base_key_schema: &[KeySchemaElement],
    keyed_filter: Option<&[&WhereExpr]>,
) -> Result<(), DynamoDbError> {
    let Some(projected) = projected_attributes(info, base_key_schema) else {
        return Ok(());
    };
    if let Some(filter) = keyed_filter {
        let mut roots = Vec::new();
        for term in filter {
            where_roots(term, &mut roots);
        }
        let missing = unprojected(roots, &projected);
        if !missing.is_empty() {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: Secondary index {} does not \
                 project one or more filter attributes: [{}]",
                info.index_name,
                missing.join(", ")
            )));
        }
    }
    if info.index_type == IndexType::Gsi
        && let Projection::Paths(paths) = &stmt.projection
    {
        let missing = unprojected(paths.iter().map(|p| p.root.as_str()), &projected);
        if !missing.is_empty() {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: Global secondary index {} does \
                 not project [{}]",
                info.index_name,
                missing.join(", ")
            )));
        }
    }
    Ok(())
}

/// Whether a read of `info` must read each walked item back from the base
/// table: an LSI whose projection names an attribute the index does not hold.
/// `SELECT *` is served from the index alone.
pub(super) fn reaches_back(
    projection: &Projection,
    info: &IndexInfo,
    base_key_schema: &[KeySchemaElement],
) -> bool {
    if info.index_type != IndexType::Lsi {
        return false;
    }
    match (projection, projected_attributes(info, base_key_schema)) {
        (Projection::Paths(paths), Some(projected)) => {
            paths.iter().any(|p| !projected.contains(p.root.as_str()))
        }
        _ => false,
    }
}

/// The attributes an index holds (table key, index key and any `INCLUDE`
/// attributes), or `None` when it projects every attribute.
fn projected_attributes<'a>(
    info: &'a IndexInfo,
    base_key_schema: &'a [KeySchemaElement],
) -> Option<BTreeSet<&'a str>> {
    if info.projection.projection_type == ProjectionType::All {
        return None;
    }
    let mut held: BTreeSet<&str> = base_key_schema
        .iter()
        .chain(&info.key_schema)
        .map(|k| k.attribute_name.as_str())
        .collect();
    if let Some(non_key) = &info.projection.non_key_attributes {
        held.extend(non_key.iter().map(String::as_str));
    }
    Some(held)
}

/// The names in `names` the index does not hold, deduplicated, in the order
/// the statement first names them.
fn unprojected<'a>(
    names: impl IntoIterator<Item = &'a str>,
    held: &BTreeSet<&str>,
) -> Vec<&'a str> {
    let mut missing: Vec<&str> = Vec::new();
    for name in names {
        if !held.contains(name) && !missing.contains(&name) {
            missing.push(name);
        }
    }
    missing
}

/// The top-level attribute name of every path a WHERE term references.
pub(super) fn where_roots<'a>(expr: &'a WhereExpr, out: &mut Vec<&'a str>) {
    match expr {
        WhereExpr::Compare(a, _, b)
        | WhereExpr::BeginsWith(a, b)
        | WhereExpr::Contains(a, b)
        | WhereExpr::AttributeType(a, b) => {
            operand_roots(a, out);
            operand_roots(b, out);
        }
        WhereExpr::Between(a, low, high) => {
            operand_roots(a, out);
            operand_roots(low, out);
            operand_roots(high, out);
        }
        WhereExpr::In(a, items) => {
            operand_roots(a, out);
            items.iter().for_each(|i| operand_roots(i, out));
        }
        WhereExpr::Function(_, args) => args.iter().for_each(|a| operand_roots(a, out)),
        WhereExpr::IsMissing(p, _) => out.push(p.root.as_str()),
        WhereExpr::IsType(a, _, _) => operand_roots(a, out),
        WhereExpr::And(a, b) | WhereExpr::Or(a, b) => {
            where_roots(a, out);
            where_roots(b, out);
        }
        WhereExpr::Not(a) | WhereExpr::ComparedWithBoolean(a, _) => where_roots(a, out),
    }
}

/// The top-level attribute name of every path an operand references.
pub(super) fn operand_roots<'a>(op: &'a Operand, out: &mut Vec<&'a str>) {
    match op {
        Operand::Path(p) => out.push(p.root.as_str()),
        Operand::Arith(a, _, b) => {
            operand_roots(a, out);
            operand_roots(b, out);
        }
        Operand::Function(_, args) => args.iter().for_each(|a| operand_roots(a, out)),
        Operand::Literal(_) | Operand::Parameter(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::partiql::ast::Statement;
    use extenddb_core::partiql::parse;
    use extenddb_core::partiql::plan::{KeyNames, WhereAnalysis};
    use extenddb_core::types::KeyType;
    use extenddb_core::types::Projection as IndexProjection;

    fn select(sql: &str) -> SelectStatement {
        match parse(sql).unwrap() {
            Statement::Select(s) => s,
            _ => panic!("not a select"),
        }
    }

    fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        }
    }

    fn base() -> Vec<KeySchemaElement> {
        vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)]
    }

    fn index(index_type: IndexType, keys: Vec<KeySchemaElement>, include: &[&str]) -> IndexInfo {
        IndexInfo {
            index_name: "idx".to_owned(),
            index_id: "idx".to_owned(),
            index_type,
            key_schema: keys,
            projection: IndexProjection {
                projection_type: if include.is_empty() {
                    ProjectionType::KeysOnly
                } else {
                    ProjectionType::Include
                },
                non_key_attributes: (!include.is_empty())
                    .then(|| include.iter().map(|s| (*s).to_owned()).collect()),
            },
        }
    }

    fn gsi() -> IndexInfo {
        index(IndexType::Gsi, vec![key("g", KeyType::Hash)], &["kept"])
    }

    fn lsi() -> IndexInfo {
        index(
            IndexType::Lsi,
            vec![key("pk", KeyType::Hash), key("l", KeyType::Range)],
            &[],
        )
    }

    /// The refusal of `sql` on `info`, with the keyed filter the WHERE
    /// analysis gives it against the index keys.
    fn message(sql: &str, info: &IndexInfo) -> Option<String> {
        let stmt = select(sql);
        let name = |key_type| {
            info.key_schema
                .iter()
                .find(|k| k.key_type == key_type)
                .map(|k| k.attribute_name.as_str())
        };
        let keys = KeyNames {
            partition: name(KeyType::Hash).unwrap(),
            sort: name(KeyType::Range),
        };
        let value_of = |op: &Operand| match op {
            Operand::Literal(v) => Some(v.clone()),
            _ => None,
        };
        let filter = stmt
            .where_clause
            .as_ref()
            .and_then(|w| WhereAnalysis::new(w, keys, &value_of).shared_filter());
        check(&stmt, info, &base(), filter.as_deref())
            .err()
            .map(|e| e.message().to_owned())
    }

    const FILTER: &str = "One or more parameter values were invalid: Secondary index idx does not \
                          project one or more filter attributes: ";

    #[test]
    fn keyed_filter_on_an_unprojected_attribute_is_refused_on_both_kinds() {
        let m = message(
            "SELECT pk FROM \"t\".\"idx\" WHERE g = 'a' AND hidden = 1",
            &gsi(),
        );
        assert_eq!(m.unwrap(), format!("{FILTER}[hidden]"));
        let m = message(
            "SELECT pk FROM \"t\".\"idx\" WHERE pk = 'a' AND hidden = 1",
            &lsi(),
        );
        assert_eq!(m.unwrap(), format!("{FILTER}[hidden]"));
    }

    #[test]
    fn keyed_shapes_count_in_negation_functions_and_in_lists() {
        for where_clause in [
            "g = 'a' AND NOT hidden = 1",
            "g = 'a' AND hidden IS MISSING",
            "g = 'a' AND begins_with(hidden, 'x')",
            "g IN ['a', 'b'] AND hidden = 1",
            "g = 'a' AND (hidden = 1 OR kept = 2)",
            "g = 'a' AND hidden.child = 1",
        ] {
            let sql = format!("SELECT pk FROM \"t\".\"idx\" WHERE {where_clause}");
            assert_eq!(
                message(&sql, &gsi()),
                Some(format!("{FILTER}[hidden]")),
                "{sql}"
            );
        }
        for sort_condition in [
            "l BETWEEN 'a' AND 'b'",
            "l > 'a'",
            "begins_with(l, 'a')",
            "l IN ['a', 'b']",
        ] {
            let sql = format!(
                "SELECT pk FROM \"t\".\"idx\" WHERE pk = 'a' AND {sort_condition} AND hidden = 1"
            );
            assert_eq!(
                message(&sql, &lsi()),
                Some(format!("{FILTER}[hidden]")),
                "{sql}"
            );
        }
        // `<>` cannot be a sort-key condition, so the read is a scan.
        let sql = "SELECT pk FROM \"t\".\"idx\" WHERE pk = 'a' AND l <> 'a' AND hidden = 1";
        assert_eq!(message(sql, &lsi()), None);
    }

    #[test]
    fn unkeyed_reads_filter_on_index_rows() {
        for where_clause in [
            "hidden = 1",
            "hidden = 1 AND hidden = 2",
            "(g = 'a' AND hidden = 1) OR g = 'b'",
            "g <> 'a' AND hidden = 1",
            "begins_with(g, 'a') AND hidden = 1",
            "g = 'a' AND g = 'b' AND hidden = 1",
            "g = 'a' AND NOT g = 'b' AND hidden = 1",
        ] {
            let sql = format!("SELECT pk FROM \"t\".\"idx\" WHERE {where_clause}");
            assert_eq!(message(&sql, &gsi()), None, "{sql}");
        }
    }

    #[test]
    fn projected_filters_and_projections_pass() {
        let sql =
            "SELECT pk, sk, g, kept FROM \"t\".\"idx\" WHERE g = 'a' AND kept = 1 AND sk = 's'";
        assert_eq!(message(sql, &gsi()), None);
        assert_eq!(
            message("SELECT * FROM \"t\".\"idx\" WHERE g = 'a'", &gsi()),
            None
        );
    }

    #[test]
    fn missing_attributes_are_listed_once_in_statement_order() {
        let sql =
            "SELECT pk FROM \"t\".\"idx\" WHERE g = 'a' AND zeta = 1 AND alpha = 2 AND zeta = 3";
        assert_eq!(message(sql, &gsi()), Some(format!("{FILTER}[zeta, alpha]")));
    }

    #[test]
    fn gsi_projection_of_an_unprojected_attribute_is_refused_keyed_or_not() {
        let want = "One or more parameter values were invalid: Global secondary index idx does \
                    not project [hidden]";
        for sql in [
            "SELECT hidden FROM \"t\".\"idx\" WHERE g = 'a'",
            "SELECT hidden FROM \"t\".\"idx\"",
            "SELECT pk, hidden.child FROM \"t\".\"idx\" WHERE hidden = 1",
        ] {
            assert_eq!(message(sql, &gsi()).as_deref(), Some(want), "{sql}");
        }
        // On a keyed read the filter refusal is checked first.
        let sql = "SELECT hidden FROM \"t\".\"idx\" WHERE g = 'a' AND other = 1";
        assert_eq!(message(sql, &gsi()), Some(format!("{FILTER}[other]")));
    }

    #[test]
    fn lsi_projection_reaches_back_instead_of_refusing() {
        let s = select("SELECT hidden FROM \"t\".\"idx\" WHERE pk = 'a'");
        assert!(check(&s, &lsi(), &base(), Some(&[])).is_ok());
        assert!(reaches_back(&s.projection, &lsi(), &base()));
        let s = select("SELECT pk, l FROM \"t\".\"idx\" WHERE pk = 'a'");
        assert!(!reaches_back(&s.projection, &lsi(), &base()));
        let s = select("SELECT * FROM \"t\".\"idx\" WHERE pk = 'a'");
        assert!(!reaches_back(&s.projection, &lsi(), &base()));
        let s = select("SELECT hidden FROM \"t\".\"idx\" WHERE g = 'a'");
        assert!(!reaches_back(&s.projection, &gsi(), &base()));
    }

    #[test]
    fn an_all_projection_index_constrains_nothing() {
        let mut all = gsi();
        all.projection.projection_type = ProjectionType::All;
        all.projection.non_key_attributes = None;
        assert_eq!(
            message(
                "SELECT hidden FROM \"t\".\"idx\" WHERE g = 'a' AND other = 1",
                &all
            ),
            None
        );
    }
}
