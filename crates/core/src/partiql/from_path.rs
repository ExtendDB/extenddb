// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Refusals of a statement's FROM path (`"table"` or `"table"."index"`).
//!
//! The service checks the path after the statement has parsed and before it
//! looks the table up (measured 2026-11-02), so the parser records the first
//! refusal and raises it only once the whole statement has parsed. The table
//! and index names are not resolved here: a qualifier naming no index gets the
//! same refusal as one naming a real index.

use super::Statement;

/// The refusal of a table or index name outside the service's name rule:
/// 3 to 255 characters, each a letter, a digit, `_`, `-` or `.` (measured
/// 2026-11-02 with 1, 2, 3, 255 and 256 characters and with `bad!name`).
pub const NAME_RULE: &str = "Table name should be within the length [3, 255] and only contain 'a-z', 'A-Z', '0-9', '-', '_', '.'.";

pub(super) fn breaks_name_rule(name: &str) -> bool {
    !(3..=255).contains(&name.len())
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// The refusal of a `SELECT` FROM path, or `None` when the path is accepted.
///
/// An empty component is reported ahead of a path that is too long, whichever
/// component is empty, and both ahead of an index name outside the name rule
/// (measured 2026-11-02: the rule answers ahead of every other check on an
/// index name). The table name gets the same rule from
/// [`table_name_refusal`] once the statement has parsed.
pub(super) fn refusal(components: &[&str]) -> Option<String> {
    if components.iter().any(|c| c.is_empty()) {
        Some("Path component cannot be an empty string".to_owned())
    } else if components.len() > 2 {
        Some("A path may contain at most 2 components in the FROM clause".to_owned())
    } else if components[1..].iter().any(|c| breaks_name_rule(c)) {
        Some(NAME_RULE.to_owned())
    } else {
        None
    }
}

/// The refusal of a parsed statement's table name outside the name rule. The
/// service checks it before it looks the table up, for SELECT, INSERT, UPDATE
/// and DELETE alike (measured 2026-11-02: `"ab"` on a table that cannot
/// exist, and `INSERT INTO ""`).
#[must_use]
pub fn table_name_refusal(stmt: &Statement) -> Option<&'static str> {
    let table = match stmt {
        Statement::Select(s) | Statement::Exists(s) => &s.from.table,
        Statement::Insert(i) => &i.table,
        Statement::Update(u) => &u.table,
        Statement::Delete(d) => &d.table,
    };
    breaks_name_rule(table).then_some(NAME_RULE)
}

/// The refusal of an `UPDATE` or `DELETE` whose target path names an index
/// and is not refused by [`refusal`]. Those statements accept an index
/// qualifier grammatically and refuse it after the parse.
pub(super) const ON_INDEX: &str = "This operation is not supported on an index";

/// The refusal of an `INSERT` whose target is a path rather than a table
/// name. It carries the `line:column:length` span of the table name token.
pub(super) fn insert_refusal(table_span: &str) -> String {
    format!(
        "FROM clause may only contain a single table name in data manipulation statements \
         at {table_span}"
    )
}

#[cfg(test)]
mod tests {
    use crate::partiql::parse;

    fn err(sql: &str) -> String {
        parse(sql).unwrap_err().message
    }

    const EMPTY: &str = "Path component cannot be an empty string";
    const TOO_LONG: &str = "A path may contain at most 2 components in the FROM clause";
    const ON_INDEX: &str = "This operation is not supported on an index";

    #[test]
    fn select_path_with_three_components_is_refused() {
        assert_eq!(
            err("SELECT * FROM \"t\".\"i\".\"x\" WHERE pk = 'a'"),
            TOO_LONG
        );
        assert_eq!(err("SELECT * FROM \"t\".\"i\".x.y"), TOO_LONG);
    }

    #[test]
    fn select_path_with_an_empty_component_is_refused() {
        assert_eq!(err("SELECT * FROM \"t\".\"\" WHERE pk = 'a'"), EMPTY);
        assert_eq!(err("SELECT * FROM \"\" WHERE pk = 'a'"), EMPTY);
        // The empty component wins over the length, wherever it sits.
        assert_eq!(err("SELECT * FROM \"t\".\"i\".\"\""), EMPTY);
    }

    #[test]
    fn select_path_refusal_waits_for_the_rest_of_the_statement() {
        // A syntax error later in the statement is reported first.
        assert_eq!(
            err("SELECT * FROM \"t\".\"i\".\"x\" WHERE pk = 'a'; SELECT 1"),
            "Statement wasn't well formed, can't be processed: Unexpected token after \
             semicolon. (Only one query is allowed.)"
        );
        // The path is refused ahead of the LIMIT clause.
        assert_eq!(err("SELECT * FROM \"t\".\"i\".\"x\" LIMIT 1"), TOO_LONG);
    }

    #[test]
    fn select_with_one_qualifier_still_parses() {
        assert!(parse("SELECT * FROM \"t\".\"idx\" WHERE pk = 'a'").is_ok());
        assert!(parse("SELECT * FROM \"t\".idx WHERE pk = 'a'").is_ok());
    }

    #[test]
    fn an_index_name_outside_the_name_rule_is_refused_first() {
        let long = "x".repeat(256);
        for index in ["i", "ix", "bad!name", long.as_str()] {
            for sql in [
                format!("SELECT * FROM \"t\".\"{index}\" WHERE pk = 'a'"),
                format!("UPDATE \"t\".\"{index}\" SET a = 1 WHERE pk = 'a'"),
                format!("DELETE FROM \"t\".\"{index}\" WHERE pk = 'a'"),
            ] {
                assert_eq!(err(&sql), super::NAME_RULE, "{sql}");
            }
        }
        let longest = "x".repeat(255);
        assert!(parse(&format!("SELECT * FROM \"t\".\"{longest}\"")).is_ok());
        assert_eq!(
            err(&format!(
                "UPDATE \"t\".\"{longest}\" SET a = 1 WHERE pk = 'a'"
            )),
            ON_INDEX
        );
    }

    #[test]
    fn a_table_name_outside_the_name_rule_is_found_on_the_parsed_statement() {
        let refusal = |sql: &str| super::table_name_refusal(&parse(sql).unwrap());
        assert_eq!(refusal("SELECT * FROM \"ab\""), Some(super::NAME_RULE));
        assert_eq!(
            refusal("INSERT INTO \"\" VALUE {'pk': 'a'}"),
            Some(super::NAME_RULE)
        );
        assert_eq!(
            refusal("DELETE FROM \"bad!name\" WHERE pk = 'a'"),
            Some(super::NAME_RULE)
        );
        assert_eq!(refusal("UPDATE \"a-b.c_1\" SET a = 1 WHERE pk = 'a'"), None);
    }

    #[test]
    fn update_and_delete_refuse_an_index_qualifier_after_the_parse() {
        assert_eq!(
            err("UPDATE \"t\".\"idx\" SET a = 1 WHERE pk = 'a' AND sk = 'b'"),
            ON_INDEX
        );
        assert_eq!(
            err("DELETE FROM \"t\".\"idx\" WHERE pk = 'a' AND sk = 'b'"),
            ON_INDEX
        );
        // The statement must parse first: a missing SET is a syntax error.
        assert_eq!(
            err("UPDATE \"t\".\"i\" WHERE pk = 'a'"),
            "Statement wasn't well formed, can't be processed: Expected data manipulation"
        );
    }

    #[test]
    fn update_and_delete_paths_take_the_select_path_refusals_first() {
        assert_eq!(
            err("UPDATE \"t\".\"i\".\"x\" SET a = 1 WHERE pk = 'a'"),
            TOO_LONG
        );
        assert_eq!(err("DELETE FROM \"t\".\"\" WHERE pk = 'a'"), EMPTY);
        assert_eq!(err("UPDATE \"\" SET a = 1 WHERE pk = 'a'"), EMPTY);
    }

    #[test]
    fn insert_into_a_path_is_refused_at_the_table_name() {
        // The table name token starts at column 13 and is 3 characters long.
        assert_eq!(
            err("INSERT INTO \"t\".\"i\" VALUE {'pk': 'a'}"),
            "FROM clause may only contain a single table name in data manipulation \
             statements at 1:13:3"
        );
        assert_eq!(
            err("INSERT INTO \"t\".\"\".\"x\" VALUE {'pk': 'a'}"),
            "FROM clause may only contain a single table name in data manipulation \
             statements at 1:13:3"
        );
    }

    #[test]
    fn plain_targets_are_unchanged() {
        assert!(parse("INSERT INTO \"t\" VALUE {'pk': 'a'}").is_ok());
        assert!(parse("UPDATE \"t\" SET a = 1 WHERE pk = 'a'").is_ok());
        assert!(parse("DELETE FROM \"t\" WHERE pk = 'a'").is_ok());
    }
}
