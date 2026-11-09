// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How the service's lexer classifies an unquoted word, and its lists of reserved words.

/// How the service's lexer reads an unquoted word. The parser reports a word
/// by this class where a term, a path step or a name should start. Measured
/// 2026-11-02 for 274 words in four positions (a condition, the argument of
/// `attribute_exists`, a SET target and a projection): every word in these
/// lists is refused as a SET target with "Expected identifier for simple
/// path" and after a dot with "Invalid path dot component", and any other
/// word is a name. Where a condition starts, a [`WordClass::Keyword`] is
/// "Unexpected keyword", an operator "Unexpected operator" and a special word
/// "Unexpected term"; the keywords that start a form of their own (`date`,
/// `cast`, `select`, ...) are answered by the parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WordClass {
    /// An ordinary identifier: an attribute or function name.
    Name,
    /// A reserved keyword (`select`, `value`, `inner`, `size`, ...).
    Keyword,
    /// A keyword that is an operator: `and`, `or`, `not`, `is`, `in`, `like`,
    /// `between`, `union`, `except`, `intersect`. `not` also starts a term of
    /// its own, which the parser reads.
    Operator,
    /// `true`, `false`, `null` and `missing`.
    Literal,
    /// A word with a token type of its own (`as`, `at`, `by`, `asc`, `desc`,
    /// `for`, `nulls`, `first`, `last`): never a keyword term, never a name.
    Special,
}

/// Classify an unquoted word, case-insensitively.
#[must_use]
pub fn word_class(word: &str) -> WordClass {
    let lower = word.to_ascii_lowercase();
    let lower = lower.as_str();
    if OPERATOR_WORDS.contains(&lower) {
        WordClass::Operator
    } else if SPECIAL_WORDS.contains(&lower) {
        WordClass::Special
    } else if matches!(lower, "true" | "false" | "null" | "missing") {
        WordClass::Literal
    } else if KEYWORDS.binary_search(&lower).is_ok() {
        WordClass::Keyword
    } else {
        WordClass::Name
    }
}

/// Whether an unquoted word is a keyword that names a function and so may be
/// called (`size(x)`, `exists(...)`); any other keyword cannot start a call.
#[must_use]
pub fn is_function_keyword(word: &str) -> bool {
    FUNCTION_KEYWORDS.contains(&word.to_ascii_lowercase().as_str())
}

const OPERATOR_WORDS: &[&str] = &[
    "and",
    "or",
    "not",
    "is",
    "in",
    "like",
    "between",
    "union",
    "except",
    "intersect",
];

const SPECIAL_WORDS: &[&str] = &[
    "as", "at", "by", "asc", "desc", "for", "nulls", "first", "last",
];

/// Keywords that start a function call when `(` follows them.
const FUNCTION_KEYWORDS: &[&str] = &[
    "exists",
    "count",
    "avg",
    "max",
    "min",
    "sum",
    "substring",
    "char_length",
    "character_length",
    "octet_length",
    "bit_length",
    "upper",
    "lower",
    "size",
    "sexp",
    "list",
    "bag",
    "cast",
    "can_cast",
    "can_lossless_cast",
    "nullif",
    "coalesce",
    "trim",
    "extract",
    "date_add",
    "date_diff",
];

/// The reserved keywords, sorted for binary search. Words in
/// [`OPERATOR_WORDS`], [`SPECIAL_WORDS`] and the literals are classified
/// before this list is consulted.
const KEYWORDS: &[&str] = &[
    "absolute",
    "action",
    "add",
    "all",
    "allocate",
    "alter",
    "any",
    "are",
    "assertion",
    "authorization",
    "avg",
    "bag",
    "begin",
    "bigint",
    "bit",
    "bit_length",
    "blob",
    "bool",
    "boolean",
    "can_cast",
    "can_lossless_cast",
    "cascade",
    "cascaded",
    "case",
    "cast",
    "catalog",
    "char",
    "char_length",
    "character",
    "character_length",
    "check",
    "clob",
    "close",
    "coalesce",
    "collate",
    "collation",
    "column",
    "commit",
    "conflict",
    "connect",
    "connection",
    "constraint",
    "constraints",
    "continue",
    "convert",
    "corresponding",
    "count",
    "create",
    "cross",
    "current",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "cursor",
    "date",
    "date_add",
    "date_diff",
    "deallocate",
    "dec",
    "decimal",
    "declare",
    "default",
    "deferrable",
    "deferred",
    "delete",
    "describe",
    "descriptor",
    "diagnostics",
    "disconnect",
    "distinct",
    "do",
    "domain",
    "double",
    "drop",
    "else",
    "end",
    "escape",
    "exception",
    "exec",
    "execute",
    "exists",
    "external",
    "extract",
    "fetch",
    "float",
    "foreign",
    "found",
    "from",
    "full",
    "get",
    "global",
    "go",
    "goto",
    "grant",
    "group",
    "having",
    "identity",
    "immediate",
    "index",
    "indicator",
    "initially",
    "inner",
    "input",
    "insensitive",
    "insert",
    "int",
    "int2",
    "int4",
    "int8",
    "integer",
    "integer2",
    "integer4",
    "integer8",
    "interval",
    "into",
    "isolation",
    "join",
    "key",
    "language",
    "left",
    "let",
    "level",
    "limit",
    "list",
    "local",
    "lower",
    "match",
    "max",
    "min",
    "modified",
    "module",
    "names",
    "national",
    "natural",
    "nchar",
    "new",
    "next",
    "no",
    "nothing",
    "nullif",
    "numeric",
    "octet_length",
    "of",
    "offset",
    "old",
    "on",
    "only",
    "open",
    "option",
    "order",
    "outer",
    "output",
    "overlaps",
    "pad",
    "partial",
    "pivot",
    "position",
    "precision",
    "prepare",
    "preserve",
    "primary",
    "prior",
    "privileges",
    "procedure",
    "public",
    "read",
    "real",
    "references",
    "relative",
    "remove",
    "restrict",
    "returning",
    "revoke",
    "right",
    "rollback",
    "rows",
    "schema",
    "scroll",
    "section",
    "select",
    "session",
    "session_user",
    "set",
    "sexp",
    "size",
    "smallint",
    "some",
    "space",
    "sql",
    "sqlcode",
    "sqlerror",
    "sqlstate",
    "string",
    "struct",
    "substring",
    "sum",
    "symbol",
    "system_user",
    "table",
    "temporary",
    "then",
    "time",
    "timestamp",
    "to",
    "transaction",
    "translate",
    "translation",
    "trim",
    "tuple",
    "unique",
    "unknown",
    "unpivot",
    "update",
    "upper",
    "usage",
    "user",
    "using",
    "value",
    "values",
    "varchar",
    "varying",
    "view",
    "when",
    "whenever",
    "where",
    "with",
    "work",
    "write",
    "zone",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_keyword_list_is_sorted_for_binary_search() {
        assert!(KEYWORDS.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn words_are_classified_case_insensitively() {
        assert_eq!(word_class("ORDER"), WordClass::Keyword);
        assert_eq!(word_class("inner"), WordClass::Keyword);
        assert_eq!(word_class("Is"), WordClass::Operator);
        assert_eq!(word_class("LIKE"), WordClass::Operator);
        assert_eq!(word_class("as"), WordClass::Special);
        assert_eq!(word_class("NULL"), WordClass::Literal);
        assert_eq!(word_class("pk"), WordClass::Name);
        assert_eq!(word_class("attribute_exists"), WordClass::Name);
        assert!(is_function_keyword("SIZE"));
        assert!(!is_function_keyword("value"));
    }
}
