# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for how a PartiQL statement answers a reserved word:
where a condition starts, as the argument of attribute_exists, as a SET target,
as a projection, as a table, and as a write target.

The service reads a reserved word where a term starts as the start of a form
of its own when the word begins one (CASE, DATE, a FROM-led form, EXEC, ...),
so the answer is often the error that form meets at the next token.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

WF = "Statement wasn't well formed, can't be processed: "
NOT_FOUND = ("ResourceNotFoundException", "Requested resource not found")

KEYWORD = ("ValidationException", f"{WF}Unexpected keyword")
OPERATOR = ("ValidationException", f"{WF}Unexpected operator")
TERM = ("ValidationException", f"{WF}Unexpected term")
SIMPLE_PATH = ("ValidationException", f"{WF}Expected identifier for simple path")
MANIPULATION = ("ValidationException", f"{WF}Expected data manipulation")


def _wf(reason):
    return ("ValidationException", f"{WF}{reason}")


def _plain(message):
    return ("ValidationException", message)


def _in_select_list(word):
    return _wf(
        f"Expected identifier or an expression but found unexpected keyword '{word}' in a select list."
    )


def _answer(client, statement):
    try:
        client.execute_statement(Statement=statement)
    except ClientError as e:
        return (e.response["Error"]["Code"], e.response["Error"]["Message"])
    return ("OK", "")


@pytest.fixture(scope="module")
def absent():
    """A valid table name no table has."""
    return f"extenddb-absent-{uuid.uuid4().hex[:12]}"


@pytest.fixture(scope="module")
def stable(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


DATE = _wf("Expected date string followed by the keyword DATE, found SYMBOL")
TIME = _wf("Expected time string followed by the keyword TIME OR TIME WITH TIME ZONE, found SYMBOL")
PATH_NEEDED = _plain(
    "Operator or function requires a document path; operator or function: attribute_exists"
)
MISSING = _plain("MISSING must be compared using IS operator")

# word -> answers to `WHERE <w> = 1`, `WHERE attribute_exists(<w>)`,
# `SET <w> = 1` and `SELECT <w> FROM`, on a table that does not exist.
POSITIONS = {
    "order": (KEYWORD, KEYWORD, SIMPLE_PATH, KEYWORD),
    "inner": (KEYWORD, KEYWORD, SIMPLE_PATH, KEYWORD),
    "count": (KEYWORD, KEYWORD, SIMPLE_PATH, KEYWORD),
    "insert": (KEYWORD, KEYWORD, SIMPLE_PATH, KEYWORD),
    "and": (OPERATOR, OPERATOR, SIMPLE_PATH, OPERATOR),
    "like": (OPERATOR, OPERATOR, SIMPLE_PATH, OPERATOR),
    "as": (TERM, TERM, SIMPLE_PATH, TERM),
    "first": (TERM, TERM, SIMPLE_PATH, TERM),
    "not": (OPERATOR, TERM, SIMPLE_PATH, MANIPULATION),
    "case": (OPERATOR, TERM, SIMPLE_PATH, MANIPULATION),
    "select": (OPERATOR, TERM, SIMPLE_PATH, _in_select_list("select")),
    "from": (OPERATOR, TERM, SIMPLE_PATH, _in_select_list("from")),
    "distinct": (KEYWORD, KEYWORD, SIMPLE_PATH, _in_select_list("from")),
    "value": (KEYWORD, KEYWORD, SIMPLE_PATH, MANIPULATION),
    "where": (KEYWORD, KEYWORD, SIMPLE_PATH, _in_select_list("where")),
    "delete": (
        _wf("Expected FROM after DELETE"),
        _wf("Expected FROM after DELETE"),
        SIMPLE_PATH,
        _in_select_list("delete"),
    ),
    "set": (SIMPLE_PATH, SIMPLE_PATH, SIMPLE_PATH, _in_select_list("set")),
    "exec": (
        _wf("Keyword = only expected at the top level in the query"),
        _wf("Expected RIGHT_PAREN"),
        SIMPLE_PATH,
        _wf("Expected FROM after SELECT list"),
    ),
    "date": (DATE, DATE, SIMPLE_PATH, DATE),
    "time": (TIME, TIME, SIMPLE_PATH, _wf("Expected WITH keyword")),
    "cast": (
        _wf("Missing left parenthesis after cast"),
        _wf("Missing left parenthesis after cast"),
        SIMPLE_PATH,
        _wf("Missing left parenthesis after cast"),
    ),
    "substring": (_wf("Expected LEFT_PAREN"), _wf("Expected LEFT_PAREN"), SIMPLE_PATH, _wf("Expected LEFT_PAREN")),
    "values": (
        _wf("Expected LEFT_PAREN for row value constructor"),
        _wf("Expected LEFT_PAREN for row value constructor"),
        SIMPLE_PATH,
        _wf("Expected LEFT_PAREN for row value constructor"),
    ),
    "create": (
        _wf("Unexpected token following CREATE"),
        _wf("Unexpected token following CREATE"),
        SIMPLE_PATH,
        _wf("Unexpected token following CREATE"),
    ),
    "missing": (MISSING, MISSING, SIMPLE_PATH, _plain("Unexpected path component at 1:8:7")),
    "null": (NOT_FOUND, PATH_NEEDED, SIMPLE_PATH, _plain("Unexpected path component at 1:8:4")),
    "true": (NOT_FOUND, PATH_NEEDED, SIMPLE_PATH, _plain("Unexpected path component at 1:8:4")),
    "year": (NOT_FOUND, NOT_FOUND, NOT_FOUND, NOT_FOUND),
    "partition": (NOT_FOUND, NOT_FOUND, NOT_FOUND, NOT_FOUND),
}


@pytest.mark.parametrize("word", sorted(POSITIONS))
def test_a_word_in_the_four_positions(dynamodb_client, absent, word):
    q = f'"{absent}"'
    statements = [
        f"SELECT * FROM {q} WHERE {word} = 1",
        f"SELECT * FROM {q} WHERE attribute_exists({word})",
        f"UPDATE {q} SET {word} = 1 WHERE pk = 'a'",
        f"SELECT {word} FROM {q}",
    ]
    answers = tuple(_answer(dynamodb_client, s) for s in statements)
    assert answers == POSITIONS[word]


@pytest.mark.parametrize(
    ("word", "expected"),
    [
        ("where", KEYWORD),
        ("value", KEYWORD),
        ("as", TERM),
        ("and", OPERATOR),
        ("set", SIMPLE_PATH),
        ("select", _in_select_list("where")),
        ("null", _plain("Unexpected from source")),
    ],
)
def test_a_word_where_the_table_of_a_select_starts(dynamodb_client, word, expected):
    assert _answer(dynamodb_client, f"SELECT * FROM {word} WHERE pk = 'p'") == expected


@pytest.mark.parametrize("word", ["where", "select", "inner", "as", "and", "null", "count"])
def test_a_word_where_the_table_of_a_delete_starts(dynamodb_client, word):
    assert _answer(dynamodb_client, f"DELETE FROM {word} WHERE pk = 'p'") == SIMPLE_PATH


@pytest.mark.parametrize(
    ("word", "expected"),
    [
        ("case", MANIPULATION),
        ("exec", _wf("Expected FROM after SELECT list")),
        ("time", _wf("Expected WITH keyword")),
        ("nulls", TERM),
    ],
)
def test_a_projected_word_before_a_where(dynamodb_client, stable, word, expected):
    assert _answer(dynamodb_client, f"SELECT {word} FROM \"{stable}\" WHERE pk = 'p'") == expected


def test_exec_after_a_condition(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'p' AND exec = 1"
    assert _answer(dynamodb_client, statement) == _wf(
        "Keyword = only expected at the top level in the query"
    )


def test_order_by_a_reserved_word(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'p' ORDER BY value"
    assert _answer(dynamodb_client, statement) == KEYWORD
