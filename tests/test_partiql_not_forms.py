# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for `NOT IN`, `NOT BETWEEN`, the members `IN`
takes, and `LIKE`.

`a NOT IN [...]`, `a NOT IN (...)` and `a NOT BETWEEN x AND y` run wherever a
condition does: a read's WHERE, an UPDATE or DELETE condition, on key
attributes and with parameters. Each means the negation of the predicate after
the NOT and counts two operators towards the limit of 300. A malformed form
gets the message of the predicate it spells, and a NOT before anything else
after an operand is a token left over. `IN` takes its members in brackets, in
parentheses or in a bag, which is not checked as a set literal there and may
hold names, and `LIKE` is refused with or without NOT and ESCAPE, also inside
the argument of a call. A predicate may be compared with a boolean, and a
comparison with a number, which holds for no item; a partition key equality
compared with false selects no item. An IS after a predicate or an IS test
is refused for its left side. An operand is not a condition, alone or as an
argument of AND.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (2026-11-02).
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

WF = "Statement wasn't well formed, can't be processed: "
IN_OPERANDS = (
    "IN operator must have a left hand argument of type Variable Reference and right hand "
    "argument of type Seq with at least one member"
)
TOO_MANY_OPERATORS = "The expression contains too many operators; operator count: 301"
IS_LEFT_SIDE = "The left hand side argument to IS must be a Variable Reference, Literal, or Path"
ITEM = {"pk": {"S": "a"}, "a": {"N": "1"}}
FIVE = {"pk": {"S": "b"}, "a": {"N": "5"}}


@pytest.fixture(scope="module")
def readable(dynamodb_client):
    """A table keyed on a string pk, holding one item with a = 1."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(TableName=name, Item=ITEM)
        yield name


@pytest.fixture(scope="module")
def sorted_table(dynamodb_client):
    """A table keyed on a string pk and a string sk, holding one item."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "a"}, "sk": {"S": "x"}, "a": {"S": "x"}}
        )
        yield name


@pytest.fixture(scope="module")
def pair(dynamodb_client):
    """A table keyed on a string pk, holding items with a = 1 and a = 5."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(TableName=name, Item=ITEM)
        dynamodb_client.put_item(TableName=name, Item=FIVE)
        yield name


@pytest.fixture
def writable(dynamodb_client):
    """A table keyed on a string pk, holding one item with a = 1, per test."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(TableName=name, Item=ITEM)
        yield name


def _rows(client, statement: str, **kwargs) -> list:
    return client.execute_statement(Statement=statement, **kwargs)["Items"]


def _message(client, statement: str) -> str:
    with pytest.raises(ClientError) as e:
        client.execute_statement(Statement=statement)
    err = e.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


@pytest.mark.parametrize(
    "condition, matches",
    [
        ("a NOT IN [1, 2]", False),
        ("a NOT IN [2]", True),
        ("a NOT IN (1, 2)", False),
        ("a NOT BETWEEN 1 AND 2", False),
        ("a NOT BETWEEN 2 AND 3", True),
        ("NOT a NOT IN [1]", True),
        ("a NOT IN [1] AND pk = 'a'", False),
        ("pk = 'a' AND a NOT IN [1]", False),
        ("a NOT IN [1, 'x', [1], {'k': 1}]", False),
        ("pk NOT IN ['a']", False),
        ("pk NOT IN [1]", True),
        ("pk NOT BETWEEN 'a' AND 'b'", False),
    ],
)
def test_a_not_form_selects_what_its_predicate_does_not(
    dynamodb_client, readable, condition, matches
):
    rows = _rows(dynamodb_client, f'SELECT * FROM "{readable}" WHERE {condition}')
    assert rows == ([ITEM] if matches else [])


def test_in_takes_its_members_in_parentheses(dynamodb_client, readable):
    rows = _rows(dynamodb_client, f'SELECT * FROM "{readable}" WHERE a IN (1, 2)')
    assert rows == [ITEM]


def test_not_in_takes_parameters(dynamodb_client, readable):
    rows = _rows(
        dynamodb_client,
        f'SELECT * FROM "{readable}" WHERE a NOT IN [?, ?]',
        Parameters=[{"N": "1"}, {"N": "2"}],
    )
    assert rows == []


@pytest.mark.parametrize(
    "condition", ["pk = 'a' AND sk NOT IN ['y']", "pk = 'a' AND sk NOT BETWEEN 'y' AND 'z'"]
)
def test_a_not_form_on_a_sort_key_filters_the_query(dynamodb_client, sorted_table, condition):
    rows = _rows(dynamodb_client, f'SELECT * FROM "{sorted_table}" WHERE {condition}')
    assert rows == [{"pk": {"S": "a"}, "sk": {"S": "x"}, "a": {"S": "x"}}]


def test_an_update_condition_with_not_in_that_fails(dynamodb_client, writable):
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(
            Statement=f"UPDATE \"{writable}\" SET q = 1 WHERE pk = 'a' AND a NOT IN [1]"
        )
    assert e.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_an_update_condition_with_not_between_that_holds(dynamodb_client, writable):
    rows = _rows(
        dynamodb_client,
        f"UPDATE \"{writable}\" SET q = 2 WHERE pk = 'a' AND a NOT BETWEEN 5 AND 6 "
        "RETURNING ALL NEW *",
    )
    assert rows == [{"pk": {"S": "a"}, "a": {"N": "1"}, "q": {"N": "2"}}]


def test_a_delete_of_a_missing_item_with_not_in(dynamodb_client, writable):
    rows = _rows(dynamodb_client, f"DELETE FROM \"{writable}\" WHERE pk = 'zz' AND a NOT IN [1]")
    assert rows == []


@pytest.mark.parametrize("term", ["a NOT IN [1]", "a NOT BETWEEN 1 AND 2"])
def test_a_not_form_counts_two_operators(dynamodb_client, readable, term):
    head = f'SELECT * FROM "{readable}" WHERE '
    assert _rows(dynamodb_client, head + "NOT " * 298 + f"({term})") == []
    assert _message(dynamodb_client, head + "NOT " * 299 + f"({term})") == TOO_MANY_OPERATORS


@pytest.mark.parametrize(
    "condition, message",
    [
        ("a NOT IN 1", IN_OPERANDS),
        ("a NOT IN []", IN_OPERANDS),
        ("a NOT BETWEEN 1", WF + "Expected AND keyword"),
        ("a NOT = 1", WF + "Unexpected token after expression"),
        ("a NOT IS MISSING", WF + "Unexpected token after expression"),
        ("a IS NOT IN [1]", WF + "Expected type name"),
        ("a NOT LIKE", WF + "Missing right-hand side expression of infix operator"),
    ],
)
def test_a_malformed_not_form_is_refused(dynamodb_client, readable, condition, message):
    statement = f'SELECT * FROM "{readable}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == message


@pytest.mark.parametrize("condition", ["a LIKE 'x'", "a NOT LIKE 'x'", "a LIKE 'x' ESCAPE 'y'"])
def test_like_is_refused_as_an_operator_a_condition_does_not_take(
    dynamodb_client, readable, condition
):
    statement = f'SELECT * FROM "{readable}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == (
        "Unsupported operator in Condition Expression. Operator: like"
    )


@pytest.mark.parametrize(
    "condition, keys",
    [
        # A member in parentheses of its own is a list when a comma separates
        # what they hold, and what they hold otherwise.
        ("a IN ((1, 2))", []),
        ("a IN ((5))", ["b"]),
        ("a IN ([5])", []),
        ("a IN ([1], 2)", []),
        ("a NOT IN ([5])", ["a", "b"]),
        ("a IN (1)", ["a"]),
        ("a IN (pk, 1)", ["a"]),
        ("a IN <<1, 5>>", ["a", "b"]),
        # A bag there is not checked as a set literal.
        ("a IN <<1, 1>>", ["a"]),
        ("a IN <<'x', 1>>", ["a"]),
        # An IN, with or without NOT, may be compared with a boolean, and so
        # may a comparison.
        ("a NOT IN [1] = true", ["b"]),
        ("(a NOT IN [1]) = true", ["b"]),
        ("a IN [1] AND pk = 'a' = true", ["a"]),
    ],
)
def test_the_members_an_in_takes(dynamodb_client, pair, condition, keys):
    rows = _rows(dynamodb_client, f'SELECT * FROM "{pair}" WHERE {condition}')
    assert sorted(row["pk"]["S"] for row in rows) == keys


@pytest.mark.parametrize(
    "condition, message",
    [
        ("a IN ()", WF + "Unexpected term"),
        ("a NOT IN ()", WF + "Unexpected term"),
        ("a IN (1,)", WF + "Unexpected term"),
        ("a IN (1, 2", WF + "Expected RIGHT_PAREN"),
        ("a IN 5", IN_OPERANDS),
        ("a IN (1, 2) IS MISSING", IS_LEFT_SIDE),
        ("(a NOT = 1)", WF + "Expected RIGHT_PAREN"),
        ("a NOT", WF + "Unexpected token after expression"),
        ("a NOT AND pk = 'a'", WF + "Unexpected token after expression"),
        ("a NOT NOT IN [1]", WF + "Unexpected token after expression"),
        ("a NOT NOT BETWEEN 1 AND 2", WF + "Unexpected token after expression"),
        # A group that is not closed.
        ("(a = 1 b)", WF + "Expected RIGHT_PAREN"),
        ("(a = 1", WF + "Expected RIGHT_PAREN"),
        ("((a = 1)", WF + "Expected RIGHT_PAREN"),
        # An IS after any predicate is refused for its left side.
        ("a BETWEEN 1 AND 2 IS MISSING", IS_LEFT_SIDE),
        ("a = 1 IS MISSING", IS_LEFT_SIDE),
    ],
)
def test_a_malformed_in_or_not_is_refused(dynamodb_client, pair, condition, message):
    statement = f'SELECT * FROM "{pair}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == message


def test_a_name_inside_a_list_in_parentheses_is_refused_at_its_position(dynamodb_client, pair):
    statement = f'SELECT * FROM "{pair}" WHERE a IN ((pk, 1))'
    column = statement.index("pk,") + 1
    assert _message(dynamodb_client, statement) == (
        f"Unsupported data type: Id under key root[0] at 1:{column}:2"
    )


def test_like_inside_the_argument_of_a_call_is_refused(dynamodb_client, readable):
    statement = f"SELECT * FROM \"{readable}\" WHERE foo(a LIKE 'x')"
    assert _message(dynamodb_client, statement) == (
        "Unsupported operator in Condition Expression. Operator: like"
    )


def test_escape_with_nothing_after_it(dynamodb_client, readable):
    statement = f"SELECT * FROM \"{readable}\" WHERE a LIKE 'x' ESCAPE"
    assert _message(dynamodb_client, statement) == WF + "Expected expression after ESCAPE"


@pytest.mark.parametrize(
    "condition, message",
    [
        (
            "1 IS MISSING",
            "Operator or function requires a document path; operator or function: "
            "attribute_not_exists",
        ),
        (
            "'x' IS NOT MISSING",
            "Operator or function requires a document path; operator or function: "
            "attribute_not_exists",
        ),
        ("a IS NULL IS NULL", IS_LEFT_SIDE),
    ],
)
def test_an_is_test_on_a_literal_or_after_an_is_test(dynamodb_client, pair, condition, message):
    assert _message(dynamodb_client, f'SELECT * FROM "{pair}" WHERE {condition}') == message


@pytest.mark.parametrize(
    "member, token, kind, length",
    [('"pk"', '"pk"', "Id", 4), ("m.b", "m.b", "Path", 1), ("foo(b)", "foo", "Call", 3)],
    ids=["quoted-name", "path", "call"],
)
def test_a_term_inside_a_list_in_parentheses_is_refused_by_what_it_is(
    dynamodb_client, pair, member, token, kind, length
):
    statement = f'SELECT * FROM "{pair}" WHERE a IN (({member}, 1))'
    column = statement.index(token) + 1
    assert _message(dynamodb_client, statement) == (
        f"Unsupported data type: {kind} under key root[0] at 1:{column}:{length}"
    )


def test_a_name_in_a_bag_after_in_is_an_operand(dynamodb_client, pair):
    assert _rows(dynamodb_client, f'SELECT * FROM "{pair}" WHERE a IN <<pk>>') == []


@pytest.mark.parametrize(
    "condition, keys",
    [
        ("a = 1 = true = true", ["a"]),
        ("a = 1 = 1", []),
        # The partition key equality is still read as the key condition, so
        # the item it reads fails the comparison with false.
        ("pk = 'a' = false", []),
    ],
)
def test_a_comparison_compared_with_a_boolean_or_a_number(dynamodb_client, pair, condition, keys):
    rows = _rows(dynamodb_client, f'SELECT * FROM "{pair}" WHERE {condition}')
    assert sorted(row["pk"]["S"] for row in rows) == keys


@pytest.mark.parametrize(
    "condition, parameters",
    [
        ("a", []),
        ("1", []),
        ("'x'", []),
        ("?", [{"N": "1"}]),
        ("[1]", []),
        ("m.b", []),
        ("true", []),
    ],
)
def test_an_operand_that_is_the_whole_condition_is_refused(
    dynamodb_client, pair, condition, parameters
):
    kwargs = {"Parameters": parameters} if parameters else {}
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(
            Statement=f'SELECT * FROM "{pair}" WHERE {condition}', **kwargs
        )
    err = e.value.response["Error"]
    assert (err["Code"], err["Message"]) == (
        "ValidationException",
        "WHERE clause must contain an expression that resolves to a boolean",
    )


def test_an_operand_that_and_takes_is_refused(dynamodb_client, pair):
    statement = f"SELECT * FROM \"{pair}\" WHERE a AND pk = 'a'"
    assert _message(dynamodb_client, statement) == "The arguments to AND/OR must be Expressions"
