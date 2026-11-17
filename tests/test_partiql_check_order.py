# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the order of the checks on a PartiQL statement
and its WHERE condition.

The service walks the condition as a tree, within the operator limit and past
it: an operator before its operands, the operands in the order they are
written, and the arguments of a call before its name. It reports the first
number literal that breaks an item number rule, `?` with no parameter left in
the request, or refusal it meets on that walk, ahead of the operator count.
An alias on the table and a table name outside the name rule come before all
of those, then an alias in the select list, an UPDATE of an index and a LIMIT
clause. In ExecuteStatement an EXISTS statement comes before the name rule.
The SET clause of an UPDATE, and parameters beyond the ones the statement
reads, come after the operator count; in the clause a number comes before a
unary plus. ExecuteStatement, BatchExecuteStatement and ExecuteTransaction
keep the same order.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (2026-11-02).
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

TOO_MANY_OPERATORS = "The expression contains too many operators; operator count: 301"
PARAMETERS = "Number of parameters in request and statement don't match."
OVERFLOW = (
    "Number overflow. Attempting to store a number with magnitude larger than supported "
    "range under "
)
UNDERFLOW = (
    "Number underflow. Attempting to store a number with magnitude smaller than supported "
    "range under "
)
PLUS = "Unsupported operator in Condition Expression. Operator: +"
LIKE = "Unsupported operator in Condition Expression. Operator: like"
MISSING = "MISSING must be compared using IS operator"
IS_LEFT_SIDE = "The left hand side argument to IS must be a Variable Reference, Literal, or Path"
NAME_RULE = (
    "Table name should be within the length [3, 255] and only contain 'a-z', 'A-Z', '0-9', "
    "'-', '_', '.'."
)
NOTS = "NOT " * 300 + "a = 1"
DEEP = "[" * 33 + "1" + "]" * 33
NESTED = "Nesting Levels have exceeded supported limits under root" + "[0]" * 32
IN_OF_101 = "a IN [" + ", ".join(str(i) for i in range(101)) + "]"
PROJECTED_CALL = "Unexpected path component at 1:8:5"


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """A table keyed on a string pk."""
    with scoped_table(dynamodb_client) as name:
        yield name


def _message(client, statement: str, **kwargs) -> str:
    with pytest.raises(ClientError) as e:
        client.execute_statement(Statement=statement, **kwargs)
    err = e.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


@pytest.mark.parametrize(
    "condition, message",
    [
        ("b = 1e126 AND c = ?", OVERFLOW + "root"),
        ("c = ? AND b = 1e126", PARAMETERS),
        ("b = 1 + 1 AND c = 1e126", PLUS),
        ("c = 1e126 AND b = 1 + 1", OVERFLOW + "root"),
        ("c = 1e126 AND foo(b)", OVERFLOW + "root"),
        ("c = 1e126 AND b = MISSING", OVERFLOW + "root"),
        ("c = 1e126 AND d = 123456789012345678901234567890123456789", OVERFLOW + "root"),
        ("b = [1e126]", OVERFLOW + "root[0]"),
        ("b = {'k': 1e-131}", UNDERFLOW + "root.k"),
    ],
)
def test_the_first_problem_in_the_condition_comes_before_the_operator_count(
    dynamodb_client, table, condition, message
):
    statement = f'SELECT * FROM "{table}" WHERE {condition} AND {NOTS}'
    assert _message(dynamodb_client, statement) == message


def test_a_parameter_after_the_operators_comes_before_their_count(dynamodb_client, table):
    statement = f'SELECT * FROM "{table}" WHERE {NOTS} AND c = ?'
    assert _message(dynamodb_client, statement) == PARAMETERS


def test_an_alias_comes_before_the_condition(dynamodb_client, table):
    statement = f'SELECT * FROM "{table}" AS x WHERE c = 1e126 AND {NOTS}'
    assert _message(dynamodb_client, statement) == "Aliasing is not supported"


@pytest.mark.parametrize(
    "condition, given, message",
    [
        ("c = ?", [{"N": "1"}, {"N": "2"}], TOO_MANY_OPERATORS),
        ("c = ? AND d = ?", [{"N": "1"}], PARAMETERS),
    ],
)
def test_the_parameters_the_condition_reads_are_counted_before_the_operators(
    dynamodb_client, table, condition, given, message
):
    statement = f'SELECT * FROM "{table}" WHERE {condition} AND {NOTS}'
    assert _message(dynamodb_client, statement, Parameters=given) == message


@pytest.mark.parametrize("value", ["1e126", "[1, 1e126]", "?", "+b", "b IS NULL", "[?]"])
def test_a_set_value_comes_after_the_operator_count(dynamodb_client, table, value):
    statement = f"UPDATE \"{table}\" SET q = {value} WHERE pk = 'a' AND {NOTS}"
    assert _message(dynamodb_client, statement) == TOO_MANY_OPERATORS


def test_a_set_clause_after_the_condition_comes_after_the_operator_count(
    dynamodb_client, table
):
    statement = f"FROM \"{table}\" WHERE pk = 'a' AND {NOTS} SET q = +b"
    assert _message(dynamodb_client, statement) == TOO_MANY_OPERATORS


@pytest.mark.parametrize(
    "condition, message",
    [
        ("b = 1e126 + 1", PLUS),
        ("b = 1 + 1e126", PLUS),
        ("? IS NULL AND c = 1e126", IS_LEFT_SIDE),
        ("foo(1e126)", OVERFLOW + "root"),
    ],
)
def test_the_walk_meets_an_operator_before_its_operands_and_a_name_after_its_arguments(
    dynamodb_client, table, condition, message
):
    statement = f'SELECT * FROM "{table}" WHERE {condition} AND {NOTS}'
    assert _message(dynamodb_client, statement) == message


@pytest.mark.parametrize(
    "condition, message",
    [
        ("c = 1e126 AND b = 1 + 1", OVERFLOW + "root"),
        ("b = 1 + 1 AND c = 1e126", PLUS),
        ("c = 1e126 AND foo(b)", OVERFLOW + "root"),
        ("c = 1e126 AND b = MISSING", OVERFLOW + "root"),
        ("b = MISSING AND c = 1e126", MISSING),
        ("c = 1e126 AND b = 'x' || 'y'", OVERFLOW + "root"),
        ("c = 1e126 AND a LIKE 'x'", OVERFLOW + "root"),
        ("a LIKE 'x' AND c = 1e126", LIKE),
        ("c = ? AND b = 1 + 1", PARAMETERS),
        ("b = 1 + 1 AND c = ?", PLUS),
        ("c = 1e126 AND a IS INT4", OVERFLOW + "root"),
        (f"c = 1e126 AND b = {DEEP}", OVERFLOW + "root"),
        (f"foo({DEEP})", NESTED),
        ("foo(1 + 1)", PLUS),
    ],
)
def test_the_walk_keeps_its_order_within_the_operator_limit(
    dynamodb_client, table, condition, message
):
    statement = f'SELECT * FROM "{table}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == message


def test_an_unknown_function_before_a_number_is_met_first(dynamodb_client, table):
    statement = f'SELECT * FROM "{table}" WHERE foo(b) AND c = 1e126'
    column = statement.index("foo(") + 1
    assert _message(dynamodb_client, statement) == f"Unrecognized function: foo at 1:{column}:3"


def test_a_parameter_inside_the_argument_of_an_unknown_function_is_met_first(
    dynamodb_client, table
):
    statement = f'SELECT * FROM "{table}" WHERE foo([?])'
    column = statement.index("?") + 1
    assert _message(dynamodb_client, statement) == (
        f"Unsupported data type: Parameter under key root[0] at 1:{column}:1"
    )


def test_an_operand_with_a_plus_as_the_condition_is_refused_for_it(dynamodb_client, table):
    assert _message(dynamodb_client, f'SELECT * FROM "{table}" WHERE a + 1') == PLUS


@pytest.mark.parametrize(
    "condition",
    [
        "b = 1 + 1",
        "c = 1e126",
        NOTS,
        f"c = 1e126 AND {NOTS}",
        "a IN [" + ", ".join(str(i) for i in range(101)) + "]",
    ],
    ids=["plus", "overflow", "301-operators", "overflow-then-301-operators", "in-of-101"],
)
def test_the_table_name_rule_comes_first(dynamodb_client, condition):
    statement = f'SELECT * FROM "tt" WHERE {condition}'
    assert _message(dynamodb_client, statement) == NAME_RULE


@pytest.mark.parametrize(
    "condition, message",
    [("c = ?", PARAMETERS), ("c = 1e126", OVERFLOW + "root")],
)
def test_a_batch_keeps_the_order(dynamodb_client, table, condition, message):
    statement = f'SELECT * FROM "{table}" WHERE {condition} AND {NOTS}'
    response = dynamodb_client.batch_execute_statement(Statements=[{"Statement": statement}])
    assert response["Responses"] == [{"Error": {"Code": "ValidationError", "Message": message}}]


@pytest.mark.parametrize(
    "condition, message",
    [("c = ?", PARAMETERS), ("c = 1e126", OVERFLOW + "root")],
)
def test_a_transaction_keeps_the_order(dynamodb_client, table, condition, message):
    statement = f'SELECT * FROM "{table}" WHERE {condition} AND {NOTS}'
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
    err = e.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    assert err["Message"] == f"Validation failed in TransactStatements[0]: {message}"


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT a AS b FROM \"tt\" WHERE pk = 'a'",
        "UPDATE \"tt\".\"idx\" SET q = 1 WHERE pk = 'a'",
    ],
    ids=["select-list-alias", "update-of-an-index"],
)
def test_the_table_name_rule_comes_before_a_select_list_alias_and_an_update_of_an_index(
    dynamodb_client, statement
):
    assert _message(dynamodb_client, statement) == NAME_RULE


def test_exists_in_execute_statement_comes_before_the_table_name_rule(dynamodb_client):
    statement = "EXISTS(SELECT * FROM \"tt\" WHERE pk = 'a' AND b = 1 + 1)"
    assert _message(dynamodb_client, statement) == (
        "EXISTS can only be used in ExecuteTransaction write requests."
    )


def test_a_select_list_call_comes_before_an_in_with_no_list(dynamodb_client, table):
    statement = f'SELECT COUNT(*) FROM "{table}" WHERE a IN 1'
    assert _message(dynamodb_client, statement) == PROJECTED_CALL
    response = dynamodb_client.batch_execute_statement(Statements=[{"Statement": statement}])
    assert response["Responses"] == [
        {"Error": {"Code": "ValidationError", "Message": PROJECTED_CALL}}
    ]
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
    err = e.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    assert err["Message"] == f"Validation failed in TransactStatements[0]: {PROJECTED_CALL}"


@pytest.mark.parametrize("sets", ["q = 1e126, r = +b", "r = +b, q = 1e126"])
def test_a_number_in_a_set_clause_comes_before_a_unary_plus(dynamodb_client, table, sets):
    statement = f"UPDATE \"{table}\" SET {sets} WHERE pk = 'a'"
    assert _message(dynamodb_client, statement) == OVERFLOW + "root"


@pytest.mark.parametrize(
    "condition",
    ["c = 1e126", "c = ?", "b = 1 + 1", NOTS, IN_OF_101],
    ids=["overflow", "no-parameter", "plus", "301-operators", "in-of-101"],
)
def test_a_limit_clause_comes_before_the_condition(dynamodb_client, table, condition):
    statement = f'SELECT * FROM "{table}" WHERE {condition} LIMIT 2'
    # The token after LIMIT is named.
    column = statement.index("LIMIT 2") + len("LIMIT ") + 1
    assert _message(dynamodb_client, statement) == f"Unsupported clause: LIMIT at 1:{column}:1"


def test_a_delete_returning_all_with_nothing_after_it_comes_before_the_condition(
    dynamodb_client, table
):
    statement = f"DELETE FROM \"{table}\" WHERE pk = 'a' AND c = 1e126 RETURNING ALL"
    assert _message(dynamodb_client, statement) == (
        "Statement wasn't well formed, can't be processed: "
        "Expected ( MODIFIED | ALL ) ( NEW | OLD ) in each returning element."
    )


@pytest.mark.parametrize("tail", ["", f" AND {NOTS}"], ids=["alone", "then-301-operators"])
def test_the_refusal_of_is_int4_comes_after_its_left_side(dynamodb_client, table, tail):
    statement = f'SELECT * FROM "{table}" WHERE 1e126 IS INT4{tail}'
    assert _message(dynamodb_client, statement) == OVERFLOW + "root"


def test_a_set_parameter_comes_after_the_operator_count_in_a_batch(dynamodb_client, table):
    statement = f"UPDATE \"{table}\" SET q = ? WHERE pk = 'a' AND {NOTS}"
    response = dynamodb_client.batch_execute_statement(Statements=[{"Statement": statement}])
    assert response["Responses"] == [
        {"Error": {"Code": "ValidationError", "Message": TOO_MANY_OPERATORS}}
    ]


def test_a_set_parameter_comes_after_the_operator_count_in_a_transaction(
    dynamodb_client, table
):
    statement = f"UPDATE \"{table}\" SET q = ? WHERE pk = 'a' AND {NOTS}"
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
    assert e.value.response["Error"]["Message"] == (
        f"Validation failed in TransactStatements[0]: {TOO_MANY_OPERATORS}"
    )
