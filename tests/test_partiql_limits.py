# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the limits on a PartiQL statement: its length,
the operators its WHERE condition may hold, how deep a literal value may nest,
signs written before an operand, and statements nested deep enough that the
server must refuse them rather than run out of stack.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both.
"""

from __future__ import annotations

import os

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import scoped_table

WF = "Statement wasn't well formed, can't be processed: "
TOO_MANY_OPERATORS = "The expression contains too many operators; operator count: 301"
MISSING_RHS = WF + "Missing right-hand side expression of infix operator"
MAX_LENGTH = 8192


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """A table keyed on a string pk."""
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="module")
def once(endpoint_url):
    """A client that sends each request once and does not check parameters.

    The service answers some of the statements below with an internal error,
    which a client would otherwise retry.
    """
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(parameter_validation=False, retries={"total_max_attempts": 1}),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


def _where(table: str) -> str:
    return f'SELECT * FROM "{table}" WHERE '


def _items(client, statement: str) -> list:
    return client.execute_statement(Statement=statement).get("Items", [])


def _message(client, statement: str) -> str:
    with pytest.raises(ClientError) as e:
        client.execute_statement(Statement=statement)
    err = e.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


def _nested(levels: int) -> str:
    """The path of the value inside `levels` nested lists."""
    return "root" + "[0]" * levels


# --- length -----------------------------------------------------------------


def test_a_statement_of_8192_characters_runs(once, table):
    head = _where(table) + "pk = 'a'"
    assert _items(once, head + " " * (MAX_LENGTH - len(head))) == []


def test_a_statement_one_character_longer_is_refused_echoing_it(once, table):
    head = _where(table) + "pk = 'a'"
    statement = head + " " * (MAX_LENGTH + 1 - len(head))
    assert _message(once, statement) == (
        f"1 validation error detected: Value '{statement}' at 'statement' failed to "
        "satisfy constraint: Member must have length less than or equal to 8192"
    )


def test_the_next_page_of_a_long_statement_of_four_byte_characters_runs(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        for key in ["a", "b"]:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": key}, "note": {"S": "n"}})
        statement = f"SELECT * FROM \"{name}\" WHERE note <> '{chr(0x1F600) * 6200}'"
        keys, token = [], None
        for _ in range(4):
            args = {"NextToken": token} if token else {}
            response = dynamodb_client.execute_statement(Statement=statement, Limit=1, **args)
            keys += [item["pk"]["S"] for item in response["Items"]]
            token = response.get("NextToken")
            if not token:
                break
        assert sorted(keys) == ["a", "b"]


# --- operators --------------------------------------------------------------


def test_299_nots_before_a_comparison_run(dynamodb_client, table):
    assert _items(dynamodb_client, _where(table) + "NOT " * 299 + "a = 1") == []


def test_300_nots_before_a_comparison_are_too_many_operators(dynamodb_client, table):
    statement = _where(table) + "NOT " * 300 + "a = 1"
    assert _message(dynamodb_client, statement) == TOO_MANY_OPERATORS


@pytest.mark.parametrize("terms", [151, 299, 300, 301])
def test_an_and_chain_past_300_operators_is_refused(dynamodb_client, table, terms):
    # Each comparison and each AND count: 151 terms are 301 operators.
    condition = " AND ".join(f"a{i} = 1" for i in range(terms))
    assert _message(dynamodb_client, _where(table) + condition) == TOO_MANY_OPERATORS


@pytest.mark.parametrize("joiner", [" AND ", " OR "])
def test_150_comparisons_are_299_operators_and_run(dynamodb_client, table, joiner):
    condition = joiner.join(f"a{i} = 1" for i in range(150))
    assert _items(dynamodb_client, _where(table) + condition) == []


def test_a_run_of_nots_in_a_write_condition_keeps_its_parity(dynamodb_client, table):
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{table}\" VALUE {{'pk': 'nots', 'a': 2}}"
    )
    update = f"UPDATE \"{table}\" SET b = 1 WHERE pk = 'nots' AND "
    # 297 NOTs: the key equality, AND, the NOTs and `a = 1` are 300 operators.
    dynamodb_client.execute_statement(Statement=update + "NOT " * 297 + "a = 1")
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(Statement=update + "NOT " * 296 + "a = 1")
    assert e.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
    item = _items(dynamodb_client, _where(table) + "pk = 'nots'")
    assert item == [{"pk": {"S": "nots"}, "a": {"N": "2"}, "b": {"N": "1"}}]


# --- what comes before the operator count ----------------------------------


NOTS = "NOT " * 300


@pytest.mark.parametrize(
    "tail, message",
    [
        (
            "a = 1 AND b = 1e126",
            "Number overflow. Attempting to store a number with magnitude larger than "
            "supported range under root",
        ),
        (
            "a = 1 AND b = 1e-131",
            "Number underflow. Attempting to store a number with magnitude smaller than "
            "supported range under root",
        ),
        (
            "a = 1 AND b = 1" + "1" * 40,
            "Attempting to store more than 38 significant digits in a Number under root",
        ),
        ("a = 1 AND b = ?", "Number of parameters in request and statement don't match."),
    ],
)
def test_a_number_or_parameter_problem_comes_before_the_operator_count(
    dynamodb_client, table, tail, message
):
    assert _message(dynamodb_client, _where(table) + NOTS + tail) == message


def test_a_number_problem_before_the_operators_comes_before_a_missing_table(
    dynamodb_client, table
):
    statement = f'SELECT * FROM "{table}-missing" WHERE b = 1e126 AND {NOTS}a = 1'
    assert _message(dynamodb_client, statement) == (
        "Number overflow. Attempting to store a number with magnitude larger than "
        "supported range under root"
    )


@pytest.mark.parametrize(
    "tail, message",
    [
        (
            "a = 1 AND b = [" + "[" * 32 + "1" + "]" * 33,
            "Nesting Levels have exceeded supported limits under " + _nested(32),
        ),
        ("a = 1 AND b = 1 + 1", "Unsupported operator in Condition Expression. Operator: +"),
        ("a = 1 AND b = 'x' || 'y'", "Unsupported operator in Condition Expression. Operator: ||"),
        ("a = 1 AND", MISSING_RHS),
        ("a = 1 AND b = MISSING", "MISSING must be compared using IS operator"),
        # A call with too many arguments is refused after the operator count.
        ("a = 1 AND size(b, c) = 1", TOO_MANY_OPERATORS),
    ],
)
def test_the_other_refusals_keep_their_place_around_the_operator_count(
    dynamodb_client, table, tail, message
):
    assert _message(dynamodb_client, _where(table) + NOTS + tail) == message


def test_an_unknown_function_comes_before_the_operator_count(dynamodb_client, table):
    statement = _where(table) + NOTS + "a = 1 AND foo(b)"
    column = statement.index("foo(") + 1
    assert _message(dynamodb_client, statement) == f"Unrecognized function: foo at 1:{column}:3"


# --- long conditions within the operator limit -------------------------------


def test_150_begins_with_calls_anded_run(dynamodb_client, table):
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{table}\" VALUE {{'pk': 'bw', 'bw': 'xy'}}"
    )
    condition = " AND ".join(["begins_with(bw, 'x')"] * 150)
    assert _items(dynamodb_client, _where(table) + condition) == [
        {"pk": {"S": "bw"}, "bw": {"S": "xy"}}
    ]


def test_35_in_lists_of_100_members_ored_run(dynamodb_client, table):
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{table}\" VALUE {{'pk': 'ors', 'ors': 1}}"
    )
    condition = " OR ".join("ors IN [" + ",".join(["1"] * 100) + "]" for _ in range(35))
    statement = _where(table) + condition
    assert len(statement) <= MAX_LENGTH
    assert _items(dynamodb_client, statement) == [{"pk": {"S": "ors"}, "ors": {"N": "1"}}]


def test_a_projection_of_1000_paths_runs(dynamodb_client, table):
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{table}\" VALUE {{'pk': 'paths', 'x5': 5}}"
    )
    paths = ", ".join(f"x{i}" for i in range(1000))
    statement = f"SELECT {paths} FROM \"{table}\" WHERE pk = 'paths'"
    assert _items(dynamodb_client, statement) == [{"x5": {"N": "5"}}]


def test_a_set_clause_of_400_assignments_runs(dynamodb_client, table):
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{table}\" VALUE {{'pk': 'sets'}}"
    )
    sets = ", ".join(f"x{i} = {i}" for i in range(400))
    dynamodb_client.execute_statement(
        Statement=f"UPDATE \"{table}\" SET {sets} WHERE pk = 'sets'"
    )
    [item] = _items(dynamodb_client, _where(table) + "pk = 'sets'")
    assert item == {"pk": {"S": "sets"}, **{f"x{i}": {"N": str(i)} for i in range(400)}}


@pytest.mark.parametrize("members", [101, 1000])
def test_an_in_list_of_more_than_100_members_is_refused(dynamodb_client, table, members):
    statement = _where(table) + "a IN [" + ", ".join(str(i) for i in range(members)) + "]"
    assert _message(dynamodb_client, statement) == (
        f"The IN operator is provided with too many operands; number of operands: {members}"
    )


# --- conditions nested within the operator limit -----------------------------


def _alternated(joiner: str, depth: int) -> str:
    """`NOT (a = 1 <joiner> NOT (a = 1 <joiner> ...))`, `depth` levels."""
    return f"NOT (a = 1 {joiner} " * depth + "a = 1" + ")" * depth


@pytest.mark.parametrize(
    "joiner, depth", [("AND", 99), ("OR", 74), ("OR", 99)]
)
def test_not_alternated_with_and_or_or_to_depth_99_runs(dynamodb_client, table, joiner, depth):
    dynamodb_client.execute_statement(Statement=_where(table) + _alternated(joiner, depth))


def test_not_alternated_with_and_to_depth_100_is_too_many_operators(dynamodb_client, table):
    statement = _where(table) + _alternated("AND", 100)
    assert _message(dynamodb_client, statement) == TOO_MANY_OPERATORS


@pytest.mark.parametrize("depth, runs", [(149, True), (150, False)])
def test_and_nested_to_depth_149_runs(dynamodb_client, table, depth, runs):
    statement = _where(table) + "(a = 1 AND " * depth + "a = 1" + ")" * depth
    if runs:
        dynamodb_client.execute_statement(Statement=statement)
    else:
        assert _message(dynamodb_client, statement) == TOO_MANY_OPERATORS


@pytest.mark.parametrize("depth, runs", [(99, True), (140, False)])
def test_nested_and_of_size_calls(dynamodb_client, table, depth, runs):
    statement = _where(table) + "(size(a) = 1 AND " * depth + "a = 1" + ")" * depth
    if runs:
        dynamodb_client.execute_statement(Statement=statement)
    else:
        assert _message(dynamodb_client, statement) == TOO_MANY_OPERATORS


@pytest.mark.parametrize("depth", [99, 148])
def test_an_update_condition_nested_to_depth_148_runs(dynamodb_client, table, depth):
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{table}\" VALUE {{'pk': 'nested{depth}', 'n': 1}}"
    )
    dynamodb_client.execute_statement(
        Statement=f"UPDATE \"{table}\" SET q = 1 WHERE pk = 'nested{depth}' AND "
        + "(n = 1 AND " * depth
        + "n = 1"
        + ")" * depth
    )


# --- groups -----------------------------------------------------------------


@pytest.mark.parametrize("after_key", [False, True])
def test_1000_nested_groups_run(dynamodb_client, table, after_key):
    head = _where(table) + ("pk = 'a' AND " if after_key else "")
    assert _items(dynamodb_client, head + "(" * 1000 + "a = 1" + ")" * 1000) == []


# --- literal nesting --------------------------------------------------------


@pytest.mark.parametrize("levels", [32, 33, 34, 40, 100, 1000])
def test_a_list_literal_nested_32_levels_or_more_names_the_32nd(
    dynamodb_client, table, levels
):
    statement = (
        f"INSERT INTO \"{table}\" VALUE {{'pk': 'l{levels}', 'x': "
        + "[" * levels
        + "1"
        + "]" * levels
        + "}"
    )
    assert _message(dynamodb_client, statement) == (
        "Nesting Levels have exceeded supported limits under " + _nested(32)
    )


@pytest.mark.parametrize("levels", [32, 33, 34, 40, 100, 1000])
def test_a_map_literal_nested_32_levels_or_more_names_the_32nd(
    dynamodb_client, table, levels
):
    statement = (
        f"INSERT INTO \"{table}\" VALUE {{'pk': 'm{levels}', 'x': "
        + "{'a': " * levels
        + "1"
        + "}" * levels
        + "}"
    )
    assert _message(dynamodb_client, statement) == (
        "Nesting Levels have exceeded supported limits under root" + ".a" * 32
    )


@pytest.mark.parametrize("levels", [33, 1000])
def test_a_deep_list_literal_in_a_condition_names_the_32nd(dynamodb_client, table, levels):
    statement = _where(table) + "x = " + "[" * levels + "1" + "]" * levels
    assert _message(dynamodb_client, statement) == (
        "Nesting Levels have exceeded supported limits under " + _nested(32)
    )


def test_a_deep_list_literal_as_a_set_value_names_the_32nd(dynamodb_client, table):
    statement = f"UPDATE \"{table}\" SET x = " + "[" * 33 + "1" + "]" * 33 + " WHERE pk = 'u'"
    assert _message(dynamodb_client, statement) == (
        "Nesting Levels have exceeded supported limits under " + _nested(32)
    )


def test_a_list_literal_of_31_levels_is_stored(dynamodb_client, table):
    statement = (
        f"INSERT INTO \"{table}\" VALUE {{'pk': 'l31', 'x': " + "[" * 31 + "1" + "]" * 31 + "}"
    )
    dynamodb_client.execute_statement(Statement=statement)
    [item] = _items(dynamodb_client, _where(table) + "pk = 'l31'")
    value, levels = item["x"], 0
    while "L" in value:
        value, levels = value["L"][0], levels + 1
    assert (levels, value) == (31, {"N": "1"})


def test_a_deep_literal_that_breaks_off_reports_the_syntax_error(dynamodb_client, table):
    statement = (
        f"INSERT INTO \"{table}\" VALUE {{'pk':'mix','x':{{'a':[{{'b':"
        + "[" * 30
        + "1"
        + "]" * 30
        + "}]}}}}"
    )
    assert _message(dynamodb_client, statement) == WF + "Unexpected token after expression"


# --- signs ------------------------------------------------------------------


@pytest.mark.parametrize("signs", [2, 10, 100])
@pytest.mark.parametrize("operand", ["1", "b"])
def test_two_minus_signs_start_a_comment(dynamodb_client, table, signs, operand):
    statement = _where(table) + "a = " + "-" * signs + operand
    assert _message(dynamodb_client, statement) == MISSING_RHS


def test_1000_plus_signs_before_a_name_are_refused_naming_the_operator(dynamodb_client, table):
    # The service gives this answer for 1000 signs on every try. From about
    # 1500 signs it sometimes answers InternalFailure instead, so the test
    # stays at 1000.
    statement = _where(table) + "a = " + "+" * 1000 + "a"
    assert _message(dynamodb_client, statement) == (
        "Unsupported operator in Condition Expression. Operator: +"
    )


# --- statements nested to the length limit ----------------------------------


def _fill(head: str, open_: str, inner: str, close: str = "", tail: str = "") -> str:
    """`head`, as many `open_` as fit in the length limit, `inner`, as many
    `close`, then `tail`."""
    times = (MAX_LENGTH - len(head) - len(inner) - len(tail)) // (len(open_) + len(close))
    return head + open_ * times + inner + close * times + tail


def _shapes(table: str) -> dict[str, str]:
    where = _where(table)
    insert = f"INSERT INTO \"{table}\" VALUE {{'pk': 'deep', 'x': "
    return {
        "groups": _fill(where, "(", "a = 1", ")"),
        "groups after a key": _fill(where + "pk = 'a' AND ", "(", "a = 1", ")"),
        "nots": _fill(where, "NOT ", "a = 1"),
        "plus signs": _fill(where + "a = ", "+", "a"),
        "spaced minus signs": _fill(where + "a = ", "- ", "b"),
        "bags": _fill(where + "a = ", "<<", "1", ">>"),
        "lists": _fill(insert, "[", "1", "]", "}"),
        "maps": _fill(insert, "{'a': ", "1", "}", "}"),
        "and chain": where + " AND ".join(f"a{i} = 1" for i in range(600)),
        "or chain": where + " OR ".join(f"a{i} = 1" for i in range(600)),
        "calls": _fill(f"UPDATE \"{table}\" SET a = ", "f(", "x", ")", " WHERE pk = 'p'"),
    }


SHAPES = list(_shapes("t"))


@pytest.mark.parametrize("shape", SHAPES)
def test_a_statement_nested_to_the_length_limit_is_refused_and_the_server_keeps_serving(
    once, table, shape
):
    statement = _shapes(table)[shape]
    assert len(statement) <= MAX_LENGTH
    with pytest.raises(ClientError):
        once.execute_statement(Statement=statement)
    assert _items(once, _where(table) + "pk = 'a'") == []
