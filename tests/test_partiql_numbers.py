# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for PartiQL number conversion and the checks that
answer before a statement's table is resolved.

Covered: how a number in ``Parameters`` or in the statement text is read and
which message a bad one gets, the order between nesting depth and number
conversion in parameters, the operand-type check on ordering comparisons, and
where these answers fall relative to parse errors, table resolution, the
multi-attribute key refusal and the batch and transaction envelopes.

Runs against real DynamoDB (EXTENDDB_TEST_ENDPOINT unset, AWS credentials) and
against an extenddb server (EXTENDDB_TEST_ENDPOINT set) with identical
assertions. Every expected value was measured on the service (2026-11-02).
Tables use the ``extenddb-measure-pqn-`` prefix and are deleted at the end.
"""

from __future__ import annotations

import os
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active

# ExtendDB walks the members of a parameter map in the order the parsed request
# holds them. That is the order the request sent, as the service walks them
# (measured 2026-11-02), when the server is built together with the MongoDB
# backend crate, whose JSON dependency keeps map order: any build of the whole
# workspace, as CI's are. A build of the server package alone with another
# backend sorts map members by name, and answers differently when a map's first
# member in request order is not its first by name. Not strict, because which
# order a server has depends on how it was built.
MAP_MEMBERS_IN_NAME_ORDER = pytest.mark.xfail(
    bool(os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()),
    reason="a server built without the MongoDB crate checks parameter map members in name order",
    raises=AssertionError,
    strict=False,
)

NOT_WELL_FORMED = "Statement wasn't well formed, can't be processed: "
UNCONVERTIBLE = "The parameter cannot be converted to a numeric value"
OVERFLOW = (
    "Number overflow. Attempting to store a number with magnitude larger than "
    "supported range"
)
UNDERFLOW = (
    "Number underflow. Attempting to store a number with magnitude smaller than "
    "supported range"
)
DIGITS = "Attempting to store more than 38 significant digits in a Number"
NESTED = (
    "Nesting Levels have exceeded supported limits: Attributes in the item have "
    "nested levels beyond supported limit"
)
REFUSED = (
    "PartiQL statements cannot be executed on tables with multi-attribute HASH "
    "keys or multi-attribute RANGE keys."
)
DIGITS_39 = "123456789012345678901234567890123456789"
PROJECTED_CALL = "Unexpected path component at 1:8:5"


def operand_type(op: str, type_code: str) -> str:
    return (
        "Incorrect operand type for operator or function; operator or function: "
        f"{op}, operand type: {type_code}"
    )


def deep(levels: int, leaf: dict | None = None) -> dict:
    """``levels`` maps wrapped around ``leaf``."""
    value = leaf if leaf is not None else {"S": "leaf"}
    for _ in range(levels):
        value = {"M": {"n": value}}
    return value


# ---- tables ------------------------------------------------------------------


@pytest.fixture(scope="module")
def tables(dynamodb_client):
    """A string-keyed composite table, a number-keyed table and an absent name."""
    prefix = f"extenddb-measure-pqn-{uuid.uuid4().hex[:12]}"
    names = {"str": f"{prefix}-s", "num": f"{prefix}-n"}
    dynamodb_client.create_table(
        TableName=names["str"],
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    )
    dynamodb_client.create_table(
        TableName=names["num"],
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "N"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
    )
    try:
        for name in names.values():
            wait_for_active(dynamodb_client, name)
        dynamodb_client.put_item(
            TableName=names["str"],
            Item={"pk": {"S": "p"}, "sk": {"S": "s"}, "a": {"N": "1"}, "val": {"S": "v"}},
        )
        names["absent"] = f"{prefix}-absent"
        yield names
    finally:
        for name in ("str", "num"):
            try:
                dynamodb_client.delete_table(TableName=names[name])
            except ClientError:
                pass


@pytest.fixture(scope="module")
def multi_table(dynamodb_client):
    """A table whose GSI key has two HASH attributes. A backend that cannot store
    a multi-attribute GSI skips the tests that need it."""
    name = f"extenddb-measure-pqn-{uuid.uuid4().hex[:12]}-m"
    try:
        dynamodb_client.create_table(
            TableName=name,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": n, "AttributeType": "S"} for n in ("pk", "h1", "h2", "r1")
            ],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi",
                    "KeySchema": [
                        {"AttributeName": "h1", "KeyType": "HASH"},
                        {"AttributeName": "h2", "KeyType": "HASH"},
                        {"AttributeName": "r1", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
    except ClientError as exc:
        pytest.skip(f"multi-attribute GSI not supported here: {exc}")
    try:
        wait_for_active(dynamodb_client, name)
        yield name
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
        except ClientError:
            pass


# ---- helpers -----------------------------------------------------------------


def error(call) -> dict:
    with pytest.raises(ClientError) as excinfo:
        call()
    return excinfo.value.response["Error"]


def message(client, statement: str, parameters: list | None = None) -> str:
    kwargs: dict = {"Statement": statement}
    if parameters is not None:
        kwargs["Parameters"] = parameters
    err = error(lambda: client.execute_statement(**kwargs))
    assert err["Code"] == "ValidationException", err
    return err["Message"]


def transaction_message(client, statements: list[dict]) -> str:
    err = error(lambda: client.execute_transaction(TransactStatements=statements))
    assert err["Code"] == "ValidationException", err
    return err["Message"]


def batch_errors(client, statements: list[dict]) -> list:
    resp = client.batch_execute_statement(Statements=statements)
    return [entry.get("Error") for entry in resp["Responses"]]


def positions(tables) -> dict:
    """Every position a parameter can take, as statements with one ``?``."""
    s = tables["str"]
    return {
        "where": f"SELECT * FROM \"{s}\" WHERE pk = 'p' AND n = ?",
        "insert": f"INSERT INTO \"{s}\" VALUE {{'pk': 'p-insert', 'sk': 's', 'n': ?}}",
        "update": f"UPDATE \"{s}\" SET n = ? WHERE pk = 'p' AND sk = 's'",
        "key": f"SELECT * FROM \"{s}\" WHERE pk = ?",
    }


POSITIONS = ["where", "insert", "update", "key"]


# ---- parameters ----------------------------------------------------------------


class TestParameterNumbers:
    @pytest.mark.parametrize("position", POSITIONS)
    @pytest.mark.parametrize(
        "value",
        ["1e99999999999", "-1e99999999999", "1e-99999999999", "1e2147483648", "1e-2147483648"],
    )
    def test_an_exponent_or_scale_outside_32_bits_cannot_be_converted(
        self, dynamodb_client, tables, value, position
    ):
        statement = positions(tables)[position]
        assert message(dynamodb_client, statement, [{"N": value}]) == f"{UNCONVERTIBLE}: {value}"

    @pytest.mark.parametrize("position", POSITIONS)
    def test_the_largest_exponent_wraps_to_an_underflow(self, dynamodb_client, tables, position):
        statement = positions(tables)[position]
        assert message(dynamodb_client, statement, [{"N": "1e2147483647"}]) == UNDERFLOW

    @pytest.mark.parametrize("position", POSITIONS)
    def test_an_empty_number_has_nothing_after_the_message(
        self, dynamodb_client, tables, position
    ):
        statement = positions(tables)[position]
        assert message(dynamodb_client, statement, [{"N": ""}]) == UNCONVERTIBLE

    @pytest.mark.parametrize(
        "parameter",
        [
            {"L": [{"N": "1"}, {"N": "1e99999999999"}]},
            {"M": {"x": {"N": "1e99999999999"}}},
            {"NS": ["1", "1e99999999999"]},
        ],
        ids=["list", "map", "number-set"],
    )
    def test_a_number_inside_a_collection_is_converted(self, dynamodb_client, tables, parameter):
        statement = positions(tables)["where"]
        assert (
            message(dynamodb_client, statement, [parameter])
            == f"{UNCONVERTIBLE}: 1e99999999999"
        )

    @pytest.mark.parametrize(
        ("value", "expected"),
        [("1e126", OVERFLOW), ("1e-131", UNDERFLOW), (DIGITS_39, DIGITS)],
        ids=["overflow", "underflow", "digits"],
    )
    def test_a_number_rule_gets_the_bare_message(self, dynamodb_client, tables, value, expected):
        statement = positions(tables)["where"]
        assert message(dynamodb_client, statement, [{"N": value}]) == expected

    def test_forms_the_decimal_reader_accepts_are_stored_normalized(
        self, dynamodb_client, tables
    ):
        s = tables["str"]
        dynamodb_client.execute_statement(
            Statement=(
                f"INSERT INTO \"{s}\" VALUE {{'pk': 'p-forms', 'sk': 's', "
                "'a': ?, 'b': ?, 'c': ?, 'd': ?, 'e': ?, 'f': ?}"
            ),
            Parameters=[
                {"N": "+1"},
                {"N": ".5"},
                {"N": "5."},
                {"N": "1.50"},
                {"N": "1e+2"},
                {"N": "1e00000000001"},
            ],
        )
        item = dynamodb_client.get_item(
            TableName=s, Key={"pk": {"S": "p-forms"}, "sk": {"S": "s"}}, ConsistentRead=True
        )["Item"]
        assert {k: item[k]["N"] for k in "abcdef"} == {
            "a": "1",
            "b": "0.5",
            "c": "5",
            "d": "1.5",
            "e": "100",
            "f": "10",
        }

    @pytest.mark.parametrize(
        "value", [" 1", "1 ", "1e", "1.2.3", "0x10", "Infinity", "1e12345678901"]
    )
    def test_forms_the_decimal_reader_refuses(self, dynamodb_client, tables, value):
        statement = positions(tables)["where"]
        assert message(dynamodb_client, statement, [{"N": value}]) == f"{UNCONVERTIBLE}: {value}"

    def test_a_parameter_is_converted_ahead_of_a_parse_error(self, dynamodb_client, tables):
        statement = f"SELECT * FROM \"{tables['str']}\" WHERE pk = ? AND"
        assert message(dynamodb_client, statement, [{"N": "abc"}]) == f"{UNCONVERTIBLE}: abc"

    def test_a_parameter_is_converted_ahead_of_table_resolution(self, dynamodb_client, tables):
        statement = f"SELECT * FROM \"{tables['absent']}\" WHERE pk = ?"
        assert message(dynamodb_client, statement, [{"N": "abc"}]) == f"{UNCONVERTIBLE}: abc"

    def test_a_parameter_without_a_placeholder_is_converted(self, dynamodb_client, tables):
        statement = f"SELECT * FROM \"{tables['str']}\" WHERE pk = 'p'"
        assert message(dynamodb_client, statement, [{"N": "abc"}]) == f"{UNCONVERTIBLE}: abc"

    def test_execute_transaction_reports_no_statement_position(self, dynamodb_client, tables):
        s = tables["str"]
        statements = [
            {
                "Statement": f"UPDATE \"{s}\" SET n = ? WHERE pk = 'p' AND sk = 's'",
                "Parameters": [{"N": "1e99999999999"}],
            }
        ]
        assert (
            transaction_message(dynamodb_client, statements)
            == f"{UNCONVERTIBLE}: 1e99999999999"
        )

    def test_a_statement_check_ahead_of_a_later_parameter_in_a_transaction(
        self, dynamodb_client, tables
    ):
        s = tables["str"]
        statements = [
            {
                "Statement": f"UPDATE \"{s}\" SET a = 1 WHERE pk = 'p' AND sk = 's' AND val < ?",
                "Parameters": [{"BOOL": True}],
            },
            {
                "Statement": f"UPDATE \"{s}\" SET a = ? WHERE pk = 'q' AND sk = 's'",
                "Parameters": [{"N": "abc"}],
            },
        ]
        assert transaction_message(dynamodb_client, statements) == (
            "Validation failed in TransactStatements[0]: " + operand_type("<", "BOOL")
        )

    def test_batch_execute_statement_fails_only_that_statement(self, dynamodb_client, tables):
        s = tables["str"]
        errors = batch_errors(
            dynamodb_client,
            [
                {"Statement": f"SELECT * FROM \"{s}\" WHERE pk = 'p' AND sk = 's'"},
                {
                    "Statement": f"SELECT * FROM \"{s}\" WHERE pk = 'q' AND sk = 's' AND n = ?",
                    "Parameters": [{"N": "1e99999999999"}],
                },
            ],
        )
        assert errors == [
            None,
            {"Code": "ValidationError", "Message": f"{UNCONVERTIBLE}: 1e99999999999"},
        ]


class TestParameterNesting:
    def pair(self, tables) -> str:
        return f"SELECT * FROM \"{tables['str']}\" WHERE pk = 'p' AND x = ? AND y = ?"

    def single(self, tables) -> str:
        return f"SELECT * FROM \"{tables['str']}\" WHERE pk = 'p' AND x = ?"

    def test_the_first_problem_in_parameter_order_is_reported(self, dynamodb_client, tables):
        bad = {"N": "abc"}
        assert message(dynamodb_client, self.pair(tables), [deep(40), bad]) == NESTED
        assert (
            message(dynamodb_client, self.pair(tables), [bad, deep(40)])
            == f"{UNCONVERTIBLE}: abc"
        )

    @MAP_MEMBERS_IN_NAME_ORDER
    def test_the_first_problem_in_map_member_order_is_reported(self, dynamodb_client, tables):
        deep_first = {"M": {"d": deep(40), "bad": {"N": "abc"}}}
        assert message(dynamodb_client, self.single(tables), [deep_first]) == NESTED

    def test_a_bad_member_ahead_of_a_deep_one_is_reported(self, dynamodb_client, tables):
        bad_first = {"M": {"bad": {"N": "abc"}, "d": deep(40)}}
        assert (
            message(dynamodb_client, self.single(tables), [bad_first])
            == f"{UNCONVERTIBLE}: abc"
        )

    def test_a_bad_number_below_the_limit_is_never_reached(self, dynamodb_client, tables):
        parameter = deep(40, {"N": "abc"})
        assert message(dynamodb_client, self.single(tables), [parameter]) == NESTED

    def test_depth_is_reported_ahead_of_a_later_overflow(self, dynamodb_client, tables):
        parameters = [deep(40), {"N": "1e126"}]
        assert message(dynamodb_client, self.pair(tables), parameters) == NESTED

    def test_a_parameter_holds_32_levels_and_refuses_33(self, dynamodb_client, tables):
        resp = dynamodb_client.execute_statement(
            Statement=self.single(tables), Parameters=[deep(32)]
        )
        assert resp["Items"] == []
        assert message(dynamodb_client, self.single(tables), [deep(33)]) == NESTED

    def test_execute_transaction_reports_depth_without_a_position(
        self, dynamodb_client, tables
    ):
        statements = [
            {
                "Statement": (
                    f"UPDATE \"{tables['str']}\" SET x = ?, y = ? WHERE pk = 'p' AND sk = 's'"
                ),
                "Parameters": [deep(40), {"N": "abc"}],
            }
        ]
        assert transaction_message(dynamodb_client, statements) == NESTED

    def test_batch_execute_statement_reports_depth_per_statement(self, dynamodb_client, tables):
        errors = batch_errors(
            dynamodb_client,
            [{"Statement": self.pair(tables), "Parameters": [deep(40), {"N": "abc"}]}],
        )
        assert errors == [{"Code": "ValidationError", "Message": NESTED}]


# ---- literals ------------------------------------------------------------------


class TestLiteralNumbers:
    @pytest.mark.parametrize("literal", ["1e2147483648", "1e99999999999", "1.5e-2147483647"])
    def test_a_literal_that_cannot_be_converted_has_nothing_after_the_colon(
        self, dynamodb_client, tables, literal
    ):
        statement = f"SELECT * FROM \"{tables['num']}\" WHERE pk = {literal}"
        assert message(dynamodb_client, statement) == NOT_WELL_FORMED

    def test_a_literal_that_cannot_be_converted_wins_over_an_earlier_parse_error(
        self, dynamodb_client, tables
    ):
        statement = f"FOO * FROM \"{tables['num']}\" WHERE pk = 1e99999999999"
        assert message(dynamodb_client, statement) == NOT_WELL_FORMED

    # `1.2.3` reads as two numbers; tests/test_partiql_grammar.py covers it.
    @pytest.mark.parametrize("literal", ["1e"])
    def test_a_malformed_number_literal_has_nothing_after_the_colon(
        self, dynamodb_client, tables, literal
    ):
        statement = f"SELECT * FROM \"{tables['num']}\" WHERE pk = {literal}"
        assert message(dynamodb_client, statement) == NOT_WELL_FORMED

    @pytest.mark.parametrize(
        ("clause", "expected"),
        [
            ("WHERE pk = 1e126", f"{OVERFLOW} under root"),
            ("WHERE pk = 1e-131", f"{UNDERFLOW} under root"),
            (f"WHERE pk = {DIGITS_39}", f"{DIGITS} under root"),
            ("WHERE pk = 1e2147483647", f"{UNDERFLOW} under root"),
            ("WHERE pk IN [1, 1e126]", f"{OVERFLOW} under root"),
        ],
        ids=["overflow", "underflow", "digits", "wrap", "in-list"],
    )
    def test_a_where_literal_names_root(self, dynamodb_client, tables, clause, expected):
        statement = f"SELECT * FROM \"{tables['num']}\" {clause}"
        assert message(dynamodb_client, statement) == expected

    def test_write_literals_name_root(self, dynamodb_client, tables):
        n = tables["num"]
        assert (
            message(dynamodb_client, f"INSERT INTO \"{n}\" VALUE {{'pk': 1e126}}")
            == f"{OVERFLOW} under root"
        )
        assert (
            message(dynamodb_client, f"UPDATE \"{n}\" SET a = 1e-131 WHERE pk = 1")
            == f"{UNDERFLOW} under root"
        )

    @pytest.mark.parametrize(
        ("value", "position"),
        [
            ("{'x': 1e126}", "root.x"),
            ("[1, 1e126]", "root[1]"),
            ("{'a': {'b': 1e126}}", "root.a.b"),
            ("[[0, 1e126]]", "root[0][1]"),
            ("{'a': [1, 1e126]}", "root.a[1]"),
            ("[{'a': 1e126}]", "root[0].a"),
            ("<<1, 1e126>>", "root"),
        ],
        ids=["map", "list", "map-map", "list-list", "map-list", "list-map", "set"],
    )
    def test_a_literal_inside_a_value_names_its_position(
        self, dynamodb_client, tables, value, position
    ):
        statement = f"INSERT INTO \"{tables['num']}\" VALUE {{'pk': 1, 'v': {value}}}"
        assert message(dynamodb_client, statement) == f"{OVERFLOW} under {position}"

    def test_a_literal_rule_answers_ahead_of_table_resolution(self, dynamodb_client, tables):
        a = tables["absent"]
        assert (
            message(dynamodb_client, f"SELECT * FROM \"{a}\" WHERE pk = 1e126")
            == f"{OVERFLOW} under root"
        )
        assert (
            message(dynamodb_client, f"INSERT INTO \"{a}\" VALUE {{'pk': 1e126}}")
            == f"{OVERFLOW} under root"
        )

    def test_a_literal_rule_answers_ahead_of_the_key_type(self, dynamodb_client, tables):
        statement = f"SELECT * FROM \"{tables['str']}\" WHERE pk = 1e126"
        assert message(dynamodb_client, statement) == f"{OVERFLOW} under root"

    def test_a_literal_rule_in_a_transaction_names_the_statement(self, dynamodb_client, tables):
        statements = [{"Statement": f"UPDATE \"{tables['num']}\" SET a = 1e126 WHERE pk = 1"}]
        assert transaction_message(dynamodb_client, statements) == (
            f"Validation failed in TransactStatements[0]: {OVERFLOW} under root"
        )

    def test_a_literal_rule_in_a_batch_fails_that_statement(self, dynamodb_client, tables):
        errors = batch_errors(
            dynamodb_client,
            [{"Statement": f"UPDATE \"{tables['num']}\" SET a = 1e126 WHERE pk = 1"}],
        )
        assert errors == [{"Code": "ValidationError", "Message": f"{OVERFLOW} under root"}]


# ---- ordering operand types ------------------------------------------------------


class TestOrderingOperandTypes:
    @pytest.mark.parametrize("op", ["<", "<=", ">", ">="])
    def test_each_operator_is_named_as_written(self, dynamodb_client, tables, op):
        statement = f"SELECT pk FROM \"{tables['str']}\" WHERE pk = ? AND val {op} ?"
        parameters = [{"S": "p"}, {"BOOL": True}]
        assert message(dynamodb_client, statement, parameters) == operand_type(op, "BOOL")

    def test_between_is_named(self, dynamodb_client, tables):
        statement = f"SELECT pk FROM \"{tables['str']}\" WHERE pk = 'p' AND val BETWEEN ? AND ?"
        parameters = [{"BOOL": True}, {"BOOL": False}]
        assert message(dynamodb_client, statement, parameters) == operand_type("BETWEEN", "BOOL")

    def test_the_left_operand_is_reported_first(self, dynamodb_client, tables):
        statement = f"SELECT pk FROM \"{tables['str']}\" WHERE pk = 'p' AND ? < ?"
        parameters = [{"NULL": True}, {"BOOL": True}]
        assert message(dynamodb_client, statement, parameters) == operand_type("<", "NULL")

    @pytest.mark.parametrize(
        "statement",
        [
            "SELECT pk FROM \"{t}\" WHERE pk = 'p' AND val < ?",
            "UPDATE \"{t}\" SET a = 1 WHERE pk = 'p' AND sk = 's' AND val < ?",
            "DELETE FROM \"{t}\" WHERE pk = 'p' AND sk = 's' AND val < ?",
        ],
        ids=["select", "update", "delete"],
    )
    def test_the_check_answers_ahead_of_table_resolution(
        self, dynamodb_client, tables, statement
    ):
        text = statement.format(t=tables["absent"])
        assert message(dynamodb_client, text, [{"BOOL": True}]) == operand_type("<", "BOOL")

    def test_a_literal_operand_is_checked(self, dynamodb_client, tables):
        statement = f"SELECT pk FROM \"{tables['absent']}\" WHERE pk = 'p' AND val < true"
        assert message(dynamodb_client, statement) == operand_type("<", "BOOL")

    def test_a_batch_statement_gets_the_bare_message(self, dynamodb_client, tables):
        s = tables["str"]
        errors = batch_errors(
            dynamodb_client,
            [
                {
                    "Statement": f"SELECT * FROM \"{s}\" WHERE pk = 'p' AND sk = 's' AND val < ?",
                    "Parameters": [{"BOOL": True}],
                }
            ],
        )
        assert errors == [{"Code": "ValidationError", "Message": operand_type("<", "BOOL")}]

    def test_a_transaction_statement_is_named(self, dynamodb_client, tables):
        s = tables["str"]
        statements = [
            {
                "Statement": f"UPDATE \"{s}\" SET a = 1 WHERE pk = 'p' AND sk = 's' AND val < ?",
                "Parameters": [{"BOOL": True}],
            }
        ]
        assert transaction_message(dynamodb_client, statements) == (
            "Validation failed in TransactStatements[0]: " + operand_type("<", "BOOL")
        )

    def test_a_function_operand_type_message_is_bare(self, dynamodb_client, tables):
        statement = f"SELECT * FROM \"{tables['str']}\" WHERE pk = 'p' AND begins_with(val, ?)"
        assert message(dynamodb_client, statement, [{"N": "1"}]) == operand_type(
            "begins_with", "N"
        )


# ---- multi-attribute key refusal -----------------------------------------------


class TestMultiAttributeOrder:
    def test_an_unconvertible_literal_is_reported_ahead_of_the_refusal(
        self, dynamodb_client, multi_table
    ):
        statement = f"SELECT * FROM \"{multi_table}\".\"gsi\" WHERE h1 = 1e99999999999"
        assert message(dynamodb_client, statement) == NOT_WELL_FORMED

    def test_an_unconvertible_parameter_is_reported_ahead_of_the_refusal(
        self, dynamodb_client, multi_table
    ):
        statement = f"SELECT * FROM \"{multi_table}\".\"gsi\" WHERE h1 = ?"
        assert (
            message(dynamodb_client, statement, [{"N": "1e99999999999"}])
            == f"{UNCONVERTIBLE}: 1e99999999999"
        )

    def test_the_refusal_is_reported_ahead_of_a_projected_call(
        self, dynamodb_client, multi_table
    ):
        statement = f"SELECT COUNT(*) FROM \"{multi_table}\".\"gsi\""
        assert message(dynamodb_client, statement) == REFUSED

    def test_a_batch_reports_the_refusal_ahead_of_a_projected_call(
        self, dynamodb_client, multi_table
    ):
        statement = f"SELECT COUNT(*) FROM \"{multi_table}\".\"gsi\" WHERE h1 = 'a'"
        assert batch_errors(dynamodb_client, [{"Statement": statement}]) == [
            {"Code": "ValidationError", "Message": REFUSED}
        ]


class TestProjectedCall:
    """The three operations check a projected call after the table name rule
    and ahead of number literals and operand types."""

    TABLE_NAME_RULE = (
        "Table name should be within the length [3, 255] and only contain 'a-z', 'A-Z', "
        "'0-9', '-', '_', '.'."
    )

    def answers(self, client, statement: str) -> tuple:
        """The message from ExecuteStatement, the member error from
        BatchExecuteStatement and the message from ExecuteTransaction."""
        members = [{"Statement": statement}]
        return (
            message(client, statement),
            batch_errors(client, members),
            transaction_message(client, members),
        )

    def test_the_table_name_rule_is_reported_ahead_of_a_projected_call(self, dynamodb_client):
        rule = self.TABLE_NAME_RULE
        assert self.answers(dynamodb_client, "SELECT COUNT(*) FROM \"t\" WHERE pk = 'a'") == (
            rule,
            [{"Code": "ValidationError", "Message": rule}],
            f"Validation failed in TransactStatements[0]: {rule}",
        )

    @pytest.mark.parametrize(
        "where", ["pk = 1e126", "pk = 'p' AND v < true"], ids=["number", "operand-type"]
    )
    def test_a_projected_call_is_reported_ahead_of_the_literal_checks(
        self, dynamodb_client, tables, where
    ):
        statement = f"SELECT COUNT(*) FROM \"{tables['str']}\" WHERE {where}"
        assert self.answers(dynamodb_client, statement) == (
            PROJECTED_CALL,
            [{"Code": "ValidationError", "Message": PROJECTED_CALL}],
            f"Validation failed in TransactStatements[0]: {PROJECTED_CALL}",
        )

    @pytest.mark.parametrize("where", ["", " WHERE pk = 'p'"], ids=["no-where", "where"])
    def test_a_projected_call_is_reported_ahead_of_a_missing_table(
        self, dynamodb_client, tables, where
    ):
        statement = f"SELECT COUNT(*) FROM \"{tables['absent']}\"{where}"
        assert message(dynamodb_client, statement) == PROJECTED_CALL

    @pytest.mark.parametrize("table", ["str", "absent"])
    def test_a_batch_fails_only_the_statement_with_a_projected_call(
        self, dynamodb_client, tables, table
    ):
        s = tables["str"]
        errors = batch_errors(
            dynamodb_client,
            [
                {"Statement": f"SELECT * FROM \"{s}\" WHERE pk = 'p' AND sk = 's'"},
                {
                    "Statement": (
                        f"SELECT COUNT(*) FROM \"{tables[table]}\" WHERE pk = 'q' AND sk = 's'"
                    )
                },
            ],
        )
        assert errors == [None, {"Code": "ValidationError", "Message": PROJECTED_CALL}]

    @pytest.mark.parametrize("position", [0, 1])
    @pytest.mark.parametrize("table", ["str", "absent"])
    def test_a_transaction_names_the_member_with_a_projected_call(
        self, dynamodb_client, tables, table, position
    ):
        statements = [
            {"Statement": f"SELECT * FROM \"{tables['str']}\" WHERE pk = 'p' AND sk = 's'"}
        ]
        count = f"SELECT COUNT(*) FROM \"{tables[table]}\" WHERE pk = 'q' AND sk = 's'"
        statements.insert(position, {"Statement": count})
        assert transaction_message(dynamodb_client, statements) == (
            f"Validation failed in TransactStatements[{position}]: {PROJECTED_CALL}"
        )
