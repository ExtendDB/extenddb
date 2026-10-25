# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Scan checks the numbers in ``ExpressionAttributeValues`` at request time.

An ``N`` value that is not a number, an ``N`` nested in an ``L`` or ``M``, an
``NS`` member, an overflow, an underflow, a value with more than 38
significant digits or an empty ``N`` is refused with ``ValidationException``
before any row is read, whether a ``FilterExpression`` reads the value,
nothing reads it, or the request carries no expression at all. Every expected
answer below was measured against the service on 2026-10-25 (09:23 to 09:28
UTC) with each request sent twice.

The rows also pin where the check falls against the other early Scan checks:
after the ``Limit``, ``Segment`` and ``TotalSegments`` range constraints (one
aggregated message, ``Value at 'Limit'`` with no value echoed), the expression
and non-expression parameter mix and the ``ReturnConsumedCapacity`` enum; and
before the ``Select`` enum, the Segment pairing checks, the table lookup, the
IndexName, ConsistentRead, Select with ProjectionExpression,
ExclusiveStartKey, undefined placeholder, unused value and filter syntax
checks.

Dual-target: runs against the service and against extenddb with the same
assertions.

Run:
  EXTENDDB_TEST_ENDPOINT=https://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_scan_number_values.py -v
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

NUMERIC = "The parameter cannot be converted to a numeric value: "
OVERFLOW = (
    "Number overflow. Attempting to store a number with magnitude larger than "
    "supported range"
)
UNDERFLOW = (
    "Number underflow. Attempting to store a number with magnitude smaller than "
    "supported range"
)
TOO_MANY_DIGITS = "Attempting to store more than 38 significant digits in a Number"
LIMIT_RANGE = (
    "Value at 'Limit' failed to satisfy constraint: "
    "Member must have value greater than or equal to 1"
)
RCC_ENUM = (
    "1 validation error detected: Value 'NOPE' at 'returnConsumedCapacity' failed "
    "to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
)
SELECT_ENUM = (
    "1 validation error detected: Value 'NOPE' at 'select' failed to satisfy "
    "constraint: Member must satisfy enum value set: "
    "[SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"
)

S = {"AttributeType": "S"}
SX = {"S": "x"}
NB = {"N": "bob"}
NB_FILTER = {"FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": NB}}
OK_FILTER = {"FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "1"}}}
SCAN_FILTER = {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [SX]}}


def _err(exc_info) -> tuple[str, str]:
    err = exc_info.value.response["Error"]
    return err["Code"], err["Message"]


def _scan_error(client, **kwargs) -> tuple[str, str]:
    with pytest.raises(ClientError) as exc_info:
        client.scan(**kwargs)
    return _err(exc_info)


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """pk S, sk S; GSI gsi1 on gpk S (hash only)."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", **S},
            {"AttributeName": "sk", **S},
            {"AttributeName": "gpk", **S},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
    ) as name:
        for pk in ("alice", "bob"):
            dynamodb_client.put_item(TableName=name, Item={
                "pk": {"S": pk}, "sk": {"S": "1"}, "gpk": {"S": pk}, "a": {"N": "1"}})
        yield name


class TestNumberValues:
    """Every number in ExpressionAttributeValues is checked."""

    @pytest.mark.parametrize("value, expected", [
        pytest.param(NB, NUMERIC + "bob", id="N-bob"),
        pytest.param({"L": [SX, NB]}, NUMERIC + "bob", id="L-with-N-bob"),
        pytest.param({"M": {"a": NB}}, NUMERIC + "bob", id="M-with-N-bob"),
        pytest.param({"NS": ["1", "bob"]}, NUMERIC + "bob", id="NS-with-bob"),
        pytest.param({"M": {"a": {"L": [{"M": {"b": NB}}]}}}, NUMERIC + "bob", id="M-L-M-N-bob"),
        pytest.param({"N": "1e200"}, OVERFLOW, id="overflow"),
        pytest.param({"N": "1e-200"}, UNDERFLOW, id="underflow"),
        pytest.param({"N": "1" * 39}, TOO_MANY_DIGITS, id="39-digits"),
        pytest.param({"N": ""}, NUMERIC, id="empty"),
        pytest.param({"N": " 1"}, NUMERIC + " 1", id="leading-space"),
    ])
    def test_value_read_by_the_filter(self, dynamodb_client, table, value, expected):
        assert _scan_error(
            dynamodb_client, TableName=table,
            FilterExpression="a = :n", ExpressionAttributeValues={":n": value},
        ) == ("ValidationException", expected)

    @pytest.mark.parametrize("value, expected", [
        pytest.param(NB, NUMERIC + "bob", id="N-bob"),
        pytest.param({"L": [SX, NB]}, NUMERIC + "bob", id="L-with-N-bob"),
        pytest.param({"M": {"a": NB}}, NUMERIC + "bob", id="M-with-N-bob"),
        pytest.param({"NS": ["1", "bob"]}, NUMERIC + "bob", id="NS-with-bob"),
        pytest.param({"M": {"a": {"L": [{"M": {"b": NB}}]}}}, NUMERIC + "bob", id="M-L-M-N-bob"),
        pytest.param({"N": "1e200"}, OVERFLOW, id="overflow"),
        pytest.param({"N": "1" * 39}, TOO_MANY_DIGITS, id="39-digits"),
        pytest.param({"N": ""}, NUMERIC, id="empty"),
    ])
    def test_value_nothing_reads(self, dynamodb_client, table, value, expected):
        """The filter reads :p; :n is checked all the same, ahead of the unused value check."""
        assert _scan_error(
            dynamodb_client, TableName=table,
            FilterExpression="a = :p", ExpressionAttributeValues={":p": SX, ":n": value},
        ) == ("ValidationException", expected)

    def test_no_expression_at_all(self, dynamodb_client, table):
        assert _scan_error(
            dynamodb_client, TableName=table, ExpressionAttributeValues={":n": NB},
        ) == ("ValidationException", NUMERIC + "bob")

    def test_with_scan_filter_only(self, dynamodb_client, table):
        assert _scan_error(
            dynamodb_client, TableName=table, ScanFilter=SCAN_FILTER,
            ExpressionAttributeValues={":n": NB},
        ) == ("ValidationException", NUMERIC + "bob")

    def test_with_projection_expression_only(self, dynamodb_client, table):
        assert _scan_error(
            dynamodb_client, TableName=table, ProjectionExpression="a",
            ExpressionAttributeValues={":n": NB},
        ) == ("ValidationException", NUMERIC + "bob")

    def test_with_conditional_operator_and_no_filter(self, dynamodb_client, table):
        """ConditionalOperator alone does not join the parameter mix; the number is reported."""
        assert _scan_error(
            dynamodb_client, TableName=table, ConditionalOperator="AND",
            ExpressionAttributeValues={":n": NB},
        ) == ("ValidationException", NUMERIC + "bob")

    def test_two_bad_numbers_report_one_of_them(self, dynamodb_client, table):
        """The service's choice between two bad values varied between two runs; either is accepted."""
        code, msg = _scan_error(
            dynamodb_client, TableName=table, FilterExpression="a = :a",
            ExpressionAttributeValues={":a": {"N": "one"}, ":b": {"N": "two"}},
        )
        assert code == "ValidationException"
        assert msg in (NUMERIC + "one", NUMERIC + "two")

    def test_well_formed_numbers_pass(self, dynamodb_client, table):
        for value in ("1", "1.0e1"):
            response = dynamodb_client.scan(
                TableName=table, FilterExpression="a = :n",
                ExpressionAttributeValues={":n": {"N": value}},
            )
            assert "Count" in response, value


class TestChecksBeforeTheNumbers:
    """Range constraints, the parameter mix and ReturnConsumedCapacity win over a bad number."""

    @pytest.mark.parametrize("limit", [0, -1])
    def test_limit_range(self, dynamodb_client_no_validation, table, limit):
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table, Limit=limit, **NB_FILTER,
        ) == ("ValidationException", "1 validation error detected: " + LIMIT_RANGE)

    @pytest.mark.parametrize("kwargs, text", [
        pytest.param({"Segment": 0, "TotalSegments": 0},
                     "Value at 'TotalSegments' failed to satisfy constraint: "
                     "Member must have value greater than or equal to 1", id="TotalSegments-0"),
        pytest.param({"Segment": 0, "TotalSegments": 1_000_001},
                     "Value at 'TotalSegments' failed to satisfy constraint: "
                     "Member must have value less than or equal to 1000000", id="TotalSegments-1000001"),
        pytest.param({"Segment": -1, "TotalSegments": 2},
                     "Value at 'Segment' failed to satisfy constraint: "
                     "Member must have value greater than or equal to 0", id="Segment-minus-1"),
        pytest.param({"Segment": 1_000_000, "TotalSegments": 1_000_000},
                     "Value at 'Segment' failed to satisfy constraint: "
                     "Member must have value less than or equal to 999999", id="Segment-1000000"),
        pytest.param({"Segment": -1},
                     "Value at 'Segment' failed to satisfy constraint: "
                     "Member must have value greater than or equal to 0", id="Segment-minus-1-alone"),
        pytest.param({"TotalSegments": 0},
                     "Value at 'TotalSegments' failed to satisfy constraint: "
                     "Member must have value greater than or equal to 1", id="TotalSegments-0-alone"),
    ])
    def test_segment_ranges(self, dynamodb_client_no_validation, table, kwargs, text):
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table, **kwargs, **NB_FILTER,
        ) == ("ValidationException", "1 validation error detected: " + text)

    def test_ranges_aggregate_limit_then_segment_then_total_segments(
        self, dynamodb_client_no_validation, table,
    ):
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table,
            Limit=0, Segment=-1, TotalSegments=1_000_001,
        ) == ("ValidationException", (
            "3 validation errors detected: " + LIMIT_RANGE + "; "
            "Value at 'Segment' failed to satisfy constraint: "
            "Member must have value greater than or equal to 0; "
            "Value at 'TotalSegments' failed to satisfy constraint: "
            "Member must have value less than or equal to 1000000"
        ))

    def test_limit_range_beats_the_missing_table(self, dynamodb_client_no_validation):
        assert _scan_error(
            dynamodb_client_no_validation, TableName="extenddb-test-no-such-table", Limit=0,
        ) == ("ValidationException", "1 validation error detected: " + LIMIT_RANGE)

    @pytest.mark.parametrize("extra, non_expression, expression", [
        pytest.param({"ScanFilter": SCAN_FILTER}, "ScanFilter", "FilterExpression", id="ScanFilter"),
        pytest.param({"AttributesToGet": ["a"], "ProjectionExpression": "a"}, "AttributesToGet",
                     "ProjectionExpression, FilterExpression", id="AttributesToGet"),
        pytest.param({"ConditionalOperator": "AND"}, "ConditionalOperator", "FilterExpression",
                     id="ConditionalOperator"),
    ])
    def test_parameter_mix(self, dynamodb_client, table, extra, non_expression, expression):
        assert _scan_error(dynamodb_client, TableName=table, **extra, **NB_FILTER) == (
            "ValidationException",
            "Can not use both expression and non-expression parameters in the same request: "
            f"Non-expression parameters: {{{non_expression}}} Expression parameters: {{{expression}}}",
        )

    def test_parameter_mix_beats_return_consumed_capacity(self, dynamodb_client_no_validation, table):
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table, ScanFilter=SCAN_FILTER,
            ReturnConsumedCapacity="NOPE", **OK_FILTER,
        ) == ("ValidationException", (
            "Can not use both expression and non-expression parameters in the same request: "
            "Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}"
        ))

    def test_limit_range_beats_parameter_mix(self, dynamodb_client_no_validation, table):
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table, ScanFilter=SCAN_FILTER, Limit=0, **OK_FILTER,
        ) == ("ValidationException", "1 validation error detected: " + LIMIT_RANGE)

    def test_return_consumed_capacity_enum(self, dynamodb_client_no_validation, table):
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table, ReturnConsumedCapacity="NOPE", **NB_FILTER,
        ) == ("ValidationException", RCC_ENUM)

    def test_both_enums_invalid_report_return_consumed_capacity_alone(
        self, dynamodb_client_no_validation, table,
    ):
        for extra in ({}, NB_FILTER):
            assert _scan_error(
                dynamodb_client_no_validation, TableName=table,
                Select="NOPE", ReturnConsumedCapacity="NOPE", **extra,
            ) == ("ValidationException", RCC_ENUM)

    def test_limit_range_beats_both_enums(self, dynamodb_client_no_validation, table):
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table,
            Limit=0, Select="NOPE", ReturnConsumedCapacity="NOPE",
        ) == ("ValidationException", "1 validation error detected: " + LIMIT_RANGE)


class TestChecksAfterTheNumbers:
    """A bad number wins over every check the handler makes later."""

    def test_select_enum(self, dynamodb_client_no_validation, table):
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table, Select="NOPE", **NB_FILTER,
        ) == ("ValidationException", NUMERIC + "bob")
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table, Select="NOPE", **OK_FILTER,
        ) == ("ValidationException", SELECT_ENUM)

    @pytest.mark.parametrize("kwargs", [
        pytest.param({"Segment": 0}, id="Segment-without-TotalSegments"),
        pytest.param({"TotalSegments": 2}, id="TotalSegments-without-Segment"),
        pytest.param({"Segment": 5, "TotalSegments": 2}, id="Segment-not-below-TotalSegments"),
        pytest.param({"IndexName": "nope"}, id="unknown-IndexName"),
        pytest.param({"IndexName": "gsi1", "ConsistentRead": True}, id="ConsistentRead-on-GSI"),
        pytest.param({"Select": "COUNT", "ProjectionExpression": "a"}, id="COUNT-with-ProjectionExpression"),
        pytest.param({"ExclusiveStartKey": {"wrong": SX}}, id="ExclusiveStartKey-off-schema"),
    ])
    def test_handler_checks(self, dynamodb_client, table, kwargs):
        assert _scan_error(
            dynamodb_client, TableName=table, **kwargs, **NB_FILTER,
        ) == ("ValidationException", NUMERIC + "bob")

    def test_missing_table(self, dynamodb_client):
        assert _scan_error(
            dynamodb_client, TableName="extenddb-test-no-such-table", **NB_FILTER,
        ) == ("ValidationException", NUMERIC + "bob")

    def test_undefined_placeholder(self, dynamodb_client, table):
        assert _scan_error(
            dynamodb_client, TableName=table, FilterExpression="a = :zz",
            ExpressionAttributeValues={":n": NB},
        ) == ("ValidationException", NUMERIC + "bob")

    def test_filter_syntax_error(self, dynamodb_client, table):
        assert _scan_error(
            dynamodb_client, TableName=table, FilterExpression="a = = :n",
            ExpressionAttributeValues={":n": NB},
        ) == ("ValidationException", NUMERIC + "bob")
