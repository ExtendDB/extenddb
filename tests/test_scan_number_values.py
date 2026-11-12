# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Scan checks the numbers in ``ExpressionAttributeValues`` at request time.

An ``N`` value that is not a number, an ``N`` nested in an ``L`` or ``M``, an
``NS`` member, an overflow, an underflow, a value with more than 38
significant digits or an empty ``N`` is refused with ``ValidationException``
before any row is read, whether a ``FilterExpression`` reads the value,
nothing reads it, or the request carries no expression at all. Every expected
answer below was measured against the service on 2026-10-25 (09:23 to 09:28,
10:38, 10:55 and 11:31 UTC) with each request sent twice. A Unicode decimal
digit in an ``N`` value counts as the digit it stands for; a field of the
wrong JSON type anywhere in the body answers ``SerializationException`` ahead
of every check.

The rows also pin where the check falls against the other early Scan checks:
after the ``Limit``, ``Segment`` and ``TotalSegments`` range constraints
(``Value at 'Limit'`` with no value echoed for a ``Limit`` inside the int32
range; the ``Segment`` and ``TotalSegments`` rows assert the position of the
check, not its text), the expression and non-expression parameter mix and the
``ReturnConsumedCapacity`` enum; and
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

    @pytest.mark.parametrize("kwargs, field", [
        pytest.param({"Segment": 0, "TotalSegments": 0}, "totalsegments", id="TotalSegments-0"),
        pytest.param({"Segment": 0, "TotalSegments": 1_000_001}, "totalsegments", id="TotalSegments-1000001"),
        pytest.param({"Segment": -1, "TotalSegments": 2}, "segment", id="Segment-minus-1"),
        pytest.param({"Segment": 1_000_000, "TotalSegments": 1_000_000}, "segment", id="Segment-1000000"),
        pytest.param({"Segment": -1}, "segment", id="Segment-minus-1-alone"),
        pytest.param({"TotalSegments": 0}, "totalsegments", id="TotalSegments-0-alone"),
    ])
    def test_segment_ranges(self, dynamodb_client_no_validation, table, kwargs, field):
        # The range fault names the field and is reported ahead of the number;
        # the text itself differs between the service and extenddb.
        code, msg = _scan_error(dynamodb_client_no_validation, TableName=table, **kwargs, **NB_FILTER)
        assert code == "ValidationException"
        assert field in msg.lower().replace("'", "")
        assert "bob" not in msg

    def test_limit_range_is_reported_ahead_of_the_segment_ranges_and_the_number(
        self, dynamodb_client_no_validation, table,
    ):
        code, msg = _scan_error(
            dynamodb_client_no_validation, TableName=table,
            Limit=0, Segment=-1, TotalSegments=1_000_001, **NB_FILTER,
        )
        assert code == "ValidationException"
        assert msg.endswith(LIMIT_RANGE) or msg.startswith("3 validation errors detected: " + LIMIT_RANGE)
        assert "bob" not in msg

    def test_limit_below_int32_answers_the_saturated_value(self, dynamodb_client_no_validation, table):
        # Measured 2026-10-25 10:38 and 10:55 UTC: outside int32 the service
        # echoes the saturated value in the older form, and beats the number.
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table, Limit=-2_147_483_649, **NB_FILTER,
        ) == ("ValidationException", (
            "1 validation error detected: Value '-2147483648' at 'limit' failed to "
            "satisfy constraint: Member must have value greater than or equal to 1"
        ))
        assert _scan_error(
            dynamodb_client_no_validation, TableName=table, Limit=-2_147_483_648, **NB_FILTER,
        ) == ("ValidationException", "1 validation error detected: " + LIMIT_RANGE)

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


FW_ONE = "\uff11"        # fullwidth digit one
FW_SEVEN = "\uff17"      # fullwidth digit seven
AR_ONE = "\u0661"        # Arabic-Indic digit one
DEVA_ONE = "\u0967"      # Devanagari digit one
# Superscript two, vulgar half, fullwidth minus then 1, 1 fullwidth full stop 5.
NOT_DIGITS = ["\u00b2", "\u00bd", "\uff0d1", "1\uff0e5"]


class TestUnicodeDecimalDigits:
    """A Unicode decimal digit (general category Nd) in an N value passes the
    number check as the digit it stands for; other characters are refused with
    the value echoed as sent (measured 2026-10-25 10:38 and 10:55 UTC). The
    service also reads such a value as the digit when it compares; this change
    does not, so only the acceptance is asserted."""

    @pytest.mark.parametrize("raw", [FW_ONE, AR_ONE], ids=["fullwidth-1", "arabic-indic-1"])
    def test_digit_in_the_filter_is_accepted(self, dynamodb_client, table, raw):
        # The request is accepted and every row is scanned.
        resp = dynamodb_client.scan(
            TableName=table, FilterExpression="a = :n",
            ExpressionAttributeValues={":n": {"N": raw}})
        assert resp["ScannedCount"] == 2

    @pytest.mark.parametrize("raw", [FW_SEVEN, FW_ONE + "e2"], ids=["fullwidth-7", "fullwidth-1e2"])
    def test_digit_that_matches_no_row(self, dynamodb_client, table, raw):
        # Accepted; no stored item has a equal to 7 or 100.
        resp = dynamodb_client.scan(
            TableName=table, FilterExpression="a = :n",
            ExpressionAttributeValues={":n": {"N": raw}})
        assert (resp["Count"], resp["ScannedCount"]) == (0, 2)

    def test_digit_in_a_number_set_passes_the_check(self, dynamodb_client, table):
        assert _scan_error(
            dynamodb_client, TableName=table, FilterExpression="a = :p",
            ExpressionAttributeValues={":p": SX, ":s": {"NS": ["1", "\uff12"]}},
        ) == ("ValidationException",
              "Value provided in ExpressionAttributeValues unused in expressions: keys: {:s}")

    def test_39_devanagari_digits_are_39_digits(self, dynamodb_client, table):
        assert _scan_error(
            dynamodb_client, TableName=table, FilterExpression="a = :n",
            ExpressionAttributeValues={":n": {"N": DEVA_ONE * 39}},
        ) == ("ValidationException", TOO_MANY_DIGITS)

    @pytest.mark.parametrize("raw", NOT_DIGITS, ids=["superscript", "fraction", "fullwidth-minus", "fullwidth-point"])
    def test_characters_that_are_not_digits_are_refused(self, dynamodb_client, table, raw):
        assert _scan_error(
            dynamodb_client, TableName=table, FilterExpression="a = :n",
            ExpressionAttributeValues={":n": {"N": raw}},
        ) == ("ValidationException", NUMERIC + raw)


def _raw_scan(client, body: dict):
    """POST ``body`` as the Scan JSON exactly as written, signed with the
    client's credentials, so a field of the wrong JSON type reaches the server
    (boto3 would refuse or reshape it)."""
    import json

    from botocore.awsrequest import AWSRequest

    req = AWSRequest(method="POST", url=client.meta.endpoint_url, data=json.dumps(body),
                     headers={"Content-Type": "application/x-amz-json-1.0",
                              "X-Amz-Target": "DynamoDB_20120810.Scan"})
    client._request_signer.sign("Scan", req)
    resp = client._endpoint.http_session.send(req.prepare())
    return resp.status_code, json.loads(resp.content.decode("utf-8", "replace") or "{}")


SHAPE_FAULTS = [
    ("Limit 0 with ExpressionAttributeValues string", {"Limit": 0, "ExpressionAttributeValues": "x"}),
    ("Segment -1 with TotalSegments string", {"Segment": -1, "TotalSegments": "2"}),
    ("ExpressionAttributeNames string with N bob", {"ExpressionAttributeNames": "x", **NB_FILTER}),
    ("AttributesToGet object with ProjectionExpression",
     {"AttributesToGet": {"a": 1}, "ProjectionExpression": "a"}),
]


class TestShapeFaultsAreReportedFirst:
    """A field of the wrong JSON type anywhere in the body answers
    SerializationException ahead of the range, parameter mix and number checks
    (measured 2026-10-25 10:38 UTC). The message texts differ between the
    service and extenddb, so only the error type is asserted."""

    @pytest.mark.parametrize("row,body", SHAPE_FAULTS, ids=[r[0] for r in SHAPE_FAULTS])
    def test_rows(self, dynamodb_client, table, row, body):
        status, payload = _raw_scan(dynamodb_client, {"TableName": table, **body})
        assert status == 400
        assert payload.get("__type", "").endswith("SerializationException"), payload

    def test_control_without_a_shape_fault_answers_the_check(self, dynamodb_client, table):
        status, payload = _raw_scan(dynamodb_client, {"TableName": table, "Limit": 0, **NB_FILTER})
        assert status == 400
        assert payload.get("__type", "").endswith("ValidationException")
        assert payload.get("message", payload.get("Message")) == "1 validation error detected: " + LIMIT_RANGE
