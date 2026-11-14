# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query request bodies the service does not read as written: a field of the
wrong JSON type, and an ExpressionAttributeValues value the service cannot
read as one type. boto3 refuses or reshapes these bodies, so they go out as
written. Every expected answer below was measured against the service on
2026-10-25 (11:31 UTC) and 2026-11-11.

Dual-target: runs against the service and against extenddb with the same
assertions.

Run:
  EXTENDDB_TEST_ENDPOINT=https://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_query_value_shapes.py -v
"""

from __future__ import annotations

import pytest

from test_query_key_value_types import (  # noqa: F401  (ss_table is a fixture)
    LIMIT_MSG,
    NB,
    NUMERIC,
    RCC_MSG,
    SX,
    ss_table,
)


def _raw_query(client, body: dict):
    """POST ``body`` as the Query JSON exactly as written, signed with the
    client's credentials, so a field of the wrong JSON type reaches the server
    (boto3 would refuse or reshape it)."""
    import json

    from botocore.awsrequest import AWSRequest

    req = AWSRequest(method="POST", url=client.meta.endpoint_url, data=json.dumps(body),
                     headers={"Content-Type": "application/x-amz-json-1.0",
                              "X-Amz-Target": "DynamoDB_20120810.Query"})
    client._request_signer.sign("Query", req)
    resp = client._endpoint.http_session.send(req.prepare())
    return resp.status_code, json.loads(resp.content.decode("utf-8", "replace") or "{}")


SHAPE_FAULTS = [
    ("Limit 0 with ExpressionAttributeValues string",
     {"KeyConditionExpression": "pk = :p", "Limit": 0, "ExpressionAttributeValues": "x"}),
    ("ExpressionAttributeNames string with N bob",
     {"KeyConditionExpression": "pk = :p", "ExpressionAttributeNames": "x",
      "ExpressionAttributeValues": {":p": NB}}),
    ("Limit string with N bob",
     {"KeyConditionExpression": "pk = :p", "Limit": "1", "ExpressionAttributeValues": {":p": NB}}),
    ("AttributesToGet object with ProjectionExpression",
     {"KeyConditionExpression": "pk = :p", "AttributesToGet": {"a": 1}, "ProjectionExpression": "a",
      "ExpressionAttributeValues": {":p": SX}}),
    ("QueryFilter array with FilterExpression",
     {"KeyConditionExpression": "pk = :p", "QueryFilter": [1], "FilterExpression": "a = :p",
      "ExpressionAttributeValues": {":p": SX}}),
    ("Select number with N bob",
     {"KeyConditionExpression": "pk = :p", "Select": 5, "ExpressionAttributeValues": {":p": NB}}),
    ("Limit 0 with Select number",
     {"KeyConditionExpression": "pk = :p", "Limit": 0, "Select": 5,
      "ExpressionAttributeValues": {":p": SX}}),
    # A field of the wrong JSON type inside a value the service cannot read as
    # one type (measured 2026-11-11).
    ("string beside a null in an L",
     {"KeyConditionExpression": "pk = :p",
      "ExpressionAttributeValues": {":p": SX, ":z": {"L": [None, "x"]}}}),
    ("N number beside an unknown descriptor in an L",
     {"KeyConditionExpression": "pk = :p",
      "ExpressionAttributeValues": {":p": SX, ":z": {"L": [{"X": "1"}, {"N": 5}]}}}),
    ("S number beside an unknown descriptor with Limit 0",
     {"KeyConditionExpression": "pk = :p", "Limit": 0,
      "ExpressionAttributeValues": {":p": SX, ":z": {"S": 5, "X": "1"}}}),
    ("B not base64 beside a null in an L",
     {"KeyConditionExpression": "pk = :p",
      "ExpressionAttributeValues": {":p": SX, ":z": {"L": [None, {"B": "!!"}]}}}),
]


class TestShapeFaultsAreReportedFirst:
    """A field of the wrong JSON type anywhere in the body answers
    SerializationException ahead of the Limit, parameter mix, enum and number
    checks (measured 2026-10-25 11:31 UTC). The message texts differ between
    the service and extenddb, so only the error type is asserted."""

    @pytest.mark.parametrize("row,body", SHAPE_FAULTS, ids=[r[0] for r in SHAPE_FAULTS])
    def test_rows(self, dynamodb_client, ss_table, row, body):
        status, payload = _raw_query(dynamodb_client, {"TableName": ss_table, **body})
        assert status == 400
        assert payload.get("__type", "").endswith("SerializationException"), payload

    def test_control_without_a_shape_fault_answers_the_check(self, dynamodb_client, ss_table):
        status, payload = _raw_query(dynamodb_client, {
            "TableName": ss_table, "KeyConditionExpression": "pk = :p", "Limit": 0,
            "ExpressionAttributeValues": {":p": NB}})
        assert status == 400
        assert payload.get("__type", "").endswith("ValidationException")
        assert payload.get("message", payload.get("Message")) == LIMIT_MSG


INVALID_VALUE = "ExpressionAttributeValues contains invalid value: "
EMPTY_AV = "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"
NULL_AV = "The supplied Item contains a null AttributeValue"
SEVERAL_AV = ("Supplied AttributeValue has more than one datatypes set, must contain exactly "
              "one of the supported datatypes")
X1 = {"X": "1"}
# An S with an unknown descriptor beside it, which the service reads as an S.
S_PLUS_X = {"S": "a", "X": "1"}
LIMIT_0_AT = ("Value '0' at 'limit' failed to satisfy constraint: "
              "Member must have value greater than or equal to 1")

# (row, values beside :p, other fields, the service's message).
UNREADABLE = [
    ("unknown descriptor", {":z": X1}, {}, INVALID_VALUE + EMPTY_AV + " for key :z"),
    ("null", {":z": None}, {}, INVALID_VALUE + NULL_AV + " for key :z"),
    ("no descriptor", {":z": {}}, {}, INVALID_VALUE + EMPTY_AV + " for key :z"),
    ("two descriptors", {":z": {"S": "a", "N": "1"}}, {}, INVALID_VALUE + SEVERAL_AV + " for key :z"),
    ("unknown descriptor in an L", {":z": {"L": [X1]}}, {}, INVALID_VALUE + EMPTY_AV + " for key :z"),
    ("null M member", {":z": {"M": {"k": None}}}, {},
     INVALID_VALUE + NULL_AV + ": k is null for key :z"),
    ("N bob beside S plus X", {":a": S_PLUS_X, ":u": NB}, {}, INVALID_VALUE + NUMERIC + "bob for key :u"),
    ("empty N beside S plus X", {":a": S_PLUS_X, ":u": {"N": ""}}, {},
     INVALID_VALUE + "The parameter cannot be converted to a numeric value for key :u"),
    ("fullwidth one bob beside S plus X", {":a": S_PLUS_X, ":u": {"N": "\uff11bob"}}, {},
     INVALID_VALUE + NUMERIC + "\uff11bob for key :u"),
    ("Limit 0", {":z": X1}, {"Limit": 0}, "1 validation error detected: " + LIMIT_0_AT),
    ("Limit 0 beside null", {":z": None}, {"Limit": 0}, "1 validation error detected: " + LIMIT_0_AT),
    ("Limit 0 and ReturnConsumedCapacity", {":z": X1}, {"Limit": 0, "ReturnConsumedCapacity": "bad"},
     "2 validation errors detected: " + RCC_MSG.removeprefix("1 validation error detected: ")
     + "; " + LIMIT_0_AT),
    ("invalid Select", {":z": X1, ":u": NB}, {"Select": "bad"},
     "1 validation error detected: Value 'bad' at 'select' failed to satisfy constraint: Member "
     "must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, "
     "ALL_PROJECTED_ATTRIBUTES]"),
    ("AttributesToGet", {":z": X1, ":u": NB}, {"AttributesToGet": ["a"]},
     "Can not use both expression and non-expression parameters in the same request: "
     "Non-expression parameters: {AttributesToGet} Expression parameters: {KeyConditionExpression}"),
]


class TestValuesTheServiceCannotReadAsOneType:
    """A value in ExpressionAttributeValues that the service cannot read as one
    type (a null, no type descriptor it knows, more than one, or an unknown one
    beside a known one) is named with its placeholder, and beside such a value
    Select, ReturnConsumedCapacity and Limit are reported together, ahead of
    the parameter mix (measured 2026-11-11). boto3 cannot send an unknown
    descriptor or a null, so the bodies go out as written."""

    @pytest.mark.parametrize("row,values,extra,expected", UNREADABLE, ids=[r[0] for r in UNREADABLE])
    def test_rows(self, dynamodb_client, ss_table, row, values, extra, expected):
        status, payload = _raw_query(dynamodb_client, {
            "TableName": ss_table, "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": SX, **values}, **extra})
        assert status == 400, payload
        assert payload.get("__type", "").endswith("ValidationException"), payload
        assert payload.get("message", payload.get("Message")) == expected

    def test_the_value_the_service_cannot_read_is_named_beside_n_bob(self, dynamodb_client,
                                                                     ss_table):
        """A value the service cannot read as one type is named ahead of a
        number fault in another value: :z for this body on 28 of 28 requests
        (measured 2026-11-11)."""
        status, payload = _raw_query(dynamodb_client, {
            "TableName": ss_table, "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": SX, ":u": NB, ":z": X1}})
        assert status == 400, payload
        assert payload.get("__type", "").endswith("ValidationException"), payload
        assert payload.get("message", payload.get("Message")) == (
            INVALID_VALUE + EMPTY_AV + " for key :z")
