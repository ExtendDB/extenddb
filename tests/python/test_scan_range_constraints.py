# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Scan range constraints on ``Limit``, ``Segment`` and ``TotalSegments``.

Every request below is sent as raw signed JSON, so a number keeps the form
written here (``0.0``, ``1e2``, ``9223372036854775808``). Each request is a
row of the table measured against DynamoDB (us-east-1) on 2026-10-25 at 09:23,
09:25, 09:28, 10:38, 10:55 and 12:56 UTC, or of the ones measured on
2026-10-30 at 12:40 UTC, 2026-10-31 at 12:36 UTC and 2026-10-31 between 23:54
and 23:56 UTC where a row says so, each sent twice, and each expected answer
is the service's. Where a table records
only that the service accepted a request, the test asserts what a Scan with
that limit returns on this table of 12 items. ``Segment: 0, TotalSegments: 1``
with ``ConsistentRead: true`` returned the whole table in one page on
2026-10-31, and the test asserts the same here.

When each field present is an integer inside the int32 range, a fault reads
``Value at '<Field>' failed to satisfy constraint: ...`` with the field
capitalised and no value echoed, and several faults are reported in one
message in the order Limit, Segment, TotalSegments. When a field is a float
or outside int32, or a value is not one AttributeValue (an
``ExpressionAttributeValues`` entry or an ``L`` or ``M`` member of one, an
``ExclusiveStartKey`` value, or an ``AttributeValueList`` entry of
``ScanFilter``, or on Query of ``KeyConditions`` or ``QueryFilter``), the
service saturates or truncates the value and uses the older form,
``Value '<n>' at '<field>' ...``, in the order totalSegments, segment, limit. A float or an out-of-range integer
that satisfies the constraint is accepted.
"""

from __future__ import annotations

import json

import pytest
from botocore.awsrequest import AWSRequest

from helpers import unique_name, wait_for_active, wait_for_deleted

ITEMS = 12

LIMIT = (
    "Value at 'Limit' failed to satisfy constraint: "
    "Member must have value greater than or equal to 1"
)
SEGMENT_LOW = (
    "Value at 'Segment' failed to satisfy constraint: "
    "Member must have value greater than or equal to 0"
)
SEGMENT_HIGH = (
    "Value at 'Segment' failed to satisfy constraint: "
    "Member must have value less than or equal to 999999"
)
TOTAL_LOW = (
    "Value at 'TotalSegments' failed to satisfy constraint: "
    "Member must have value greater than or equal to 1"
)
TOTAL_HIGH = (
    "Value at 'TotalSegments' failed to satisfy constraint: "
    "Member must have value less than or equal to 1000000"
)


def one(clause: str) -> str:
    return "1 validation error detected: " + clause


def older(value: str, field: str, constraint: str) -> str:
    return (
        f"Value '{value}' at '{field}' failed to satisfy constraint: "
        f"Member must have value {constraint}"
    )


OLDER_LIMIT_0 = older("0", "limit", "greater than or equal to 1")
OLDER_LIMIT_MIN = older("-2147483648", "limit", "greater than or equal to 1")

# (id, the fields after TableName as JSON text, the expected message)
NEWER_FORM = [
    ("Limit-0", '"Limit": 0', one(LIMIT)),
    ("Limit-int32-min", '"Limit": -2147483648', one(LIMIT)),
    ("Segment-minus-1", '"Segment": -1', one(SEGMENT_LOW)),
    ("Segment-minus-1-of-2", '"Segment": -1, "TotalSegments": 2', one(SEGMENT_LOW)),
    ("Segment-1000000-of-1000000", '"Segment": 1000000, "TotalSegments": 1000000',
     one(SEGMENT_HIGH)),
    ("TotalSegments-0", '"TotalSegments": 0', one(TOTAL_LOW)),
    ("TotalSegments-0-Segment-0", '"Segment": 0, "TotalSegments": 0', one(TOTAL_LOW)),
    ("TotalSegments-minus-1-Segment-0", '"Segment": 0, "TotalSegments": -1', one(TOTAL_LOW)),
    ("TotalSegments-1000001", '"TotalSegments": 1000001', one(TOTAL_HIGH)),
    ("Limit-Segment-TotalSegments", '"Limit": 0, "Segment": -1, "TotalSegments": 1000001',
     f"3 validation errors detected: {LIMIT}; {SEGMENT_LOW}; {TOTAL_HIGH}"),
    ("Limit-TotalSegments", '"Limit": 0, "TotalSegments": 0',
     f"2 validation errors detected: {LIMIT}; {TOTAL_LOW}"),
    ("Segment-low-TotalSegments-high", '"Segment": -1, "TotalSegments": 1000001',
     f"2 validation errors detected: {SEGMENT_LOW}; {TOTAL_HIGH}"),
    ("Segment-high-TotalSegments-low", '"Segment": 1000000, "TotalSegments": 0',
     f"2 validation errors detected: {SEGMENT_HIGH}; {TOTAL_LOW}"),
]

OLDER_FORM = [
    ("Limit-below-int32", '"Limit": -2147483649', one(OLDER_LIMIT_MIN)),
    ("TotalSegments-above-int32", '"Segment": 0, "TotalSegments": 2147483648',
     one(older("2147483647", "totalSegments", "less than or equal to 1000000"))),
    ("Limit-0.0", '"Limit": 0.0', one(OLDER_LIMIT_0)),
    ("Limit-below-int32-Segment-minus-1", '"Limit": -2147483649, "Segment": -1',
     "2 validation errors detected: "
     f"{older('-1', 'segment', 'greater than or equal to 0')}; {OLDER_LIMIT_MIN}"),
    ("TotalSegments-1000001-Limit-0.0", '"TotalSegments": 1000001, "Limit": 0.0',
     "2 validation errors detected: "
     f"{older('1000001', 'totalSegments', 'less than or equal to 1000000')}; {OLDER_LIMIT_0}"),
    # Measured 2026-10-30 12:40 UTC.
    ("three-faults-Limit-0.0", '"Limit": 0.0, "Segment": -1, "TotalSegments": 1000001',
     "3 validation errors detected: "
     f"{older('1000001', 'totalSegments', 'less than or equal to 1000000')}; "
     f"{older('-1', 'segment', 'greater than or equal to 0')}; {OLDER_LIMIT_0}"),
    ("three-faults-TotalSegments-1000001.0",
     '"Limit": 0, "Segment": -1, "TotalSegments": 1000001.0',
     "3 validation errors detected: "
     f"{older('1000001', 'totalSegments', 'less than or equal to 1000000')}; "
     f"{older('-1', 'segment', 'greater than or equal to 0')}; {OLDER_LIMIT_0}"),
    ("Limit-minus-1.5", '"Limit": -1.5',
     one(older("-1", "limit", "greater than or equal to 1"))),
    ("Limit-minus-0.5", '"Limit": -0.5', one(OLDER_LIMIT_0)),
    ("Limit-minus-0", '"Limit": -0', one(OLDER_LIMIT_0)),
]

# ExpressionAttributeValues entries the service does not read as one
# AttributeValue (older form beside Limit 0), and two it does (newer form).
UNREADABLE = [("empty", {}), ("unknown-type", {"X": "1"}), ("two-types", {"S": "x", "N": "1"})]
READABLE = [("empty-NS", {"NS": []}), ("duplicate-NS", {"NS": ["1", "1"]})]
# Measured 2026-10-30 12:40 UTC on Scan, each under :d beside a filter that
# reads :p: older form.
UNREADABLE_MEMBERS = [("L-empty-member", {"L": [{}]}),
                      ("M-member-type-X", {"M": {"k": {"X": "1"}}}),
                      ("lower-case-s", {"s": "x"})]


def _conditions(name: str, entry: dict) -> dict:
    return {name: {"ComparisonOperator": "EQ", "AttributeValueList": [entry]}}


def _scan_filter(entry: dict) -> str:
    return '"ScanFilter": ' + json.dumps(_conditions("a", entry))


# Measured 2026-10-31 between 23:54 and 23:56 UTC: a value that is not one
# AttributeValue in ExclusiveStartKey or in a condition list answers the older
# form; a readable one, also of the wrong type for the key, the newer form.
KEY_AND_LISTS = [
    ("ExclusiveStartKey-empty", '"Limit": 0, "ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}',
     one(OLDER_LIMIT_0)),
    ("ExclusiveStartKey-type-X",
     '"Limit": 0, "ExclusiveStartKey": {"pk": {"X": "1"}, "sk": {"S": "x"}}', one(OLDER_LIMIT_0)),
    ("ScanFilter-empty", '"Limit": 0, ' + _scan_filter({}), one(OLDER_LIMIT_0)),
    ("ScanFilter-type-X", '"Limit": 0, ' + _scan_filter({"X": "1"}), one(OLDER_LIMIT_0)),
    ("Segment-beside-ExclusiveStartKey-empty",
     '"Segment": -1, "TotalSegments": 2, "ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}',
     one(older("-1", "segment", "greater than or equal to 0"))),
    ("ExclusiveStartKey-N-on-an-S-key",
     '"Limit": 0, "ExclusiveStartKey": {"pk": {"N": "1"}, "sk": {"S": "x"}}', one(LIMIT)),
    ("ScanFilter-N", '"Limit": 0, ' + _scan_filter({"N": "1"}), one(LIMIT)),
]
QUERY_KEY_AND_LISTS = [
    ("ExclusiveStartKey-empty",
     {"KeyConditionExpression": "pk = :p", "Limit": 0,
      "ExpressionAttributeValues": {":p": {"S": "p0"}},
      "ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}}, one(OLDER_LIMIT_0)),
    ("QueryFilter-empty",
     {"KeyConditions": _conditions("pk", {"S": "p0"}), "Limit": 0,
      "QueryFilter": _conditions("a", {})}, one(OLDER_LIMIT_0)),
    ("KeyConditions-empty", {"KeyConditions": _conditions("pk", {}), "Limit": 0},
     one(OLDER_LIMIT_0)),
    ("KeyConditions-S", {"KeyConditions": _conditions("pk", {"S": "p0"}), "Limit": 0}, one(LIMIT)),
]


def _raw(client, operation: str, body_text: str):
    """POST ``body_text`` exactly as written, signed with the client's credentials."""
    req = AWSRequest(method="POST", url=client.meta.endpoint_url, data=body_text,
                     headers={"Content-Type": "application/x-amz-json-1.0",
                              "X-Amz-Target": f"DynamoDB_20120810.{operation}"})
    client._request_signer.sign(operation, req)
    resp = client._endpoint.http_session.send(req.prepare())
    return resp.status_code, json.loads(resp.content.decode("utf-8", "replace") or "{}")


def _scan(client, table: str, fields: str):
    return _raw(client, "Scan", '{"TableName": %s, %s}' % (json.dumps(table), fields))


def _validation_message(status: int, payload: dict) -> str:
    assert status == 400, payload
    assert payload.get("__type", "").endswith("ValidationException"), payload
    return payload.get("message", payload.get("Message"))


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """pk S, sk S, with ITEMS items."""
    name = unique_name("scanrange")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    for i in range(ITEMS):
        dynamodb_client.put_item(TableName=name, Item={
            "pk": {"S": f"p{i}"}, "sk": {"S": "s"}, "a": {"N": str(i)}})
    yield name
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


class TestIntegersInsideInt32:
    """The newer form, every fault in one message, Limit, Segment, TotalSegments."""

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in NEWER_FORM],
                             ids=[r[0] for r in NEWER_FORM])
    def test_rows(self, dynamodb_client, table, fields, expected):
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected


class TestFloatsAndValuesOutsideInt32:
    """The older form: the value saturated or truncated and echoed, limit last."""

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in OLDER_FORM],
                             ids=[r[0] for r in OLDER_FORM])
    def test_rows(self, dynamodb_client, table, fields, expected):
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected


class TestFloatsAndValuesOutsideInt32AreAccepted:
    """A value that satisfies its constraint once saturated or truncated is
    accepted and used as saturated or truncated."""

    @pytest.mark.parametrize("fields", [
        '"Limit": 2147483648', '"Limit": 9223372036854775808', '"Limit": 1e2',
    ], ids=["Limit-2147483648", "Limit-2-to-the-63", "Limit-1e2"])
    def test_large_limits_are_accepted(self, dynamodb_client, table, fields):
        status, payload = _scan(dynamodb_client, table, fields + ', "ConsistentRead": true')
        assert status == 200, payload
        assert (payload["Count"], payload["ScannedCount"]) == (ITEMS, ITEMS)

    @pytest.mark.parametrize("fields", ['"Limit": 1.5', '"Limit": 1.0'],
                             ids=["Limit-1.5", "Limit-1.0"])
    def test_a_fractional_limit_is_truncated(self, dynamodb_client, table, fields):
        status, payload = _scan(dynamodb_client, table, fields + ', "ConsistentRead": true')
        assert status == 200, payload
        assert (payload["Count"], payload["ScannedCount"]) == (1, 1)

    def test_a_fractional_segment_is_truncated(self, dynamodb_client, table):
        # The TotalSegments of 2 is this test's; the table records only Segment 1.5.
        status, half = _scan(dynamodb_client, table,
                             '"Segment": 1.5, "TotalSegments": 2, "ConsistentRead": true')
        assert status == 200, half
        status, whole = _scan(dynamodb_client, table,
                              '"Segment": 1, "TotalSegments": 2, "ConsistentRead": true')
        assert status == 200, whole

        def keys(page):
            return sorted(item["pk"]["S"] for item in page["Items"])

        assert keys(whole), "segment 1 of 2 holds at least one item"
        assert keys(half) == keys(whole)

    def test_segment_0_of_1_returns_every_item(self, dynamodb_client, table):
        status, payload = _scan(dynamodb_client, table,
                                '"Segment": 0, "TotalSegments": 1, "ConsistentRead": true')
        assert status == 200, payload
        assert (payload["Count"], payload["ScannedCount"]) == (ITEMS, ITEMS)


class TestBesideAnExpressionAttributeValuesEntry:
    """Limit 0 beside an entry that is not one AttributeValue answers the older
    form on Scan and on Query (measured 12:56 UTC); beside an empty or a
    duplicate NS, the newer form."""

    @pytest.mark.parametrize("entry", [e for _, e in UNREADABLE], ids=[i for i, _ in UNREADABLE])
    def test_scan_unreadable_entry(self, dynamodb_client, table, entry):
        fields = '"Limit": 0, "ExpressionAttributeValues": ' + json.dumps({":a": entry})
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == one(OLDER_LIMIT_0)

    @pytest.mark.parametrize("entry", [e for _, e in READABLE], ids=[i for i, _ in READABLE])
    def test_scan_readable_entry(self, dynamodb_client, table, entry):
        fields = '"Limit": 0, "ExpressionAttributeValues": ' + json.dumps({":a": entry})
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == one(LIMIT)

    @pytest.mark.parametrize("entry, expected",
                             [(e, one(OLDER_LIMIT_0)) for _, e in UNREADABLE]
                             + [(e, one(LIMIT)) for _, e in READABLE],
                             ids=[i for i, _ in UNREADABLE] + [i for i, _ in READABLE])
    def test_query(self, dynamodb_client, table, entry, expected):
        body = json.dumps({
            "TableName": table, "KeyConditionExpression": "pk = :p", "Limit": 0,
            "ExpressionAttributeValues": {":p": {"S": "p0"}, ":a": entry},
        })
        assert _validation_message(*_raw(dynamodb_client, "Query", body)) == expected

    def test_query_limit_below_int32_beside_an_empty_entry(self, dynamodb_client, table):
        # Measured 2026-10-31 12:36 UTC: the echoed value is saturated to
        # int32, as the Scan row Limit -2147483649 is.
        body = json.dumps({
            "TableName": table, "KeyConditionExpression": "pk = :p", "Limit": -2147483649,
            "ExpressionAttributeValues": {":p": {"S": "p0"}, ":a": {}},
        })
        assert _validation_message(*_raw(dynamodb_client, "Query", body)) == one(OLDER_LIMIT_MIN)

    @pytest.mark.parametrize("entry", [e for _, e in UNREADABLE_MEMBERS],
                             ids=[i for i, _ in UNREADABLE_MEMBERS])
    def test_scan_unreadable_member(self, dynamodb_client, table, entry):
        fields = ('"Limit": 0, "FilterExpression": "a = :p", "ExpressionAttributeValues": '
                  + json.dumps({":p": {"S": "x"}, ":d": entry}))
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == one(OLDER_LIMIT_0)

    @pytest.mark.parametrize("entry", [e for _, e in UNREADABLE_MEMBERS],
                             ids=[i for i, _ in UNREADABLE_MEMBERS])
    def test_query_unreadable_member(self, dynamodb_client, table, entry):
        # Measured on Query 2026-10-31 12:36 UTC, each entry under :d beside
        # the key condition's :p.
        body = json.dumps({
            "TableName": table, "KeyConditionExpression": "pk = :p", "Limit": 0,
            "ExpressionAttributeValues": {":p": {"S": "p0"}, ":d": entry},
        })
        assert _validation_message(*_raw(dynamodb_client, "Query", body)) == one(OLDER_LIMIT_0)


class TestBesideAnUnreadableValueInTheKeyOrAConditionList:
    """Measured 2026-10-31 between 23:54 and 23:56 UTC: a value that is not one
    AttributeValue in ExclusiveStartKey, or in the AttributeValueList of a
    ScanFilter, KeyConditions or QueryFilter condition, switches to the older
    form, as an ExpressionAttributeValues entry does."""

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in KEY_AND_LISTS],
                             ids=[r[0] for r in KEY_AND_LISTS])
    def test_scan(self, dynamodb_client, table, fields, expected):
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in QUERY_KEY_AND_LISTS],
                             ids=[r[0] for r in QUERY_KEY_AND_LISTS])
    def test_query(self, dynamodb_client, table, fields, expected):
        body = json.dumps({"TableName": table, **fields})
        assert _validation_message(*_raw(dynamodb_client, "Query", body)) == expected
