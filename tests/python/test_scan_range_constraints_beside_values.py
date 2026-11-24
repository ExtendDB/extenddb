# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Scan range constraints beside an attribute value in the request.

Each row here puts ``Limit``, ``Segment`` or ``TotalSegments`` beside an
``ExpressionAttributeValues`` entry, an ``ExclusiveStartKey`` value or an
``AttributeValueList`` entry of ``ScanFilter``, ``KeyConditions`` or
``QueryFilter``, and asserts which form of the message the service answers,
on Scan and on Query. ``test_scan_range_constraints.py`` describes the two
forms and holds the rows on the range fields alone; the messages, the request
helpers and the table fixture are imported from there.
"""

from __future__ import annotations

import json

import pytest

from test_scan_range_constraints import (  # noqa: F401  (table is a fixture)
    LIMIT,
    OLDER_LIMIT_0,
    OLDER_LIMIT_MIN,
    OLDER_SEGMENT_MINUS_1,
    RCC_BAD,
    SELECT_BAD,
    _conditions,
    _raw,
    _scan,
    _scan_filter,
    _validation_message,
    one,
    table,
)

# ExpressionAttributeValues entries the service does not read as one
# AttributeValue (older form beside Limit 0), and two it does (newer form).
UNREADABLE = [("empty", {}), ("unknown-type", {"X": "1"}), ("two-types", {"S": "x", "N": "1"})]
READABLE = [("empty-NS", {"NS": []}), ("duplicate-NS", {"NS": ["1", "1"]})]
# Measured 2026-10-30 12:40 UTC on Scan, each under :d beside a filter that
# reads :p: older form.
# Measured 2026-11-12: an L nested in an L does the same, on Scan and Query.
UNREADABLE_MEMBERS = [("L-empty-member", {"L": [{}]}),
                      ("M-member-type-X", {"M": {"k": {"X": "1"}}}),
                      ("lower-case-s", {"s": "x"}),
                      ("L-in-L-empty-member", {"L": [{"L": [{}]}]})]

# Measured 2026-11-24 at 05:41 UTC: on the older path an empty set and a set
# with a duplicate member are refused with that path's wrapped text, a number
# set's duplicate as "Input collection contains duplicates", the others with
# the deserializer's text, an invalid names key ahead of them.
INVALID_VALUE = "ExpressionAttributeValues contains invalid value: "
DUPLICATE_NS = INVALID_VALUE + "Input collection contains duplicates for key :a"
OLDER_FORM_SETS = [
    ("duplicate-NS", '{"NS": ["1", "1"]}', DUPLICATE_NS),
    ("empty-NS", '{"NS": []}',
     INVALID_VALUE + "One or more parameter values were invalid: An number set  may not be empty "
     "for key :a"),
    ("duplicate-SS", '{"SS": ["x", "x"]}',
     INVALID_VALUE + "One or more parameter values were invalid: Input collection [x, x] contains "
     "duplicates. for key :a"),
    ("duplicate-NS-in-L", '{"L": [{"NS": ["1", "1"]}]}', DUPLICATE_NS),
]

# Measured 2026-11-12 and 2026-11-23 at 14:51 UTC (M07, M08): a null
# is a value the service cannot read as one type, as an entry, an L member of
# one or a payload in an ExclusiveStartKey value; {"NULL": true}, {"L": []} and
# a null ExclusiveStartKey member keep the newer form.
NULL_ROWS = [
    ("null-entry", '"Limit": 0, "ExpressionAttributeValues": {":p": null}', one(OLDER_LIMIT_0)),
    ("L-null-member", '"Limit": 0, "ExpressionAttributeValues": {":p": {"L": [null]}}',
     one(OLDER_LIMIT_0)),
    ("ExclusiveStartKey-S-null",
     '"Limit": 0, "ExclusiveStartKey": {"pk": {"S": null}, "sk": {"S": "s0"}}',
     one(OLDER_LIMIT_0)),
    ("Segment-minus-1-null-entry",
     '"Segment": -1, "TotalSegments": 2, "ExpressionAttributeValues": {":p": null}',
     one(OLDER_SEGMENT_MINUS_1)),
    ("NULL-true", '"Limit": 0, "ExpressionAttributeValues": {":p": {"NULL": true}}', one(LIMIT)),
    ("L-empty", '"Limit": 0, "ExpressionAttributeValues": {":p": {"L": []}}', one(LIMIT)),
    ("ExclusiveStartKey-null", '"Limit": 0, "ExclusiveStartKey": null', one(LIMIT)),
]

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
     one(OLDER_SEGMENT_MINUS_1)),
    # Measured 2026-11-23 at 14:51 UTC: an M member of a key value and an L
    # member of a list entry are read as the entry is.
    ("ExclusiveStartKey-M-empty-member",
     '"Limit": 0, "ExclusiveStartKey": {"pk": {"M": {"k": {}}}, "sk": {"S": "s0"}}',
     one(OLDER_LIMIT_0)),
    ("ScanFilter-L-empty-member", '"Limit": 0, ' + _scan_filter({"L": [{}]}), one(OLDER_LIMIT_0)),
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
    # Measured 2026-11-12: a null list entry.
    ("KeyConditions-null", {"KeyConditions": _conditions("pk", None), "Limit": 0},
     one(OLDER_LIMIT_0)),
]

# Measured 2026-11-23 at 14:51 UTC (M09, M10, M13) and 18:45 UTC (F05 to F08):
# a Query Limit that is a float or outside int32 takes the older path without
# a value the service cannot read, with the enum clauses in the one message,
# ahead of the mix, and the numbers wrapped.
QUERY_OLDER_FORM = [
    ("Limit-below-int32",
     {"KeyConditionExpression": "pk = :p", "Limit": -2147483649,
      "ExpressionAttributeValues": {":p": {"S": "p0"}}}, one(OLDER_LIMIT_MIN)),
    ("Limit-0.0",
     {"KeyConditionExpression": "pk = :p", "Limit": 0.0,
      "ExpressionAttributeValues": {":p": {"S": "p0"}}}, one(OLDER_LIMIT_0)),
    ("Limit-0-RCC-empty-entry",
     {"KeyConditionExpression": "pk = :p", "Limit": 0, "ReturnConsumedCapacity": "BAD",
      "ExpressionAttributeValues": {":p": {"S": "p0"}, ":a": {}}},
     f"2 validation errors detected: {RCC_BAD}; {OLDER_LIMIT_0}"),
    ("Limit-below-int32-both-enums",
     {"KeyConditionExpression": "pk = :p", "Limit": -2147483649, "Select": "BAD",
      "ReturnConsumedCapacity": "BAD", "ExpressionAttributeValues": {":p": {"S": "p0"}}},
     f"3 validation errors detected: {SELECT_BAD}; {RCC_BAD}; {OLDER_LIMIT_MIN}"),
    ("Limit-1.5-both-enums",
     {"KeyConditionExpression": "pk = :p", "Limit": 1.5, "Select": "BAD",
      "ReturnConsumedCapacity": "BAD", "ExpressionAttributeValues": {":p": {"S": "p0"}}},
     f"2 validation errors detected: {SELECT_BAD}; {RCC_BAD}"),
    ("Limit-1.5-beside-N-bob",
     {"KeyConditionExpression": "pk = :p", "Limit": 1.5, "FilterExpression": "a = :n",
      "ExpressionAttributeValues": {":p": {"S": "p0"}, ":n": {"N": "bob"}}},
     "ExpressionAttributeValues contains invalid value: "
     "The parameter cannot be converted to a numeric value: bob for key :n"),
    ("Limit-0.0-ahead-of-the-mix",
     {"KeyConditionExpression": "pk = :p", "Limit": 0.0, "FilterExpression": "a = :p",
      "QueryFilter": _conditions("a", {"S": "x"}),
      "ExpressionAttributeValues": {":p": {"S": "p0"}}}, one(OLDER_LIMIT_0)),
]


class TestANullIsAValueTheServiceCannotRead:
    @pytest.mark.parametrize("fields, expected", [r[1:] for r in NULL_ROWS],
                             ids=[r[0] for r in NULL_ROWS])
    def test_rows(self, dynamodb_client, table, fields, expected):
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected


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

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in QUERY_OLDER_FORM],
                             ids=[r[0] for r in QUERY_OLDER_FORM])
    def test_query_limit_float_or_outside_int32(self, dynamodb_client, table, fields, expected):
        body = json.dumps({"TableName": table, **fields})
        assert _validation_message(*_raw(dynamodb_client, "Query", body)) == expected

    def test_query_limit_long_fraction(self, dynamodb_client, table):
        # Measured 2026-11-24 05:41 UTC (a table that does not exist): the
        # written decimal is truncated, so 0.99999999999999999999 answers the
        # limit clause with '0' and 2.99999999999999999999 is accepted.
        def body(limit: str) -> str:
            return ('{"TableName": %s, "KeyConditionExpression": "pk = :p", "Limit": %s, '
                    '"ExpressionAttributeValues": {":p": {"S": "p0"}}}' % (json.dumps(table), limit))

        refused = _raw(dynamodb_client, "Query", body("0.99999999999999999999"))
        assert _validation_message(*refused) == one(OLDER_LIMIT_0)
        status, payload = _raw(dynamodb_client, "Query", body("2.99999999999999999999"))
        assert status == 200, payload
        assert payload["Count"] == 1

    @pytest.mark.parametrize("entry, expected", [r[1:] for r in OLDER_FORM_SETS],
                             ids=[r[0] for r in OLDER_FORM_SETS])
    def test_scan_set_on_the_older_path(self, dynamodb_client, table, entry, expected):
        fields = ('"Limit": 1.5, "FilterExpression": "a = :a", "ExpressionAttributeValues": '
                  '{":a": ' + entry + '}')
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected

    def test_query_duplicate_ns_on_the_older_path(self, dynamodb_client, table):
        body = json.dumps({
            "TableName": table, "KeyConditionExpression": "pk = :p", "Limit": 1.5,
            "FilterExpression": "a = :a",
            "ExpressionAttributeValues": {":p": {"S": "p0"}, ":a": {"NS": ["1", "1"]}},
        })
        assert _validation_message(*_raw(dynamodb_client, "Query", body)) == DUPLICATE_NS

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
