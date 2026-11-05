# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Scan request validation: an empty ``ScanFilter`` in the parameter mix, an
empty ``AttributesToGet``, a malformed number in ``ExclusiveStartKey`` and the
nesting depth of ``ExpressionAttributeValues``.

Every request below is sent as raw signed JSON. Each is a row of the table
measured against DynamoDB (us-east-1) on 2026-10-25 at 10:38 and 10:55 UTC on
a Scan of an existing table of 29 items, or of the ones measured on 2026-10-30
at 12:40 UTC and on 2026-10-31 between 23:54 and 23:56 UTC where a row says
so, each sent twice, and each expected answer is the service's. The table here
holds 12 items, so the count the accepted request returns is 12.

Depth is the number of ``L`` wrappers around the leaf: 32 or more answers the
nesting text, 31 reaches the unused-value check. A malformed number at the
bottom is reported first, at the depths 31 to 60 that were measured.

The faults of ``ExclusiveStartKey`` come at two places (measured on
2026-11-05, each request twice): a value that is empty, carries an unknown type
descriptor or two descriptors is reported after the constraint clauses and the
request parameter checks, with the prefix ``The provided starting key is
invalid: ``, and ahead of the expression checks; a number or set fault is
reported after the expression and request checks, with no prefix, and ahead of
the table lookup. With faults in two members the service's choice follows the
attribute names and the request's order, so those tests accept either fault.
"""

from __future__ import annotations

import json

import pytest
from botocore.awsrequest import AWSRequest

from helpers import unique_name, wait_for_active, wait_for_deleted

ITEMS = 12

MIX = (
    "Can not use both expression and non-expression parameters in the same "
    "request: Non-expression parameters: {ScanFilter} "
    "Expression parameters: {FilterExpression}"
)
LENGTH = (
    "1 validation error detected: Value at 'AttributesToGet' failed to "
    "satisfy constraint: Member must have length greater than or equal to 1"
)
LENGTH_AND_LIMIT = (
    "2 validation errors detected: Value at 'AttributesToGet' failed to "
    "satisfy constraint: Member must have length greater than or equal to 1; "
    "Value at 'Limit' failed to satisfy constraint: Member must have value "
    "greater than or equal to 1"
)
NUMERIC_BOB = "The parameter cannot be converted to a numeric value: bob"
RCC_ENUM = (
    "1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed "
    "to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
)
NESTING = (
    "Nesting Levels have exceeded supported limits: "
    "Attributes in the item have nested levels beyond supported limit"
)
UNUSED = "Value provided in ExpressionAttributeValues unused in expressions: keys: {:d}"
LIMIT = (
    "1 validation error detected: Value at 'Limit' failed to satisfy constraint: "
    "Member must have value greater than or equal to 1"
)
SEGMENT = (
    "1 validation error detected: Value at 'Segment' failed to satisfy constraint: "
    "Member must have value greater than or equal to 0"
)
SELECT_ENUM = (
    "1 validation error detected: Value 'BAD' at 'select' failed to satisfy constraint: "
    "Member must satisfy enum value set: "
    "[SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"
)
SELECT_SPECIFIC = (
    "Must specify the AttributesToGet or ProjectionExpression when choosing to get "
    "SPECIFIC_ATTRIBUTES"
)
UNUSED_U = "Value provided in ExpressionAttributeValues unused in expressions: keys: {:u}"
KEY_PREFIX = "The provided starting key is invalid: "
EMPTY_KEY_VALUE = (
    KEY_PREFIX + "Supplied AttributeValue is empty, must contain exactly one of the "
    "supported datatypes"
)
TWO_DESCRIPTORS = (
    KEY_PREFIX + "Supplied AttributeValue has more than one datatypes set, must contain "
    "exactly one of the supported datatypes"
)
INVALID = "One or more parameter values were invalid: "
NS_EMPTY = INVALID + "An number set  may not be empty"
SS_EMPTY = INVALID + "An string set  may not be empty"
BS_EMPTY = INVALID + "Binary sets should not be empty"
NS_DUP = INVALID + "Input collection contains duplicates."
SS_DUP = INVALID + 'Input collection ["a", "a"] contains duplicates.'
NUMERIC_FRED = NUMERIC_BOB.replace("bob", "fred")
EAV_NO_EXPRESSION = "ExpressionAttributeValues can only be specified when using expressions"
MISSING_INDEX = "The table does not have the specified index: nope"

VALID_FILTER = '"FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "1"}}'
BOB_FILTER = '"FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "bob"}}'
ONE = {"N": "1"}
BOB = {"N": "bob"}
BOB_KEY = '"ExclusiveStartKey": {"pk": {"N": "bob"}, "sk": {"S": "x"}}'

# (id, the fields after TableName as JSON text, the expected message)
REFUSED = [
    ("ScanFilter-empty-with-FilterExpression", '"ScanFilter": {}, ' + VALID_FILTER, MIX),
    ("ScanFilter-empty-with-unused-bob",
     '"ScanFilter": {}, "ExpressionAttributeValues": {":n": {"N": "bob"}}', NUMERIC_BOB),
    ("AttributesToGet-empty", '"AttributesToGet": []', LENGTH),
    ("AttributesToGet-empty-with-FilterExpression",
     '"AttributesToGet": [], ' + VALID_FILTER, LENGTH),
    ("AttributesToGet-empty-with-FilterExpression-and-bob",
     '"AttributesToGet": [], ' + BOB_FILTER, LENGTH),
    ("ExclusiveStartKey-bob-on-an-S-key",
     '"ExclusiveStartKey": {"pk": {"N": "bob"}, "sk": {"S": "x"}}, ' + VALID_FILTER,
     NUMERIC_BOB),
    # Measured 2026-10-30 12:40 UTC.
    ("AttributesToGet-empty-with-Limit-0", '"AttributesToGet": [], "Limit": 0',
     LENGTH_AND_LIMIT),
    ("AttributesToGet-empty-with-ScanFilter-empty-and-FilterExpression",
     '"AttributesToGet": [], "ScanFilter": {}, ' + VALID_FILTER, LENGTH),
    ("ExclusiveStartKey-bob-with-no-filter",
     '"ExclusiveStartKey": {"pk": {"N": "bob"}, "sk": {"S": "x"}}', NUMERIC_BOB),
    ("ExclusiveStartKey-bob-in-the-index-key-of-a-GSI-scan",
     '"IndexName": "gsi", "ExclusiveStartKey": {"gpk": {"N": "bob"}, "pk": {"S": "p"}, '
     '"sk": {"S": "x"}}', NUMERIC_BOB),
    ("ExclusiveStartKey-bob-with-ReturnConsumedCapacity-BAD",
     '"ExclusiveStartKey": {"pk": {"N": "bob"}, "sk": {"S": "x"}}, '
     '"ReturnConsumedCapacity": "BAD"', RCC_ENUM),
]

# Measured 2026-10-31 between 23:54 and 23:56 UTC: each check sent beside the
# key comes ahead of the number in ExclusiveStartKey.
KEY_AFTER_THE_OTHER_CHECKS = [
    ("Limit-0", BOB_KEY + ', "Limit": 0', LIMIT),
    ("Segment-minus-1", BOB_KEY + ', "Segment": -1, "TotalSegments": 2', SEGMENT),
    ("Select-BAD", BOB_KEY + ', "Select": "BAD"', SELECT_ENUM),
    ("ScanFilter-empty-with-FilterExpression", BOB_KEY + ', "ScanFilter": {}, ' + VALID_FILTER,
     MIX),
    ("AttributesToGet-empty", BOB_KEY + ', "AttributesToGet": []', LENGTH),
    ("unused-value",
     BOB_KEY + ', "FilterExpression": "a = :p", '
     '"ExpressionAttributeValues": {":p": {"S": "x"}, ":u": {"S": "y"}}', UNUSED_U),
    ("bob-in-the-values-and-fred-in-the-key",
     '"ExclusiveStartKey": {"pk": {"N": "fred"}, "sk": {"S": "x"}}, ' + BOB_FILTER, NUMERIC_BOB),
]

# Measured 2026-10-31 and 2026-11-05: a number fault anywhere in the key, in
# an NS, in an L, in a non-key attribute and in an LSI key, answers the number
# text with no prefix.
KEY_NUMBER_FAULTS = [
    ("bob-in-an-NS", '"ExclusiveStartKey": {"pk": {"NS": ["bob"]}, "sk": {"S": "x"}}',
     NUMERIC_BOB),
    ("bob-in-an-L", '"ExclusiveStartKey": {"pk": {"L": [{"N": "bob"}]}, "sk": {"S": "x"}}',
     NUMERIC_BOB),
    ("bob-in-the-L-of-a-non-key-attribute",
     '"ExclusiveStartKey": {"pk": {"S": "p0"}, "sk": {"S": "x"}, "z": {"L": [{"N": "bob"}]}}',
     NUMERIC_BOB),
    ("bob-in-the-index-key-of-an-LSI-scan",
     '"IndexName": "lsi", "ExclusiveStartKey": {"pk": {"S": "p0"}, "sk": {"S": "x"}, '
     '"ln": {"N": "bob"}}', NUMERIC_BOB),
]

# Measured 2026-11-05: each key alone (the keys of MEASURED-scan-r3 section 4).
# A shape fault answers the service's text with the key prefix; a set fault
# answers the service's text with no prefix.
KEYS_ALONE = [
    ("empty", '{"pk": {}, "sk": {"S": "x"}}', EMPTY_KEY_VALUE),
    ("type-X", '{"pk": {"X": "1"}, "sk": {"S": "x"}}', EMPTY_KEY_VALUE),
    ("lower-case-s", '{"pk": {"s": "x"}, "sk": {"S": "x"}}', EMPTY_KEY_VALUE),
    ("empty-in-an-M", '{"pk": {"M": {"k": {}}}, "sk": {"S": "x"}}', EMPTY_KEY_VALUE),
    ("empty-in-an-L", '{"pk": {"L": [{}]}, "sk": {"S": "x"}}', EMPTY_KEY_VALUE),
    ("empty-non-key-attribute", '{"pk": {"S": "p0"}, "sk": {"S": "x"}, "z": {}}', EMPTY_KEY_VALUE),
    ("two-descriptors", '{"pk": {"S": "x", "N": "1"}, "sk": {"S": "x"}}', TWO_DESCRIPTORS),
    ("NS-empty", '{"pk": {"NS": []}, "sk": {"S": "x"}}', NS_EMPTY),
    ("SS-empty", '{"pk": {"SS": []}, "sk": {"S": "x"}}', SS_EMPTY),
    ("BS-empty", '{"pk": {"BS": []}, "sk": {"S": "x"}}', BS_EMPTY),
    ("NS-duplicates", '{"pk": {"NS": ["1", "1"]}, "sk": {"S": "x"}}', NS_DUP),
    ("NS-duplicates-by-value", '{"pk": {"NS": ["1", "1.0"]}, "sk": {"S": "x"}}', NS_DUP),
    ("SS-duplicates", '{"pk": {"SS": ["a", "a"]}, "sk": {"S": "x"}}', SS_DUP),
    ("NS-empty-non-key-attribute", '{"pk": {"S": "p0"}, "sk": {"S": "x"}, "z": {"NS": []}}',
     NS_EMPTY),
]

# Measured 2026-11-05: faults in two members of one key. The service's choice
# follows the attribute names and, on some tables, the request's member order:
# {"pk": {}, "sk": {"N": "bob"}} answered bob on 2026-10-31, and {"a": {}, "b":
# {"N": "bob"}} the empty value. Each row accepts either member's fault.
TWO_MEMBER_FAULTS = [
    ("empty-pk-then-bob", '{"pk": {}, "sk": {"N": "bob"}}', (EMPTY_KEY_VALUE, NUMERIC_BOB)),
    ("bob-then-empty-pk", '{"sk": {"N": "bob"}, "pk": {}}', (EMPTY_KEY_VALUE, NUMERIC_BOB)),
    ("type-X-pk-then-bob", '{"pk": {"X": "1"}, "sk": {"N": "bob"}}',
     (EMPTY_KEY_VALUE, NUMERIC_BOB)),
    ("bob-then-empty-sk", '{"pk": {"N": "bob"}, "sk": {}}', (EMPTY_KEY_VALUE, NUMERIC_BOB)),
    ("two-descriptors-then-bob", '{"pk": {"S": "x", "N": "1"}, "sk": {"N": "bob"}}',
     (TWO_DESCRIPTORS, NUMERIC_BOB)),
    ("NS-empty-then-bob", '{"pk": {"NS": []}, "sk": {"N": "bob"}}', (NS_EMPTY, NUMERIC_BOB)),
    ("empty-then-NS-empty", '{"pk": {}, "sk": {"NS": []}}', (EMPTY_KEY_VALUE, NS_EMPTY)),
    ("NS-empty-then-empty", '{"pk": {"NS": []}, "sk": {}}', (EMPTY_KEY_VALUE, NS_EMPTY)),
    ("empty-then-NS-bob", '{"pk": {}, "sk": {"NS": ["1", "bob"]}}', (EMPTY_KEY_VALUE, NUMERIC_BOB)),
]

# Measured 2026-11-05 (MEASURED-scan-r3 section 2): beside each class of key
# fault, a check of each stage. The request parameter checks come ahead of
# every key fault; the shape faults come ahead of the expression checks and of
# the table lookup; the expression checks come ahead of the number and set
# faults, which come ahead of the table lookup.
KEY_CLASSES = [
    ("empty", '{"pk": {}, "sk": {"S": "x"}}', EMPTY_KEY_VALUE, True),
    ("type-X", '{"pk": {"X": "1"}, "sk": {"S": "x"}}', EMPTY_KEY_VALUE, True),
    ("two-descriptors", '{"pk": {"S": "x", "N": "1"}, "sk": {"S": "x"}}', TWO_DESCRIPTORS, True),
    ("bob", '{"pk": {"N": "bob"}, "sk": {"S": "x"}}', NUMERIC_BOB, False),
    ("NS-empty", '{"pk": {"NS": []}, "sk": {"S": "x"}}', NS_EMPTY, False),
    ("SS-duplicates", '{"pk": {"SS": ["a", "a"]}, "sk": {"S": "x"}}', SS_DUP, False),
]
STAGES = [
    # A request parameter check (S1): ahead of every key fault. The head's
    # text carries a suffix the service dropped (#722), so the test asserts the
    # prefix of the text.
    ("ExpressionAttributeValues-with-no-expression",
     '"ExpressionAttributeValues": {":a": {"S": "x"}}', "parameter"),
    # An expression check (S3): ahead of a number or set fault, after a shape
    # fault.
    ("unused-value",
     '"FilterExpression": "a = :p", "ExpressionAttributeValues": {":p": {"S": "x"}, ":u": {"S": "y"}}',
     "expression"),
    # The table's indexes (S5): after every key fault.
    ("missing-index", '"IndexName": "nope"', "table"),
]

# Measured 2026-10-31 between 23:54 and 23:56 UTC on a table with an N hash
# key: the number texts for a leading space, an empty text and 39 digits.
N_KEY_FAULTS = [
    ("leading-space", '"ExclusiveStartKey": {"pk": {"N": " 1"}}',
     "The parameter cannot be converted to a numeric value:  1"),
    ("empty-text", '"ExclusiveStartKey": {"pk": {"N": ""}}',
     "The parameter cannot be converted to a numeric value: "),
    ("39-digits", '"ExclusiveStartKey": {"pk": {"N": "111111111111111111111111111111111111111"}}',
     "Attempting to store more than 38 significant digits in a Number"),
]


def nest(leaf: dict, levels: int) -> dict:
    """``leaf`` inside ``levels`` lists."""
    value = leaf
    for _ in range(levels):
        value = {"L": [value]}
    return value


def unused_value(value: dict) -> str:
    """A filter that reads ``:p`` and ``value`` under ``:d``, which nothing reads."""
    return ('"FilterExpression": "a = :p", "ExpressionAttributeValues": '
            + json.dumps({":p": {"S": "x"}, ":d": value}))


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
    """pk S, sk S, a GSI ``gsi`` on gpk S, an LSI ``lsi`` on ln N, with ITEMS items."""
    name = unique_name("scanreq")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
            {"AttributeName": "ln", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
        LocalSecondaryIndexes=[{
            "IndexName": "lsi",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "ln", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    for i in range(ITEMS):
        dynamodb_client.put_item(TableName=name, Item={
            "pk": {"S": f"p{i}"}, "sk": {"S": "s"}, "a": {"N": str(i)}})
    yield name
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


@pytest.fixture(scope="module")
def n_table(dynamodb_client):
    """pk N, with items 1 to 6."""
    name = unique_name("scanreqn")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "N"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    for i in range(1, 7):
        dynamodb_client.put_item(TableName=name, Item={"pk": {"N": str(i)}})
    yield name
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


class TestRequestChecks:
    """The parameter mix, the length constraint and the numbers in the key."""

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in REFUSED],
                             ids=[r[0] for r in REFUSED])
    def test_rows(self, dynamodb_client, table, fields, expected):
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected

    def test_an_empty_scan_filter_alone_is_accepted(self, dynamodb_client, table):
        status, payload = _scan(dynamodb_client, table,
                                '"ScanFilter": {}, "ConsistentRead": true')
        assert status == 200, payload
        assert (payload["Count"], payload["ScannedCount"]) == (ITEMS, ITEMS)


class TestExclusiveStartKey:
    """The key's two stages (measured 2026-10-31 and 2026-11-05)."""

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in KEY_AFTER_THE_OTHER_CHECKS],
                             ids=[r[0] for r in KEY_AFTER_THE_OTHER_CHECKS])
    def test_the_other_checks_come_first(self, dynamodb_client, table, fields, expected):
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected

    def test_32_levels_are_refused_ahead_of_the_key(self, dynamodb_client, table):
        fields = BOB_KEY + ", " + unused_value(nest(ONE, 32))
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == NESTING

    def test_the_select_rule_is_checked_ahead_of_the_key(self, dynamodb_client, table):
        # The service answers '1 validation error detected: ' and then this
        # text (#832). The Scan text here has no prefix, so only the check's
        # place is asserted.
        fields = BOB_KEY + ', "Select": "SPECIFIC_ATTRIBUTES"'
        message = _validation_message(*_scan(dynamodb_client, table, fields))
        assert message.endswith(SELECT_SPECIFIC), message

    def test_the_filter_syntax_is_checked_ahead_of_the_key(self, dynamodb_client, table):
        # The service answers 'Invalid FilterExpression: Syntax error; token:
        # "=", near: "= = :p"'. The parser here words the syntax error
        # differently, so only the check's place is asserted.
        fields = (BOB_KEY + ', "FilterExpression": "a = = :p", '
                  '"ExpressionAttributeValues": {":p": {"S": "x"}}')
        message = _validation_message(*_scan(dynamodb_client, table, fields))
        assert message.startswith("Invalid FilterExpression:"), message

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in KEY_NUMBER_FAULTS],
                             ids=[r[0] for r in KEY_NUMBER_FAULTS])
    def test_a_number_fault_anywhere_in_the_key(self, dynamodb_client, table, fields, expected):
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected

    @pytest.mark.parametrize("key, expected", [r[1:] for r in KEYS_ALONE],
                             ids=[r[0] for r in KEYS_ALONE])
    def test_each_key_alone(self, dynamodb_client, table, key, expected):
        fields = '"ExclusiveStartKey": ' + key
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected

    @pytest.mark.parametrize("key, either", [r[1:] for r in TWO_MEMBER_FAULTS],
                             ids=[r[0] for r in TWO_MEMBER_FAULTS])
    def test_two_faulty_members_answer_one_of_the_two_faults(
            self, dynamodb_client, table, key, either):
        fields = '"ExclusiveStartKey": ' + key
        message = _validation_message(*_scan(dynamodb_client, table, fields))
        assert message in either, message

    def test_two_number_faults_name_one_of_them(self, dynamodb_client, table):
        # The service named bob twice on 2026-10-31 (23:54 to 23:56 UTC) and
        # fred on 2026-11-04 (23:45 UTC) for this key.
        fields = '"ExclusiveStartKey": {"pk": {"N": "bob"}, "sk": {"N": "fred"}}'
        message = _validation_message(*_scan(dynamodb_client, table, fields))
        assert message in (NUMERIC_BOB, NUMERIC_FRED), message

    @pytest.mark.parametrize("key, key_text, shape", [r[1:] for r in KEY_CLASSES],
                             ids=[r[0] for r in KEY_CLASSES])
    @pytest.mark.parametrize("check, stage", [(r[1], r[2]) for r in STAGES],
                             ids=[r[0] for r in STAGES])
    def test_each_stage_beside_each_key_fault(
            self, dynamodb_client, table, key, key_text, shape, check, stage):
        fields = '"ExclusiveStartKey": ' + key + ", " + check
        message = _validation_message(*_scan(dynamodb_client, table, fields))
        if stage == "parameter":
            assert message.startswith(EAV_NO_EXPRESSION), message
        elif stage == "expression" and not shape:
            assert message == UNUSED_U
        else:
            # A shape fault comes ahead of the expression check; every key
            # fault comes ahead of the index resolution.
            assert message == key_text

    def test_the_segment_pairing_is_checked_ahead_of_a_number_in_the_key(
            self, dynamodb_client, table):
        # Measured 2026-11-05: Segment without TotalSegments answers its own
        # text beside bob in the key, and the key's text beside an empty value.
        fields = BOB_KEY + ', "Segment": 0'
        message = _validation_message(*_scan(dynamodb_client, table, fields))
        assert message == ("The TotalSegments parameter is required but was not present in the "
                           "request when Segment parameter is present"), message
        fields = '"ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}, "Segment": 0'
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == EMPTY_KEY_VALUE

    def test_an_undefined_filter_value_is_checked_ahead_of_a_number_in_the_key(
            self, dynamodb_client, table):
        # Measured 2026-11-05.
        fields = BOB_KEY + ', "FilterExpression": "a = :zz"'
        message = _validation_message(*_scan(dynamodb_client, table, fields))
        assert message == ("Invalid FilterExpression: An expression attribute value used in "
                           "expression is not defined; attribute value: :zz"), message

    def test_a_missing_table_is_reported_after_a_number_in_the_key(self, dynamodb_client):
        # Measured 2026-11-05: the key's number text ahead of
        # ResourceNotFoundException.
        status, payload = _scan(dynamodb_client, unique_name("nosuch"), BOB_KEY)
        assert status == 400, payload
        assert payload.get("__type", "").endswith("ValidationException"), payload
        assert payload.get("message") == NUMERIC_BOB, payload

    def test_an_empty_expression_value_is_reported_after_the_key_and_ahead_of_the_table(
            self, dynamodb_client, table):
        # Measured 2026-11-05: {":a": {}} read by the filter answers its own
        # text with no key and the key's text beside bob or an empty set.
        eav = '"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {}}'
        text = ("ExpressionAttributeValues contains invalid value: Supplied AttributeValue is "
                "empty, must contain exactly one of the supported datatypes for key :a")
        assert _validation_message(*_scan(dynamodb_client, table, eav)) == text
        assert _validation_message(*_scan(dynamodb_client, table, BOB_KEY + ", " + eav)) == NUMERIC_BOB
        fields = '"ExclusiveStartKey": {"pk": {"NS": []}, "sk": {"S": "x"}}, ' + eav
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == NS_EMPTY
        status, payload = _scan(dynamodb_client, unique_name("nosuch"), eav)
        assert payload.get("message") == text, payload

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in N_KEY_FAULTS],
                             ids=[r[0] for r in N_KEY_FAULTS])
    def test_the_number_texts_on_an_n_hash_key(self, dynamodb_client, n_table, fields, expected):
        assert _validation_message(*_scan(dynamodb_client, n_table, fields)) == expected


class TestNestingDepth:
    """``ExpressionAttributeValues`` nested in lists."""

    @pytest.mark.parametrize("levels", [32, 33, 40, 60])
    def test_a_valid_number_at_the_bottom_is_refused_from_32_levels(
            self, dynamodb_client, table, levels):
        fields = unused_value(nest(ONE, levels))
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == NESTING

    def test_31_levels_reach_the_unused_value_check(self, dynamodb_client, table):
        fields = unused_value(nest(ONE, 31))
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == UNUSED

    def test_32_levels_are_refused_ahead_of_the_select_enum(self, dynamodb_client, table):
        # Measured 2026-10-30 12:40 UTC.
        fields = unused_value(nest(ONE, 32)) + ', "Select": "BAD"'
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == NESTING

    @pytest.mark.parametrize("levels", [31, 32, 33, 40, 60])
    def test_a_malformed_number_at_the_bottom_is_reported_first(
            self, dynamodb_client, table, levels):
        fields = unused_value(nest(BOB, levels))
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == NUMERIC_BOB

    def test_a_map_at_32_levels_with_a_malformed_number_is_reported_first(
            self, dynamodb_client, table):
        fields = unused_value({"M": {"k": nest(BOB, 31)}})
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == NUMERIC_BOB
