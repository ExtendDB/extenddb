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

The service follows one of two check orders (measured 2026-10-31 and
2026-11-05, each request twice). A request that
holds a value that is not one AttributeValue (empty, an unknown or lower-case
type descriptor, two descriptors) in ``ExpressionAttributeValues``,
``ExclusiveStartKey`` or a ``ScanFilter`` list follows order U: the constraint
clauses in the older form with the ``Select`` enum among them, the request
parameter checks, the key's faults (shape and value together, the first faulty
member in the fitted member order, a shape fault with the prefix ``The provided
starting key is invalid: ``), the unreadable ``ExpressionAttributeValues``
entries, the nesting depth, the expression checks, the table. Every other
request follows order R: the constraint clauses in the newer form, the parameter
mix, the nesting depth, a set fault in ``ExpressionAttributeValues``, the request
parameter checks, the ``Select`` enum, the expression checks, the key's number
and set faults, the table. The set texts quote their members in order R and not
in order U. With two value faults in two key members the service's own sends
disagreed, so those tests accept either fault.

A malformed number in a ``ScanFilter`` list answers the number text ahead of
the key's faults (measured 2026-11-05); that check is #564 item 3, which PR
#507 makes, so its two rows are expected to fail here. In order U the
``ExpressionAttributeValues`` entries are visited in the same fitted order as
the key's members (the hash buckets, then the request's order), a malformed
number among them answers ``ExpressionAttributeValues contains invalid value:
... for key :a``, and a ``ScanFilter`` list value that is not one AttributeValue
answers the bare empty-value text after the no-expression checks. A key of four
members is visited with eight buckets (measured 2026-11-05 on a GSI with a
range key).
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

# Measured 2026-11-05: each key alone.
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

# Measured 2026-10-31 and 2026-11-05: faults in two members of one key, one of
# them a shape fault. The service names the first faulty member in the fitted
# member order (sk ahead of pk and of z in both
# request orders; the set texts of order U where the set member comes first).
TWO_MEMBER_FAULTS = [
    ("empty-pk-then-bob", '{"pk": {}, "sk": {"N": "bob"}}', NUMERIC_BOB),
    ("bob-then-empty-pk", '{"sk": {"N": "bob"}, "pk": {}}', NUMERIC_BOB),
    ("type-X-pk-then-bob", '{"pk": {"X": "1"}, "sk": {"N": "bob"}}', NUMERIC_BOB),
    ("bob-then-empty-sk", '{"pk": {"N": "bob"}, "sk": {}}', EMPTY_KEY_VALUE),
    ("two-descriptors-then-bob", '{"pk": {"S": "x", "N": "1"}, "sk": {"N": "bob"}}', NUMERIC_BOB),
    ("empty-then-NS-empty", '{"pk": {}, "sk": {"NS": []}}', NS_EMPTY),
    ("NS-empty-then-empty", '{"pk": {"NS": []}, "sk": {}}', EMPTY_KEY_VALUE),
    ("empty-then-NS-bob", '{"pk": {}, "sk": {"NS": ["1", "bob"]}}', NUMERIC_BOB),
    ("empty-in-an-L-then-bob", '{"pk": {"L": [{}]}, "sk": {"N": "bob"}}', NUMERIC_BOB),
    ("type-X-then-bob-in-z", '{"pk": {"X": "1"}, "sk": {"S": "x"}, "z": {"N": "bob"}}',
     NUMERIC_BOB),
    ("two-descriptors-then-SS-empty", '{"pk": {"S": "x", "N": "1"}, "sk": {"SS": []}}',
     KEY_PREFIX + SS_EMPTY),
    ("empty-then-NS-duplicates", '{"pk": {}, "sk": {"NS": ["1", "1"]}}',
     "Input collection contains duplicates"),
    ("empty-then-SS-duplicates", '{"pk": {}, "sk": {"SS": ["a", "a"]}}',
     INVALID + "Input collection [a, a] contains duplicates."),
]

# Two value faults in two members: the service's two sends disagreed on 8 of
# 22 such rows (2026-11-05), so each row accepts either member's fault.
TWO_VALUE_FAULTS = [
    ("NS-empty-then-bob", '{"pk": {"NS": []}, "sk": {"N": "bob"}}', (NS_EMPTY, NUMERIC_BOB)),
    ("SS-duplicates-then-bob", '{"pk": {"SS": ["a", "a"]}, "sk": {"N": "bob"}}',
     (SS_DUP, NUMERIC_BOB)),
]

# Measured 2026-11-05 on tables with other
# key names: (id, table fixture, key, expected). HASH a, RANGE b: a first; HASH
# pk, RANGE a: a first in both request orders; HASH id, RANGE ts: the member
# first in the request; the GSI key of three members: gpk ahead of pk.
MEMBER_ORDER_ROWS = [
    ("ab-empty-and-bob", "ab_table", '{"a": {}, "b": {"N": "bob"}}', EMPTY_KEY_VALUE),
    ("ab-bob-and-empty", "ab_table", '{"a": {"N": "bob"}, "b": {}}', NUMERIC_BOB),
    ("pa-empty-and-bob", "pa_table", '{"pk": {}, "a": {"N": "bob"}}', NUMERIC_BOB),
    ("pa-bob-and-empty-reversed", "pa_table", '{"a": {"N": "bob"}, "pk": {}}', NUMERIC_BOB),
    ("it-empty-and-bob", "it_table", '{"id": {}, "ts": {"N": "bob"}}', EMPTY_KEY_VALUE),
    ("it-bob-and-empty-reversed", "it_table", '{"ts": {"N": "bob"}, "id": {}}', NUMERIC_BOB),
]

# Measured 2026-11-05: beside each class of key
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
    # An expression check: ahead of a number or set fault (order R), after the
    # key's faults (order U).
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


EAV_VALUE = "ExpressionAttributeValues contains invalid value: "
EAV_EMPTY_E = (EAV_VALUE + "Supplied AttributeValue is empty, must contain exactly one of the "
               "supported datatypes for key :e")
EAV_EMPTY_A = EAV_EMPTY_E.replace(":e", ":a")
EAV_TWO_A = (EAV_VALUE + "Supplied AttributeValue has more than one datatypes set, must contain "
             "exactly one of the supported datatypes for key :a")
EAN_NO_EXPRESSION = "ExpressionAttributeNames can only be specified when using expressions"
CONDOP = "ConditionalOperator can only be used when Filter or Expected has two or more elements"
UNDEFINED_ZZ = ("Invalid FilterExpression: An expression attribute value used in expression "
                "is not defined; attribute value: :zz")
BEGINS_N = ("Invalid FilterExpression: Incorrect operand type for operator or function; "
            "operator or function: begins_with, operand type: N")
SEGMENT_ALONE = ("The TotalSegments parameter is required but was not present in the request "
                 "when Segment parameter is present")
EMPTY_KEY = '"ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}'
FILTER_P = '"FilterExpression": "a = :p"'
NEST32_S = json.dumps(nest({"S": "x"}, 32))
NEST32_N = json.dumps(nest({"N": "1"}, 32))

# Measured 2026-10-31 (B4-7) and 2026-11-05:
# representative rows of each order. An expected text given as ("startswith",
# prefix) asserts the check's place where the text predates this branch (#722,
# #838, #839). "4th" names the rows that fail on 45705bf27.
TWO_ORDERS = [
    # Order U: an unreadable ExpressionAttributeValues entry, read or unused,
    # after the Select enum, the request parameter checks and the key's faults,
    # ahead of the nesting check, the unused values and the table.
    ("B4-7", FILTER_P + ', "ExpressionAttributeValues": {":p": {"S": "x"}, ":d": '
     + NEST32_N + ', ":e": {}}', EAV_EMPTY_E),
    ("B4-7-flat", FILTER_P + ', "ExpressionAttributeValues": {":p": {"S": "x"}, ":e": {}}',
     EAV_EMPTY_E),
    ("B4-7-X", FILTER_P + ', "ExpressionAttributeValues": {":p": {"S": "x"}, ":e": {"X": "1"}}',
     EAV_EMPTY_E),
    ("eav-empty-select-bad", FILTER_P + ', "ExpressionAttributeValues": {":p": {"S": "x"}, '
     '":e": {}}, "Select": "BAD"', SELECT_ENUM),
    ("eav-empty-unused-and-key-empty", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":e": {}}, ' + EMPTY_KEY, EMPTY_KEY_VALUE),
    ("eav-empty-read-plus-unused",
     '"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {}, ":u": {"S": "x"}}',
     EAV_EMPTY_A),
    ("eav-read-X", '"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {"X": "1"}}',
     EAV_EMPTY_A),
    ("eav-read-two",
     '"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {"S": "x", "N": "1"}}',
     EAV_TWO_A),
    ("eav-read-empty-begins",
     '"FilterExpression": "begins_with(a, :a)", "ExpressionAttributeValues": {":a": {}}',
     EAV_EMPTY_A),
    ("B4-7-noexpr-empty", '"ExpressionAttributeValues": {":e": {}}',
     ("startswith", EAV_NO_EXPRESSION)),
    ("B4-7-noexpr-two", '"ExpressionAttributeValues": {":e": {"S": "x", "N": "1"}}',
     ("startswith", EAV_NO_EXPRESSION)),
    # Order U: the nesting check after the key's faults (the three b.*.nesting
    # rows of 2026-11-05 and the nest-*-key-empty rows).
    ("nesting-beside-empty-key", FILTER_P + ', "ExpressionAttributeValues": {":p": '
     + NEST32_S + '}, ' + EMPTY_KEY, EMPTY_KEY_VALUE),
    ("nesting-beside-type-X-key", FILTER_P + ', "ExpressionAttributeValues": {":p": '
     + NEST32_S + '}, "ExclusiveStartKey": {"pk": {"X": "1"}, "sk": {"S": "x"}}',
     EMPTY_KEY_VALUE),
    ("nesting-beside-two-descriptors-key", FILTER_P + ', "ExpressionAttributeValues": {":p": '
     + NEST32_S + '}, "ExclusiveStartKey": {"pk": {"S": "x", "N": "1"}, "sk": {"S": "x"}}',
     TWO_DESCRIPTORS),
    ("nest-read-N-key-empty", FILTER_P + ', "ExpressionAttributeValues": {":p": '
     + NEST32_N + '}, ' + EMPTY_KEY, EMPTY_KEY_VALUE),
    ("nest-unused-S-key-empty", FILTER_P + ', "ExpressionAttributeValues": {":p": {"S": "x"}, '
     '":d": ' + NEST32_S + '}, ' + EMPTY_KEY, EMPTY_KEY_VALUE),
    ("nest-unused-N-key-empty", FILTER_P + ', "ExpressionAttributeValues": {":p": {"S": "x"}, '
     '":d": ' + NEST32_N + '}, ' + EMPTY_KEY, EMPTY_KEY_VALUE),
    # Order U: the parameter mix and the other request parameter checks ahead of
    # the key's shape fault (arity-key-empty-and-eav-noexpr).
    ("mix-beside-empty-key", '"ScanFilter": {"a": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": [{"N": "1"}]}}, "FilterExpression": "a = :p", '
     '"ExpressionAttributeValues": {":p": {"N": "1"}}, ' + EMPTY_KEY, MIX),
    ("arity-key-empty-and-ean-noexpr", '"ScanFilter": {"a": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": []}}, "ExpressionAttributeNames": {"#a": "a"}, ' + EMPTY_KEY,
     EAN_NO_EXPRESSION),
    # Order R: the nesting check ahead of the Select enum, the filter syntax,
    # the no-expression check and the key's number.
    ("nest-unused-S-select-bad", FILTER_P + ', "ExpressionAttributeValues": {":p": {"S": "x"}, '
     '":d": ' + NEST32_S + '}, "Select": "BAD"', NESTING),
    ("nest-read-S-filter-syntax", '"FilterExpression": "a = = :p", "ExpressionAttributeValues": '
     '{":p": ' + NEST32_S + '}', NESTING),
    ("nest-read-S-eav-noexpr-sibling", '"ProjectionExpression": "a", '
     '"ExpressionAttributeValues": {":p": ' + NEST32_S + '}', NESTING),
    ("nest-read-S-key-bob", FILTER_P + ', "ExpressionAttributeValues": {":p": ' + NEST32_S
     + '}, ' + BOB_KEY, NESTING),
    ("B4-5a", '"ExpressionAttributeValues": {":d": ' + NEST32_N + '}', NESTING),
    # Order R: a set fault in ExpressionAttributeValues ahead of the
    # no-expression check; the set texts with the members JSON-quoted.
    ("B4-7-noexpr-NSe", '"ExpressionAttributeValues": {":e": {"NS": []}}', NS_EMPTY),
    ("eav-read-NSe", '"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {"NS": []}}',
     NS_EMPTY),
    ("eav-read-SSdup",
     '"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {"SS": ["a", "a"]}}',
     SS_DUP),
    # Order R: the constraint enums and the request parameter checks among
    # themselves, and the Select enum after the no-expression check.
    ("mix-rcc-bad", '"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": '
     '[{"N": "1"}]}}, "FilterExpression": "a = :p", "ExpressionAttributeValues": '
     '{":p": {"N": "1"}}, "ReturnConsumedCapacity": "BAD"', RCC_ENUM),
    ("rcc-bad-select-bad", '"ReturnConsumedCapacity": "BAD", "Select": "BAD"', RCC_ENUM),
    ("mix-select-bad", '"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": '
     '[{"N": "1"}]}}, "FilterExpression": "a = :p", "ExpressionAttributeValues": '
     '{":p": {"N": "1"}}, "Select": "BAD"', MIX),
    ("select-bad-eav-noexpr", '"ExpressionAttributeValues": {":p": {"S": "x"}}, "Select": "BAD"',
     ("startswith", EAV_NO_EXPRESSION)),
    ("eav-noexpr-ean-noexpr", '"ExpressionAttributeValues": {":p": {"S": "x"}}, '
     '"ExpressionAttributeNames": {"#a": "a"}', EAN_NO_EXPRESSION),
    ("ean-noexpr-condop", '"ExpressionAttributeNames": {"#a": "a"}, "ConditionalOperator": "AND"',
     EAN_NO_EXPRESSION),
    ("condop-arity", '"ConditionalOperator": "AND", "ScanFilter": {"a": {"ComparisonOperator": '
     '"EQ", "AttributeValueList": []}}', CONDOP),
    # Order R: the expression checks among themselves (section 6 step 5).
    ("filter-syntax-segment-alone", '"FilterExpression": "a = = :p", '
     '"ExpressionAttributeValues": {":p": {"N": "1"}}, "Segment": 0',
     ("startswith", "Invalid FilterExpression:")),
    ("segment-alone-unused", FILTER_P + ', "ExpressionAttributeValues": {":p": {"S": "x"}, '
     '":u": {"S": "y"}}, "Segment": 0', UNUSED_U),
    ("undefined-unused", '"FilterExpression": "a = :zz", "ExpressionAttributeValues": '
     '{":u": {"S": "y"}}', UNDEFINED_ZZ),
    ("undefined-segment-alone", '"FilterExpression": "a = :zz", "Segment": 0', UNDEFINED_ZZ),
    ("undefined-select-specific", '"FilterExpression": "a = :zz", "Select": "SPECIFIC_ATTRIBUTES"',
     UNDEFINED_ZZ),
    ("begins-n-segment-alone", '"FilterExpression": "begins_with(a, :p)", '
     '"ExpressionAttributeValues": {":p": {"N": "1"}}, "Segment": 0', BEGINS_N),
    ("begins-n-unused", '"FilterExpression": "begins_with(a, :p)", '
     '"ExpressionAttributeValues": {":p": {"N": "1"}, ":u": {"S": "y"}}', BEGINS_N),
    ("projection-syntax-filter-syntax", '"FilterExpression": "a = = :p", '
     '"ExpressionAttributeValues": {":p": {"N": "1"}}, "ProjectionExpression": "a,,b"',
     ("startswith", "Invalid FilterExpression:")),
    ("select-specific-filter-syntax", '"FilterExpression": "a = = :p", '
     '"ExpressionAttributeValues": {":p": {"N": "1"}}, "Select": "SPECIFIC_ATTRIBUTES"',
     ("startswith", "Invalid FilterExpression:")),
    ("allproj-noindex-filter-syntax", '"Select": "ALL_PROJECTED_ATTRIBUTES", '
     '"FilterExpression": "a = = :p", "ExpressionAttributeValues": {":p": {"N": "1"}}',
     ("startswith", "Invalid FilterExpression:")),
    ("select-count-pe-unused", '"Select": "COUNT", "ProjectionExpression": "a", ' + FILTER_P
     + ', "ExpressionAttributeValues": {":p": {"S": "x"}, ":u": {"S": "y"}}', UNUSED_U),
    ("segment-ge-total-unused", FILTER_P + ', "ExpressionAttributeValues": {":p": {"S": "x"}, '
     '":u": {"S": "y"}}, "Segment": 2, "TotalSegments": 2', UNUSED_U),
    ("select-count-atg-segment-alone", '"Select": "COUNT", "AttributesToGet": ["a"], "Segment": 0',
     ("startswith", "Cannot specify the ")),
    ("select-count-atg-unused", '"Select": "COUNT", "AttributesToGet": ["a"], '
     '"ExpressionAttributeValues": {":u": {"S": "y"}}', ("startswith", EAV_NO_EXPRESSION)),
    # Order R: the key's value faults after the expression checks, their texts.
    ("key-bob-eav-bob", '"FilterExpression": "a = :n", "ExpressionAttributeValues": '
     '{":n": {"N": "fred"}}, ' + BOB_KEY, NUMERIC_FRED),
    ("key-bob-and-NSe-one-member",
     '"ExclusiveStartKey": {"pk": {"S": "x"}, "sk": {"S": "x"}, "z": {"NS": ["bob", "1", "1"]}}',
     NUMERIC_BOB),
    ("key-NS-dup-and-bob",
     '"ExclusiveStartKey": {"pk": {"S": "x"}, "sk": {"S": "x"}, "z": {"NS": ["1", "1", "bob"]}}',
     NUMERIC_BOB),
    ("key-BS-dup",
     '"ExclusiveStartKey": {"pk": {"S": "x"}, "sk": {"S": "x"}, "z": {"BS": ["YQ==", "YQ=="]}}',
     INVALID + 'Input collection ["YQ==","YQ=="]of type BS contains duplicates.'),
    ("key-SS-dup-quotes",
     '"ExclusiveStartKey": {"pk": {"S": "x"}, "sk": {"S": "x"}, "z": {"SS": ["a\\"b", "a\\"b"]}}',
     INVALID + 'Input collection ["a\\"b", "a\\"b"] contains duplicates.'),
    ("key-bob-schema-mismatch", '"ExclusiveStartKey": {"pk": {"N": "bob"}}', NUMERIC_BOB),
    # Order U: the key's set faults beside an unreadable expression value, the
    # order-U texts (the NS duplicate text bare, the SS members unquoted).
    ("key-bob-eav-empty-read-and-NSe", '"FilterExpression": "a = :a", '
     '"ExpressionAttributeValues": {":a": {}}, "ExclusiveStartKey": {"pk": {"NS": []}, '
     '"sk": {"S": "x"}}', NS_EMPTY),
    ("key-NS-dup-beside-eav-empty", '"FilterExpression": "a = :a", '
     '"ExpressionAttributeValues": {":a": {}}, "ExclusiveStartKey": {"pk": {"NS": ["1", "1"]}, '
     '"sk": {"S": "x"}}', "Input collection contains duplicates"),
    ("key-SS-dup-beside-eav-empty", '"FilterExpression": "a = :a", '
     '"ExpressionAttributeValues": {":a": {}}, "ExclusiveStartKey": {"pk": {"SS": ["a", "a"]}, '
     '"sk": {"S": "x"}}', INVALID + "Input collection [a, a] contains duplicates."),
    # Order U: the key's shape fault ahead of the table's index rules.
    ("key-empty-and-consistent-gsi", '"IndexName": "gsi", "ConsistentRead": true, '
     '"ExclusiveStartKey": {"gpk": {"S": "g"}, "pk": {"S": "p"}, "sk": {}}', EMPTY_KEY_VALUE),
    ("key-shape-M-nested-N-bob", '"ExclusiveStartKey": {"pk": {"S": "x"}, "sk": {"S": "x"}, '
     '"z": {"M": {"k": {"N": "bob"}, "j": {}}}}', EMPTY_KEY_VALUE),
]


EMPTY_VALUE_BARE = ("Supplied AttributeValue is empty, must contain exactly one of the "
                    "supported datatypes")
MORE_THAN_ONE_BARE = ("Supplied AttributeValue has more than one datatypes set, must contain "
                      "exactly one of the supported datatypes")
EAV_EMPTY_B = EAV_EMPTY_E.replace(":e", ":b")
EAV_EMPTY_P = EAV_EMPTY_E.replace(":e", ":p")
EAV_NUMBER_A = EAV_VALUE + NUMERIC_BOB + " for key :a"
EAV_NUMBER_N = EAV_VALUE + NUMERIC_BOB + " for key :n"
SF_EMPTY = '"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{}]}}'
SF_BOB = '"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "bob"}]}}'
NONASCII_KEY_A_BOB = '"ExclusiveStartKey": {"\\u00e4": {"N": "bob"}, "\\u00e9": {}}'
NONASCII_KEY_E_EMPTY = '"ExclusiveStartKey": {"\\u00e9": {}, "\\u00e4": {"N": "bob"}}'

# Measured 2026-11-05 at 05:31, 06:20 and 07:38 UTC: (id, table
# fixture or None for a missing table, the fields, the expected text). A text
# given as ("startswith", prefix) asserts the check's place where the text
# predates this branch (#838); ("either", a, b) accepts both answers where the
# service's two sends differed.
ROWS_2026_11_05 = [
    # 7.1: a malformed number in a ScanFilter list, ahead of the key's number.
    # The check is #564 item 3, which PR #507 makes (SCAN_FILTER_NUMBER_ROWS).
    ("sfR-N-bob", "table", SF_BOB, NUMERIC_BOB),
    ("sfR-N-bob-and-key-bob", "table",
     SF_BOB + ', "ExclusiveStartKey": {"pk": {"N": "fred"}, "sk": {"S": "x"}}', NUMERIC_BOB),
    # 7.2: order U, the ExpressionAttributeValues entries in the fitted order,
    # a number among them in the invalid-value form, after the key's faults.
    ("eavU-num-first-sorted", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":a": {"N": "bob"}, ":e": {}}', EAV_NUMBER_A),
    ("eavU-num-last-sorted", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":n": {"N": "bob"}, ":e": {}}', EAV_NUMBER_N),
    ("eavU-num-and-key-empty", "table", '"FilterExpression": "a = :n", '
     '"ExpressionAttributeValues": {":n": {"N": "bob"}}, ' + EMPTY_KEY, EMPTY_KEY_VALUE),
    ("eavU-num-and-key-empty-and-eav-empty", "table", '"FilterExpression": "a = :n", '
     '"ExpressionAttributeValues": {":n": {"N": "bob"}, ":e": {}}, ' + EMPTY_KEY,
     EMPTY_KEY_VALUE),
    ("eavU-two-faulty-entries", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":b": {}, ":a": {"X": "1"}}', EAV_EMPTY_B),
    # 7.3: four members on a GSI with a range key (eight buckets: gpk and pk
    # ahead of gsk), and names outside ASCII.
    ("key4-gsk-bob-gpk-empty", "t4_table", '"IndexName": "gsi2", "ExclusiveStartKey": '
     '{"gpk": {}, "gsk": {"N": "bob"}, "pk": {"S": "p"}, "sk": {"S": "s"}}', EMPTY_KEY_VALUE),
    ("key4-gsk-bob-gpk-empty-reversed", "t4_table", '"IndexName": "gsi2", "ExclusiveStartKey": '
     '{"sk": {"S": "s"}, "pk": {"S": "p"}, "gsk": {"N": "bob"}, "gpk": {}}', EMPTY_KEY_VALUE),
    ("key4-pk-bob-gsk-empty", "t4_table", '"IndexName": "gsi2", "ExclusiveStartKey": '
     '{"gpk": {"S": "g"}, "gsk": {}, "pk": {"N": "bob"}, "sk": {"S": "s"}}', NUMERIC_BOB),
    ("keyU-nonascii-a-bob-e-empty", "u_table", NONASCII_KEY_A_BOB, NUMERIC_BOB),
    ("keyU-nonascii-e-empty-a-bob", "u_table", NONASCII_KEY_E_EMPTY, NUMERIC_BOB),
    # 7.4: a ScanFilter list value that is not one AttributeValue, in order U:
    # the bare empty-value text for {} and for an unknown descriptor, after the
    # no-expression checks and ahead of ConditionalOperator, the value count,
    # the key's faults and the table; the Select enum ahead of it.
    ("sfU-empty-alone", "table", SF_EMPTY, EMPTY_VALUE_BARE),
    ("sfU-X-alone", "table",
     '"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"X": "1"}]}}',
     EMPTY_VALUE_BARE),
    ("sfU-empty-and-ean-noexpr", "table", SF_EMPTY + ', "ExpressionAttributeNames": {"#a": "a"}',
     EAN_NO_EXPRESSION),
    ("sfU-empty-and-key-empty", "table", SF_EMPTY + ", " + EMPTY_KEY, EMPTY_VALUE_BARE),
    ("sfU-empty-and-key-bob", "table", SF_EMPTY + ", " + BOB_KEY, EMPTY_VALUE_BARE),
    ("sfU-empty-arity", "table",
     '"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{}, {}]}}',
     EMPTY_VALUE_BARE),
    ("sfU-empty-condop", "table", '"ConditionalOperator": "AND", ' + SF_EMPTY, EMPTY_VALUE_BARE),
    ("sfU-empty-select-bad", "table", SF_EMPTY + ', "Select": "BAD"', SELECT_ENUM),
    ("sfU-empty-missing-table", None, SF_EMPTY, EMPTY_VALUE_BARE),
    # 7.5 and the other rows the head answers as the service does.
    ("keyU-SSe-beside-eav-empty", "table", '"FilterExpression": "a = :a", '
     '"ExpressionAttributeValues": {":a": {}}, "ExclusiveStartKey": {"pk": {"SS": []}, '
     '"sk": {"S": "x"}}', KEY_PREFIX + SS_EMPTY),
    ("keyU-BSe-beside-pk-empty", "table", '"ExclusiveStartKey": {"pk": {}, "sk": {"BS": []}}',
     KEY_PREFIX + BS_EMPTY),
    ("keyU-BSdup-beside-pk-empty", "table",
     '"ExclusiveStartKey": {"pk": {}, "sk": {"BS": ["YQ==", "YQ=="]}}',
     INVALID + "Input collection [YQ==, YQ==]of type BS contains duplicates."),
    ("eavR-NSe-and-num-first-sorted", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":a": {"N": "bob"}, ":e": {"NS": []}}', ("either", NS_EMPTY, NUMERIC_BOB)),
    ("eavR-NSe-and-num-last-sorted", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":n": {"N": "bob"}, ":e": {"NS": []}}', ("either", NS_EMPTY, NUMERIC_BOB)),
    ("eavR-NSe-and-ean-noexpr", "table", '"ExpressionAttributeNames": {"#a": "a"}, '
     '"ExpressionAttributeValues": {":e": {"NS": []}}', NS_EMPTY),
    ("eavR-NSe-and-rcc-bad", "table", '"ReturnConsumedCapacity": "BAD", '
     '"ExpressionAttributeValues": {":e": {"NS": []}}', RCC_ENUM),
    ("eavR-NSe-and-select-bad", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":e": {"NS": []}}, "Select": "BAD"', NS_EMPTY),
    # Re-measured 2026-11-05 at 06:20 UTC with the nested value written out
    # (32 L levels): the empty set ahead of the nesting check in order R, and
    # the empty leaf of a nested entry named in order U.
    ("eavR-NSe-and-nest", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + NEST32_N + ', ":e": {"NS": []}}', NS_EMPTY),
    ("eavU-nest32-empty-leaf", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + json.dumps(nest({}, 32)) + '}', EAV_EMPTY_E.replace(":e", ":d")),
    ("eavU-empty-and-filter-syntax", "table", '"FilterExpression": "a = = :p", '
     '"ExpressionAttributeValues": {":p": {}}', EAV_EMPTY_P),
    ("eavU-empty-and-undefined", "table", '"FilterExpression": "a = :zz", '
     '"ExpressionAttributeValues": {":e": {}}', EAV_EMPTY_E),
    ("eavU-empty-and-projection-syntax", "table", '"ProjectionExpression": "a,,b", ' + FILTER_P
     + ', "ExpressionAttributeValues": {":p": {}}', EAV_EMPTY_P),
    ("eavU-empty-and-select-bad-and-mix", "table", '"ScanFilter": {"a": {"ComparisonOperator": '
     '"EQ", "AttributeValueList": [{"N": "1"}]}}, ' + FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"N": "1"}, ":e": {}}, "Select": "BAD"', SELECT_ENUM),
    ("eavU-empty-and-rcc-bad", "table", '"ReturnConsumedCapacity": "BAD", ' + FILTER_P
     + ', "ExpressionAttributeValues": {":p": {"S": "x"}, ":e": {}}', RCC_ENUM),
    ("selR-condop-select-bad", "table", '"ConditionalOperator": "AND", "ScanFilter": {"a": '
     '{"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "1"}]}}, "Select": "BAD"', CONDOP),
    ("selR-arity-select-bad", "table", '"ScanFilter": {"a": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": []}}, "Select": "BAD"',
     ("either", "Invalid number of attribute values for comparison",
      INVALID + "Invalid number of argument(s) for the EQ ComparisonOperator")),
    ("flag-unknown-field-empty", "table", '"Limit": 0, "Foo": {}', LIMIT),
    ("flag-queryfilter-on-scan", "table", '"Limit": 0, "QueryFilter": {"a": {"ComparisonOperator": '
     '"EQ", "AttributeValueList": [{}]}}', LIMIT),
    # Measured 2026-11-05 at 07:38 UTC. Order U, an ExpressionAttributeValues
    # set fault takes the invalid-value form at the map's step, in the entries'
    # hash order (:s in bucket 1 ahead of :e in 3).
    ("eavU-NSe-entry", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":s": {"NS": []}, ":e": {}}', EAV_VALUE + NS_EMPTY + " for key :s"),
    ("eavU-SSdup-entry", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":s": {"SS": ["a", "a"]}, ":e": {}}',
     EAV_VALUE + INVALID + "Input collection [a, a] contains duplicates. for key :s"),
    # Inside one entry the first fault in the value's element order is named.
    ("eavU-number-then-empty-in-one-L", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":l": {"L": [{"N": "bob"}, {}]}}', EAV_NUMBER_A.replace(":a", ":l")),
    # Order U: the entries' hash order with the request's order inside a bucket
    # (:e and :a share bucket 3), four and seven entries (eight and sixteen
    # buckets), a number below the top of an entry, the 38-digit text.
    ("eavU-reqorder-e-then-a", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":e": {}, ":a": {"N": "bob"}}', EAV_EMPTY_E),
    ("eavU-four-entries", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":b": {"S": "y"}, ":a": {"N": "bob"}, ":e": {}}', EAV_EMPTY_E),
    ("eavU-seven-entries", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":b": {}, ":e": {"S": "e"}, ":a": {"S": "a"}, ":n": {"N": "bob"}, '
     '":d": {"S": "d"}, ":z": {"S": "z"}}', EAV_NUMBER_N),
    ("eavU-number-in-L", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":l": {"L": [{"N": "bob"}]}, ":e": {}}', EAV_NUMBER_A.replace(":a", ":l")),
    ("eavU-number-in-NS", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":s": {"NS": ["bob"]}, ":e": {}}', EAV_NUMBER_A.replace(":a", ":s")),
    ("eavU-39-digits", "table", FILTER_P + ', "ExpressionAttributeValues": {":p": {"S": "x"}, '
     '":a": {"N": "123456789012345678901234567890123456789"}, ":e": {}}',
     EAV_VALUE + "Attempting to store more than 38 significant digits in a Number for key :a"),
    # Order R, two faulty entries: the numbers in the hash order (:n in bucket 0
    # ahead of :a in 3); the set faults in sorted name order, where the
    # service's two sends disagreed (the SS text, then the NS text) and the
    # sorted order is pinned so that the answer does not depend on the backend.
    ("eavR-two-numbers", "table", '"FilterExpression": "a = :a", "ExpressionAttributeValues": '
     '{":n": {"N": "fred"}, ":a": {"N": "bob"}}', NUMERIC_BOB.replace("bob", "fred")),
    ("eavR-two-set-faults", "table", '"ExpressionAttributeValues": {":n": {"NS": []}, ":a": {"SS": []}}',
     ("either", SS_EMPTY, NS_EMPTY)),
    # Order R: a malformed number inside an entry the deserializer refuses comes
    # after the nesting check of another entry; with the number ahead of the
    # set fault in one L the service's two sends disagreed. The SS duplicates
    # beside a nested value answered the duplicates text and the nesting text.
    ("eavR-NSbobdup-and-nest", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + NEST32_N + ', ":a": {"NS": ["bob", "1", "1"]}}', NESTING),
    ("eavR-L-bob-then-NSe-and-nest", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + NEST32_N + ', ":l": {"L": [{"N": "bob"}, {"NS": []}]}}',
     ("either", NUMERIC_BOB, NESTING)),
    ("eavR-SSdup-and-nest", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + NEST32_N + ', ":s": {"SS": ["a", "a"]}}',
     ("either", INVALID + 'Input collection ["a", "a"] contains duplicates.', NESTING)),
    ("eavR-NSe-and-mix", "table", FILTER_P + ', "ScanFilter": {"a": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": [{"N": "1"}]}}, "ExpressionAttributeValues": {":p": {"S": "x"}, '
     '":e": {"NS": []}}', MIX),
    # Keys of five and seven members on the GSI (eight and sixteen buckets), and
    # a key name outside the Basic Multilingual Plane.
    ("key5-gpk-bob-gsk-empty", "t4_table", '"IndexName": "gsi2", "ExclusiveStartKey": '
     '{"gpk": {"N": "bob"}, "gsk": {}, "pk": {"S": "p"}, "sk": {"S": "s"}, "z": {"S": "z"}}',
     NUMERIC_BOB),
    ("key7-a-bob-sk-empty", "t4_table", '"IndexName": "gsi2", "ExclusiveStartKey": '
     '{"gpk": {"S": "g"}, "gsk": {"S": "h"}, "pk": {"S": "p"}, "sk": {}, "a": {"N": "bob"}, '
     '"b": {"S": "b"}, "c": {"S": "c"}}', NUMERIC_BOB),
    ("keyU-supplementary-first", "v_table",
     '"ExclusiveStartKey": {"\\ud834\\udd1e": {}, "a": {"N": "bob"}}', EMPTY_KEY_VALUE),
    ("keyU-supplementary-second", "v_table",
     '"ExclusiveStartKey": {"a": {"N": "bob"}, "\\ud834\\udd1e": {}}', NUMERIC_BOB),
    # Order U, the ScanFilter lists: two descriptors, an empty value inside an
    # L, which condition is named (the fitted order: pk ahead of sk, the
    # request's order for ts and id in one bucket), which list value (the
    # list's order), and ExpressionAttributeValues with no expression first.
    ("sfU-two-descriptors", "table", '"ScanFilter": {"a": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": [{"S": "x", "N": "1"}]}}', MORE_THAN_ONE_BARE),
    ("sfU-empty-in-L", "table", '"ScanFilter": {"a": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": [{"L": [{}]}]}}', EMPTY_VALUE_BARE),
    ("sfU-two-conditions-pk-sk", "table", '"ScanFilter": {"pk": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": [{}]}, "sk": {"ComparisonOperator": "EQ", "AttributeValueList": '
     '[{"S": "x", "N": "1"}]}}', MORE_THAN_ONE_BARE),
    ("sfU-two-conditions-same-bucket", "table", '"ScanFilter": {"ts": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": [{}]}, "id": {"ComparisonOperator": "EQ", "AttributeValueList": '
     '[{"S": "x", "N": "1"}]}}', EMPTY_VALUE_BARE),
    ("sfU-list-two-values", "table", '"ScanFilter": {"a": {"ComparisonOperator": "BETWEEN", '
     '"AttributeValueList": [{"S": "x", "N": "1"}, {}]}}', MORE_THAN_ONE_BARE),
    ("sfU-eav-noexpr-and-list-empty", "table", '"ExpressionAttributeValues": {":a": {"S": "x"}}, '
     '"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{}]}}',
     ("startswith", EAV_NO_EXPRESSION)),
    # Order R: a set fault in a ScanFilter list value, the deserializer's text.
    ("sfR-NSe-in-list", "table", '"ScanFilter": {"a": {"ComparisonOperator": "CONTAINS", '
     '"AttributeValueList": [{"NS": []}]}}', NS_EMPTY),
    # Order U: the empty BS of a key member keeps the key prefix beside an
    # unreadable expression value, inside an L and in a member that is not a
    # key attribute.
    ("keyU-BSe-beside-eav-empty", "table", '"FilterExpression": "a = :a", '
     '"ExpressionAttributeValues": {":a": {}}, "ExclusiveStartKey": {"pk": {"BS": []}, '
     '"sk": {"S": "x"}}', KEY_PREFIX + INVALID + "Binary sets should not be empty"),
    ("keyU-BSe-in-L-beside-pk-empty", "table", '"ExclusiveStartKey": {"pk": {}, "sk": {"L": '
     '[{"BS": []}]}}', KEY_PREFIX + INVALID + "Binary sets should not be empty"),
    ("keyU-BSe-in-nonkey-member", "table", '"ExclusiveStartKey": {"pk": {}, "sk": {"S": "s"}, '
     '"b": {"BS": []}}', KEY_PREFIX + INVALID + "Binary sets should not be empty"),
]


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


def _two_member_table(dynamodb_client, prefix: str, hash_key: str, range_key: str):
    name = unique_name(prefix)
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": hash_key, "AttributeType": "S"},
                              {"AttributeName": range_key, "AttributeType": "S"}],
        KeySchema=[{"AttributeName": hash_key, "KeyType": "HASH"},
                   {"AttributeName": range_key, "KeyType": "RANGE"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    return name


@pytest.fixture(scope="module")
def ab_table(dynamodb_client):
    """HASH a S, RANGE b S, no items."""
    name = _two_member_table(dynamodb_client, "scanreqab", "a", "b")
    yield name
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


@pytest.fixture(scope="module")
def pa_table(dynamodb_client):
    """HASH pk S, RANGE a S, no items."""
    name = _two_member_table(dynamodb_client, "scanreqpa", "pk", "a")
    yield name
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


@pytest.fixture(scope="module")
def it_table(dynamodb_client):
    """HASH id S, RANGE ts S, no items."""
    name = _two_member_table(dynamodb_client, "scanreqit", "id", "ts")
    yield name
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


@pytest.fixture(scope="module")
def t4_table(dynamodb_client):
    """pk S, sk S, with a GSI ``gsi2`` on gpk S HASH and gsk S RANGE: a four-member index key."""
    name = unique_name("scanreqt4")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"}
                              for n in ("pk", "sk", "gpk", "gsk")],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi2",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"},
                          {"AttributeName": "gsk", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    yield name
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


@pytest.fixture(scope="module")
def u_table(dynamodb_client):
    """HASH \u00e4 S, RANGE \u00e9 S: member names outside ASCII."""
    name = _two_member_table(dynamodb_client, "scanrequ", "\u00e4", "\u00e9")
    yield name
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


@pytest.fixture(scope="module")
def v_table(dynamodb_client):
    """HASH \U0001d11e S, RANGE a S: a member name outside the Basic Multilingual Plane."""
    name = _two_member_table(dynamodb_client, "scanreqv", "\U0001d11e", "a")
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

    @pytest.mark.parametrize("key, expected", [r[1:] for r in TWO_MEMBER_FAULTS],
                             ids=[r[0] for r in TWO_MEMBER_FAULTS])
    def test_two_faulty_members_answer_the_first_in_the_fitted_order(
            self, dynamodb_client, table, key, expected):
        fields = '"ExclusiveStartKey": ' + key
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected

    @pytest.mark.parametrize("key, either", [r[1:] for r in TWO_VALUE_FAULTS],
                             ids=[r[0] for r in TWO_VALUE_FAULTS])
    def test_two_value_faults_answer_one_of_the_two(self, dynamodb_client, table, key, either):
        fields = '"ExclusiveStartKey": ' + key
        message = _validation_message(*_scan(dynamodb_client, table, fields))
        assert message in either, message

    @pytest.mark.parametrize("fixture, key, expected", [r[1:] for r in MEMBER_ORDER_ROWS],
                             ids=[r[0] for r in MEMBER_ORDER_ROWS])
    def test_the_member_order_on_other_tables(self, dynamodb_client, request, fixture, key,
                                              expected):
        name = request.getfixturevalue(fixture)
        fields = '"ExclusiveStartKey": ' + key
        assert _validation_message(*_scan(dynamodb_client, name, fields)) == expected

    def test_the_member_order_on_a_gsi_key_of_three_members(self, dynamodb_client, table):
        fields = ('"IndexName": "gsi", "ExclusiveStartKey": {"gpk": {}, "pk": {"N": "bob"}, '
                  '"sk": {"S": "x"}}')
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == EMPTY_KEY_VALUE

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


class TestTwoOrders:
    """Representative rows of each order (measured 2026-10-31 and 2026-11-05)."""

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in TWO_ORDERS],
                             ids=[r[0] for r in TWO_ORDERS])
    def test_rows(self, dynamodb_client, table, fields, expected):
        message = _validation_message(*_scan(dynamodb_client, table, fields))
        if isinstance(expected, tuple):
            # The text predates this branch (#722, #838, #839): the check's
            # place is asserted.
            assert message.startswith(expected[1]), message
        else:
            assert message == expected

    def test_an_unreadable_expression_value_on_a_missing_table(self, dynamodb_client):
        # Measured 2026-11-05: ahead of ResourceNotFoundException.
        fields = '"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {}}'
        status, payload = _scan(dynamodb_client, unique_name("nosuch"), fields)
        assert payload.get("message") == EAV_EMPTY_A, payload

    def test_bob_in_the_key_of_an_n_table(self, dynamodb_client, n_table):
        # Measured 2026-11-05.
        fields = '"ExclusiveStartKey": {"pk": {"N": "bob"}}'
        assert _validation_message(*_scan(dynamodb_client, n_table, fields)) == NUMERIC_BOB


# The two ScanFilter number rows: the measured answers are PR #507's check
# (#564 item 3, measured 2026-11-05), so they are expected to fail here until
# #507 lands.
SCAN_FILTER_NUMBER_ROWS = {"sfR-N-bob", "sfR-N-bob-and-key-bob"}


def _rows_2026_11_05_params():
    return [
        pytest.param(*r[1:], marks=pytest.mark.xfail(reason="#507"))
        if r[0] in SCAN_FILTER_NUMBER_ROWS else r[1:]
        for r in ROWS_2026_11_05
    ]


class TestRowsOf20261105:
    """The rows measured 2026-11-05 at 05:31, 06:20 and 07:38 UTC."""

    @pytest.mark.parametrize("fixture, fields, expected", _rows_2026_11_05_params(),
                             ids=[r[0] for r in ROWS_2026_11_05])
    def test_rows(self, dynamodb_client, request, fixture, fields, expected):
        name = request.getfixturevalue(fixture) if fixture else unique_name("nosuch")
        message = _validation_message(*_scan(dynamodb_client, name, fields))
        if isinstance(expected, tuple) and expected[0] == "startswith":
            # The text predates this branch (#838): the check's place is asserted.
            assert message.startswith(expected[1]), message
        elif isinstance(expected, tuple):
            assert message in expected[1:], message
        else:
            assert message == expected


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
