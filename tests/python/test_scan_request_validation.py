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
2026-11-05, each request twice; MEASURED-scan-r3 section 6). A request that
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

# Measured 2026-10-31 and 2026-11-05: faults in two members of one key, one of
# them a shape fault. The service names the first faulty member in the fitted
# member order (MEASURED-scan-r3 section 6.3: sk ahead of pk and of z in both
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

# Measured 2026-11-05 (R5, MEASURED-scan-r3 section 6.3) on tables with other
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

# Measured 2026-10-31 (B4-7) and 2026-11-05 (R5, MEASURED-scan-r3 section 6):
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
    # rows of R3 and the nest-*-key-empty rows of R5).
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
    # the key's shape fault (RS-f Minor-1; arity-key-empty-and-eav-noexpr).
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
