# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Scan request validation: the rows measured on 2026-11-05 and 2026-11-23,
each sent as raw signed JSON and compared with the service's answer byte
for byte. The check orders are in test_scan_request_validation.py."""

from __future__ import annotations

import json

import pytest

from helpers import unique_name
from scan_request_support import (
    MIX, NUMERIC_BOB, RCC_ENUM, NESTING, LIMIT, SELECT_ENUM, KEY_PREFIX, EMPTY_KEY_VALUE, INVALID,
    NS_EMPTY, SS_EMPTY, BS_EMPTY, NUMERIC_FRED, EAV_NO_EXPRESSION, BOB_KEY, EAV_VALUE, EAV_EMPTY_E,
    EAV_NESTING_D, EAN_NO_EXPRESSION, CONDOP, EMPTY_KEY, FILTER_P, NEST33_N, EMPTY_VALUE_BARE,
    MORE_THAN_ONE_BARE,
    EAV_EMPTY_B, EAV_EMPTY_P, EAV_NUMBER_A, EAV_NUMBER_N, SF_EMPTY, SF_BOB, NONASCII_KEY_A_BOB,
    NONASCII_KEY_E_EMPTY, nest, _scan, _validation_message, table, t4_table, u_table, v_table,
)

# Measured 2026-11-05 at 05:31, 06:20 and 07:38 UTC: (id, table
# fixture or None for a missing table, the fields, the expected text). A text
# given as ("startswith", prefix) asserts the check's place where the text
# predates this branch (#838); ("either", a, b) accepts both answers where the
# service's two sends differed.
ROWS_2026_11_05 = [
    # 7.1: a malformed number in a ScanFilter list, ahead of the key's number.
    # The check is #564 item 3, which PR #507 makes (KNOWN_ROWS).
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
    # In order R an empty set beside a value nested 33 levels answered the set
    # text in 2 of 2 sends on 2026-11-24 (beside 32 levels, the nesting text in
    # 12 of 14 sends in the review of 2026-11-12). In order U the empty leaf
    # inside 32 levels is named (2026-11-05 at 06:20 UTC), and inside 33 levels
    # the entry's nesting (2026-11-24 at 08:46 UTC).
    ("eavR-NSe-and-nest", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + NEST33_N + ', ":e": {"NS": []}}', ("either", NS_EMPTY, NESTING)),
    ("eavU-nest32-empty-leaf", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + json.dumps(nest({}, 32)) + '}', EAV_EMPTY_E.replace(":e", ":d")),
    ("eavU-nest33-empty-leaf", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + json.dumps(nest({}, 33)) + '}', EAV_NESTING_D),
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
    # The service's text (2026-11-23, 2 sends); the value count text here is
    # #838's (KNOWN_ROWS).
    ("selR-arity-select-bad", "table", '"ScanFilter": {"a": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": []}}, "Select": "BAD"',
     INVALID + "Invalid number of argument(s) for the EQ ComparisonOperator"),
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
    # Order R, two faulty entries: the numbers, then the set faults, each in
    # placeholder order. The service named either number (fred in 4 of 6
    # sends, bob in 2, 2026-11-23) and either set fault (the SS text, then the
    # NS text, 2026-11-05).
    ("eavR-two-numbers", "table", '"FilterExpression": "a = :a", "ExpressionAttributeValues": '
     '{":n": {"N": "fred"}, ":a": {"N": "bob"}}', ("either", NUMERIC_FRED, NUMERIC_BOB)),
    ("eavR-two-set-faults", "table", '"ExpressionAttributeValues": {":n": {"NS": []}, ":a": {"SS": []}}',
     ("either", SS_EMPTY, NS_EMPTY)),
    # Order R, beside an entry nested 33 levels (2026-11-24 at 08:46 and 08:59
    # UTC): a malformed number inside an entry with a set fault answered the
    # number in one send and the nesting text in the other, as did a number
    # ahead of a set fault in one L, and the SS duplicates answered the
    # duplicates text and the nesting text (beside 32 levels, the nesting text
    # in 3 of 6 sends on 2026-11-12).
    ("eavR-NSbobdup-and-nest", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + NEST33_N + ', ":a": {"NS": ["bob", "1", "1"]}}',
     ("either", NUMERIC_BOB, NESTING)),
    ("eavR-L-bob-then-NSe-and-nest", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + NEST33_N + ', ":l": {"L": [{"N": "bob"}, {"NS": []}]}}',
     ("either", NUMERIC_BOB, NESTING)),
    ("eavR-SSdup-and-nest", "table", FILTER_P + ', "ExpressionAttributeValues": '
     '{":p": {"S": "x"}, ":d": ' + NEST33_N + ', ":s": {"SS": ["a", "a"]}}',
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


# The older-form clause texts for the rows of 2026-11-23 below.
OLDER_SELECT = ("Value 'BAD' at 'select' failed to satisfy constraint: Member must satisfy enum "
                "value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]")
OLDER_RCC = ("Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must "
             "satisfy enum value set: [INDEXES, TOTAL, NONE]")
OLDER_LIMIT = ("Value '0' at 'limit' failed to satisfy constraint: Member must have value greater "
               "than or equal to 1")
OLDER_ATG = ("Value '[]' at 'attributesToGet' failed to satisfy constraint: Member must have "
             "length greater than or equal to 1")
OLDER_TOTAL = ("Value '1000001' at 'totalSegments' failed to satisfy constraint: Member must have "
               "value less than or equal to 1000000")
OLDER_SEGMENT = ("Value '-1' at 'segment' failed to satisfy constraint: Member must have value "
                 "greater than or equal to 0")

# Names that share bucket 10 of a sixteen-bucket HashMap with :dt.
BUCKET_10 = [":aa", ":bb", ":cc", ":dd", ":ee", ":ff", ":gg", ":hh", ":ii"]


def _entries(names: list) -> str:
    """An ExpressionAttributeValues object in the order of ``names``, :aa and
    :dt empty, the others {"S": "x"}."""
    return "{" + ", ".join(f'"{n}": {{}}' if n in (":aa", ":dt") else f'"{n}": {{"S": "x"}}'
                           for n in names) + "}"


# Measured 2026-11-23 at 14:03 and 21:49 UTC on a table that does not exist
# (us-east-1, each body twice): (id, fields, expected).
ROWS_2026_11_23 = [
    # The enum clauses inside the older-form message, in the service's order,
    # and the AttributesToGet length clause last.
    ("d1-select-rcc-unreadable", '"Select": "BAD", "ReturnConsumedCapacity": "BAD", '
     '"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {}}',
     "2 validation errors detected: " + OLDER_SELECT + "; " + OLDER_RCC),
    ("d1-limit-float-both-enums", '"Limit": 1.5, "Select": "BAD", "ReturnConsumedCapacity": "BAD"',
     "2 validation errors detected: " + OLDER_SELECT + "; " + OLDER_RCC),
    ("A1-atg-and-limit-older", '"AttributesToGet": [], "Limit": 0.0',
     "2 validation errors detected: " + OLDER_LIMIT + "; " + OLDER_ATG),
    ("A2-six-older-clauses", '"AttributesToGet": [], "Limit": 0.0, "Segment": -1, '
     '"TotalSegments": 1000001, "Select": "BAD", "ReturnConsumedCapacity": "BAD"',
     "6 validation errors detected: " + "; ".join(
         [OLDER_SELECT, OLDER_TOTAL, OLDER_RCC, OLDER_SEGMENT, OLDER_LIMIT, OLDER_ATG])),
    ("A3-atg-and-select-beside-empty", '"AttributesToGet": [], "Select": "BAD", '
     '"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {}}',
     "2 validation errors detected: " + OLDER_SELECT + "; " + OLDER_ATG),
    # One ScanFilter list in element order over shape, number and set faults.
    ("d2-sf-empty-then-NSe", '"ScanFilter": {"a": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": [{}, {"NS": []}]}}', EMPTY_VALUE_BARE),
    ("d2-sf-bob-then-empty", '"ScanFilter": {"a": {"ComparisonOperator": "EQ", '
     '"AttributeValueList": [{"N": "bob"}, {}]}}', NUMERIC_BOB),
    # One key member in element order.
    ("d8-key-L-bob-empty", '"ExclusiveStartKey": {"pk": {"L": [{"N": "bob"}, {}]}, '
     '"sk": {"S": "s"}}', NUMERIC_BOB),
    # Eight, nine and ten entries in bucket 10 of sixteen, :aa and :dt empty:
    # the ninth name in one bucket doubles the table, and :dt comes first.
    ("H8-eight-names-one-bucket", '"FilterExpression": "a = :bb", "ExpressionAttributeValues": '
     + _entries(BUCKET_10[:7] + [":dt"]), EAV_VALUE + EMPTY_VALUE_BARE + " for key :aa"),
    ("H9-nine-names-one-bucket", '"FilterExpression": "a = :bb", "ExpressionAttributeValues": '
     + _entries(BUCKET_10[:8] + [":dt"]), EAV_VALUE + EMPTY_VALUE_BARE + " for key :dt"),
    ("H10-ten-names-one-bucket", '"FilterExpression": "a = :bb", "ExpressionAttributeValues": '
     + _entries(BUCKET_10 + [":dt"]), EAV_VALUE + EMPTY_VALUE_BARE + " for key :dt"),
    # The key's faults ahead of an empty name.
    ("E1-key-empty-and-empty-name", '"ExclusiveStartKey": {"pk": {}}, "FilterExpression": '
     '"#a = :x", "ExpressionAttributeNames": {"#a": ""}, "ExpressionAttributeValues": '
     '{":x": {"S": "x"}}', EMPTY_KEY_VALUE),
]

# Measured 2026-11-23 at 14:03 UTC on a table that does not exist (us-east-1,
# each body twice): a member of the wrong JSON type behind a value fault in the
# same map or list answers SerializationException. (id, fields, the message,
# or None where the deserializer's text differs from the service's
# "NUMBER_VALUE cannot be converted to String").
SHAPE_ROWS_2026_11_23 = [
    ("d3-eav-L-S5-empty", '"FilterExpression": "a = :a", "ExpressionAttributeValues": '
     '{":a": {"L": [{"S": 5}, {}]}}', None),
    ("d4-key-NSe-S5", '"ExclusiveStartKey": {"pk": {"NS": []}, "sk": {"S": 5}}', None),
    ("d5-atg-NSe-limit-string", '"AttributesToGet": [], "ExclusiveStartKey": '
     '{"pk": {"NS": []}}, "Limit": "5"', "STRING_VALUE cannot be converted to Integer"),
]

# The rows whose measured answers another issue's check gives, expected to
# fail here until it lands: the two ScanFilter number rows are PR #507's check
# (#564 item 3, measured 2026-11-05), and the value count text of a ScanFilter
# condition is #838's.
KNOWN_ROWS = {"sfR-N-bob": "#507", "sfR-N-bob-and-key-bob": "#507",
              "selR-arity-select-bad": "#838"}


def _rows_2026_11_05_params():
    return [
        pytest.param(*r[1:], marks=pytest.mark.xfail(reason=KNOWN_ROWS[r[0]]))
        if r[0] in KNOWN_ROWS else r[1:]
        for r in ROWS_2026_11_05
    ]


def _assert_row(client, table: str, fields: str, expected) -> None:
    """``fields`` answers ``expected``: a text, ("startswith", prefix) or
    ("either", a, b)."""
    message = _validation_message(*_scan(client, table, fields))
    if isinstance(expected, tuple) and expected[0] == "startswith":
        # The text predates this branch (#838): the check's place is asserted.
        assert message.startswith(expected[1]), message
    elif isinstance(expected, tuple):
        assert message in expected[1:], message
    else:
        assert message == expected


class TestRowsOf20261105:
    """The rows measured 2026-11-05 at 05:31, 06:20 and 07:38 UTC."""

    @pytest.mark.parametrize("fixture, fields, expected", _rows_2026_11_05_params(),
                             ids=[r[0] for r in ROWS_2026_11_05])
    def test_rows(self, dynamodb_client, request, fixture, fields, expected):
        name = request.getfixturevalue(fixture) if fixture else unique_name("nosuch")
        _assert_row(dynamodb_client, name, fields, expected)


class TestRowsOf20261123:
    """The rows measured 2026-11-23, on a table that does not exist."""

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in ROWS_2026_11_23],
                             ids=[r[0] for r in ROWS_2026_11_23])
    def test_rows(self, dynamodb_client, fields, expected):
        _assert_row(dynamodb_client, unique_name("nosuch"), fields, expected)

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in SHAPE_ROWS_2026_11_23],
                             ids=[r[0] for r in SHAPE_ROWS_2026_11_23])
    def test_shape_rows(self, dynamodb_client, fields, expected):
        status, payload = _scan(dynamodb_client, unique_name("nosuch"), fields)
        assert status == 400, payload
        assert payload.get("__type", "").endswith("SerializationException"), payload
        if expected is not None:
            assert payload.get("message", payload.get("Message")) == expected
