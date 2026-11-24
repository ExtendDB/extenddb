# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A malformed Scan from a principal that may not Scan the table answers the
request's error, not AccessDeniedException. The principal is a user created
through the extenddb management API, so this file is skipped against the
service.

Run:
  EXTENDDB_TEST_ENDPOINT=https://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_scan_checks_before_authorization.py -v
"""

from __future__ import annotations

from conftest import scoped_table
from test_query_leading_keys_values import ManagementPrincipals
from test_scan_number_values import (
    EAN_KEY,
    EMPTY_AV,
    INVALID_VALUE,
    LIMIT_RANGE,
    MIX,
    NB_FILTER,
    NULL_BESIDE_SELECT,
    NUMERIC,
    OK_FILTER,
    ONE_ERROR,
    OVERFLOW,
    RCC_ENUM,
    SCAN_FILTER,
    SELECT_BAD,
    SELECT_ENUM,
    X1,
    _raw_scan,
)

VALIDATION = "ValidationException"
SERIALIZATION = "SerializationException"

# (row, body, error code, message). Measured with a role allowed only
# dynamodb:DescribeTable, each body twice: the first nine rows on 2026-11-19 at
# 16:34 UTC on a table of the role's account, the rows from "Limit of the wrong
# JSON type" on 2026-11-24 at 08:28, 08:33 and 09:59 UTC on a table that does
# not exist. The service answers each with the code and text below, except
# where a row's comment gives the service's text, and the valid body with
# AccessDeniedException.
ROWS = [
    ("N bob", NB_FILTER, VALIDATION, NUMERIC + "bob"),
    ("Limit 0", {"Limit": 0}, VALIDATION, ONE_ERROR + LIMIT_RANGE),
    ("ReturnConsumedCapacity", {"ReturnConsumedCapacity": "NOPE"}, VALIDATION, RCC_ENUM),
    ("mix", {"ScanFilter": SCAN_FILTER, "FilterExpression": "attribute_exists(a)"}, VALIDATION, MIX),
    ("Select", {"Select": "NOPE"}, VALIDATION, SELECT_ENUM),
    ("names", {"ExpressionAttributeNames": {"a": "b"}, "FilterExpression": "attribute_exists(a)"},
     VALIDATION, EAN_KEY),
    ("unknown type descriptor", {"FilterExpression": "a = :x", "ExpressionAttributeValues": {":x": X1}},
     VALIDATION, INVALID_VALUE + EMPTY_AV + " for key :x"),
    ("null beside Select", {**NULL_BESIDE_SELECT, "ExpressionAttributeValues": {":v": None}},
     VALIDATION, SELECT_BAD),
    ("exponent above int32",
     {"FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "1e9999999999"}}},
     VALIDATION, OVERFLOW),
    # A float Limit puts the request on the service's older validation path,
    # where an empty or duplicate set is refused with that path's wrapped
    # text. Measured 2026-11-24 from a principal allowed only DescribeTable
    # (the duplicate NS, in the review of #850) and at 05:41 UTC on a table
    # that does not exist (both texts).
    ("duplicate NS on the older path",
     {"Limit": 1.5, "FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {"NS": ["1", "1"]}}},
     VALIDATION, INVALID_VALUE + "Input collection contains duplicates for key :a"),
    ("empty NS on the older path",
     {"Limit": 1.5, "FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {"NS": []}}},
     VALIDATION,
     INVALID_VALUE + "One or more parameter values were invalid: An number set  may not be empty for key :a"),
    ("Limit of the wrong JSON type", {"Limit": "5"}, SERIALIZATION, "STRING_VALUE cannot be converted to Integer"),
    # The service answers "NUMBER_VALUE cannot be converted to String"; the
    # text here is #1012's.
    ("Select of the wrong JSON type", {"Select": 5}, SERIALIZATION,
     "Start of structure or map found where not expected: invalid type: integer `5`, expected a string"),
    ("names with no expression", {"ExpressionAttributeNames": {"#a": "a"}},
     VALIDATION, "ExpressionAttributeNames can only be specified when using expressions"),
    # On the newer path the service's text has no suffix (#722), so the row
    # takes the older path, where extenddb and the service agree.
    ("values with no expression on the older path",
     {"Limit": 1.5, "ExpressionAttributeValues": {":a": {"S": "x"}}}, VALIDATION,
     "ExpressionAttributeValues can only be specified when using expressions: FilterExpression is null"),
    ("ConditionalOperator without a ScanFilter", {"ConditionalOperator": "AND"},
     VALIDATION, "ConditionalOperator cannot be used without Filter or Expected"),
    # The service answers "One or more parameter values were invalid: Invalid
    # number of argument(s) for the EQ ComparisonOperator"; the text here is
    # #838's.
    ("ScanFilter value count", {"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": []}}},
     VALIDATION, "Invalid number of attribute values for comparison"),
    ("empty ScanFilter list value on the older path",
     {"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{}]}}}, VALIDATION, EMPTY_AV),
    ("empty key value on the older path", {"ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}},
     VALIDATION, "The provided starting key is invalid: " + EMPTY_AV),
]


class TestChecksBeforeAuthorization(ManagementPrincipals):
    """extenddb only: the principal needs a user created through the
    management API."""

    def test_a_principal_that_may_not_scan_gets_the_validation_exception(self):
        owner = self._client({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
        })
        with scoped_table(owner) as name:
            arn = f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/{name}"
            describe_only = self._client({
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "dynamodb:DescribeTable", "Resource": arn}],
            })
            failures = []
            for row, body, code, message in ROWS:
                status, payload = _raw_scan(describe_only, {"TableName": name, **body})
                got = (status, payload.get("__type", "").rsplit("#", 1)[-1],
                       payload.get("message", payload.get("Message")))
                if got != (400, code, message):
                    failures.append(f"{row}: {got}")
            status, payload = _raw_scan(describe_only, {"TableName": name, **OK_FILTER})
            if (status, payload.get("__type", "").rsplit("#", 1)[-1]) != (400, "AccessDeniedException"):
                failures.append(f"valid body: {status} {payload}")
        assert not failures, "\n".join(failures)
