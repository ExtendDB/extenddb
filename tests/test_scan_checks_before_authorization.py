# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A malformed Scan from a principal that may not Scan the table answers the
request's ValidationException, not AccessDeniedException. The principal is a
user created through the extenddb management API, so this file is skipped
against the service.

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

# Measured 2026-11-19 16:34 UTC with a role allowed only dynamodb:DescribeTable
# on the table, each body twice: the service answers each with the text below,
# and the valid body with AccessDeniedException.
ROWS = [
    ("N bob", NB_FILTER, NUMERIC + "bob"),
    ("Limit 0", {"Limit": 0}, ONE_ERROR + LIMIT_RANGE),
    ("ReturnConsumedCapacity", {"ReturnConsumedCapacity": "NOPE"}, RCC_ENUM),
    ("mix", {"ScanFilter": SCAN_FILTER, "FilterExpression": "attribute_exists(a)"}, MIX),
    ("Select", {"Select": "NOPE"}, SELECT_ENUM),
    ("names", {"ExpressionAttributeNames": {"a": "b"}, "FilterExpression": "attribute_exists(a)"}, EAN_KEY),
    ("unknown type descriptor", {"FilterExpression": "a = :x", "ExpressionAttributeValues": {":x": X1}},
     INVALID_VALUE + EMPTY_AV + " for key :x"),
    ("null beside Select", {**NULL_BESIDE_SELECT, "ExpressionAttributeValues": {":v": None}}, SELECT_BAD),
    ("exponent above int32",
     {"FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "1e9999999999"}}}, OVERFLOW),
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
            for row, body, message in ROWS:
                status, payload = _raw_scan(describe_only, {"TableName": name, **body})
                got = (status, payload.get("__type", "").rsplit("#", 1)[-1],
                       payload.get("message", payload.get("Message")))
                if got != (400, "ValidationException", message):
                    failures.append(f"{row}: {got}")
            status, payload = _raw_scan(describe_only, {"TableName": name, **OK_FILTER})
            if (status, payload.get("__type", "").rsplit("#", 1)[-1]) != (400, "AccessDeniedException"):
                failures.append(f"valid body: {status} {payload}")
        assert not failures, "\n".join(failures)
