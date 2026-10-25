# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The 300-operator cap does not apply to the legacy condition parameters.

ScanFilter, QueryFilter and Expected are not expressions, and Amazon DynamoDB
accepts 160 conditions in each: a Scan with a 160-condition ScanFilter and a
Query with a 160-condition QueryFilter return normally, and a PutItem with a
160-condition Expected is evaluated (measured 2026-10-25 in us-east-1, each
request twice). ExtendDB desugars these parameters into expressions, which
must not be counted.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

CONDITIONS = 160


def _eq_conditions(n: int) -> dict:
    return {
        f"a{i}": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}
        for i in range(n)
    }


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "v": {"S": "x"}},
        )
        yield name


def test_scan_filter_with_160_conditions_is_not_counted(dynamodb_client, table):
    # Measured 2026-10-25: OK, Count 0, ScannedCount 1.
    out = dynamodb_client.scan(
        TableName=table, Limit=1, ScanFilter=_eq_conditions(CONDITIONS)
    )
    assert out["Count"] == 0
    assert out["ScannedCount"] == 1


def test_query_filter_with_160_conditions_is_not_counted(dynamodb_client, table):
    # Measured 2026-10-25: OK, Count 0.
    out = dynamodb_client.query(
        TableName=table,
        Limit=1,
        KeyConditions={
            "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "p1"}]}
        },
        QueryFilter=_eq_conditions(CONDITIONS),
    )
    assert out["Count"] == 0


def test_expected_with_160_conditions_is_evaluated(dynamodb_client, table):
    # Measured 2026-10-25: ConditionalCheckFailedException, the item has none of the
    # compared attributes.
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.put_item(
            TableName=table,
            Item={"pk": {"S": "absent"}, "sk": {"S": "absent"}},
            Expected={f"a{i}": {"Value": {"S": "x"}} for i in range(CONDITIONS)},
        )
    assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
