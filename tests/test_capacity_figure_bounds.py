# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""How CreateTable, UpdateTable and RestoreTableFromBackup read and bound a
capacity figure, as Amazon DynamoDB answered in us-east-1 in November 2026.
The SDK cannot send these figures, so the bodies go out as raw signed JSON.
"""

from __future__ import annotations

import os
import re
import uuid

import boto3
import pytest

from conftest import wait_for_active
from raw_signed_request import signed_post

MAX = 9223372036854775807
GUIDE = (
    "Refer to the Amazon DynamoDB Developer Guide for current limits and how "
    "to request higher limits."
)
INVALID = "ValidationException"
LIMIT = "LimitExceededException"


def bounds(value: int, unit: str = "WriteCapacityUnits") -> str:
    return re.escape(f"Given value {value} for {unit} is out of bounds")


MINIMUM = r"(?!.*out of bounds).*greater than or equal to 1.*"
PER_INDEX = re.escape(
    f"The requested WriteCapacityUnits for index by-label, {MAX}, is above "
    "the per index maximum for the account in {region}. Per table maximum: "
    "{write_max}. " + GUIDE
)
PER_TABLE = re.escape(
    "The requested WriteCapacityUnits, 1000000000000, is above the per table "
    "maximum for the account in {region}. Per table maximum: {write_max}. "
    + GUIDE
)
ACCOUNT = (
    re.escape(
        "This request would have caused the WriteCapacityUnits limit to be "
        "exceeded for the account in {region}. Current WriteCapacityUnits "
        "reserved by the account: "
    )
    + r"\d+\. Limit: \d+\. "
    + re.escape("Requested: 1000000000006. " + GUIDE)
)

# (operation, table read, table write, index write, error, text pattern).
# An index is created beside the table on CreateTable when its write figure
# is given. UpdateTable goes to a table provisioned at 5 and 7, and
# RestoreTableFromBackup to a backup that does not exist. A row with no error
# creates the table.
ROWS = [
    ("UpdateTable", 5, MAX + 1, None, INVALID, bounds(MAX)),
    ("CreateTable", 5, MAX + 1, None, INVALID, bounds(MAX)),
    ("CreateTable", 5, 5, MAX + 1, LIMIT, PER_INDEX),
    ("CreateTable", MAX + 1, 5, None, INVALID, bounds(MAX, "ReadCapacityUnits")),
    ("CreateTable", 10**12 + 1, 5, None, INVALID, bounds(10**12 + 1, "ReadCapacityUnits")),
    ("CreateTable", MAX + 1, 0, None, INVALID, MINIMUM),
    ("CreateTable", 5, -MAX - 2, None, INVALID, MINIMUM),
    ("CreateTable", 5, 5, 10**12 + 1, LIMIT, ACCOUNT),
    ("UpdateTable", 5, 10**12, None, LIMIT, PER_TABLE),
    ("UpdateTable", 5, 10**12 + 1, None, INVALID, bounds(10**12 + 1)),
    ("CreateTable", 5, 5.5, None, None, None),
    ("RestoreTableFromBackup", 10**12 + 1, 5, None, "BackupNotFoundException",
     r"Backup not found: arn:\S+"),
    # Measured on 2026-11-25: with both figures out of bounds, the read units.
    ("UpdateTable", MAX + 1, MAX + 1, None, INVALID, bounds(MAX, "ReadCapacityUnits")),
]


@pytest.fixture(scope="module")
def target(dynamodb_client):
    """Post a raw body to the target, and the account's write maximum."""
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    region = dynamodb_client.meta.region_name
    endpoint = endpoint or f"https://dynamodb.{region}.amazonaws.com/"
    creds = boto3.Session().get_credentials().get_frozen_credentials()

    def post(operation: str, body: dict):
        return signed_post(
            endpoint, creds.access_key, creds.secret_key, region, operation,
            body, creds.token,
        )

    limits = dynamodb_client.describe_limits()
    return post, region, limits["TableMaxWriteCapacityUnits"]


@pytest.fixture(scope="module")
def provisioned_table(dynamodb_client):
    name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
    dynamodb_client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PROVISIONED",
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 7},
    )
    wait_for_active(dynamodb_client, name)
    yield name
    dynamodb_client.delete_table(TableName=name)


@pytest.mark.parametrize("operation,read,write,index_write,error,text", ROWS)
def test_capacity_figure(
    dynamodb_client, target, provisioned_table, operation, read, write,
    index_write, error, text,
):
    post, region, write_max = target
    throughput = {"ReadCapacityUnits": read, "WriteCapacityUnits": write}
    name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
    if operation == "UpdateTable":
        body = {"TableName": provisioned_table, "ProvisionedThroughput": throughput}
    elif operation == "RestoreTableFromBackup":
        arn = dynamodb_client.describe_table(TableName=provisioned_table)["Table"]["TableArn"]
        body = {
            "TargetTableName": name,
            "BackupArn": f"{arn.rsplit('/', 1)[0]}/{name}/backup/01763424000000-1029abcd",
            "BillingModeOverride": "PROVISIONED",
            "ProvisionedThroughputOverride": throughput,
        }
    else:
        attrs = [{"AttributeName": "pk", "AttributeType": "S"}]
        body = {
            "TableName": name,
            "BillingMode": "PROVISIONED",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": attrs,
            "ProvisionedThroughput": throughput,
        }
        if index_write is not None:
            attrs.append({"AttributeName": "label", "AttributeType": "S"})
            body["GlobalSecondaryIndexes"] = [{
                "IndexName": "by-label",
                "KeySchema": [{"AttributeName": "label", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5, "WriteCapacityUnits": index_write,
                },
            }]
    resp = post(operation, body)
    if error is None:
        assert resp.status_code == 200, resp.text
        wait_for_active(dynamodb_client, name)
        dynamodb_client.delete_table(TableName=name)
        created = resp.json()["TableDescription"]["ProvisionedThroughput"]
        assert (created["ReadCapacityUnits"], created["WriteCapacityUnits"]) == (read, int(write))
        return
    assert resp.status_code == 400, resp.text
    answer = resp.json()
    assert answer["__type"].endswith("#" + error), answer
    message = answer.get("message", answer.get("Message", ""))
    pattern = text.replace(r"\{region\}", re.escape(region)).replace(
        r"\{write_max\}", str(write_max)
    )
    assert re.fullmatch(pattern, message), message
    if operation == "UpdateTable":
        table = dynamodb_client.describe_table(TableName=provisioned_table)["Table"]
        assert table["TableStatus"] == "ACTIVE"
        throughput = table["ProvisionedThroughput"]
        assert (throughput["ReadCapacityUnits"], throughput["WriteCapacityUnits"]) == (5, 7)
