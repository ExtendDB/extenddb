# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""MongoDB-specific backup/restore coverage.

The table operations are exercised through the DynamoDB API.  These tests are
gated by the MongoDB runner because the backup metadata being checked is part
of the MongoDB implementation; other backends retain the shared API tests.
"""

from __future__ import annotations

import os
import time

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted


@pytest.fixture()
def mongodb_container() -> str:
    container = os.environ.get("EXTENDDB_TEST_MONGODB_CONTAINER", "").strip()
    if not container:
        pytest.skip("requires devtools/run-mongodb-tests")
    return container


def _create_backup(client, table_name: str, backup_name: str) -> str:
    deadline = time.monotonic() + 30
    while True:
        try:
            response = client.create_backup(
                TableName=table_name,
                BackupName=backup_name,
            )
            return response["BackupDetails"]["BackupArn"]
        except ClientError as exc:
            if (
                exc.response["Error"]["Code"]
                != "ContinuousBackupsUnavailableException"
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(0.2)


def _cleanup(client, *table_names: str) -> None:
    for table_name in table_names:
        try:
            client.delete_table(TableName=table_name)
        except client.exceptions.ResourceNotFoundException:
            continue
        wait_for_deleted(client, table_name)


def test_restore_pay_per_request_preserves_on_demand_billing(
    dynamodb_client, unique_table_name, mongodb_container
):
    source = unique_table_name
    restored = f"{source}-restored"
    dynamodb_client.create_table(
        TableName=source,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, source)

    backup_arn = None
    try:
        dynamodb_client.put_item(
            TableName=source,
            Item={"pk": {"S": "item-1"}, "value": {"S": "value"}},
        )
        backup_arn = _create_backup(dynamodb_client, source, f"{source}-backup")
        dynamodb_client.restore_table_from_backup(
            TargetTableName=restored,
            BackupArn=backup_arn,
        )
        wait_for_active(dynamodb_client, restored)

        throughput = dynamodb_client.describe_table(TableName=restored)["Table"][
            "ProvisionedThroughput"
        ]
        assert throughput["ReadCapacityUnits"] == 0
        assert throughput["WriteCapacityUnits"] == 0
    finally:
        _cleanup(dynamodb_client, restored, source)
        if backup_arn:
            dynamodb_client.delete_backup(BackupArn=backup_arn)


def test_restore_preserves_provisioned_throughput_and_secondary_indexes(
    dynamodb_client, unique_table_name, mongodb_container
):
    source = unique_table_name
    restored = f"{source}-restored"
    dynamodb_client.create_table(
        TableName=source,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
            {"AttributeName": "gsi_sk", "AttributeType": "S"},
            {"AttributeName": "lsi_sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 7, "WriteCapacityUnits": 9},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "restore_gsi",
                "KeySchema": [
                    {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi_sk", "KeyType": "RANGE"},
                ],
                "Projection": {
                    "ProjectionType": "INCLUDE",
                    "NonKeyAttributes": ["gsi_extra"],
                },
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 11,
                    "WriteCapacityUnits": 13,
                },
            }
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "restore_lsi",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsi_sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    wait_for_active(dynamodb_client, source)

    backup_arn = None
    try:
        dynamodb_client.put_item(
            TableName=source,
            Item={
                "pk": {"S": "partition-1"},
                "sk": {"S": "sort-1"},
                "gsi_pk": {"S": "gsi-partition-1"},
                "gsi_sk": {"S": "gsi-sort-1"},
                "gsi_extra": {"S": "included-value"},
                "lsi_sk": {"S": "lsi-sort-1"},
            },
        )
        backup_arn = _create_backup(dynamodb_client, source, f"{source}-backup")
        dynamodb_client.restore_table_from_backup(
            TargetTableName=restored,
            BackupArn=backup_arn,
        )
        wait_for_active(dynamodb_client, restored)

        table = dynamodb_client.describe_table(TableName=restored)["Table"]
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 7
        assert table["ProvisionedThroughput"]["WriteCapacityUnits"] == 9

        gsi = next(
            index
            for index in table["GlobalSecondaryIndexes"]
            if index["IndexName"] == "restore_gsi"
        )
        assert gsi["KeySchema"] == [
            {"AttributeName": "gsi_pk", "KeyType": "HASH"},
            {"AttributeName": "gsi_sk", "KeyType": "RANGE"},
        ]
        assert gsi["Projection"] == {
            "ProjectionType": "INCLUDE",
            "NonKeyAttributes": ["gsi_extra"],
        }
        assert gsi["ProvisionedThroughput"]["ReadCapacityUnits"] == 11
        assert gsi["ProvisionedThroughput"]["WriteCapacityUnits"] == 13

        lsi = next(
            index
            for index in table["LocalSecondaryIndexes"]
            if index["IndexName"] == "restore_lsi"
        )
        assert lsi["KeySchema"] == [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "lsi_sk", "KeyType": "RANGE"},
        ]
        assert lsi["Projection"] == {"ProjectionType": "ALL"}

        query = dynamodb_client.query(
            TableName=restored,
            IndexName="restore_gsi",
            KeyConditionExpression="gsi_pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "gsi-partition-1"}},
        )
        assert query["Count"] == 1
        assert query["Items"][0]["gsi_extra"] == {"S": "included-value"}
    finally:
        _cleanup(dynamodb_client, restored, source)
        if backup_arn:
            dynamodb_client.delete_backup(BackupArn=backup_arn)
