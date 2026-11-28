# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""RestoreTableFromBackup refuses an override that conflicts with the indexes
or the billing mode of the backup with ValidationException, in DynamoDB's
words.

Each row is DynamoDB's answer, measured in us-east-1 on 13, 14 and 17 Nov
2026 with each request restored to a free table name at least twice. Where a
request holds two conflicts, the answer gives the order: an on-demand maximum
is refused ahead of the provisioned throughput beside it, and ahead of a
missing index throughput unless BillingModeOverride is PROVISIONED. A refused
restore creates no table. An on-demand maximum that sets no member is
accepted on a PROVISIONED restore.

Requests are signed by hand, so that a throughput below 1 reaches the server
and every SDK version sends the on-demand members.
"""

from __future__ import annotations

import uuid

import pytest

from conftest import wait_for_active
from test_backup_restore_fidelity import (
    RESTORE_TIMEOUT_S, _cleanup, _create_backup, _error, _gsi, _signed,
)

PK = {"AttributeName": "pk", "KeyType": "HASH"}
ON_DEMAND = {"BillingMode": "PAY_PER_REQUEST"}
GSI1 = _gsi("gsi1", "g", None, {"ProjectionType": "ALL"})
LSI1 = {
    "IndexName": "lsi1",
    "KeySchema": [PK, {"AttributeName": "l", "KeyType": "RANGE"}],
    "Projection": {"ProjectionType": "KEYS_ONLY"},
}


def _attrs(*names: str) -> list[dict]:
    return [{"AttributeName": n, "AttributeType": "S"} for n in names]


def _pt(read: int, write: int) -> dict:
    return {"ReadCapacityUnits": read, "WriteCapacityUnits": write}


# The source table of each backup, as CreateTable members.
TABLES = {
    "on-demand": {"KeySchema": [PK], "AttributeDefinitions": _attrs("pk"), **ON_DEMAND},
    "indexed": {
        "KeySchema": [PK, {"AttributeName": "sk", "KeyType": "RANGE"}],
        "AttributeDefinitions": _attrs("pk", "sk", "g", "l"),
        "GlobalSecondaryIndexes": [GSI1],
        "LocalSecondaryIndexes": [LSI1],
        **ON_DEMAND,
    },
    "provisioned": {
        "KeySchema": [PK],
        "AttributeDefinitions": _attrs("pk", "g"),
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": _pt(1, 1),
        "GlobalSecondaryIndexes": [{**GSI1, "ProvisionedThroughput": _pt(1, 1)}],
    },
    "ceilings": {
        "KeySchema": [PK],
        "AttributeDefinitions": _attrs("pk", "g"),
        "OnDemandThroughput": {"MaxReadRequestUnits": 7, "MaxWriteRequestUnits": 8},
        "GlobalSecondaryIndexes": [
            {**GSI1, "OnDemandThroughput": {"MaxReadRequestUnits": 5, "MaxWriteRequestUnits": 6}}
        ],
        **ON_DEMAND,
    },
}

BY_WEIGHT = {"IndexName": "ByWeight", "Projection": {"ProjectionType": "ALL"}}
NOT_IN_SOURCE = (
    "Invalid Request: Index {} does not match a secondary index that existed in the source "
    "table and cannot be created during the restore operation"
)
TO_PROVISIONED = {"BillingModeOverride": "PROVISIONED", "ProvisionedThroughputOverride": _pt(2, 3)}
READ_10 = {"OnDemandThroughputOverride": {"MaxReadRequestUnits": 10}}

# (backup, overrides, DynamoDB's message)
ROWS = [
    ("on-demand", {"ProvisionedThroughputOverride": _pt(1, 1)},
     "Invalid Request: Cannot override ProvisionedThroughput unless BillingMode is PROVISIONED"),
    ("on-demand", {"BillingModeOverride": "PROVISIONED",
                   "ProvisionedThroughputOverride": _pt(0, 1)},
     "1 validation error detected: Value '0' at 'provisionedThroughputOverride.readCapacityUnits' "
     "failed to satisfy constraint: Member must have value greater than or equal to 1"),
    ("indexed", {"BillingModeOverride": "PROVISIONED"},
     "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden to "
     "PROVISIONED"),
    ("indexed", {"GlobalSecondaryIndexOverride": [
        {**BY_WEIGHT, "KeySchema": [{"AttributeName": "Weight", "KeyType": "HASH"}]}]},
     NOT_IN_SOURCE.format("ByWeight")),
    ("indexed", {"LocalSecondaryIndexOverride": [
        {**BY_WEIGHT, "KeySchema": [PK, {"AttributeName": "Weight", "KeyType": "RANGE"}]}]},
     NOT_IN_SOURCE.format("ByWeight")),
    ("indexed", {"GlobalSecondaryIndexOverride": [
        {**GSI1, "Projection": {"ProjectionType": "KEYS_ONLY"}}]},
     NOT_IN_SOURCE.format("gsi1")),
    ("indexed", {"LocalSecondaryIndexOverride": [
        {**LSI1, "Projection": {"ProjectionType": "ALL"}}]},
     NOT_IN_SOURCE.format("lsi1")),
    ("indexed", {"GlobalSecondaryIndexOverride": [{**GSI1, "ProvisionedThroughput": _pt(4, 5)}]},
     "Invalid Request: Cannot override ProvisionedThroughput unless BillingMode is PROVISIONED "
     "for index gsi1"),
    ("indexed", {**TO_PROVISIONED, **READ_10},
     "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput when "
     "BillingMode is PROVISIONED"),
    ("indexed", TO_PROVISIONED,
     "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden to "
     "PROVISIONED for index gsi1"),
    ("provisioned", READ_10,
     "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput unless "
     "BillingModeOverride is PAY_PER_REQUEST"),
    ("provisioned", {"GlobalSecondaryIndexOverride": [GSI1]},
     "Invalid Request: Must specify provisioned throughput for index gsi1"),
    ("provisioned", {"GlobalSecondaryIndexOverride": [
        {**GSI1, "OnDemandThroughput": {"MaxReadRequestUnits": 5}}]},
     "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput unless "
     "BillingModeOverride is PAY_PER_REQUEST for index gsi1"),
    ("ceilings", {**TO_PROVISIONED, "GlobalSecondaryIndexOverride": [
        {**GSI1, "ProvisionedThroughput": _pt(1, 1),
         "OnDemandThroughput": {"MaxWriteRequestUnits": 5}}]},
     "Invalid Request: Cannot override MaxWriteRequestUnits for OnDemandThroughput when "
     "BillingMode is PROVISIONED for index gsi1"),
    ("ceilings", {"GlobalSecondaryIndexOverride": [
        {**GSI1, "ProvisionedThroughput": _pt(1, 1),
         "OnDemandThroughput": {"MaxReadRequestUnits": 5}}]},
     "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput when "
     "ProvisionedThroughputOverride is present for index gsi1"),
    ("ceilings", {"ProvisionedThroughputOverride": _pt(2, 3), **READ_10},
     "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput when "
     "ProvisionedThroughputOverride is present"),
    ("ceilings", {"BillingModeOverride": "PAY_PER_REQUEST",
                  "ProvisionedThroughputOverride": _pt(2, 3)},
     "Invalid Request: Cannot override ProvisionedThroughput if BillingMode is overridden to "
     "PAY_PER_REQUEST"),
    ("ceilings", {"BillingModeOverride": "PAY_PER_REQUEST",
                  "GlobalSecondaryIndexOverride": [{**GSI1, "ProvisionedThroughput": _pt(1, 1)}]},
     "Invalid Request: Cannot override ProvisionedThroughput if BillingMode is overridden to "
     "PAY_PER_REQUEST for index gsi1"),
    # Measured on 17 Nov 2026.
    ("provisioned", {"BillingModeOverride": "PROVISIONED",
                     "ProvisionedThroughputOverride": _pt(0, 0)},
     "2 validation errors detected: "
     "Value '0' at 'provisionedThroughputOverride.writeCapacityUnits' failed to satisfy "
     "constraint: Member must have value greater than or equal to 1; "
     "Value '0' at 'provisionedThroughputOverride.readCapacityUnits' failed to satisfy "
     "constraint: Member must have value greater than or equal to 1"),
    ("provisioned", {"ProvisionedThroughputOverride": _pt(2, 3), **READ_10},
     "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput when "
     "ProvisionedThroughputOverride is present"),
    ("provisioned", {"GlobalSecondaryIndexOverride": [
        {**GSI1, "ProvisionedThroughput": _pt(1, 1),
         "OnDemandThroughput": {"MaxReadRequestUnits": 5}}]},
     "Invalid Request: Cannot override MaxReadRequestUnits for OnDemandThroughput when "
     "ProvisionedThroughputOverride is present for index gsi1"),
    ("provisioned", {"GlobalSecondaryIndexOverride": [{**GSI1, "OnDemandThroughput": {}}]},
     "Invalid Request: Must specify provisioned throughput for index gsi1"),
    ("indexed", {"BillingModeOverride": "PROVISIONED", "ProvisionedThroughputOverride": _pt(1, 1),
                 "GlobalSecondaryIndexOverride": [
                     {**GSI1, "OnDemandThroughput": {"MaxReadRequestUnits": 5}}]},
     "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden to "
     "PROVISIONED for index gsi1"),
]

# Accepted on the provisioned backup (measured on 17 Nov 2026): the target is
# PROVISIONED at 1 and 1, with no on-demand maximum.
ACCEPTED = [
    {"OnDemandThroughputOverride": {}},
    {"GlobalSecondaryIndexOverride": [
        {**GSI1, "ProvisionedThroughput": _pt(1, 1), "OnDemandThroughput": {}}]},
]


@pytest.fixture(scope="module")
def backups(dynamodb_client):
    """The ARN of an AVAILABLE backup of each table in TABLES."""
    prefix = f"restore-refuse-{uuid.uuid4().hex[:8]}"
    names = {kind: f"{prefix}-{kind}" for kind in TABLES}
    arns: dict[str, str] = {}
    try:
        for kind, members in TABLES.items():
            resp = _signed("CreateTable", {"TableName": names[kind], **members})
            assert resp.status_code == 200, resp.text
        for kind, name in names.items():
            wait_for_active(dynamodb_client, name)
            arns[kind] = _create_backup(dynamodb_client, name)
        yield arns
    finally:
        try:
            for arn in arns.values():
                _cleanup(dynamodb_client, arn)
        finally:
            _cleanup(dynamodb_client, None, *names.values())


@pytest.mark.parametrize(
    ("backup", "overrides", "expected"),
    ROWS,
    ids=[f"{i + 1}-{row[0]}" for i, row in enumerate(ROWS)],
)
def test_a_conflicting_override_is_refused_with_the_services_text(
    dynamodb_client, backups, backup, overrides, expected
):
    target = f"restore-refused-{uuid.uuid4().hex[:10]}"
    resp = _signed(
        "RestoreTableFromBackup",
        {"TargetTableName": target, "BackupArn": backups[backup], **overrides},
    )
    assert resp.status_code == 400, resp.text
    assert _error(resp) == ("ValidationException", expected)
    with pytest.raises(dynamodb_client.exceptions.ResourceNotFoundException):
        dynamodb_client.describe_table(TableName=target)


@pytest.mark.parametrize("overrides", ACCEPTED, ids=["table", "index"])
def test_an_on_demand_maximum_that_sets_no_member_restores_provisioned(
    dynamodb_client, backups, overrides
):
    target = f"restore-accepted-{uuid.uuid4().hex[:10]}"
    try:
        resp = _signed(
            "RestoreTableFromBackup",
            {"TargetTableName": target, "BackupArn": backups["provisioned"], **overrides},
        )
        assert resp.status_code == 200, resp.text
        wait_for_active(dynamodb_client, target, timeout=RESTORE_TIMEOUT_S)
        table = _signed("DescribeTable", {"TableName": target}).json()["Table"]
        for described in [table, *table["GlobalSecondaryIndexes"]]:
            throughput = described["ProvisionedThroughput"]
            assert {k: throughput[k] for k in _pt(1, 1)} == _pt(1, 1), described
        assert "OnDemandThroughput" not in table, table
    finally:
        _cleanup(dynamodb_client, None, target)
