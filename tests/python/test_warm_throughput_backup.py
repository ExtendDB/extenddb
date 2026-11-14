# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""WarmThroughput and backups (rows S6a to S6d, measured against Amazon DynamoDB
on 2026-10-23): a backup records no warm throughput, a restored table and its
indexes report the on-demand defaults whatever the source had set, and a
GlobalSecondaryIndexOverride entry carrying WarmThroughput is refused. The file
runs against ExtendDB and the service with the same assertions."""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name
from warm_throughput_helpers import (  # noqa: F401  (tables is a fixture)
    GSI,
    ON_DEMAND_DEFAULTS,
    _create,
    _validation,
    _wait_all_active,
    _wait_warm_active,
    tables,
)

GSI_OVERRIDE_WARM_THROUGHPUT = (
    "One or more parameter values were invalid: WarmThroughput is not supported on Global "
    "Secondary Indexes via RestoreTableFromBackup"
)


def _wait_backup_available(client, arn: str, timeout: float = 900.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        desc = client.describe_backup(BackupArn=arn)["BackupDescription"]
        if desc["BackupDetails"]["BackupStatus"] == "AVAILABLE":
            return desc
        if time.monotonic() > deadline:
            raise TimeoutError(f"backup {arn} did not become AVAILABLE")
        time.sleep(1)


def test_a_restored_table_reports_the_defaults_and_a_gsi_override_cannot_set_it(
    dynamodb_client, tables
):
    """Rows S6a to S6d."""
    name, _ = _create(
        dynamodb_client,
        tables,
        BillingMode="PAY_PER_REQUEST",
        WarmThroughput={"ReadUnitsPerSecond": 30000, "WriteUnitsPerSecond": 8000},
        GlobalSecondaryIndexes=[
            {**GSI, "WarmThroughput": {"ReadUnitsPerSecond": 24000, "WriteUnitsPerSecond": 8000}}
        ],
    )
    _wait_all_active(dynamodb_client, name)
    _wait_warm_active(dynamodb_client, name, timeout=900)
    # A just-created table refuses CreateBackup with
    # ContinuousBackupsUnavailableException for a short while on the service
    # (backups are still being enabled); ExtendDB accepts at once. Retried the
    # way test_backup_restore_fidelity.py does.
    deadline = time.monotonic() + 60
    while True:
        try:
            arn = dynamodb_client.create_backup(
                TableName=name, BackupName=f"wt-{int(time.time())}"
            )["BackupDetails"]["BackupArn"]
            break
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code != "ContinuousBackupsUnavailableException" or time.monotonic() >= deadline:
                raise
            time.sleep(2)
    try:
        # S6b: the backup records no warm throughput.
        desc = _wait_backup_available(dynamodb_client, arn)
        assert "WarmThroughput" not in desc["SourceTableDetails"]
        for gsi in desc.get("SourceTableFeatureDetails", {}).get("GlobalSecondaryIndexes", []):
            assert "WarmThroughput" not in gsi

        # S6c: no member while CREATING; the defaults once ACTIVE, on the table
        # and the index, although the source had set larger values.
        target = unique_name("wt-restore")
        tables.append(target)
        restored = dynamodb_client.restore_table_from_backup(
            TargetTableName=target, BackupArn=arn
        )["TableDescription"]
        if restored["TableStatus"] == "CREATING":
            assert "WarmThroughput" not in restored
            assert all("WarmThroughput" not in g for g in restored["GlobalSecondaryIndexes"])
        _wait_all_active(dynamodb_client, target)
        active = _wait_warm_active(dynamodb_client, target, timeout=900)
        assert active["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "ACTIVE"}
        assert active["GlobalSecondaryIndexes"][0]["WarmThroughput"] == {
            **ON_DEMAND_DEFAULTS,
            "Status": "ACTIVE",
        }

        # S6d: an override entry may not carry WarmThroughput.
        with pytest.raises(ClientError) as exc:
            dynamodb_client.restore_table_from_backup(
                TargetTableName=unique_name("wt-restore"),
                BackupArn=arn,
                GlobalSecondaryIndexOverride=[
                    {
                        **GSI,
                        "WarmThroughput": {
                            "ReadUnitsPerSecond": 24000,
                            "WriteUnitsPerSecond": 8000,
                        },
                    }
                ],
            )
        _validation(exc, GSI_OVERRIDE_WARM_THROUGHPUT)
    finally:
        try:
            dynamodb_client.delete_backup(BackupArn=arn)
        except ClientError:
            pass
