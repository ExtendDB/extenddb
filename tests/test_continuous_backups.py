# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Continuous backups and point-in-time recovery surface.

ExtendDB deliberately diverges from real DynamoDB here (see
docs/differences-from-dynamodb.md): DescribeContinuousBackups always reports
PointInTimeRecoveryStatus DISABLED, UpdateContinuousBackups refuses to enable
recovery with ContinuousBackupsUnavailableException, and
RestoreTableToPointInTime refuses with PointInTimeRecoveryUnavailableException
after resolving the source table. These tests pin that divergence, so they run
only against ExtendDB.
"""

from __future__ import annotations

import os
import uuid

import pytest
from botocore.exceptions import ClientError

pytestmark = pytest.mark.skipif(
    not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip(),
    reason="pins ExtendDB's documented divergence: point-in-time recovery is unsupported",
)


def test_describe_continuous_backups_reports_recovery_disabled(
    dynamodb_client, create_and_cleanup_table, unique_table_name
):
    """ContinuousBackupsStatus is ENABLED, recovery is DISABLED, no window."""
    create_and_cleanup_table()
    resp = dynamodb_client.describe_continuous_backups(TableName=unique_table_name)
    desc = resp["ContinuousBackupsDescription"]
    assert desc["ContinuousBackupsStatus"] == "ENABLED"
    pitr = desc["PointInTimeRecoveryDescription"]
    assert pitr["PointInTimeRecoveryStatus"] == "DISABLED"
    assert "EarliestRestorableDateTime" not in pitr
    assert "LatestRestorableDateTime" not in pitr


def test_enable_point_in_time_recovery_is_refused(
    dynamodb_client, create_and_cleanup_table, unique_table_name
):
    """Enabling recovery fails with the typed exception and changes nothing."""
    create_and_cleanup_table()
    with pytest.raises(ClientError) as excinfo:
        dynamodb_client.update_continuous_backups(
            TableName=unique_table_name,
            PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
        )
    assert (
        excinfo.value.response["Error"]["Code"]
        == "ContinuousBackupsUnavailableException"
    )

    resp = dynamodb_client.describe_continuous_backups(TableName=unique_table_name)
    assert (
        resp["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"][
            "PointInTimeRecoveryStatus"
        ]
        == "DISABLED"
    )


def test_disable_point_in_time_recovery_reports_disabled(
    dynamodb_client, create_and_cleanup_table, unique_table_name
):
    """Disabling recovery that was never enabled succeeds as a no-op."""
    create_and_cleanup_table()
    resp = dynamodb_client.update_continuous_backups(
        TableName=unique_table_name,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": False},
    )
    desc = resp["ContinuousBackupsDescription"]
    assert desc["ContinuousBackupsStatus"] == "ENABLED"
    assert (
        desc["PointInTimeRecoveryDescription"]["PointInTimeRecoveryStatus"]
        == "DISABLED"
    )


def test_restore_to_point_in_time_is_refused(
    dynamodb_client, create_and_cleanup_table, unique_table_name
):
    """Restore on an existing table fails with the restore-modeled exception."""
    create_and_cleanup_table()
    with pytest.raises(ClientError) as excinfo:
        dynamodb_client.restore_table_to_point_in_time(
            SourceTableName=unique_table_name,
            TargetTableName=f"{unique_table_name}-restored",
            UseLatestRestorableTime=True,
        )
    err = excinfo.value.response["Error"]
    assert err["Code"] == "PointInTimeRecoveryUnavailableException"
    assert unique_table_name in err["Message"]


def test_restore_to_point_in_time_missing_source_table(dynamodb_client):
    """Restore resolves the source table first: a missing table reports
    TableNotFoundException, not the recovery-unavailable error."""
    missing = f"extenddb-missing-{uuid.uuid4().hex[:12]}"
    with pytest.raises(ClientError) as excinfo:
        dynamodb_client.restore_table_to_point_in_time(
            SourceTableName=missing,
            TargetTableName=f"{missing}-restored",
            UseLatestRestorableTime=True,
        )
    assert excinfo.value.response["Error"]["Code"] == "TableNotFoundException"
