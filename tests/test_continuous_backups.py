# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Continuous backups and point-in-time recovery surface.

ExtendDB deliberately diverges from real DynamoDB here (see
docs/differences-from-dynamodb.md): DescribeContinuousBackups always reports
PointInTimeRecoveryStatus DISABLED, UpdateContinuousBackups refuses to enable
recovery with ContinuousBackupsUnavailableException, and
RestoreTableToPointInTime refuses every request with
PointInTimeRecoveryUnavailableException. Point-in-time recovery is not in this
release, so the server gives both refusals before it authorizes the request or
reads any table: every caller and every request shape gets the same answer.
These tests pin that divergence, so they run only against ExtendDB.

The request validation of DescribeContinuousBackups and of a disable, and its
order, are the service's: a missing table name member and the table name
constraints, the missing recovery specification, and TableNotFoundException
for a missing table. The expected texts were measured on DynamoDB, two samples
each, with client-side parameter validation off.
"""

from __future__ import annotations

import os
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import ENABLE_REFUSED, RESTORE_REFUSED

pytestmark = pytest.mark.skipif(
    not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip(),
    reason="pins ExtendDB's documented divergence: point-in-time recovery is unsupported",
)

TABLE_NAME_REQUIRED = (
    "The parameter 'TableName' is required but was not present in the request"
)
SPECIFICATION_REQUIRED = (
    "1 validation error detected: Value null at 'pointInTimeRecoverySpecification' "
    "failed to satisfy constraint: Member must not be null"
)
FLAG_REQUIRED = (
    "1 validation error detected: Value null at "
    "'pointInTimeRecoverySpecification.pointInTimeRecoveryEnabled' "
    "failed to satisfy constraint: Member must not be null"
)
NAME_PATTERN = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
EMPTY_TABLE_NAME = (
    "2 validation errors detected: Value '' at 'tableName' failed to satisfy "
    "constraint: Member must have length greater than or equal to 3; Value '' at "
    f"'tableName' failed to satisfy constraint: {NAME_PATTERN}"
)


def _missing_name() -> str:
    return f"extenddb-missing-{uuid.uuid4().hex[:12]}"


def _error(call, **params) -> dict:
    with pytest.raises(ClientError) as excinfo:
        call(**params)
    return excinfo.value.response["Error"]


def _assert_error(call, code: str, message: str, **params) -> None:
    err = _error(call, **params)
    assert (err["Code"], err["Message"]) == (code, message), params


def _table_not_found(name: str) -> tuple[str, str]:
    return "TableNotFoundException", f"Table not found: {name}"


def test_describe_continuous_backups_reports_recovery_disabled(
    dynamodb_client, create_and_cleanup_table, unique_table_name
):
    """ContinuousBackupsStatus is ENABLED, recovery is DISABLED, no window."""
    create_and_cleanup_table()
    resp = dynamodb_client.describe_continuous_backups(TableName=unique_table_name)
    desc = resp["ContinuousBackupsDescription"]
    assert desc["ContinuousBackupsStatus"] == "ENABLED"
    pitr = desc["PointInTimeRecoveryDescription"]
    assert pitr == {"PointInTimeRecoveryStatus": "DISABLED"}


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
    err = excinfo.value.response["Error"]
    assert (err["Code"], err["Message"]) == ENABLE_REFUSED

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
    assert desc["PointInTimeRecoveryDescription"] == {
        "PointInTimeRecoveryStatus": "DISABLED"
    }


def test_a_missing_table_on_the_continuous_backups_operations(
    dynamodb_client,
):
    """TableNotFoundException with the service's text on a describe and a
    disable. An enable is refused before the table is looked up."""
    missing = _missing_name()
    _assert_error(
        dynamodb_client.describe_continuous_backups,
        *_table_not_found(missing),
        TableName=missing,
    )
    _assert_error(
        dynamodb_client.update_continuous_backups,
        *_table_not_found(missing),
        TableName=missing,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": False},
    )
    _assert_error(
        dynamodb_client.update_continuous_backups,
        *ENABLE_REFUSED,
        TableName=missing,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )


def test_the_recovery_specification_is_required_before_the_table_is_looked_up(
    dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name
):
    """A missing specification is refused for a missing table too, so it is
    checked first; an empty one names its missing flag."""
    create_and_cleanup_table()
    update = dynamodb_client_no_validation.update_continuous_backups
    for name in (_missing_name(), unique_table_name):
        _assert_error(update, "ValidationException", SPECIFICATION_REQUIRED, TableName=name)
    _assert_error(
        update,
        "ValidationException",
        FLAG_REQUIRED,
        TableName=unique_table_name,
        PointInTimeRecoverySpecification={},
    )


def test_a_missing_table_name_member_is_the_required_parameter_text(
    dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name
):
    """No TableName on a describe or a disable: the service's text."""
    create_and_cleanup_table()
    client = dynamodb_client_no_validation
    _assert_error(
        client.describe_continuous_backups, "ValidationException", TABLE_NAME_REQUIRED
    )
    _assert_error(
        client.update_continuous_backups,
        "ValidationException",
        TABLE_NAME_REQUIRED,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": False},
    )


def test_an_empty_table_name_is_refused_before_the_table_is_looked_up(
    dynamodb_client_no_validation,
):
    """An empty TableName fails the length and pattern constraints on a
    describe and a disable (M335 probes 56 and 57), before any table is
    looked up."""
    client = dynamodb_client_no_validation
    _assert_error(
        client.describe_continuous_backups,
        "ValidationException",
        EMPTY_TABLE_NAME,
        TableName="",
    )
    _assert_error(
        client.update_continuous_backups,
        "ValidationException",
        EMPTY_TABLE_NAME,
        TableName="",
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": False},
    )


def test_the_recovery_flag_as_the_string_false_disables(
    dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name
):
    """PointInTimeRecoveryEnabled given as the JSON string "false" is taken as
    false: 200 with the description (M335 probe 54)."""
    create_and_cleanup_table()
    resp = dynamodb_client_no_validation.update_continuous_backups(
        TableName=unique_table_name,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": "false"},
    )
    assert resp["ContinuousBackupsDescription"] == {
        "ContinuousBackupsStatus": "ENABLED",
        "PointInTimeRecoveryDescription": {"PointInTimeRecoveryStatus": "DISABLED"},
    }


def test_every_restore_gets_the_refusal(
    dynamodb_client, dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name
):
    """Every RestoreTableToPointInTime gets the same refusal: an existing
    source, a missing one, an index ARN, a source named twice or not at all,
    no target, bad names and bad time members."""
    create_and_cleanup_table()
    table_arn = dynamodb_client.describe_table(TableName=unique_table_name)["Table"][
        "TableArn"
    ]
    prefix = table_arn[: -len(unique_table_name)]
    target = f"{unique_table_name}-restored"
    missing = _missing_name()
    shapes = [
        {"SourceTableName": unique_table_name, "TargetTableName": target,
         "UseLatestRestorableTime": True},
        {"SourceTableArn": table_arn, "TargetTableName": target,
         "RestoreDateTime": 1_700_000_000},
        {"SourceTableName": missing, "TargetTableName": target, "UseLatestRestorableTime": True},
        {"SourceTableArn": prefix + missing, "TargetTableName": target,
         "UseLatestRestorableTime": True},
        {"SourceTableArn": f"{table_arn}/index/gsi1", "TargetTableName": target},
        {"SourceTableName": unique_table_name, "SourceTableArn": table_arn,
         "TargetTableName": target, "UseLatestRestorableTime": True},
        {"TargetTableName": target, "UseLatestRestorableTime": True},
        {"SourceTableName": unique_table_name, "UseLatestRestorableTime": True},
        {"SourceTableName": "", "TargetTableName": target, "UseLatestRestorableTime": True},
        {"SourceTableName": f"arn:aws:dynamodb:us-east-1:123456789012:table/{missing}",
         "TargetTableName": target, "UseLatestRestorableTime": True},
        {"SourceTableName": unique_table_name, "TargetTableName": target},
        {"SourceTableName": unique_table_name, "TargetTableName": target,
         "UseLatestRestorableTime": True, "RestoreDateTime": 1_700_000_000},
        {"SourceTableName": unique_table_name, "TargetTableName": unique_table_name,
         "UseLatestRestorableTime": True},
    ]
    restore = dynamodb_client_no_validation.restore_table_to_point_in_time
    for params in shapes:
        _assert_error(restore, *RESTORE_REFUSED, **params)
