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

The request validation before those answers, and its order, are the
service's: a missing table name member and the table name constraints, the
missing recovery specification, the restore's source member and time members,
and TableNotFoundException for a missing table on all three operations. The
expected texts were measured on DynamoDB, two samples each, with client-side
parameter validation off.
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
NO_RESTORE_TIME = (
    "Invalid Request: Either one of RestoreDateTime or UseLatestRestorableTime "
    "must be specified, but not both"
)
TWO_RESTORE_TIMES = (
    "Invalid Request: Both RestoreDateTime and UseLatestRestorableTime cannot be set "
    "for point-in-time restore request. Please specify either RestoreDateTime time "
    "or UseLatestRestorableTime"
)
SOURCE_NAMED_TWICE = (
    "Invalid Request: Must provide exactly one of: sourceTableArn, sourceTableName"
)
INVALID_TABLE_ARN = "Invalid Request: Table ARN is invalid."
NAME_PATTERN = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
EMPTY_TABLE_NAME = (
    "2 validation errors detected: Value '' at 'tableName' failed to satisfy "
    "constraint: Member must have length greater than or equal to 3; Value '' at "
    f"'tableName' failed to satisfy constraint: {NAME_PATTERN}"
)
SOURCE_TABLE_NAME_LENGTH = (
    "TableName must be at least 3 characters long and at most 255 characters long"
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


def _recovery_not_enabled(name: str) -> tuple[str, str]:
    return (
        "PointInTimeRecoveryUnavailableException",
        f"Point in time recovery is not enabled for table '{name}'",
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
    assert desc["PointInTimeRecoveryDescription"] == {
        "PointInTimeRecoveryStatus": "DISABLED"
    }


def test_restore_to_point_in_time_is_refused(
    dynamodb_client, create_and_cleanup_table, unique_table_name
):
    """Restore on an existing table fails with the restore-modeled exception."""
    create_and_cleanup_table()
    _assert_error(
        dynamodb_client.restore_table_to_point_in_time,
        *_recovery_not_enabled(unique_table_name),
        SourceTableName=unique_table_name,
        TargetTableName=f"{unique_table_name}-restored",
        UseLatestRestorableTime=True,
    )


def test_restore_to_point_in_time_missing_source_table(dynamodb_client):
    """Restore resolves the source table first: a missing table reports
    TableNotFoundException, not the recovery-unavailable error."""
    missing = _missing_name()
    _assert_error(
        dynamodb_client.restore_table_to_point_in_time,
        *_table_not_found(missing),
        SourceTableName=missing,
        TargetTableName=f"{missing}-restored",
        UseLatestRestorableTime=True,
    )


def test_a_missing_table_is_table_not_found_on_both_continuous_backups_operations(
    dynamodb_client,
):
    """TableNotFoundException with the service's text, for either flag."""
    missing = _missing_name()
    _assert_error(
        dynamodb_client.describe_continuous_backups,
        *_table_not_found(missing),
        TableName=missing,
    )
    for enabled in (False, True):
        _assert_error(
            dynamodb_client.update_continuous_backups,
            *_table_not_found(missing),
            TableName=missing,
            PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": enabled},
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
    """No TableName, no restore source, or no TargetTableName: the same text,
    and a missing target is reported before the source is looked up."""
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
    restore = client.restore_table_to_point_in_time
    for params in (
        {"TargetTableName": f"{unique_table_name}-restored"},
        {"SourceTableName": unique_table_name},
        {"SourceTableName": _missing_name()},
    ):
        _assert_error(
            restore,
            "ValidationException",
            TABLE_NAME_REQUIRED,
            UseLatestRestorableTime=True,
            **params,
        )


def test_restore_checks_the_source_then_the_time_members_then_recovery(
    dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name
):
    """The source lookup comes before the time members, the time members
    before the recovery check, and the target is never looked up."""
    create_and_cleanup_table()
    other = f"{unique_table_name}-other"
    create_and_cleanup_table(table_name=other)
    restore = dynamodb_client_no_validation.restore_table_to_point_in_time
    target = f"{unique_table_name}-restored"
    missing = _missing_name()

    _assert_error(
        restore, *_table_not_found(missing), SourceTableName=missing, TargetTableName=target
    )
    for times in ({}, {"UseLatestRestorableTime": False}):
        _assert_error(
            restore,
            "ValidationException",
            NO_RESTORE_TIME,
            SourceTableName=unique_table_name,
            TargetTableName=target,
            **times,
        )
    _assert_error(
        restore,
        "ValidationException",
        TWO_RESTORE_TIMES,
        SourceTableName=unique_table_name,
        TargetTableName=target,
        UseLatestRestorableTime=True,
        RestoreDateTime=1_700_000_000,
    )
    _assert_error(
        restore,
        *_recovery_not_enabled(unique_table_name),
        SourceTableName=unique_table_name,
        TargetTableName=target,
        RestoreDateTime=1_700_000_000,
    )
    # An existing target is not checked before the recovery check.
    _assert_error(
        restore,
        *_recovery_not_enabled(unique_table_name),
        SourceTableName=unique_table_name,
        TargetTableName=other,
        UseLatestRestorableTime=True,
    )


def test_restore_source_table_arn_names_the_source_and_must_name_a_table(
    dynamodb_client, dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name
):
    """SourceTableArn names the source as SourceTableName does; an index ARN
    is refused, with or without a time member; giving both is refused."""
    create_and_cleanup_table()
    other = f"{unique_table_name}-other"
    create_and_cleanup_table(table_name=other)
    table_arn = dynamodb_client.describe_table(TableName=unique_table_name)["Table"][
        "TableArn"
    ]
    prefix = table_arn[: -len(unique_table_name)]
    restore = dynamodb_client_no_validation.restore_table_to_point_in_time
    target = f"{unique_table_name}-restored"
    missing = _missing_name()

    _assert_error(
        restore,
        *_recovery_not_enabled(unique_table_name),
        SourceTableArn=table_arn,
        TargetTableName=target,
        UseLatestRestorableTime=True,
    )
    _assert_error(
        restore,
        *_table_not_found(missing),
        SourceTableArn=prefix + missing,
        TargetTableName=target,
        UseLatestRestorableTime=True,
    )
    for times in ({"UseLatestRestorableTime": True}, {}):
        _assert_error(
            restore,
            "ValidationException",
            INVALID_TABLE_ARN,
            SourceTableArn=f"{table_arn}/index/gsi1",
            TargetTableName=target,
            **times,
        )
    for arn in (table_arn, prefix + other):
        _assert_error(
            restore,
            "ValidationException",
            SOURCE_NAMED_TWICE,
            SourceTableName=unique_table_name,
            SourceTableArn=arn,
            TargetTableName=target,
            UseLatestRestorableTime=True,
        )


def test_an_empty_table_name_is_refused_before_the_table_is_looked_up(
    dynamodb_client_no_validation,
):
    """An empty TableName fails the length and pattern constraints on both
    continuous-backups operations (M335 probes 56 and 57), and an empty
    SourceTableName fails the length check on the restore (probe 58), before
    any table is looked up."""
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
    _assert_error(
        client.restore_table_to_point_in_time,
        "ValidationException",
        SOURCE_TABLE_NAME_LENGTH,
        SourceTableName="",
        TargetTableName=f"{_missing_name()}-restored",
        UseLatestRestorableTime=True,
    )


def test_a_table_arn_as_the_restore_source_name_fails_the_name_pattern(
    dynamodb_client_no_validation,
):
    """SourceTableName holding a table ARN fails the name pattern at
    sourceTableName, before the lookup (M335 probe 46)."""
    arn = f"arn:aws:dynamodb:us-east-1:123456789012:table/{_missing_name()}"
    _assert_error(
        dynamodb_client_no_validation.restore_table_to_point_in_time,
        "ValidationException",
        f"1 validation error detected: Value '{arn}' at 'sourceTableName' failed to "
        f"satisfy constraint: {NAME_PATTERN}",
        SourceTableName=arn,
        TargetTableName=f"{_missing_name()}-restored",
        UseLatestRestorableTime=True,
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
