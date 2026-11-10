# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Point-in-time recovery: the enable, the disable and the description.

Every expectation was measured on DynamoDB, two samples each. Runs against
real DynamoDB, and against ExtendDB when the server is built with the pitr
feature (EXTENDDB_TEST_PITR=1).

- An enable answers ENABLED at once, with RecoveryPeriodInDays (35 when the
  request gives none) and an undocumented PointInTimeRecoveryWindowDays equal
  to it, and with EarliestRestorableDateTime and LatestRestorableDateTime both
  the enable instant in whole seconds.
- An enable on an enabled table keeps the window; it changes the period only
  when the request gives one.
- RecoveryPeriodInDays is 1 to 35, checked before the table lookup, on an
  enable and on a disable; a disable that gives one is refused. A number with
  a fraction is truncated toward zero, one outside the 32-bit range is
  clamped to it, and another JSON type is a SerializationException checked
  before every other member. A bad period and a missing
  PointInTimeRecoveryEnabled are reported in one message, the period first.
- An enable of a RestoreTableFromBackup target being created is refused:
  TableNotFoundException for its first seconds, then
  ContinuousBackupsUnavailableException "Backups are being enabled".
- A disable answers DISABLED with no window; a re-enable starts a new window
  and takes 35 when it gives no period.
"""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest
from botocore.exceptions import ClientError

pytestmark = pytest.mark.skipif(
    bool(os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip())
    and os.environ.get("EXTENDDB_TEST_PITR") != "1",
    reason="needs a server built with the pitr feature (EXTENDDB_TEST_PITR=1)",
)

PERIOD_PATH = "pointInTimeRecoverySpecification.recoveryPeriodInDays"


def _too_low(value: int) -> str:
    return (
        f"1 validation error detected: Value '{value}' at '{PERIOD_PATH}' failed to "
        "satisfy constraint: Member must have value greater than or equal to 1"
    )


def _too_high(value: int) -> str:
    return (
        f"1 validation error detected: Value '{value}' at '{PERIOD_PATH}' failed to "
        "satisfy constraint: Member must have value less than or equal to 35"
    )


PERIOD_ON_DISABLE = (
    "Invalid Request: Cannot specify RecoveryPeriodInDays when disabling "
    "point-in-time recovery."
)


@pytest.fixture()
def raw_bodies(dynamodb_client_no_validation):
    """Every response body the no-validation client receives during the test,
    as parsed JSON, including members the SDK model does not know."""
    bodies: list[dict] = []

    def keep(http_response, **_):
        bodies.append(json.loads(http_response.text or "{}"))

    events = dynamodb_client_no_validation.meta.events
    events.register("after-call.dynamodb", keep, unique_id="pitr-raw-bodies")
    yield bodies
    events.unregister("after-call.dynamodb", unique_id="pitr-raw-bodies")


def _update(client, name, **spec):
    return client.update_continuous_backups(
        TableName=name, PointInTimeRecoverySpecification=spec
    )


def _while_backups_start(call):
    """`call` on a new table. The service refuses an enable and a backup for
    a few seconds after the table turns ACTIVE (measured: "Backups are being
    enabled for the table: <name>. Please retry later"), so retry that one
    refusal for up to 30 s."""
    deadline = time.time() + 30
    while True:
        try:
            return call()
        except ClientError as e:
            err = e.response["Error"]
            starting = err["Code"] == "ContinuousBackupsUnavailableException" and err[
                "Message"
            ].startswith("Backups are being enabled for the table")
            if not starting or time.time() > deadline:
                raise
            time.sleep(2)


def _first_enable(client, name, **spec):
    """The first enable of a new table."""
    return _while_backups_start(
        lambda: _update(client, name, PointInTimeRecoveryEnabled=True, **spec)
    )


def _error(call, **params) -> tuple[str, str]:
    with pytest.raises(ClientError) as excinfo:
        call(**params)
    err = excinfo.value.response["Error"]
    return err["Code"], err["Message"]


def test_an_enable_reports_the_window_and_the_period(
    dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name, raw_bodies
):
    client = dynamodb_client_no_validation
    create_and_cleanup_table()
    bodies = raw_bodies
    _first_enable(client, unique_table_name)
    after = time.time()
    before = int(after) - 2
    client.describe_continuous_backups(TableName=unique_table_name)
    for body in bodies[-2:]:
        desc = body["ContinuousBackupsDescription"]
        assert desc["ContinuousBackupsStatus"] == "ENABLED"
        pitr = desc["PointInTimeRecoveryDescription"]
        assert pitr["PointInTimeRecoveryStatus"] == "ENABLED"
        assert pitr["RecoveryPeriodInDays"] == 35
        assert pitr["PointInTimeRecoveryWindowDays"] == 35
        earliest = pitr["EarliestRestorableDateTime"]
        assert earliest == int(earliest), pitr
        assert before - 1 <= earliest <= after, pitr
        assert pitr["LatestRestorableDateTime"] == earliest, pitr


def test_an_enable_on_an_enabled_table_changes_only_a_given_period(
    dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name
):
    client = dynamodb_client_no_validation
    create_and_cleanup_table()

    def pitr(resp):
        return resp["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]

    first = pitr(_first_enable(client, unique_table_name, RecoveryPeriodInDays=7))
    assert first["RecoveryPeriodInDays"] == 7
    for _ in range(2):
        again = pitr(
            _update(
                client, unique_table_name, PointInTimeRecoveryEnabled=True, RecoveryPeriodInDays=20
            )
        )
        assert again["RecoveryPeriodInDays"] == 20
        assert again["EarliestRestorableDateTime"] == first["EarliestRestorableDateTime"]
    for _ in range(2):
        kept = pitr(_update(client, unique_table_name, PointInTimeRecoveryEnabled=True))
        assert kept["RecoveryPeriodInDays"] == 20
        assert kept["EarliestRestorableDateTime"] == first["EarliestRestorableDateTime"]


@pytest.mark.parametrize(
    ("enabled", "value", "message"),
    [
        (True, 0, _too_low(0)),
        (True, -1, _too_low(-1)),
        (True, 36, _too_high(36)),
        (False, 0, _too_low(0)),
        (False, 7, PERIOD_ON_DISABLE),
    ],
)
def test_the_period_is_checked_before_the_table(
    dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name, enabled, value, message
):
    client = dynamodb_client_no_validation
    create_and_cleanup_table()
    missing = f"extenddb-missing-{uuid.uuid4().hex[:12]}"
    for name in (unique_table_name, missing):
        for _ in range(2):
            assert _error(
                client.update_continuous_backups,
                TableName=name,
                PointInTimeRecoverySpecification={
                    "PointInTimeRecoveryEnabled": enabled,
                    "RecoveryPeriodInDays": value,
                },
            ) == ("ValidationException", message), name


def test_a_valid_period_on_a_missing_table_is_table_not_found(dynamodb_client_no_validation):
    missing = f"extenddb-missing-{uuid.uuid4().hex[:12]}"
    assert _error(
        dynamodb_client_no_validation.update_continuous_backups,
        TableName=missing,
        PointInTimeRecoverySpecification={
            "PointInTimeRecoveryEnabled": True,
            "RecoveryPeriodInDays": 7,
        },
    ) == ("TableNotFoundException", f"Table not found: {missing}")


def test_a_disable_drops_the_window_and_a_re_enable_starts_a_new_one(
    dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name, raw_bodies
):
    client = dynamodb_client_no_validation
    create_and_cleanup_table()
    bodies = raw_bodies
    first = _first_enable(client, unique_table_name, RecoveryPeriodInDays=7)[
        "ContinuousBackupsDescription"
    ]["PointInTimeRecoveryDescription"]
    time.sleep(1.5)
    for _ in range(2):
        _update(client, unique_table_name, PointInTimeRecoveryEnabled=False)
        client.describe_continuous_backups(TableName=unique_table_name)
    for body in bodies[-4:]:
        assert body == {
            "ContinuousBackupsDescription": {
                "ContinuousBackupsStatus": "ENABLED",
                "PointInTimeRecoveryDescription": {"PointInTimeRecoveryStatus": "DISABLED"},
            }
        }
    again = _update(client, unique_table_name, PointInTimeRecoveryEnabled=True)[
        "ContinuousBackupsDescription"
    ]["PointInTimeRecoveryDescription"]
    assert again["RecoveryPeriodInDays"] == 35
    assert again["EarliestRestorableDateTime"] > first["EarliestRestorableDateTime"]


FLAG_NULL = (
    "Value null at 'pointInTimeRecoverySpecification.pointInTimeRecoveryEnabled' "
    "failed to satisfy constraint: Member must not be null"
)
NAME_PATTERN = (
    "1 validation error detected: Value 'bad name!' at 'tableName' failed to "
    "satisfy constraint: Member must satisfy regular expression pattern: "
    "[a-zA-Z0-9_.-]+"
)


def _missing() -> str:
    return f"extenddb-missing-{uuid.uuid4().hex[:12]}"


def _twice(call, **params) -> tuple[str, str]:
    """The answer to two identical requests, which must be equal."""
    first = _error(call, **params)
    assert _error(call, **params) == first, params
    return first


def _with_period(value, enabled=True) -> dict:
    return {"PointInTimeRecoveryEnabled": enabled, "RecoveryPeriodInDays": value}


@pytest.mark.parametrize(
    ("case", "value", "message"),
    [
        ("'7'", "7", "STRING_VALUE cannot be converted to Integer"),
        ("'abc'", "abc", "STRING_VALUE cannot be converted to Integer"),
        ("''", "", "STRING_VALUE cannot be converted to Integer"),
        ("'7.5'", "7.5", "STRING_VALUE cannot be converted to Integer"),
        ("true", True, "TRUE_VALUE cannot be converted to Integer"),
        ("false", False, "FALSE_VALUE cannot be converted to Integer"),
        ("[]", [], ""),
        ("{}", {}, "Start of structure or map found where not expected"),
    ],
)
def test_a_period_of_another_json_type_is_a_serialization_error(
    dynamodb_client_no_validation, case, value, message
):
    """Measured on the live service, two samples each."""
    assert _twice(
        dynamodb_client_no_validation.update_continuous_backups,
        TableName=_missing(),
        PointInTimeRecoverySpecification=_with_period(value),
    ) == ("SerializationException", message), case


@pytest.mark.parametrize(
    ("case", "params", "message"),
    [
        ("a TableName the pattern refuses", {"TableName": "bad name!"}, "STRING_VALUE cannot be converted to Integer"),
        ("no TableName", {}, "STRING_VALUE cannot be converted to Integer"),
        ("empty TableName", {"TableName": ""}, "STRING_VALUE cannot be converted to Integer"),
    ],
)
def test_the_period_type_is_checked_before_the_table_name(
    dynamodb_client_no_validation, case, params, message
):
    """A type error in RecoveryPeriodInDays comes before a missing TableName
    and before its constraints (measured)."""
    assert _twice(
        dynamodb_client_no_validation.update_continuous_backups,
        PointInTimeRecoverySpecification=_with_period("x"),
        **params,
    ) == ("SerializationException", message), case


def test_the_table_name_constraints_come_before_the_period_bounds(
    dynamodb_client_no_validation,
):
    """A value the member clamps is no type error: the TableName pattern is
    reported, and the period bounds are not (measured); without TableName
    the required-member text comes first."""
    update = dynamodb_client_no_validation.update_continuous_backups
    for spec in (_with_period(1e20), {"RecoveryPeriodInDays": 0}):
        assert _twice(
            update, TableName="bad name!", PointInTimeRecoverySpecification=spec
        ) == ("ValidationException", NAME_PATTERN), spec
    assert _twice(
        update, TableName="ab", PointInTimeRecoverySpecification={"RecoveryPeriodInDays": 0}
    ) == (
        "ValidationException",
        "1 validation error detected: Value 'ab' at 'tableName' failed to satisfy "
        "constraint: Member must have length greater than or equal to 3",
    )
    assert _twice(update, PointInTimeRecoverySpecification=_with_period(0)) == (
        "ValidationException",
        "The parameter 'TableName' is required but was not present in the request",
    )


def _two(value: str, constraint: str) -> str:
    return (
        f"2 validation errors detected: Value '{value}' at '{PERIOD_PATH}' failed to "
        f"satisfy constraint: Member must have value {constraint}; {FLAG_NULL}"
    )


@pytest.mark.parametrize(
    ("case", "spec", "message"),
    [
        ("0", {"RecoveryPeriodInDays": 0}, _two("0", "greater than or equal to 1")),
        ("36", {"RecoveryPeriodInDays": 36}, _two("36", "less than or equal to 35")),
        ("0.5", {"RecoveryPeriodInDays": 0.5}, _two("0", "greater than or equal to 1")),
        (
            "1e20",
            {"RecoveryPeriodInDays": 1e20},
            _two("2147483647", "less than or equal to 35"),
        ),
        (
            "-2147483649",
            {"RecoveryPeriodInDays": -2147483649},
            _two("-2147483648", "greater than or equal to 1"),
        ),
        (
            "null flag",
            {"PointInTimeRecoveryEnabled": None, "RecoveryPeriodInDays": 0},
            _two("0", "greater than or equal to 1"),
        ),
        ("7", {"RecoveryPeriodInDays": 7}, f"1 validation error detected: {FLAG_NULL}"),
    ],
)
def test_a_bad_period_and_a_missing_flag_are_reported_together(
    dynamodb_client_no_validation, case, spec, message
):
    assert _twice(
        dynamodb_client_no_validation.update_continuous_backups,
        TableName=_missing(),
        PointInTimeRecoverySpecification=spec,
    ) == ("ValidationException", message), case


@pytest.mark.parametrize(
    ("case", "value", "message"),
    [
        ("0.5", 0.5, _too_low(0)),
        ("0.9", 0.9, _too_low(0)),
        ("1e-5", 1e-5, _too_low(0)),
        ("-0.5", -0.5, _too_low(0)),
        ("-1.5", -1.5, _too_low(-1)),
        ("36.5", 36.5, _too_high(36)),
        ("1e20", 1e20, _too_high(2147483647)),
        ("2^31", 2**31, _too_high(2147483647)),
        ("2^63", 2**63, _too_high(2147483647)),
        ("2147483647.5", 2147483647.5, _too_high(2147483647)),
        ("-2^31-1", -(2**31) - 1, _too_low(-2147483648)),
        ("-1e20", -1e20, _too_low(-2147483648)),
    ],
)
def test_a_period_is_truncated_and_clamped_before_its_bounds(
    dynamodb_client_no_validation, case, value, message
):
    """Measured on the live service, two samples each."""
    assert _twice(
        dynamodb_client_no_validation.update_continuous_backups,
        TableName=_missing(),
        PointInTimeRecoverySpecification=_with_period(value),
    ) == ("ValidationException", message), case


def test_a_period_in_bounds_after_truncation_reaches_the_table(
    dynamodb_client_no_validation, create_and_cleanup_table, unique_table_name
):
    """7.5, 7.0, 10.0, 1.9 and 35.9 on a missing table are
    TableNotFoundException; on a disable 7.5 is the disable refusal; on an
    enabled table 7.9 is stored as 7 (measured, two samples each)."""
    client = dynamodb_client_no_validation
    missing = _missing()
    for value in (7.5, 7.0, 10.0, 1.9, 35.9):
        assert _twice(
            client.update_continuous_backups,
            TableName=missing,
            PointInTimeRecoverySpecification=_with_period(value),
        ) == ("TableNotFoundException", f"Table not found: {missing}"), value
    assert _twice(
        client.update_continuous_backups,
        TableName=missing,
        PointInTimeRecoverySpecification=_with_period(7.5, enabled=False),
    ) == ("ValidationException", PERIOD_ON_DISABLE)
    create_and_cleanup_table()
    _first_enable(client, unique_table_name)
    for _ in range(2):
        pitr = _update(
            client, unique_table_name, PointInTimeRecoveryEnabled=True, RecoveryPeriodInDays=7.9
        )["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
        assert pitr["RecoveryPeriodInDays"] == 7
    for _ in range(2):
        described = client.describe_continuous_backups(TableName=unique_table_name)
        pitr = described["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
        assert pitr["RecoveryPeriodInDays"] == 7


def _restore_source_items() -> int:
    """Enough items that loading the restore target outlasts several enable
    attempts on ExtendDB; on DynamoDB every restore takes minutes."""
    return 25 if not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip() else 20_000


def test_an_enable_of_a_restore_target_being_created_is_refused(
    dynamodb_client, create_and_cleanup_table, unique_table_name
):
    """While a RestoreTableFromBackup target is CREATING, an enable answers
    TableNotFoundException "Table not found: <name>" for the target's first
    seconds, then ContinuousBackupsUnavailableException "Backups are being
    enabled for the table: <name>. Please retry later" until it is ACTIVE
    (measured on DynamoDB, two targets polled every 4.5 s). ExtendDB gives
    the second answer for the whole load. So recovery is never reported
    enabled for a table whose load wrote no history: once the target is
    ACTIVE its recovery is DISABLED, and an enable then takes the loaded
    items as its baseline. An enable after a refusal that says the backups
    are being enabled may succeed as the load ends."""
    client = dynamodb_client
    create_and_cleanup_table()
    target = f"{unique_table_name}-r"
    not_found = ("TableNotFoundException", f"Table not found: {target}")
    starting = (
        "ContinuousBackupsUnavailableException",
        f"Backups are being enabled for the table: {target}. Please retry later",
    )
    total = _restore_source_items()
    for start in range(0, total, 25):
        client.batch_write_item(
            RequestItems={
                unique_table_name: [
                    {"PutRequest": {"Item": {"pk": {"S": f"k{i}"}, "v": {"S": "x" * 300}}}}
                    for i in range(start, min(start + 25, total))
                ]
            }
        )
    arn = _while_backups_start(
        lambda: client.create_backup(TableName=unique_table_name, BackupName="pitr-creating")
    )["BackupDetails"]["BackupArn"]
    try:
        deadline = time.time() + 600
        while (
            client.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"][
                "BackupStatus"
            ]
            != "AVAILABLE"
        ):
            assert time.time() < deadline, "the backup did not become AVAILABLE"
            time.sleep(1)
        client.restore_table_from_backup(TargetTableName=target, BackupArn=arn)
        try:
            answers = []
            enabled_at_the_flip = False
            deadline = time.time() + 1800
            while True:
                before = client.describe_table(TableName=target)["Table"]["TableStatus"]
                if before != "CREATING":
                    break
                try:
                    _update(client, target, PointInTimeRecoveryEnabled=True)
                    answer = None
                except ClientError as e:
                    answer = (e.response["Error"]["Code"], e.response["Error"]["Message"])
                if answer is None:
                    # The service accepts an enable at the end of the load
                    # while DescribeTable still reports CREATING (measured).
                    enabled_at_the_flip = True
                    break
                assert answer in (not_found, starting), answer
                assert not (answers and answers[-1] == starting and answer == not_found)
                answers.append(answer)
                assert time.time() < deadline, "the restore did not finish"
                time.sleep(0.05 if os.environ.get("EXTENDDB_TEST_ENDPOINT") else 4)
            assert starting in answers, answers
            client.get_waiter("table_exists").wait(TableName=target)
            for _ in range(2):
                pitr = client.describe_continuous_backups(TableName=target)[
                    "ContinuousBackupsDescription"
                ]["PointInTimeRecoveryDescription"]
                expected = "ENABLED" if enabled_at_the_flip else "DISABLED"
                assert pitr["PointInTimeRecoveryStatus"] == expected
            enabled = _first_enable(client, target)["ContinuousBackupsDescription"]
            assert (
                enabled["PointInTimeRecoveryDescription"]["PointInTimeRecoveryStatus"]
                == "ENABLED"
            )
        finally:
            _delete_when_possible(client, target)
    finally:
        client.delete_backup(BackupArn=arn)


def _delete_when_possible(client, name: str) -> None:
    deadline = time.time() + 1800
    while True:
        try:
            client.delete_table(TableName=name)
            break
        except client.exceptions.ResourceNotFoundException:
            return
        except client.exceptions.ResourceInUseException:
            assert time.time() < deadline, f"{name} could not be deleted"
            time.sleep(5)
    waiter = client.get_waiter("table_not_exists")
    waiter.wait(TableName=name, WaiterConfig={"Delay": 2, "MaxAttempts": 300})
