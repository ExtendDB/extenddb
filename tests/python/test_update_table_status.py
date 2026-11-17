# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The TableStatus an UpdateTable answers, and what the table serves meanwhile.

Measured on Amazon DynamoDB in us-east-1 on 13, 14 and 17 Nov 2026 (client
retries off), on tables keyed on one S attribute. A ProvisionedThroughput
change, a switch of BillingMode either way, a TableClass change and a stream
enable answer UPDATING, and a DescribeTable sent right after reports UPDATING
too; a DeletionProtectionEnabled, an OnDemandThroughput or a WarmThroughput
change alone answers ACTIVE. The table reports ACTIVE again once the change
applies. While a capacity change applies the service refuses another capacity
change with ResourceInUseException, answers a DeletionProtectionEnabled change
with ACTIVE while its DescribeTable still says UPDATING, and serves
CreateBackup, UpdateContinuousBackups, UpdateTimeToLive and PutItem.

Runs against ExtendDB (DYNAMODB_ENDPOINT) and against the service. On ExtendDB
the window lasts control_plane_delay_seconds (0.05 s in the test runs), so a
call that returned after the window could have ended can see ACTIVE; see
`_assert_opened_status`.
"""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted
from warm_throughput_helpers import EXTENDDB_WINDOW_SECONDS, _assert_opened_status

KEY = {
    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
}
PROVISIONED_5 = {
    "BillingMode": "PROVISIONED",
    "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
}
ON_DEMAND = {"BillingMode": "PAY_PER_REQUEST"}
IOPS = (
    "Attempt to change a resource which is still in use: Table IOPS are currently being "
    "updated. Table: "
)


def _stream(view_type: str) -> dict:
    return {"StreamSpecification": {"StreamEnabled": True, "StreamViewType": view_type}}


# The table before the request, the UpdateTable members, and the TableStatus
# the service answered, which its DescribeTable right after reported too.
ROWS = {
    "throughput": (
        PROVISIONED_5,
        {"ProvisionedThroughput": {"ReadCapacityUnits": 9, "WriteCapacityUnits": 11}},
        "UPDATING",
    ),
    "on_demand": (PROVISIONED_5, {"BillingMode": "PAY_PER_REQUEST"}, "UPDATING"),
    "provisioned": (ON_DEMAND, PROVISIONED_5, "UPDATING"),
    "table_class": (ON_DEMAND, {"TableClass": "STANDARD_INFREQUENT_ACCESS"}, "UPDATING"),
    "stream_keys_only": (ON_DEMAND, _stream("KEYS_ONLY"), "UPDATING"),
    "stream_new_image": (ON_DEMAND, _stream("NEW_IMAGE"), "UPDATING"),
    "stream_old_image": (ON_DEMAND, _stream("OLD_IMAGE"), "UPDATING"),
    "stream_new_and_old": (ON_DEMAND, _stream("NEW_AND_OLD_IMAGES"), "UPDATING"),
    "deletion_protection": (ON_DEMAND, {"DeletionProtectionEnabled": True}, "ACTIVE"),
    "on_demand_throughput": (
        ON_DEMAND,
        {"OnDemandThroughput": {"MaxReadRequestUnits": 100, "MaxWriteRequestUnits": 100}},
        "ACTIVE",
    ),
    "warm_throughput": (
        ON_DEMAND,
        {"WarmThroughput": {"ReadUnitsPerSecond": 12100, "WriteUnitsPerSecond": 4100}},
        "ACTIVE",
    ),
}

# Inside a capacity change's window: the requests the service refused with the
# IOPS sentence, and the ones it served.
REFUSED = {
    "throughput": {"ProvisionedThroughput": {"ReadCapacityUnits": 20, "WriteCapacityUnits": 20}},
    "on_demand": {"BillingMode": "PAY_PER_REQUEST"},
}
SERVED = [
    "deletion_protection",
    "create_backup",
    "update_continuous_backups",
    "update_time_to_live",
    "put_item",
]


def _call(fn, **kwargs) -> dict:
    """The response without its metadata, or the error's status, code and
    message, and the seconds the call took."""
    start = time.monotonic()
    try:
        resp = fn(**kwargs)
        resp.pop("ResponseMetadata", None)
        return {"status": 200, "response": resp, "took": time.monotonic() - start}
    except ClientError as e:
        return {
            "status": e.response["ResponseMetadata"]["HTTPStatusCode"],
            "code": e.response["Error"]["Code"],
            "message": e.response["Error"]["Message"],
            "took": time.monotonic() - start,
        }


@pytest.fixture(scope="module")
def run(dynamodb_client, is_extenddb):
    """Run every row once, in the order of the table, and record every answer."""
    ddb = dynamodb_client
    rows: dict = {}
    names: list[str] = []
    protected: list[str] = []
    try:
        for row, (before, members, _expected) in ROWS.items():
            name = unique_name("t1043")
            names.append(name)
            ddb.create_table(TableName=name, **KEY, **before)
            wait_for_active(ddb, name)
            start = time.monotonic()
            update = _call(ddb.update_table, TableName=name, **members)
            describe = _call(ddb.describe_table, TableName=name)
            describe["since_update"] = time.monotonic() - start
            wait_for_active(ddb, name, timeout=120)
            rows[row] = {"update": update, "describe": describe}
            if row == "deletion_protection":
                ddb.update_table(TableName=name, DeletionProtectionEnabled=False)
                wait_for_active(ddb, name)

        name = unique_name("t1043")
        names.append(name)
        ddb.create_table(TableName=name, **KEY, **PROVISIONED_5)
        wait_for_active(ddb, name)
        start = time.monotonic()
        first = _call(
            ddb.update_table,
            TableName=name,
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )
        refused = {}
        for kind, members in REFUSED.items():
            refused[kind] = _call(ddb.update_table, TableName=name, **members)
            refused[kind]["since_update"] = time.monotonic() - start
        protected.append(name)
        served = {
            "deletion_protection": _call(ddb.update_table, TableName=name, DeletionProtectionEnabled=True)
        }
        describe = _call(ddb.describe_table, TableName=name)
        describe["since_update"] = time.monotonic() - start
        for operation, method, kwargs in [
            ("create_backup", ddb.create_backup, {"BackupName": unique_name("b1043")}),
            ("update_continuous_backups", ddb.update_continuous_backups,
             {"PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": False}}),
            ("update_time_to_live", ddb.update_time_to_live,
             {"TimeToLiveSpecification": {"Enabled": True, "AttributeName": "ttl"}}),
            ("put_item", ddb.put_item, {"Item": {"pk": {"S": "a"}}}),
        ]:
            served[operation] = _call(method, TableName=name, **kwargs)
        rows["capacity"] = {
            "first": first,
            "refused": refused,
            "served": served,
            "describe": describe,
            "name": name,
        }
        wait_for_active(ddb, name, timeout=120)
        yield rows
    finally:
        backup = rows.get("capacity", {}).get("served", {}).get("create_backup", {})
        if backup.get("status") == 200:
            try:
                ddb.delete_backup(BackupArn=backup["response"]["BackupDetails"]["BackupArn"])
            except ClientError:
                pass
        for name in protected:
            try:
                wait_for_active(ddb, name, timeout=120)
                ddb.update_table(TableName=name, DeletionProtectionEnabled=False)
                wait_for_active(ddb, name, timeout=120)
            except (ClientError, TimeoutError):
                pass
        for name in names:
            try:
                ddb.delete_table(TableName=name)
                wait_for_deleted(ddb, name)
            except ClientError:
                pass


@pytest.mark.parametrize("row", list(ROWS))
def test_the_response_reports_the_measured_status(run, row, is_extenddb):
    expected = ROWS[row][2]
    update = run[row]["update"]
    assert update["status"] == 200, update
    desc = update["response"]["TableDescription"]
    _assert_opened_status(desc["TableStatus"], is_extenddb, update["took"], expected)


@pytest.mark.parametrize("row", list(ROWS))
def test_describe_table_right_after_reports_the_same_status(run, row, is_extenddb):
    expected = ROWS[row][2]
    describe = run[row]["describe"]
    assert describe["status"] == 200, describe
    _assert_opened_status(
        describe["response"]["Table"]["TableStatus"],
        is_extenddb,
        describe["since_update"],
        expected,
    )


@pytest.mark.parametrize("kind", list(REFUSED))
def test_a_capacity_change_inside_the_window_is_refused(run, kind, is_extenddb):
    answer = run["capacity"]["refused"][kind]
    if is_extenddb and answer["since_update"] >= EXTENDDB_WINDOW_SECONDS and answer["status"] == 200:
        return
    assert (answer["status"], answer["code"], answer["message"]) == (
        400,
        "ResourceInUseException",
        IOPS + run["capacity"]["name"],
    ), answer


@pytest.mark.parametrize("operation", SERVED)
def test_the_table_is_served_inside_the_window(run, operation):
    assert run["capacity"]["first"]["status"] == 200, run["capacity"]["first"]
    assert run["capacity"]["served"][operation]["status"] == 200, run["capacity"]["served"][operation]


def test_deletion_protection_inside_the_window_answers_active(run, is_extenddb):
    served = run["capacity"]["served"]["deletion_protection"]
    assert served["response"]["TableDescription"]["TableStatus"] == "ACTIVE"
    describe = run["capacity"]["describe"]
    assert describe["status"] == 200, describe
    _assert_opened_status(
        describe["response"]["Table"]["TableStatus"], is_extenddb, describe["since_update"]
    )
