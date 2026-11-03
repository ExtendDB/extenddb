# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Backup and restore reproduce the table they were taken from.

Every backend; hash-only keys and composite keys with S, N, and B sort keys,
plus secondary indexes and provisioned throughput. A restore must reach ACTIVE
with the source's key schema, attribute definitions, and exactly the source's
items, attribute for attribute. (Multi-part base keys are a preview that
restore refuses; that refusal is tested at the storage level.) Comparing whole items rather than counts is what catches a
backend that restores the right number of rows under the wrong keys.

Readiness assessment P0-2: on PostgreSQL every table with a sort key backed up
without its sort keys, and restoring it left the target in CREATING forever.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError, ReadTimeoutError

from conftest import _is_real_dynamodb, _poll_interval, wait_for_active, wait_for_deleted

# Checks of ExtendDB-only behaviour (its own refusal texts, members not
# measured on DynamoDB). Everything else in this module holds
# against DynamoDB too.
extenddb_only = pytest.mark.skipif(
    _is_real_dynamodb(), reason="extenddb_only: skipped against DynamoDB"
)

ITEMS_PER_TABLE = 60
# The service takes minutes to make a backup AVAILABLE and to restore a table.
BACKUP_TIMEOUT_S = 900.0
RESTORE_TIMEOUT_S = 1800.0


def _create_backup(client, table_name: str) -> str:
    """CreateBackup, then wait for the backup to be AVAILABLE.

    The service creates a backup asynchronously and refuses to restore one
    that is still CREATING; ExtendDB returns it AVAILABLE. A just-created
    table can also refuse CreateBackup for a short while, which is retried.
    """
    deadline = time.monotonic() + 60
    while True:
        try:
            arn = client.create_backup(
                TableName=table_name, BackupName=f"{table_name}-bkp"
            )["BackupDetails"]["BackupArn"]
            break
        except ClientError as e:
            code = e.response["Error"]["Code"]
            retryable = code in ("ContinuousBackupsUnavailableException", "TableInUseException")
            if not retryable or time.monotonic() >= deadline:
                raise
            time.sleep(0.5)
    deadline = time.monotonic() + BACKUP_TIMEOUT_S
    try:
        while True:
            status = client.describe_backup(BackupArn=arn)["BackupDescription"][
                "BackupDetails"
            ]["BackupStatus"]
            if status == "AVAILABLE":
                return arn
            assert status == "CREATING", f"backup {arn} is {status}"
            assert time.monotonic() < deadline, f"backup {arn} not AVAILABLE in time"
            time.sleep(_poll_interval())
    except BaseException:
        # The caller never learns the ARN, so the backup is removed here.
        try:
            client.delete_backup(BackupArn=arn)
        except ClientError:
            pass
        raise


def _backup_creation_time(client, backup_arn: str):
    return client.describe_backup(BackupArn=backup_arn)["BackupDescription"][
        "BackupDetails"
    ]["BackupCreationDateTime"]


def _assert_restore_summary(summary: dict, backup_arn: str, source_arn: str, created_at):
    """A CREATING restore target's RestoreSummary, as the service reports it.

    RestoreDateTime is the backup's creation time, not the restore's wall
    clock (measured on DynamoDB).
    """
    assert summary["SourceBackupArn"] == backup_arn
    assert summary["SourceTableArn"] == source_arn
    assert summary["RestoreDateTime"] == created_at
    assert summary["RestoreInProgress"] is True


def _wait_for_restore(
    client, table_name: str, backup_arn: str, source_arn: str, created_at,
    timeout: float = RESTORE_TIMEOUT_S,
) -> bool:
    """Poll a restore target to ACTIVE; return whether it was seen CREATING.

    While CREATING, DescribeTable carries the RestoreSummary; once ACTIVE the
    member is gone (the service drops it at ACTIVE, measured on DynamoDB,
    and ExtendDB does the same).
    """
    seen_creating = False
    deadline = time.monotonic() + timeout
    while True:
        table = client.describe_table(TableName=table_name)["Table"]
        status = table["TableStatus"]
        if status == "ACTIVE":
            assert "RestoreSummary" not in table, table.get("RestoreSummary")
            return seen_creating
        assert status == "CREATING", f"restore target {table_name} is {status}"
        seen_creating = True
        _assert_restore_summary(table["RestoreSummary"], backup_arn, source_arn, created_at)
        assert time.monotonic() < deadline, f"{table_name} not ACTIVE in {timeout}s"
        time.sleep(_poll_interval())


def _scan_all(client, table_name: str) -> list[dict]:
    items: list[dict] = []
    kwargs: dict = {"TableName": table_name, "ConsistentRead": True}
    while True:
        resp = client.scan(**kwargs)
        items += resp["Items"]
        if "LastEvaluatedKey" not in resp:
            return items
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def _canonical(item: dict) -> str:
    """Order-independent, type-preserving form of a wire item for comparison."""
    import json

    def norm(v):
        if isinstance(v, dict):
            return {k: norm(v[k]) for k in sorted(v)}
        if isinstance(v, list):
            return [norm(x) for x in v]
        if isinstance(v, (bytes, bytearray)):
            return {"__bytes__": bytes(v).hex()}
        return v

    # Set members have no order on the wire.
    def sort_sets(av):
        if isinstance(av, dict):
            out = {}
            for t, val in av.items():
                if t in ("SS", "NS"):
                    out[t] = sorted(val)
                elif t == "BS":
                    out[t] = sorted(bytes(b).hex() for b in val)
                elif t == "M":
                    out[t] = {k: sort_sets(x) for k, x in val.items()}
                elif t == "L":
                    out[t] = [sort_sets(x) for x in val]
                else:
                    out[t] = val
            return out
        return av

    return json.dumps(norm({k: sort_sets(v) for k, v in item.items()}), sort_keys=True)


def _drop(client, *names: str) -> None:
    """Delete tables, waiting out ones still being created or restored."""
    for name in names:
        deadline = time.monotonic() + RESTORE_TIMEOUT_S
        while True:
            try:
                client.delete_table(TableName=name)
                break
            except client.exceptions.ResourceNotFoundException:
                break
            except client.exceptions.ResourceInUseException:
                # The service refuses to delete a table that is CREATING,
                # which a restore target is until the restore finishes.
                if time.monotonic() >= deadline:
                    raise
                time.sleep(_poll_interval() * 25)
        wait_for_deleted(client, name)


def _sort_value(kind: str, i: int) -> dict:
    if kind == "S":
        return {"S": f"sort-{i:04d}"}
    if kind == "N":
        # Negative, fractional, and large magnitudes in one column.
        return {"N": str((i - 30) * 1.5 if i % 3 else (i - 30) * 10**20)}
    return {"B": i.to_bytes(2, "big") + b"\x00\xff"}


def _hash_value(kind: str, n: int) -> dict:
    if kind == "S":
        return {"S": f"part-{n}"}
    if kind == "N":
        return {"N": str(n * 7 - 100)}
    return {"B": b"\x00p" + n.to_bytes(2, "big")}


def _item(i: int, sort_kind: str | None, hash_kind: str = "S") -> dict:
    # With a sort key, several items share a partition; without one, every
    # partition key must be distinct or the puts overwrite each other.
    item: dict = {
        "pk": _hash_value(hash_kind, i % 7 if sort_kind else i),
        "str": {"S": f"value-{i}"},
        "num": {"N": str(i)},
        "nested": {"M": {"list": {"L": [{"N": "1"}, {"S": "two"}, {"BOOL": i % 2 == 0}]}}},
        "tags": {"SS": [f"t{i}", "shared"]},
        "blob": {"B": bytes([i % 256]) * 3},
    }
    if i % 5 == 0:
        item["maybe"] = {"NULL": True}
    if sort_kind:
        item["sk"] = _sort_value(sort_kind, i)
    return item


def _query_all(client, **kwargs) -> list[dict]:
    items: list[dict] = []
    while True:
        resp = client.query(**kwargs)
        items += resp["Items"]
        if "LastEvaluatedKey" not in resp:
            return items
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def _index_items(client, table: str, index: str, hash_attr: str, values: list) -> list[str]:
    """Canonical items an index serves, across every listed partition.

    Values are strings for an S hash key, or typed attribute values.
    """
    out: list[str] = []
    for v in values:
        out += [
            _canonical(i)
            for i in _query_all(
                client,
                TableName=table,
                IndexName=index,
                KeyConditionExpression="#h = :v",
                ExpressionAttributeNames={"#h": hash_attr},
                ExpressionAttributeValues={":v": v if isinstance(v, dict) else {"S": v}},
            )
        ]
    return sorted(out)


def _cleanup(client, backup_arn: str | None, *tables: str) -> None:
    """Delete tables and the backup; each step runs even if an earlier one fails."""
    if tables:
        try:
            _drop(client, tables[0])
        finally:
            _cleanup(client, backup_arn, *tables[1:])
        return
    if backup_arn:
        try:
            client.delete_backup(BackupArn=backup_arn)
        except client.exceptions.BackupNotFoundException:
            pass


def _round_trip(client, sort_kind: str | None, hash_kind: str = "S") -> None:
    source = f"restore-fid-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    key_schema = [{"AttributeName": "pk", "KeyType": "HASH"}]
    attr_defs = [{"AttributeName": "pk", "AttributeType": hash_kind}]
    if sort_kind:
        key_schema.append({"AttributeName": "sk", "KeyType": "RANGE"})
        attr_defs.append({"AttributeName": "sk", "AttributeType": sort_kind})
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs,
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, source)
        for i in range(ITEMS_PER_TABLE):
            client.put_item(TableName=source, Item=_item(i, sort_kind, hash_kind))
        # Compare against what the source serves, not what was sent: numbers
        # come back canonicalized (`-42.0` reads as `-42`), here as in DynamoDB.
        expected = _scan_all(client, source)
        assert len(expected) == ITEMS_PER_TABLE

        backup_arn = _create_backup(client, source)
        source_arn = client.describe_table(TableName=source)["Table"]["TableArn"]
        created_at = _backup_creation_time(client, backup_arn)
        resp = client.restore_table_from_backup(TargetTableName=restored, BackupArn=backup_arn)
        assert resp["TableDescription"]["TableName"] == restored
        assert resp["TableDescription"]["TableStatus"] == "CREATING"
        # Omitted from this response by the service (measured on DynamoDB);
        # DescribeTable carries both.
        assert "DeletionProtectionEnabled" not in resp["TableDescription"]
        assert "NumberOfDecreasesToday" not in resp["TableDescription"]["ProvisionedThroughput"]
        _assert_restore_summary(
            resp["TableDescription"]["RestoreSummary"], backup_arn, source_arn, created_at
        )
        _wait_for_restore(client, restored, backup_arn, source_arn, created_at)

        table = client.describe_table(TableName=restored)["Table"]
        assert table["KeySchema"] == key_schema
        assert sorted(table["AttributeDefinitions"], key=lambda a: a["AttributeName"]) == sorted(
            attr_defs, key=lambda a: a["AttributeName"]
        )
        assert table["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"

        got = sorted(_canonical(i) for i in _scan_all(client, restored))
        want = sorted(_canonical(i) for i in expected)
        assert len(got) == len(want), f"restored {len(got)} of {len(want)} items"
        assert got == want

        # Point reads by full key, so a restore that stored the right items
        # under the wrong key columns fails here even if Scan looked right.
        for item in expected[:10]:
            key = {"pk": item["pk"]}
            if sort_kind:
                key["sk"] = item["sk"]
            fetched = client.get_item(TableName=restored, Key=key, ConsistentRead=True)
            assert _canonical(fetched.get("Item", {})) == _canonical(item)

        # The restored table takes writes like any other.
        client.put_item(
            TableName=restored, Item=_item(ITEMS_PER_TABLE, sort_kind, hash_kind)
        )
    finally:
        _cleanup(client, backup_arn, restored, source)


@pytest.mark.parametrize("hash_kind", ["S", "N", "B"])
def test_restore_hash_only_table(dynamodb_client, hash_kind):
    _round_trip(dynamodb_client, None, hash_kind)


@pytest.mark.parametrize("sort_kind", ["S", "N", "B"])
def test_restore_composite_key_table(dynamodb_client, sort_kind):
    _round_trip(dynamodb_client, sort_kind)


def _gsi(name: str, hash_attr: str, range_attr: str | None, projection: dict, **extra) -> dict:
    ks = [{"AttributeName": hash_attr, "KeyType": "HASH"}]
    if range_attr:
        ks.append({"AttributeName": range_attr, "KeyType": "RANGE"})
    return {"IndexName": name, "KeySchema": ks, "Projection": projection, **extra}


def _index_by_name(indexes: list[dict]) -> dict:
    return {i["IndexName"]: i for i in indexes}


def test_restore_preserves_indexes_and_provisioned_throughput(dynamodb_client):
    """GSIs and LSIs, their key schemas, projections, and throughput, the
    table's provisioned throughput, and every index's contents come back."""
    client = dynamodb_client
    source = f"restore-idx-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    gsi_tp = {"ReadCapacityUnits": 3, "WriteCapacityUnits": 4}
    gsis = [
        _gsi("by_owner", "owner", "rank", {"ProjectionType": "ALL"}, ProvisionedThroughput=gsi_tp),
        _gsi(
            "by_kind",
            "kind",
            None,
            {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["note"]},
            ProvisionedThroughput=gsi_tp,
        ),
        _gsi("by_kind_keys", "kind", "rank", {"ProjectionType": "KEYS_ONLY"},
             ProvisionedThroughput=gsi_tp),
        _gsi("by_code", "code", "tag", {"ProjectionType": "ALL"},
             ProvisionedThroughput=gsi_tp),
    ]
    lsis = [
        {
            "IndexName": "by_rank",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "rank", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }
    ]
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "owner", "AttributeType": "S"},
                {"AttributeName": "kind", "AttributeType": "S"},
                {"AttributeName": "rank", "AttributeType": "N"},
                {"AttributeName": "code", "AttributeType": "N"},
                {"AttributeName": "tag", "AttributeType": "B"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 7, "WriteCapacityUnits": 9},
            GlobalSecondaryIndexes=gsis,
            LocalSecondaryIndexes=lsis,
        )
        wait_for_active(client, source)
        owners = [f"owner-{i}" for i in range(3)]
        kinds = ["red", "blue"]
        for i in range(ITEMS_PER_TABLE):
            item = {
                "pk": {"S": f"part-{i % 4}"},
                "sk": {"S": f"sort-{i:04d}"},
                # Unique, so every ordered index read is fully determined.
                "rank": {"N": str(i * 3 - 50)},
                "code": {"N": str(i % 5 - 2)},
                "tag": {"B": bytes([255 - i, i])},
                "note": {"S": f"note-{i}"},
                "other": {"S": "not projected into by_kind"},
            }
            # Sparse: some items lack one index key or the other.
            if i % 3:
                item["owner"] = {"S": owners[i % 3]}
            if i % 4:
                item["kind"] = {"S": kinds[i % 2]}
            client.put_item(TableName=source, Item=item)

        # What each index serves on the source, read after the source's GSIs
        # have caught up with the writes.
        def index_view(table: str) -> dict:
            return {
                "by_owner": _index_items(client, table, "by_owner", "owner", owners),
                "by_kind": _index_items(client, table, "by_kind", "kind", kinds),
                "by_kind_keys": _index_items(client, table, "by_kind_keys", "kind", kinds),
                "by_rank": _index_items(client, table, "by_rank", "pk",
                                        [f"part-{p}" for p in range(4)]),
                "by_code": _index_items(client, table, "by_code", "code",
                                        [{"N": str(c)} for c in range(-2, 3)]),
            }

        expected_sizes = {
            "by_owner": sum(1 for i in range(ITEMS_PER_TABLE) if i % 3),
            "by_kind": sum(1 for i in range(ITEMS_PER_TABLE) if i % 4),
            "by_kind_keys": sum(1 for i in range(ITEMS_PER_TABLE) if i % 4),
            "by_rank": ITEMS_PER_TABLE,
            "by_code": ITEMS_PER_TABLE,
        }
        deadline = time.monotonic() + 120
        while True:
            want = index_view(source)
            sizes = {k: len(v) for k, v in want.items()}
            if sizes == expected_sizes:
                break
            assert time.monotonic() < deadline, f"source GSIs did not converge: {sizes}"
            time.sleep(_poll_interval() * 10)

        backup_arn = _create_backup(client, source)
        client.restore_table_from_backup(TargetTableName=restored, BackupArn=backup_arn)
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)

        table = client.describe_table(TableName=restored)["Table"]
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 7
        assert table["ProvisionedThroughput"]["WriteCapacityUnits"] == 9
        got_gsis = _index_by_name(table.get("GlobalSecondaryIndexes", []))
        assert sorted(got_gsis) == sorted(g["IndexName"] for g in gsis)
        for g in gsis:
            r = got_gsis[g["IndexName"]]
            assert r["KeySchema"] == g["KeySchema"]
            assert r["Projection"] == g["Projection"]
            assert r["ProvisionedThroughput"]["ReadCapacityUnits"] == 3
            assert r["ProvisionedThroughput"]["WriteCapacityUnits"] == 4
        for g in gsis:
            assert got_gsis[g["IndexName"]]["IndexStatus"] == "ACTIVE"
        got_lsis = _index_by_name(table.get("LocalSecondaryIndexes", []))
        assert sorted(got_lsis) == ["by_rank"]
        assert got_lsis["by_rank"]["KeySchema"] == lsis[0]["KeySchema"]
        assert got_lsis["by_rank"]["Projection"] == lsis[0]["Projection"]

        # The service backfills GSIs after the table turns ACTIVE; poll.
        deadline = time.monotonic() + RESTORE_TIMEOUT_S
        while True:
            got = index_view(restored)
            if got == want:
                break
            assert time.monotonic() < deadline, {
                k: (len(got[k]), len(want[k])) for k in want
            }
            time.sleep(_poll_interval() * 10)

        # Projections checked against their definitions, not just against the
        # source table, so a projection bug both tables share still fails.
        def attr_names(index: str, hash_attr: str, value: dict) -> set[frozenset]:
            return {
                frozenset(i)
                for i in _query_all(
                    client,
                    TableName=restored,
                    IndexName=index,
                    KeyConditionExpression="#h = :v",
                    ExpressionAttributeNames={"#h": hash_attr},
                    ExpressionAttributeValues={":v": value},
                )
            }

        assert attr_names("by_kind", "kind", {"S": "blue"}) == {
            frozenset({"pk", "sk", "kind", "note"})
        }
        assert attr_names("by_kind_keys", "kind", {"S": "blue"}) == {
            frozenset({"pk", "sk", "kind", "rank"})
        }
        all_attrs = attr_names("by_owner", "owner", {"S": owners[1]})
        assert all_attrs and all({"other", "note", "rank", "tag"} <= a for a in all_attrs)

        # Ordered, ranged reads on a numeric index sort key come back in the
        # same order from both tables, in both directions.
        for forward in (True, False):
            def ranked(table: str) -> list[str]:
                return [
                    _canonical(i)
                    for i in _query_all(
                        client,
                        TableName=table,
                        IndexName="by_owner",
                        KeyConditionExpression="#o = :o AND #r BETWEEN :lo AND :hi",
                        ExpressionAttributeNames={"#o": "owner", "#r": "rank"},
                        ExpressionAttributeValues={
                            ":o": {"S": owners[1]},
                            ":lo": {"N": "-20"},
                            ":hi": {"N": "100"},
                        },
                        ScanIndexForward=forward,
                    )
                ]

            src_order = ranked(source)
            assert src_order
            assert ranked(restored) == src_order
    finally:
        _cleanup(client, backup_arn, restored, source)


def test_restore_normalizes_stale_throughput_after_on_demand_switch(dynamodb_client):
    """A backup taken after an on-demand switch does not restore stale capacity."""
    client = dynamodb_client
    source = f"restore-ondemand-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
            ],
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 7, "WriteCapacityUnits": 9},
            GlobalSecondaryIndexes=[
                _gsi(
                    "by_gpk",
                    "gpk",
                    None,
                    {"ProjectionType": "ALL"},
                    ProvisionedThroughput={
                        "ReadCapacityUnits": 3,
                        "WriteCapacityUnits": 4,
                    },
                )
            ],
        )
        wait_for_active(client, source)
        client.update_table(TableName=source, BillingMode="PAY_PER_REQUEST")
        # On the service a billing-mode switch leaves the table UPDATING for
        # longer than the default 120 s wait (the test timed out there with the
        # default wait); ExtendDB switches at once.
        wait_for_active(client, source, timeout=RESTORE_TIMEOUT_S)

        backup_arn = _create_backup(client, source)
        if not _is_real_dynamodb():
            # Restoring it PROVISIONED again with no GSI list is refused: the
            # override asks for each GSI's throughput from the request, and
            # whatever throughput the backup still records for the index from
            # before the switch does not stand in for it (the service's reply
            # to this exact request was not measured; its text conditions on
            # the override being present).
            with pytest.raises(ClientError) as exc_info:
                client.restore_table_from_backup(
                    TargetTableName=restored,
                    BackupArn=backup_arn,
                    BillingModeOverride="PROVISIONED",
                    ProvisionedThroughputOverride={
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5,
                    },
                )
            assert exc_info.value.response["Error"]["Message"] == (
                "Invalid Request: Must override ProvisionedThroughput if "
                "BillingMode is overridden to PROVISIONED for index by_gpk"
            )
            with pytest.raises(client.exceptions.ResourceNotFoundException):
                client.describe_table(TableName=restored)
        client.restore_table_from_backup(TargetTableName=restored, BackupArn=backup_arn)
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)

        table = client.describe_table(TableName=restored)["Table"]
        assert table["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 0
        assert table["ProvisionedThroughput"]["WriteCapacityUnits"] == 0
        gsi = _index_by_name(table["GlobalSecondaryIndexes"])["by_gpk"]
        assert gsi["ProvisionedThroughput"]["ReadCapacityUnits"] == 0
        assert gsi["ProvisionedThroughput"]["WriteCapacityUnits"] == 0
    finally:
        _cleanup(client, backup_arn, restored, source)


def test_restore_override_pay_per_request_to_provisioned(dynamodb_client):
    client = dynamodb_client
    source = f"restore-override-ppr-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, source)

        backup_arn = _create_backup(client, source)
        client.restore_table_from_backup(
            TargetTableName=restored,
            BackupArn=backup_arn,
            BillingModeOverride="PROVISIONED",
            ProvisionedThroughputOverride={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
        )
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)

        table = client.describe_table(TableName=restored)["Table"]
        # BillingModeSummary is omitted for a provisioned table by both the
        # service and ExtendDB; omission therefore means PROVISIONED here.
        assert table.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED") == (
            "PROVISIONED"
        )
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 5
        assert table["ProvisionedThroughput"]["WriteCapacityUnits"] == 5
    finally:
        _cleanup(client, backup_arn, restored, source)


def test_restore_override_provisioned_to_pay_per_request(dynamodb_client):
    client = dynamodb_client
    source = f"restore-override-prov-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PROVISIONED",
            ProvisionedThroughput={
                "ReadCapacityUnits": 7,
                "WriteCapacityUnits": 9,
            },
        )
        wait_for_active(client, source)

        backup_arn = _create_backup(client, source)
        client.restore_table_from_backup(
            TargetTableName=restored,
            BackupArn=backup_arn,
            BillingModeOverride="PAY_PER_REQUEST",
        )
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)

        table = client.describe_table(TableName=restored)["Table"]
        assert table["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 0
        assert table["ProvisionedThroughput"]["WriteCapacityUnits"] == 0
    finally:
        _cleanup(client, backup_arn, restored, source)


def test_provisioned_restore_override_requiring_gsi_override_leaves_no_target(
    dynamodb_client,
):
    client = dynamodb_client
    source = f"restore-override-gsi-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                _gsi("by_gpk", "gpk", None, {"ProjectionType": "ALL"})
            ],
        )
        wait_for_active(client, source)
        backup_arn = _create_backup(client, source)

        with pytest.raises(ClientError) as exc_info:
            client.restore_table_from_backup(
                TargetTableName=restored,
                BackupArn=backup_arn,
                BillingModeOverride="PROVISIONED",
                ProvisionedThroughputOverride={
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            )
        error = exc_info.value.response["Error"]
        assert error["Code"] == "ValidationException"
        # The service's reply to this request, measured on DynamoDB; the same
        # on ExtendDB.
        assert error["Message"] == (
            "Invalid Request: Must override ProvisionedThroughput if "
            "BillingMode is overridden to PROVISIONED for index by_gpk"
        )
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_table(TableName=restored)

        # The same with a GSI list that omits the throughput: refused too.
        with pytest.raises(ClientError) as exc_info:
            client.restore_table_from_backup(
                TargetTableName=restored,
                BackupArn=backup_arn,
                BillingModeOverride="PROVISIONED",
                ProvisionedThroughputOverride={
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
                GlobalSecondaryIndexOverride=[
                    _gsi("by_gpk", "gpk", None, {"ProjectionType": "ALL"})
                ],
            )
        assert exc_info.value.response["Error"]["Message"] == (
            "Invalid Request: Must override ProvisionedThroughput if "
            "BillingMode is overridden to PROVISIONED for index by_gpk"
        )
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_table(TableName=restored)

        # The route that works: the list supplies the index's throughput.
        client.restore_table_from_backup(
            TargetTableName=restored,
            BackupArn=backup_arn,
            BillingModeOverride="PROVISIONED",
            ProvisionedThroughputOverride={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            GlobalSecondaryIndexOverride=[
                _gsi(
                    "by_gpk",
                    "gpk",
                    None,
                    {"ProjectionType": "ALL"},
                    ProvisionedThroughput={"ReadCapacityUnits": 2, "WriteCapacityUnits": 3},
                )
            ],
        )
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)
        table = client.describe_table(TableName=restored)["Table"]
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 5
        gsi = _index_by_name(table["GlobalSecondaryIndexes"])["by_gpk"]
        assert gsi["ProvisionedThroughput"]["ReadCapacityUnits"] == 2
        assert gsi["ProvisionedThroughput"]["WriteCapacityUnits"] == 3

        if not _is_real_dynamodb():
            # The restored table is PROVISIONED with a GSI that records its
            # throughput. Backing it up and restoring with the override and no
            # GSI list is refused the same way: the override, not the recorded
            # throughput, decides (the service's reply to a PROVISIONED backup
            # was not measured). Without the override the backup restores
            # with its recorded GSI throughput.
            second_arn = _create_backup(client, restored)
            try:
                with pytest.raises(ClientError) as exc_info:
                    client.restore_table_from_backup(
                        TargetTableName=restored + "2",
                        BackupArn=second_arn,
                        BillingModeOverride="PROVISIONED",
                        ProvisionedThroughputOverride={
                            "ReadCapacityUnits": 5,
                            "WriteCapacityUnits": 5,
                        },
                    )
                assert exc_info.value.response["Error"]["Message"] == (
                    "Invalid Request: Must override ProvisionedThroughput if "
                    "BillingMode is overridden to PROVISIONED for index by_gpk"
                )
                with pytest.raises(client.exceptions.ResourceNotFoundException):
                    client.describe_table(TableName=restored + "2")
                client.restore_table_from_backup(
                    TargetTableName=restored + "2", BackupArn=second_arn
                )
                wait_for_active(client, restored + "2", timeout=RESTORE_TIMEOUT_S)
                table = client.describe_table(TableName=restored + "2")["Table"]
                gsi = _index_by_name(table["GlobalSecondaryIndexes"])["by_gpk"]
                assert gsi["ProvisionedThroughput"]["ReadCapacityUnits"] == 2
                assert gsi["ProvisionedThroughput"]["WriteCapacityUnits"] == 3
            finally:
                _cleanup(client, second_arn, restored + "2")
    finally:
        _cleanup(client, backup_arn, restored, source)


# ---------------------------------------------------------------------------
# Restore runs after the response, and the rest of the backup API surface as
# measured on DynamoDB.
# ---------------------------------------------------------------------------

# Big enough that the copy is still running when the restore response and the
# probes in the window test arrive, on every backend (the tests fail rather
# than pass vacuously when it is too small). Override with
# EXTENDDB_TEST_BACKUP_ITEMS.
LARGE_ITEMS = int(os.environ.get("EXTENDDB_TEST_BACKUP_ITEMS", "60000"))
# The response test's client timeout. Cassandra's response waits for the
# target's schema, which under a parallel test suite can take several seconds;
# the check that the copy runs after the response is that the target is still
# CREATING when polled, not this timeout.
LARGE_READ_TIMEOUT_S = 10


def _new_client(**config):
    """A client of its own, configured like the conftest fixture.

    boto3 clients are not shared across threads here (lesson 7), and the
    timeout test needs one with its own read timeout and no retries.
    """
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }
    if config:
        kwargs["config"] = Config(**config)
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    if endpoint:
        kwargs["endpoint_url"] = endpoint
        if endpoint.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


def _count(client, table_name: str) -> int:
    total = 0
    kwargs: dict = {"TableName": table_name, "Select": "COUNT", "ConsistentRead": True}
    while True:
        resp = client.scan(**kwargs)
        total += resp["Count"]
        if "LastEvaluatedKey" not in resp:
            return total
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def _load(table_name: str, n: int, client_factory=None) -> None:
    """Write n items with BatchWriteItem from 8 threads, one client each."""
    local = threading.local()
    client_factory = client_factory or _new_client

    def write(start: int) -> None:
        if not hasattr(local, "client"):
            local.client = client_factory()
        requests = [
            {
                "PutRequest": {
                    "Item": {
                        "pk": {"S": f"item-{i:07d}"},
                        "gpk": {"S": f"g{i % 50}"},
                        "gsk": {"S": f"s{i % 7}"},
                        "pad": {"S": "x" * 200},
                    }
                }
            }
            for i in range(start, min(start + 25, n))
        ]
        pending = {table_name: requests}
        while pending:
            pending = local.client.batch_write_item(RequestItems=pending).get(
                "UnprocessedItems"
            ) or {}
            if pending:
                time.sleep(0.05)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(0, n, 25)))


@pytest.fixture(scope="module")
def large_backup(dynamodb_client):
    """A PAY_PER_REQUEST table of LARGE_ITEMS items, and a backup of it.

    The table has no secondary indexes: those are covered by the smaller
    tests above. With GSIs, loading the table and building its indexes (and
    each restore's) kept a backend's asynchronous GSI queue or index
    backfill worker busy for minutes, which delayed the GSI tests of other
    modules running at the same time.
    """
    client = dynamodb_client
    source = f"restore-large-{uuid.uuid4().hex[:10]}"
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, source)
        _load(source, LARGE_ITEMS)
        assert _count(client, source) == LARGE_ITEMS
        backup_arn = _create_backup(client, source)
        yield {
            "source": source,
            "source_arn": client.describe_table(TableName=source)["Table"]["TableArn"],
            "backup_arn": backup_arn,
            "created_at": _backup_creation_time(client, backup_arn),
        }
    finally:
        _cleanup(client, backup_arn, source)


def test_restore_response_arrives_before_its_copy_finishes(dynamodb_client, large_backup):
    """The CREATING response reaches a client with a 10 s read timeout and no
    retries before the copy finishes, and the table then reaches ACTIVE with
    every item.

    This shows the response does not wait for the copy. It does not drop a
    client; test_restore_survives_a_client_that_disconnects_before_the_response
    does. Before the copy was detached, PostgreSQL and SQLite answered only
    after the copy, so the polled target was already ACTIVE.
    """
    impatient = _new_client(
        read_timeout=LARGE_READ_TIMEOUT_S,
        connect_timeout=5,
        retries={"total_max_attempts": 1, "mode": "standard"},
    )
    target = f"{large_backup['source']}-t"
    try:
        sent = time.monotonic()
        resp = impatient.restore_table_from_backup(
            TargetTableName=target, BackupArn=large_backup["backup_arn"]
        )
        answered = time.monotonic() - sent
        assert resp["TableDescription"]["TableStatus"] == "CREATING"
        assert answered < LARGE_READ_TIMEOUT_S
        _assert_restore_summary(
            resp["TableDescription"]["RestoreSummary"],
            large_backup["backup_arn"],
            large_backup["source_arn"],
            large_backup["created_at"],
        )
        seen_creating = _wait_for_restore(
            dynamodb_client,
            target,
            large_backup["backup_arn"],
            large_backup["source_arn"],
            large_backup["created_at"],
        )
        took = time.monotonic() - sent
        # The response came while the copy was still running: an in-request
        # copy answers with the table ACTIVE and is never seen CREATING.
        assert seen_creating, (
            f"restore of {LARGE_ITEMS} items was never seen CREATING (ACTIVE after "
            f"{took:.2f}s); raise EXTENDDB_TEST_BACKUP_ITEMS"
        )
        print(f"restore of {LARGE_ITEMS} items: response {answered:.3f}s, ACTIVE {took:.2f}s")
        assert _count(dynamodb_client, target) == LARGE_ITEMS
    finally:
        _cleanup(dynamodb_client, None, target)


def _table_appears(client, name: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while True:
        try:
            client.describe_table(TableName=name)
            return True
        except client.exceptions.ResourceNotFoundException:
            if time.monotonic() >= deadline:
                return False
            time.sleep(_poll_interval() * 5)


def test_restore_survives_a_client_that_disconnects_before_the_response(
    dynamodb_client, large_backup
):
    """A client that sends RestoreTableFromBackup and closes its connection
    before any response (read timeout 1 ms, no retries) leaves no half-made
    state: either the restore runs and the target reaches ACTIVE with every
    item, or the request was cancelled before it was registered and the name
    and the backup are free (a new restore into the name is accepted).

    The server runs the restore in a task of its own once the request is
    authenticated and authorized; a disconnect before that cancels the
    request with nothing created. Before the copy was detached, PostgreSQL
    lost a restore whose client went away mid-copy and SQLite left its
    target CREATING until restart.
    """
    gone = _new_client(
        read_timeout=0.001,
        connect_timeout=5,
        retries={"total_max_attempts": 1, "mode": "standard"},
    )
    target = f"{large_backup['source']}-d"
    try:
        try:
            gone.restore_table_from_backup(
                TargetTableName=target, BackupArn=large_backup["backup_arn"]
            )
        except ReadTimeoutError:
            pass
        else:
            pytest.fail(
                "the response arrived within 1 ms, so the client never disconnected "
                "and this test checked nothing"
            )
        registered = _table_appears(dynamodb_client, target, timeout=30)
        if not registered:
            resp = dynamodb_client.restore_table_from_backup(
                TargetTableName=target, BackupArn=large_backup["backup_arn"]
            )
            assert resp["TableDescription"]["TableStatus"] == "CREATING"
        _wait_for_restore(
            dynamodb_client,
            target,
            large_backup["backup_arn"],
            large_backup["source_arn"],
            large_backup["created_at"],
        )
        assert _count(dynamodb_client, target) == LARGE_ITEMS
        print(
            "disconnected restore: "
            + ("registered and finished" if registered else "cancelled before registration")
        )
    finally:
        _cleanup(dynamodb_client, None, target)


def test_first_read_after_active_succeeds(dynamodb_client, large_backup):
    """A read on the restore target while it is CREATING is
    ResourceNotFoundException, and the first read after DescribeTable reports
    ACTIVE succeeds (measured on DynamoDB).

    A server that cached the not-found answer of a GetItem during the copy
    (5 s TTL) would keep failing reads for up to 5 s after ACTIVE; this test
    catches that. At least one probe must
    land in the CREATING window, or the test checked nothing.
    """
    client = dynamodb_client
    target = f"{large_backup['source']}-k"
    key = {"pk": {"S": "item-0000000"}}
    seen_not_found = 0
    try:
        client.restore_table_from_backup(
            TargetTableName=target, BackupArn=large_backup["backup_arn"]
        )
        deadline = time.monotonic() + RESTORE_TIMEOUT_S
        while True:
            status = client.describe_table(TableName=target)["Table"]["TableStatus"]
            if status == "ACTIVE":
                break
            assert status == "CREATING", status
            # A read while CREATING: refused as not found, and (before the
            # fix) poisoning the cache for the reads after ACTIVE. The table
            # can turn ACTIVE between the status read and this read (a fast
            # copy); a read that succeeds is then the first read after
            # ACTIVE, which the status read below confirms.
            try:
                client.get_item(TableName=target, Key=key)
            except client.exceptions.ResourceNotFoundException:
                seen_not_found += 1
            else:
                status = client.describe_table(TableName=target)["Table"]["TableStatus"]
                assert status == "ACTIVE", f"a read on a {status} target succeeded"
                break
            assert time.monotonic() < deadline, "restore did not finish"
            time.sleep(_poll_interval())
        assert seen_not_found > 0, (
            f"no read landed while {target} was CREATING; raise EXTENDDB_TEST_BACKUP_ITEMS"
        )
        # The first read after ACTIVE, with no retry and no wait.
        first = time.monotonic()
        got = client.get_item(TableName=target, Key=key, ConsistentRead=True)
        print(
            f"first GetItem after ACTIVE: {time.monotonic() - first:.3f}s after "
            f"{seen_not_found} not-found probes while CREATING"
        )
        assert got["Item"]["pk"] == key["pk"], got
        # And a Scan too (the same cache).
        assert client.scan(TableName=target, Limit=1)["Count"] == 1
    finally:
        _cleanup(client, None, target)


def test_create_backup_of_a_creating_table_is_table_not_found(dynamodb_client, large_backup):
    """CreateBackup on a table that is still CREATING is
    TableNotFoundException "Table not found: <name>" (measured on DynamoDB),
    as for a table that does not exist. The probe must land
    while the target is CREATING.
    """
    client = dynamodb_client
    target = f"{large_backup['source']}-nf"
    try:
        client.restore_table_from_backup(
            TargetTableName=target, BackupArn=large_backup["backup_arn"]
        )
        before = client.describe_table(TableName=target)["Table"]["TableStatus"]
        with pytest.raises(ClientError) as exc_info:
            client.create_backup(TableName=target, BackupName="too-early")
        assert before == "CREATING", (
            f"the target was {before} before the probe; raise EXTENDDB_TEST_BACKUP_ITEMS"
        )
        error = exc_info.value.response["Error"]
        assert error["Code"] == "TableNotFoundException", error
        assert error["Message"] == f"Table not found: {target}", error
        # Liveness: the restore finishes and the table then backs up.
        wait_for_active(client, target, timeout=RESTORE_TIMEOUT_S)
        arn = _create_backup(client, target)
        _cleanup(client, arn)
    finally:
        _cleanup(client, None, target)


def test_restore_into_a_creating_target_and_a_second_restore_of_its_backup_are_refused(
    dynamodb_client, large_backup
):
    """While a restore target is CREATING (measured on DynamoDB):
    RestoreTableFromBackup into its name is TableInUseException "Table:
    <name> is already being restored from backup: <arn>", and a second
    restore of the same backup into another name is BackupInUseException.
    Both probes must land while the target is CREATING.
    """
    client = dynamodb_client
    target = f"{large_backup['source']}-iu"
    other = f"{large_backup['source']}-iu2"
    arn = large_backup["backup_arn"]
    try:
        client.restore_table_from_backup(TargetTableName=target, BackupArn=arn)
        before = client.describe_table(TableName=target)["Table"]["TableStatus"]
        with pytest.raises(ClientError) as same_name:
            client.restore_table_from_backup(TargetTableName=target, BackupArn=arn)
        with pytest.raises(ClientError) as same_backup:
            client.restore_table_from_backup(TargetTableName=other, BackupArn=arn)
        assert before == "CREATING", (
            f"the target was {before} before the probes; raise EXTENDDB_TEST_BACKUP_ITEMS"
        )
        error = same_name.value.response["Error"]
        assert error["Code"] == "TableInUseException", error
        assert error["Message"] == (
            f"Table: {target} is already being restored from backup: {arn}"
        ), error
        error = same_backup.value.response["Error"]
        assert error["Code"] == "BackupInUseException", error
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_table(TableName=other)
        # Liveness: the first restore finishes whole, and the backup is then
        # free for another restore.
        wait_for_active(client, target, timeout=RESTORE_TIMEOUT_S)
        assert _count(client, target) == LARGE_ITEMS
        client.restore_table_from_backup(TargetTableName=other, BackupArn=arn)
        wait_for_active(client, other, timeout=RESTORE_TIMEOUT_S)
    finally:
        _cleanup(client, None, target, other)


def test_target_and_backup_are_held_while_the_copy_runs(dynamodb_client, large_backup):
    """DeleteTable of a CREATING restore target is ResourceInUseException, and
    (ExtendDB) DeleteBackup of its source is BackupInUseException.

    Each probe is judged on the status read just before it: if the target was
    CREATING the probe must be refused. A probe that found the target already
    ACTIVE checked nothing, and the test fails asking for a larger backup.
    This test runs last of the large-backup tests: a probe that lands after
    the copy can delete the module's backup.
    """
    client = dynamodb_client
    target = f"{large_backup['source']}-h"
    probes = []
    if not _is_real_dynamodb():
        # Not measured on the service; the ExtendDB contract.
        probes.append((
            "DeleteBackup",
            lambda: client.delete_backup(BackupArn=large_backup["backup_arn"]),
            client.exceptions.BackupInUseException,
        ))
    probes.append((
        "DeleteTable",
        lambda: client.delete_table(TableName=target),
        client.exceptions.ResourceInUseException,
    ))
    in_window, after = [], []
    try:
        client.restore_table_from_backup(
            TargetTableName=target, BackupArn=large_backup["backup_arn"]
        )
        for name, call, refusal in probes:
            before = client.describe_table(TableName=target)["Table"]["TableStatus"]
            try:
                call()
            except refusal:
                refused = True
            else:
                refused = False
            if before == "CREATING":
                assert refused, f"{name} succeeded while its restore target was CREATING"
                in_window.append(name)
            else:
                assert before == "ACTIVE", f"restore target {target} is {before}"
                after.append(name)
        assert not after, (
            f"{after} found the target already ACTIVE, so they checked nothing "
            f"(refused while CREATING: {in_window}); raise EXTENDDB_TEST_BACKUP_ITEMS"
        )
        # The refusals left the restore running: it still finishes, whole.
        # (Liveness: the refused probes did not freeze the target.)
        _wait_for_restore(
            client,
            target,
            large_backup["backup_arn"],
            large_backup["source_arn"],
            large_backup["created_at"],
        )
        assert _count(client, target) == LARGE_ITEMS
        print(f"probes refused in the CREATING window: {in_window}")
    finally:
        _cleanup(client, None, target)


def _feature_source(client, name: str, gsi: bool, ttl: bool) -> None:
    """On-demand table with an LSI and a stream, plus a GSI or TTL.

    GSI and TTL go on separate tables: the Cassandra backend refuses TTL on a
    table with asynchronously propagated GSIs (a backend limit, not part of
    what this test is about).
    """
    attrs = [
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "sk", "AttributeType": "S"},
        {"AttributeName": "lsk", "AttributeType": "N"},
    ]
    extra: dict = {}
    if gsi:
        attrs.append({"AttributeName": "gpk", "AttributeType": "S"})
        extra["GlobalSecondaryIndexes"] = [
            _gsi("by_gpk", "gpk", None, {"ProjectionType": "KEYS_ONLY"})
        ]
    client.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=attrs,
        BillingMode="PAY_PER_REQUEST",
        LocalSecondaryIndexes=[
            {
                "IndexName": "by_lsk",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["note"]},
            }
        ],
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
        **extra,
    )
    wait_for_active(client, name)
    if ttl:
        client.update_time_to_live(
            TableName=name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires"},
        )


def _assert_lsi_and_stream(features: dict) -> None:
    lsis = _index_by_name(features["LocalSecondaryIndexes"])
    assert sorted(lsis) == ["by_lsk"]
    assert lsis["by_lsk"]["KeySchema"][1] == {"AttributeName": "lsk", "KeyType": "RANGE"}
    assert lsis["by_lsk"]["Projection"] == {
        "ProjectionType": "INCLUDE",
        "NonKeyAttributes": ["note"],
    }
    if not _is_real_dynamodb():
        # Not measured on the service by the review.
        assert features["StreamDescription"] == {
            "StreamEnabled": True,
            "StreamViewType": "NEW_IMAGE",
        }


def test_describe_backup_reports_the_source_table_features(dynamodb_client):
    """SourceTableDetails.ProvisionedThroughput and SourceTableFeatureDetails
    (GSIs, LSIs, TTL, stream), which DescribeBackup used to omit."""
    client = dynamodb_client
    with_gsi = f"restore-feat-{uuid.uuid4().hex[:10]}"
    with_ttl = f"{with_gsi}-ttl"
    arns: list = []
    try:
        _feature_source(client, with_gsi, gsi=True, ttl=False)
        _feature_source(client, with_ttl, gsi=False, ttl=True)
        arns.append(_create_backup(client, with_gsi))
        # What DescribeTimeToLive says just before the backup: ENABLING for a
        # while after UpdateTimeToLive on the service, and on some backends.
        ttl_then = client.describe_time_to_live(TableName=with_ttl)["TimeToLiveDescription"]
        arns.append(_create_backup(client, with_ttl))
        gsi_desc, ttl_desc = (
            client.describe_backup(BackupArn=a)["BackupDescription"] for a in arns
        )

        for desc, name in ((gsi_desc, with_gsi), (ttl_desc, with_ttl)):
            details = desc["SourceTableDetails"]
            assert details["TableName"] == name
            assert details["BillingMode"] == "PAY_PER_REQUEST"
            # Required in the shape; zeros for an on-demand table (measured).
            assert details["ProvisionedThroughput"]["ReadCapacityUnits"] == 0
            assert details["ProvisionedThroughput"]["WriteCapacityUnits"] == 0
            _assert_lsi_and_stream(desc["SourceTableFeatureDetails"])

        gsis = _index_by_name(gsi_desc["SourceTableFeatureDetails"]["GlobalSecondaryIndexes"])
        assert sorted(gsis) == ["by_gpk"]
        assert gsis["by_gpk"]["KeySchema"] == [{"AttributeName": "gpk", "KeyType": "HASH"}]
        assert gsis["by_gpk"]["Projection"] == {"ProjectionType": "KEYS_ONLY"}
        if not _is_real_dynamodb():
            assert gsis["by_gpk"]["ProvisionedThroughput"] == {
                "ReadCapacityUnits": 0,
                "WriteCapacityUnits": 0,
            }

        ttl = ttl_desc["SourceTableFeatureDetails"]["TimeToLiveDescription"]
        assert ttl["AttributeName"] == "expires"
        assert ttl["TimeToLiveStatus"] in ("ENABLED", "ENABLING")
        if not _is_real_dynamodb():
            # The table's TTL as DescribeTimeToLive reported it at CreateBackup:
            # the state read just before, or the ENABLED it may have reached since.
            assert ttl["AttributeName"] == ttl_then["AttributeName"]
            assert ttl["TimeToLiveStatus"] in (ttl_then["TimeToLiveStatus"], "ENABLED"), (
                ttl,
                ttl_then,
            )
    finally:
        try:
            for arn in arns:
                _cleanup(client, arn)
        finally:
            _cleanup(client, None, with_gsi, with_ttl)


def test_describe_backup_reports_provisioned_throughput_and_sse(dynamodb_client):
    """A PROVISIONED source with SSE enabled: SourceTableDetails carries the
    table's throughput, and SourceTableFeatureDetails the GSI's throughput and
    the SSEDescription."""
    client = dynamodb_client
    source = f"restore-featp-{uuid.uuid4().hex[:10]}"
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
            ],
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 7, "WriteCapacityUnits": 9},
            GlobalSecondaryIndexes=[
                _gsi(
                    "by_gpk",
                    "gpk",
                    None,
                    {"ProjectionType": "KEYS_ONLY"},
                    ProvisionedThroughput={"ReadCapacityUnits": 3, "WriteCapacityUnits": 4},
                )
            ],
            SSESpecification={"Enabled": True, "SSEType": "KMS"},
        )
        wait_for_active(client, source)
        backup_arn = _create_backup(client, source)
        desc = client.describe_backup(BackupArn=backup_arn)["BackupDescription"]

        details = desc["SourceTableDetails"]
        assert details["BillingMode"] == "PROVISIONED"
        assert details["ProvisionedThroughput"]["ReadCapacityUnits"] == 7
        assert details["ProvisionedThroughput"]["WriteCapacityUnits"] == 9

        features = desc["SourceTableFeatureDetails"]
        gsis = _index_by_name(features["GlobalSecondaryIndexes"])
        assert sorted(gsis) == ["by_gpk"]
        assert gsis["by_gpk"]["ProvisionedThroughput"]["ReadCapacityUnits"] == 3
        assert gsis["by_gpk"]["ProvisionedThroughput"]["WriteCapacityUnits"] == 4
        if not _is_real_dynamodb():
            # Not measured on the service.
            sse = features["SSEDescription"]
            assert sse["Status"] == "ENABLED"
            assert sse["SSEType"] == "KMS"
    finally:
        _cleanup(client, backup_arn, source)


def test_list_backups_pages_with_limit(dynamodb_client):
    client = dynamodb_client
    source = f"restore-page-{uuid.uuid4().hex[:10]}"
    arns: list[str] = []
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, source)
        table_id = client.describe_table(TableName=source)["Table"]["TableId"]
        for _ in range(3):
            arns.append(_create_backup(client, source))

        seen: list[str] = []
        pages = 0
        kwargs: dict = {"TableName": source, "Limit": 1}
        while True:
            resp = client.list_backups(**kwargs)
            pages += 1
            if not _is_real_dynamodb():
                # The service may end on an empty page; not measured.
                assert len(resp["BackupSummaries"]) == 1, (pages, resp["BackupSummaries"])
            for summary in resp["BackupSummaries"]:
                seen.append(summary["BackupArn"])
                if not _is_real_dynamodb():
                    assert summary["TableId"] == table_id
            if "LastEvaluatedBackupArn" not in resp:
                break
            assert resp["LastEvaluatedBackupArn"] == seen[-1]
            kwargs["ExclusiveStartBackupArn"] = resp["LastEvaluatedBackupArn"]
            assert pages <= 10, f"paging did not end: {seen}"
        assert sorted(seen) == sorted(arns), seen
        assert len(seen) == len(set(seen)) == 3
        if not _is_real_dynamodb():
            # Three backups at one per page: the third page is the last and
            # carries no LastEvaluatedBackupArn (no empty fourth page). The
            # service's contract allows a cursor on the last page; not measured.
            assert pages == 3, pages
    finally:
        for arn in arns:
            _cleanup(client, arn)
        _cleanup(client, None, source)


def test_list_backups_filters_on_backup_type_and_time_range(dynamodb_client):
    """BackupType and TimeRangeLowerBound/UpperBound narrow the listing.

    The bounds go out as whole epoch seconds (botocore's JSON timestamp
    serialization), so the backups are taken more than two seconds apart and
    the bounds are placed between them; the inclusive lower and exclusive
    upper edges at the millisecond are checked in the engine's unit tests.
    """
    from datetime import datetime, timezone

    client = dynamodb_client
    source = f"restore-filter-{uuid.uuid4().hex[:10]}"
    arns: list[str] = []
    try:
        _simple_source(client, source)
        arns.append(_create_backup(client, source))
        time.sleep(2.2)
        arns.append(_create_backup(client, source))
        first, second = arns
        t1 = _backup_creation_time(client, first).timestamp()
        t2 = _backup_creation_time(client, second).timestamp()
        assert t2 - t1 > 2, (t1, t2)

        def listed(**kwargs) -> list[str]:
            out: list[str] = []
            kwargs = {"TableName": source, **kwargs}
            while True:
                resp = client.list_backups(**kwargs)
                out += [s["BackupArn"] for s in resp["BackupSummaries"]]
                if "LastEvaluatedBackupArn" not in resp:
                    return sorted(out)
                kwargs["ExclusiveStartBackupArn"] = resp["LastEvaluatedBackupArn"]

        def at(seconds: float) -> datetime:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)

        both = sorted(arns)
        assert listed() == both
        assert listed(BackupType="USER") == both
        assert listed(BackupType="ALL") == both
        assert listed(BackupType="SYSTEM") == []
        assert listed(BackupType="AWS_BACKUP") == []

        between = int(t2)  # after the first backup, at or before the second
        assert t1 < between <= t2
        assert listed(TimeRangeLowerBound=at(between)) == [second]
        assert listed(TimeRangeUpperBound=at(between)) == [first]
        assert listed(TimeRangeLowerBound=at(int(t1)), TimeRangeUpperBound=at(int(t2) + 2)) == both
        assert listed(TimeRangeLowerBound=at(int(t2) + 2)) == []
        # Paging walks the filtered listing.
        assert listed(TimeRangeLowerBound=at(between), Limit=1) == [second]
        assert listed(BackupType="USER", Limit=1) == both

        if not _is_real_dynamodb():
            # Not measured on the service: the enum refusal's text.
            with pytest.raises(ClientError) as exc_info:
                client.list_backups(TableName=source, BackupType="user")
            error = exc_info.value.response["Error"]
            assert error["Code"] == "ValidationException"
            assert "'backupType'" in error["Message"]
    finally:
        for arn in arns:
            _cleanup(client, arn)
        _cleanup(client, None, source)


def _simple_source(client, name: str, **extra) -> None:
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        **extra,
    )
    wait_for_active(client, name)
    client.put_item(TableName=name, Item={"pk": {"S": "a"}})


def test_restore_into_an_existing_name_is_table_already_exists(dynamodb_client):
    client = dynamodb_client
    source = f"restore-exists-{uuid.uuid4().hex[:10]}"
    backup_arn = None
    try:
        _simple_source(client, source)
        backup_arn = _create_backup(client, source)
        with pytest.raises(ClientError) as exc_info:
            client.restore_table_from_backup(TargetTableName=source, BackupArn=backup_arn)
        assert exc_info.value.response["Error"]["Code"] == "TableAlreadyExistsException"
        # The existing table is untouched.
        table = client.describe_table(TableName=source)["Table"]
        assert table["TableStatus"] == "ACTIVE"
        assert "RestoreSummary" not in table
    finally:
        _cleanup(client, backup_arn, source)


def test_backup_arn_shape_and_account_are_checked(dynamodb_client):
    client = dynamodb_client
    source = f"restore-arn-{uuid.uuid4().hex[:10]}"
    backup_arn = None
    try:
        _simple_source(client, source)
        backup_arn = _create_backup(client, source)
        table_arn = client.describe_table(TableName=source)["Table"]["TableArn"]
        backup_id = backup_arn.rsplit("/", 1)[1]

        # A table ARN with a stream-like suffix: long enough for the SDK's
        # length check, not a backup ARN.
        malformed = f"{table_arn}/notabackup/{backup_id}"
        for call in (
            lambda: client.describe_backup(BackupArn=malformed),
            lambda: client.restore_table_from_backup(
                TargetTableName=f"{source}-m", BackupArn=malformed
            ),
        ):
            with pytest.raises(ClientError) as exc_info:
                call()
            assert exc_info.value.response["Error"]["Code"] == "ValidationException"

        # The same ARN in another partition (measured on DynamoDB for
        # DescribeBackup).
        other_partition = ":".join(
            ["arn", "aws-cn" if backup_arn.split(":")[1] != "aws-cn" else "aws"]
            + backup_arn.split(":")[2:]
        )
        with pytest.raises(ClientError) as exc_info:
            client.describe_backup(BackupArn=other_partition)
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"
        if not _is_real_dynamodb():
            for call in (
                lambda: client.delete_backup(BackupArn=other_partition),
                lambda: client.restore_table_from_backup(
                    TargetTableName=f"{source}-p", BackupArn=other_partition
                ),
                lambda: client.list_backups(ExclusiveStartBackupArn=other_partition),
            ):
                with pytest.raises(ClientError) as exc_info:
                    call()
                assert exc_info.value.response["Error"]["Code"] == "ValidationException"

        if not _is_real_dynamodb():
            # Another account's (well-formed) ARN. Not measured on the service.
            parts = backup_arn.split(":")
            parts[4] = "111111111111" if parts[4] != "111111111111" else "222222222222"
            foreign = ":".join(parts)
            for call in (
                lambda: client.describe_backup(BackupArn=foreign),
                lambda: client.delete_backup(BackupArn=foreign),
                lambda: client.restore_table_from_backup(
                    TargetTableName=f"{source}-f", BackupArn=foreign
                ),
            ):
                with pytest.raises(ClientError) as exc_info:
                    call()
                assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"
            # Liveness: the real ARN still works.
            assert (
                client.describe_backup(BackupArn=backup_arn)["BackupDescription"][
                    "BackupDetails"
                ]["BackupStatus"]
                == "AVAILABLE"
            )
        for name in (f"{source}-m", f"{source}-f", f"{source}-p"):
            with pytest.raises(client.exceptions.ResourceNotFoundException):
                client.describe_table(TableName=name)
    finally:
        _cleanup(client, backup_arn, source)


def test_empty_index_overrides_restore_no_indexes_and_sse_override_is_applied(dynamodb_client):
    client = dynamodb_client
    source = f"restore-noop-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    plain = f"{source}-n"
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[_gsi("by_gpk", "gpk", None, {"ProjectionType": "ALL"})],
        )
        wait_for_active(client, source)
        client.put_item(TableName=source, Item={"pk": {"S": "a"}, "gpk": {"S": "g"}})
        assert "SSEDescription" not in client.describe_table(TableName=source)["Table"]
        backup_arn = _create_backup(client, source)
        client.restore_table_from_backup(
            TargetTableName=restored,
            BackupArn=backup_arn,
            GlobalSecondaryIndexOverride=[],
            LocalSecondaryIndexOverride=[],
            SSESpecificationOverride={"Enabled": True, "SSEType": "KMS"},
        )
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)
        table = client.describe_table(TableName=restored)["Table"]
        assert table["SSEDescription"]["Status"] == "ENABLED"
        assert table["SSEDescription"]["SSEType"] == "KMS"
        # An empty GlobalSecondaryIndexOverride restores no GSI (measured on
        # DynamoDB), and the attribute definition only the
        # index used goes with it.
        assert "GlobalSecondaryIndexes" not in table, table
        assert sorted(a["AttributeName"] for a in table["AttributeDefinitions"]) == ["pk"]
        assert _count(client, restored) == 1

        # Without the override, the restore of a source with no SSE has none,
        # and its GSI.
        client.restore_table_from_backup(TargetTableName=plain, BackupArn=backup_arn)
        wait_for_active(client, plain, timeout=RESTORE_TIMEOUT_S)
        plain_table = client.describe_table(TableName=plain)["Table"]
        assert "SSEDescription" not in plain_table
        assert [g["IndexName"] for g in plain_table["GlobalSecondaryIndexes"]] == ["by_gpk"]
    finally:
        _cleanup(client, backup_arn, plain, restored, source)


@pytest.mark.parametrize("billing", ["PROVISIONED", "PAY_PER_REQUEST"])
def test_index_overrides_listing_the_backups_indexes_restore_them(dynamodb_client, billing):
    """GlobalSecondaryIndexOverride and LocalSecondaryIndexOverride built from
    DescribeBackup's SourceTableFeatureDetails, as a console restore sends
    them, restore the backup's indexes, for a PROVISIONED and an on-demand
    backup (whose GSIs DescribeBackup reports with ProvisionedThroughput
    {0, 0}). A changed list is applied as the service applies it (measured on
    DynamoDB); a changed index definition is refused by both."""
    client = dynamodb_client
    on_demand = billing == "PAY_PER_REQUEST"
    source = f"restore-ixo-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    changed = f"{source}-c"
    backup_arn = None
    gsi_tp = {"ReadCapacityUnits": 3, "WriteCapacityUnits": 4}
    gsi_extra = {} if on_demand else {"ProvisionedThroughput": gsi_tp}
    billing_args = (
        {"BillingMode": "PAY_PER_REQUEST"}
        if on_demand
        else {"ProvisionedThroughput": {"ReadCapacityUnits": 7, "WriteCapacityUnits": 9}}
    )
    try:
        client.create_table(
            TableName=source,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
                {"AttributeName": "lsk", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                _gsi("by_gpk", "gpk", None,
                     {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["x", "y"]},
                     **gsi_extra),
                _gsi("by_gpk_keys", "gpk", "sk", {"ProjectionType": "KEYS_ONLY"},
                     **gsi_extra),
            ],
            LocalSecondaryIndexes=[
                {
                    "IndexName": "by_lsk",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "lsk", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            **billing_args,
        )
        wait_for_active(client, source)
        for i in range(5):
            client.put_item(
                TableName=source,
                Item={"pk": {"S": "p"}, "sk": {"S": f"s{i}"}, "gpk": {"S": "g"},
                      "lsk": {"S": f"l{i}"}, "x": {"N": str(i)}},
            )
        backup_arn = _create_backup(client, source)
        features = client.describe_backup(BackupArn=backup_arn)["BackupDescription"][
            "SourceTableFeatureDetails"
        ]
        # An on-demand backup's GSIs are reported with ProvisionedThroughput
        # {0, 0} (measured on DynamoDB), which an override cannot carry: the
        # service model requires at least 1 per member and botocore refuses
        # it before sending. A client leaves the member out.
        echoed_members = ("IndexName", "KeySchema", "Projection") + (
            () if on_demand else ("ProvisionedThroughput",)
        )
        if on_demand:
            for g in features["GlobalSecondaryIndexes"]:
                assert g["ProvisionedThroughput"] == {
                    "ReadCapacityUnits": 0, "WriteCapacityUnits": 0
                }, g
        gsi_override = [
            {k: g[k] for k in echoed_members if k in g}
            for g in reversed(features["GlobalSecondaryIndexes"])
        ]
        lsi_override = [
            {k: i[k] for k in ("IndexName", "KeySchema", "Projection")}
            for i in features["LocalSecondaryIndexes"]
        ]
        assert sorted(g["IndexName"] for g in gsi_override) == ["by_gpk", "by_gpk_keys"]

        resp = client.restore_table_from_backup(
            TargetTableName=restored,
            BackupArn=backup_arn,
            GlobalSecondaryIndexOverride=gsi_override,
            LocalSecondaryIndexOverride=lsi_override,
        )["TableDescription"]
        # The CREATING response omits these (measured on DynamoDB for an
        # on-demand restore; the PROVISIONED case is ExtendDB's contract).
        assert "ProvisionedThroughput" in resp, resp
        if on_demand or not _is_real_dynamodb():
            assert "DeletionProtectionEnabled" not in resp
            assert "NumberOfDecreasesToday" not in resp["ProvisionedThroughput"]
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)
        src = client.describe_table(TableName=source)["Table"]
        got = client.describe_table(TableName=restored)["Table"]
        for member in ("GlobalSecondaryIndexes", "LocalSecondaryIndexes"):
            want = _index_by_name(src[member])
            have = _index_by_name(got[member])
            assert sorted(have) == sorted(want), member
            for name, index in want.items():
                assert have[name]["KeySchema"] == index["KeySchema"], name
                assert have[name]["Projection"]["ProjectionType"] == (
                    index["Projection"]["ProjectionType"]
                ), name
                assert sorted(have[name]["Projection"].get("NonKeyAttributes", [])) == sorted(
                    index["Projection"].get("NonKeyAttributes", [])
                ), name
        if on_demand:
            assert got["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
        else:
            for g in got["GlobalSecondaryIndexes"]:
                assert g["ProvisionedThroughput"]["ReadCapacityUnits"] == 3
                assert g["ProvisionedThroughput"]["WriteCapacityUnits"] == 4
        assert _count(client, restored) == 5

        # A changed list is applied, as on the service (measured on DynamoDB): a
        # subset of the GSIs restores those, and a GSI entry's throughput is
        # the restored index's (on a PROVISIONED restore).
        subset = gsi_override[:1]
        changed_overrides = {"GlobalSecondaryIndexOverride": subset}
        if not on_demand:
            changed_overrides["GlobalSecondaryIndexOverride"] = [
                {**g, "ProvisionedThroughput": {"ReadCapacityUnits": 5,
                                               "WriteCapacityUnits": 6}}
                for g in subset
            ]
        client.restore_table_from_backup(
            TargetTableName=changed, BackupArn=backup_arn, **changed_overrides
        )
        wait_for_active(client, changed, timeout=RESTORE_TIMEOUT_S)
        got = client.describe_table(TableName=changed)["Table"]
        assert [g["IndexName"] for g in got["GlobalSecondaryIndexes"]] == [
            subset[0]["IndexName"]
        ]
        assert sorted(i["IndexName"] for i in got["LocalSecondaryIndexes"]) == ["by_lsk"]
        if not on_demand:
            gsi = got["GlobalSecondaryIndexes"][0]
            assert gsi["ProvisionedThroughput"]["ReadCapacityUnits"] == 5
            assert gsi["ProvisionedThroughput"]["WriteCapacityUnits"] == 6
        assert _count(client, changed) == 5
        _drop(client, changed)

        # A changed index definition is refused by both, before the target is
        # created.
        for overrides in (
            {"LocalSecondaryIndexOverride": [
                {**lsi_override[0], "Projection": {"ProjectionType": "KEYS_ONLY"}}
            ]},
            {"GlobalSecondaryIndexOverride": [
                {**gsi_override[0], "IndexName": "not_in_the_backup"}
            ]},
        ):
            with pytest.raises(ClientError) as exc_info:
                client.restore_table_from_backup(
                    TargetTableName=changed, BackupArn=backup_arn, **overrides
                )
            error = exc_info.value.response["Error"]
            assert error["Code"] == "ValidationException", overrides
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_table(TableName=changed)
    finally:
        _cleanup(client, backup_arn, changed, restored, source)


def test_on_demand_throughput_override_is_applied(dynamodb_client):
    """OnDemandThroughputOverride is the restored table's on-demand
    throughput, equal to the backup's or changed (measured on DynamoDB).

    Sent signed by hand: botocore 1.33.13 does not model the member.
    """
    from test_vector_index_query_scan import _error_message, _signed_post

    client = dynamodb_client
    source = f"restore-odt-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    changed = f"{source}-c"
    backup_arn = None
    odt = {"MaxReadRequestUnits": 50, "MaxWriteRequestUnits": 60}
    try:
        resp = _signed_post(
            "CreateTable",
            {
                "TableName": source,
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST",
                "OnDemandThroughput": odt,
            },
        )
        assert resp.status_code == 200, resp.text
        wait_for_active(client, source)
        client.put_item(TableName=source, Item={"pk": {"S": "a"}})
        backup_arn = _create_backup(client, source)

        resp = _signed_post(
            "RestoreTableFromBackup",
            {"TargetTableName": restored, "BackupArn": backup_arn,
             "OnDemandThroughputOverride": odt},
        )
        assert resp.status_code == 200, resp.text
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)
        table = _signed_post("DescribeTable", {"TableName": restored}).json()["Table"]
        assert table["OnDemandThroughput"] == odt
        assert _count(client, restored) == 1

        resp = _signed_post(
            "RestoreTableFromBackup",
            {"TargetTableName": changed, "BackupArn": backup_arn,
             "OnDemandThroughputOverride": {**odt, "MaxWriteRequestUnits": 61}},
        )
        assert resp.status_code == 200, resp.text
        wait_for_active(client, changed, timeout=RESTORE_TIMEOUT_S)
        table = _signed_post("DescribeTable", {"TableName": changed}).json()["Table"]
        assert table["OnDemandThroughput"] == {**odt, "MaxWriteRequestUnits": 61}
    finally:
        _cleanup(client, backup_arn, changed, restored, source)


@extenddb_only
def test_create_backup_of_a_vector_table_is_refused(dynamodb_client):
    """Refused at CreateBackup, not at restore time."""
    from test_vector_index_query_scan import _error_message, _signed_post

    client = dynamodb_client
    name = f"restore-vec-{uuid.uuid4().hex[:10]}"
    resp = _signed_post(
        "CreateTable",
        {
            "TableName": name,
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {
                    "IndexName": "vidx",
                    "Dimensions": 4,
                    "DistanceFunction": "COSINE",
                    "VectorAttribute": {"AttributeName": "emb"},
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        },
    )
    if resp.status_code == 400 and "not supported" in _error_message(resp):
        pytest.skip("target backend does not support vector indexes")
    assert resp.status_code == 200, resp.text
    try:
        wait_for_active(client, name, timeout=180)
        vector = _signed_post("DescribeTable", {"TableName": name}).json()["Table"]
        if not vector.get("VectorIndexes"):
            pytest.skip("target accepted CreateTable but did not create the vector index")
        with pytest.raises(ClientError) as exc_info:
            client.create_backup(TableName=name, BackupName=f"{name}-bkp")
        error = exc_info.value.response["Error"]
        assert error["Code"] == "ValidationException"
        assert error["Message"] == (
            f"table {name} has 1 vector index(es); restoring a table with vector "
            "indexes is not supported by this storage backend"
        )
        listed = client.list_backups(TableName=name)["BackupSummaries"]
        assert listed == [], listed
    finally:
        _cleanup(client, None, name)


def test_update_table_on_a_restored_table_reports_no_restore_summary(dynamodb_client):
    """Once a restored table is ACTIVE, no description-returning response
    carries RestoreSummary: UpdateTable's no more than DescribeTable's.

    The service drops the member at ACTIVE on DescribeTable (measured);
    UpdateTable was not measured on the service, so that half is checked on
    ExtendDB only.
    """
    client = dynamodb_client
    source = f"restore-upd-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    backup_arn = None
    try:
        _simple_source(client, source)
        backup_arn = _create_backup(client, source)
        client.restore_table_from_backup(TargetTableName=restored, BackupArn=backup_arn)
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)
        assert "RestoreSummary" not in client.describe_table(TableName=restored)["Table"]

        resp = client.update_table(
            TableName=restored,
            StreamSpecification={"StreamEnabled": True, "StreamViewType": "KEYS_ONLY"},
        )
        if not _is_real_dynamodb():
            desc = resp["TableDescription"]
            assert "RestoreSummary" not in desc, desc.get("RestoreSummary")
        wait_for_active(client, restored)
        assert "RestoreSummary" not in client.describe_table(TableName=restored)["Table"]
    finally:
        _cleanup(client, backup_arn, restored, source)


@extenddb_only
def test_a_provisioned_gsi_without_throughput_restores_with_the_table_throughput(
    dynamodb_client,
):
    """ExtendDB's CreateTable accepts a PROVISIONED table whose GSI has no
    ProvisionedThroughput, and UpdateTable to PROVISIONED leaves an on-demand
    table's GSI without one. A backup of either restores with no overrides,
    and the GSI takes the table's throughput.

    ExtendDB only: the service refuses the CreateTable and the UpdateTable,
    so it cannot produce such a backup. Before, the restore was refused with
    "ProvisionedThroughput must be specified for index".
    """
    client = dynamodb_client
    base = f"restore-gsitp-{uuid.uuid4().hex[:10]}"
    created, switched = f"{base}-c", f"{base}-s"
    restored = {created: f"{created}-r", switched: f"{switched}-r"}
    attrs = [
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "gpk", "AttributeType": "S"},
    ]
    gsi = _gsi("by_gpk", "gpk", None, {"ProjectionType": "ALL"})
    arns: list = []
    try:
        client.create_table(
            TableName=created,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=attrs,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 6},
            GlobalSecondaryIndexes=[gsi],
        )
        client.create_table(
            TableName=switched,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=attrs,
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[gsi],
        )
        wait_for_active(client, created)
        wait_for_active(client, switched)
        client.update_table(
            TableName=switched,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 6},
        )
        wait_for_active(client, switched)
        for name in (created, switched):
            client.put_item(TableName=name, Item={"pk": {"S": "a"}, "gpk": {"S": "g"}})
            arn = _create_backup(client, name)
            arns.append(arn)
            client.restore_table_from_backup(TargetTableName=restored[name], BackupArn=arn)
        for name in (created, switched):
            wait_for_active(client, restored[name], timeout=RESTORE_TIMEOUT_S)
            table = client.describe_table(TableName=restored[name])["Table"]
            assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 5
            assert table["ProvisionedThroughput"]["WriteCapacityUnits"] == 6
            got = _index_by_name(table["GlobalSecondaryIndexes"])["by_gpk"]
            assert got["ProvisionedThroughput"]["ReadCapacityUnits"] == 5, (name, got)
            assert got["ProvisionedThroughput"]["WriteCapacityUnits"] == 6, (name, got)
            assert _count(client, restored[name]) == 1
    finally:
        try:
            for arn in arns:
                _cleanup(client, arn)
        finally:
            _cleanup(client, None, restored[created], restored[switched], created, switched)


# ---------------------------------------------------------------------------
# Validation and response shapes measured on DynamoDB.
# ---------------------------------------------------------------------------


def _signed(operation: str, body: dict):
    from test_vector_index_query_scan import _signed_post

    return _signed_post(operation, body)


def _error(resp) -> tuple[str, str]:
    """(error type after the `#`, message) of a JSON-1.0 error response."""
    payload = resp.json()
    return (
        payload.get("__type", "").rsplit("#", 1)[-1],
        payload.get("message", payload.get("Message", "")),
    )


def test_backup_and_table_names_are_validated(dynamodb_client):
    """BackupName and TableName on CreateBackup and ListBackups: length 3 to
    255 and [a-zA-Z0-9_.-]+, with the service's texts (measured on DynamoDB;
    the order of the two clauses when both fail was not captured, so it is
    asserted exactly on ExtendDB and as membership on the service). Sent
    signed by hand: botocore refuses a too-short name
    before sending (its validator checks the minimum, not the pattern or
    the maximum). Before the check, a 200 KB name was a 500 on Cassandra and a
    NUL byte a 500 on PostgreSQL.
    """
    client = dynamodb_client
    source = f"restore-names-{uuid.uuid4().hex[:10]}"
    constraint = (
        "Value '{v}' at '{path}' failed to satisfy constraint: {c}"
    )
    short = "Member must have length greater than or equal to 3"
    long_ = "Member must have length less than or equal to 255"
    pattern = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
    try:
        _simple_source(client, source)
        cases = [
            ("CreateBackup", {"TableName": source, "BackupName": "ab"},
             "1 validation error detected: " + constraint.format(v="ab", path="backupName", c=short)),
            ("CreateBackup", {"TableName": source, "BackupName": "a b"},
             "1 validation error detected: " + constraint.format(v="a b", path="backupName", c=pattern)),
            ("CreateBackup", {"TableName": source, "BackupName": "x" * 256},
             "1 validation error detected: " + constraint.format(v="x" * 256, path="backupName", c=long_)),
            ("CreateBackup", {"TableName": source, "BackupName": "a!"},
             "2 validation errors detected: "
             + constraint.format(v="a!", path="backupName", c=short) + "; "
             + constraint.format(v="a!", path="backupName", c=pattern)),
            ("CreateBackup", {"TableName": "t t", "BackupName": "fine"},
             "1 validation error detected: " + constraint.format(v="t t", path="tableName", c=pattern)),
            ("ListBackups", {"TableName": "ab"},
             "1 validation error detected: " + constraint.format(v="ab", path="tableName", c=short)),
            ("ListBackups", {"TableName": "a*"},
             "2 validation errors detected: "
             + constraint.format(v="a*", path="tableName", c=short) + "; "
             + constraint.format(v="a*", path="tableName", c=pattern)),
        ]
        for operation, body, expected in cases:
            resp = _signed(operation, body)
            assert resp.status_code == 400, (operation, body, resp.text)
            code, message = _error(resp)
            assert code == "ValidationException", (operation, body, resp.text)
            if _is_real_dynamodb():
                # The clauses, their count, and the member path; the order of
                # the two clauses on the service was not captured.
                head, _, clauses = expected.partition(": ")
                assert message.startswith(head + ": "), (operation, body, message)
                for clause in clauses.split("; "):
                    assert clause in message, (operation, body, message)
            else:
                assert message == expected, (operation, body, message)
        # A NUL byte and a 200 KB name are refused, not stored.
        for name in ("nul\x00byte", "y" * (200 * 1024)):
            resp = _signed("CreateBackup", {"TableName": source, "BackupName": name})
            assert resp.status_code == 400, resp.text[:200]
            code, message = _error(resp)
            assert code == "ValidationException"
            assert "'backupName'" in message
        assert client.list_backups(TableName=source)["BackupSummaries"] == []
    finally:
        _cleanup(client, None, source)


def test_backup_arn_shapes_the_service_refuses_and_trims(dynamodb_client):
    """BackupArn shapes the service refuses as ValidationException "Invalid
    Request: BackupArn is not valid" (not an ARN, oversize, control
    characters, a slash in the region) or "Invalid Backup ARN" (an id suffix
    of the wrong length or not hex, a wildcard), on DescribeBackup,
    DeleteBackup, RestoreTableFromBackup and as ExclusiveStartBackupArn; a
    trailing newline is trimmed and an ARN naming another region of the
    caller's account resolves (all measured on DynamoDB).
    Which text goes with which input was not captured; the pairing here is
    ExtendDB's and is checked on ExtendDB only.
    """
    client = dynamodb_client
    source = f"restore-arnshape-{uuid.uuid4().hex[:10]}"
    backup_arn = None
    try:
        _simple_source(client, source)
        backup_arn = _create_backup(client, source)
        prefix, backup_id = backup_arn.rsplit("/", 1)
        region = backup_arn.split(":")[3]
        other_region = "eu-west-3" if region != "eu-west-3" else "eu-west-1"
        in_other_region = ":".join(
            backup_arn.split(":")[:3] + [other_region] + backup_arn.split(":")[4:]
        )
        for resolving in (backup_arn + "\n", in_other_region, in_other_region + "\n"):
            resp = _signed("DescribeBackup", {"BackupArn": resolving})
            assert resp.status_code == 200, (resolving, resp.text)
            desc = resp.json()["BackupDescription"]
            assert desc["BackupDetails"]["BackupArn"] == backup_arn, resolving
            assert desc["BackupDetails"]["BackupStatus"] == "AVAILABLE"

        not_valid = "Invalid Request: BackupArn is not valid"
        invalid_id = "Invalid Backup ARN"
        bad = [
            (f"{prefix}/{backup_id}\x00", not_valid),
            (f"{prefix}/{backup_id}\x07", not_valid),
            (backup_arn.replace(f":{region}:", f":{region}/..:"), not_valid),
            (backup_arn.replace(f"table/{source}", "table/" + "x" * 1000), not_valid),
            (f"{prefix}/*", invalid_id),
            (f"{prefix}/{backup_id[:-1]}", invalid_id),
            (f"{prefix}/{backup_id}0", invalid_id),
            (f"{prefix}/{backup_id[:-2]}zz", invalid_id),
        ]
        for arn, expected in bad:
            for operation, body in (
                ("DescribeBackup", {"BackupArn": arn}),
                ("DeleteBackup", {"BackupArn": arn}),
                ("RestoreTableFromBackup", {"BackupArn": arn, "TargetTableName": f"{source}-x"}),
                ("ListBackups", {"ExclusiveStartBackupArn": arn}),
            ):
                resp = _signed(operation, body)
                assert resp.status_code == 400, (operation, arn, resp.text)
                code, message = _error(resp)
                assert code == "ValidationException", (operation, arn, resp.text)
                if not _is_real_dynamodb():
                    assert message == expected, (operation, arn, message)
                else:
                    assert message in (not_valid, invalid_id), (operation, arn, message)
        # Liveness: the real ARN still resolves, and nothing was created.
        assert (
            client.describe_backup(BackupArn=backup_arn)["BackupDescription"]["BackupDetails"][
                "BackupStatus"
            ]
            == "AVAILABLE"
        )
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_table(TableName=f"{source}-x")
    finally:
        _cleanup(client, backup_arn, source)


def test_list_backups_cursor_and_time_range_are_checked(dynamodb_client):
    """ExclusiveStartBackupArn of another account is ValidationException
    "Invalid Request: The supplied ExclusiveStartBackupArn does not match the
    account"; of another region, or of a table other than TableName's,
    ValidationException ("does not match the region", "does not match the
    table name predicate"); a time bound before the epoch is
    ValidationException; a lower bound above the upper bound is
    ValidationException "Time range lower bound must be less than or equal to
    upper bound" (all measured on DynamoDB).
    """
    client = dynamodb_client
    source = f"restore-cursor-{uuid.uuid4().hex[:10]}"
    backup_arn = None
    try:
        _simple_source(client, source)
        backup_arn = _create_backup(client, source)
        parts = backup_arn.split(":")
        other_account = ":".join(
            parts[:4] + ["111111111111" if parts[4] != "111111111111" else "222222222222"]
            + parts[5:]
        )
        other_region = ":".join(
            parts[:3] + ["eu-west-3" if parts[3] != "eu-west-3" else "eu-west-1"] + parts[4:]
        )
        other_table = backup_arn.replace(f"table/{source}/", "table/some-other-table/")
        cases = [
            ({"ExclusiveStartBackupArn": other_account},
             "Invalid Request: The supplied ExclusiveStartBackupArn does not match the account"),
            ({"ExclusiveStartBackupArn": other_region}, "does not match the region"),
            ({"ExclusiveStartBackupArn": other_table, "TableName": source},
             "does not match the table name predicate"),
            ({"TimeRangeLowerBound": 20, "TimeRangeUpperBound": 10},
             "Time range lower bound must be less than or equal to upper bound"),
        ]
        for body, expected in cases:
            with pytest.raises(ClientError) as exc_info:
                client.list_backups(**body)
            error = exc_info.value.response["Error"]
            assert error["Code"] == "ValidationException", (body, error)
            assert expected in error["Message"], (body, error)
        # A bound before the epoch (botocore sends a negative timestamp).
        for body in (
            {"TimeRangeLowerBound": -5},
            {"TimeRangeUpperBound": -5},
        ):
            with pytest.raises(ClientError) as exc_info:
                client.list_backups(**body)
            assert exc_info.value.response["Error"]["Code"] == "ValidationException", body
        # Equal bounds are accepted and list nothing; another table's cursor
        # without TableName is accepted.
        now = time.time()
        assert client.list_backups(
            TimeRangeLowerBound=now, TimeRangeUpperBound=now
        )["BackupSummaries"] == []
        client.list_backups(ExclusiveStartBackupArn=other_table)
        # Liveness: the listing itself still works.
        assert backup_arn in [
            b["BackupArn"] for b in client.list_backups(TableName=source)["BackupSummaries"]
        ]
    finally:
        _cleanup(client, backup_arn, source)


def test_create_backup_of_a_missing_table_is_table_not_found(dynamodb_client):
    """TableNotFoundException "Table not found: <name>" (measured on DynamoDB),
    not ResourceNotFoundException, and the same text on
    every backend."""
    client = dynamodb_client
    name = f"restore-missing-{uuid.uuid4().hex[:10]}"
    with pytest.raises(ClientError) as exc_info:
        client.create_backup(TableName=name, BackupName="of-nothing")
    error = exc_info.value.response["Error"]
    assert error["Code"] == "TableNotFoundException", error
    assert error["Message"] == f"Table not found: {name}", error


def test_delete_backup_answers_the_full_description_deleted(dynamodb_client):
    """DeleteBackup returns the DescribeBackup description with BackupStatus
    DELETED: SourceTableDetails.ProvisionedThroughput (a required member) and
    SourceTableFeatureDetails included (measured on DynamoDB). A typed SDK unmarshalling the output failed without them.
    """
    client = dynamodb_client
    source = f"restore-delshape-{uuid.uuid4().hex[:10]}"
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
            ],
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 7, "WriteCapacityUnits": 9},
            GlobalSecondaryIndexes=[
                _gsi("by_gpk", "gpk", None, {"ProjectionType": "ALL"},
                     ProvisionedThroughput={"ReadCapacityUnits": 3, "WriteCapacityUnits": 4})
            ],
        )
        wait_for_active(client, source)
        backup_arn = _create_backup(client, source)
        described = client.describe_backup(BackupArn=backup_arn)["BackupDescription"]
        deleted = client.delete_backup(BackupArn=backup_arn)["BackupDescription"]
        backup_arn = None
        assert deleted["BackupDetails"]["BackupStatus"] == "DELETED"
        assert deleted["SourceTableDetails"]["ProvisionedThroughput"] == {
            "ReadCapacityUnits": 7, "WriteCapacityUnits": 9,
        }
        assert deleted["SourceTableDetails"]["TableName"] == source
        gsis = deleted["SourceTableFeatureDetails"]["GlobalSecondaryIndexes"]
        assert [g["IndexName"] for g in gsis] == ["by_gpk"]
        assert gsis[0]["ProvisionedThroughput"] == {"ReadCapacityUnits": 3, "WriteCapacityUnits": 4}
        # Everything but the status is what DescribeBackup said.
        described["BackupDetails"]["BackupStatus"] = "DELETED"
        assert deleted == described
    finally:
        _cleanup(client, backup_arn, source)


def test_create_backup_accepts_the_table_arn(dynamodb_client):
    """CreateBackup with TableName given as the table's ARN is accepted
    (measured on DynamoDB). Sent signed by hand: the model
    gives the member the TableName shape, so botocore refuses the ARN form
    client-side (max 255 is not checked, but the pattern excludes `:`).
    """
    client = dynamodb_client
    source = f"restore-tarn-{uuid.uuid4().hex[:10]}"
    backup_arn = None
    try:
        _simple_source(client, source)
        table_arn = client.describe_table(TableName=source)["Table"]["TableArn"]
        resp = _signed("CreateBackup", {"TableName": table_arn, "BackupName": "by-arn"})
        assert resp.status_code == 200, resp.text
        details = resp.json()["BackupDetails"]
        backup_arn = details["BackupArn"]
        assert backup_arn.startswith(f"{table_arn}/backup/"), backup_arn
        desc = client.describe_backup(BackupArn=backup_arn)["BackupDescription"]
        assert desc["SourceTableDetails"]["TableName"] == source
        assert desc["SourceTableDetails"]["TableArn"] == table_arn
    finally:
        _cleanup(client, backup_arn, source)


def test_table_class_is_not_restored(dynamodb_client):
    """A STANDARD_INFREQUENT_ACCESS source restores as STANDARD (measured on
    DynamoDB); DescribeBackup still reports nothing of the
    class (the service's SourceTableDetails has no TableClass member).
    """
    client = dynamodb_client
    source = f"restore-class-{uuid.uuid4().hex[:10]}"
    restored = f"{source}-r"
    backup_arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            TableClass="STANDARD_INFREQUENT_ACCESS",
        )
        wait_for_active(client, source)
        assert (
            client.describe_table(TableName=source)["Table"]["TableClassSummary"]["TableClass"]
            == "STANDARD_INFREQUENT_ACCESS"
        )
        client.put_item(TableName=source, Item={"pk": {"S": "a"}})
        backup_arn = _create_backup(client, source)
        client.restore_table_from_backup(TargetTableName=restored, BackupArn=backup_arn)
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)
        table = client.describe_table(TableName=restored)["Table"]
        # The service omits TableClassSummary for a STANDARD table; ExtendDB
        # may report it as STANDARD.
        assert table.get("TableClassSummary", {}).get("TableClass", "STANDARD") == "STANDARD", (
            table.get("TableClassSummary")
        )
        assert _count(client, restored) == 1
    finally:
        _cleanup(client, backup_arn, restored, source)
