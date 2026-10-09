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

import time
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import _poll_interval, wait_for_active, wait_for_deleted

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
        resp = client.restore_table_from_backup(TargetTableName=restored, BackupArn=backup_arn)
        assert resp["TableDescription"]["TableName"] == restored
        started = resp["TableDescription"]["RestoreSummary"]
        assert started["SourceBackupArn"] == backup_arn
        assert started["RestoreInProgress"] is True
        wait_for_active(client, restored, timeout=RESTORE_TIMEOUT_S)

        table = client.describe_table(TableName=restored)["Table"]
        # DescribeTable keeps reporting where the table came from, finished.
        summary = table["RestoreSummary"]
        assert summary["SourceBackupArn"] == backup_arn
        assert summary["RestoreInProgress"] is False
        assert summary["RestoreDateTime"] == started["RestoreDateTime"]
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
        wait_for_active(client, source)

        backup_arn = _create_backup(client, source)
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
        assert error["Message"] == (
            "One or more parameter values were invalid: "
            "GlobalSecondaryIndexOverride must be specified for index: by_gpk "
            "when BillingModeOverride is PROVISIONED"
        )

        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_table(TableName=restored)
    finally:
        _cleanup(client, backup_arn, restored, source)
