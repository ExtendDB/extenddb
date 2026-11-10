# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""MongoDB-specific backup/restore coverage.

The table operations are exercised through the DynamoDB API.  These tests are
gated by the MongoDB runner because the backup metadata being checked is part
of the MongoDB implementation; other backends retain the shared API tests. The
runner's server has a filesystem backup store, so CreateBackup is the
store-backed job and RestoreTableFromBackup the store-backed restore; the
cells at the end meet the MongoDB target's own rules through that path.
"""

from __future__ import annotations

import json
import os
import subprocess
import time

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted


@pytest.fixture()
def mongodb_container() -> str:
    container = os.environ.get("EXTENDDB_TEST_MONGODB_CONTAINER", "").strip()
    if not container:
        pytest.skip("requires devtools/run-mongodb-tests")
    # These cells read the in-catalog create's documents (`backups`) and
    # restore its backups at once; on a server with a `[backup.store]`
    # CreateBackup is the store-backed job (CREATING, then AVAILABLE in the
    # store) and RestoreTableFromBackup of its backups is refused until the
    # restore job lands (PR 9a), which brings these cells back there.
    store = os.environ.get("EXTENDDB_TEST_BACKUP_STORE", "").strip()
    if store in ("filesystem", "s3"):
        pytest.skip(
            f"EXTENDDB_TEST_BACKUP_STORE={store}: these cells read and restore the "
            "in-catalog create's backups; the store-backed restore is PR 9a's"
        )
    return container


BACKUP_STORE = os.environ.get("EXTENDDB_TEST_BACKUP_STORE", "").strip() or None


def _create_backup(client, table_name: str, backup_name: str) -> str:
    """CreateBackup, waited for AVAILABLE: with the runner's filesystem store
    the create is the asynchronous job, and a restore or a delete of a
    CREATING backup is BackupInUseException."""
    deadline = time.monotonic() + 30
    while True:
        try:
            response = client.create_backup(
                TableName=table_name,
                BackupName=backup_name,
            )
            arn = response["BackupDetails"]["BackupArn"]
            break
        except ClientError as exc:
            if (
                exc.response["Error"]["Code"]
                != "ContinuousBackupsUnavailableException"
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(0.2)
    _wait_backup_available(client, arn)
    return arn


def _remove_legacy_capacity_metadata(container: str, backup_arn: str) -> None:
    """Make a real backup document look like one from before throughput capture."""
    script = """
const backupArn = %s;
const backups = db.getSiblingDB("extenddb_catalog").backups;
const result = backups.updateOne(
  { _id: backupArn },
  { $unset: { provisioned_throughput: "" } },
);
if (result.matchedCount !== 1) {
  throw new Error(`backup not found: ${backupArn}`);
}
if (backups.findOne({ _id: backupArn }).provisioned_throughput !== undefined) {
  throw new Error(`provisioned_throughput was not removed: ${backupArn}`);
}
""" % json.dumps(backup_arn)
    result = subprocess.run(
        ["docker", "exec", container, "mongosh", "--quiet", "--eval", script],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        "failed to remove legacy backup metadata: "
        f"{result.stdout}\n{result.stderr}"
    )


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


def test_restore_legacy_backup_falls_back_and_populates_lsi(
    dynamodb_client, unique_table_name, mongodb_container
):
    """A pre-throughput backup still restores and its LSI remains queryable.
    The backup edited is an in-catalog one (the legacy `backups` document),
    so the cell runs only on a server without a backup store:
    `devtools/run-mongodb-tests --backup-store none -- --pytest --filter
    test_mongodb_backup_restore` (the CI jobs run the suite with a filesystem
    store, where this cell skips). With a store, CreateBackup writes the
    store and the fallback is pinned by the crate's
    `restore_uses_legacy_fallback_when_capacity_metadata_is_missing`."""
    if BACKUP_STORE != "none":
        pytest.skip("the legacy throughput fallback is a rule of in-catalog backups (EXTENDDB_TEST_BACKUP_STORE is not none)")
    source = unique_table_name
    restored = f"{source}-restored"
    dynamodb_client.create_table(
        TableName=source,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "lsi_sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 7, "WriteCapacityUnits": 9},
        LocalSecondaryIndexes=[
            {
                "IndexName": "legacy_lsi",
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
                "lsi_sk": {"S": "lsi-sort-1"},
                "value": {"S": "legacy-value"},
            },
        )
        backup_arn = _create_backup(dynamodb_client, source, f"{source}-backup")
        _remove_legacy_capacity_metadata(mongodb_container, backup_arn)

        dynamodb_client.restore_table_from_backup(
            TargetTableName=restored,
            BackupArn=backup_arn,
        )
        wait_for_active(dynamodb_client, restored)

        table = dynamodb_client.describe_table(TableName=restored)["Table"]
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 5
        assert table["ProvisionedThroughput"]["WriteCapacityUnits"] == 5
        assert any(
            index["IndexName"] == "legacy_lsi"
            for index in table["LocalSecondaryIndexes"]
        )

        query = dynamodb_client.query(
            TableName=restored,
            IndexName="legacy_lsi",
            KeyConditionExpression="pk = :pk AND lsi_sk = :lsi_sk",
            ExpressionAttributeValues={
                ":pk": {"S": "partition-1"},
                ":lsi_sk": {"S": "lsi-sort-1"},
            },
        )
        assert query["Count"] == 1
        assert query["Items"][0]["value"] == {"S": "legacy-value"}
    finally:
        _cleanup(dynamodb_client, restored, source)
        if backup_arn:
            dynamodb_client.delete_backup(BackupArn=backup_arn)


# --- the store-backed restore's MongoDB rules ---------------------------------------
#
# These cells rewrite a backup's prefix under the runner's filesystem store
# (EXTENDDB_TEST_BACKUP_ROOT) and read the restore job's log lines from the
# server's log (EXTENDDB_TEST_SERVER_LOG); devtools/run-mongodb-tests exports
# both. A rule the backend's write path enforces (an `N` sort key beyond
# Decimal128) is met inside the job, so the restore is FAILED with that text
# as its reason and the client sees the target disappear; a rule the request
# checks (vector indexes into a backend without vector support) is a
# synchronous ValidationException before any table exists.

ACCOUNT = "123456789012"


@pytest.fixture()
def backup_root(mongodb_container) -> str:
    root = os.environ.get("EXTENDDB_TEST_BACKUP_ROOT", "").strip()
    log = os.environ.get("EXTENDDB_TEST_SERVER_LOG", "").strip()
    if not root or not log:
        pytest.skip("requires devtools/run-mongodb-tests (EXTENDDB_TEST_BACKUP_ROOT, EXTENDDB_TEST_SERVER_LOG)")
    return root


def _wait_backup_available(client, arn: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        status = client.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]["BackupStatus"]
        if status == "AVAILABLE":
            return
        assert status == "CREATING", status
        assert time.monotonic() < deadline, f"backup {arn} still CREATING after {timeout} s"
        time.sleep(0.2)


def _prefix(root: str, table: str, arn: str) -> str:
    return os.path.join(root, ACCOUNT, table, arn.rsplit("/", 1)[1])


def _read_manifest(prefix: str) -> dict:
    with open(os.path.join(prefix, "extenddb-manifest.json")) as f:
        return json.load(f)


def _write_manifest(prefix: str, manifest: dict) -> None:
    """The manifest and its sha256 sidecar rewritten under `prefix`, private
    as the store writes them, and the two DynamoDB export manifests for the
    manifest's data files (one `md5Checksum` and one `itemCount` per file,
    agreeing with the extenddb manifest by construction), so the prefix
    stays a whole backup in the format's sense, as `tests/test_backup_job.py`
    and `devtools/backup-restore-matrix` rewrite theirs."""
    import hashlib

    text = json.dumps(manifest, separators=(",", ":")).encode()
    path = os.path.join(prefix, "extenddb-manifest.json")
    with open(path, "wb") as f:
        f.write(text)
    os.chmod(path, 0o600)
    sidecar = os.path.join(prefix, "extenddb-manifest.sha256")
    with open(sidecar, "w") as f:
        f.write(f"{hashlib.sha256(text).hexdigest()}  extenddb-manifest.json\n")
    os.chmod(sidecar, 0o600)
    summary_path = os.path.join(prefix, "manifest-summary.json")
    with open(summary_path) as f:
        summary = json.load(f)
    s3_prefix = summary["s3Prefix"]
    files_path = os.path.join(prefix, "manifest-files.json")
    with open(files_path, "w") as f:
        for entry in manifest["data_files"]:
            line = {
                "itemCount": entry["item_count"],
                "md5Checksum": entry["md5"],
                "dataFileS3Key": f"{s3_prefix}/{entry['key']}" if s3_prefix else entry["key"],
            }
            f.write(json.dumps(line, separators=(",", ":")) + "\n")
    os.chmod(files_path, 0o600)
    summary["itemCount"] = sum(entry["item_count"] for entry in manifest["data_files"])
    summary["billedSizeBytes"] = manifest["backup_size_bytes"]
    with open(summary_path, "w") as f:
        f.write(json.dumps(summary, separators=(",", ":")))
    os.chmod(summary_path, 0o600)


def _write_unordered_file(prefix: str, rows: list[dict]) -> dict:
    """The one data file of an unordered backup holding `rows`; its manifest
    entry."""
    import base64
    import gzip
    import hashlib
    import io

    lines = b"".join(json.dumps({"Item": r}, separators=(",", ":")).encode() + b"\n" for r in rows)
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz:
        gz.write(lines)
    compressed = buffer.getvalue()
    path = os.path.join(prefix, "data", "000001.json.gz")
    for name in os.listdir(os.path.join(prefix, "data")):
        os.remove(os.path.join(prefix, "data", name))
    with open(path, "wb") as f:
        f.write(compressed)
    os.chmod(path, 0o600)
    return {
        "key": "data/000001.json.gz", "item_count": len(rows),
        "sha256": hashlib.sha256(compressed).hexdigest(),
        "md5": base64.b64encode(hashlib.md5(compressed).digest()).decode(),
        "bytes": len(compressed), "uncompressed_bytes": len(lines), "key_range": None,
    }


def _composite_table(client, name: str) -> None:
    client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(client, name)


def _delete_backup_once_available(client, arn: str | None) -> None:
    if arn:
        _wait_backup_available(client, arn)
        client.delete_backup(BackupArn=arn)


def test_a_vector_manifest_is_refused_before_any_table(dynamodb_client, unique_table_name, backup_root):
    """A backup whose manifest records a vector index (the prefix of a real
    backup rewritten under the root) is refused by RestoreTableFromBackup
    with the vector ValidationException, and no table of the target's name
    exists afterwards: the request checks the manifest against the backend
    before it registers anything."""
    client = dynamodb_client
    source = unique_table_name
    target = f"{source}-restored"
    backup_arn = None
    try:
        _composite_table(client, source)
        client.put_item(TableName=source, Item={"pk": {"S": "a"}, "sk": {"N": "1"}, "emb": {"L": [{"N": "0.1"}]}})
        backup_arn = _create_backup(client, source, f"{source}-backup")
        _wait_backup_available(client, backup_arn)
        prefix = _prefix(backup_root, source, backup_arn)
        manifest = _read_manifest(prefix)
        manifest["source_table"]["vector_indexes"] = [{
            "IndexName": "by-emb", "Dimensions": 3, "DistanceFunction": "COSINE",
            "VectorAttribute": {"AttributeName": "emb"},
        }]
        _write_manifest(prefix, manifest)
        with pytest.raises(ClientError) as refused:
            client.restore_table_from_backup(TargetTableName=target, BackupArn=backup_arn)
        err = refused.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert err["Message"] == (
            f"backup {backup_arn} has 1 vector index(es); restoring a table with "
            "vector indexes is not supported by this storage backend"
        ), err
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_table(TableName=target)
    finally:
        _cleanup(client, target, source)
        _delete_backup_once_available(client, backup_arn)


def test_a_sort_key_beyond_decimal128_fails_the_restore(dynamodb_client, unique_table_name, backup_root):
    """A data file holding an `N` sort key of 35 significant digits (more
    than Decimal128's 34, which the sort-key column is): the restore is
    accepted CREATING, the job fails the file with the write path's
    ValidationException text, the target is gone, and the server log carries
    the Decimal128 text as the FAILED reason."""
    client = dynamodb_client
    source = unique_table_name
    target = f"{source}-restored"
    backup_arn = None
    try:
        _composite_table(client, source)
        for i in range(3):
            client.put_item(TableName=source, Item={"pk": {"S": "a"}, "sk": {"N": str(i)}})
        backup_arn = _create_backup(client, source, f"{source}-backup")
        _wait_backup_available(client, backup_arn)
        prefix = _prefix(backup_root, source, backup_arn)
        manifest = _read_manifest(prefix)
        long_key = "12345678901234567890123456789012345"
        rows = [{"pk": {"S": "a"}, "sk": {"N": str(i)}} for i in range(3)]
        rows.append({"pk": {"S": "a"}, "sk": {"N": long_key}})
        entry = _write_unordered_file(prefix, rows)
        manifest["item_order"] = "unordered"
        manifest["data_files"] = [entry]
        manifest["backup_size_bytes"] = entry["bytes"]
        manifest["source_table"]["item_count"] = len(rows)
        _write_manifest(prefix, manifest)
        resp = client.restore_table_from_backup(TargetTableName=target, BackupArn=backup_arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        deadline = time.monotonic() + 120
        while True:
            try:
                status = client.describe_table(TableName=target)["Table"]["TableStatus"]
            except client.exceptions.ResourceNotFoundException:
                break
            assert status in ("CREATING", "DELETING"), f"{target} reached {status}"
            assert time.monotonic() < deadline, f"{target} still exists after 120 s"
            time.sleep(0.2)
        deadline = time.monotonic() + 15
        while True:
            with open(os.environ["EXTENDDB_TEST_SERVER_LOG"], errors="replace") as f:
                failed = [line for line in f if ": FAILED (" in line and f" into {target} " in line]
            if failed or time.monotonic() > deadline:
                break
            time.sleep(0.25)
        assert len(failed) == 1, failed
        assert "Decimal128" in failed[0] and long_key in failed[0], failed[0]
        assert "the target was removed" in failed[0], failed[0]
    finally:
        _cleanup(client, target, source)
        _delete_backup_once_available(client, backup_arn)


def test_a_non_key_number_of_38_digits_restores_intact(dynamodb_client, unique_table_name, backup_root):
    """A non-key `N` of 38 significant digits (the service's precision, past
    Decimal128's) is stored as the item's text and comes back from a restore
    equal to what the source serves."""
    client = dynamodb_client
    source = unique_table_name
    target = f"{source}-restored"
    backup_arn = None
    big = "12345678901234567890123456789012345678"
    try:
        _composite_table(client, source)
        client.put_item(TableName=source, Item={"pk": {"S": "a"}, "sk": {"N": "1"}, "big": {"N": big}})
        key = {"pk": {"S": "a"}, "sk": {"N": "1"}}
        served = client.get_item(TableName=source, Key=key, ConsistentRead=True)["Item"]
        assert served["big"] == {"N": big}, served
        backup_arn = _create_backup(client, source, f"{source}-backup")
        _wait_backup_available(client, backup_arn)
        client.restore_table_from_backup(TargetTableName=target, BackupArn=backup_arn)
        wait_for_active(client, target)
        restored = client.get_item(TableName=target, Key=key, ConsistentRead=True)["Item"]
        assert restored == served, (restored, served)
    finally:
        _cleanup(client, target, source)
        _delete_backup_once_available(client, backup_arn)
