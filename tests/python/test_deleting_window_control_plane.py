# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Requests during a table's DELETING window.

Amazon DynamoDB keeps a table describable as ``TableStatus DELETING`` for a
few seconds after ``DeleteTable`` returns. In that window a second
``DeleteTable`` and an ``UpdateTable`` answer ``ResourceInUseException`` with
``Attempt to change a resource which is still in use: Table is being deleted:
<name>`` (measured 2026-10-24), and ``PutItem``, ``GetItem``, ``Query`` and
``Scan`` are served; once the table is gone, ``GetItem`` of an item written in
the window is ``ResourceNotFoundException`` (measured 2026-10-25).

``TagResource`` and ``UntagResource`` on the table ARN answer that same
sentence in the window and ``ListTagsOfResource`` is served with the tags; on
the table's stream ARN all three are served (measured 2026-10-26). The
table-ARN answers hold for the first 0.6 to 2.3 s after ``DeleteTable``
returns; after that the service answers all three with
``ResourceNotFoundException`` ``Requested resource not found: ResourceArn:
<arn> not found`` while ``DescribeTable`` still reads ``DELETING`` (measured
2026-11-20, five runs), and the stream ARN stays served. The tests assert the
first answer after ``DeleteTable``. ``DescribeTimeToLive`` answers
``ValidationException`` ``Cannot describe time to live while table is in
DELETING state: Current table state is DELETING``, and ``UpdateTimeToLive`` is
served and answers with the specification it wrote (both measured
2026-10-25).

Against the service these tests run on its natural window. Against ExtendDB on
PostgreSQL the window is held open the way a slow reader would hold it: an
outside transaction keeps ``ACCESS SHARE`` on the table's data table, so the
drop waits and the catalog row stays ``DELETING``. That needs
``EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING`` (a base URL with no database
component) and the catalog database name (``EXTENDDB_TEST_PG_CATALOG_DB``,
default ``extenddb_catalog``). A ``DeleteTable`` that finds another session
holding the data table marks the row ``DELETING`` and leaves the drop to the
control plane, whatever ``control_plane_delay_seconds`` is, so the row stays
``DELETING`` until the lock is released. Without the connection string, or on a
backend other than PostgreSQL (``EXTENDDB_TEST_BACKEND``, exported by
``devtools/run-tests --backend``), these tests skip on ExtendDB; the
storage-level tests in each backend crate carry the rows there.
"""

from __future__ import annotations

import os
import time

import pytest
from botocore.exceptions import ClientError

from helpers import assert_validation, unique_name, wait_for_active, wait_for_deleted

PG_ADMIN = os.environ.get("EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING", "").rstrip("/")
CATALOG_DB = os.environ.get("EXTENDDB_TEST_PG_CATALOG_DB", "extenddb_catalog")
BACKEND = os.environ.get("EXTENDDB_TEST_BACKEND", "").strip()
# What GetItem, and Query and Scan on an index, answer once the table is gone
# (measured 2026-11-24, names that do not exist).
NOT_FOUND = "Requested resource not found"


def _being_deleted(name: str) -> str:
    return f"Attempt to change a resource which is still in use: Table is being deleted: {name}"


def _catalog_setting(key: str) -> str | None:
    import psycopg2

    with psycopg2.connect(f"{PG_ADMIN}/{CATALOG_DB}") as catalog, catalog.cursor() as cur:
        cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
        row = cur.fetchone()
    return row[0] if row else None


class _HeldLock:
    """``ACCESS SHARE`` on ``_ddb_<table_id>``, held until ``release``."""

    def __init__(self, table_id: str):
        import psycopg2

        data_db = _catalog_setting("data_database_name") or CATALOG_DB
        self.conn = psycopg2.connect(f"{PG_ADMIN}/{data_db}")
        cur = self.conn.cursor()
        cur.execute(f'LOCK TABLE "_ddb_{table_id}" IN ACCESS SHARE MODE')

    def release(self) -> None:
        self.conn.rollback()
        self.conn.close()


def _skip_unless_window_can_be_held(dynamodb_client, name: str) -> None:
    """Skip, after removing the table, when this deployment cannot hold the window."""
    reason = None
    if BACKEND and BACKEND != "postgres":
        reason = f"the window is held through PostgreSQL only (EXTENDDB_TEST_BACKEND={BACKEND})"
    elif not PG_ADMIN:
        reason = "EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING is not set; cannot hold the table DELETING"
    if reason is not None:
        dynamodb_client.delete_table(TableName=name)
        pytest.skip(reason)


def _open_window(
    dynamodb_client, is_extenddb: bool, name: str, with_index: bool = False, stream: bool = False
) -> _HeldLock | None:
    """Create the table (with a GSI ``gsi1`` on ``gk`` when ``with_index``, with a
    stream when ``stream`` is set) and, on ExtendDB, hold the lock that keeps it
    DELETING."""
    attributes = [{"AttributeName": "pk", "AttributeType": "S"}]
    extra = {}
    if with_index:
        attributes.append({"AttributeName": "gk", "AttributeType": "S"})
        extra["GlobalSecondaryIndexes"] = [
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ]
    if stream:
        extra["StreamSpecification"] = {"StreamEnabled": True, "StreamViewType": "KEYS_ONLY"}
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=attributes,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        **extra,
    )
    wait_for_active(dynamodb_client, name)
    if not is_extenddb:
        return None
    _skip_unless_window_can_be_held(dynamodb_client, name)
    table_id = dynamodb_client.describe_table(TableName=name)["Table"]["TableId"]
    return _HeldLock(table_id)


def _assert_in_use(exc: ClientError, name: str) -> None:
    err = exc.response["Error"]
    assert err["Code"] == "ResourceInUseException"
    assert err["Message"] == _being_deleted(name)


def _delete_table_after_tag_write(dynamodb_client, name: str):
    """DeleteTable, retried while the service refuses it after a tag write.

    For a few seconds after ``TagResource`` or ``UntagResource`` the service
    answers ``DeleteTable`` on the table with ``ResourceInUseException``
    ``Attempt to change a resource which is still in use: Table tags are being
    updated: <name>`` (measured 2026-10-26 and 2026-11-20). A retried
    ``DeleteTable`` starts no new tag update, where another tag write would.
    ExtendDB has no such state, so its first ``DeleteTable`` is answered.
    """
    for _ in range(30):
        try:
            return dynamodb_client.delete_table(TableName=name)
        except ClientError as exc:
            if "tags are being updated" not in exc.response["Error"]["Message"]:
                raise
            time.sleep(1)
    raise TimeoutError(f"DeleteTable of {name} was still refused after 30 attempts")


class TestDeletingWindowControlPlane:
    def test_second_delete_table_is_being_deleted(self, dynamodb_client, is_extenddb):
        """A second DeleteTable while the first is still running is ResourceInUseException."""
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name)
        try:
            resp = dynamodb_client.delete_table(TableName=name)
            assert resp["TableDescription"]["TableStatus"] == "DELETING"
            with pytest.raises(ClientError) as exc:
                dynamodb_client.delete_table(TableName=name)
            _assert_in_use(exc.value, name)
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name, timeout=120.0)

    def test_update_table_is_being_deleted(self, dynamodb_client, is_extenddb):
        """UpdateTable while the table is DELETING is ResourceInUseException with the DELETING sentence."""
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name)
        try:
            dynamodb_client.delete_table(TableName=name)
            with pytest.raises(ClientError) as exc:
                dynamodb_client.update_table(TableName=name, DeletionProtectionEnabled=True)
            _assert_in_use(exc.value, name)
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name, timeout=120.0)

    def test_tag_changes_are_being_deleted_and_tags_are_listed(self, dynamodb_client, is_extenddb):
        """TagResource and UntagResource while the table is DELETING are ResourceInUseException
        with the DELETING sentence, and ListTagsOfResource lists the tag the table had before
        DeleteTable (measured 2026-11-03). On the service these are the first answers after
        DeleteTable, which hold for 0.6 to 2.3 s (measured 2026-11-20)."""
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name)
        deleted = False
        try:
            arn = dynamodb_client.describe_table(TableName=name)["Table"]["TableArn"]
            dynamodb_client.tag_resource(ResourceArn=arn, Tags=[{"Key": "k1", "Value": "v1"}])
            _delete_table_after_tag_write(dynamodb_client, name)
            deleted = True
            with pytest.raises(ClientError) as exc:
                dynamodb_client.tag_resource(ResourceArn=arn, Tags=[{"Key": "k2", "Value": "v2"}])
            _assert_in_use(exc.value, name)
            with pytest.raises(ClientError) as exc:
                dynamodb_client.untag_resource(ResourceArn=arn, TagKeys=["k1"])
            _assert_in_use(exc.value, name)
            listed = dynamodb_client.list_tags_of_resource(ResourceArn=arn)["Tags"]
            assert listed == [{"Key": "k1", "Value": "v1"}]
        finally:
            if lock is not None:
                lock.release()
            if not deleted:
                _delete_table_after_tag_write(dynamodb_client, name)
        wait_for_deleted(dynamodb_client, name, timeout=120.0)


class TestDeletingWindowDataPlane:
    def test_data_plane_is_served_until_the_table_is_gone(self, dynamodb_client, is_extenddb):
        """PutItem, GetItem, Query and Scan are served while the table is DELETING;
        GetItem of the item written in the window is ResourceNotFoundException once
        the table is gone."""
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name)
        try:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "pre"}})
            resp = dynamodb_client.delete_table(TableName=name)
            assert resp["TableDescription"]["TableStatus"] == "DELETING"

            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "w1"}})
            got = dynamodb_client.get_item(
                TableName=name, Key={"pk": {"S": "pre"}}, ConsistentRead=True
            )
            assert got["Item"] == {"pk": {"S": "pre"}}
            got = dynamodb_client.get_item(
                TableName=name, Key={"pk": {"S": "w1"}}, ConsistentRead=True
            )
            assert got["Item"] == {"pk": {"S": "w1"}}
            queried = dynamodb_client.query(
                TableName=name,
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "pre"}},
                ConsistentRead=True,
            )
            assert queried["Items"] == [{"pk": {"S": "pre"}}]
            scanned = dynamodb_client.scan(TableName=name, Limit=1, ConsistentRead=True)
            assert len(scanned["Items"]) == 1
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name, timeout=120.0)

        with pytest.raises(ClientError) as exc:
            dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "w1"}}, ConsistentRead=True)
        assert_validation(exc.value, NOT_FOUND, "ResourceNotFoundException")

    def test_index_reads_after_the_table_is_gone_are_not_found(self, dynamodb_client, is_extenddb):
        """Query and Scan on an index of a table that is gone are ResourceNotFoundException,
        as GetItem is, after a GetItem during the window: on ExtendDB that read stores the
        DELETING table's key info in the key-info cache, and the entry outlives the table."""
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name, with_index=True)
        try:
            resp = dynamodb_client.delete_table(TableName=name)
            assert resp["TableDescription"]["TableStatus"] == "DELETING"
            got = dynamodb_client.get_item(
                TableName=name, Key={"pk": {"S": "absent"}}, ConsistentRead=True
            )
            assert "Item" not in got
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name, timeout=120.0)

        with pytest.raises(ClientError) as exc:
            dynamodb_client.query(
                TableName=name,
                IndexName="gsi1",
                KeyConditionExpression="gk = :gk",
                ExpressionAttributeValues={":gk": {"S": "x"}},
            )
        assert_validation(exc.value, NOT_FOUND, "ResourceNotFoundException")
        with pytest.raises(ClientError) as exc:
            dynamodb_client.scan(TableName=name, IndexName="gsi1")
        assert_validation(exc.value, NOT_FOUND, "ResourceNotFoundException")


_DESCRIBE_TTL_REFUSED = (
    "ValidationException",
    "Cannot describe time to live while table is in DELETING state: "
    "Current table state is DELETING",
)
_MADE_UP_NOT_FOUND = (
    "ResourceNotFoundException",
    "Requested resource not found: ResourceArn: {made_up} not found",
)
_TTL_ON = {"Enabled": True, "AttributeName": "ttl"}
_STREAM_TAG = [{"Key": "sk", "Value": "sv"}]

# The TTL calls and the stream ARN tag calls in the window, one table per row:
# on the service a tag write starts a tag update that refuses the next tag
# write on the resource for a few seconds. A row is (the table has a stream,
# steps before DeleteTable, steps after it). A step is (operation, request,
# answer), where the answer is the response body or an (error code, message)
# pair. ``{name}``, ``{stream}`` (the table's LatestStreamArn) and
# ``{made_up}`` (a stream ARN of the table whose label names no stream) are
# filled in per table. Measured: the TTL rows 2026-10-25 (three tables, every
# call while DescribeTable read DELETING, the DescribeTimeToLive refusal before
# and after an UpdateTimeToLive); the stream ARN rows 2026-10-26; the made-up
# label 2026-11-20 on a DELETING table and 2026-11-24 on a table that does not
# exist.
WINDOW_ROWS = [
    pytest.param(
        False,
        [],
        [("describe_time_to_live", {"TableName": "{name}"}, _DESCRIBE_TTL_REFUSED)],
        id="describe-ttl",
    ),
    pytest.param(
        False,
        [],
        [
            (
                "update_time_to_live",
                {"TableName": "{name}", "TimeToLiveSpecification": _TTL_ON},
                {"TimeToLiveSpecification": _TTL_ON},
            ),
            ("describe_time_to_live", {"TableName": "{name}"}, _DESCRIBE_TTL_REFUSED),
        ],
        id="update-ttl",
    ),
    pytest.param(
        True,
        [],
        [("tag_resource", {"ResourceArn": "{stream}", "Tags": _STREAM_TAG}, {})],
        id="tag-stream",
    ),
    pytest.param(
        True,
        [("tag_resource", {"ResourceArn": "{stream}", "Tags": _STREAM_TAG}, {})],
        [
            ("list_tags_of_resource", {"ResourceArn": "{stream}"}, {"Tags": _STREAM_TAG}),
            ("untag_resource", {"ResourceArn": "{stream}", "TagKeys": ["sk"]}, {}),
        ],
        id="untag-stream",
    ),
    pytest.param(
        True,
        [],
        [
            (
                "tag_resource",
                {"ResourceArn": "{made_up}", "Tags": [{"Key": "fk", "Value": "fv"}]},
                _MADE_UP_NOT_FOUND,
            ),
            ("list_tags_of_resource", {"ResourceArn": "{made_up}"}, _MADE_UP_NOT_FOUND),
        ],
        id="made-up-stream-label",
    ),
]


def _fill(value, values: dict):
    """``value`` with each ``{key}`` in its strings replaced from ``values``."""
    if isinstance(value, str):
        return value.format(**values)
    if isinstance(value, dict):
        return {key: _fill(item, values) for key, item in value.items()}
    if isinstance(value, list):
        return [_fill(item, values) for item in value]
    return value


def _run_step(dynamodb_client, step, values: dict) -> None:
    """Send one request of a row and assert its whole answer."""
    operation, request, answer = step
    call = getattr(dynamodb_client, operation)
    if isinstance(answer, tuple):
        code, message = answer
        with pytest.raises(ClientError) as exc:
            call(**_fill(request, values))
        assert_validation(exc.value, _fill(message, values), code)
    else:
        resp = call(**_fill(request, values))
        resp.pop("ResponseMetadata", None)
        assert resp == _fill(answer, values), operation


class TestDeletingWindowTimeToLiveAndStreamTags:
    """``DescribeTimeToLive`` is refused during the window and
    ``UpdateTimeToLive`` runs its ordinary rules. The table's stream ARN is a
    resource of its own: both tag writes and the read are served on it, where
    the table ARN refuses the writes (``TestDeletingWindowControlPlane``). A
    stream ARN whose label names no stream is ``ResourceNotFoundException``.
    ``DescribeTimeToLive`` on a ``CREATING`` table is refused with the same
    sentence naming ``CREATING`` (measured 2026-10-26); the fixture cannot hold
    a table ``CREATING``, so that row is pinned in the engine's unit tests."""

    @pytest.mark.parametrize("stream, before, after", WINDOW_ROWS)
    def test_answers_in_the_window(self, dynamodb_client, is_extenddb, stream, before, after):
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name, stream=stream)
        deleted = False
        try:
            table = dynamodb_client.describe_table(TableName=name)["Table"]
            values = {
                "name": name,
                "stream": table.get("LatestStreamArn", ""),
                "made_up": f"{table['TableArn']}/stream/2000-01-01T00:00:00.000",
            }
            for step in before:
                _run_step(dynamodb_client, step, values)
            _delete_table_after_tag_write(dynamodb_client, name)
            deleted = True
            for step in after:
                _run_step(dynamodb_client, step, values)
        finally:
            if lock is not None:
                lock.release()
            if not deleted:
                _delete_table_after_tag_write(dynamodb_client, name)
        wait_for_deleted(dynamodb_client, name, timeout=120.0)
