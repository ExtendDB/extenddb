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
the table's stream ARN all three are served (measured 2026-10-26).
``DescribeTimeToLive`` answers ``ValidationException`` ``Cannot describe time
to live while table is in DELETING state: Current table state is DELETING``,
and ``UpdateTimeToLive`` is served and answers with the specification it wrote
(both measured 2026-10-25).

Against the service these tests run on its natural window. Against ExtendDB on
PostgreSQL the window is held open the way a slow reader would hold it: an
outside transaction keeps ``ACCESS SHARE`` on the table's data table, so the
drop waits and the catalog row stays ``DELETING``. That needs
``EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING`` (a base URL with no database
component) and the catalog database name (``EXTENDDB_TEST_PG_CATALOG_DB``,
default ``extenddb_catalog``), and a server whose ``DeleteTable`` marks the row
``DELETING`` before the data drop. Before #504 that is the asynchronous path
only, taken when ``control_plane_delay_seconds`` is 1 or more; on the
immediate path the catalog row is removed first and the drop waits on the held
lock inside the request, so the tests read the server's delay from the
``settings`` table and skip when it is under 1. #504 marks the row on both
paths and removes that condition. Without the connection string, or on a
backend other than PostgreSQL (``EXTENDDB_TEST_BACKEND``, exported by
``devtools/run-tests --backend``), these tests skip on ExtendDB; the
storage-level tests in each backend crate carry the rows there.
"""

from __future__ import annotations

import os
import time

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

PG_ADMIN = os.environ.get("EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING", "").rstrip("/")
CATALOG_DB = os.environ.get("EXTENDDB_TEST_PG_CATALOG_DB", "extenddb_catalog")
BACKEND = os.environ.get("EXTENDDB_TEST_BACKEND", "").strip()


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
    else:
        delay = _catalog_setting("control_plane_delay_seconds")
        try:
            delay_secs = float(delay) if delay is not None else 0.25
        except ValueError:
            delay_secs = 0.25
        if delay_secs < 1.0:
            reason = (
                f"control_plane_delay_seconds is {delay}: DeleteTable removes the catalog row "
                "before the drop, so a held lock blocks the request rather than holding the row "
                "DELETING (#504 removes this condition)"
            )
    if reason is not None:
        dynamodb_client.delete_table(TableName=name)
        pytest.skip(reason)


def _open_window(
    dynamodb_client, is_extenddb: bool, name: str, stream: bool = False
) -> _HeldLock | None:
    """Create the table, with a stream when ``stream`` is set, and on ExtendDB
    hold the lock that keeps it DELETING."""
    extra = (
        {"StreamSpecification": {"StreamEnabled": True, "StreamViewType": "KEYS_ONLY"}}
        if stream
        else {}
    )
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
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


def _settle_tags(dynamodb_client, is_extenddb: bool, arn: str) -> None:
    """On the service, wait out the tag update a tag write starts.

    For several seconds after ``TagResource`` or ``UntagResource`` the service
    answers another tag write on the resource with ``LimitExceededException``
    and ``DeleteTable`` on the table with ``ResourceInUseException``, both
    ``Table tags are being updated: <name>`` (measured 2026-10-26). An
    ``UntagResource`` of a key the resource does not carry is retried until it
    is answered. ExtendDB has no such state.
    """
    if is_extenddb:
        return
    for _ in range(30):
        try:
            dynamodb_client.untag_resource(ResourceArn=arn, TagKeys=["settle-probe"])
            return
        except ClientError as exc:
            if "tags are being updated" not in exc.response["Error"]["Message"]:
                raise
            time.sleep(1)
    raise TimeoutError(f"the tags of {arn} were still being updated after 30 attempts")


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
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestDeletingWindowTimeToLive:
    """``DescribeTimeToLive`` is refused during the window and
    ``UpdateTimeToLive`` runs its ordinary rules (measured 2026-10-25 on three
    tables, every call issued while ``DescribeTable`` read ``DELETING``).
    ``DescribeTimeToLive`` on a ``CREATING`` table is refused with the same
    sentence naming ``CREATING`` (measured 2026-10-26); the fixture cannot hold
    a table ``CREATING``, so that row is pinned in the engine's unit tests."""

    def test_describe_time_to_live_is_refused(self, dynamodb_client, is_extenddb):
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name)
        try:
            dynamodb_client.delete_table(TableName=name)
            with pytest.raises(ClientError) as exc:
                dynamodb_client.describe_time_to_live(TableName=name)
            err = exc.value.response["Error"]
            assert err["Code"] == "ValidationException"
            assert err["Message"] == (
                "Cannot describe time to live while table is in DELETING state: "
                "Current table state is DELETING"
            )
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name, timeout=120.0)

    def test_update_time_to_live_is_served(self, dynamodb_client, is_extenddb):
        """The enable is accepted and answers with the specification it wrote;
        DescribeTimeToLive stays refused after it."""
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name)
        try:
            dynamodb_client.delete_table(TableName=name)
            resp = dynamodb_client.update_time_to_live(
                TableName=name,
                TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
            )
            assert resp["TimeToLiveSpecification"] == {
                "Enabled": True,
                "AttributeName": "ttl",
            }
            with pytest.raises(ClientError) as exc:
                dynamodb_client.describe_time_to_live(TableName=name)
            assert exc.value.response["Error"]["Code"] == "ValidationException"
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name, timeout=120.0)


class TestDeletingWindowTags:
    """``TagResource`` and ``UntagResource`` on the table ARN are refused during
    the window and ``ListTagsOfResource`` is served with the tags (measured
    2026-10-25). The table's stream ARN is a resource of its own: both writes
    and the read are served on it during the window (measured 2026-10-26)."""

    def test_tag_calls_during_the_window(self, dynamodb_client, is_extenddb):
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name)
        try:
            arn = dynamodb_client.describe_table(TableName=name)["Table"]["TableArn"]
            dynamodb_client.tag_resource(
                ResourceArn=arn, Tags=[{"Key": "env", "Value": "test"}]
            )
            _settle_tags(dynamodb_client, is_extenddb, arn)
            dynamodb_client.delete_table(TableName=name)

            with pytest.raises(ClientError) as exc:
                dynamodb_client.tag_resource(
                    ResourceArn=arn, Tags=[{"Key": "added", "Value": "in-window"}]
                )
            _assert_in_use(exc.value, name)

            with pytest.raises(ClientError) as exc:
                dynamodb_client.untag_resource(ResourceArn=arn, TagKeys=["env"])
            _assert_in_use(exc.value, name)

            listed = dynamodb_client.list_tags_of_resource(ResourceArn=arn)
            assert listed["Tags"] == [{"Key": "env", "Value": "test"}]
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name, timeout=120.0)

    def test_stream_arn_tag_resource_is_served(self, dynamodb_client, is_extenddb):
        """TagResource on the stream ARN, the first tag write after the delete,
        answers 200 with an empty body."""
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name, stream=True)
        try:
            stream_arn = dynamodb_client.describe_table(TableName=name)["Table"]["LatestStreamArn"]
            dynamodb_client.delete_table(TableName=name)

            resp = dynamodb_client.tag_resource(
                ResourceArn=stream_arn, Tags=[{"Key": "sk", "Value": "sv"}]
            )
            resp.pop("ResponseMetadata", None)
            assert resp == {}
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name, timeout=120.0)

    def test_stream_arn_untag_resource_is_served(self, dynamodb_client, is_extenddb):
        """ListTagsOfResource on the stream ARN answers with the tags written
        before the delete, and UntagResource on it, the first tag write after
        the delete, answers 200 with an empty body."""
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name, stream=True)
        try:
            stream_arn = dynamodb_client.describe_table(TableName=name)["Table"]["LatestStreamArn"]
            dynamodb_client.tag_resource(
                ResourceArn=stream_arn, Tags=[{"Key": "sk", "Value": "sv"}]
            )
            _settle_tags(dynamodb_client, is_extenddb, stream_arn)
            dynamodb_client.delete_table(TableName=name)

            listed = dynamodb_client.list_tags_of_resource(ResourceArn=stream_arn)
            assert listed["Tags"] == [{"Key": "sk", "Value": "sv"}]

            resp = dynamodb_client.untag_resource(ResourceArn=stream_arn, TagKeys=["sk"])
            resp.pop("ResponseMetadata", None)
            assert resp == {}
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name, timeout=120.0)
