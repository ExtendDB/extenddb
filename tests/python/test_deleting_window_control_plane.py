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


def _open_window(dynamodb_client, is_extenddb: bool, name: str) -> _HeldLock | None:
    """Create the table and, on ExtendDB, hold the lock that keeps it DELETING."""
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
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


class TestConcurrentDeleteTable:
    """Two DeleteTable calls sent at once, which needs no held window.

    The service answers one of them 200 and the other
    ``ResourceInUseException`` with the sentence above (measured 2026-10-25,
    five rounds of five). Cassandra and MongoDB answer the same. PostgreSQL
    and SQLite with ``control_plane_delay_seconds`` under 1 (the CI setting)
    remove the catalog row inside the request, so a second call that arrives
    after the first has finished finds no row and answers
    ``ResourceNotFoundException``; that answer is accepted there only. The
    PostgreSQL and SQLite CI legs run ``devtools/run-tests`` without
    ``--backend``, so an empty ``EXTENDDB_TEST_BACKEND`` counts as one of them.
    Two 200s mean both calls ran the deletion, which is what #553 reports on
    Cassandra and #550 fixed on SQLite and MongoDB.
    """

    ROUNDS = 3
    ROW_REMOVED_IN_REQUEST = ("", "postgres", "sqlite")

    def test_only_one_of_two_concurrent_delete_tables_deletes(
        self, dynamodb_client, endpoint_url, is_extenddb
    ):
        from concurrent.futures import ThreadPoolExecutor

        from helpers import make_dynamodb_client

        loser_codes = ("ResourceInUseException",)
        if is_extenddb and BACKEND in self.ROW_REMOVED_IN_REQUEST:
            loser_codes += ("ResourceNotFoundException",)

        for round_index in range(self.ROUNDS):
            name = unique_name("del-pair")
            dynamodb_client.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(dynamodb_client, name)

            def _delete(_worker: int, table: str = name):
                client = make_dynamodb_client(endpoint_url)
                try:
                    return ("ok", client.delete_table(TableName=table))
                except ClientError as exc:
                    return ("err", exc)

            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(_delete, range(2)))

            accepted = [o for kind, o in outcomes if kind == "ok"]
            refused = [o for kind, o in outcomes if kind == "err"]
            assert len(accepted) == 1, (
                f"round {round_index}: {len(accepted)} of the two DeleteTable calls were "
                f"accepted; one call deletes the table and the other is refused"
            )
            assert accepted[0]["TableDescription"]["TableStatus"] == "DELETING"
            err = refused[0].response["Error"]
            assert err["Code"] in loser_codes, (round_index, BACKEND, err)
            if err["Code"] == "ResourceInUseException":
                assert err["Message"] == _being_deleted(name)

            wait_for_deleted(dynamodb_client, name, timeout=120.0)
