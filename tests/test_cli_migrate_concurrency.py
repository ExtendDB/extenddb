# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Migration concurrency CLI tests.

Cover the PostgreSQL migration lock that serializes `extenddb migrate`, so two
migrators that start together cannot both apply a migration.

The barrier: the runner commits a migration's body and its ledger row in one
transaction, so a session that holds an uncommitted `extenddb_migrations` row
for the same id makes the migrator's ledger insert wait, inside its
transaction and under the migration lock, at a known point. Each statement of
a body waits at most 5 s for a lock and is retried three times, so the tests
release the barrier within that window.

Like the other CLI lifecycle tests these require a PostgreSQL instance
(EXTENDDB_TEST_PG_CONNECTION_STRING) and a built binary, and are excluded from
the backend-agnostic pytest suite.
"""

from __future__ import annotations

import os
import select
import signal
import subprocess
import time

from lifecycle_helpers import (
    EXTENDDB_BINARY,
    PG_ADMIN_CONN,
    _init_args,
    _pg_args,
    _run_extenddb,
)

# The advisory key of the migration lock, `NAMESPACE` and `MIGRATION_KEY` in
# crates/storage-postgres/src/migrations/locks.rs.
LOCK_NAMESPACE = 0x0045_4442
MIGRATION_LOCK_OBJID = 1

WAITING = "Another migrator holds the migration lock"


def _init(cli_env):
    result = _run_extenddb(
        "init", *_init_args(cli_env),
        config=cli_env["config_path"],
        env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _migrate_cmd(cli_env):
    return [
        EXTENDDB_BINARY, "migrate", "--yes", *_pg_args(),
        "--config", cli_env["config_path"],
    ]


def _decode(data):
    """Decode captured subprocess output for assertions and diagnostics."""
    return data.decode("utf-8", errors="replace")


def _read_until(proc, expected, timeout=15.0):
    """Read raw stdout until expected appears, without waiting for process exit."""
    assert proc.stdout is not None
    expected_bytes = expected.encode()
    output = bytearray()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        ready, _, _ = select.select([proc.stdout], [], [], min(0.1, remaining))
        if not ready:
            if proc.poll() is not None:
                output.extend(os.read(proc.stdout.fileno(), 4096))
                break
            continue
        chunk = os.read(proc.stdout.fileno(), 4096)
        if not chunk:
            break
        output.extend(chunk)
        if expected_bytes in output:
            return _decode(output)
    raise AssertionError(
        f"timed out waiting for {expected!r}; output so far: {_decode(output)!r}"
    )


def _lose_migration_1_in_the_data_database(data_db):
    """The data database without migration 1: its `gsi_pending` table and its
    ledger row gone, so `migrate --yes` has one body to run there."""
    import psycopg2

    conn = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS gsi_pending")
            cur.execute("DELETE FROM extenddb_migrations WHERE id = 1")
    finally:
        conn.close()


def _barrier(data_db):
    """A session holding an uncommitted ledger row for migration 1 in the data
    database. Roll it back to let the migrator's own insert through."""
    import psycopg2

    blocker = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
    with blocker.cursor() as cur:
        cur.execute(
            "INSERT INTO extenddb_migrations (id, name, kind, checksum, outcome, applied_by) "
            "VALUES (1, 'baseline', 'breaking', 'barrier', 'applied', 'the test')"
        )
    return blocker


def _wait_for_blocked_ledger_insert(data_db, exclude=(), timeout=15.0):
    """Wait until a migrator waits on the barrier, inside its transaction, in
    a session other than those in `exclude`; returns that session's pid."""
    import psycopg2

    observer = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
    observer.autocommit = True
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with observer.cursor() as cur:
                cur.execute(
                    """
                    SELECT pid FROM pg_stat_activity
                    WHERE datname = %s
                      AND state = 'active'
                      AND wait_event_type = 'Lock'
                      AND query LIKE 'INSERT INTO extenddb_migrations%%'
                      AND NOT (pid = ANY(%s))
                    """,
                    (data_db, list(exclude)),
                )
                row = cur.fetchone()
                if row:
                    return row[0]
            time.sleep(0.05)
        with observer.cursor() as cur:
            cur.execute(
                "SELECT datname, pid, application_name, state, wait_event_type, left(query, 60) "
                "FROM pg_stat_activity WHERE datname IN (%s, %s)",
                (data_db, data_db + "_catalog"),
            )
            sessions = cur.fetchall()
    finally:
        observer.close()
    raise AssertionError(
        f"migrator did not block on the extenddb_migrations insert; sessions: {sessions}"
    )


def _assert_applied_once(data_db):
    import psycopg2

    conn = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.gsi_pending') IS NOT NULL")
            assert cur.fetchone()[0] is True
            cur.execute("SELECT count(*), max(checksum) FROM extenddb_migrations WHERE id = 1")
            count, checksum = cur.fetchone()
            assert count == 1
            assert checksum != "barrier", "the barrier row was committed"
    finally:
        conn.close()


def _terminate(proc):
    """Best-effort cleanup for a subprocess after a failed assertion."""
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate(timeout=5)


class TestMigrateConcurrency:
    """Concurrent `migrate` runs are serialized by an advisory lock."""

    def test_concurrent_migrate_no_race(self, cli_env):
        _init(cli_env)
        data_db = cli_env["db_name"][: -len("_catalog")]
        _lose_migration_1_in_the_data_database(data_db)
        blocker = _barrier(data_db)

        cmd = _migrate_cmd(cli_env)
        p1 = None
        p2 = None
        barrier_released = False
        try:
            p1 = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL, bufsize=0,
            )
            _wait_for_blocked_ledger_insert(data_db)

            # The first migrator holds the migration lock at a known point.
            # The second stops at the lock, and says so at once.
            p2 = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL, bufsize=0,
            )
            p2_prefix = _read_until(p2, WAITING)
            assert p2.poll() is None, "second migrator exited instead of waiting"

            blocker.rollback()
            barrier_released = True

            out1_raw = p1.communicate(timeout=60)
            out2_tail_raw = p2.communicate(timeout=60)
            out1 = (_decode(out1_raw[0]), _decode(out1_raw[1]))
            out2 = (p2_prefix + _decode(out2_tail_raw[0]), _decode(out2_tail_raw[1]))
        finally:
            try:
                if not barrier_released:
                    blocker.rollback()
            finally:
                try:
                    blocker.close()
                finally:
                    try:
                        _terminate(p1)
                    finally:
                        _terminate(p2)

        assert p1.returncode == 0, out1
        assert p2.returncode == 0, out2

        # The first applied migration 1 in the data database; the second read
        # the ledger only after the first committed and released the lock.
        assert "Applied 1 baseline" in out1[0], out1
        assert "Applied" not in out2[0], out2
        assert "Up to date" in out2[0], out2
        _assert_applied_once(data_db)

    def test_sigkill_releases_lock_and_waiting_peer_recovers(self, cli_env):
        """A killed lock holder releases the lock, commits nothing, and the
        waiting peer applies the migration.

        The killed holder's body session can outlive the process: it is blocked
        on the barrier, and the database notices its client is gone only through
        `client_connection_check_interval` or when the lock wait ends. The peer
        takes the lock at once, then waits until no session of an earlier
        migrator runs a statement, so it never runs migration 1 beside the
        holder's uncommitted tables.
        """
        import psycopg2

        _init(cli_env)
        data_db = cli_env["db_name"][: -len("_catalog")]
        _lose_migration_1_in_the_data_database(data_db)
        blocker = _barrier(data_db)

        cmd = _migrate_cmd(cli_env)
        holder = None
        peer = None
        barrier_released = False
        try:
            holder = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL, bufsize=0,
            )
            holder_session = _wait_for_blocked_ledger_insert(data_db)

            peer = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL, bufsize=0,
            )
            peer_waiting = _read_until(peer, WAITING)
            assert peer.poll() is None, "peer exited instead of waiting for holder"

            # SIGKILL gives the holder no chance to release. PostgreSQL ends the
            # lock's session at once, which frees the lock for the waiting peer,
            # and the body's session when its lock wait ends.
            holder.kill()
            holder_out_raw = holder.communicate(timeout=10)
            holder_out = (_decode(holder_out_raw[0]), _decode(holder_out_raw[1]))
            assert holder.returncode == -signal.SIGKILL, holder_out

            observer = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
            observer.autocommit = True
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    with observer.cursor() as cur:
                        cur.execute(
                            "SELECT count(*) FROM pg_stat_activity WHERE pid = %s",
                            (holder_session,),
                        )
                        if cur.fetchone()[0] == 0:
                            break
                    time.sleep(0.1)
                else:
                    raise AssertionError("the killed holder's body session did not end")
            finally:
                observer.close()

            blocker.rollback()
            barrier_released = True

            peer_tail_raw = peer.communicate(timeout=90)
            peer_out = (peer_waiting + _decode(peer_tail_raw[0]), _decode(peer_tail_raw[1]))
        finally:
            try:
                if not barrier_released:
                    blocker.rollback()
            finally:
                try:
                    blocker.close()
                finally:
                    try:
                        _terminate(holder)
                    finally:
                        _terminate(peer)

        assert WAITING in peer_out[0], peer_out[0]
        assert peer.returncode == 0, peer_out
        assert "Applied 1 baseline" in peer_out[0], peer_out[0]
        _assert_applied_once(data_db)

    def test_migrate_waits_for_a_held_lock(self, cli_env):
        """migrate blocks on a lock held elsewhere, and says so immediately.

        The lock is held from an external session, so contention is guaranteed
        instead of depending on process timing.
        """
        import psycopg2

        _init(cli_env)

        # Advisory locks are scoped to a database, and migrate takes this one on
        # the catalog database. Session- and transaction-level requests for the
        # same key conflict with each other.
        conn = psycopg2.connect(PG_ADMIN_CONN + "/" + cli_env["db_name"])
        conn.autocommit = True
        proc = None
        lock_held = False
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_lock(%s, %s)",
                    (LOCK_NAMESPACE, MIGRATION_LOCK_OBJID),
                )
            lock_held = True
            proc = subprocess.Popen(
                _migrate_cmd(cli_env), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL, bufsize=0,
            )
            # The explanation appears while the external session still holds
            # the lock, so stdout is flushed before the wait.
            prefix = _read_until(proc, WAITING)
            assert proc.poll() is None, "migrate should block while the lock is held"

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_unlock(%s, %s)",
                    (LOCK_NAMESPACE, MIGRATION_LOCK_OBJID),
                )
            lock_held = False

            out_tail, err = proc.communicate(timeout=60)
            out = prefix + _decode(out_tail)
            err = _decode(err)
            assert proc.returncode == 0, (out, err)
            assert WAITING in out, out
            assert "Up to date" in out, out
        finally:
            try:
                if lock_held:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_advisory_unlock(%s, %s)",
                            (LOCK_NAMESPACE, MIGRATION_LOCK_OBJID),
                        )
            finally:
                try:
                    conn.close()
                finally:
                    _terminate(proc)
