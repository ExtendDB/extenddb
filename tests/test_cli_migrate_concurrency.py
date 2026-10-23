# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Migration concurrency CLI tests.

Cover the PostgreSQL advisory lock that serializes `extenddb migrate`, so two
replicas starting at once cannot race each other applying schema changes.

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

# Must match ADVISORY_LOCK_NAMESPACE and MIGRATION_LOCK_OBJID in
# crates/storage-postgres/src/bootstrapper.rs.
LOCK_NAMESPACE = 0x0045_4442
MIGRATION_LOCK_OBJID = 1


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


def _wait_for_blocked_ledger_insert(data_db, timeout=15.0):
    """Wait until a migrator is blocked recording 002 in schema_history."""
    import psycopg2

    observer = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
    observer.autocommit = True
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with observer.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM pg_stat_activity
                        WHERE datname = %s
                          AND state = 'active'
                          AND wait_event_type = 'Lock'
                          AND query LIKE 'INSERT INTO schema_history%%'
                    )
                    """,
                    (data_db,),
                )
                if cur.fetchone()[0]:
                    return
            time.sleep(0.05)
    finally:
        observer.close()
    raise AssertionError("migrator did not block on the schema_history insert")


def _assert_002_applied_but_unrecorded(data_db):
    """Prove the barrier is after migration commit and before ledger commit."""
    import psycopg2

    observer = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
    observer.autocommit = True
    try:
        with observer.cursor() as cur:
            cur.execute("SELECT to_regclass('public.gsi_pending') IS NOT NULL")
            assert cur.fetchone()[0] is True, "002 schema should already be committed"
            cur.execute(
                "SELECT count(*) FROM schema_history "
                "WHERE filename = '002_gsi_pending.sql'"
            )
            assert cur.fetchone()[0] == 0, "002 ledger row should still be uncommitted"
    finally:
        observer.close()


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
        import psycopg2

        _init(cli_env)
        data_db = cli_env["db_name"][: -len("_catalog")]

        # Simulate a pre-002 deployment so migrate has real work to do.
        blocker = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
        blocker.autocommit = True
        with blocker.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS gsi_pending")
            cur.execute(
                "DELETE FROM schema_history WHERE filename = '002_gsi_pending.sql'"
            )

        # Insert the ledger row without committing it. A migrator's ordinary
        # pending check cannot see this row, so it applies 002, then its own
        # INSERT blocks on the uncommitted unique-key conflict. This gives the
        # test a deterministic point after migration SQL and before recording.
        blocker.autocommit = False
        with blocker.cursor() as cur:
            cur.execute(
                "INSERT INTO schema_history (filename) VALUES ('002_gsi_pending.sql')"
            )

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
            _assert_002_applied_but_unrecorded(data_db)

            # The first migrator now holds the migration lock at a known point.
            # The second must stop at that lock rather than reach its pending
            # check or ledger insert. Reading this line before releasing the
            # blocker also proves that wait visibility is flushed immediately.
            p2 = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL, bufsize=0,
            )
            p2_prefix = _read_until(p2, "waiting for it to finish")
            assert p2.poll() is None, "second migrator exited instead of waiting"

            blocker.rollback()
            barrier_released = True

            out1_raw = p1.communicate(timeout=60)
            out2_tail_raw = p2.communicate(timeout=60)
            out1 = (_decode(out1_raw[0]), _decode(out1_raw[1]))
            out2 = (
                p2_prefix + _decode(out2_tail_raw[0]),
                _decode(out2_tail_raw[1]),
            )
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

        # Exactly one process applied 002. The second took its pending snapshot
        # only after the first committed the ledger row and released the lock.
        outputs = [out1[0], out2[0]]
        applied = [o for o in outputs if "Applying 002_gsi_pending.sql" in o]
        assert len(applied) == 1, f"expected exactly one migrator to apply 002: {outputs}"
        others = [o for o in outputs if "Applying 002_gsi_pending.sql" not in o]
        assert len(others) == 1, outputs
        assert (
            "Everything is up to date" in others[0]
            or "002_gsi_pending.sql — already applied" in others[0]
        ), f"the second migrator should have observed 002 as done: {others[0]}"
        assert "Migration lock acquired" in out2[0], out2[0]

        # The migration and its ledger entry each landed exactly once.
        conn = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.gsi_pending') IS NOT NULL")
                assert cur.fetchone()[0] is True
                cur.execute(
                    "SELECT count(*) FROM schema_history "
                    "WHERE filename = '002_gsi_pending.sql'"
                )
                assert cur.fetchone()[0] == 1
        finally:
            conn.close()

    def test_sigkill_releases_lock_and_waiting_peer_recovers(self, cli_env):
        """A killed lock holder releases the lock and exposes the ledger gap."""
        import psycopg2

        _init(cli_env)
        data_db = cli_env["db_name"][: -len("_catalog")]

        blocker = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
        blocker.autocommit = True
        with blocker.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS gsi_pending")
            cur.execute(
                "DELETE FROM schema_history WHERE filename = '002_gsi_pending.sql'"
            )
        blocker.autocommit = False
        with blocker.cursor() as cur:
            cur.execute(
                "INSERT INTO schema_history (filename) VALUES ('002_gsi_pending.sql')"
            )

        cmd = _migrate_cmd(cli_env)
        holder = None
        peer = None
        barrier_released = False
        try:
            holder = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL, bufsize=0,
            )
            _wait_for_blocked_ledger_insert(data_db)
            _assert_002_applied_but_unrecorded(data_db)

            peer = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL, bufsize=0,
            )
            peer_waiting = _read_until(peer, "waiting for it to finish")
            assert peer.poll() is None, "peer exited instead of waiting for holder"

            # SIGKILL gives the holder no chance to call release. PostgreSQL must
            # roll back the dedicated transaction when the connection dies,
            # which releases pg_advisory_xact_lock for the waiting peer.
            holder.kill()
            holder_out_raw = holder.communicate(timeout=10)
            holder_out = (_decode(holder_out_raw[0]), _decode(holder_out_raw[1]))
            assert holder.returncode == -signal.SIGKILL, holder_out

            peer_acquired = _read_until(peer, "Migration lock acquired")
            assert peer.poll() is None, "peer should next block on the ledger barrier"

            blocker.rollback()
            barrier_released = True

            peer_tail_raw = peer.communicate(timeout=60)
            peer_out = (
                peer_waiting + peer_acquired + _decode(peer_tail_raw[0]),
                _decode(peer_tail_raw[1]),
            )
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

        assert peer.returncode == 0, peer_out
        assert "waiting for it to finish" in peer_out[0], peer_out[0]
        assert "Migration lock acquired" in peer_out[0], peer_out[0]

        conn = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.gsi_pending') IS NOT NULL")
                assert cur.fetchone()[0] is True
                cur.execute(
                    "SELECT count(*) FROM schema_history "
                    "WHERE filename = '002_gsi_pending.sql'"
                )
                assert cur.fetchone()[0] == 1
        finally:
            conn.close()

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
            # Observe the explanation while the external session still holds
            # the lock. This also guards the explicit stdout flush in Rust.
            prefix = _read_until(proc, "waiting for it to finish")
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
            assert "waiting for it to finish" in out, out
            assert "Migration lock acquired" in out, out
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
