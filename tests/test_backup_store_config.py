# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The `[backup.store]` configuration at the server's boundary.

Each test starts its own `extenddb serve --foreground` on an isolated
deployment and reads `/health`, the server's log and the API's answers:

- the component `backup_store` on `/health`: absent without a store, healthy
  with a filesystem store, `checking` while an S3 store's first check is in
  flight, unhealthy (status `degraded`) while an S3 endpoint refuses
  connections or stalls, and healthy again once it answers, without a restart;
- the restore and delete of a store-backed backup refused with a
  ValidationException naming the store's kind and the problem's category and
  not its root while the store is checking or unhealthy; CreateBackup served
  throughout (this release's create writes the in-catalog format, which
  touches no store);
- a stalled S3 endpoint holding the listener for no longer than the server's
  own startup, and each reconciler cycle for no longer than the check's bound;
- the configuration errors that stop startup with the key, path or bucket in
  the message: an S3 store without `spool_path` on PostgreSQL, a `spool_path`
  that is the database directory, a filesystem root that does not exist, and
  a defaulted SQLite root under a group-writable parent configured explicitly
  (the same root defaulted is a warning that leaves the store unconfigured
  while the catalog has no rows for it; with rows for it the defaulted root
  is handled as a configured one: unhealthy, reported, its rows refused);
- the default root beside a SQLite database file created `0700` with
  `extenddb-store.json` in it, and a relative root resolved against the
  shell's directory in daemon mode;
- `extenddb backup list` and `backup purge --confirm` on an empty store, and
  `backup purge` refused while a server holds the SQLite serve lock.

The binary at `EXTENDDB_BINARY` decides which cells run: a SQLite build runs
the SQLite cells, a PostgreSQL build (with
`EXTENDDB_TEST_PG_CONNECTION_STRING`) the PostgreSQL ones. The S3 cells need
moto at `EXTENDDB_TEST_S3_ENDPOINT` (an `http://127.0.0.1:<port>` endpoint)
and skip with a reason otherwise. Excluded from the backend-agnostic suite.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import ssl
import subprocess
import threading
import time
import urllib.request
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

import lifecycle_helpers as lh

EXTENDDB_BINARY = lh.EXTENDDB_BINARY
S3_ENDPOINT = os.environ.get("EXTENDDB_TEST_S3_ENDPOINT", "").strip()
ADMIN_PASSWORD = "TestPass1!"
_DEVTOOLS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "devtools")


def _health(port: int) -> dict:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(f"https://127.0.0.1:{port}/health")
    with urllib.request.urlopen(req, context=ctx, timeout=5) as resp:
        assert resp.status == 200
        return json.loads(resp.read())


def _wait_health(port: int, predicate, timeout: float = 20.0, server: "_Server | None" = None) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if server is not None and server.proc.poll() is not None:
            raise AssertionError(f"the server exited {server.proc.returncode}:\n{server.log_text()}")
        try:
            last = _health(port)
            if predicate(last):
                return last
        except Exception:  # noqa: BLE001 - the server may not listen yet
            pass
        time.sleep(0.25)
    log = server.log_text() if server is not None else ""
    raise AssertionError(f"/health did not reach the expected state; last: {last!r}\n{log}")


def _append(config_path: str, toml: str) -> None:
    with open(config_path, "a") as f:
        f.write("\n" + toml + "\n")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Server:
    """A `serve --foreground` process with its log in a file."""

    def __init__(self, config_path: str, log_path: str, env: dict | None = None):
        self.log_path = log_path
        self.log = open(log_path, "wb")  # noqa: SIM115 - closed in stop()
        # A SQLite deployment's paths are relative to the config's directory.
        self.proc = subprocess.Popen(
            [EXTENDDB_BINARY, "serve", "--foreground", "--config", config_path],
            stdout=self.log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=env,
            cwd=os.path.dirname(os.path.abspath(config_path)),
        )

    def log_text(self) -> str:
        self.log.flush()
        with open(self.log_path, errors="replace") as f:
            return f.read()

    def wait_exit(self, timeout: float = 20.0) -> int:
        return self.proc.wait(timeout=timeout)

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.log.close()


def _provision(port: int, cert_path: str, admin_password: str) -> dict:
    env = os.environ.copy()
    env.update(
        {
            "EXTENDDB_TEST_ENDPOINT": f"https://127.0.0.1:{port}",
            "EXTENDDB_ADMIN_USER": "admin",
            "EXTENDDB_ADMIN_PASSWORD": admin_password,
            "EXTENDDB_CA_CERT": cert_path,
        }
    )
    res = subprocess.run(
        ["python3", os.path.join(_DEVTOOLS, "provision-test-credentials")],
        capture_output=True,
        text=True,
        env=env,
        check=True,
        timeout=60,
    )
    creds = {}
    for line in res.stdout.splitlines():
        line = line.strip()
        if line.startswith("export "):
            k, _, v = line[len("export "):].partition("=")
            creds[k] = v.strip().strip('"').strip("'")
    assert "AWS_ACCESS_KEY_ID" in creds, f"provisioning failed: {res.stdout}\n{res.stderr}"
    return creds


def _client(port: int, creds: dict):
    return boto3.client(
        "dynamodb",
        endpoint_url=f"https://127.0.0.1:{port}",
        region_name="us-east-1",
        aws_access_key_id=creds["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=creds["AWS_SECRET_ACCESS_KEY"],
        verify=False,
    )


def _create_table(ddb, name: str) -> None:
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if ddb.describe_table(TableName=name)["Table"]["TableStatus"] == "ACTIVE":
            return
        time.sleep(0.05)
    raise AssertionError("table did not become ACTIVE")


class _Forwarder:
    """A TCP forwarder from a local port to moto, started when the test wants
    the S3 endpoint to start answering."""

    def __init__(self, listen_port: int, upstream: tuple[str, int]):
        self.listen_port = listen_port
        self.upstream = upstream
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", listen_port))
        self.sock.listen(16)
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _pump(self, a: socket.socket, b: socket.socket) -> None:
        try:
            while not self.stopping.is_set():
                data = a.recv(65536)
                if not data:
                    break
                b.sendall(data)
        except OSError:
            pass
        finally:
            for s in (a, b):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def _accept(self) -> None:
        self.sock.settimeout(0.5)
        while not self.stopping.is_set():
            try:
                client, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                upstream = socket.create_connection(self.upstream, timeout=5)
            except OSError:
                client.close()
                continue
            threading.Thread(target=self._pump, args=(client, upstream), daemon=True).start()
            threading.Thread(target=self._pump, args=(upstream, client), daemon=True).start()

    def stop(self) -> None:
        self.stopping.set()
        self.sock.close()


def _moto_or_skip() -> tuple[str, int]:
    if not S3_ENDPOINT.startswith("http://"):
        pytest.skip("EXTENDDB_TEST_S3_ENDPOINT is not an http:// moto endpoint")
    host_port = S3_ENDPOINT[len("http://"):].rstrip("/")
    host, _, port = host_port.partition(":")
    return host, int(port)


def _s3_env() -> dict:
    env = os.environ.copy()
    env.update(
        {
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_REGION": "us-east-1",
            "AWS_EC2_METADATA_DISABLED": "true",
        }
    )
    env.pop("AWS_SESSION_TOKEN", None)
    env.pop("AWS_PROFILE", None)
    return env


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture()
def moto_bucket():
    """A bucket of this test's own on the shared moto, emptied and deleted at
    teardown so the endpoint does not accumulate one per run."""
    _moto_or_skip()
    bucket = f"extenddb-wire-{uuid.uuid4().hex[:12]}"
    s3 = _s3_client()
    s3.create_bucket(Bucket=bucket)
    yield bucket
    try:
        listed = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        if listed:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in listed]})
        for upload in s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
            s3.abort_multipart_upload(Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"])
        s3.delete_bucket(Bucket=bucket)
    except ClientError:
        pass


def _assert_store_refusal(exc: ClientError, kind: str, root_fragment: str, category: str = "unreachable") -> None:
    """The refusal of a store-backed backup's restore or delete: a
    ValidationException naming the store's kind and the problem's category
    (`unreachable`, `manifest probe failed`, `first check is pending`) and
    never the root."""
    err = exc.response["Error"]
    assert err["Code"] == "ValidationException", err
    assert f"{kind} backup store" in err["Message"], err
    assert category in err["Message"], err
    assert root_fragment not in err["Message"], err


_SOURCE_TABLE_JSON = json.dumps({
    "table_name": "Music", "table_id": "t-1", "table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/Music",
    "key_schema": [{"AttributeName": "pk", "KeyType": "HASH"}],
    "attribute_definitions": [{"AttributeName": "pk", "AttributeType": "S"}],
    "billing_mode": "PAY_PER_REQUEST", "provisioned_throughput": None, "on_demand_throughput": None,
    "global_secondary_indexes": [], "local_secondary_indexes": [], "vector_indexes": None,
    "stream_specification": None, "time_to_live": None, "sse_description": None,
    "item_count": 0, "table_size_bytes": 0, "table_creation_date_time": 0.0,
})


def _sqlite_module():
    try:
        # The catalog's schema needs a SQLite newer than some hosts' stdlib
        # module links; the wheel carries a current library.
        import pysqlite3 as sqlite3
    except ImportError:
        import sqlite3
    return sqlite3


def _seed_backup_row(db_path: str, backup_id: str, status: str, store_kind: str, store_root: str, store_id: str) -> str:
    """One `backup_index` row of account 123456789012's `Music` table under
    the given store, as a server that used the store leaves it; returns the
    backup's ARN."""
    account = "123456789012"
    arn = f"arn:aws:dynamodb:us-east-1:{account}:table/Music/backup/{backup_id}"
    conn = _sqlite_module().connect(db_path, timeout=10)
    try:
        conn.execute(
            "INSERT INTO backup_index (account_id, backup_id, table_name, backup_arn, backup_name, status, "
            "store_kind, store_root, store_id, size_bytes, item_count, created_at, source_table_details_json, "
            "owner_generation, heartbeat_at) VALUES (?, ?, 'Music', ?, 'nightly', ?, ?, ?, ?, 1, 1, "
            "'2026-10-01T00:00:00.000Z', ?, 7, '2026-10-01T00:00:00.000Z')",
            (account, backup_id, arn, status, store_kind, store_root, store_id, _SOURCE_TABLE_JSON),
        )
        conn.commit()
    finally:
        conn.close()
    return arn


# --- SQLite ---------------------------------------------------------------------------


def _sqlite_build() -> bool:
    if not os.path.isfile(EXTENDDB_BINARY):
        return False
    res = subprocess.run(
        [EXTENDDB_BINARY, "version"], capture_output=True, text=True, timeout=30
    )
    return "(sqlite)" in res.stdout


class _SqliteDeployment:
    def __init__(self, tmp_dir: str):
        self.dir = tmp_dir
        self.config = os.path.join(tmp_dir, "extenddb.toml")
        self.db_path = os.path.join(tmp_dir, "extenddb.sqlite")
        self.port = _free_port()
        env = os.environ.copy()
        env["EXTENDDB_ADMIN_PASSWORD"] = ADMIN_PASSWORD
        subprocess.run(
            [EXTENDDB_BINARY, "init", "--backend", "sqlite", "--config", self.config],
            cwd=tmp_dir,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=60,
            check=True,
            env=env,
        )
        lh._patch_config_port(self.config, self.port)
        self.cert = os.path.expanduser("~/.extenddb/tls/cert.pem")

    def run(self, *args, env=None):
        return subprocess.run(
            [EXTENDDB_BINARY, args[0], "--config", self.config, *args[1:]],
            cwd=self.dir,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=180,
            env=env,
        )


@pytest.fixture()
def sqlite_deployment(tmp_path):
    if not _sqlite_build():
        pytest.skip("EXTENDDB_BINARY is not a SQLite build")
    return _SqliteDeployment(str(tmp_path))


class TestSqlite:
    def test_default_root_is_created_private_and_reported_healthy(self, sqlite_deployment, tmp_path):
        d = sqlite_deployment
        server = _Server(d.config, str(tmp_path / "serve.log"))
        try:
            health = _wait_health(d.port, lambda h: "components" in h, server=server)
            assert health["status"] == "healthy", health
            component = health["components"]["backup_store"]
            assert component == {"status": "healthy", "kind": "filesystem"}, component
            root = os.path.join(d.dir, "backups")
            assert os.path.isdir(root)
            assert (os.stat(root).st_mode & 0o777) == 0o700
            identity = os.path.join(root, "extenddb-store.json")
            with open(identity) as f:
                file = json.load(f)
            assert file["format_version"] == 1
            assert file["backend"] == "sqlite"
            assert file["store_id"] and file["catalog_id"]
            assert (os.stat(identity).st_mode & 0o777) == 0o600
            log = server.log_text()
            assert "backup store: filesystem" in log and root in log, log
            assert f"store id {file['store_id']}" in log, log
            # The console banner names the store beside the storage line.
            assert "  storage: sqlite" in log, log
            assert f"  backup store: filesystem path {root} (the default beside the database file)" in log, log
            # CreateBackup takes the in-catalog path and says the store is configured.
            creds = _provision(d.port, d.cert, ADMIN_PASSWORD)
            ddb = _client(d.port, creds)
            _create_table(ddb, "Music")
            details = ddb.create_backup(TableName="Music", BackupName="nightly")["BackupDetails"]
            assert details["BackupStatus"] in ("CREATING", "AVAILABLE")
            assert "writing the in-catalog backup format; the configured filesystem backup store" in server.log_text()
            assert "configure [backup.store]" not in server.log_text()
        finally:
            server.stop()

    def test_disabled_leaves_no_component_and_the_legacy_create_warns(self, sqlite_deployment, tmp_path):
        d = sqlite_deployment
        _append(d.config, '[backup.store]\nkind = "disabled"\n')
        server = _Server(d.config, str(tmp_path / "serve.log"))
        try:
            health = _wait_health(d.port, lambda h: "components" in h, server=server)
            assert health["status"] == "healthy"
            assert "backup_store" not in health["components"], health
            assert not os.path.exists(os.path.join(d.dir, "backups"))
            assert 'backup store: disabled' in server.log_text()
            creds = _provision(d.port, d.cert, ADMIN_PASSWORD)
            ddb = _client(d.port, creds)
            _create_table(ddb, "Music")
            ddb.create_backup(TableName="Music", BackupName="nightly")
            assert "configure [backup.store] to write backups to a backup store" in server.log_text()
        finally:
            server.stop()

    def test_default_root_under_a_group_writable_parent_warns_and_the_same_root_explicit_fails(
        self, sqlite_deployment, tmp_path
    ):
        d = sqlite_deployment
        os.chmod(d.dir, 0o775)
        server = _Server(d.config, str(tmp_path / "serve.log"))
        try:
            health = _wait_health(d.port, lambda h: "components" in h, server=server)
            assert "backup_store" not in health["components"], health
            log = server.log_text()
            # `open` applies the ancestor rule; `validate` would report the
            # same root the same way. Either is the warning path.
            assert ("is not usable" in log or "failed its validation" in log) and "unconfigured" in log, log
            assert "writable by group or others" in log, log
            assert "kind = \"disabled\" to silence this" in log, log
            creds = _provision(d.port, d.cert, ADMIN_PASSWORD)
            ddb = _client(d.port, creds)
            _create_table(ddb, "Music")
            ddb.create_backup(TableName="Music", BackupName="nightly")
            assert "configure [backup.store] to write backups to a backup store" in server.log_text()
        finally:
            server.stop()
        # The same root configured explicitly is a startup error naming it.
        root = os.path.join(d.dir, "backups")
        _append(d.config, f'[backup.store]\nkind = "filesystem"\n[backup.store.filesystem]\npath = "{root}"\n')
        server = _Server(d.config, str(tmp_path / "serve2.log"))
        try:
            assert server.wait_exit() != 0
            log = server.log_text()
            assert "cannot be opened" in log or "failed its startup validation" in log, log
            assert "writable by group or others" in log and d.dir in log, log
        finally:
            server.stop()

    def test_a_missing_explicit_root_and_a_spool_in_the_database_directory_stop_startup(
        self, sqlite_deployment, tmp_path
    ):
        d = sqlite_deployment
        missing = os.path.join(d.dir, "nowhere")
        _append(d.config, f'[backup.store]\nkind = "filesystem"\n[backup.store.filesystem]\npath = "{missing}"\n')
        server = _Server(d.config, str(tmp_path / "serve.log"))
        try:
            assert server.wait_exit() != 0
            log = server.log_text()
            assert "cannot be opened" in log and missing in log, log
        finally:
            server.stop()
        # A spool that is the database directory.
        (tmp_path / "second").mkdir()
        d2 = _SqliteDeployment(str(tmp_path / "second"))
        _append(
            d2.config,
            f'[backup]\nspool_path = "{d2.dir}"\n[backup.store]\nkind = "s3"\n[backup.store.s3]\nbucket = "b"\n',
        )
        server = _Server(d2.config, str(tmp_path / "serve2.log"))
        try:
            assert server.wait_exit() != 0
            log = server.log_text()
            assert "backup.spool_path" in log and "database directory" in log, log
        finally:
            server.stop()

    def test_an_unreachable_s3_endpoint_starts_the_server_unhealthy_and_recovers(
        self, sqlite_deployment, tmp_path, moto_bucket
    ):
        d = sqlite_deployment
        moto_host, moto_port = _moto_or_skip()
        bucket = moto_bucket
        dead_port = _free_port()
        _append(
            d.config,
            "[backup]\nreconcile_interval_secs = 1\n"
            '[backup.store]\nkind = "s3"\n[backup.store.s3]\n'
            f'bucket = "{bucket}"\nendpoint = "http://127.0.0.1:{dead_port}"\n'
            'force_path_style = true\nregion = "us-east-1"\n',
        )
        server = _Server(d.config, str(tmp_path / "serve.log"), env=_s3_env())
        forwarder = None
        try:
            # The first check runs after the listener is up: the component is
            # `checking` first, then `unhealthy` when the connection is refused.
            health = _wait_health(
                d.port, lambda h: h.get("components", {}).get("backup_store", {}).get("status") == "unhealthy",
                server=server,
            )
            assert health["status"] == "degraded", health
            component = health["components"]["backup_store"]
            assert component["status"] == "unhealthy" and component["kind"] == "s3", component
            assert component["reason"] == "unreachable", component
            assert bucket not in json.dumps(component)
            spool = os.path.join(d.dir, "backup-spool")
            assert (os.stat(spool).st_mode & 0o777) == 0o700
            assert (os.stat(os.path.join(spool, ".lock")).st_mode & 0o777) == 0o600
            creds = _provision(d.port, d.cert, ADMIN_PASSWORD)
            ddb = _client(d.port, creds)
            _create_table(ddb, "Music")
            # The in-catalog create touches no store: it runs on an unhealthy
            # one, as it does on a server with none.
            details = ddb.create_backup(TableName="Music", BackupName="nightly")["BackupDetails"]
            assert details["BackupStatus"] in ("CREATING", "AVAILABLE")
            log = server.log_text()
            assert "is unhealthy: the store could not be reached" in log, log
            assert bucket in log, "the operator's line names the bucket"

            # The endpoint starts answering: the next reconciler cycle recovers.
            forwarder = _Forwarder(dead_port, (moto_host, moto_port))
            health = _wait_health(
                d.port, lambda h: h["components"]["backup_store"]["status"] == "healthy", timeout=30, server=server
            )
            assert health["status"] == "healthy"
            assert "is healthy again" in server.log_text()
            details = ddb.create_backup(TableName="Music", BackupName="nightly2")["BackupDetails"]
            assert details["BackupStatus"] in ("CREATING", "AVAILABLE")
            # The identity file was created on the first passing check.
            body = _s3_client().get_object(Bucket=bucket, Key="extenddb-store.json")["Body"].read()
            assert json.loads(body)["backend"] == "sqlite"

            # And degrades again once the endpoint is gone.
            forwarder.stop()
            forwarder = None
            health = _wait_health(
                d.port, lambda h: h["components"]["backup_store"]["status"] == "unhealthy", timeout=30, server=server
            )
            assert health["status"] == "degraded"
            details = ddb.create_backup(TableName="Music", BackupName="again")["BackupDetails"]
            assert details["BackupStatus"] in ("CREATING", "AVAILABLE"), "the in-catalog create is not gated"
        finally:
            if forwarder is not None:
                forwarder.stop()
            server.stop()

    def test_a_stalled_s3_endpoint_holds_neither_the_listener_nor_the_reconciler(self, sqlite_deployment, tmp_path):
        """An endpoint that accepts connections and never answers. Checked:
        the server binds and serves before the store's first check, which runs
        after the listener is up (`/health` answers well within the SDK's
        attempt timeout with the component `checking`); the in-catalog
        CreateBackup is served meanwhile; the restore of a seeded store-backed
        row is refused naming the pending check; the check is abandoned at its
        30 s bound, the component turns `unhealthy` (`unreachable`) without a
        restart and the refusal names that; and the reconciler keeps cycling
        while the endpoint stalls (the next cycle's check opens a new
        connection to the tarpit within seconds of the abandoned one)."""
        d = sqlite_deployment
        tarpit = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tarpit.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tarpit.bind(("127.0.0.1", 0))
        tarpit.listen(16)
        tarpit_port = tarpit.getsockname()[1]
        held = []
        accepted_at = []

        def accept_and_hold():
            while True:
                try:
                    conn, _ = tarpit.accept()
                except OSError:
                    return
                held.append(conn)
                accepted_at.append(time.monotonic())

        threading.Thread(target=accept_and_hold, daemon=True).start()
        # A row of the store, as a server that used it leaves one; the root
        # label is the bucket and the endpoint in the spelling the store uses.
        store_root = f"s3://tarpit-bucket/?endpoint=http://127.0.0.1:{tarpit_port}"
        arn = _seed_backup_row(d.db_path, "01791976431438-00000001", "AVAILABLE", "s3", store_root, "store-seeded")
        _append(
            d.config,
            "[backup]\nreconcile_interval_secs = 1\n"
            '[backup.store]\nkind = "s3"\n[backup.store.s3]\n'
            f'bucket = "tarpit-bucket"\nendpoint = "http://127.0.0.1:{tarpit_port}"\n'
            'force_path_style = true\nregion = "us-east-1"\n',
        )
        started = time.monotonic()
        server = _Server(d.config, str(tmp_path / "serve.log"), env=_s3_env())
        try:
            health = _wait_health(d.port, lambda h: "components" in h, timeout=30, server=server)
            answered_after = time.monotonic() - started
            # The first answer is the server's own startup (TLS, the catalog
            # open), about a second here; a check awaited before the listener
            # would add the SDK's 60 s attempt timeout at the least.
            assert answered_after < 30.0, f"/health first answered after {answered_after:.1f} s"
            assert health["status"] == "healthy", health
            assert health["components"]["backup_store"] == {"status": "checking", "kind": "s3"}, health
            deadline = time.monotonic() + 20
            while not held and time.monotonic() < deadline:
                time.sleep(0.1)
            assert held, "the store's check reached the endpoint"
            log = server.log_text()
            assert "listening on" in log and "run after the listener is up" in log, log
            assert "backup store: s3" in log and "tarpit-bucket" in log, log
            # Requests are served while the check hangs: the in-catalog create
            # runs, and the seeded row's restore is refused naming the pending
            # check and the store's kind, not the bucket.
            creds = _provision(d.port, d.cert, ADMIN_PASSWORD)
            ddb = _client(d.port, creds)
            _create_table(ddb, "Music")
            details = ddb.create_backup(TableName="Music", BackupName="nightly")["BackupDetails"]
            assert details["BackupStatus"] in ("CREATING", "AVAILABLE")
            with pytest.raises(ClientError) as refused:
                ddb.restore_table_from_backup(TargetTableName="Restored", BackupArn=arn)
            _assert_store_refusal(refused.value, "s3", "tarpit-bucket", category="first check is pending")
            assert "Restored" not in [t for t in ddb.list_tables()["TableNames"]]

            # The check is abandoned at its bound (30 s, under the SDK's 60 s
            # attempt timeout): the component turns unhealthy without a
            # restart, the log says the check was abandoned and the cycle's
            # duties ran.
            health = _wait_health(
                d.port, lambda h: h["components"]["backup_store"]["status"] == "unhealthy", timeout=45, server=server
            )
            assert health["status"] == "degraded", health
            assert health["components"]["backup_store"]["reason"] == "unreachable", health
            log = server.log_text()
            assert "check did not finish within 30 s and was abandoned" in log, log
            assert "is unhealthy: the store could not be reached: the check did not finish within 30 s" in log, log
            with pytest.raises(ClientError) as refused:
                ddb.restore_table_from_backup(TargetTableName="Restored", BackupArn=arn)
            _assert_store_refusal(refused.value, "s3", "tarpit-bucket", category="unreachable")
            # The reconciler keeps cycling: the next cycle's check opens a new
            # connection to the tarpit within seconds of the abandoned one
            # (the SDK's own retry would come 60 s after the first).
            first = accepted_at[0]
            deadline = time.monotonic() + 15
            while len(accepted_at) < 2 and time.monotonic() < deadline:
                time.sleep(0.2)
            assert len(accepted_at) >= 2, "the next cycle's check reached the endpoint"
            assert accepted_at[1] - first < 45.0, f"the second connection came {accepted_at[1] - first:.1f} s after the first"
            assert "is unhealthy" in log and log.count("is unhealthy: the store could not be reached") == 1, \
                "the transition is logged once"
            details = ddb.create_backup(TableName="Music", BackupName="nightly2")["BackupDetails"]
            assert details["BackupStatus"] in ("CREATING", "AVAILABLE")
        finally:
            server.stop()
            tarpit.close()
            for conn in held:
                conn.close()
        # Shutdown did not wait on the stalled check.
        with open(server.log_path, errors="replace") as f:
            final_log = f.read()
        assert "did not drain" not in final_log, final_log

    def test_a_default_root_with_rows_is_unhealthy_rather_than_unconfigured(self, sqlite_deployment, tmp_path):
        """The default root's leniency ends with the catalog's first row for
        it: a row seeded under the default root with its manifest missing
        starts the server with the store configured and unhealthy (`/health`
        degraded, `manifest probe failed`), the row's restore refused naming
        the kind and the category and not the root, recovery when the manifest
        is put back, and `backup purge --confirm` and `backup list` reaching
        the store (the purge refuses on the unhealthy store, not for want of
        one)."""
        d = sqlite_deployment
        _append(d.config, "[backup]\nreconcile_interval_secs = 1\n")
        server = _Server(d.config, str(tmp_path / "serve.log"))
        try:
            _wait_health(d.port, lambda h: "backup_store" in h.get("components", {}), server=server)
        finally:
            server.stop()
        root = os.path.realpath(os.path.join(d.dir, "backups"))
        with open(os.path.join(root, "extenddb-store.json")) as f:
            store_id = json.load(f)["store_id"]
        backup_id = "01791976431438-00000001"
        arn = _seed_backup_row(d.db_path, backup_id, "AVAILABLE", "filesystem", root, store_id)
        manifest_dir = os.path.join(root, "123456789012", "Music", backup_id)
        manifest = os.path.join(manifest_dir, "extenddb-manifest.json")

        server = _Server(d.config, str(tmp_path / "serve2.log"))
        try:
            health = _wait_health(d.port, lambda h: "backup_store" in h.get("components", {}), server=server)
            assert health["status"] == "degraded", health
            assert health["components"]["backup_store"] == {
                "status": "unhealthy", "kind": "filesystem", "reason": "manifest probe failed",
            }, health
            log = server.log_text()
            assert "is unhealthy: the manifest of " + arn + " is not under the root" in log, log
            assert "backup rows for it, so it is handled as a configured root" in log, log
            assert "unconfigured" not in log, log
            creds = _provision(d.port, d.cert, ADMIN_PASSWORD)
            ddb = _client(d.port, creds)
            with pytest.raises(ClientError) as refused:
                ddb.restore_table_from_backup(TargetTableName="Restored", BackupArn=arn)
            _assert_store_refusal(refused.value, "filesystem", root, category="manifest probe failed")
            with pytest.raises(ClientError) as refused:
                ddb.delete_backup(BackupArn=arn)
            _assert_store_refusal(refused.value, "filesystem", root, category="manifest probe failed")
            # The in-catalog create is served on the unhealthy store.
            _create_table(ddb, "Music")
            details = ddb.create_backup(TableName="Music", BackupName="nightly")["BackupDetails"]
            assert details["BackupStatus"] in ("CREATING", "AVAILABLE")
            # The manifest put back: the next cycle recovers the store.
            for level in (os.path.dirname(os.path.dirname(manifest_dir)), os.path.dirname(manifest_dir), manifest_dir):
                if not os.path.isdir(level):
                    os.mkdir(level, 0o700)
            with open(manifest, "wb") as f:
                f.write(b"{}")
            health = _wait_health(
                d.port, lambda h: h["components"]["backup_store"]["status"] == "healthy", timeout=30, server=server
            )
            assert health["status"] == "healthy", health
            assert "is healthy again" in server.log_text()
        finally:
            server.stop()

        # The commands reach the store: with the manifest gone again, the
        # listing (a read of the file and the rows, no probe) shows the row
        # under the store, and the purge, which runs the check, refuses on
        # the unhealthy store and deletes nothing.
        os.remove(manifest)
        res = d.run("backup", "list")
        assert res.returncode == 0, res.stdout + res.stderr
        assert backup_id in res.stdout and f"backups in the filesystem store {root}" in res.stdout, res.stdout
        res = d.run("backup", "purge", "--confirm")
        out = res.stdout + res.stderr
        assert res.returncode != 0, out
        assert "is unhealthy" in out and "manifest" in out, out
        assert "no backup store is configured" not in out, out
        res = d.run("backup", "list")
        assert backup_id in res.stdout, "nothing was deleted: " + res.stdout

    def test_a_relative_root_resolves_against_the_shell_in_daemon_mode(self, sqlite_deployment, tmp_path):
        """A relative `[backup.store.filesystem] path` is made absolute at
        config load, against the shell's directory, so the daemonized child
        (whose working directory is `/`) opens the same root the operator
        named: the root beside the config gets the identity file and nothing
        appears under `/`."""
        d = sqlite_deployment
        os.mkdir(os.path.join(d.dir, "rel-backups"), 0o700)
        run_dir = os.path.join(d.dir, "run")
        with open(d.config) as f:
            text = f.read()
        import re
        text, n = re.subn(r'(?m)^[ \t]*#?[ \t]*run_dir[ \t]*=.*$', f'run_dir = "{run_dir}"', text, count=1)
        assert n == 1, text
        with open(d.config, "w") as f:
            f.write(text)
        _append(d.config, '[backup.store]\nkind = "filesystem"\n[backup.store.filesystem]\npath = "rel-backups"\n')
        res = subprocess.run(
            [EXTENDDB_BINARY, "serve", "--config", d.config], cwd=d.dir, capture_output=True, text=True,
            stdin=subprocess.DEVNULL, timeout=60,
        )
        try:
            assert res.returncode == 0, res.stdout + res.stderr
            assert f"backup store: filesystem path {os.path.join(d.dir, 'rel-backups')}" in res.stdout, res.stdout
            _wait_health(d.port, lambda h: h.get("components", {}).get("backup_store", {}).get("status") == "healthy")
            assert os.path.isfile(os.path.join(d.dir, "rel-backups", "extenddb-store.json"))
            assert not os.path.exists("/rel-backups")
        finally:
            # Stop the daemon whatever the outcome above; a daemon that never
            # started has no PID file and `stop` says so.
            subprocess.run(
                [EXTENDDB_BINARY, "stop", "--config", d.config], cwd=d.dir, capture_output=True, text=True,
                stdin=subprocess.DEVNULL, timeout=60,
            )
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                try:
                    _health(d.port)
                except Exception:  # noqa: BLE001 - the listener is gone
                    break
                time.sleep(0.25)

    def test_a_catalog_re_created_after_destroy_warns_about_the_old_default_root(self, sqlite_deployment, tmp_path):
        """`destroy` then `init --overwrite` on the same directory: the default
        root's identity file names the old catalog. The server starts with the
        store unconfigured and a warning naming the file (not degraded, not a
        startup error), CreateBackup keeps the in-catalog path, and `destroy`'s
        output said to remove the file."""
        d = sqlite_deployment
        server = _Server(d.config, str(tmp_path / "serve.log"))
        try:
            _wait_health(d.port, lambda h: "backup_store" in h.get("components", {}), server=server)
        finally:
            server.stop()
        identity = os.path.join(d.dir, "backups", "extenddb-store.json")
        with open(identity) as f:
            old_catalog = json.load(f)["catalog_id"]
        res = d.run("destroy", "--yes")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "needs that file removed" in res.stdout, res.stdout
        assert os.path.isfile(identity)
        env = os.environ.copy()
        env["EXTENDDB_ADMIN_PASSWORD"] = ADMIN_PASSWORD
        subprocess.run(
            [EXTENDDB_BINARY, "init", "--backend", "sqlite", "--config", d.config, "--overwrite"],
            cwd=d.dir, capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=60, check=True, env=env,
        )
        lh._patch_config_port(d.config, d.port)
        server = _Server(d.config, str(tmp_path / "serve2.log"))
        try:
            health = _wait_health(d.port, lambda h: "components" in h, server=server)
            assert health == {"status": "healthy", "components": {}}, health
            log = server.log_text()
            assert "failed its check" in log and "extenddb-store.json names catalog" in log, log
            assert old_catalog in log and "destroy" in log and "unconfigured" in log, log
            creds = _provision(d.port, d.cert, ADMIN_PASSWORD)
            ddb = _client(d.port, creds)
            _create_table(ddb, "Music")
            details = ddb.create_backup(TableName="Music", BackupName="nightly")["BackupDetails"]
            assert details["BackupStatus"] in ("CREATING", "AVAILABLE")
            assert "configure [backup.store] to write backups to a backup store" in server.log_text()
        finally:
            server.stop()

    def test_backup_purge_deletes_each_rows_prefix_and_row_and_leaves_the_rest(self, sqlite_deployment, tmp_path):
        """Rows of every status seeded in the catalog with objects under their
        prefixes: purge deletes each row's objects and the row outright; a
        prefix the catalog has no row for, the identity file and `exports/`
        stay; `backup list` shows the rows before and none after.
        """
        try:
            # The catalog's schema needs a SQLite newer than some hosts'
            # stdlib module links; the wheel carries a current library.
            import pysqlite3 as sqlite3
        except ImportError:
            import sqlite3

        d = sqlite_deployment
        # The shortest window the config accepts (three of the 15 s the jobs
        # heartbeat at), so a purge that waited on a dead job would fail
        # this test in 90 s rather than the default 600.
        _append(d.config, "[backup]\nstale_job_after_secs = 45\n")
        server = _Server(d.config, str(tmp_path / "serve.log"))
        try:
            _wait_health(d.port, lambda h: "backup_store" in h.get("components", {}), server=server)
        finally:
            server.stop()
        root = os.path.join(d.dir, "backups")
        with open(os.path.join(root, "extenddb-store.json")) as f:
            store_id = json.load(f)["store_id"]
        source_table = json.dumps({
            "table_name": "Music", "table_id": "t-1", "table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/Music",
            "key_schema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "attribute_definitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "billing_mode": "PAY_PER_REQUEST", "provisioned_throughput": None, "on_demand_throughput": None,
            "global_secondary_indexes": [], "local_secondary_indexes": [], "vector_indexes": None,
            "stream_specification": None, "time_to_live": None, "sse_description": None,
            "item_count": 0, "table_size_bytes": 0, "table_creation_date_time": 0.0,
        })
        account = "123456789012"
        rows = [
            ("01791976431438-00000001", "AVAILABLE"),
            ("01791976431439-00000002", "FAILED"),
            ("01791976431440-00000003", "DELETING"),
            # A CREATING row whose owner is dead (its heartbeat is old).
            ("01791976431441-00000004", "CREATING"),
            # A FAILED and a CREATING row whose heartbeats are young (five
            # seconds old): the stopped server's jobs wrote them, and under
            # the serve lock those jobs are dead, so the command neither
            # waits on them nor leaves them.
            ("01791976431442-00000005", "FAILED"),
            ("01791976431443-00000006", "CREATING"),
        ]
        young = {"01791976431442-00000005", "01791976431443-00000006"}

        def prefix(backup_id):
            return os.path.join(root, account, "Music", backup_id)

        def private_dirs(path):
            # Every level 0700: the store refuses a directory under its root
            # that another user can reach (`makedirs` leaves the intermediate
            # levels at the umask's mode).
            parts = []
            while not os.path.isdir(path):
                parts.append(path)
                path = os.path.dirname(path)
            for p in reversed(parts):
                os.mkdir(p, 0o700)

        conn = sqlite3.connect(d.db_path, timeout=10)
        try:
            for backup_id, status in rows:
                arn = f"arn:aws:dynamodb:us-east-1:{account}:table/Music/backup/{backup_id}"
                heartbeat = (
                    "strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-5 seconds')" if backup_id in young
                    else "'2026-10-01T00:00:00.000Z'"
                )
                conn.execute(
                    "INSERT INTO backup_index (account_id, backup_id, table_name, backup_arn, backup_name, status, "
                    "store_kind, store_root, store_id, size_bytes, item_count, created_at, source_table_details_json, "
                    "owner_generation, heartbeat_at) VALUES (?, ?, 'Music', ?, 'nightly', ?, 'filesystem', ?, ?, 1, 1, "
                    f"'2026-10-01T00:00:00.000Z', ?, 7, {heartbeat})",
                    (account, backup_id, arn, status, root, store_id, source_table),
                )
            # A restore the stopped server left RUNNING (its target CREATING):
            # under the serve lock its owner is dead, and the command resolves
            # it before the wait instead of waiting on it.
            conn.execute(
                "INSERT INTO accounts (account_id, account_name) SELECT ?, 'purge-test' WHERE NOT EXISTS "
                "(SELECT 1 FROM accounts WHERE account_id = ?)",
                (account, account),
            )
            conn.execute(
                "INSERT INTO tables (account_id, table_name, key_schema, attribute_definitions, table_status, "
                "table_arn, table_id) VALUES (?, 'Restored', '[]', '[]', 'CREATING', "
                "'arn:aws:dynamodb:us-east-1:123456789012:table/Restored', 'tid-restored')",
                (account,),
            )
            conn.execute(
                "INSERT INTO table_restores (table_id, source_backup_arn, restore_date_time, status, kind, "
                "owner_generation, heartbeat_at) VALUES ('tid-restored', "
                "'arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01791976431400-00000000', "
                "'2026-10-01T00:00:00.000Z', 'RUNNING', 'legacy', 9, '2026-10-01T00:00:00.000Z')",
            )
            conn.commit()
        finally:
            conn.close()
        for backup_id, status in rows:
            private_dirs(os.path.join(prefix(backup_id), "data"))
            # A CREATING backup has its data files and no manifest yet (the
            # writer puts the manifest last), so nothing adopts a prefix a
            # failure between purge's two steps leaves.
            names = ["data/00001.json.gz"] if status == "CREATING" else ["extenddb-manifest.json", "data/00001.json.gz"]
            for name in names:
                with open(os.path.join(prefix(backup_id), name), "wb") as f:
                    f.write(b"x")
        # A prefix with no row (another catalog's, or a copy) and exports/.
        orphan = prefix("01791976431499-0000beef")
        private_dirs(orphan)
        with open(os.path.join(orphan, "extenddb-manifest.json"), "wb") as f:
            f.write(b"x")
        private_dirs(os.path.join(root, "exports", "e-1"))
        with open(os.path.join(root, "exports", "e-1", "manifest-summary.json"), "wb") as f:
            f.write(b"x")

        res = d.run("backup", "list")
        assert res.returncode == 0, res.stdout + res.stderr
        for backup_id, status in rows:
            assert backup_id in res.stdout and status in res.stdout, res.stdout
        assert f"store_root={root}" in res.stdout and f"store_id={store_id}" in res.stdout, res.stdout
        assert "6 row(s)" in res.stdout, res.stdout
        res = d.run("backup", "list", "--failed")
        assert res.returncode == 0 and "2 row(s)" in res.stdout and "FAILED" in res.stdout, res.stdout

        # The dry run prints the plan, changes nothing and exits 0.
        res = d.run("backup", "purge")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "Dry run: nothing was deleted" in res.stdout and "--confirm" in res.stdout, res.stdout
        for backup_id, _ in rows:
            assert os.path.isfile(os.path.join(prefix(backup_id), "data", "00001.json.gz")), "nothing deleted without --confirm"
        # `--config` after the subcommand is accepted too.
        res = subprocess.run(
            [EXTENDDB_BINARY, "backup", "purge", "--config", d.config], cwd=d.dir, capture_output=True, text=True,
            stdin=subprocess.DEVNULL, timeout=120,
        )
        assert res.returncode == 0 and "Dry run" in res.stdout, res.stdout + res.stderr

        res = d.run("backup", "purge", "--confirm")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "6 backup(s) deleted, 0 left" in res.stdout, res.stdout
        # The interrupted restore was resolved before the wait, which then
        # had nothing to wait for (the default window would be 600 s); the
        # young FAILED and CREATING rows were neither waited on (the window
        # is zero under the serve lock) nor left.
        assert "removed the restore target Restored" in res.stdout, res.stdout
        assert "Waiting for running jobs" not in res.stdout, "purge waited on a dead job: " + res.stdout
        for backup_id, _ in rows:
            assert not os.path.exists(prefix(backup_id)), f"{backup_id}'s prefix is gone"
            assert f"deleted arn:aws:dynamodb:us-east-1:{account}:table/Music/backup/{backup_id}" in res.stdout
        assert os.path.isfile(os.path.join(orphan, "extenddb-manifest.json")), "a prefix with no row stays"
        assert os.path.isfile(os.path.join(root, "exports", "e-1", "manifest-summary.json")), "exports/ stays"
        assert os.path.isfile(os.path.join(root, "extenddb-store.json")), "the identity file stays"
        out = res.stdout + res.stderr
        assert "extenddb::audit" in out and out.count("deleted backup objects and catalog row") == 6, out
        conn = sqlite3.connect(d.db_path, timeout=10)
        try:
            assert conn.execute("SELECT count(*) FROM backup_index").fetchone()[0] == 0, "rows gone, no tombstones"
            assert conn.execute("SELECT count(*) FROM backup_maintenance").fetchone()[0] == 0, "the claim is released"
            assert conn.execute("SELECT count(*) FROM tables WHERE table_name = 'Restored'").fetchone()[0] == 0, \
                "the dead restore's target is gone"
        finally:
            conn.close()
        res = d.run("backup", "list")
        assert res.returncode == 0 and "(none)" in res.stdout, res.stdout

        # The wait's limit and its refusal: purge waits the window twice,
        # refuses naming the row, releases the claim and deletes nothing.
        # The lever is a FAILED row whose heartbeat is stamped 120 s ahead
        # of the clock: under the zero window the serve lock gives (every
        # owner is dead, as the first half of this test showed on a 5 s old
        # heartbeat) only a heartbeat in the future still reads as live, and
        # no stopped server on this host writes one. The rule the wait
        # applies on the other backends, a FAILED row with a heartbeat
        # within the window held while a job that has not yet seen its
        # claim lost may still write under its prefix, is tested on
        # PostgreSQL in `crates/storage-postgres/tests/backup_restore.rs`
        # (`the_wait_passes_a_failed_record_and_holds_on_a_failed_row_with_a_live_heartbeat`).
        live_failed = "01791976431450-00000005"
        conn = sqlite3.connect(d.db_path, timeout=10)
        try:
            conn.execute(
                "INSERT INTO backup_index (account_id, backup_id, table_name, backup_arn, backup_name, status, "
                "store_kind, store_root, store_id, size_bytes, item_count, created_at, source_table_details_json, "
                "owner_generation, heartbeat_at) VALUES (?, ?, 'Music', ?, 'nightly', 'FAILED', 'filesystem', ?, ?, 1, 1, "
                "'2026-10-01T00:00:00.000Z', ?, 7, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+120 seconds'))",
                (account, live_failed, f"arn:aws:dynamodb:us-east-1:{account}:table/Music/backup/{live_failed}", root,
                 store_id, source_table),
            )
            conn.commit()
        finally:
            conn.close()
        private_dirs(prefix(live_failed))
        with open(os.path.join(prefix(live_failed), "extenddb-manifest.json"), "wb") as f:
            f.write(b"x")
        started = time.monotonic()
        res = d.run("backup", "purge", "--confirm")
        waited = time.monotonic() - started
        assert res.returncode != 0, res.stdout + res.stderr
        out = res.stdout + res.stderr
        assert "Waiting for running jobs to finish: the FAILED backup" in out and live_failed in out, out
        assert "jobs were still running after waiting" in out and "(2 * stale_job_after_secs)" in out, out
        assert waited >= 90, f"the wait is twice the window: {waited:.1f} s"
        assert os.path.isfile(os.path.join(prefix(live_failed), "extenddb-manifest.json")), "nothing deleted"
        conn = sqlite3.connect(d.db_path, timeout=10)
        try:
            assert conn.execute("SELECT count(*) FROM backup_index").fetchone()[0] == 1
            assert conn.execute("SELECT count(*) FROM backup_maintenance").fetchone()[0] == 0, "the claim is released"
            conn.execute("DELETE FROM backup_index")
            conn.commit()
        finally:
            conn.close()
        import shutil
        shutil.rmtree(prefix(live_failed))

        # A signal while the command waits releases the claim at once and
        # deletes nothing; a SIGKILL leaves the claim's row, which the next
        # server start clears, so CreateBackup is not refused for the
        # window. The waited-on row's heartbeat is 300 s ahead (the zero
        # window under the serve lock cannot pass it).
        waited_on = "01791976431460-00000007"
        conn = sqlite3.connect(d.db_path, timeout=10)
        try:
            conn.execute(
                "INSERT INTO backup_index (account_id, backup_id, table_name, backup_arn, backup_name, status, "
                "store_kind, store_root, store_id, size_bytes, item_count, created_at, source_table_details_json, "
                "owner_generation, heartbeat_at) VALUES (?, ?, 'Music', ?, 'nightly', 'FAILED', 'filesystem', ?, ?, 1, 1, "
                "'2026-10-01T00:00:00.000Z', ?, 7, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+300 seconds'))",
                (account, waited_on, f"arn:aws:dynamodb:us-east-1:{account}:table/Music/backup/{waited_on}", root,
                 store_id, source_table),
            )
            conn.commit()
        finally:
            conn.close()
        private_dirs(prefix(waited_on))
        with open(os.path.join(prefix(waited_on), "extenddb-manifest.json"), "wb") as f:
            f.write(b"x")

        def waiting_purge():
            proc = subprocess.Popen(
                [EXTENDDB_BINARY, "backup", "--config", d.config, "purge", "--confirm"], cwd=d.dir,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, text=True,
            )
            lines = []
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                line = proc.stdout.readline()
                if not line:
                    break
                lines.append(line)
                if "Waiting for running jobs to finish" in line:
                    return proc, lines
            proc.kill()
            raise AssertionError("purge did not reach its wait: " + "".join(lines))

        def claim_rows():
            conn = sqlite3.connect(d.db_path, timeout=10)
            try:
                return conn.execute("SELECT count(*) FROM backup_maintenance").fetchone()[0]
            finally:
                conn.close()

        proc, lines = waiting_purge()
        assert claim_rows() == 1, "the claim is held during the wait"
        signalled = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        rest = proc.communicate(timeout=30)[0]
        out = "".join(lines) + rest
        released_after = time.monotonic() - signalled
        assert proc.returncode != 0, out
        assert "SIGTERM received" in out and "stopped before any deletion: SIGTERM received while waiting" in out, out
        assert "The maintenance claim is released" in out, out
        assert released_after < 5, f"the signal ended the wait at once, not at the limit: {released_after:.1f} s"
        assert claim_rows() == 0, "the claim is released on SIGTERM"
        assert os.path.isfile(os.path.join(prefix(waited_on), "extenddb-manifest.json")), "nothing deleted"

        proc, lines = waiting_purge()
        proc.kill()
        proc.communicate(timeout=30)
        assert claim_rows() == 1, "a killed purge leaves its claim's row"
        # The server starts on the file and clears the row; the first
        # CreateBackup is accepted at once (it would be LimitExceeded for
        # the 45 s window otherwise).
        server = _Server(d.config, str(tmp_path / "serve-after-kill.log"))
        try:
            _wait_health(d.port, lambda h: "backup_store" in h.get("components", {}), server=server)
            assert claim_rows() == 0, "the server start cleared the left-behind claim"
            assert "maintenance claim held by extenddb backup purge" in server.log_text() \
                and "was left behind; cleared" in server.log_text(), server.log_text()
            creds = _provision(d.port, d.cert, ADMIN_PASSWORD)
            ddb = _client(d.port, creds)
            _create_table(ddb, "Music")
            details = ddb.create_backup(TableName="Music", BackupName="after-kill")["BackupDetails"]
            assert details["BackupStatus"] in ("CREATING", "AVAILABLE"), details
            assert "LimitExceededException" not in server.log_text(), server.log_text()
        finally:
            server.stop()
        conn = sqlite3.connect(d.db_path, timeout=10)
        try:
            conn.execute("DELETE FROM backup_index")
            conn.commit()
        finally:
            conn.close()
        shutil.rmtree(prefix(waited_on))

        # The server starts again against the purged root, healthy.
        server = _Server(d.config, str(tmp_path / "serve2.log"))
        try:
            health = _wait_health(d.port, lambda h: "backup_store" in h.get("components", {}), server=server)
            assert health["components"]["backup_store"]["status"] == "healthy", health
        finally:
            server.stop()

    def test_backup_list_and_purge_on_an_empty_store_and_the_serve_lock(self, sqlite_deployment, tmp_path):
        d = sqlite_deployment
        # Create the default root with a server once, then stop it.
        # Before any server: `list` is a read that creates neither the default
        # root nor its identity file, and says the root does not exist yet.
        res = d.run("backup", "list")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "does not exist yet" in res.stdout and "(none)" in res.stdout, res.stdout
        assert not os.path.exists(os.path.join(d.dir, "backups")), "a read creates nothing"
        # The dry run is the same read: it prints an empty plan naming the
        # reason and exits 0.
        res = d.run("backup", "purge")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "Store:  none (the default root" in res.stdout and "does not exist yet" in res.stdout, res.stdout
        assert "(none)" in res.stdout and "Dry run: nothing was deleted" in res.stdout, res.stdout
        assert not os.path.exists(os.path.join(d.dir, "backups")), "a read creates nothing"
        # The root created by hand, empty: `list` and the dry run read it and
        # leave it without an identity file.
        os.mkdir(os.path.join(d.dir, "backups"), 0o700)
        res = d.run("backup", "list")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "has no extenddb-store.json yet" in res.stdout and "(none)" in res.stdout, res.stdout
        res = d.run("backup", "purge")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "Dry run: nothing was deleted" in res.stdout and "no extenddb-store.json yet" in res.stdout, res.stdout
        assert not os.path.exists(os.path.join(d.dir, "backups", "extenddb-store.json")), "a read creates nothing"
        server = _Server(d.config, str(tmp_path / "serve.log"))
        try:
            _wait_health(d.port, lambda h: "backup_store" in h.get("components", {}), server=server)
            # While the server runs, `list` and the dry run are reads beside
            # it (no serve lock); `purge --confirm` opens the catalog as a
            # server does and refuses.
            res = d.run("backup", "list")
            assert res.returncode == 0, res.stdout + res.stderr
            assert "backups in the filesystem store" in res.stdout and "(none)" in res.stdout, res.stdout
            res = d.run("backup", "purge")
            assert res.returncode == 0, res.stdout + res.stderr
            assert "Dry run: nothing was deleted" in res.stdout, res.stdout
            res = d.run("backup", "purge", "--confirm")
            assert res.returncode != 0, res.stdout + res.stderr
            assert "another extenddb process" in (res.stdout + res.stderr), res.stdout + res.stderr
            assert server.proc.poll() is None, "the server kept running"
        finally:
            server.stop()
        res = d.run("backup", "list")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "backups in the filesystem store" in res.stdout and "(none)" in res.stdout, res.stdout
        res = d.run("backup", "list", "--failed")
        assert res.returncode == 0 and "(none)" in res.stdout, res.stdout + res.stderr
        res = d.run("backup", "purge")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "Dry run: nothing was deleted" in res.stdout, res.stdout
        res = d.run("backup", "purge", "--confirm")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "0 backup(s) deleted" in res.stdout, res.stdout
        assert "Took the maintenance claim" in res.stdout, res.stdout
        assert os.path.isfile(os.path.join(d.dir, "backups", "extenddb-store.json"))
        # `destroy` names the store as left untouched.
        res = d.run("destroy", "--yes")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "backup store ([backup.store]) is left untouched" in res.stdout, res.stdout
        assert os.path.isfile(os.path.join(d.dir, "backups", "extenddb-store.json"))


# --- PostgreSQL -----------------------------------------------------------------------


def _pg_build() -> bool:
    if not os.path.isfile(EXTENDDB_BINARY):
        return False
    res = subprocess.run([EXTENDDB_BINARY, "version"], capture_output=True, text=True, timeout=30)
    return "(postgres)" in res.stdout


@pytest.fixture()
def pg_env(request):
    if not _pg_build():
        pytest.skip("EXTENDDB_BINARY is not a PostgreSQL build")
    # `cli_env` requires PostgreSQL and a binary; it is requested only once
    # the build is known to be the PostgreSQL one, so the other build skips.
    cli_env = request.getfixturevalue("cli_env")
    result = lh._run_extenddb(
        "init", *lh._init_args(cli_env), config=cli_env["config_path"],
        env_override={"EXTENDDB_ADMIN_PASSWORD": ADMIN_PASSWORD},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    lh._patch_config_port(cli_env["config_path"], cli_env["port"])
    return cli_env


class TestPostgres:
    def test_without_a_store_there_is_no_component_and_create_backup_warns(self, pg_env, tmp_path):
        server = _Server(pg_env["config_path"], str(tmp_path / "serve.log"))
        try:
            health = _wait_health(pg_env["port"], lambda h: "components" in h, server=server)
            assert health == {"status": "healthy", "components": {}}, health
            log = server.log_text()
            assert "backup store: none configured" in log, log
            creds = _provision(pg_env["port"], os.path.join(pg_env["tls_dir"], "cert.pem"), ADMIN_PASSWORD)
            ddb = _client(pg_env["port"], creds)
            _create_table(ddb, "Music")
            arn = ddb.create_backup(TableName="Music", BackupName="nightly")["BackupDetails"]["BackupArn"]
            assert "configure [backup.store] to write backups to a backup store" in server.log_text()
            # The in-catalog backup is served.
            assert ddb.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]["BackupArn"] == arn
            listed = ddb.list_backups(TableName="Music")["BackupSummaries"]
            assert [b["BackupArn"] for b in listed] == [arn]
        finally:
            server.stop()

    def test_an_s3_store_without_a_spool_stops_startup(self, pg_env, tmp_path):
        _append(pg_env["config_path"], '[backup.store]\nkind = "s3"\n[backup.store.s3]\nbucket = "b"\n')
        server = _Server(pg_env["config_path"], str(tmp_path / "serve.log"), env=_s3_env())
        try:
            assert server.wait_exit() != 0
            log = server.log_text()
            assert "backup.spool_path is required" in log, log
        finally:
            server.stop()

    def test_a_filesystem_store_is_healthy_and_a_bad_bucket_name_stops_startup(self, pg_env, tmp_path):
        root = tmp_path / "backups"
        root.mkdir(mode=0o700)
        _append(
            pg_env["config_path"],
            f'[backup.store]\nkind = "filesystem"\n[backup.store.filesystem]\npath = "{root}"\n',
        )
        server = _Server(pg_env["config_path"], str(tmp_path / "serve.log"))
        try:
            health = _wait_health(pg_env["port"], lambda h: "components" in h, server=server)
            assert health["components"]["backup_store"] == {"status": "healthy", "kind": "filesystem"}
            with open(root / "extenddb-store.json") as f:
                assert json.load(f)["backend"] == "postgres"
        finally:
            server.stop()
        # A bucket name the SDK routes to S3 Express is a configuration error
        # naming the bucket.
        with open(pg_env["config_path"]) as f:
            text = f.read()
        text = text[: text.index("[backup.store]")]
        with open(pg_env["config_path"], "w") as f:
            f.write(text)
        _append(
            pg_env["config_path"],
            f'[backup]\nspool_path = "{tmp_path / "spool"}"\n'
            '[backup.store]\nkind = "s3"\n[backup.store.s3]\nbucket = "my-bucket--usw2-az1--x-s3"\n',
        )
        server = _Server(pg_env["config_path"], str(tmp_path / "serve2.log"), env=_s3_env())
        try:
            assert server.wait_exit() != 0
            log = server.log_text()
            assert "cannot be opened" in log and "my-bucket--usw2-az1--x-s3" in log, log
        finally:
            server.stop()
