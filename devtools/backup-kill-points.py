#!/usr/bin/env python3
"""The API half of kill-points.sh and bench-rss.sh: load a table, start a
backup, poll it, read the store.

Usage (every call reads EXTENDDB_TEST_ENDPOINT and the AWS_* credentials
provision-test-credentials exported):
  kill-points.py create-table <name> [--items N] [--pad BYTES] [--gsis N] [--threads N]
  kill-points.py create-backup <name> <backup-name>      -> prints the ARN
  kill-points.py status <arn>                            -> CREATING|AVAILABLE|NOTFOUND
  kill-points.py wait-available <arn> [--timeout S]      -> prints size,count,duration
  kill-points.py describe <arn>                          -> JSON
  kill-points.py put-worst <name> [--count N] [--sets N] [--small N] [--interleave N]
                                                         -> the worst-expansion items after N small ones, or at every Nth key among them
  kill-points.py put-random <name> [--count N] [--bytes N]
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import urllib3
from botocore.config import Config
from botocore.exceptions import ClientError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ENDPOINT = os.environ.get("EXTENDDB_TEST_ENDPOINT", "")


def client():
    return boto3.client(
        "dynamodb",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        verify=False,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}, read_timeout=120),
    )


def wait_active(c, name: str) -> None:
    while c.describe_table(TableName=name)["Table"]["TableStatus"] != "ACTIVE":
        time.sleep(0.2)
    # Every GSI ACTIVE too.
    while any(
        g.get("IndexStatus") != "ACTIVE"
        for g in c.describe_table(TableName=name)["Table"].get("GlobalSecondaryIndexes", [])
    ):
        time.sleep(0.2)


def create_table(args) -> None:
    c = client()
    attrs = [
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "sk", "AttributeType": "N"},
    ]
    gsis = []
    for i in range(args.gsis):
        attrs.append({"AttributeName": f"g{i}", "AttributeType": "S"})
        gsis.append(
            {
                "IndexName": f"by-g{i}",
                "KeySchema": [
                    {"AttributeName": f"g{i}", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        )
    kwargs = {"GlobalSecondaryIndexes": gsis} if gsis else {}
    c.create_table(
        TableName=args.name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=attrs,
        BillingMode="PAY_PER_REQUEST",
        **kwargs,
    )
    wait_active(c, args.name)
    n = args.items
    if n == 0:
        return
    local = threading.local()
    started = time.monotonic()

    def write(start: int) -> None:
        if not hasattr(local, "c"):
            local.c = client()
        reqs = []
        for i in range(start, min(start + 25, n)):
            item = {
                "pk": {"S": f"p{i % 1000:05d}"},
                "sk": {"N": str(i)},
                "pad": {"S": "x" * args.pad},
            }
            for g in range(args.gsis):
                item[f"g{g}"] = {"S": f"v{(i // (g + 1)) % 97}"}
            reqs.append({"PutRequest": {"Item": item}})
        pending = {args.name: reqs}
        while pending:
            pending = local.c.batch_write_item(RequestItems=pending).get("UnprocessedItems") or {}
            if pending:
                time.sleep(0.05)

    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        list(pool.map(write, range(0, n, 25)))
    wait_active(c, args.name)
    print(f"loaded {n} items in {time.monotonic() - started:.1f} s", file=sys.stderr)


def put_worst(args) -> None:
    """#394's worst-expansion lines (`data_file.rs`'s line-cap table): an
    item of 2,262 `NS` sets of the 90 values `1.0E-130`..`9.9E-130` is
    409,429 bytes as the service sizes items and 27.7 MB as a data-file line
    (and as stored item text, the server normalizing each number to its
    133-character decimal form), placed after `--small` small rows (their
    `a...` keys sort before `zz...`) or, with `--interleave N`, at every Nth
    key among them (the shape of a table whose large items are a minority:
    key order does not sort items by size)."""
    c = client()
    every = args.interleave
    big_positions = set([i for i in range(args.small) if every and i % every == every - 1][: args.count])
    small = [
        {"PutRequest": {"Item": {"pk": {"S": f"a{i:04d}"}, "sk": {"N": str(i)}, "pad": {"S": "x" * 50}}}}
        for i in range(args.small) if i not in big_positions
    ]
    for i in range(0, len(small), 25):
        pending = {args.name: small[i : i + 25]}
        while pending:
            pending = c.batch_write_item(RequestItems=pending).get("UnprocessedItems") or {}
    values = [f"{a}.{b}E-130" for a in range(1, 10) for b in range(10)]
    # MongoDB stores an item as one BSON document, capped at 16 MiB, so
    # #394's 2,262-set shape (28.9 MB as BSON) cannot be stored there; --sets
    # lowers the count to the largest row the backend takes.
    sets = [{"NS": values} for _ in range(args.sets)]
    if every:
        for i in sorted(big_positions):
            c.put_item(TableName=args.name, Item={"pk": {"S": f"a{i:04d}"}, "sk": {"N": str(i)}, "l": {"L": sets}})
        print(f"worst-expansion: {args.small} small items with {len(big_positions)} list-of-{args.sets}-NS items interleaved every {every}", file=sys.stderr)
    else:
        for i in range(args.count):
            c.put_item(TableName=args.name, Item={"pk": {"S": f"zz{i:03d}"}, "sk": {"N": "0"}, "l": {"L": sets}})
        print(f"worst-expansion: {args.small} small items then {args.count} list-of-{args.sets}-NS items", file=sys.stderr)


def put_random(args) -> None:
    """Incompressible items: `B` attributes of random bytes, `args.bytes` in
    all across `args.count` items."""
    c = client()
    per = args.bytes // args.count
    for i in range(args.count):
        c.put_item(
            TableName=args.name,
            Item={"pk": {"S": f"r{i:05d}"}, "sk": {"N": "0"}, "blob": {"B": os.urandom(per)}},
        )
    print(f"random: {args.count} items of {per} bytes", file=sys.stderr)


def create_backup(args) -> None:
    c = client()
    d = c.create_backup(TableName=args.name, BackupName=args.backup_name)["BackupDetails"]
    print(d["BackupArn"])
    print(d["BackupStatus"], file=sys.stderr)


def status(args) -> None:
    c = client()
    try:
        print(c.describe_backup(BackupArn=args.arn)["BackupDescription"]["BackupDetails"]["BackupStatus"])
    except ClientError as e:
        if e.response["Error"]["Code"] == "BackupNotFoundException":
            print("NOTFOUND")
        else:
            raise


def wait_available(args) -> None:
    c = client()
    started = time.monotonic()
    while True:
        d = c.describe_backup(BackupArn=args.arn)["BackupDescription"]
        s = d["BackupDetails"]["BackupStatus"]
        if s == "AVAILABLE":
            print(
                json.dumps(
                    {
                        "size_bytes": d["BackupDetails"]["BackupSizeBytes"],
                        "item_count": d["SourceTableDetails"]["ItemCount"],
                        "duration_s": round(time.monotonic() - started, 2),
                    }
                )
            )
            return
        if s != "CREATING":
            print(f"backup is {s}", file=sys.stderr)
            sys.exit(2)
        if time.monotonic() - started > args.timeout:
            print("timeout", file=sys.stderr)
            sys.exit(3)
        time.sleep(0.2)


def describe(args) -> None:
    c = client()
    print(json.dumps(c.describe_backup(BackupArn=args.arn)["BackupDescription"], default=str, indent=1))


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("create-table")
    a.add_argument("name")
    a.add_argument("--items", type=int, default=100000)
    a.add_argument("--pad", type=int, default=200)
    a.add_argument("--gsis", type=int, default=2)
    a.add_argument("--threads", type=int, default=16)
    a.set_defaults(fn=create_table)
    a = sub.add_parser("put-worst")
    a.add_argument("name")
    a.add_argument("--count", type=int, default=20)
    a.add_argument("--sets", type=int, default=2262)
    a.add_argument("--small", type=int, default=1000)
    a.add_argument("--interleave", type=int, default=0, help="place the large items at every Nth key among the small ones instead of after them")
    a.set_defaults(fn=put_worst)
    a = sub.add_parser("put-random")
    a.add_argument("name")
    a.add_argument("--count", type=int, default=256)
    a.add_argument("--bytes", type=int, default=64 * 1024 * 1024)
    a.set_defaults(fn=put_random)
    a = sub.add_parser("create-backup")
    a.add_argument("name")
    a.add_argument("backup_name")
    a.set_defaults(fn=create_backup)
    a = sub.add_parser("status")
    a.add_argument("arn")
    a.set_defaults(fn=status)
    a = sub.add_parser("wait-available")
    a.add_argument("arn")
    a.add_argument("--timeout", type=float, default=1800)
    a.set_defaults(fn=wait_available)
    a = sub.add_parser("describe")
    a.add_argument("arn")
    a.set_defaults(fn=describe)
    args = p.parse_args()
    args.fn(args)





# ===========================================================================
# PR 9: the restore kill points.
#
# Each run builds one backend's deployment from the hooks binary
# (`--features extenddb-app/backup-test-hooks`, whose feature chain pulls
# `extenddb-storage/test-hooks`, so the engine's restore points and the
# backend's own point are armed), `reconcile_interval_secs = 2`,
# `stale_job_after_secs = 45`; loads a table with one GSI (`by-g`, every
# item carries `g`), takes a backup, then starts a restore paused at the
# named point (`EXTENDDB_BACKUP_PAUSE`), kills the server by PID, restarts
# it without the pause, and asserts the outcome: at every ACTIVE outcome
# the base table's `Scan` count and the GSI's `Scan` count both equal
# `--items` (a resumed Cassandra restore once left the GSI short while the
# base count was exact, so the base count alone is not the assertion).
# Logs under `--work`.
#
#   kill-points.py restore <sqlite|postgres|mongodb|cassandra> <point> \
#       --bin <hooks binary> [--items N] [--work DIR] [--second-reach] \
#       [--vector] [--source-bin <sqlite build>]
#
# `pg_terminate_backend` is not a pause point of its own: the server is
# paused at `restore_after_file_commit`, the session holding the claim's
# advisory lock is terminated while the job is paused (psql must answer a
# row), the script waits past one heartbeat period and releases, and asserts
# the job's lost-claim stop, the record RUNNING with its progress row, the
# reconciler's takeover no earlier than `stale_job_after_secs` after the
# owner's last beat (read from the record at the termination; the beat
# precedes the termination by up to one period), and the exact counts. `--vector` gives the source table a
# vector index; with `restore_between_failed_and_removal` on postgres the
# cell then asserts the removed target's data table and `_ddb_vec_` relation
# are gone from the data database.
#
# Cassandra has no `SnapshotEngine`, so its CreateBackup is the in-catalog
# format and no backup taken on a Cassandra server restores through the
# store-backed job. A Cassandra kill point therefore takes its backup on a
# SQLite source server (`--source-bin`, a SQLite build), copies the prefix
# under the Cassandra target's root and adopts it with `extenddb backup
# sync` beside the target's server, as `devtools/backup-restore-matrix`
# does (the sync takes the maintenance claim and releases it before it
# exits, so the restore request that follows is not refused), then restores
# on the paused Cassandra server. The matrix script is imported for
# `copy_prefix` and `prefix_of`.
#
# `--second-reach` kills at the point's second reach instead of its first:
# the first pause is released (the release file written, then removed once
# the job logs its release), and the server is SIGKILLed when the job
# reaches the point again. For `restore_mongodb_after_insert_many` and
# `restore_cassandra_before_progress` that puts the kill after an earlier
# batch's progress is recorded, so the resume continues from that progress
# (MongoDB: each batch commits with its progress, nothing is re-applied;
# Cassandra: at most one batch re-applied by the upsert). The first-reach
# kill of `restore_cassandra_before_progress` leaves the first batch's items
# with no progress recorded; the resume finds the count at or below the
# backup's total and re-applies the first file with upserts (the engine's
# `target_not_empty` rule fails only a count above the total). Every kill
# point's outcome is therefore ACTIVE with the exact count, except the
# tampered-file point, whose target is removed.
#
# The services are local (PostgreSQL 55432, MongoDB 27117, Cassandra 9042).
# Every server is stopped by the PID this script started, never by a
# pattern over the process list; every scratch database, keyspace and the
# fixed MongoDB databases are dropped.
# ===========================================================================

import os
import re
import shutil
import signal
import subprocess

import requests
import urllib.parse  # noqa: F401

PG = {"host": "127.0.0.1", "port": "55432", "user": "postgres", "password": "devpass"}
# The MongoDB replica set a MongoDB kill point uses (its fixed databases are
# dropped per run, so give the script a replica set of its own).
MONGODB_URI = os.environ.get("EXTENDDB_TEST_MONGODB_URI") or "mongodb://127.0.0.1:27117/?directConnection=true"
ACCOUNT = "123456789012"
ADMIN_PASSWORD = "TestPass1!"
REGION = "us-east-1"


def _say(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _repo() -> str:
    """The ExtendDB checkout whose `devtools/` scripts this script calls
    (`provision-test-credentials`, `backup-restore-matrix`): `EXTENDDB_REPO`,
    or the working directory when the script is run from the checkout."""
    return os.path.abspath(os.environ.get("EXTENDDB_REPO") or os.getcwd())


def _free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _psql(stmt: str, database: str = "postgres") -> str:
    env = dict(os.environ, PGPASSWORD=PG["password"])
    r = subprocess.run(
        ["psql", "-h", PG["host"], "-p", PG["port"], "-U", PG["user"], "-d", database,
         "-v", "ON_ERROR_STOP=1", "-tAq", "-c", stmt],
        env=env, capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise SystemExit(f"psql failed: {r.stderr}\n{stmt}")
    return r.stdout.strip()


def _sqlite_connect(path: str):
    try:
        import pysqlite3 as sqlite3  # type: ignore
    except ImportError:
        import sqlite3  # type: ignore
    return sqlite3.connect(path, timeout=10)


class Deployment:
    """One backend's server for a kill-point run, from the hooks binary."""

    def __init__(self, backend: str, binary: str, work: str):
        self.backend = backend
        self.bin = binary
        self.work = work
        os.makedirs(work, exist_ok=True)
        self.config = os.path.join(work, "extenddb.toml")
        self.root = os.path.join(work, "root")
        self.spool = os.path.join(work, "spool")
        self.port = _free_port()
        self.endpoint = f"https://127.0.0.1:{self.port}"
        self.tag = f"kp{int(time.time()) % 1_000_000}"
        self.proc = None
        self.creds = {}

    def _free_port_again(self) -> None:
        self.port = _free_port()
        self.endpoint = f"https://127.0.0.1:{self.port}"

    def init(self) -> None:
        env = dict(os.environ, EXTENDDB_ADMIN_PASSWORD=ADMIN_PASSWORD)
        if self.backend == "sqlite":
            cmd = [self.bin, "init", "--backend", "sqlite", "--sqlite-path",
                   os.path.join(self.work, "db.sqlite"), "--config", self.config]
        elif self.backend == "postgres":
            cmd = [self.bin, "init", "--pg-host", PG["host"], "--pg-port", PG["port"],
                   "--pg-user", PG["user"], "--pg-pass", PG["password"],
                   "--data-db", f"{self.tag}_data", "--catalog-db", f"{self.tag}_catalog",
                   "--extenddb-user", f"{self.tag}_app", "--config", self.config]
        elif self.backend == "mongodb":
            self._mongo_drop()
            with open(self.config, "w") as f:
                f.write(f'[storage]\nbackend = "mongodb"\n\n[storage.mongodb]\nconnection_string = "{MONGODB_URI}"\n')
            cmd = [self.bin, "init", "--backend", "mongodb", "--config", self.config, "--overwrite"]
        elif self.backend == "cassandra":
            cmd = [self.bin, "init", "--backend", "cassandra",
                   "--cassandra-contact-points", "127.0.0.1:9042",
                   "--cassandra-user", "cassandra", "--cassandra-pass", "cassandra",
                   "--config", self.config]
        else:
            raise SystemExit(f"unknown backend {self.backend}")
        with open(os.path.join(self.work, "init.log"), "w") as log:
            r = subprocess.run(cmd, cwd=self.work, env=env, stdout=log, stderr=subprocess.STDOUT)
        if r.returncode != 0:
            print(open(os.path.join(self.work, "init.log")).read())
            raise SystemExit(f"init of the {self.backend} server failed")
        self._write_config()

    def _write_config(self) -> None:
        home = os.path.expanduser("~")
        os.makedirs(self.root, mode=0o700, exist_ok=True)
        store = f'[backup.store]\nkind = "filesystem"\n[backup.store.filesystem]\npath = "{self.root}"\n'
        backup = "[backup]\nreconcile_interval_secs = 2\nstale_job_after_secs = 45\nmax_concurrent_jobs = 2\ndata_file_target_bytes = 33554432\n"
        if self.backend == "sqlite":
            text = open(self.config).read()
            text = re.sub(r"(?m)^[ \t]*#?[ \t]*port[ \t]*=[ \t]*\d+.*$", f"port = {self.port}", text, count=1)
            text = re.sub(r'(?m)^run_dir = .*$', f'run_dir = "{self.work}/run"', text)
            text += "\n" + backup + store
            open(self.config, "w").write(text)
        elif self.backend == "postgres":
            text = open(self.config).read()
            text = re.sub(r"(?m)^[ \t]*#?[ \t]*port[ \t]*=[ \t]*\d+.*$", f"port = {self.port}", text, count=1)
            text = re.sub(r'(?m)^run_dir = .*$', f'run_dir = "{self.work}/run"', text)
            text += "\n" + backup + store
            open(self.config, "w").write(text)
        else:
            conn = (MONGODB_URI if self.backend == "mongodb"
                    else "")
            storage = (f'[storage]\nbackend = "mongodb"\n\n[storage.mongodb]\nconnection_string = "{MONGODB_URI}"\n'
                       if self.backend == "mongodb" else
                       # `replication_factor = 1` as `init` writes it for a single
                       # node: the default is 3, and the data keyspace the first
                       # CreateTable creates under RF 3 on one node answers every
                       # lightweight transaction and quorum read with "No nodes
                       # available in query plan" (the catalog keyspace, created by
                       # `init` under RF 1, keeps working, so the server starts and
                       # the table reports ACTIVE before the first write fails).
                       '[storage]\nbackend = "cassandra"\n\n[storage.cassandra]\n'
                       'contact_points = ["127.0.0.1:9042"]\nusername = "cassandra"\npassword = "cassandra"\n'
                       'keyspace_prefix = "extenddb"\nreplication_factor = 1\ndatacenter = "datacenter1"\n')
            open(self.config, "w").write(
                f'[server]\nbind_addr = "127.0.0.1"\nport = {self.port}\nregion = "{REGION}"\n'
                f'run_dir = "{self.work}/run"\n\n'
                f'[server.tls]\ncert_path = "{home}/.extenddb/tls/cert.pem"\n'
                f'key_path = "{home}/.extenddb/tls/key.pem"\n\n'
                f'{storage}\n[auth]\nprovider = "builtin"\n\n{backup}\n{store}')

    def start(self, log_name: str, pause: str | None = None) -> None:
        env = dict(os.environ)
        if pause:
            env["EXTENDDB_BACKUP_PAUSE"] = pause
            env["EXTENDDB_BACKUP_PAUSE_RELEASE"] = os.path.join(self.work, "release")
        self.log_path = os.path.join(self.work, log_name)
        self.log = open(self.log_path, "a")
        self.proc = subprocess.Popen(
            [self.bin, "serve", "--foreground", "--config", self.config],
            cwd=self.work, env=env, stdout=self.log, stderr=subprocess.STDOUT)
        for _ in range(300):
            try:
                if requests.get(f"{self.endpoint}/health", verify=False, timeout=2).status_code == 200:
                    break
            except requests.RequestException:
                pass
            if self.proc.poll() is not None:
                break
            time.sleep(0.2)
        if self.proc.poll() is not None:
            raise SystemExit(f"the {self.backend} server exited at start; see {self.log_path}")
        _say(f"{self.backend} pid {self.proc.pid} on {self.port} ({log_name})")

    def sigkill(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=10)
            _say(f"SIGKILLed pid {self.proc.pid}")

    def sigterm(self) -> int:
        self.proc.send_signal(signal.SIGTERM)
        code = self.proc.wait(timeout=60)
        _say(f"SIGTERM pid {self.proc.pid}, exit {code}")
        return code

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.proc.send_signal(signal.SIGKILL)
                self.proc.wait(timeout=10)

    def provision(self) -> None:
        env = dict(os.environ, EXTENDDB_TEST_ENDPOINT=self.endpoint, EXTENDDB_ADMIN_USER="admin",
                   EXTENDDB_ADMIN_PASSWORD=ADMIN_PASSWORD, EXTENDDB_CA_CERT=os.path.expanduser("~/.extenddb/tls/cert.pem"))
        r = subprocess.run([sys.executable, os.path.join(_repo(), "devtools", "provision-test-credentials")],
                           env=env, capture_output=True, text=True)
        if r.returncode != 0:
            raise SystemExit(f"provisioning failed: {r.stderr}")
        for line in r.stdout.splitlines():
            m = re.match(r"export (\w+)=(.*)", line.strip())
            if m:
                self.creds[m.group(1)] = m.group(2)
                os.environ[m.group(1)] = m.group(2)
        os.environ["EXTENDDB_TEST_ENDPOINT"] = self.endpoint
        for key, value in [("control_plane_delay_seconds", "0.05"), ("index_propagation_delay_ms", "0")]:
            subprocess.run([self.bin, "settings", "--config", self.config, "set", key, value],
                           cwd=self.work, capture_output=True)

    def teardown(self) -> None:
        self.stop()
        if self.backend == "postgres":
            _psql(f"DROP DATABASE IF EXISTS {self.tag}_data WITH (FORCE)")
            _psql(f"DROP DATABASE IF EXISTS {self.tag}_catalog WITH (FORCE)")
            _psql(f"DROP ROLE IF EXISTS {self.tag}_app")
        elif self.backend == "mongodb":
            self._mongo_drop()
        elif self.backend == "cassandra":
            try:
                from cassandra.auth import PlainTextAuthProvider
                from cassandra.cluster import Cluster
                cluster = Cluster(["127.0.0.1"], port=9042,
                                  auth_provider=PlainTextAuthProvider("cassandra", "cassandra"))
                s = cluster.connect()
                for ks in ("extenddb_catalog", f"extenddb_account_{ACCOUNT}"):
                    s.execute(f"DROP KEYSPACE IF EXISTS {ks}")
                cluster.shutdown()
            except Exception as e:  # noqa: BLE001
                print(f"cassandra teardown: {e}")

    def _mongo_drop(self) -> None:
        """Drop the fixed databases and wait until each drop has completed: on
        a replica set `dropDatabase` is two-phase and a collection created
        while the database is drop-pending fails with code 215 or vanishes
        when the drop lands (a server started over a pending drop answered
        `ResourceNotFoundException` to `BatchWriteItem` into an ACTIVE
        table). The probe creates a collection until the server accepts it,
        then drops the collection, which leaves no pending database."""
        from pymongo import MongoClient
        from pymongo.errors import OperationFailure
        c = MongoClient(MONGODB_URI)
        try:
            for name in ("extenddb_catalog", "extenddb_data"):
                c.drop_database(name)
            deadline = time.monotonic() + 120
            for name in ("extenddb_catalog", "extenddb_data"):
                while True:
                    try:
                        c[name].create_collection("drop_probe")
                    except OperationFailure as e:
                        if e.code == 215 and time.monotonic() < deadline:
                            time.sleep(0.25)
                            continue
                        raise
                    c[name].drop_collection("drop_probe")
                    break
        finally:
            c.close()


def _ddb(dep: Deployment):
    import boto3
    from botocore.config import Config
    return boto3.client("dynamodb", endpoint_url=dep.endpoint, region_name=REGION, verify=False,
                        aws_access_key_id=dep.creds["AWS_ACCESS_KEY_ID"],
                        aws_secret_access_key=dep.creds["AWS_SECRET_ACCESS_KEY"],
                        config=Config(retries={"max_attempts": 10, "mode": "standard"}, read_timeout=120))


# The points and which backends they apply to.
RESTORE_POINTS = {
    "restore_after_download": ["postgres", "sqlite", "mongodb", "cassandra"],
    "restore_after_file_commit": ["postgres", "sqlite", "mongodb", "cassandra"],
    "restore_sqlite_mid_batch": ["sqlite"],
    "restore_cassandra_before_progress": ["cassandra"],
    "restore_mongodb_after_insert_many": ["mongodb"],
    "restore_postgres_before_progress_delete": ["postgres"],
    "restore_mongodb_before_worker_flip": ["mongodb"],
    "restore_between_failed_and_removal": ["postgres", "sqlite", "mongodb", "cassandra"],
    "restore_fenced_out": ["postgres"],
    "pg_terminate_backend": ["postgres"],
    "sigterm": ["postgres", "sqlite", "mongodb", "cassandra"],
}


def _wait_table_active(ddb, name: str, timeout: float = 120) -> None:
    """Poll DescribeTable until the table and every GSI are ACTIVE, the way
    the fidelity suite's `wait_for_active` does, before any load."""
    deadline = time.monotonic() + timeout
    while True:
        t = ddb.describe_table(TableName=name)["Table"]
        gsis = t.get("GlobalSecondaryIndexes") or []
        if t["TableStatus"] == "ACTIVE" and all(g.get("IndexStatus", "ACTIVE") == "ACTIVE" for g in gsis):
            return
        assert time.monotonic() < deadline, f"{name} did not reach ACTIVE"
        time.sleep(0.3)


def _batch_write_ready(ddb, name: str, reqs) -> None:
    """Write one batch, tolerating the load-readiness window on a freshly
    created table: a `ResourceNotFoundException` on MongoDB (a collection not
    yet visible) or an `InternalServerError` on Cassandra (schema not settled
    on the coordinator) right after the table reports ACTIVE is a harness
    race, not a restore defect, so re-poll ACTIVE and retry the batch for up
    to 60 s rather than aborting the load."""
    from botocore.exceptions import ClientError

    deadline = time.monotonic() + 240
    pending = {name: reqs}
    while pending:
        try:
            pending = ddb.batch_write_item(RequestItems=pending).get("UnprocessedItems") or {}
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("ResourceNotFoundException", "InternalServerError") and time.monotonic() < deadline:
                _wait_table_active(ddb, name)
                time.sleep(2)
                continue
            raise


def _signed_post(dep: Deployment, operation: str, body: dict):
    """A hand-signed request for an operation botocore's model lacks
    (CreateTable with VectorIndexes), as `devtools/backup-restore-matrix`
    sends it."""
    import json
    import boto3
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    body_bytes = json.dumps(body).encode()
    headers = {"X-Amz-Target": f"DynamoDB_20120810.{operation}", "Content-Type": "application/x-amz-json-1.0"}
    frozen = boto3.Session(aws_access_key_id=dep.creds["AWS_ACCESS_KEY_ID"],
                           aws_secret_access_key=dep.creds["AWS_SECRET_ACCESS_KEY"]).get_credentials().get_frozen_credentials()
    req = AWSRequest(method="POST", url=dep.endpoint, data=body_bytes, headers=headers)
    SigV4Auth(frozen, "dynamodb", REGION).add_auth(req)
    return requests.post(dep.endpoint, data=body_bytes, headers=dict(req.headers), verify=False)


def _create_table_and_backup(dep: Deployment, items: int, big_file: bool, vector: bool = False):
    ddb = _ddb(dep)
    name = f"kp{dep.tag}"
    attrs = [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "N"},
             {"AttributeName": "g", "AttributeType": "S"}]
    body = dict(TableName=name, KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                                           {"AttributeName": "sk", "KeyType": "RANGE"}],
                AttributeDefinitions=attrs, BillingMode="PAY_PER_REQUEST",
                GlobalSecondaryIndexes=[{"IndexName": "by-g",
                                         "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"},
                                                       {"AttributeName": "sk", "KeyType": "RANGE"}],
                                         "Projection": {"ProjectionType": "ALL"}}])
    if vector:
        # ExtendDB's VectorIndexes member is not in botocore's model, so the
        # CreateTable goes through a signed POST, as the matrix script's does.
        body["VectorIndexes"] = [{"IndexName": "by-emb", "Dimensions": 3, "DistanceFunction": "COSINE",
                                  "VectorAttribute": {"AttributeName": "emb"},
                                  "Projection": {"ProjectionType": "ALL"}}]
        r = _signed_post(dep, "CreateTable", body)
        if r.status_code != 200:
            raise SystemExit(f"CreateTable with a vector index: {r.status_code} {r.text}")
    else:
        ddb.create_table(**body)
    _wait_table_active(ddb, name)
    if dep.backend == "cassandra":
        # Cassandra's DDL settles on the coordinator a moment after the table
        # reports ACTIVE; a load that starts in that gap meets "No nodes
        # available in query plan" or an InternalServerError from the LWT.
        # `_batch_write_ready` retries through it; this settle wait shortens it.
        time.sleep(3)
    # big_file forces more than 10,000 items into one data file for the
    # SQLite and Cassandra mid-batch points (each sits between two batches of
    # one file); otherwise 4,000-byte items give two or three data files at
    # the 32 MiB file target. (A Cassandra deployment never takes the backup
    # here: its backup is the in-catalog format, module comment.)
    pad = 20 if big_file else 4000
    reqs = []
    for i in range(items):
        item = {"pk": {"S": f"p{i % 50:04d}"}, "sk": {"N": str(i)}, "g": {"S": f"g{i % 7}"}, "pad": {"S": "x" * pad}}
        if vector and i % 5 == 0:
            item["emb"] = {"L": [{"N": "0.1"}, {"N": str(0.01 * (i % 50))}, {"N": "0.3"}]}
        reqs.append({"PutRequest": {"Item": item}})
        if len(reqs) == 25 or i == items - 1:
            _batch_write_ready(ddb, name, reqs)
            reqs = []
    arn = ddb.create_backup(TableName=name, BackupName="kpbackup")["BackupDetails"]["BackupArn"]
    deadline = time.monotonic() + 600
    while True:
        d = ddb.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]["BackupStatus"]
        if d == "AVAILABLE":
            break
        assert d == "CREATING", d
        assert time.monotonic() < deadline, "backup did not become AVAILABLE"
        time.sleep(0.2)
    return ddb, name, arn


def _count(ddb, name: str, index: str | None = None) -> int:
    """The `Scan` count of the table, or of its GSI `index` (an eventually
    consistent read, the only kind a GSI takes)."""
    total, start = 0, None
    while True:
        kw = {"TableName": name, "Select": "COUNT"}
        if index:
            kw["IndexName"] = index
        else:
            kw["ConsistentRead"] = True
        if start:
            kw["ExclusiveStartKey"] = start
        r = ddb.scan(**kw)
        total += r["Count"]
        start = r.get("LastEvaluatedKey")
        if not start:
            return total


def _assert_exact(ddb, name: str, items: int, what: str) -> None:
    """The base table and the GSI `by-g` both hold `items` rows. Every item
    of `_create_table_and_backup`'s fixture carries `g`, so a GSI short of
    the base count is an index row the restore lost (a re-applied batch on
    Cassandra deleted its index rows before #742 was corrected)."""
    n = _count(ddb, name)
    assert n == items, f"{what}: restored {n}, expected {items}"
    g = _count(ddb, name, index="by-g")
    assert g == items, f"{what}: the GSI by-g holds {g} rows, expected {items}"


def _wait_reached(dep: Deployment, point: str, timeout: float = 120, times: int = 1) -> bool:
    """True once the log holds `times` lines of the point being reached."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with open(dep.log_path, errors="replace") as f:
            if sum(1 for line in f if f"reached kill point {point}" in line) >= times:
                return True
        if dep.proc.poll() is not None:
            return False
        time.sleep(0.2)
    return False


def _release_first_reach(dep: Deployment, point: str) -> None:
    """Release the paused job from the point's first reach and remove the
    release file again, so the next reach pauses. The pause polls the file
    every 100 ms and the job reaches the point again as soon as the next
    file's or batch's writes are done, so the log is polled every 10 ms for
    the release line and the file removed at once; a second reach the file
    still released (a next write that took less than that) is a setup
    failure, not a kill, and the run says so."""
    release = os.path.join(dep.work, "release")
    open(release, "w").close()
    deadline = time.monotonic() + 30
    while True:
        with open(dep.log_path, errors="replace") as f:
            released = sum(1 for line in f if f"released from kill point {point}" in line)
        if released >= 1:
            break
        if time.monotonic() > deadline:
            raise SystemExit(f"the job was not released from {point}; see {dep.log_path}")
        time.sleep(0.01)
    os.remove(release)
    if released >= 2:
        raise SystemExit(f"the second reach of {point} was released before the file was removed; raise --items")
    _say(f"{dep.backend}/{point}: first reach released; waiting for the second reach")


def _log_has(dep: Deployment, needle: str, timeout: float = 20) -> str | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with open(dep.log_path, errors="replace") as f:
            for line in f:
                if needle in line:
                    return line
        time.sleep(0.25)
    return None


def _tamper_a_data_file(dep: Deployment, arn: str) -> str:
    """Corrupt one data file's bytes under the store root so its sha256 no
    longer matches the manifest, which fails the restore in its verification
    pass before any item is written (5.3 step 3). Returns the file path."""
    import glob

    files = sorted(glob.glob(os.path.join(dep.root, "**", "data", "*.json.gz"), recursive=True))
    if not files:
        raise SystemExit(f"no data files under {dep.root} to tamper")
    victim = files[-1]
    with open(victim, "r+b") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(size // 2)
        b = f.read(16)
        f.seek(size // 2)
        f.write(bytes((x ^ 0xFF) for x in b))
    _say(f"tampered {os.path.relpath(victim, dep.root)}")
    return victim


def _matrix_module():
    """`devtools/backup-restore-matrix` imported as a module, for the prefix
    copy a Cassandra target needs."""
    import importlib.machinery
    import importlib.util

    path = os.path.join(_repo(), "devtools", "backup-restore-matrix")
    # The script has no `.py` suffix, so the loader is named explicitly.
    loader = importlib.machinery.SourceFileLoader("backup_restore_matrix", path)
    spec = importlib.util.spec_from_loader("backup_restore_matrix", loader)
    module = importlib.util.module_from_spec(spec)
    saved = sys.argv
    sys.argv = ["backup-restore-matrix"]
    try:
        spec.loader.exec_module(module)
    finally:
        sys.argv = saved
    return module


def _adopt_backup(source: Deployment, target: Deployment, arn: str) -> None:
    """Copy the source's backup prefix under the target's root and adopt it
    with `extenddb backup sync` beside the target's server, as the matrix's
    `adopt_backup` does: the sync must exit 0 and name the one backup
    adopted. Its output is kept in `sync.log`."""
    mx = _matrix_module()
    src_prefix = mx.prefix_of(source.root, arn)
    dst_prefix = mx.prefix_of(target.root, arn)
    mx.copy_prefix(src_prefix, dst_prefix, target.root)
    r = subprocess.run(
        [target.bin, "backup", "sync", "--config", target.config],
        cwd=target.work, capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=600,
    )
    with open(os.path.join(target.work, "sync.log"), "a") as log:
        log.write(f"$ extenddb backup sync: exit {r.returncode}\n{r.stdout}{r.stderr}\n")
    if r.returncode != 0 or f"  {arn}" not in r.stdout.splitlines() or "1 backup(s) adopted" not in r.stdout:
        raise SystemExit(f"backup sync on the {target.backend} target did not adopt {arn} "
                         f"(exit {r.returncode}): {(r.stdout + r.stderr)[-3000:]}")
    _say(f"backup {arn} adopted by the {target.backend} target (prefix copied, backup sync)")


def _run_restore_point(backend: str, point: str, binary: str, items: int, work: str,
                       second_reach: bool = False, source_bin: str | None = None,
                       vector: bool = False) -> int:
    """One restore kill point on one backend. Returns 0 on pass, 1 on a
    failed assertion, 2 on setup error. With `second_reach` the SIGKILL is
    taken at the point's second reach (module comment). On Cassandra the
    backup is taken on a SQLite source (`source_bin`) and adopted by the
    target (module comment). With `vector` the
    source table carries a vector index and the tamper cell on PostgreSQL
    asserts the removed target's data table and vector relation gone."""
    import boto3  # noqa: F401
    from botocore.exceptions import ClientError

    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work, exist_ok=True)
    dep = Deployment(backend, binary, work)
    big_file = point in ("restore_sqlite_mid_batch", "restore_cassandra_before_progress")
    if second_reach and point in ("sigterm", "pg_terminate_backend", "restore_fenced_out"):
        raise SystemExit(f"--second-reach does not apply to {point}")
    if vector and (backend != "postgres" or point != "restore_between_failed_and_removal"):
        raise SystemExit("--vector applies to restore_between_failed_and_removal on postgres")
    source = None
    if backend == "cassandra":
        if not source_bin:
            raise SystemExit("a Cassandra kill point needs --source-bin (a SQLite build) for its backup")
        source = Deployment("sqlite", os.path.abspath(source_bin), os.path.join(work, "source"))
    rc = 0
    try:
        if source is not None:
            source.init()
            source.start("source.log")
            source.provision()
            _, name, arn = _create_table_and_backup(source, items, big_file)
            source.stop()
            _say(f"source (sqlite) backup {arn} of {items} items taken and the source stopped")
        dep.init()
        if point == "sigterm":
            dep.start("serve.log")
        elif point == "pg_terminate_backend":
            # Not a pause point of its own: the claim session is terminated
            # while the job is paused after its first file's commit.
            dep.start("serve-paused.log", pause="restore_after_file_commit")
        else:
            dep.start("serve-paused.log", pause=point)
        dep.provision()
        if source is not None:
            _adopt_backup(source, dep, arn)
            ddb = _ddb(dep)
        else:
            ddb, name, arn = _create_table_and_backup(dep, items, big_file, vector=vector)
        target = f"{name}r"
        if point == "restore_between_failed_and_removal":
            _tamper_a_data_file(dep, arn)
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        vector_relation = None
        if vector:
            # The registration created the target's catalog rows; read the
            # table id and the vector index id so the data relations can be
            # probed once the failing restore's target is gone.
            vids = _psql("SELECT t.table_id, v.index_id FROM tables t JOIN vector_indexes v ON v.table_id = t.table_id "
                         f"WHERE t.table_name = '{target}' AND t.account_id = '{ACCOUNT}'", database=f"{dep.tag}_catalog")
            assert vids and "|" in vids, f"the target's vector index row was not found: {vids!r}"
            vector_relation = tuple(vids.splitlines()[0].split("|"))
            _say(f"{backend}/{point}: the target's table id {vector_relation[0]}, vector index id {vector_relation[1]}")

        if point == "sigterm":
            time.sleep(0.5)
            status = ddb.describe_table(TableName=target)["Table"]["TableStatus"]
            if status != "CREATING":
                raise SystemExit(f"the restore finished ({status}) before SIGTERM; raise --items")
            dep.sigterm()
            log = open(dep.log_path, errors="replace").read()
            assert "stopped by the server's shutdown" in log, log[-2000:]
            assert ": FAILED (" not in log, log[-2000:]
            dep.start("serve-restart.log")
            dep.provision()
            ddb = _ddb(dep)
            deadline = time.monotonic() + 240
            while ddb.describe_table(TableName=target)["Table"]["TableStatus"] != "ACTIVE":
                assert time.monotonic() < deadline, "the restore did not resume"
                time.sleep(0.5)
            _assert_exact(ddb, target, items, point)
            _say(f"{backend}/{point}: RUNNING across restart, resumed ACTIVE with {items} items (base and GSI exact)")
            return 0

        if point == "pg_terminate_backend":
            # The job is paused at restore_after_file_commit (file 0 committed,
            # one restore_progress row). Terminate the session that holds the
            # claim's advisory lock while the job is paused, wait past one
            # heartbeat period so the timer's next beat meets the terminated
            # session, then release: the job stops at its next check as a
            # lost claim with no further catalog or table access, the record
            # stays RUNNING with its progress row, and the same server's
            # reconciler takes it over once heartbeat_at is stale (45 s after
            # the last beat, so at least stale_job_after_secs after the
            # termination) and finishes it.
            paused_at = "restore_after_file_commit"
            if not _wait_reached(dep, paused_at, timeout=300):
                raise SystemExit(f"the kill point {paused_at} was not reached; see {dep.log_path}")
            out = _psql("SELECT pg_terminate_backend(l.pid) FROM pg_locks l "
                        "JOIN pg_stat_activity a ON a.pid = l.pid "
                        "WHERE l.locktype = 'advisory' AND l.objsubid = 1 "
                        f"AND a.datname = '{dep.tag}_catalog' AND a.pid <> pg_backend_pid()", database="postgres")
            terminated_at = time.monotonic()
            assert out.strip() == "t", f"no claim session was terminated (psql answered {out!r})"
            # The stale window counts from the owner's last confirmed beat on
            # the database's clock, which precedes the termination by up to
            # one heartbeat period; read that age now (no beat lands on the
            # terminated session, so it stays until the takeover).
            beat_age = float(_psql("SELECT extract(epoch FROM now() - heartbeat_at) FROM table_restores",
                                   database=f"{dep.tag}_catalog"))
            _say(f"{backend}/{point}: terminated the claim session (advisory objsubid=1) while paused at {paused_at}; "
                 f"the last beat was {beat_age:.1f} s old")
            time.sleep(20)
            open(os.path.join(dep.work, "release"), "w").close()
            stopped = _log_has(dep, "stopped with no further catalog or table access", timeout=120)
            assert stopped, "the job did not stop as a lost claim after its claim session was terminated"
            _say(f"{backend}/{point}: the job stopped {time.monotonic() - terminated_at:.1f} s after the termination: "
                 f"{stopped.strip()[-240:]}")
            rec = _psql("SELECT status, file_index, items_applied FROM table_restores", database=f"{dep.tag}_catalog")
            prog = _psql("SELECT file_index, items_applied FROM restore_progress ORDER BY 1", database=f"{dep.tag}_data")
            assert rec.startswith("RUNNING|"), f"the record is not RUNNING after the lost claim: {rec!r}"
            assert prog, "no restore_progress row survived the lost claim"
            _say(f"{backend}/{point}: record {rec!r}; progress rows {prog.replace(chr(10), ' ')!r}")
            took = _log_has(dep, "taken over by this instance and resumed", timeout=180)
            assert took, "the reconciler did not take the record over"
            took_after = time.monotonic() - terminated_at
            assert took_after + beat_age >= 45, (f"the takeover came {took_after:.1f} s after the termination and "
                                                 f"{took_after + beat_age:.1f} s after the last beat, inside stale_job_after_secs (45)")
            _say(f"{backend}/{point}: takeover {took_after:.1f} s after the termination, {took_after + beat_age:.1f} s "
                 f"after the last beat: {took.strip()[-200:]}")
            # The pause is the server's, so the successor's resume pauses at
            # the same point; release it.
            time.sleep(0.5)
            open(os.path.join(dep.work, "release"), "w").close()
            deadline = time.monotonic() + 240
            while True:
                st = ddb.describe_table(TableName=target)["Table"]["TableStatus"]
                if st == "ACTIVE":
                    break
                assert time.monotonic() < deadline, f"stuck at {st}"
                time.sleep(0.5)
            _assert_exact(ddb, target, items, point)
            _say(f"{backend}/{point}: lost claim, record RUNNING with its progress row, takeover {took_after:.1f} s "
                 f"after the termination, resumed ACTIVE with {items} items (base and GSI exact)")
            return 0

        if point == "restore_fenced_out":
            # The job pauses after loading every file, with its heartbeat
            # timer stopped (restore.rs). The claim session is terminated so
            # the advisory lock frees (on PostgreSQL the lock, not the stale
            # heartbeat alone, is what the reconciler waits on, 4(e)); the
            # same server's reconciler then takes the record over on a free
            # permit (max_concurrent_jobs = 2) once the heartbeat is stale and
            # finishes the restore. Releasing the paused job last, its own
            # DONE write matches no record (the successor's generation fenced
            # it out) and it makes no table access.
            if not _wait_reached(dep, point):
                raise SystemExit(f"the kill point {point} was not reached; see {dep.log_path}")
            _say(f"{backend}/{point}: reached; terminating the claim session so its lock frees")
            _psql("SELECT pg_terminate_backend(l.pid) FROM pg_locks l "
                  "JOIN pg_stat_activity a ON a.pid = l.pid "
                  "WHERE l.locktype = 'advisory' AND l.objsubid = 1 "
                  f"AND a.datname = '{dep.tag}_catalog' AND a.pid <> pg_backend_pid()", database="postgres")
            took = _log_has(dep, "taken over by this instance and resumed", timeout=120)
            assert took, "the reconciler did not take the fenced-out record over"
            # The server's pause is global, so the successor's resume job also
            # pauses at this point with its own heartbeat stopped; release now
            # so it finishes. The fenced original then proceeds to its own DONE
            # write, which matches no record (the successor bumped the
            # generation on takeover) and makes no table access.
            open(os.path.join(dep.work, "release"), "w").close()
            deadline = time.monotonic() + 240
            while ddb.describe_table(TableName=target)["Table"]["TableStatus"] != "ACTIVE":
                assert time.monotonic() < deadline, "the successor did not finish the restore"
                time.sleep(0.5)
            _assert_exact(ddb, target, items, f"{point} (the successor's table)")
            line = _log_has(dep, "no table access was made", timeout=30)
            assert line, "the released fenced-out job did not log its NotMatched DONE write"
            _say(f"{backend}/{point}: successor finished ACTIVE with {items} items; "
                 f"the released job's DONE write matched no record ({line.strip()[-80:]})")
            return 0

        # The SIGKILL points: wait for the line, SIGKILL, restart, assert.
        if not _wait_reached(dep, point, timeout=300):
            raise SystemExit(f"the kill point {point} was not reached; see {dep.log_path}")
        if second_reach:
            _release_first_reach(dep, point)
            if not _wait_reached(dep, point, timeout=300, times=2):
                raise SystemExit(f"the kill point {point} was not reached a second time; see {dep.log_path}")
            with open(dep.log_path, errors="replace") as f:
                released = sum(1 for line in f if f"released from kill point {point}" in line)
            if released >= 2:
                raise SystemExit(f"the second reach of {point} was released before the file was removed; raise --items")
            _say(f"{backend}/{point}: reached a second time; SIGKILL")
        else:
            _say(f"{backend}/{point}: reached; SIGKILL")
        dep.sigkill()
        dep.start("serve-restart.log")
        dep.provision()
        ddb = _ddb(dep)
        # The FAILED-target duty waits out the stale window (45 s) before it
        # removes the target, so give it more than one window.
        deadline = time.monotonic() + 240

        # `restore_cassandra_before_progress` at its first reach pauses
        # before the first batch's progress is recorded, so the kill leaves
        # the batch's items with no recorded progress. The resume counts the
        # target, finds no more items than the backup's total, and re-applies
        # the first file with upserts (the engine fails `target_not_empty`
        # only above the total), so the outcome below is ACTIVE with the exact
        # count, as for every other point. (MongoDB commits each batch with
        # its progress, so its first reach leaves nothing unrecorded.)

        want_gone = point == "restore_between_failed_and_removal"
        while True:
            try:
                st = ddb.describe_table(TableName=target)["Table"]["TableStatus"]
            except ClientError as e:
                assert e.response["Error"]["Code"] == "ResourceNotFoundException", e.response["Error"]
                st = "GONE"
            if want_gone and st == "GONE":
                break
            if not want_gone and st == "ACTIVE":
                break
            assert time.monotonic() < deadline, f"stuck at {st} for {point}"
            time.sleep(0.5)
        if not want_gone:
            _assert_exact(ddb, target, items, point)
            _say(f"{backend}/{point}{' (second reach)' if second_reach else ''}: resumed and ACTIVE with {items} items (base and GSI exact)")
            if second_reach:
                for needle in ("taken over by this instance and resumed", "resumed from data file", ": ACTIVE, "):
                    line = _log_has(dep, needle, timeout=5)
                    if line:
                        _say(f"    {line.strip()[-200:]}")
            if point == "restore_postgres_before_progress_delete":
                line = _log_has(dep, "restore progress row", timeout=20)
                _say(f"{backend}/{point}: {line.strip() if line else 'progress rows swept (no line seen)'}")
        else:
            line = _log_has(dep, "target", timeout=5)
            _say(f"{backend}/{point}: FAILED target removed after the stale window (target gone)")
            if vector_relation is not None:
                table_id, index_id = vector_relation
                left = _psql(f"SELECT to_regclass('\"_ddb_{table_id}\"') IS NOT NULL, "
                             f"to_regclass('\"_ddb_vec_{index_id}\"') IS NOT NULL", database=f"{dep.tag}_data")
                assert left == "f|f", f"the removed target left a data relation behind (data table, vector relation exist: {left!r})"
                _say(f"{backend}/{point} (vector): the data table _ddb_{table_id} and the vector relation "
                     f"_ddb_vec_{index_id} are gone with the target")
        return 0
    except AssertionError as e:
        print(f"KILL-POINT FAILED {backend}/{point}: {e}")
        return 1
    except SystemExit as e:
        print(f"KILL-POINT SETUP {backend}/{point}: {e}")
        return 2
    finally:
        try:
            dep.teardown()
        except Exception as e:  # noqa: BLE001
            print(f"teardown {backend}/{point}: {e}")
        if source is not None:
            source.stop()
        _say(f"logs under {work}")


def _restore_main(argv) -> None:
    p = argparse.ArgumentParser(prog="kill-points.py restore")
    p.add_argument("backend", choices=["sqlite", "postgres", "mongodb", "cassandra"])
    p.add_argument("point")
    p.add_argument("--bin", required=True)
    p.add_argument("--items", type=int, default=12000)
    p.add_argument("--work", required=True)
    p.add_argument("--second-reach", action="store_true",
                   help="SIGKILL at the point's second reach (after an earlier progress record)")
    p.add_argument("--source-bin", help="a SQLite build that takes the backup a Cassandra target restores")
    p.add_argument("--vector", action="store_true",
                   help="give the source table a vector index; restore_between_failed_and_removal on postgres then asserts the removed target's vector relation gone")
    a = p.parse_args(argv)
    if a.point not in RESTORE_POINTS:
        raise SystemExit(f"unknown point {a.point}; known: {', '.join(RESTORE_POINTS)}")
    if a.backend not in RESTORE_POINTS[a.point]:
        raise SystemExit(f"{a.point} does not apply to {a.backend}")
    sys.exit(_run_restore_point(a.backend, a.point, os.path.abspath(a.bin), a.items, os.path.abspath(a.work),
                                second_reach=a.second_reach, source_bin=a.source_bin,
                                vector=a.vector))


# Dispatch `kill-points.py restore ...` to the restore orchestrator, every
# other subcommand to PR 8's API helper.
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "restore":
        _restore_main(sys.argv[2:])
    else:
        main()
