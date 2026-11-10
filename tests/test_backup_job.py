# SPDX-License-Identifier: Apache-2.0

"""The store-backed CreateBackup job at the server's boundary.

Each test starts its own `extenddb serve --foreground` on an isolated
deployment with a filesystem backup store (SQLite: the default root beside
the database file; PostgreSQL: a root under the test's directory) and reads
the API's answers, the server's log, `extenddb backup list --failed` and the
store's root:

- a second CreateBackup while one job runs with `max_concurrent_jobs = 1`
  waits for the slot and is answered only once the first job has ended (the
  first backup AVAILABLE), and a third is accepted at once afterwards; a wait
  that outlives the 30 s bound is LimitExceededException naming the bound;
- a table holding an `N` value the wire let in (#618: `1E126` or `abc` in a
  non-key attribute, `1E126` in the range key of a key-ordered table) written
  through each of the four paths that store it unvalidated (BatchWriteItem,
  TransactWriteItems, UpdateItem on an absent item in the key it creates,
  ImportTable) gives a FAILED row, no manifest under the root, three log
  lines naming each item's key and attribute, and a FAILED reason giving the
  count and the first item and naming #618;
- an item over `max_item_size_bytes` (stored under the default limit, the
  server restarted with a lower one) gives a reason that does not name #618;
- a refused item with a UUID hash key and a ten-digit epoch `N` range key is
  named by its whole key, and the wire's reason for its value, in the log
  line and in `backup list --failed`.

The store-backed RestoreTableFromBackup job, on both deployments:

- a data file tampered with under the root fails the restore before any item
  is loaded, the target gone and the log naming the file;
- SIGTERM mid-restore leaves the record RUNNING and the restarted server
  resumes it (PostgreSQL after the stale window, SQLite at once);
- an S3 store (moto) cut mid-download leaves the record RUNNING and the
  restore completes once the store answers;
- an unordered backup holding one key in two files, and a key-ordered file
  leaving its `key_range` (both hand-built under the root), fail the restore
  with the target gone;
- a table keyed on two HASH attributes (the `allow_multipart_table_keys`
  preview, enabled on the cell's own deployment) backs up and restores with
  the source's item set, key schema and attribute definitions;
- (SQLite) a RUNNING restore record with no recorded progress whose target
  holds more items than the backup, the state a catalog restored from a dump
  leaves when it names a restore that finished and was written to since,
  seeded at the catalog, is FAILED `target_not_empty`
  by the restarted server's reconciler, the target left CREATING and
  `DeleteTable`, `DeleteBackup` and a second restore refused, and `extenddb
  backup remove-target` removes the target and its record, after which
  `DescribeTable` is `ResourceNotFoundException` and `DeleteBackup` succeeds.

`extenddb backup sync`, on the SQLite, PostgreSQL and MongoDB deployments
(MongoDB's row edits run `mongosh` in the replica set's container, which
`EXTENDDB_TEST_MONGODB_CONTAINER` names; "beside a server" is PostgreSQL and
MongoDB, whose commands take the maintenance claim):

- after the catalog loses four rows, sync gives an AVAILABLE row to the one
  whole prefix, and DescribeBackup, ListBackups and a restore of it work; a
  prefix without its manifest, with a sidecar that does not match, moved
  under another backup id, under `changelog/` or `exports/`, or with a
  FAILED, DELETING or CREATING row is reported and not adopted, the row
  left as it is (on SQLite, where sync takes the serve lock and refuses
  beside a server, too: plain sync runs no dead-owner resolution);
- (SQLite) plain sync after a SIGTERM mid-restore leaves the RUNNING record
  to the next server start, which resumes the restore to ACTIVE;
- a row whose manifest is gone is reported and stays AVAILABLE, and
  `--prune-missing` removes the row and deletes no object; an empty root, a
  root holding only the identity file and one with another store id each
  make the store unhealthy, and sync and `--prune-missing` refuse with no
  row changed and nothing written; so does a missing manifest behind the
  newest AVAILABLE row, until the manifest is back;
- a row-less prefix whose manifest the store cannot read is reported and
  the run exits 1 as incomplete, its summary line saying so; once it can be
  read, the next sync adopts it;
- prefixes whose manifests a server of another region wrote are adopted
  under this server's region: ListBackups names it, DescribeBackup answers
  either spelling, and a restore and a DeleteBackup by the other region's
  spelling work;
- `--adopt-store` on a root whose file names another catalog refuses, with
  nothing written, while this catalog's newest backup has no manifest under
  it, and adopts the root once the manifest is back;
- an identity file rewritten to name another catalog while a plain sync is
  in its prefix loop ends the command with exit 1, naming that catalog;
- (beside a server) while `--prune-missing` holds the maintenance claim, `backup
  purge` and `--adopt-store` fail at once, and `--adopt-store` on a root
  the catalog owns waits for a running job as `--prune-missing` does;
- (beside a server) on host-local roots, `--prune-missing` on the host that
  lacks a backup's bytes removes its row only, the other host keeps the
  bytes, and a sync there adopts them again;
- (beside a server) while purge holds the claim a plain sync fails at once;
- (SQLite and PostgreSQL: a second MongoDB catalog needs a replica set of
  its own) `--adopt-store` from a second catalog configured with the root makes that
  catalog the owner, its server restores the adopted backup, and the first
  catalog's server finds the store owned by another catalog;
- an identity file rewritten to name a third catalog while an adopting sync
  is in its prefix loop ends the command with exit 1, naming that catalog;
- (PostgreSQL) a DeleteBackup on the first catalog's server, still running
  with its last check healthy after the adoption, is refused by its
  deletion fence and leaves the bytes, and the adopting catalog's store
  stays healthy.

The binary at `EXTENDDB_BINARY` decides which deployment runs: a SQLite build,
a PostgreSQL build (with `EXTENDDB_TEST_PG_CONNECTION_STRING`) or a MongoDB
build (with `EXTENDDB_TEST_MONGODB_URI`, a replica set of this suite's own: the
catalog's database names are fixed, so each test destroys the previous test's
catalog and initializes its own); the restore cells run on the SQLite and
PostgreSQL deployments. No Cassandra deployment runs this file. Excluded from the backend-agnostic suite; run by name.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import ClientError

import lifecycle_helpers as lh
from test_backup_store_config import (
    ADMIN_PASSWORD,
    EXTENDDB_BINARY,
    _append,
    _client,
    _Forwarder,
    _free_port,
    _health,
    _moto_or_skip,
    _pg_build,
    _provision,
    _s3_env,
    _Server,
    _sqlite_build,
    _SqliteDeployment,
    _wait_backup_available,
    _wait_health,
    moto_bucket,  # noqa: F401 - the fixture of the store-outage cell
)

ACCOUNT = "123456789012"
# The job's bound in the slot-wait cell: items of this count and padding keep
# the first job running for longer than the second request's round trip (0.4 s
# for 30,000 400-byte items on a SQLite build of this host), so the second
# request is sent while the first job holds the one slot and has to wait for
# it. The passing branch is the second request admitted once the first job
# ended (the first backup AVAILABLE at its answer, and seen CREATING while it
# waited); a second answered while the first is still CREATING is a broken
# bound; a refusal at the wait's bound is the slow-host branch, which the cell
# accepts; a first backup already AVAILABLE before the second is sent is a
# probe that came too late, where the cell asks for more items.
BUSY_ITEMS = int(os.environ.get("EXTENDDB_TEST_BACKUP_BUSY_ITEMS", "30000"))
LIMIT_TEXT = (
    "Too many backup and restore jobs are running on this server "
    "(at most 1 at once, the [backup] max_concurrent_jobs bound); retry later"
)


class _Deployment:
    """A deployment with a filesystem store, its config, its root and its
    catalog (`catalog`: the SQLite database file, or the PostgreSQL catalog
    database's connection string), where a cell edits a `backup_index` row
    it rewrote the prefix of."""

    def __init__(self, backend: str, config: str, port: int, root: str, cert: str, cwd: str, catalog: str = ""):
        self.backend = backend
        self.config = config
        self.port = port
        self.root = root
        self.cert = cert
        self.cwd = cwd
        self.catalog = catalog

    def run(self, *args):
        import subprocess

        return subprocess.run(
            [EXTENDDB_BINARY, args[0], "--config", self.config, *args[1:]],
            cwd=self.cwd,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=180,
        )

    def set_limit(self, key: str, value: str) -> None:
        """Set a key of the init template's `[limits]` table."""
        with open(self.config) as f:
            text = f.read()
        assert text.count("[limits]\n") == 1, text
        text = text.replace("[limits]\n", f"[limits]\n{key} = {value}\n", 1)
        with open(self.config, "w") as f:
            f.write(text)


def _mongodb_build() -> bool:
    if not os.path.isfile(EXTENDDB_BINARY):
        return False
    res = subprocess.run([EXTENDDB_BINARY, "version"], capture_output=True, text=True, timeout=30)
    return "(mongodb)" in res.stdout


def _mongodb_deployment(base: str, uri: str) -> tuple[str, int]:
    """A catalog on the replica set at `uri`, initialized afresh: the config
    stub names the connection string `init` reads, the previous test's
    catalog (the database names are fixed per replica set) is destroyed first,
    and the generated config is given a free port."""
    config = os.path.join(base, "extenddb.toml")
    with open(config, "w") as f:
        f.write(f'[storage]\nbackend = "mongodb"\n[storage.mongodb]\nconnection_string = "{uri}"\n')
    env = os.environ.copy()
    env["EXTENDDB_ADMIN_PASSWORD"] = ADMIN_PASSWORD
    common = dict(cwd=base, capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=120, env=env)
    # Nothing to destroy on the first test; `destroy` then exits non-zero.
    subprocess.run([EXTENDDB_BINARY, "destroy", "--config", config, "--yes"], **common)
    res = subprocess.run(
        [EXTENDDB_BINARY, "init", "--backend", "mongodb", "--config", config, "--overwrite"], **common
    )
    assert res.returncode == 0, res.stdout + res.stderr
    port = lh._find_free_port()
    lh._patch_config_port(config, port)
    return config, port


@pytest.fixture()
def deployment(request, tmp_path) -> _Deployment:
    """The build's deployment: SQLite with the defaulted root, or PostgreSQL
    or MongoDB with a filesystem root under the test's directory. All read
    `reconcile_interval_secs = 1`, `max_concurrent_jobs = 1` and import from
    the test's directory (ImportTable reads `<root>/<account>/<file>`)."""
    base = os.path.realpath(str(tmp_path))
    if _sqlite_build():
        d = _SqliteDeployment(str(tmp_path))
        config, port, cert = d.config, d.port, d.cert
        root = os.path.join(base, "backups")
        store = ""
        backend = "sqlite"
        catalog = d.db_path
    elif _pg_build():
        cli_env = request.getfixturevalue("cli_env")
        result = lh._run_extenddb(
            "init", *lh._init_args(cli_env), config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": ADMIN_PASSWORD},
        )
        assert result.returncode == 0, result.stdout + result.stderr
        lh._patch_config_port(cli_env["config_path"], cli_env["port"])
        config, port = cli_env["config_path"], cli_env["port"]
        cert = os.path.join(cli_env["tls_dir"], "cert.pem")
        root = os.path.join(base, "backups")
        os.mkdir(root, 0o700)
        store = f'[backup.store]\nkind = "filesystem"\n[backup.store.filesystem]\npath = "{root}"\n'
        backend = "postgres"
        catalog = cli_env["conn_string"]
    elif _mongodb_build():
        uri = os.environ.get("EXTENDDB_TEST_MONGODB_URI")
        if not uri:
            pytest.skip("EXTENDDB_TEST_MONGODB_URI is unset (a replica set of this suite's own)")
        config, port = _mongodb_deployment(base, uri)
        cert = os.path.expanduser("~/.extenddb/tls/cert.pem")
        root = os.path.join(base, "backups")
        os.mkdir(root, 0o700)
        store = f'[backup.store]\nkind = "filesystem"\n[backup.store.filesystem]\npath = "{root}"\n'
        backend = "mongodb"
        # The restore cells that edit the catalog run on SQLite and PostgreSQL.
        catalog = ""
    else:
        pytest.skip("EXTENDDB_BINARY is neither a SQLite, a PostgreSQL nor a MongoDB build")
    os.mkdir(os.path.join(base, ACCOUNT))
    _append(
        config,
        "[backup]\nreconcile_interval_secs = 1\nmax_concurrent_jobs = 1\n"
        + store
        + f'[import]\npaths = ["{base}"]\n',
    )
    return _Deployment(backend, config, port, root, cert, base, catalog)


def _started(d: _Deployment, log_path: str) -> tuple[_Server, object, dict]:
    """The server started on the deployment, a client and its credentials."""
    server = _Server(d.config, log_path)
    try:
        _wait_health(
            d.port,
            lambda h: h.get("components", {}).get("backup_store", {}).get("status") == "healthy",
            server=server,
        )
        creds = _provision(d.port, d.cert, ADMIN_PASSWORD)
    except BaseException:
        # A server whose store never came up (or whose provisioning failed)
        # would otherwise keep running after the test's failure.
        server.stop()
        raise
    return server, _client(d.port, creds), creds


def _signed_post(d: _Deployment, creds: dict, operation: str, body: dict) -> requests.Response:
    """A hand-signed request for an operation botocore's model would refuse
    (ImportTable's `FileSource`)."""
    body_bytes = json.dumps(body).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }
    endpoint = f"https://127.0.0.1:{d.port}"
    frozen = boto3.Session(
        aws_access_key_id=creds["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=creds["AWS_SECRET_ACCESS_KEY"],
    ).get_credentials().get_frozen_credentials()
    aws_req = AWSRequest(method="POST", url=endpoint, data=body_bytes, headers=headers)
    SigV4Auth(frozen, "dynamodb", "us-east-1").add_auth(aws_req)
    return requests.post(endpoint, data=body_bytes, headers=dict(aws_req.headers), verify=False)


def _wait_active(ddb, name: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ddb.describe_table(TableName=name)["Table"]["TableStatus"] == "ACTIVE":
            return
        time.sleep(0.05)
    raise AssertionError(f"{name} did not become ACTIVE")


def _create_composite_table(ddb, name: str, gsi: bool = False) -> None:
    """A key-ordered table: `pk` S hash key, `sk` N range key."""
    attrs = [
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "sk", "AttributeType": "N"},
    ]
    kwargs: dict = {}
    if gsi:
        attrs.append({"AttributeName": "g", "AttributeType": "S"})
        kwargs["GlobalSecondaryIndexes"] = [
            {
                "IndexName": "by-g",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ]
    ddb.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=attrs,
        BillingMode="PAY_PER_REQUEST",
        **kwargs,
    )
    _wait_active(ddb, name)


def _wait_backup_gone(ddb, arn: str, timeout: float = 60.0) -> None:
    """Poll DescribeBackup until the backup is BackupNotFound (its row FAILED
    or removed); an AVAILABLE backup fails the test."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            status = ddb.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]["BackupStatus"]
        except ClientError as e:
            assert e.response["Error"]["Code"] == "BackupNotFoundException", e.response["Error"]
            return
        assert status == "CREATING", f"backup {arn} is {status}"
        assert time.monotonic() < deadline, f"backup {arn} still CREATING after {timeout} s"
        time.sleep(0.1)


_RUST_ESCAPES = {"n": "\n", "r": "\r", "t": "\t", "0": "\0", "\\": "\\", '"': '"', "'": "'"}


def _rust_debug_string(quoted: str) -> str:
    """The string a Rust `{:?}` of a `String` printed: the quotes removed and
    its escapes undone (`\\n`, `\\"`, `\\\\` and `\\u{hex}` for a character
    `Debug` does not print as itself). `json.loads` refuses the `\\u{..}`
    form, so a reason holding such a character (a key the client wrote with
    one) is decoded here."""
    assert quoted.startswith('"') and quoted.endswith('"'), quoted
    out: list[str] = []
    i, body = 0, quoted[1:-1]
    while i < len(body):
        c = body[i]
        if c != "\\":
            out.append(c)
            i += 1
            continue
        e = body[i + 1]
        if e == "u":
            close = body.index("}", i + 3)
            out.append(chr(int(body[i + 3 : close], 16)))
            i = close + 1
        else:
            out.append(_RUST_ESCAPES[e])
            i += 2
    return "".join(out)


def _failed_rows(d: _Deployment) -> dict[str, str]:
    """`extenddb backup list --failed`: each FAILED row's ARN to its reason."""
    res = d.run("backup", "list", "--failed")
    assert res.returncode == 0, res.stdout + res.stderr
    rows: dict[str, str] = {}
    for line in res.stdout.splitlines():
        m = re.match(r"^FAILED\s.*?\s(arn:\S+)\s.*failure_reason=(\".*\")$", line)
        if m:
            rows[m.group(1)] = _rust_debug_string(m.group(2))
    return rows


def _prefix(d: _Deployment, table: str, arn: str) -> str:
    return os.path.join(d.root, ACCOUNT, table, arn.rsplit("/", 1)[1])


def _assert_no_manifest(d: _Deployment, table: str, arn: str) -> None:
    prefix = _prefix(d, table, arn)
    if os.path.isdir(prefix):
        left = os.listdir(prefix)
        assert "extenddb-manifest.json" not in left, left


def _restored_item_lines(log: str, arn: str) -> list[str]:
    return [
        line for line in log.splitlines() if f"backup {arn}: item cannot be restored: " in line
    ]


def test_second_create_backup_over_the_job_bound_waits_for_the_slot(deployment, tmp_path):
    """With `max_concurrent_jobs = 1`, a CreateBackup sent while one job runs
    waits for the slot (up to 30 s) and is answered only once the first job
    has ended: when its answer arrives the first backup is AVAILABLE, never
    still CREATING (a second accepted while the first is CREATING fails the
    cell: the bound did not hold). A first backup already AVAILABLE when the
    second is sent means the probe came too late and asks for more items. A
    wait that outlives the bound (a slow host) is LimitExceededException with
    the bound's text, which the cell accepts and reports. The first job
    finishes AVAILABLE with every item and a GSI in its
    SourceTableFeatureDetails, and a third CreateBackup is accepted at once
    (the slot is free)."""
    d = deployment
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name = f"busy-{uuid.uuid4().hex[:8]}"
        _create_composite_table(ddb, name, gsi=True)
        _load(d, creds, name, BUSY_ITEMS)
        first = ddb.create_backup(TableName=name, BackupName="first")["BackupDetails"]
        assert first["BackupStatus"] == "CREATING", first
        before = ddb.describe_backup(BackupArn=first["BackupArn"])["BackupDescription"]["BackupDetails"]["BackupStatus"]
        if before == "AVAILABLE":
            pytest.fail(
                "the first backup was already AVAILABLE before the second CreateBackup was sent, "
                "so the probe checks nothing; raise EXTENDDB_TEST_BACKUP_BUSY_ITEMS"
            )

        # The second request from its own client and thread: it blocks on the
        # slot while the main thread watches the first backup.
        def second_request():
            client = _client(d.port, creds)
            sent = time.monotonic()
            try:
                return client.create_backup(TableName=name, BackupName="second")["BackupDetails"], None, time.monotonic() - sent
            except ClientError as e:
                return None, e.response["Error"], time.monotonic() - sent

        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(second_request)
            # The first backup is CREATING for as long as the second waits
            # (a poll that sees it AVAILABLE is the first job's end, after
            # which the second is admitted at once).
            seen_creating = 0
            while not pending.done():
                status = ddb.describe_backup(BackupArn=first["BackupArn"])["BackupDescription"]["BackupDetails"]["BackupStatus"]
                seen_creating += status == "CREATING"
                time.sleep(0.05)
            second, refused, waited = pending.result()
        status_at_answer = ddb.describe_backup(BackupArn=first["BackupArn"])["BackupDescription"]["BackupDetails"]["BackupStatus"]
        if refused is not None:
            # The slow-host branch: the first job outlived the 30 s wait.
            assert refused["Code"] == "LimitExceededException", refused
            assert refused["Message"] == LIMIT_TEXT, refused
            assert waited >= 25, f"a refusal before the wait's bound: {waited:.1f} s"
            print(f"branch: the second request waited {waited:.1f} s and was refused at the bound ({d.backend})")
        else:
            assert status_at_answer == "AVAILABLE", (
                f"the second CreateBackup was answered while the first job was still running "
                f"(the first backup is {status_at_answer}): the max_concurrent_jobs bound did not hold"
            )
            # The second request waited: the first was CREATING while it was
            # pending (a first job that ended before the thread connected
            # would pass the branch above without any wait).
            assert seen_creating > 0, (
                "the first backup was AVAILABLE before the second request was sent, so the probe "
                "checked nothing; raise EXTENDDB_TEST_BACKUP_BUSY_ITEMS"
            )
            assert second["BackupStatus"] in ("CREATING", "AVAILABLE"), second
            print(
                f"branch: the second request waited {waited:.1f} s for the slot and was admitted; "
                f"the first was seen CREATING {seen_creating} time(s) meanwhile ({d.backend})"
            )
            _wait_backup_available(ddb, second["BackupArn"], timeout=120)
        _wait_backup_available(ddb, first["BackupArn"], timeout=120)
        desc = ddb.describe_backup(BackupArn=first["BackupArn"])["BackupDescription"]
        assert desc["SourceTableDetails"]["ItemCount"] == BUSY_ITEMS, desc["SourceTableDetails"]
        gsis = desc["SourceTableFeatureDetails"]["GlobalSecondaryIndexes"]
        assert [g["IndexName"] for g in gsis] == ["by-g"], gsis
        assert os.path.isfile(os.path.join(_prefix(d, name, first["BackupArn"]), "extenddb-manifest.json"))
        third = ddb.create_backup(TableName=name, BackupName="third")["BackupDetails"]
        assert third["BackupStatus"] == "CREATING", third
        _wait_backup_available(ddb, third["BackupArn"], timeout=120)
        listed = sorted(b["BackupName"] for b in ddb.list_backups(TableName=name)["BackupSummaries"])
        expected = ["first", "third"] if refused is not None else ["first", "second", "third"]
        assert listed == expected, listed
    finally:
        server.stop()


def _load(d: _Deployment, creds: dict, name: str, n: int) -> None:
    """Write n composite-key items with BatchWriteItem from 8 threads, one
    client each: a boto3 client shared across threads fails at random with
    TLS record-layer errors that look like server faults."""
    local = threading.local()

    def write(start: int) -> None:
        if not hasattr(local, "client"):
            local.client = _client(d.port, creds)
        requests_ = [
            {
                "PutRequest": {
                    "Item": {
                        "pk": {"S": f"p{i % 500:04d}"},
                        "sk": {"N": str(i)},
                        "g": {"S": f"g{i % 50}"},
                        "pad": {"S": "x" * 400},
                    }
                }
            }
            for i in range(start, min(start + 25, n))
        ]
        pending = {name: requests_}
        while pending:
            pending = local.client.batch_write_item(RequestItems=pending).get("UnprocessedItems") or {}
            if pending:
                time.sleep(0.05)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(0, n, 25)))


# The three items each path stores: `1E126` and `abc` in a non-key attribute
# and `1E126` in the range key (an `abc` range key is refused by the number
# column on PostgreSQL and SQLite, so no path stores one). UpdateItem
# validates the values its expression sets, so its three items carry the bad
# value in the key it creates.
BAD_ITEMS = [
    {"pk": {"S": "a"}, "sk": {"N": "1"}, "amount": {"N": "1E126"}},
    {"pk": {"S": "a"}, "sk": {"N": "2"}, "note": {"N": "abc"}},
    {"pk": {"S": "b"}, "sk": {"N": "1E126"}},
]
BAD_KEYS = [
    {"pk": {"S": "a"}, "sk": {"N": "1E126"}},
    {"pk": {"S": "b"}, "sk": {"N": "1E127"}},
    {"pk": {"S": "c"}, "sk": {"N": "1E128"}},
]
GOOD_ITEMS = [{"pk": {"S": "z"}, "sk": {"N": str(i)}, "amount": {"N": str(i)}} for i in range(5)]


def _write_batch(d, ddb, creds, name):
    _create_composite_table(ddb, name)
    ddb.batch_write_item(
        RequestItems={name: [{"PutRequest": {"Item": i}} for i in BAD_ITEMS + GOOD_ITEMS]}
    )
    return BAD_ITEMS


def _write_transact(d, ddb, creds, name):
    _create_composite_table(ddb, name)
    ddb.transact_write_items(
        TransactItems=[{"Put": {"TableName": name, "Item": i}} for i in BAD_ITEMS + GOOD_ITEMS]
    )
    return BAD_ITEMS


def _write_update(d, ddb, creds, name):
    _create_composite_table(ddb, name)
    for key in BAD_KEYS:
        ddb.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="SET note = :v",
            ExpressionAttributeValues={":v": {"S": "created from the key"}},
        )
    for item in GOOD_ITEMS:
        ddb.put_item(TableName=name, Item=item)
    return BAD_KEYS


def _write_import(d, ddb, creds, name):
    path = os.path.join(d.cwd, ACCOUNT, f"{name}.json")
    with open(path, "w") as f:
        for item in BAD_ITEMS + GOOD_ITEMS:
            f.write(json.dumps({"Item": item}) + "\n")
    resp = _signed_post(
        d,
        creds,
        "ImportTable",
        {
            "FileSource": {"Path": path},
            "InputFormat": "DYNAMODB_JSON",
            "TableCreationParameters": {
                "TableName": name,
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "N"},
                ],
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "BillingMode": "PAY_PER_REQUEST",
            },
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["ImportTableDescription"]["ImportStatus"] == "COMPLETED", resp.text
    _wait_active(ddb, name)
    return BAD_ITEMS


WRITE_PATHS = {
    "batch_write_item": _write_batch,
    "transact_write_items": _write_transact,
    "update_item": _write_update,
    "import_table": _write_import,
}


def _key_json(item: dict) -> str:
    return json.dumps({"pk": item["pk"], "sk": item["sk"]}, separators=(",", ":"))


@pytest.mark.parametrize("path", sorted(WRITE_PATHS))
def test_an_item_the_wire_let_in_fails_the_backup(deployment, tmp_path, path):
    """Three items with an `N` value #618 let in, stored through one write
    path, and five that restore: CreateBackup answers CREATING, the job
    validates every item (three log lines, one per item, with its key and
    attribute path), the row is FAILED with a reason giving the count and
    the first item and naming #618, DescribeBackup is BackupNotFound,
    ListBackups omits it, and no manifest is under the root."""
    d = deployment
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name = f"n618-{path.replace('_', '')}-{uuid.uuid4().hex[:6]}"
        bad = WRITE_PATHS[path](d, ddb, creds, name)
        stored = ddb.scan(TableName=name, ConsistentRead=True)["Items"]
        assert len(stored) == len(bad) + len(GOOD_ITEMS), stored
        details = ddb.create_backup(TableName=name, BackupName="nightly")["BackupDetails"]
        assert details["BackupStatus"] == "CREATING", details
        arn = details["BackupArn"]
        _wait_backup_gone(ddb, arn)
        assert ddb.list_backups(TableName=name)["BackupSummaries"] == []
        _assert_no_manifest(d, name, arn)
        reason = _failed_rows(d)[arn]
        first = sorted(bad, key=lambda i: (i["pk"]["S"], float(i["sk"]["N"])))[0]
        assert reason.startswith(f"{len(bad)} item(s) cannot be restored; first: key {_key_json(first)}, attribute "), reason
        assert reason.endswith(" (#618)"), reason
        if "sk" in first and first["sk"]["N"] == "1E126":
            assert ", attribute sk: " in reason, reason
        else:
            assert ", attribute amount: " in reason, reason
        log = server.log_text()
        lines = _restored_item_lines(log, arn)
        assert len(lines) == len(bad), lines
        logged_keys = {re.search(r"key (\{.*?\}\}), attribute", line).group(1) for line in lines}
        assert logged_keys == {_key_json(i) for i in bad}, logged_keys
        assert all(re.search(r"attribute (sk|amount|note): ", line) for line in lines), lines
        assert f"backup {arn}: FAILED ({reason}); its objects were deleted" in log, log
        assert "more item(s) cannot be restored" not in log
    finally:
        server.stop()


def test_a_refused_item_with_a_uuid_and_epoch_key_is_named_whole(deployment, tmp_path):
    """One refused item with a realistic key, a UUID hash key and a ten-digit
    epoch range key, and `amount: abc`: the log line and the FAILED reason
    carry the whole key as DynamoDB JSON (`{"pk":{"S":"<uuid>"},"sk":{"N":
    "1761955200"}}`), not a shortened form, and the wire's reason for the
    value, so an operator can find the item from either."""
    d = deployment
    server, ddb, _ = _started(d, str(tmp_path / "serve.log"))
    try:
        name = f"n618-key-{uuid.uuid4().hex[:6]}"
        _create_composite_table(ddb, name)
        item = {"pk": {"S": str(uuid.uuid4())}, "sk": {"N": "1761955200"}, "amount": {"N": "abc"}}
        ddb.batch_write_item(RequestItems={name: [{"PutRequest": {"Item": i}} for i in [item] + GOOD_ITEMS]})
        arn = ddb.create_backup(TableName=name, BackupName="nightly")["BackupDetails"]["BackupArn"]
        _wait_backup_gone(ddb, arn)
        key = _key_json(item)
        assert f'"pk":{{"S":"{item["pk"]["S"]}"}}' in key and '"sk":{"N":"1761955200"}' in key, key
        value_reason = "attribute amount: The parameter cannot be converted to a numeric value: abc"
        reason = _failed_rows(d)[arn]
        assert reason == f"1 item(s) cannot be restored; first: key {key}, {value_reason} (#618)", reason
        lines = _restored_item_lines(server.log_text(), arn)
        assert len(lines) == 1, lines
        assert f"backup {arn}: item cannot be restored: key {key}, {value_reason}" in lines[0], lines[0]
    finally:
        server.stop()


def test_an_item_over_max_item_size_bytes_fails_the_backup_without_naming_618(deployment, tmp_path):
    """A 300 KB item stored under the default `max_item_size_bytes`, the
    server restarted with `[limits] max_item_size_bytes = 65536`: the backup
    is FAILED with a reason naming the item's size and the limit and not
    #618, and the server's own PutItem refuses such an item."""
    d = deployment
    name = f"big-{uuid.uuid4().hex[:8]}"
    server, ddb, _ = _started(d, str(tmp_path / "serve.log"))
    try:
        _create_composite_table(ddb, name)
        ddb.put_item(TableName=name, Item={"pk": {"S": "big"}, "sk": {"N": "1"}, "pad": {"S": "x" * 300000}})
        ddb.put_item(TableName=name, Item={"pk": {"S": "small"}, "sk": {"N": "1"}, "pad": {"S": "x"}})
        # The same table backs up whole under the default limit.
        arn = ddb.create_backup(TableName=name, BackupName="under-default")["BackupDetails"]["BackupArn"]
        _wait_backup_available(ddb, arn)
    finally:
        server.stop()
    d.set_limit("max_item_size_bytes", "65536")
    server, ddb, _ = _started(d, str(tmp_path / "serve2.log"))
    try:
        with pytest.raises(ClientError) as refused:
            ddb.put_item(TableName=name, Item={"pk": {"S": "big2"}, "sk": {"N": "1"}, "pad": {"S": "x" * 100000}})
        assert refused.value.response["Error"]["Code"] == "ValidationException"
        arn = ddb.create_backup(TableName=name, BackupName="under-lower")["BackupDetails"]["BackupArn"]
        _wait_backup_gone(ddb, arn)
        _assert_no_manifest(d, name, arn)
        reason = _failed_rows(d)[arn]
        assert reason.startswith('1 item(s) cannot be restored; first: key {"pk":{"S":"big"},"sk":{"N":"1"}}: item size '), reason
        assert reason.endswith(" bytes exceeds the limit of 65536 bytes"), reason
        assert "#618" not in reason, reason
        lines = _restored_item_lines(server.log_text(), arn)
        assert len(lines) == 1 and "exceeds the limit of 65536 bytes" in lines[0], lines
        # The first backup, taken under the default limit, is untouched.
        listed = [b["BackupName"] for b in ddb.list_backups(TableName=name)["BackupSummaries"]]
        assert listed == ["under-default"], listed
    finally:
        server.stop()


# --- the restore job ---------------------------------------------------------------

# Items of this count and padding make a backup of two data files (the knob's
# floor is 32 MiB of uncompressed lines) whose restore runs for long enough,
# on a PostgreSQL build of this host, that a SIGTERM sent right after the
# restore's response finds the job inside its download or its first file's
# load. The cells check the precondition they rely on (the target still
# CREATING when the signal is sent) rather than assume it.
RESTORE_ITEMS = int(os.environ.get("EXTENDDB_TEST_RESTORE_ITEMS", "10000"))
RESTORE_PAD = 4000


# The `[backup]` keys the restore cells add to the deployment's: data files
# close at the knob's floor, so RESTORE_ITEMS padded items make two of them.
RESTORE_BACKUP_SETTINGS = "data_file_target_bytes = 33554432\n"


def _add_backup_settings(d: _Deployment, extra: str) -> None:
    """Add `extra` to the deployment's `[backup]` table before the server
    starts (TOML refuses a second `[backup]` header)."""
    with open(d.config) as f:
        text = f.read()
    head = "[backup]\nreconcile_interval_secs = 1\n"
    assert text.count(head) == 1, text
    with open(d.config, "w") as f:
        f.write(text.replace(head, head + extra))


def _load_padded(d: _Deployment, creds: dict, name: str, n: int, pad: int) -> None:
    """`_load` with items of `pad` bytes of padding and a `g` the GSI keys
    on, from 8 threads with one client each."""
    local = threading.local()

    def write(start: int) -> None:
        if not hasattr(local, "client"):
            local.client = _client(d.port, creds)
        requests_ = [
            {
                "PutRequest": {
                    "Item": {
                        "pk": {"S": f"p{i % 500:04d}"},
                        "sk": {"N": str(i)},
                        "g": {"S": f"g{i % 50}"},
                        "pad": {"S": "x" * pad},
                    }
                }
            }
            for i in range(start, min(start + 25, n))
        ]
        pending = {name: requests_}
        while pending:
            pending = local.client.batch_write_item(RequestItems=pending).get("UnprocessedItems") or {}
            if pending:
                time.sleep(0.05)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(0, n, 25)))


def _count(ddb, name: str) -> int:
    total, start = 0, None
    while True:
        kwargs = {"TableName": name, "Select": "COUNT", "ConsistentRead": True}
        if start:
            kwargs["ExclusiveStartKey"] = start
        resp = ddb.scan(**kwargs)
        total += resp["Count"]
        start = resp.get("LastEvaluatedKey")
        if not start:
            return total


def _wait_table_gone(ddb, name: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status = ddb.describe_table(TableName=name)["Table"]["TableStatus"]
        except ClientError as e:
            assert e.response["Error"]["Code"] == "ResourceNotFoundException", e.response["Error"]
            return
        assert status == "CREATING", f"{name} is {status}, it was never writable"
        time.sleep(0.2)
    raise AssertionError(f"{name} still exists after {timeout} s")


def _backed_up_table(d: _Deployment, ddb, creds: dict, prefix: str) -> tuple[str, str]:
    """A composite-key table with a GSI holding RESTORE_ITEMS padded items,
    backed up to AVAILABLE in two data files; the name and the ARN."""
    name = f"{prefix}-{uuid.uuid4().hex[:8]}"
    _create_composite_table(ddb, name, gsi=True)
    _load_padded(d, creds, name, RESTORE_ITEMS, RESTORE_PAD)
    arn = ddb.create_backup(TableName=name, BackupName="nightly")["BackupDetails"]["BackupArn"]
    _wait_backup_available(ddb, arn, timeout=180)
    files = sorted(os.listdir(os.path.join(_prefix(d, name, arn), "data")))
    assert len(files) >= 2, f"the backup has one data file ({files}); raise EXTENDDB_TEST_RESTORE_ITEMS"
    return name, arn




def _restore_cell(d: _Deployment) -> None:
    """The store-backed restore cells run on the SQLite and PostgreSQL
    deployments; on MongoDB the store-backed restore is covered by
    tests/test_mongodb_backup_restore.py and the crate's live tests."""
    if d.backend == "mongodb":
        pytest.skip("the restore cells run on the SQLite and PostgreSQL deployments")

def test_a_tampered_data_file_fails_the_restore_before_any_item(deployment, tmp_path):
    """A byte of the backup's second data file flipped under the root: the
    restore request is accepted (CREATING with a RestoreSummary), the target
    is removed once the job fails, and the server log names the file. That
    the job verified every file before it loaded any is pinned by the FAILED
    reason's text: "does not match the manifest" is the hashing pass's, which
    runs over every file before the first `bulk_load`; a mismatch found while
    a file is loaded reads "data file <key>: data file sha256 checksum
    mismatch" or, when the flipped byte sits in the gzip trailer so the
    decoder's own CRC fires first, "data file <key>: data file decode error:
    corrupt gzip stream does not have a matching checksum" instead. A
    CREATING target answers no read whatever the job did, so the GetItem
    probe below only confirms the target never became readable. The backup
    itself stays AVAILABLE."""
    d = deployment
    _restore_cell(d)
    _add_backup_settings(d, RESTORE_BACKUP_SETTINGS)
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _backed_up_table(d, ddb, creds, "tamper")
        path = os.path.join(_prefix(d, name, arn), "data", "000002.json.gz")
        with open(path, "r+b") as f:
            f.seek(-1, os.SEEK_END)
            last = f.read(1)
            f.seek(-1, os.SEEK_END)
            f.write(bytes([last[0] ^ 0xFF]))
        target = f"{name}-r"
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        assert resp["RestoreSummary"]["SourceBackupArn"] == arn, resp
        readable = []
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                got = ddb.get_item(TableName=target, Key={"pk": {"S": "p0000"}, "sk": {"N": "0"}}, ConsistentRead=True)
                if "Item" in got:
                    readable.append(got["Item"])
            except ClientError:
                pass
            try:
                ddb.describe_table(TableName=target)
            except ClientError as e:
                assert e.response["Error"]["Code"] == "ResourceNotFoundException", e.response["Error"]
                break
            time.sleep(0.2)
        else:
            raise AssertionError(f"{target} was not removed within 120 s")
        assert readable == [], f"an item of the failed restore was readable: {readable[:1]}"
        log = server.log_text()
        failed = [line for line in log.splitlines() if "FAILED (data file data/000002.json.gz does not match the manifest" in line]
        assert len(failed) == 1, log[-4000:]
        assert "the target was removed" in failed[0], failed[0]
        assert ddb.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]["BackupStatus"] == "AVAILABLE"
    finally:
        server.stop()


def test_sigterm_leaves_the_restore_running_and_it_finishes_after_restart(deployment, tmp_path):
    """SIGTERM while the restore runs: the server drains, the job stops at
    its next file or chunk with no status write, the record stays RUNNING
    (the target CREATING with its RestoreSummary across the restart), and the
    restarted server's reconciler resumes it: on PostgreSQL once the dead
    owner's heartbeat is older than `stale_job_after_secs` (45 s, the floor,
    on the database's clock); on SQLite at the first cycle, since the restart
    frees the record's slot and ages every RUNNING record. The target reaches
    ACTIVE with every item and the GSI answers."""
    d = deployment
    _restore_cell(d)
    _add_backup_settings(d, RESTORE_BACKUP_SETTINGS + "stale_job_after_secs = 45\n")
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _backed_up_table(d, ddb, creds, "sigterm")
        target = f"{name}-r"
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        status = ddb.describe_table(TableName=target)["Table"]["TableStatus"]
        assert status == "CREATING", (
            f"the restore finished ({status}) before the signal was sent; raise EXTENDDB_TEST_RESTORE_ITEMS"
        )
        server.proc.terminate()
        code = server.wait_exit(timeout=60)
        assert code == 0, server.log_text()[-4000:]
        log = server.log_text()
        assert "stopped by the server's shutdown; the record stays RUNNING" in log, log[-4000:]
        assert ": FAILED (" not in log, log[-4000:]
    finally:
        server.stop()
    stopped_at = time.monotonic()
    server, ddb, creds = _started(d, str(tmp_path / "serve2.log"))
    try:
        table = ddb.describe_table(TableName=target)["Table"]
        if d.backend == "postgres":
            # PostgreSQL resumes only once the dead owner's heartbeat is
            # stale (45 s), so right after the restart the target is still
            # CREATING, carries its RestoreSummary, and a retry into the name
            # is refused as a restore in progress. On SQLite the restart frees
            # the record's slot and the first reconciler cycle resumes at
            # once, so the target may already be ACTIVE here; the
            # resume-timing assertion below pins the at-once behaviour.
            assert table["TableStatus"] == "CREATING", table
            assert table["RestoreSummary"]["SourceBackupArn"] == arn, table
            with pytest.raises(ClientError) as refused:
                ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)
            assert refused.value.response["Error"]["Code"] == "TableInUseException", refused.value.response
        _wait_active(ddb, target, timeout=240)
        resumed_after = time.monotonic() - stopped_at
        if d.backend == "postgres":
            assert resumed_after >= 45 - 5, f"the resume waited for the stale window ({resumed_after:.1f} s)"
        else:
            assert resumed_after < 45 - 5, f"the restart's slot rule resumes at once ({resumed_after:.1f} s)"
        assert _count(ddb, target) == RESTORE_ITEMS
        by_g = ddb.query(
            TableName=target,
            IndexName="by-g",
            KeyConditionExpression="g = :g",
            ExpressionAttributeValues={":g": {"S": "g7"}},
            Select="COUNT",
        )["Count"]
        assert by_g == RESTORE_ITEMS // 50, by_g
        log = server.log_text()
        assert "taken over by this instance and resumed" in log, log[-4000:]
        assert ": ACTIVE, " in log, log[-4000:]
    finally:
        server.stop()


def test_plain_sync_on_sqlite_leaves_a_stopped_servers_restore_to_resume(deployment, tmp_path):
    """SQLite: a SIGTERM while a store-backed restore runs leaves its record
    RUNNING; plain `backup sync` then runs under the serve lock with no
    dead-owner resolution, so it neither fails the restore nor removes its
    target, and the next server start resumes the restore to ACTIVE with
    every item."""
    d = deployment
    if d.backend != "sqlite":
        pytest.skip("plain sync runs under the serve lock only on SQLite")
    _add_backup_settings(d, RESTORE_BACKUP_SETTINGS)
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _backed_up_table(d, ddb, creds, "plainsync")
        target = f"{name}-r"
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        status = ddb.describe_table(TableName=target)["Table"]["TableStatus"]
        assert status == "CREATING", (
            f"the restore finished ({status}) before the signal was sent; raise EXTENDDB_TEST_RESTORE_ITEMS"
        )
        server.proc.terminate()
        assert server.wait_exit(timeout=60) == 0, server.log_text()[-4000:]
    finally:
        server.stop()
    res = d.run("backup", "sync")
    out = res.stdout + res.stderr
    assert res.returncode == 0, out
    assert "failed the restore" not in out and "removed abandoned restore target" not in out, out
    assert "is failed rather than resumed" not in out, out
    server, ddb, creds = _started(d, str(tmp_path / "serve2.log"))
    try:
        _wait_active(ddb, target, timeout=240)
        assert _count(ddb, target) == RESTORE_ITEMS
        log = server.log_text()
        assert "taken over by this instance and resumed" in log, log[-4000:]
    finally:
        server.stop()


def test_a_store_outage_mid_download_leaves_the_restore_running_and_it_completes(request, tmp_path, moto_bucket):
    """An S3 store (moto) reached through a TCP forwarder, with a spool: the
    forwarder is cut right after the restore's response, so the job's data
    file downloads fail as transport errors; the record stays RUNNING (the
    target CREATING) while the job retries with backoff; once the forwarder
    is back the download completes and the target reaches ACTIVE with every
    item, the spool entry removed. On PostgreSQL the spool is `spool_path`;
    on SQLite the defaulted `backup-spool/` beside the database file."""
    moto_host, moto_port = _moto_or_skip()
    bucket = moto_bucket
    base = os.path.realpath(str(tmp_path))
    port = _free_port()
    store = (
        '[backup.store]\nkind = "s3"\n[backup.store.s3]\n'
        f'bucket = "{bucket}"\nendpoint = "http://127.0.0.1:{port}"\n'
        'force_path_style = true\nregion = "us-east-1"\n'
    )
    if _sqlite_build():
        sd = _SqliteDeployment(str(tmp_path))
        spool = os.path.join(base, "backup-spool")
        _append(sd.config, "[backup]\nreconcile_interval_secs = 1\nmax_concurrent_jobs = 1\n" + RESTORE_BACKUP_SETTINGS + store)
        d = _Deployment("sqlite", sd.config, sd.port, "", sd.cert, base)
    elif _pg_build():
        cli_env = request.getfixturevalue("cli_env")
        result = lh._run_extenddb(
            "init", *lh._init_args(cli_env), config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": ADMIN_PASSWORD},
        )
        assert result.returncode == 0, result.stdout + result.stderr
        lh._patch_config_port(cli_env["config_path"], cli_env["port"])
        spool = os.path.join(base, "spool")
        _append(
            cli_env["config_path"],
            "[backup]\nreconcile_interval_secs = 1\nmax_concurrent_jobs = 1\n"
            + RESTORE_BACKUP_SETTINGS
            + f'spool_path = "{spool}"\n'
            + store,
        )
        d = _Deployment("postgres", cli_env["config_path"], cli_env["port"], "", os.path.join(cli_env["tls_dir"], "cert.pem"), base)
    else:
        pytest.skip("EXTENDDB_BINARY is neither a SQLite nor a PostgreSQL build")
    forwarder = _Forwarder(port, (moto_host, moto_port))
    server = _Server(d.config, str(tmp_path / "serve.log"), env=_s3_env())
    try:
        _wait_health(
            d.port,
            lambda h: h.get("components", {}).get("backup_store", {}).get("status") == "healthy",
            timeout=60,
            server=server,
        )
        creds = _provision(d.port, d.cert, ADMIN_PASSWORD)
        ddb = _client(d.port, creds)
        name = f"outage-{uuid.uuid4().hex[:8]}"
        _create_composite_table(ddb, name, gsi=True)
        _load_padded(d, creds, name, RESTORE_ITEMS, RESTORE_PAD)
        arn = ddb.create_backup(TableName=name, BackupName="nightly")["BackupDetails"]["BackupArn"]
        _wait_backup_available(ddb, arn, timeout=180)
        target = f"{name}-r"
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        # The cut: every data file get fails until the forwarder is back.
        forwarder.stop()
        cut_at = time.monotonic()
        while time.monotonic() - cut_at < 8:
            table = ddb.describe_table(TableName=target)["Table"]
            assert table["TableStatus"] == "CREATING", table
            assert table["RestoreSummary"]["RestoreInProgress"] is True, table
            time.sleep(0.5)
        log = server.log_text()
        retried = [line for line in log.splitlines() if "retried in" in line and "data/" in line]
        assert retried, "no download attempt was retried during the cut:\n" + log[-4000:]
        forwarder = _Forwarder(port, (moto_host, moto_port))
        _wait_active(ddb, target, timeout=240)
        assert _count(ddb, target) == RESTORE_ITEMS
        entries = [e for e in os.listdir(spool) if e.startswith("restore-")]
        assert entries == [], f"the spool entry stays: {entries}"
        log = server.log_text()
        assert ": ACTIVE, " in log and ": FAILED (" not in log, log[-4000:]
    finally:
        server.stop()
        forwarder.stop()


# --- `backup remove-target`: the target_not_empty state ------------------------------


def _seed_target_not_empty(d: _Deployment, target: str) -> str:
    """Put a finished restore (table ACTIVE, record DONE) back into the state
    a catalog restored from a dump leaves when it names a restore that
    finished and was written to since: the target CREATING with its items
    (one more than the backup holds, written after the restore) and a RUNNING
    manifest record with no progress, which the restarted server's reconciler
    ages, takes over, and fails `target_not_empty` since the target holds
    more items than the backup's total (a count at or below the total is a
    first data file applied in part and is resumed). No kill produces the
    state on any backend; the catalog is edited with the server stopped.
    Returns the target's table id."""
    try:
        import pysqlite3 as sqlite3
    except ImportError:
        import sqlite3
    conn = sqlite3.connect(d.catalog, timeout=10)
    try:
        row = conn.execute(
            "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?", (ACCOUNT, target)
        ).fetchone()
        assert row, f"no catalog row for {target}"
        table_id = row[0]
        n = conn.execute(
            "UPDATE tables SET table_status = 'CREATING' WHERE table_id = ?", (table_id,)
        ).rowcount
        assert n == 1, n
        n = conn.execute(
            "UPDATE table_restores SET status = 'RUNNING', file_index = NULL, items_applied = NULL, "
            "failure_reason = NULL, completed_at = NULL, heartbeat_at = '1970-01-01T00:00:00.000Z' "
            "WHERE table_id = ? AND kind = 'manifest' AND status = 'DONE'",
            (table_id,),
        ).rowcount
        assert n == 1, n
        conn.commit()
    finally:
        conn.close()
    return table_id


def test_remove_target_removes_a_target_not_empty_restore(deployment, tmp_path):
    """The operator's remedy for a resume that found no progress and more
    items than the backup holds. A backup restored to ACTIVE, with one item
    written to the target since, is seeded back into the `target_not_empty`
    state (`_seed_target_not_empty`: the shape a catalog restored from a dump
    leaves; a catalog edit is the one way to produce it). The restarted server's
    reconciler fails the record `target_not_empty` and leaves the target
    CREATING, which `DeleteTable` refuses (`ResourceInUseException`) and
    which keeps the backup in use (`BackupInUseException` to `DeleteBackup`
    and to a second restore). `extenddb backup remove-target`, run with the
    server stopped (on SQLite it takes the serve lock), removes the target
    and its record and prints what it did; the restarted server then answers
    `ResourceNotFoundException` for the target, `DeleteBackup` succeeds, and
    a new restore into the name runs to ACTIVE with every item."""
    d = deployment
    _restore_cell(d)
    if d.backend != "sqlite":
        pytest.skip("the seeded target_not_empty state is built on the SQLite catalog (the PostgreSQL command runs beside a live server and is the engine tests')")
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _small_backed_up_table(ddb, "rmt")
        target = f"{name}-r"
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        _wait_active(ddb, target)
        assert _count(ddb, target) == HAND_BUILT_ITEMS
        # Written to since: one item the backup does not hold, so the seeded
        # record's target holds more than the backup's total.
        ddb.put_item(TableName=target, Item=_hand_item(HAND_BUILT_ITEMS))
        assert _count(ddb, target) == HAND_BUILT_ITEMS + 1
    finally:
        server.stop()
    table_id = _seed_target_not_empty(d, target)

    server, ddb, creds = _started(d, str(tmp_path / "serve2.log"))
    try:
        deadline = time.monotonic() + 60
        while "target_not_empty" not in server.log_text():
            assert time.monotonic() < deadline, server.log_text()[-4000:]
            time.sleep(0.2)
        log = server.log_text()
        failed = [line for line in log.splitlines() if "target_not_empty" in line]
        assert any(target in line for line in failed), failed
        assert ddb.describe_table(TableName=target)["Table"]["TableStatus"] == "CREATING"
        with pytest.raises(ClientError) as refused:
            ddb.delete_table(TableName=target)
        assert refused.value.response["Error"]["Code"] == "ResourceInUseException", refused.value.response
        with pytest.raises(ClientError) as refused:
            ddb.delete_backup(BackupArn=arn)
        assert refused.value.response["Error"]["Code"] == "BackupInUseException", refused.value.response
        with pytest.raises(ClientError) as refused:
            ddb.restore_table_from_backup(TargetTableName=f"{name}-again", BackupArn=arn)
        assert refused.value.response["Error"]["Code"] == "BackupInUseException", refused.value.response
        # The reconciler's next cycles leave it: the FAILED-target duty skips
        # this reason, and the target is still CREATING after two more cycles.
        time.sleep(2.5)
        assert ddb.describe_table(TableName=target)["Table"]["TableStatus"] == "CREATING"
    finally:
        server.stop()

    res = d.run("backup", "remove-target", "--account", ACCOUNT, target)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "=== extenddb backup remove-target ===" in res.stdout, res.stdout + res.stderr
    assert 'Taking the maintenance claim as "extenddb backup remove-target on ' in res.stdout, res.stdout
    assert (
        f"removed the restore target {target} (account {ACCOUNT}, table id {table_id}) and its record: "
        f"its store-backed restore from {arn} was FAILED (target_not_empty)"
    ) in res.stdout, res.stdout + res.stderr
    assert "The maintenance claim is released." in res.stdout, res.stdout
    # A second run finds no record for the name and refuses, exit 1.
    res = d.run("backup", "remove-target", "--account", ACCOUNT, target)
    assert res.returncode == 1, res.stdout + res.stderr
    assert f"{target} (account {ACCOUNT}) is not a restore target: no restore record names it" in res.stderr, (
        res.stdout + res.stderr
    )

    server, ddb, creds = _started(d, str(tmp_path / "serve3.log"))
    try:
        with pytest.raises(ClientError) as gone:
            ddb.describe_table(TableName=target)
        assert gone.value.response["Error"]["Code"] == "ResourceNotFoundException", gone.value.response
        # The name is free and the backup no longer in use: a new restore runs.
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        _wait_active(ddb, target)
        assert _count(ddb, target) == HAND_BUILT_ITEMS
        ddb.delete_table(TableName=target)
        _wait_table_gone_any_status(ddb, target)
        ddb.delete_backup(BackupArn=arn)
        _wait_backup_gone(ddb, arn)
        log = server.log_text()
        assert "target_not_empty" not in log, log[-4000:]
    finally:
        server.stop()


def _seed_running_with_progress(d: _Deployment, target: str) -> None:
    """Put a finished restore (table ACTIVE, record DONE) back into the state
    a server killed after the restore's first data file committed leaves
    (the `restore_after_file_commit` kill point): the target CREATING with
    its items and a RUNNING manifest record whose progress names the file
    and its item count. Edited at the catalog with the server stopped, as
    `_seed_target_not_empty` does."""
    try:
        import pysqlite3 as sqlite3
    except ImportError:
        import sqlite3
    conn = sqlite3.connect(d.catalog, timeout=10)
    try:
        row = conn.execute(
            "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?", (ACCOUNT, target)
        ).fetchone()
        assert row, f"no catalog row for {target}"
        table_id = row[0]
        n = conn.execute(
            "UPDATE tables SET table_status = 'CREATING' WHERE table_id = ?", (table_id,)
        ).rowcount
        assert n == 1, n
        n = conn.execute(
            "UPDATE table_restores SET status = 'RUNNING', file_index = 0, items_applied = ?, "
            "failure_reason = NULL, completed_at = NULL "
            "WHERE table_id = ? AND kind = 'manifest' AND status = 'DONE'",
            (HAND_BUILT_ITEMS, table_id),
        ).rowcount
        assert n == 1, n
        conn.commit()
    finally:
        conn.close()


def test_purge_fails_a_killed_servers_store_backed_restore_rather_than_resuming_it(deployment, tmp_path):
    """`backup purge --confirm` under the SQLite serve lock, after a server
    that was running a store-backed restore stopped (the record RUNNING with
    recorded progress, `_seed_running_with_progress`): the command's own
    reconciler cycle holds the maintenance claim, so it fails the record and
    removes its target, as it does a legacy restore, and resumes nothing (no
    `taken over by this instance` line, no restore job in the command). The
    restarted server finds no target and no backup."""
    d = deployment
    _restore_cell(d)
    if d.backend != "sqlite":
        pytest.skip("the purge's own reconciler cycle runs under the SQLite serve lock only")
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _small_backed_up_table(ddb, "prg")
        target = f"{name}-r"
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        _wait_active(ddb, target)
        assert _count(ddb, target) == HAND_BUILT_ITEMS
    finally:
        server.stop()
    _seed_running_with_progress(d, target)

    res = d.run("backup", "purge", "--confirm")
    out = res.stdout + res.stderr
    assert res.returncode == 0, out
    assert "taken over by this instance" not in out, out
    assert "resumed by this instance" not in out, out
    assert "a maintenance claim is held; it is failed rather than resumed" in out, out
    assert "Waiting for running jobs" not in out, out

    server, ddb, creds = _started(d, str(tmp_path / "serve2.log"))
    try:
        with pytest.raises(ClientError) as gone:
            ddb.describe_table(TableName=target)
        assert gone.value.response["Error"]["Code"] == "ResourceNotFoundException", gone.value.response
        with pytest.raises(ClientError) as gone:
            ddb.describe_backup(BackupArn=arn)
        assert gone.value.response["Error"]["Code"] == "BackupNotFoundException", gone.value.response
        log = server.log_text()
        assert "taken over by this instance" not in log, log[-4000:]
    finally:
        server.stop()


def _wait_table_gone_any_status(ddb, name: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            ddb.describe_table(TableName=name)
        except ClientError as e:
            assert e.response["Error"]["Code"] == "ResourceNotFoundException", e.response["Error"]
            return
        time.sleep(0.2)
    raise AssertionError(f"{name} still exists after {timeout} s")


# --- hand-built prefixes: the format's rules met at the server's boundary ----------

HAND_BUILT_ITEMS = 12


def _hand_item(i: int) -> dict:
    """Item `i` of a hand-built data file, in the shape `_create_composite_table`
    keys on (`pk` S, `sk` N) with the GSI's `g`."""
    return {"pk": {"S": f"p{i % 4:04d}"}, "sk": {"N": str(i)}, "g": {"S": f"g{i % 3}"}, "v": {"S": f"value {i}"}}


def _gzip_lines(rows: list[dict]) -> tuple[bytes, int]:
    """The data file of `rows`: one `{"Item": ..}` line each, gzipped; the
    compressed bytes and the uncompressed length."""
    import gzip
    import io

    lines = b"".join(json.dumps({"Item": r}, separators=(",", ":")).encode() + b"\n" for r in rows)
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz:
        gz.write(lines)
    return buffer.getvalue(), len(lines)


def _write_manifest(prefix: str, manifest: dict) -> None:
    """Write the manifest and its sha256 sidecar under `prefix`, the files
    private as the store writes them, and the two DynamoDB export manifests
    for the manifest's data files, so the prefix stays a whole backup in the
    format's sense (one `md5Checksum` and one `itemCount` per file, agreeing
    with the extenddb manifest by construction)."""
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


def _write_data_files(prefix: str, files: list[list[dict]]) -> list[dict]:
    """Replace the data files under `prefix` with one file per list of rows
    (unordered: no `key_range`); the manifest entries."""
    import base64
    import hashlib
    import shutil

    data_dir = os.path.join(prefix, "data")
    shutil.rmtree(data_dir)
    os.mkdir(data_dir, 0o700)
    entries = []
    for index, rows in enumerate(files, start=1):
        compressed, uncompressed = _gzip_lines(rows)
        key = f"data/{index:06d}.json.gz"
        path = os.path.join(prefix, key)
        with open(path, "wb") as f:
            f.write(compressed)
        os.chmod(path, 0o600)
        entries.append({
            "key": key, "item_count": len(rows),
            "sha256": hashlib.sha256(compressed).hexdigest(),
            "md5": base64.b64encode(hashlib.md5(compressed).digest()).decode(),
            "bytes": len(compressed), "uncompressed_bytes": uncompressed, "key_range": None,
        })
    return entries


def _update_row(d: _Deployment, arn: str, size_bytes: int, item_count: int) -> None:
    """Set the `backup_index` row's `size_bytes` and `item_count` to the
    rewritten prefix's, as `backup sync` would record them."""
    backup_id = arn.rsplit("/", 1)[1]
    if d.backend == "sqlite":
        try:
            import pysqlite3 as sqlite3
        except ImportError:
            import sqlite3
        conn = sqlite3.connect(d.catalog, timeout=10)
        try:
            n = conn.execute(
                "UPDATE backup_index SET size_bytes = ?, item_count = ? WHERE account_id = ? AND backup_id = ?",
                (size_bytes, item_count, ACCOUNT, backup_id),
            ).rowcount
            conn.commit()
        finally:
            conn.close()
    else:
        import psycopg2

        conn = psycopg2.connect(d.catalog)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE backup_index SET size_bytes = %s, item_count = %s WHERE account_id = %s AND backup_id = %s",
                    (size_bytes, item_count, ACCOUNT, backup_id),
                )
                n = cur.rowcount
            conn.commit()
        finally:
            conn.close()
    assert n == 1, f"{n} backup_index rows match {arn}"


def _small_backed_up_table(ddb, prefix: str) -> tuple[str, str]:
    """A composite-key table with a GSI holding HAND_BUILT_ITEMS items,
    backed up to AVAILABLE; the name and the ARN."""
    name = f"{prefix}-{uuid.uuid4().hex[:8]}"
    _create_composite_table(ddb, name, gsi=True)
    pending = {name: [{"PutRequest": {"Item": _hand_item(i)}} for i in range(HAND_BUILT_ITEMS)]}
    while pending:
        pending = ddb.batch_write_item(RequestItems=pending).get("UnprocessedItems") or {}
    arn = ddb.create_backup(TableName=name, BackupName="nightly")["BackupDetails"]["BackupArn"]
    _wait_backup_available(ddb, arn, timeout=60)
    return name, arn


def _restore_fails(server: _Server, ddb, target: str, arn: str, *needles: str) -> str:
    """The restore of `arn` into `target` is accepted CREATING and the target
    is gone once the job fails, no item of it ever readable; the server log's
    FAILED line, which must carry every needle. The job logs the line right
    after it removes the target, so the log is polled for a moment."""
    resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
    assert resp["TableStatus"] == "CREATING", resp
    assert resp["RestoreSummary"]["SourceBackupArn"] == arn, resp
    _wait_table_gone(ddb, target)
    deadline = time.monotonic() + 15
    while True:
        failed = [line for line in server.log_text().splitlines() if ": FAILED (" in line and f" into {target} " in line]
        if failed or time.monotonic() > deadline:
            break
        time.sleep(0.25)
    assert len(failed) == 1, server.log_text()[-4000:]
    for needle in needles:
        assert needle in failed[0], failed[0]
    assert "the target was removed" in failed[0], failed[0]
    assert ddb.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]["BackupStatus"] == "AVAILABLE"
    return failed[0]


DUPLICATE_KEY_TEXT = "holds a primary key the target already has (the backup holds a key twice)"


def test_an_unordered_backup_holding_one_key_in_two_files_fails_the_restore(deployment, tmp_path):
    """The backup's prefix rewritten under the root as an unordered backup of
    two data files that share item 0's key (the manifest and its sidecar
    rewritten, the row's size and count updated): the restore is accepted,
    the job fails the second file on the duplicate, the target is gone and
    the log names the reason. On PostgreSQL and SQLite, the two deployments
    here, the detector is the base row's plain INSERT and the reason is its
    text (`DUPLICATE_KEY_TEXT`); the engine's final count against the
    manifest's total ("the manifest lists") is the detector on a backend
    whose write is an upsert (Cassandra, through the matrix). A backend with
    neither would restore 12 of 13 items."""
    d = deployment
    _restore_cell(d)
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _small_backed_up_table(ddb, "dup")
        prefix = _prefix(d, name, arn)
        with open(os.path.join(prefix, "extenddb-manifest.json")) as f:
            manifest = json.load(f)
        rows = [_hand_item(i) for i in range(HAND_BUILT_ITEMS)]
        entries = _write_data_files(prefix, [rows[:6], rows[6:] + [rows[0]]])
        manifest["item_order"] = "unordered"
        manifest["data_files"] = entries
        manifest["backup_size_bytes"] = sum(e["bytes"] for e in entries)
        manifest["source_table"]["item_count"] = HAND_BUILT_ITEMS + 1
        _write_manifest(prefix, manifest)
        _update_row(d, arn, manifest["backup_size_bytes"], HAND_BUILT_ITEMS + 1)
        line = _restore_fails(server, ddb, f"{name}-r", arn, "data/000002.json.gz", DUPLICATE_KEY_TEXT)
        assert "the manifest lists" not in line, line
    finally:
        server.stop()


def test_a_key_ordered_file_leaving_its_key_range_fails_the_restore(deployment, tmp_path):
    """The backup's manifest rewritten so its first data file's `key_range`
    ends at its first item: the reader's key-range check ends the file's
    stream at the second item, the restore is FAILED naming the file and the
    rule, and the target is gone."""
    d = deployment
    _restore_cell(d)
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _small_backed_up_table(ddb, "range")
        prefix = _prefix(d, name, arn)
        with open(os.path.join(prefix, "extenddb-manifest.json")) as f:
            manifest = json.load(f)
        assert manifest["item_order"] == "key_ordered", manifest["item_order"]
        first = manifest["data_files"][0]
        assert first["item_count"] >= 2, first
        first["key_range"]["last"] = first["key_range"]["first"]
        _write_manifest(prefix, manifest)
        _restore_fails(server, ddb, f"{name}-r", arn, "data/000001.json.gz", "key_range")
    finally:
        server.stop()


# --- the multi-part-key preview ------------------------------------------------------

MULTIPART_ITEMS = 60


def _scan_items(ddb, name: str) -> list[dict]:
    items: list[dict] = []
    kwargs: dict = {"TableName": name, "ConsistentRead": True}
    while True:
        resp = ddb.scan(**kwargs)
        items += resp["Items"]
        if "LastEvaluatedKey" not in resp:
            return items
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def _canonical(item: dict) -> str:
    return json.dumps(item, sort_keys=True, separators=(",", ":"))


def test_a_multi_part_key_table_restores_with_the_sources_item_set(deployment, tmp_path):
    """A table keyed on two HASH attributes (`[limits]
    allow_multipart_table_keys = true` on this cell's deployment, so a
    server that refuses the second HASH key fails the cell rather than
    skipping it) backs up to the store as an `unordered` backup and
    restores: the restored table's `Scan` holds what the source's held,
    with the source's key schema and attribute definitions. The comparison
    is with the source's `Scan`, not with the items written: the preview's
    item paths address such a table by its first HASH attribute on every
    backend (on SQLite the source keeps one item per `h1` value; on
    PostgreSQL a GetItem by the whole key finds nothing on the source
    either), and that is the preview's behaviour, not the restore's; for the
    same reason the cell makes no GetItem by the whole key."""
    d = deployment
    _restore_cell(d)
    d.set_limit("allow_multipart_table_keys", "true")
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name = f"mpk-{uuid.uuid4().hex[:8]}"
        key_schema = [
            {"AttributeName": "h1", "KeyType": "HASH"},
            {"AttributeName": "h2", "KeyType": "HASH"},
        ]
        attr_defs = [
            {"AttributeName": "h1", "AttributeType": "S"},
            {"AttributeName": "h2", "AttributeType": "N"},
        ]
        ddb.create_table(TableName=name, KeySchema=key_schema, AttributeDefinitions=attr_defs, BillingMode="PAY_PER_REQUEST")
        _wait_active(ddb, name)
        for i in range(MULTIPART_ITEMS):
            ddb.put_item(
                TableName=name,
                Item={"h1": {"S": f"tenant-{i % 5}"}, "h2": {"N": str(i)}, "v": {"S": f"value-{i}"}, "n": {"N": str(i * 1.5)}},
            )
        expected = sorted(_canonical(i) for i in _scan_items(ddb, name))
        assert expected, "the source holds no item"
        print(f"branch: the source holds {len(expected)} of {MULTIPART_ITEMS} items written ({d.backend})")
        arn = ddb.create_backup(TableName=name, BackupName="nightly")["BackupDetails"]["BackupArn"]
        _wait_backup_available(ddb, arn, timeout=60)
        with open(os.path.join(_prefix(d, name, arn), "extenddb-manifest.json")) as f:
            manifest = json.load(f)
        assert manifest["item_order"] == "unordered", manifest["item_order"]
        assert manifest["source_table"]["key_schema"] == key_schema, manifest["source_table"]["key_schema"]
        target = f"{name}-r"
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        assert resp["RestoreSummary"]["SourceBackupArn"] == arn, resp
        _wait_active(ddb, target, timeout=120)
        table = ddb.describe_table(TableName=target)["Table"]
        assert table["KeySchema"] == key_schema, table["KeySchema"]
        assert sorted(table["AttributeDefinitions"], key=lambda a: a["AttributeName"]) == attr_defs, table["AttributeDefinitions"]
        assert "RestoreSummary" not in table, table
        got = sorted(_canonical(i) for i in _scan_items(ddb, target))
        assert got == expected, f"{len(got)} restored items against {len(expected)} in the source"
        log = server.log_text()
        assert f" into {target} " in log and ": ACTIVE, " in log and ": FAILED (" not in log, log[-4000:]
    finally:
        server.stop()


# --- extenddb backup sync -----------------------------------------------------------


def _sync_cell(d: _Deployment) -> None:
    """The sync cells drop and rewrite `backup_index` rows at the catalog:
    with SQL on SQLite and PostgreSQL (`_catalog`), and on MongoDB with
    `mongosh` in the replica set's container (`_mongosh`), which
    `EXTENDDB_TEST_MONGODB_CONTAINER` names."""
    if d.backend == "mongodb" and not _mongodb_container():
        pytest.skip("EXTENDDB_TEST_MONGODB_CONTAINER is unset: the MongoDB row edits run mongosh in it")


def _beside(d: _Deployment) -> bool:
    """Whether the commands run beside a server, taking the maintenance
    claim: every backend but SQLite, whose commands take the serve lock."""
    return d.backend != "sqlite"


def _mongodb_container() -> str:
    return os.environ.get("EXTENDDB_TEST_MONGODB_CONTAINER", "").strip()


def _mongosh(script: str):
    """Run `script` with `mongosh` in the replica set's container against
    the catalog database (`db`), and return the JSON it prints last."""
    res = subprocess.run(
        ["docker", "exec", _mongodb_container(), "mongosh", "--quiet", "--eval",
         'db = db.getSiblingDB("extenddb_catalog");\n' + script],
        capture_output=True, text=True, timeout=60,
    )
    assert res.returncode == 0, res.stdout + res.stderr
    return json.loads(res.stdout.strip().splitlines()[-1])


def _mongodb_row_filter(arn: str) -> str:
    return json.dumps({"account_id": ACCOUNT, "backup_id": _backup_id(arn)})


def _catalog(d: _Deployment, statement: str, params: tuple) -> int:
    """Run one statement on the deployment's catalog and return its row count.
    The statement uses `?` placeholders and `{now}` for the database's clock,
    which become `%s` and `now()` on PostgreSQL."""
    if d.backend == "sqlite":
        try:
            import pysqlite3 as sqlite3
        except ImportError:
            import sqlite3
        conn = sqlite3.connect(d.catalog, timeout=10)
        try:
            n = conn.execute(statement.format(now="strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"), params).rowcount
            conn.commit()
        finally:
            conn.close()
        return n
    import psycopg2

    conn = psycopg2.connect(d.catalog)
    try:
        with conn.cursor() as cur:
            cur.execute(statement.format(now="now()").replace("?", "%s"), params)
            n = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return n


def _backup_id(arn: str) -> str:
    return arn.rsplit("/", 1)[1]


def _drop_row(d: _Deployment, arn: str) -> None:
    """Delete the backup's `backup_index` row: the catalog lost it."""
    if d.backend == "mongodb":
        n = _mongosh(f"print(db.backup_index.deleteMany({_mongodb_row_filter(arn)}).deletedCount)")
    else:
        n = _catalog(d, "DELETE FROM backup_index WHERE account_id = ? AND backup_id = ?", (ACCOUNT, _backup_id(arn)))
    assert n == 1, f"{n} backup_index rows match {arn}"


def _set_status(d: _Deployment, arn: str, status: str, beat_age_secs: int = 0) -> None:
    """Set the backup's row to `status`, its heartbeat `beat_age_secs` ago
    and (except for CREATING) its completion now, on the database's clock:
    a FAILED row inside its retention, a CREATING row whose owner beat just
    now (or, aged, a CREATING row whose job died)."""
    if d.backend == "mongodb":
        completion = (
            '{$unset: {completed_at: ""}}' if status == "CREATING" else "{$set: {completed_at: now}}"
        )
        n = _mongosh(
            f"const now = new Date(); const filter = {_mongodb_row_filter(arn)};\n"
            f"const n = db.backup_index.updateOne(filter, {{$set: {{status: {json.dumps(status)}, "
            f"heartbeat_at: new Date(now.getTime() - {beat_age_secs * 1000})}}}}).matchedCount;\n"
            f"db.backup_index.updateOne(filter, {completion});\nprint(n)"
        )
        assert n == 1, f"{n} backup_index rows match {arn}"
        return
    if beat_age_secs:
        assert d.backend == "postgres", "an aged heartbeat is written on PostgreSQL and MongoDB"
        n = _catalog(
            d,
            "UPDATE backup_index SET status = ?, completed_at = NULL, "
            f"heartbeat_at = {{now}} - interval '{beat_age_secs} seconds' WHERE account_id = ? AND backup_id = ?",
            (status, ACCOUNT, _backup_id(arn)),
        )
        assert n == 1, f"{n} backup_index rows match {arn}"
        return
    n = _catalog(
        d,
        "UPDATE backup_index SET status = ?, heartbeat_at = {now}, "
        "completed_at = CASE WHEN ? = 'CREATING' THEN NULL ELSE {now} END "
        "WHERE account_id = ? AND backup_id = ?",
        (status, status, ACCOUNT, _backup_id(arn)),
    )
    assert n == 1, f"{n} backup_index rows match {arn}"


def _rows(d: _Deployment) -> dict[str, str]:
    """`extenddb backup list`: each row's ARN to its status."""
    res = d.run("backup", "list")
    assert res.returncode == 0, res.stdout + res.stderr
    rows: dict[str, str] = {}
    for line in res.stdout.splitlines():
        m = re.match(r"^([A-Z]+)\s+\d{12}\s+\S+\s+\d+\s+\d+\s+(arn:\S+)\s", line)
        if m:
            rows[m.group(2)] = m.group(1)
    return rows


def _key_prefix(table: str, arn: str) -> str:
    """The prefix as the store's keys, and sync's report, spell it."""
    return f"{ACCOUNT}/{table}/{_backup_id(arn)}/"


def _store_id(root: str) -> str:
    with open(os.path.join(root, "extenddb-store.json")) as f:
        return json.load(f)["store_id"]


def _private_file(path: str, data: bytes) -> None:
    """Write `path` 0600 with its missing parents 0700, as the store's own
    objects are."""
    parent = os.path.dirname(path)
    missing = []
    while not os.path.isdir(parent):
        missing.append(parent)
        parent = os.path.dirname(parent)
    for directory in reversed(missing):
        os.mkdir(directory, 0o700)
    with open(path, "wb") as f:
        f.write(data)
    os.chmod(path, 0o600)


def test_sync_rebuilds_dropped_rows_and_adopts_only_verified_prefixes(deployment, tmp_path):
    """The catalog loses the rows of four backups; `extenddb backup sync`
    gives an AVAILABLE row to the one whose prefix is whole, after which
    DescribeBackup (the manifest's name and item count), ListBackups and a
    restore of it work. Not adopted, each reported with its reason and exit
    0: the prefix whose manifest was removed, the prefix whose sidecar no
    longer matches, a prefix moved under another backup id (its manifest
    names the backup id it was written under), a file under the table's
    `changelog/` and one under `exports/`, and the prefixes whose rows are
    FAILED, DELETING and CREATING, whose rows sync reads and changes none.
    On PostgreSQL plain sync takes the maintenance claim and waits for no
    job; on SQLite it runs under the serve lock (it refuses while the
    server runs) and runs no dead-owner resolution, so the DELETING and
    CREATING rows a stopped server left stay for its next start. A second
    sync adopts nothing."""
    _sync_cell(deployment)
    d = deployment
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, kept = _small_backed_up_table(ddb, "sync")
        others: dict[str, str] = {}
        for label in ("no-manifest", "bad-sidecar", "moved", "failed", "deleting", "creating"):
            arn = ddb.create_backup(TableName=name, BackupName=label)["BackupDetails"]["BackupArn"]
            _wait_backup_available(ddb, arn, timeout=60)
            others[label] = arn
        if d.backend == "sqlite":
            # The serve lock held is the catalog refusal `serve` gives for
            # it: exit 6, before any store or row is read.
            for args in (("sync",), ("sync", "--prune-missing")):
                beside = d.run("backup", *args)
                assert beside.returncode == 6, beside.stdout + beside.stderr
                assert "another extenddb process" in beside.stdout + beside.stderr, beside.stdout + beside.stderr
                assert "=== extenddb backup sync" not in beside.stdout, beside.stdout
            assert server.proc.poll() is None, "the server kept running"
    finally:
        server.stop()
    root = d.root
    store_id = _store_id(root)
    for arn in (kept, others["no-manifest"], others["bad-sidecar"], others["moved"]):
        _drop_row(d, arn)
    os.remove(os.path.join(_prefix(d, name, others["no-manifest"]), "extenddb-manifest.json"))
    with open(os.path.join(_prefix(d, name, others["bad-sidecar"]), "extenddb-manifest.sha256"), "w") as f:
        f.write("0" * 64 + "  extenddb-manifest.json\n")
    moved_id = _backup_id(others["moved"])
    millis, suffix = moved_id.split("-")
    moved_to = f"{int(millis) + 1:014d}-{suffix}"
    os.rename(_prefix(d, name, others["moved"]), os.path.join(root, ACCOUNT, name, moved_to))
    _private_file(os.path.join(root, ACCOUNT, name, "changelog", "000001.json"), b"{}\n")
    _private_file(os.path.join(root, "exports", "01", "part-0.json"), b"{}\n")
    _set_status(d, others["failed"], "FAILED")
    _set_status(d, others["deleting"], "DELETING")
    _set_status(d, others["creating"], "CREATING")

    res = d.run("backup", "sync")
    out = res.stdout + res.stderr
    print(f"branch: {d.backend}\n{res.stdout}")
    assert res.returncode == 0, out
    assert f"Store:  filesystem {os.path.realpath(root)} (store id {store_id})" in res.stdout, res.stdout
    took_claim = 'Took the maintenance claim as "extenddb backup sync on ' in res.stdout
    assert took_claim == _beside(d), res.stdout
    assert "Waiting for running jobs" not in res.stdout, res.stdout
    adopted = res.stdout.split("--- Adopted", 1)[1].split("--- Skipped", 1)[0]
    assert adopted.splitlines()[1:-1] == [f"  {kept}"], adopted
    assert "1 backup(s) adopted" in res.stdout, res.stdout
    skipped = res.stdout.split("--- Skipped (not adopted):", 1)[1].split("--- AVAILABLE rows", 1)[0]
    expected = [
        f"{_key_prefix(name, others['no-manifest'])}: not a complete backup: no manifest",
        f"{_key_prefix(name, others['bad-sidecar'])}: not a complete backup: the sidecar does not match",
        f"{ACCOUNT}/{name}/{moved_to}/: the manifest names {others['moved']}, whose account, table and backup id are "
        "not the prefix's",
        f"{ACCOUNT}/{name}/changelog/: reserved for the change-log archive",
        "exports/: reserved for exports",
        f"{_key_prefix(name, others['failed'])}: the catalog has a row for it (FAILED, store id {store_id})",
        f"{_key_prefix(name, others['deleting'])}: the catalog has a row for it (DELETING, store id {store_id})",
        f"{_key_prefix(name, others['creating'])}: the catalog has a row for it (CREATING, store id {store_id})",
    ]
    for line in expected:
        assert f"  {line}" in skipped, f"{line!r} not in\n{skipped}"
    assert len([line for line in skipped.splitlines() if line.startswith("  ")]) == len(expected), skipped
    rows = _rows(d)
    assert rows[kept] == "AVAILABLE", rows
    for label in ("no-manifest", "bad-sidecar", "moved"):
        assert others[label] not in rows, (label, rows)
    assert rows[others["failed"]] == "FAILED", rows
    assert rows[others["deleting"]] == "DELETING", rows
    assert rows[others["creating"]] == "CREATING", rows
    for label in ("deleting", "creating"):
        assert os.path.isfile(os.path.join(_prefix(d, name, others[label]), "extenddb-manifest.json")), label
    # Nothing under a skipped prefix was changed or removed.
    assert os.path.isfile(os.path.join(_prefix(d, name, others["bad-sidecar"]), "extenddb-manifest.json"))
    assert os.path.isfile(os.path.join(root, ACCOUNT, name, moved_to, "extenddb-manifest.json"))
    assert os.path.isfile(os.path.join(root, ACCOUNT, name, "changelog", "000001.json"))

    again = d.run("backup", "sync")
    assert again.returncode == 0, again.stdout + again.stderr
    assert "0 backup(s) adopted" in again.stdout, again.stdout
    assert f"  {_key_prefix(name, kept)}: the catalog has a row for it (AVAILABLE, store id {store_id})" in again.stdout

    for label in ("failed", "deleting", "creating"):
        if rows[others[label]] in ("FAILED", "DELETING", "CREATING"):
            _set_status(d, others[label], "AVAILABLE")
    server, ddb, creds = _started(d, str(tmp_path / "serve2.log"))
    try:
        details = ddb.describe_backup(BackupArn=kept)["BackupDescription"]
        assert details["BackupDetails"]["BackupStatus"] == "AVAILABLE", details
        assert details["BackupDetails"]["BackupName"] == "nightly", details
        assert details["SourceTableDetails"]["TableName"] == name, details
        assert details["SourceTableDetails"]["ItemCount"] == HAND_BUILT_ITEMS, details
        listed = [s["BackupArn"] for s in ddb.list_backups(TableName=name)["BackupSummaries"]]
        assert kept in listed, listed
        for label in ("no-manifest", "bad-sidecar", "moved"):
            with pytest.raises(ClientError) as gone:
                ddb.describe_backup(BackupArn=others[label])
            assert gone.value.response["Error"]["Code"] == "BackupNotFoundException", (label, gone.value.response)
        target = f"{name}-r"
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=kept)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        _wait_active(ddb, target)
        assert _count(ddb, target) == HAND_BUILT_ITEMS
    finally:
        server.stop()


def _with_root_moved_away(d: _Deployment):
    """The store volume unmounted: the root renamed away and an empty
    directory of the same path in its place; returns the restore callable."""
    away = d.root + ".away"
    os.rename(d.root, away)
    os.mkdir(d.root, 0o700)

    def restore() -> None:
        shutil.rmtree(d.root)
        os.rename(away, d.root)

    return away, restore


def _tree(root: str) -> list[str]:
    out = []
    for dirpath, dirnames, filenames in os.walk(root):
        for f in filenames:
            path = os.path.join(dirpath, f)
            with open(path, "rb") as fh:
                out.append(f"{os.path.relpath(path, root)} {hashlib.sha256(fh.read()).hexdigest()}")
    return sorted(out)


def test_prune_missing_and_the_unhealthy_store_refusals(deployment, tmp_path):
    """A row whose manifest is gone (not the newest AVAILABLE one): plain
    sync reports it with the `--prune-missing` hint and leaves it AVAILABLE;
    `--prune-missing` takes the maintenance claim and removes the row
    outright (no tombstone, BackupNotFound), deleting no object: the rest of
    its prefix stays. An unhealthy store refuses plain sync and
    `--prune-missing`, exit 1, with no row changed and nothing written to the
    root, for each shape an unmounted volume gives: an empty root under the
    rows, a root holding only the identity file (the newest AVAILABLE row's
    manifest absent), and a root whose identity file carries another store
    id. Last, the newest AVAILABLE row's manifest
    removed: the store is unhealthy (its manifest probe), so sync and
    `--prune-missing` both refuse and the row stays AVAILABLE until the
    manifest is put back, after which sync runs again. PostgreSQL runs the
    commands beside its server; SQLite stops it (the serve lock)."""
    _sync_cell(deployment)
    d = deployment
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, older = _small_backed_up_table(ddb, "prune")
        newest = ddb.create_backup(TableName=name, BackupName="second")["BackupDetails"]["BackupArn"]
        _wait_backup_available(ddb, newest, timeout=60)
        if d.backend == "sqlite":
            server.stop()
        print(f"branch: {d.backend}, the server {'running' if _beside(d) else 'stopped'}")

        os.remove(os.path.join(_prefix(d, name, older), "extenddb-manifest.json"))
        res = d.run("backup", "sync")
        assert res.returncode == 0, res.stdout + res.stderr
        missing = res.stdout.split("--- AVAILABLE rows whose manifest is not in the store (the rows were AVAILABLE when the catalog was read):", 1)[1]
        missing = missing.split("=== extenddb backup sync complete", 1)[0]
        assert older in missing and newest not in missing, missing
        assert "(left AVAILABLE; run with --prune-missing to remove the rows, which deletes no object)" in missing, missing
        assert ("Took the maintenance claim" in res.stdout) == _beside(d), res.stdout
        assert _rows(d)[older] == "AVAILABLE"
        left_behind = _tree(_prefix(d, name, older))
        assert left_behind, "the older prefix keeps its other objects"

        res = d.run("backup", "sync", "--prune-missing")
        out = res.stdout + res.stderr
        assert res.returncode == 0, out
        assert 'Took the maintenance claim as "extenddb backup sync on ' in res.stdout, res.stdout
        assert f"  {older}: removed: the row is gone, and no object was deleted" in res.stdout, res.stdout
        assert "(1 removed)" in res.stdout, res.stdout
        audit = [
            line for line in out.splitlines()
            if "extenddb::audit" in line and "removed an AVAILABLE row whose manifest is not under the root" in line
        ]
        assert len(audit) == 1 and older in audit[0] and "backup sync --prune-missing" in audit[0], out
        rows = _rows(d)
        assert older not in rows and rows[newest] == "AVAILABLE", rows
        assert _tree(_prefix(d, name, older)) == left_behind, "prune deleted an object"
        if _beside(d):
            with pytest.raises(ClientError) as gone:
                ddb.describe_backup(BackupArn=older)
            assert gone.value.response["Error"]["Code"] == "BackupNotFoundException", gone.value.response

        before = _rows(d)
        with open(os.path.join(d.root, "extenddb-store.json"), "rb") as f:
            identity_file = f.read()
        other_id = json.loads(identity_file)
        other_id["store_id"] = str(uuid.uuid4())
        shapes = {
            "an empty root": None,
            "the identity file alone": identity_file,
            "another store id": (json.dumps(other_id, indent=2) + "\n").encode(),
        }
        for shape, content in shapes.items():
            away, put_back = _with_root_moved_away(d)
            try:
                if content is not None:
                    _private_file(os.path.join(d.root, "extenddb-store.json"), content)
                tree = _tree(d.root)
                for args in (("sync",), ("sync", "--prune-missing")):
                    res = d.run("backup", *args)
                    out = res.stdout + res.stderr
                    assert res.returncode == 1, (shape, args, out)
                    assert f"the filesystem backup store {os.path.realpath(d.root)} is unhealthy" in out, (shape, out)
                    assert "sync changes no row while the store is unhealthy" in out, (shape, out)
                    assert "Took the maintenance claim" not in out, (shape, out)
                    assert _tree(d.root) == tree, f"{shape}: the command wrote to the root"
                    print(f"{shape} {' '.join(args)}: {out.strip().splitlines()[-1]}")
            finally:
                put_back()
            assert _rows(d) == before, shape

        manifest = os.path.join(_prefix(d, name, newest), "extenddb-manifest.json")
        os.rename(manifest, manifest + ".aside")
        try:
            for args in (("sync",), ("sync", "--prune-missing")):
                res = d.run("backup", *args)
                out = res.stdout + res.stderr
                assert res.returncode == 1 and "is unhealthy" in out and "manifest" in out, out
            assert _rows(d) == before
        finally:
            os.rename(manifest + ".aside", manifest)
        res = d.run("backup", "sync")
        assert res.returncode == 0, res.stdout + res.stderr
        assert "0 backup(s) adopted" in res.stdout and "0 AVAILABLE row(s) whose manifest is gone" in res.stdout, res.stdout
    finally:
        server.stop()


def test_a_second_maintenance_command_fails_at_once(deployment, tmp_path):
    """`backup sync --prune-missing` takes the maintenance claim and waits
    for a CREATING row whose owner beat just now (the row edited at the
    catalog, its heartbeat the database's now); while it waits, `backup
    purge --confirm` and `backup sync --adopt-store` each fail within
    seconds, exit 1, naming the one-at-a-time rule, and change nothing. The
    first command, signalled, stops before any change and releases the
    claim. `--adopt-store` on the root this catalog owns then takes the
    claim and waits for the same live job ("Waiting for running jobs to
    finish"), and a signal stops it before any change; after that a plain
    sync and `--prune-missing` run. SQLite has
    no second command to refuse: every writing command takes the serve lock
    first, which the sync cell shows."""
    _sync_cell(deployment)
    d = deployment
    if not _beside(d):
        pytest.skip("on SQLite the serve lock refuses a second writing command before any claim is read")
    import subprocess

    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, busy = _small_backed_up_table(ddb, "claim")
    finally:
        server.stop()
    _set_status(d, busy, "CREATING")
    out_path = str(tmp_path / "first.out")
    with open(out_path, "wb") as out:
        first = subprocess.Popen(
            [EXTENDDB_BINARY, "backup", "--config", d.config, "sync", "--prune-missing"],
            cwd=d.cwd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        )
    try:
        deadline = time.monotonic() + 60
        while "Waiting for running jobs to finish" not in open(out_path).read():
            assert first.poll() is None, open(out_path).read()
            assert time.monotonic() < deadline, open(out_path).read()
            time.sleep(0.2)
        assert busy in open(out_path).read() or name in open(out_path).read(), open(out_path).read()
        for args in (("purge", "--confirm"), ("sync", "--adopt-store")):
            started = time.monotonic()
            res = d.run("backup", *args)
            took = time.monotonic() - started
            out = res.stdout + res.stderr
            assert res.returncode == 1, (args, out)
            assert "one maintenance command runs at a time" in out, (args, out)
            assert took < 15, f"{args} took {took:.1f} s to refuse"
            print(f"{' '.join(args)}: refused in {took:.1f} s")
        assert first.poll() is None, "the first command stopped while the others ran"
        first.send_signal(15)
        assert first.wait(timeout=30) == 1
        text = open(out_path).read()
        assert "stopped before any change: SIGTERM received while waiting" in text, text
    finally:
        if first.poll() is None:
            first.kill()
            first.wait(timeout=10)
    assert _rows(d)[busy] == "CREATING"
    # `--adopt-store` on a root this catalog owns takes the claim and waits
    # for the same live job before its sync, as `--prune-missing` does.
    adopt_path = str(tmp_path / "adopt.out")
    with open(adopt_path, "wb") as out:
        adopting = subprocess.Popen(
            [EXTENDDB_BINARY, "backup", "--config", d.config, "sync", "--adopt-store"],
            cwd=d.cwd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        )
    try:
        deadline = time.monotonic() + 60
        while "Waiting for running jobs to finish" not in open(adopt_path).read():
            assert adopting.poll() is None, open(adopt_path).read()
            assert time.monotonic() < deadline, open(adopt_path).read()
            time.sleep(0.2)
        text = open(adopt_path).read()
        assert "there is nothing to adopt" in text and 'Took the maintenance claim as "extenddb backup sync on ' in text, text
        assert "--- Adopted" not in text, text
        adopting.send_signal(15)
        assert adopting.wait(timeout=30) == 1
        text = open(adopt_path).read()
        assert "stopped before any change: SIGTERM received while waiting" in text, text
    finally:
        if adopting.poll() is None:
            adopting.kill()
            adopting.wait(timeout=10)
    assert _rows(d)[busy] == "CREATING"
    _set_status(d, busy, "AVAILABLE")
    res = d.run("backup", "sync", "--prune-missing")
    assert res.returncode == 0, res.stdout + res.stderr
    assert "Took the maintenance claim" in res.stdout, res.stdout


def _adopted_section(stdout: str) -> str:
    return stdout.split("--- Adopted", 1)[1].split("--- Skipped", 1)[0]


def _rows_present(d: _Deployment, arns: list[str]) -> int:
    """How many of `arns` have a `backup_index` row, read at the catalog
    (a command holding SQLite's serve lock does not stop the read)."""
    ids = tuple(_backup_id(a) for a in arns)
    if d.backend == "mongodb":
        return _mongosh(
            f"print(db.backup_index.countDocuments({{account_id: {json.dumps(ACCOUNT)}, "
            f"backup_id: {{$in: {json.dumps(list(ids))}}}}}))"
        )
    marks = ", ".join("?" for _ in ids)
    statement = f"SELECT count(*) FROM backup_index WHERE account_id = ? AND backup_id IN ({marks})"
    if d.backend == "sqlite":
        try:
            import pysqlite3 as sqlite3
        except ImportError:
            import sqlite3
        conn = sqlite3.connect(d.catalog, timeout=10)
        try:
            return conn.execute(statement, (ACCOUNT, *ids)).fetchone()[0]
        finally:
            conn.close()
    import psycopg2

    conn = psycopg2.connect(d.catalog)
    try:
        with conn.cursor() as cur:
            cur.execute(statement.replace("?", "%s"), (ACCOUNT, *ids))
            return cur.fetchone()[0]
    finally:
        conn.close()


def test_sigterm_during_the_prefix_loop_stops_sync_and_keeps_its_rows(deployment, tmp_path):
    """Two backups whose rows the catalog lost sort first under the root,
    followed by many copies of the first under later backup ids (each read
    and skipped: its manifest names another backup id). SIGTERM once both
    rows are back, while sync is still in the prefix loop: the command exits
    1 saying it stopped, the rows it inserted stay AVAILABLE, no
    missing-manifest section is examined, and the next sync runs at once
    (on PostgreSQL it takes the claim the first released) and adopts
    nothing more."""
    _sync_cell(deployment)
    d = deployment
    import signal
    import subprocess

    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, first = _small_backed_up_table(ddb, "sigterm")
        second = ddb.create_backup(TableName=name, BackupName="second")["BackupDetails"]["BackupArn"]
        _wait_backup_available(ddb, second, timeout=60)
    finally:
        server.stop()
    for arn in (first, second):
        _drop_row(d, arn)
    src = _prefix(d, name, first)
    millis, suffix = max(_backup_id(first), _backup_id(second)).split("-")
    copies = 10000
    for i in range(copies):
        shutil.copytree(src, os.path.join(d.root, ACCOUNT, name, f"{int(millis) + 1 + i:014d}-{suffix}"),
                        copy_function=os.link)
    out_path = str(tmp_path / "sync.out")
    with open(out_path, "wb") as out:
        sync = subprocess.Popen(
            [EXTENDDB_BINARY, "backup", "--config", d.config, "sync"],
            cwd=d.cwd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        )
    try:
        deadline = time.monotonic() + 60
        while _rows_present(d, [first, second]) < 2:
            assert sync.poll() is None, open(out_path).read()[-4000:]
            assert time.monotonic() < deadline, open(out_path).read()[-4000:]
            time.sleep(0.05)
        assert sync.poll() is None, "the sync ended before the signal"
        sync.send_signal(signal.SIGTERM)
        assert sync.wait(timeout=60) == 1, open(out_path).read()[-4000:]
    finally:
        if sync.poll() is None:
            sync.kill()
            sync.wait(timeout=10)
    text = open(out_path).read()
    print("\n".join(line for line in text.splitlines() if not line.startswith("  "))[-3000:])
    assert "stopped: SIGTERM received. 2 backup(s) were adopted before the stop" in text, text[-4000:]
    assert "  (not examined)" in text, text[-4000:]
    assert "=== extenddb backup sync complete" not in text, text[-4000:]
    rows = _rows(d)
    assert rows[first] == "AVAILABLE" and rows[second] == "AVAILABLE", rows
    started = time.monotonic()
    res = d.run("backup", "sync")
    took = time.monotonic() - started
    out = res.stdout + res.stderr
    assert res.returncode == 0, out[-4000:]
    if _beside(d):
        assert "Took the maintenance claim" in res.stdout, res.stdout[:2000]
    assert _adopted_section(res.stdout).splitlines()[1:-1] == ["  (none)"], res.stdout[:2000]
    assert f"0 backup(s) adopted, {copies + 2} prefix(es) skipped" in res.stdout, res.stdout[-2000:]
    print(f"the next sync: exit 0 in {took:.1f} s")


def test_prune_missing_on_a_host_local_root_deletes_no_object(deployment, tmp_path):
    """Host-local roots on two instances of one catalog, one path: backups
    K1 < K2 < K3, and the second host's root lacks K2 (its bytes are on the
    first host), while it holds the newest, K3, so its store is healthy.
    `--prune-missing` there removes K2's row outright and deletes nothing.
    Back on the first host, the reconciler's cycles leave K2's prefix whole
    (no tombstone names it), DescribeBackup answers BackupNotFound, and a
    plain sync beside the server adopts K2 again, after which it is
    AVAILABLE. A SQLite catalog has one instance, so the cell is
    PostgreSQL's."""
    _sync_cell(deployment)
    d = deployment
    if not _beside(d):
        pytest.skip("host-local roots need two instances of one catalog")
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, k1 = _small_backed_up_table(ddb, "hostlocal")
        k2 = ddb.create_backup(TableName=name, BackupName="second")["BackupDetails"]["BackupArn"]
        _wait_backup_available(ddb, k2, timeout=60)
        k3 = ddb.create_backup(TableName=name, BackupName="third")["BackupDetails"]["BackupArn"]
        _wait_backup_available(ddb, k3, timeout=60)
    finally:
        server.stop()
    k2_tree = _tree(_prefix(d, name, k2))
    host1 = d.root + ".host1"
    os.rename(d.root, host1)
    try:
        shutil.copytree(host1, d.root)
        shutil.rmtree(_prefix(d, name, k2))
        res = d.run("backup", "sync", "--prune-missing")
        out = res.stdout + res.stderr
        print(out)
        assert res.returncode == 0, out
        assert f"  {k2}: removed: the row is gone, and no object was deleted" in res.stdout, out
        rows = _rows(d)
        assert k2 not in rows and rows[k1] == "AVAILABLE" and rows[k3] == "AVAILABLE", rows
    finally:
        shutil.rmtree(d.root, ignore_errors=True)
        os.rename(host1, d.root)
    server, ddb, creds = _started(d, str(tmp_path / "serve2.log"))
    try:
        time.sleep(5)  # several reconciler cycles (reconcile_interval_secs = 1)
        assert _tree(_prefix(d, name, k2)) == k2_tree, "a deletion duty acted on K2's bytes"
        with pytest.raises(ClientError) as gone:
            ddb.describe_backup(BackupArn=k2)
        assert gone.value.response["Error"]["Code"] == "BackupNotFoundException", gone.value.response
        res = d.run("backup", "sync")
        assert res.returncode == 0, res.stdout + res.stderr
        assert _adopted_section(res.stdout).splitlines()[1:-1] == [f"  {k2}"], res.stdout
        details = ddb.describe_backup(BackupArn=k2)["BackupDescription"]["BackupDetails"]
        assert details["BackupStatus"] == "AVAILABLE" and details["BackupName"] == "second", details
    finally:
        server.stop()


def test_sync_is_refused_while_purge_holds_the_claim(deployment, tmp_path):
    """Beside servers every sync takes the maintenance claim, so no sync
    runs while purge does. A CREATING row whose job died after its manifest
    (its prefix complete, its heartbeat an hour old) and a CREATING row whose
    job beat just now: `purge --confirm` takes the claim and waits for the
    live job, and while it holds the claim a plain sync fails at once (exit
    1, one maintenance command at a time) and writes nothing. The job ends;
    purge removes the dead row first and then fails to delete its prefix
    (made read-only), reports it left and exits 1. The prefix is then a
    complete backup with no row, which a later sync adopts and the next
    purge deletes. SQLite runs both commands under the serve lock."""
    _sync_cell(deployment)
    d = deployment
    if not _beside(d):
        pytest.skip("on SQLite the serve lock orders sync and purge")
    import subprocess

    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, dead = _small_backed_up_table(ddb, "gap")
        live = ddb.create_backup(TableName=name, BackupName="live")["BackupDetails"]["BackupArn"]
        _wait_backup_available(ddb, live, timeout=60)
    finally:
        server.stop()
    _set_status(d, dead, "CREATING", beat_age_secs=3600)
    _set_status(d, live, "CREATING")
    dead_prefix = _prefix(d, name, dead)
    os.chmod(dead_prefix, 0o500)
    out_path = str(tmp_path / "purge.out")
    with open(out_path, "wb") as out:
        purge = subprocess.Popen(
            [EXTENDDB_BINARY, "backup", "--config", d.config, "purge", "--confirm"],
            cwd=d.cwd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        )
    try:
        deadline = time.monotonic() + 60
        while "Waiting for running jobs to finish" not in open(out_path).read():
            assert purge.poll() is None, open(out_path).read()
            assert time.monotonic() < deadline, open(out_path).read()
            time.sleep(0.2)
        before = _rows(d)
        started = time.monotonic()
        res = d.run("backup", "sync")
        took = time.monotonic() - started
        out = res.stdout + res.stderr
        print(f"sync beside purge: exit {res.returncode} in {took:.1f} s: {out.strip().splitlines()[-1]}")
        assert res.returncode == 1, out
        assert "one maintenance command runs at a time" in out, out
        assert "--- Adopted" not in res.stdout, out
        assert took < 15, f"sync took {took:.1f} s to refuse"
        assert _rows(d) == before
        _set_status(d, live, "AVAILABLE")
        assert purge.wait(timeout=90) == 1, open(out_path).read()
        text = open(out_path).read()
        print(text)
        assert f"left: {dead}" in text, text
    finally:
        if purge.poll() is None:
            purge.kill()
            purge.wait(timeout=10)
        os.chmod(dead_prefix, 0o700)
    assert _rows(d) == {}, "purge removed both rows"
    assert os.path.isfile(os.path.join(dead_prefix, "extenddb-manifest.json"))
    res = d.run("backup", "sync")
    assert res.returncode == 0, res.stdout + res.stderr
    assert _adopted_section(res.stdout).splitlines()[1:-1] == [f"  {dead}"], res.stdout
    res = d.run("backup", "purge", "--confirm")
    assert res.returncode == 0, res.stdout + res.stderr
    assert not os.path.exists(dead_prefix)


@pytest.fixture()
def other_catalog(request, tmp_path, deployment):
    """A second catalog of the build's backend (its own `init`), whose
    `[backup.store]` names the first deployment's root; its databases are
    dropped after the test."""
    _sync_cell(deployment)
    if deployment.backend == "mongodb":
        pytest.skip("a second MongoDB catalog needs a replica set of its own (the catalog database's name is fixed)")
    base = os.path.join(os.path.realpath(str(tmp_path)), "other")
    os.mkdir(base)
    store = (
        "[backup]\nreconcile_interval_secs = 1\n"
        f'[backup.store]\nkind = "filesystem"\n[backup.store.filesystem]\npath = "{deployment.root}"\n'
    )
    if deployment.backend == "sqlite":
        o = _SqliteDeployment(base)
        _append(o.config, store)
        yield _Deployment("sqlite", o.config, o.port, deployment.root, o.cert, base, o.db_path)
        return
    db_name = f"extenddb_test_{uuid.uuid4().hex[:8]}_catalog"
    from urllib.parse import urlparse

    parsed = urlparse(lh.PG_CONN)
    env = {"db_name": db_name, "pg_host": parsed.hostname or "localhost", "pg_port": str(parsed.port or 5432)}
    config = os.path.join(base, "extenddb.toml")
    port = _free_port()
    try:
        result = lh._run_extenddb(
            "init", *lh._init_args(env), config=config,
            env_override={"EXTENDDB_ADMIN_PASSWORD": ADMIN_PASSWORD},
        )
        assert result.returncode == 0, result.stdout + result.stderr
        lh._patch_config_port(config, port)
        _append(config, store)
        cert = os.path.join(os.path.expanduser("~/.extenddb/tls"), "cert.pem")
        yield _Deployment("postgres", config, port, deployment.root, cert, base, f"{lh.PG_CONN}/{db_name}")
    finally:
        lh._run_extenddb("destroy", "--yes", *lh._pg_args(), config=config, check=False, timeout=30)
        lh._drop_database(db_name)
        lh._drop_database(db_name[: -len("_catalog")])


def test_adopt_store_makes_this_catalog_the_owner(deployment, other_catalog, tmp_path):
    """A second catalog configured with the first deployment's root: plain
    sync refuses the root another catalog owns, naming `--adopt-store`, and
    leaves the identity file as it was. `--adopt-store` takes the claim,
    rewrites the file's owner (the store id and creation time kept), logs
    the audit line and adopts the root's backup; the first catalog's
    server finds the store owned by another catalog at its next check (on
    PostgreSQL the server kept running beside the adoption; on SQLite it was
    stopped for the serve lock and the check is its restart's), and its own
    sync now refuses. The second catalog's server describes, lists and
    restores the adopted backup."""
    _sync_cell(deployment)
    d, o = deployment, other_catalog
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _small_backed_up_table(ddb, "adopt")
        if d.backend == "sqlite":
            server.stop()
        identity = os.path.join(d.root, "extenddb-store.json")
        with open(identity) as f:
            before = json.load(f)
        res = o.run("backup", "sync")
        out = res.stdout + res.stderr
        assert res.returncode == 1, out
        assert "is owned by another catalog" in out and "run with --adopt-store to make this catalog its owner" in out, out
        # A configured root: the sentence about the shared default root is
        # not the operator's.
        assert "The default root beside a SQLite database file" not in out, out
        with open(identity) as f:
            assert json.load(f) == before, "the refused sync changed the identity file"

        res = o.run("backup", "sync", "--adopt-store")
        out = res.stdout + res.stderr
        assert res.returncode == 0, out
        assert 'Took the maintenance claim as "extenddb backup sync on ' in res.stdout, res.stdout
        assert f"Adopted the root: extenddb-store.json named catalog {before['catalog_id']} and now names" in res.stdout
        assert f"  {arn}" in res.stdout and "1 backup(s) adopted" in res.stdout, res.stdout
        with open(identity) as f:
            after = json.load(f)
        assert after["catalog_id"] != before["catalog_id"], (before, after)
        assert after["store_id"] == before["store_id"] and after["created_at_ms"] == before["created_at_ms"], (before, after)
        assert after["backend"] == o.backend and after.get("ephemeral_catalog") is False, after
        assert "extenddb::audit" in out and before["catalog_id"] in out and after["catalog_id"] in out, out

        if d.backend == "sqlite":
            server = _Server(d.config, str(tmp_path / "serve-previous-owner.log"))
        health = _wait_health(
            d.port,
            lambda h: h.get("components", {}).get("backup_store", {}).get("reason") == "owned by another catalog",
            server=server,
        )
        assert health["components"]["backup_store"]["status"] == "unhealthy", health
        print(f"branch: {d.backend}: the previous owner's store: {health['components']['backup_store']}")
    finally:
        server.stop()
    res = d.run("backup", "sync")
    assert res.returncode == 1 and "is owned by another catalog" in res.stdout + res.stderr, res.stdout + res.stderr

    server, ddb, creds = _started(o, str(tmp_path / "serve-other.log"))
    try:
        details = ddb.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]
        assert details["BackupStatus"] == "AVAILABLE" and details["BackupName"] == "nightly", details
        assert arn in [s["BackupArn"] for s in ddb.list_backups()["BackupSummaries"]]
        target = f"{name}-r"
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        _wait_active(ddb, target)
        assert _count(ddb, target) == HAND_BUILT_ITEMS
    finally:
        server.stop()


def test_the_previous_owner_deletes_nothing_under_an_adopted_root(deployment, other_catalog, tmp_path):
    """The first catalog's server keeps running, its next check an hour
    away (`reconcile_interval_secs = 3600`), while a second catalog adopts
    its root with `--adopt-store`. Its `/health` still reports the store
    healthy, as its last check left it; DeleteBackup of the adopted backup
    there is refused as an unhealthy store's, its deletion fence having
    re-read the identity file before the row was changed: the backup's
    bytes stay whole and DescribeBackup there still answers AVAILABLE;
    `/health`
    then reports the store owned by another catalog at once. The second
    catalog's server finds its store healthy and the backup AVAILABLE, and
    restores it. SQLite stops the first server for the serve lock, so the
    cell is PostgreSQL's."""
    _sync_cell(deployment)
    d, o = deployment, other_catalog
    if d.backend != "postgres":
        pytest.skip("the previous owner runs beside the adoption on PostgreSQL")
    with open(d.config) as f:
        text = f.read()
    assert text.count("reconcile_interval_secs = 1\n") == 1, text
    with open(d.config, "w") as f:
        f.write(text.replace("reconcile_interval_secs = 1\n", "reconcile_interval_secs = 3600\n"))
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _small_backed_up_table(ddb, "fence")
        before = _tree(_prefix(d, name, arn))
        res = o.run("backup", "sync", "--adopt-store")
        out = res.stdout + res.stderr
        assert res.returncode == 0, out
        assert f"  {arn}" in res.stdout and "1 backup(s) adopted" in res.stdout, out
        assert "Stop every such server before an adoption" in res.stdout, out
        health = _health(d.port)
        assert health["components"]["backup_store"]["status"] == "healthy", health
        with pytest.raises(ClientError) as refused:
            ddb.delete_backup(BackupArn=arn)
        error = refused.value.response["Error"]
        print(f"DeleteBackup on the previous owner: {error}")
        assert error["Code"] == "ValidationException", error
        assert "unhealthy (owned by another catalog)" in error["Message"], error
        assert _tree(_prefix(d, name, arn)) == before, "the previous owner deleted bytes"
        details = ddb.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]
        assert details["BackupStatus"] == "AVAILABLE", f"the refused request moved the row: {details}"
        assert _rows(d)[arn] == "AVAILABLE"
        component = _health(d.port)["components"]["backup_store"]
        assert component["status"] == "unhealthy", component
        assert component["reason"] == "owned by another catalog", component
        log = server.log_text()
        assert "refused before the row was changed; nothing was deleted" in log, log[-4000:]
    finally:
        server.stop()

    server, ddb, creds = _started(o, str(tmp_path / "serve-other.log"))
    try:
        details = ddb.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]
        assert details["BackupStatus"] == "AVAILABLE", details
        target = f"{name}-r"
        ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)
        _wait_active(ddb, target)
        assert _count(ddb, target) == HAND_BUILT_ITEMS
    finally:
        server.stop()


def test_a_prefix_the_store_cannot_read_leaves_the_sync_incomplete(deployment, tmp_path):
    """A backup whose row the catalog lost, its manifest made unreadable
    (mode 000): `extenddb backup sync` reports the prefix as one the store
    could not be read for and exits 1 saying the run is incomplete, with no
    row inserted. Once the mode is restored, the next sync adopts the
    backup and exits 0."""
    _sync_cell(deployment)
    d = deployment
    if os.geteuid() == 0:
        pytest.skip("a mode-000 file is readable by root")
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _small_backed_up_table(ddb, "unreadable")
    finally:
        server.stop()
    _drop_row(d, arn)
    manifest = os.path.join(_prefix(d, name, arn), "extenddb-manifest.json")
    os.chmod(manifest, 0o000)
    try:
        res = d.run("backup", "sync")
        out = res.stdout + res.stderr
        print(f"branch: {d.backend}: {out.strip().splitlines()[-1]}")
        assert res.returncode == 1, out
        assert f"  {_key_prefix(name, arn)}: the store could not be read: " in res.stdout, out
        assert "the run is incomplete: 1 prefix(es) were skipped for a store or catalog error" in out, out
        assert "=== extenddb backup sync incomplete: 0 backup(s) adopted, 1 prefix(es) skipped," in res.stdout, out
        assert "=== extenddb backup sync complete" not in res.stdout, out
        assert _adopted_section(res.stdout).splitlines()[1:-1] == ["  (none)"], res.stdout
        assert arn not in _rows(d)
    finally:
        os.chmod(manifest, 0o600)
    res = d.run("backup", "sync")
    out = res.stdout + res.stderr
    assert res.returncode == 0, out
    assert "=== extenddb backup sync complete: 1 backup(s) adopted" in res.stdout, out
    assert _adopted_section(res.stdout).splitlines()[1:-1] == [f"  {arn}"], res.stdout
    assert _rows(d)[arn] == "AVAILABLE"


def _respell_manifest(prefix: str, region: str, other: str) -> None:
    """Rewrite the prefix's manifest as a server of region `other` wrote
    it (its backup ARN and source table ARN in that region), with a sidecar
    that matches."""
    path = os.path.join(prefix, "extenddb-manifest.json")
    with open(path) as f:
        manifest = json.load(f)
    for key in ("backup_arn",):
        assert f":{region}:" in manifest[key], manifest[key]
        manifest[key] = manifest[key].replace(f":{region}:", f":{other}:", 1)
    table_arn = manifest["source_table"]["table_arn"]
    manifest["source_table"]["table_arn"] = table_arn.replace(f":{region}:", f":{other}:", 1)
    data = json.dumps(manifest, indent=2).encode() + b"\n"
    with open(path, "wb") as f:
        f.write(data)
    with open(os.path.join(prefix, "extenddb-manifest.sha256"), "w") as f:
        f.write(f"{hashlib.sha256(data).hexdigest()}  extenddb-manifest.json\n")


def test_sync_adopts_another_regions_prefix_under_this_servers_region(deployment, tmp_path):
    """Two backups whose rows the catalog lost, their manifests rewritten as
    a server of another region writes them (the prefix copied from such a
    server): sync adopts both, exit 0, with a warning naming the manifest's
    region, and the rows carry this server's region. ListBackups, with and
    without `TableName`, lists them in this server's region; DescribeBackup
    answers either spelling; a restore named by the other region's spelling
    reaches ACTIVE with every item, and DeleteBackup by that spelling
    deletes the backup. The manifests keep their spelling."""
    _sync_cell(deployment)
    d = deployment
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, first = _small_backed_up_table(ddb, "region")
        second = ddb.create_backup(TableName=name, BackupName="second")["BackupDetails"]["BackupArn"]
        _wait_backup_available(ddb, second, timeout=60)
    finally:
        server.stop()
    region = first.split(":")[3]
    other = "us-west-2" if region != "us-west-2" else "eu-west-1"
    for arn in (first, second):
        _drop_row(d, arn)
        _respell_manifest(_prefix(d, name, arn), region, other)
    spelled = {arn: arn.replace(f":{region}:", f":{other}:", 1) for arn in (first, second)}

    res = d.run("backup", "sync")
    out = res.stdout + res.stderr
    print(f"branch: {d.backend}\n{res.stdout}")
    assert res.returncode == 0, out
    note = f"whose manifest spells its ARN in region {other}, not this server's"
    assert sorted(_adopted_section(res.stdout).splitlines()[1:-1]) == sorted(
        f"  {a} ({note})" for a in (first, second)
    ), out
    warned = [line for line in out.splitlines() if "WARN" in line and "backup sync: adopted " in line]
    assert len(warned) == 2 and all(note in line for line in warned), out
    rows = _rows(d)
    assert rows[first] == "AVAILABLE" and rows[second] == "AVAILABLE", rows
    assert not any(f":{other}:" in arn for arn in rows), rows
    for arn in (first, second):
        with open(os.path.join(_prefix(d, name, arn), "extenddb-manifest.json")) as f:
            assert json.load(f)["backup_arn"] == spelled[arn], "the manifest kept its spelling"

    server, ddb, creds = _started(d, str(tmp_path / "serve2.log"))
    try:
        for kwargs in ({}, {"TableName": name}):
            listed = sorted(s["BackupArn"] for s in ddb.list_backups(**kwargs)["BackupSummaries"])
            assert set((first, second)) <= set(listed), (kwargs, listed)
            assert not any(f":{other}:" in a for a in listed), (kwargs, listed)
        for spelling in (first, spelled[first]):
            details = ddb.describe_backup(BackupArn=spelling)["BackupDescription"]["BackupDetails"]
            assert details["BackupStatus"] == "AVAILABLE" and details["BackupName"] == "nightly", details
            print(f"DescribeBackup {spelling}: BackupArn {details['BackupArn']}")
        target = f"{name}-r"
        resp = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=spelled[first])["TableDescription"]
        assert resp["TableStatus"] == "CREATING", resp
        _wait_active(ddb, target)
        assert _count(ddb, target) == HAND_BUILT_ITEMS
        deleted = ddb.delete_backup(BackupArn=spelled[second])["BackupDescription"]["BackupDetails"]
        assert deleted["BackupStatus"] == "DELETED", deleted
        with pytest.raises(ClientError) as gone:
            ddb.describe_backup(BackupArn=second)
        assert gone.value.response["Error"]["Code"] == "BackupNotFoundException", gone.value.response
    finally:
        server.stop()


def _foreign_identity(d: _Deployment) -> dict:
    """Rewrite the root's identity file to name a catalog that does not
    exist (the root another catalog owned, a catalog since destroyed),
    keeping the store id; returns the file as written."""
    path = os.path.join(d.root, "extenddb-store.json")
    with open(path) as f:
        file = json.load(f)
    file = dict(file, catalog_id=str(uuid.uuid4()))
    staged = path + ".staged"
    _private_file(staged, (json.dumps(file, indent=2) + "\n").encode())
    os.rename(staged, path)
    return file


def test_adopt_store_checks_the_root_before_it_writes(deployment, tmp_path):
    """A root whose identity file names another catalog over this catalog's
    two backups. With the newest backup's manifest gone, `--adopt-store`
    refuses, exit 1, saying the root would be unhealthy once this catalog
    owns it and that nothing was written: the file is unchanged and no
    audit line says a rewrite started. With the manifest back, the same
    command adopts the root (the audit lines, the file naming this catalog
    with the store id kept), syncs, and plain sync then runs."""
    _sync_cell(deployment)
    d = deployment
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, older = _small_backed_up_table(ddb, "adoptcheck")
        newest = ddb.create_backup(TableName=name, BackupName="second")["BackupDetails"]["BackupArn"]
        _wait_backup_available(ddb, newest, timeout=60)
    finally:
        server.stop()
    identity = os.path.join(d.root, "extenddb-store.json")
    with open(identity) as f:
        ours = json.load(f)
    foreign = _foreign_identity(d)
    with open(identity, "rb") as f:
        foreign_bytes = f.read()
    manifest = os.path.join(_prefix(d, name, newest), "extenddb-manifest.json")
    os.rename(manifest, manifest + ".aside")
    try:
        res = d.run("backup", "sync", "--adopt-store")
        out = res.stdout + res.stderr
        print(f"branch: {d.backend}: {out.strip().splitlines()[-1]}")
        assert res.returncode == 1, out
        assert "the root would be unhealthy once this catalog owns it" in out, out
        assert f"nothing was written, and extenddb-store.json still names catalog {foreign['catalog_id']}" in out, out
        assert "rewriting extenddb-store.json to name this catalog" not in out, out
        with open(identity, "rb") as f:
            assert f.read() == foreign_bytes, "the refused adoption wrote the identity file"
        assert _rows(d)[newest] == "AVAILABLE"
    finally:
        os.rename(manifest + ".aside", manifest)
    res = d.run("backup", "sync", "--adopt-store")
    out = res.stdout + res.stderr
    assert res.returncode == 0, out
    assert f"Adopted the root: extenddb-store.json named catalog {foreign['catalog_id']} and now names" in res.stdout, out
    assert "extenddb::audit" in out and "rewriting extenddb-store.json to name this catalog" in out, out
    with open(identity) as f:
        after = json.load(f)
    assert after["catalog_id"] == ours["catalog_id"] and after["store_id"] == ours["store_id"], (ours, after)
    res = d.run("backup", "sync")
    assert res.returncode == 0, res.stdout + res.stderr


def test_a_plain_sync_whose_root_is_adopted_during_it_ends_with_exit_1(deployment, tmp_path):
    """A plain sync of a root this catalog owns, holding one backup whose
    row the catalog lost and many copies of it under later backup ids (each
    read and skipped). Once the backup's row is back and while the sync is
    still in the prefix loop, the identity file is rewritten to name
    another catalog, as an adopter's put would leave it. The sync's last
    read of the file finds it: exit 1, naming the catalog and the store id,
    and no `complete` line."""
    _sync_cell(deployment)
    d = deployment
    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _small_backed_up_table(ddb, "plainrace")
    finally:
        server.stop()
    _drop_row(d, arn)
    src = _prefix(d, name, arn)
    millis, suffix = _backup_id(arn).split("-")
    copies = 10000
    for i in range(copies):
        shutil.copytree(src, os.path.join(d.root, ACCOUNT, name, f"{int(millis) + 1 + i:014d}-{suffix}"),
                        copy_function=os.link)
    with open(os.path.join(d.root, "extenddb-store.json")) as f:
        store_id = json.load(f)["store_id"]
    out_path = str(tmp_path / "sync.out")
    with open(out_path, "wb") as out:
        sync = subprocess.Popen(
            [EXTENDDB_BINARY, "backup", "--config", d.config, "sync"],
            cwd=d.cwd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        )
    try:
        deadline = time.monotonic() + 60
        while _rows_present(d, [arn]) < 1:
            assert sync.poll() is None, open(out_path).read()[-4000:]
            assert time.monotonic() < deadline, open(out_path).read()[-4000:]
            time.sleep(0.05)
        foreign = _foreign_identity(d)
        assert sync.poll() is None, "the sync ended before the rewrite"
        assert sync.wait(timeout=180) == 1, open(out_path).read()[-4000:]
    finally:
        if sync.poll() is None:
            sync.kill()
            sync.wait(timeout=10)
    text = open(out_path).read()
    print("\n".join(line for line in text.splitlines() if not line.startswith("  "))[-3000:])
    assert (
        f"after the sync, extenddb-store.json names catalog {foreign['catalog_id']} (store id {store_id}): "
        "another catalog adopted the root during the sync"
    ) in text, text[-4000:]
    assert "=== extenddb backup sync complete" not in text, text[-4000:]


def test_an_adopter_whose_put_lands_during_the_sync_ends_it_with_exit_1(deployment, other_catalog, tmp_path):
    """A second catalog adopts the first one's root with `--adopt-store`,
    the root holding one backup and many copies of it under later backup
    ids (each read and skipped: its manifest names another backup id). Once
    the command has printed its adoption and while its sync is still in the
    prefix loop, the identity file is rewritten to name a third catalog
    under the same store id, as a second adopter's put would leave it. The
    command's last read of the file finds it: exit 1, naming the third
    catalog and the store id. The first server is stopped throughout."""
    _sync_cell(deployment)
    d, o = deployment, other_catalog
    import subprocess

    server, ddb, creds = _started(d, str(tmp_path / "serve.log"))
    try:
        name, arn = _small_backed_up_table(ddb, "race")
    finally:
        server.stop()
    src = _prefix(d, name, arn)
    millis, suffix = _backup_id(arn).split("-")
    copies = 10000
    for i in range(copies):
        shutil.copytree(src, os.path.join(d.root, ACCOUNT, name, f"{int(millis) + 1 + i:014d}-{suffix}"),
                        copy_function=os.link)
    identity = os.path.join(d.root, "extenddb-store.json")
    with open(identity) as f:
        before = json.load(f)
    out_path = str(tmp_path / "adopt.out")
    with open(out_path, "wb") as out:
        adopting = subprocess.Popen(
            [EXTENDDB_BINARY, "backup", "--config", o.config, "sync", "--adopt-store"],
            cwd=o.cwd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        )
    try:
        deadline = time.monotonic() + 60
        while "Adopted the root:" not in open(out_path).read():
            assert adopting.poll() is None, open(out_path).read()[-4000:]
            assert time.monotonic() < deadline, open(out_path).read()[-4000:]
            time.sleep(0.05)
        third = dict(before, catalog_id=str(uuid.uuid4()))
        staged = identity + ".staged"
        _private_file(staged, (json.dumps(third, indent=2) + "\n").encode())
        os.rename(staged, identity)
        assert "--- Adopted" not in open(out_path).read(), "the sync had ended before the rewrite"
        assert adopting.poll() is None, "the command ended before the rewrite"
        assert adopting.wait(timeout=180) == 1, open(out_path).read()[-4000:]
    finally:
        if adopting.poll() is None:
            adopting.kill()
            adopting.wait(timeout=10)
    text = open(out_path).read()
    print("\n".join(line for line in text.splitlines() if not line.startswith("  "))[-3000:])
    assert (
        f"after the sync, extenddb-store.json names catalog {third['catalog_id']} (store id {before['store_id']}): "
        "another catalog adopted the root during the sync"
    ) in text, text[-4000:]
    assert f"  {arn}" in _adopted_section(text), text[-4000:]
    assert "=== extenddb backup sync complete" not in text, text[-4000:]
