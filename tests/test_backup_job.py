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

The binary at `EXTENDDB_BINARY` decides which deployment runs: a SQLite build,
a PostgreSQL build (with `EXTENDDB_TEST_PG_CONNECTION_STRING`) or a MongoDB
build (with `EXTENDDB_TEST_MONGODB_URI`, a replica set of this suite's own: the
catalog's database names are fixed, so each test destroys the previous test's
catalog and initializes its own); the restore cells run on the SQLite and
PostgreSQL deployments. Excluded from the backend-agnostic suite; run by name.
"""

from __future__ import annotations

import json
import os
import re
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
