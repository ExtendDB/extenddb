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

The binary at `EXTENDDB_BINARY` decides which deployment runs: a SQLite build,
a PostgreSQL build (with `EXTENDDB_TEST_PG_CONNECTION_STRING`) or a MongoDB
build (with `EXTENDDB_TEST_MONGODB_URI`, a replica set of this suite's own: the
catalog's database names are fixed, so each test destroys the previous test's
catalog and initializes its own). Excluded from the backend-agnostic suite; run
by name.
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
    _pg_build,
    _provision,
    _Server,
    _sqlite_build,
    _SqliteDeployment,
    _wait_backup_available,
    _wait_health,
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
    """A deployment with a filesystem store, its config and its root."""

    def __init__(self, backend: str, config: str, port: int, root: str, cert: str, cwd: str):
        self.backend = backend
        self.config = config
        self.port = port
        self.root = root
        self.cert = cert
        self.cwd = cwd

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
    else:
        pytest.skip("EXTENDDB_BINARY is neither a SQLite, a PostgreSQL nor a MongoDB build")
    os.mkdir(os.path.join(base, ACCOUNT))
    _append(
        config,
        "[backup]\nreconcile_interval_secs = 1\nmax_concurrent_jobs = 1\n"
        + store
        + f'[import]\npaths = ["{base}"]\n',
    )
    return _Deployment(backend, config, port, root, cert, base)


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
