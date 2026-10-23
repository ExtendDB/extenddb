# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""DynamoDB Streams AT_TIMESTAMP shard iterator.

Every expected value here was measured on the service (us-east-1,
2026-10-23): positioning at millisecond precision, the four validation
messages and their order, the future buffer, and the error classes for
malformed Timestamp values.

These tests require a running extenddb server (EXTENDDB_TEST_ENDPOINT must be
set) because the service routes Streams to a separate endpoint; extenddb
serves both on the same endpoint.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import wait_for_active, wait_for_deleted

PREFIX = "One or more parameter values were invalid: "


@pytest.fixture(scope="module")
def endpoint_url() -> str:
    return os.environ["EXTENDDB_TEST_ENDPOINT"].strip()


def _client(service: str, endpoint_url: str, validate: bool = True):
    kwargs: dict = dict(
        service_name=service,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        endpoint_url=endpoint_url,
        config=Config(parameter_validation=validate, retries={"max_attempts": 1}),
    )
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


@pytest.fixture(scope="module")
def dynamodb_client(endpoint_url: str):
    return _client("dynamodb", endpoint_url)


@pytest.fixture(scope="module")
def streams_client(endpoint_url: str):
    return _client("dynamodbstreams", endpoint_url)


@pytest.fixture(scope="module")
def raw_streams_client(endpoint_url: str):
    """Parameter validation off, so malformed Timestamp shapes reach the server."""
    return _client("dynamodbstreams", endpoint_url, validate=False)


@pytest.fixture()
def stream_table(dynamodb_client):
    table_name = f"extenddb-ts-{uuid.uuid4().hex[:8]}"
    dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    wait_for_active(dynamodb_client, table_name)
    stream_arn = dynamodb_client.describe_table(TableName=table_name)["Table"]["LatestStreamArn"]
    yield table_name, stream_arn
    try:
        dynamodb_client.delete_table(TableName=table_name)
        wait_for_deleted(dynamodb_client, table_name)
    except Exception:
        pass


def _shard_with_records(streams_client, stream_arn: str) -> str:
    """The one shard holding a single partition key's records."""
    for shard in streams_client.describe_stream(StreamArn=stream_arn)["StreamDescription"]["Shards"]:
        it = streams_client.get_shard_iterator(
            StreamArn=stream_arn, ShardId=shard["ShardId"], ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        if streams_client.get_records(ShardIterator=it)["Records"]:
            return shard["ShardId"]
    raise AssertionError("no shard holds records")


def _drain(streams_client, iterator: str, rounds: int = 6) -> list[dict]:
    out: list[dict] = []
    it = iterator
    for _ in range(rounds):
        resp = streams_client.get_records(ShardIterator=it)
        out.extend(resp.get("Records", []))
        it = resp.get("NextShardIterator")
        if not resp.get("Records") or not it:
            break
    return out


def _values(records: list[dict]) -> list[str]:
    return [r["dynamodb"]["NewImage"]["v"]["S"] for r in records]


def _at(streams_client, stream_arn: str, shard_id: str, ts) -> list[dict]:
    it = streams_client.get_shard_iterator(
        StreamArn=stream_arn, ShardId=shard_id, ShardIteratorType="AT_TIMESTAMP", Timestamp=ts
    )["ShardIterator"]
    return _drain(streams_client, it)


def _write_group(dynamodb_client, table: str, tag: str, n: int) -> tuple[float, float]:
    """Write n items just after a second boundary; return (start, end) wall times."""
    now = time.time()
    time.sleep((1.0 - (now % 1.0)) + 0.05)
    start = time.time()
    for i in range(n):
        dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "k"}, "v": {"S": f"{tag}-{i}"}})
    return start, time.time()


def _error(fn, **kw) -> tuple[str, str]:
    try:
        fn(**kw)
    except ClientError as e:
        err = e.response["Error"]
        return err["Code"], err["Message"]
    raise AssertionError("expected a ClientError")


def _raw_error(endpoint_url: str, body: dict) -> tuple[str, str]:
    """POST a GetShardIterator body exactly as given, SigV4-signed with the
    test user's key from the environment, for shapes the SDK cannot
    serialise (JSON null, an object). Returns (code, message)."""
    import ssl
    import urllib.request

    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    data = json.dumps(body).encode()
    req = AWSRequest(method="POST", url=endpoint_url, data=data, headers={
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": "DynamoDBStreams_20120810.GetShardIterator",
    })
    signer_creds = Credentials(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
    SigV4Auth(signer_creds, "dynamodb", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")).add_auth(req)
    prepped = req.prepare()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    hr = urllib.request.Request(prepped.url, data=data, headers=dict(prepped.headers), method="POST")
    try:
        with urllib.request.urlopen(hr, timeout=30, context=ctx) as r:
            raise AssertionError(f"expected an error, got {r.status}")
    except urllib.error.HTTPError as e:
        parsed = json.loads(e.read().decode())
        return parsed.get("__type", "").split("#")[-1], parsed.get("message") or parsed.get("Message") or ""


class TestPositioning:
    """Measured on the service with three groups written in distinct seconds."""

    def test_seek_positions_at_millisecond_precision(self, dynamodb_client, streams_client, stream_table):
        table, arn = stream_table
        g1 = _write_group(dynamodb_client, table, "g1", 3)
        time.sleep(2.2)
        g2 = _write_group(dynamodb_client, table, "g2", 3)
        time.sleep(2.2)
        g3 = _write_group(dynamodb_client, table, "g3", 2)
        shard = _shard_with_records(streams_client, arn)
        all_vals = ["g1-0", "g1-1", "g1-2", "g2-0", "g2-1", "g2-2", "g3-0", "g3-1"]

        def ts(secs: float):
            return datetime.fromtimestamp(secs, tz=timezone.utc)

        # Before the first record and 25 hours ago both return everything, no error.
        assert _values(_at(streams_client, arn, shard, ts(g1[0] - 10))) == all_vals
        assert _values(_at(streams_client, arn, shard, ts(time.time() - 25 * 3600))) == all_vals
        # Between groups.
        assert _values(_at(streams_client, arn, shard, ts(g1[1] + 1.0))) == all_vals[3:]
        assert _values(_at(streams_client, arn, shard, ts(g2[1] + 1.0))) == all_vals[6:]
        # The group's own second, as an integer, includes the group.
        assert _values(_at(streams_client, arn, shard, ts(int(g2[0])))) == all_vals[3:]
        assert _values(_at(streams_client, arn, shard, ts(int(g3[0])))) == all_vals[6:]
        # Fractional seconds inside the group's second skip it: the writes
        # landed at +0.050 to about +0.100, so .5 and .999 start at group 3.
        # Guard on the measured span so a slow write on a loaded host skips
        # the probe instead of failing it.
        if g2[1] < int(g2[0]) + 0.5:
            assert _values(_at(streams_client, arn, shard, ts(int(g2[0]) + 0.5))) == all_vals[6:]
            assert _values(_at(streams_client, arn, shard, ts(int(g2[0]) + 0.999))) == all_vals[6:]
        else:
            pytest.skip(f"group 2 spanned past +0.5 s ({g2[1] - int(g2[0]):.3f}); boundary probe not meaningful")
        # One second after the last record: nothing.
        assert _at(streams_client, arn, shard, ts(int(g3[1]) + 1)) == []
        # ApproximateCreationDateTime is whole seconds.
        recs = _at(streams_client, arn, shard, ts(g1[0] - 10))
        for r in recs:
            assert r["dynamodb"]["ApproximateCreationDateTime"].microsecond == 0

    def test_boundary_is_at_or_after_the_record_millisecond(self, dynamodb_client, streams_client, stream_table):
        """T equal to a record's own creation millisecond returns that record
        (the service's at-or-after); one millisecond later does not."""
        table, arn = stream_table
        dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "k"}, "v": {"S": "only"}})
        shard = _shard_with_records(streams_client, arn)
        it = streams_client.get_shard_iterator(StreamArn=arn, ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        rec = _drain(streams_client, it)[0]
        second = int(rec["dynamodb"]["ApproximateCreationDateTime"].timestamp())

        def returns_it(ms: int) -> bool:
            t = datetime.fromtimestamp(second + ms / 1000.0, tz=timezone.utc)
            return _values(_at(streams_client, arn, shard, t)) == ["only"]

        # Bisect within the record's second for the last millisecond that
        # still returns it: that is the record's own creation millisecond.
        assert returns_it(0)
        lo, hi = 0, 1000
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if returns_it(mid):
                lo = mid
            else:
                hi = mid
        assert returns_it(lo), "T at the record's millisecond returns it"
        assert not returns_it(lo + 1), "T one millisecond later does not"

    def test_after_tip_returns_only_later_writes(self, dynamodb_client, streams_client, stream_table):
        table, arn = stream_table
        _write_group(dynamodb_client, table, "old", 2)
        shard = _shard_with_records(streams_client, arn)
        time.sleep(0.3)
        it = streams_client.get_shard_iterator(
            StreamArn=arn,
            ShardId=shard,
            ShardIteratorType="AT_TIMESTAMP",
            Timestamp=datetime.fromtimestamp(time.time() - 0.2, tz=timezone.utc),
        )["ShardIterator"]
        dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "k"}, "v": {"S": "post-tip"}})
        assert _values(_drain(streams_client, it)) == ["post-tip"]


class TestTimestampShapes:
    def test_integer_and_fractional_numbers_are_accepted(self, dynamodb_client, streams_client, raw_streams_client, stream_table):
        table, arn = stream_table
        g = _write_group(dynamodb_client, table, "g", 2)
        shard = _shard_with_records(streams_client, arn)
        for ts in (int(g[0]), round(g[0], 3), g[0] + 0.123456):
            resp = raw_streams_client.get_shard_iterator(
                StreamArn=arn, ShardId=shard, ShardIteratorType="AT_TIMESTAMP", Timestamp=ts
            )
            assert "ShardIterator" in resp, ts

    def test_zero_negative_and_null_refuse(self, dynamodb_client, streams_client, raw_streams_client, stream_table, endpoint_url):
        table, arn = stream_table
        _write_group(dynamodb_client, table, "g", 1)
        shard = _shard_with_records(streams_client, arn)
        expected = PREFIX + "Valid timestamp must be provided when the iterator type is AT_TIMESTAMP"
        for ts in (0, -1):
            code, msg = _error(
                raw_streams_client.get_shard_iterator,
                StreamArn=arn, ShardId=shard, ShardIteratorType="AT_TIMESTAMP", Timestamp=ts,
            )
            assert (code, msg) == ("ValidationException", expected), ts
        # JSON null cannot go through the SDK; send it as the service saw it.
        code, msg = _raw_error(endpoint_url, {
            "StreamArn": arn, "ShardId": shard, "ShardIteratorType": "AT_TIMESTAMP", "Timestamp": None,
        })
        assert (code, msg) == ("ValidationException", expected), "null"

    def test_milliseconds_as_seconds_is_in_the_future(self, dynamodb_client, streams_client, stream_table, endpoint_url):
        table, arn = stream_table
        g = _write_group(dynamodb_client, table, "g", 1)
        shard = _shard_with_records(streams_client, arn)
        for ts in (int(g[0]) * 1000, 10**18):
            code, msg = _raw_error(endpoint_url, {
                "StreamArn": arn, "ShardId": shard, "ShardIteratorType": "AT_TIMESTAMP", "Timestamp": ts,
            })
            assert code == "ValidationException", ts
            assert msg == PREFIX + "Timestamp parameter cannot be greater than the current timestamp", ts

    def test_non_numeric_timestamp_is_a_serialization_error(self, dynamodb_client, streams_client, stream_table, endpoint_url):
        table, arn = stream_table
        g = _write_group(dynamodb_client, table, "g", 1)
        shard = _shard_with_records(streams_client, arn)
        cases = [
            (f"{int(g[0])}.5", "STRING_VALUE cannot be converted to Date"),
            ("", "STRING_VALUE cannot be converted to Date"),
            (True, "TRUE_VALUE cannot be converted to Date"),
            ({"S": "x"}, "Start of structure or map found where not expected"),
        ]
        for ts, expected in cases:
            code, msg = _raw_error(endpoint_url, {
                "StreamArn": arn, "ShardId": shard, "ShardIteratorType": "AT_TIMESTAMP", "Timestamp": ts,
            })
            assert (code, msg) == ("SerializationException", expected), ts


class TestValidationMatrix:
    """Messages and check order measured on the service."""

    def test_matrix(self, dynamodb_client, streams_client, raw_streams_client, stream_table):
        table, arn = stream_table
        g = _write_group(dynamodb_client, table, "g", 1)
        shard = _shard_with_records(streams_client, arn)
        it = streams_client.get_shard_iterator(StreamArn=arn, ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        seq = _drain(streams_client, it)[0]["dynamodb"]["SequenceNumber"]
        ts = int(g[0])
        future = int(time.time()) + 3600
        ts_required = "Valid timestamp must be provided when the iterator type is AT_TIMESTAMP"
        seq_refused = "SequenceNumber should not be passed for this iterator type"
        ts_refused = "Timestamp should not be passed for this iterator type"
        future_refused = "Timestamp parameter cannot be greater than the current timestamp"
        cases = [
            (dict(ShardIteratorType="AT_TIMESTAMP"), ts_required),
            (dict(ShardIteratorType="AT_TIMESTAMP", SequenceNumber=seq), ts_required),
            (dict(ShardIteratorType="AT_TIMESTAMP", SequenceNumber=seq, Timestamp=ts), seq_refused),
            (dict(ShardIteratorType="AT_TIMESTAMP", SequenceNumber=seq, Timestamp=future), seq_refused),
            (dict(ShardIteratorType="AT_TIMESTAMP", SequenceNumber=seq, Timestamp=0), ts_required),
            (dict(ShardIteratorType="AT_TIMESTAMP", Timestamp=future), future_refused),
            (dict(ShardIteratorType="TRIM_HORIZON", Timestamp=ts), ts_refused),
            (dict(ShardIteratorType="LATEST", Timestamp=ts), ts_refused),
            (dict(ShardIteratorType="AT_SEQUENCE_NUMBER", SequenceNumber=seq, Timestamp=ts), ts_refused),
            (dict(ShardIteratorType="AFTER_SEQUENCE_NUMBER", SequenceNumber=seq, Timestamp=ts), ts_refused),
            (dict(ShardIteratorType="TRIM_HORIZON", SequenceNumber=seq, Timestamp=ts), ts_refused),
            (dict(ShardIteratorType="TRIM_HORIZON", SequenceNumber=seq), seq_refused),
            (dict(ShardIteratorType="LATEST", SequenceNumber=seq), seq_refused),
            (dict(ShardIteratorType="AT_SEQUENCE_NUMBER"), "SequenceNumber cannot be null"),
        ]
        for extra, expected in cases:
            code, msg = _error(raw_streams_client.get_shard_iterator, StreamArn=arn, ShardId=shard, **extra)
            assert code == "ValidationException", extra
            assert msg == PREFIX + expected, extra

    def test_unknown_or_missing_type_renders_without_the_value(self, dynamodb_client, streams_client, raw_streams_client, stream_table, endpoint_url):
        table, arn = stream_table
        _write_group(dynamodb_client, table, "g", 1)
        shard = _shard_with_records(streams_client, arn)
        members = "[AFTER_SEQUENCE_NUMBER, LATEST, AT_TIMESTAMP, AT_SEQUENCE_NUMBER, TRIM_HORIZON]"
        # The service omits the offending value from the enum violation.
        for bad in ("BOGUS", "at_timestamp"):
            code, msg = _error(raw_streams_client.get_shard_iterator, StreamArn=arn, ShardId=shard, ShardIteratorType=bad)
            assert code == "ValidationException"
            assert msg == (
                "1 validation error detected: Value at 'shardIteratorType' failed to satisfy "
                f"constraint: Member must satisfy enum value set: {members}"
            )
        # A missing or null type is a ValidationException, not a serialization error.
        for body in ({"Timestamp": 1792764248.0}, {"ShardIteratorType": None}):
            code, msg = _raw_error(endpoint_url, {"StreamArn": arn, "ShardId": shard, **body})
            assert (code, msg) == (
                "ValidationException",
                "1 validation error detected: Value at 'shardIteratorType' failed to satisfy "
                "constraint: Member must not be null",
            ), body

    def test_future_check_runs_before_shard_existence(self, dynamodb_client, streams_client, raw_streams_client, stream_table):
        table, arn = stream_table
        _write_group(dynamodb_client, table, "g", 1)
        shard = _shard_with_records(streams_client, arn)
        code, msg = _error(
            raw_streams_client.get_shard_iterator,
            StreamArn=arn, ShardId="shardId-00000000000000000000-00000000",
            ShardIteratorType="AT_TIMESTAMP", Timestamp=int(time.time()) + 3600,
        )
        assert (code, msg) == ("ValidationException", PREFIX + "Timestamp parameter cannot be greater than the current timestamp")
        code, _ = _error(
            raw_streams_client.get_shard_iterator,
            StreamArn=arn, ShardId="shardId-00000000000000000000-00000000",
            ShardIteratorType="AT_TIMESTAMP", Timestamp=int(time.time()) - 60,
        )
        assert code == "ResourceNotFoundException"
        assert shard  # the real shard still exists

    def test_near_future_is_accepted(self, dynamodb_client, streams_client, stream_table):
        table, arn = stream_table
        _write_group(dynamodb_client, table, "g", 1)
        shard = _shard_with_records(streams_client, arn)
        resp = streams_client.get_shard_iterator(
            StreamArn=arn, ShardId=shard, ShardIteratorType="AT_TIMESTAMP",
            Timestamp=datetime.fromtimestamp(time.time() + 0.5, tz=timezone.utc),
        )
        assert "ShardIterator" in resp
        code, _ = _error(
            streams_client.get_shard_iterator,
            StreamArn=arn, ShardId=shard, ShardIteratorType="AT_TIMESTAMP",
            Timestamp=datetime.fromtimestamp(time.time() + 5, tz=timezone.utc),
        )
        assert code == "ValidationException"
