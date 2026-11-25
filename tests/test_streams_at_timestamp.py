# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""DynamoDB Streams AT_TIMESTAMP shard iterator.

Every expected value here was measured on the service (us-east-1,
2026-10-23 and 2026-11-20): positioning at millisecond precision with the
Timestamp rounded half up, the validation messages and their order, the
future buffer, and the error classes for malformed member values.

Dual-target: with EXTENDDB_TEST_ENDPOINT set, both clients use that endpoint;
without it, the streams client and the raw requests go to the service's
Streams endpoint of the region.
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


REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


def _client(service: str, endpoint_url: str | None, validate: bool = True):
    kwargs: dict = dict(
        service_name=service,
        region_name=REGION,
        config=Config(parameter_validation=validate, retries={"max_attempts": 1}),
    )
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
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


def _raw_error(endpoint_url: str | None, body: dict) -> tuple[str, str]:
    """POST a GetShardIterator body exactly as given, SigV4-signed with the
    session's credentials, for shapes the SDK cannot serialise (JSON null, an
    object, a member of the wrong type). Returns (code, message); the message
    is "" when the answer carries none."""
    import ssl
    import urllib.request

    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    url = endpoint_url or f"https://streams.dynamodb.{REGION}.amazonaws.com/"
    data = json.dumps(body).encode()
    req = AWSRequest(method="POST", url=url, data=data, headers={
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": "DynamoDBStreams_20120810.GetShardIterator",
    })
    creds = (boto3.DEFAULT_SESSION or boto3.Session()).get_credentials().get_frozen_credentials()
    SigV4Auth(creds, "dynamodb", REGION).add_auth(req)
    prepped = req.prepare()
    ctx = ssl.create_default_context()
    if endpoint_url:
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
        # One second after the last record: nothing.
        assert _at(streams_client, arn, shard, ts(int(g3[1]) + 1)) == []
        # ApproximateCreationDateTime is whole seconds.
        recs = _at(streams_client, arn, shard, ts(g1[0] - 10))
        for r in recs:
            assert r["dynamodb"]["ApproximateCreationDateTime"].microsecond == 0
        # Fractional seconds inside the group's second skip it: the writes
        # landed at +0.050 to about +0.100, so .5 and .999 start at group 3.
        # Last, and guarded on the measured span, so a slow write on a loaded
        # host skips this probe alone instead of failing it.
        if g2[1] >= int(g2[0]) + 0.5:
            pytest.skip(f"group 2 spanned past +0.5 s ({g2[1] - int(g2[0]):.3f}); boundary probe not meaningful")
        assert _values(_at(streams_client, arn, shard, ts(int(g2[0]) + 0.5))) == all_vals[6:]
        assert _values(_at(streams_client, arn, shard, ts(int(g2[0]) + 0.999))) == all_vals[6:]

    def test_boundary_is_at_or_after_the_record_millisecond(self, dynamodb_client, streams_client, raw_streams_client, stream_table):
        """T equal to a record's own creation millisecond returns that record
        (the service's at-or-after); one millisecond later does not. The
        record's millisecond lies in the write's wall-clock window, so a
        store that keeps whole seconds fails here: the write lands mid-second."""
        table, arn = stream_table
        now = time.time()
        time.sleep((1.3 - (now % 1.0)) % 1.0)
        start = time.time()
        dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "k"}, "v": {"S": "only"}})
        end = time.time()
        assert 0.25 < start % 1.0 < 0.75, f"the write did not land mid-second ({start:.3f})"
        shard = _shard_with_records(streams_client, arn)
        it = streams_client.get_shard_iterator(StreamArn=arn, ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        rec = _drain(streams_client, it)[0]
        second = int(rec["dynamodb"]["ApproximateCreationDateTime"].timestamp())
        assert second == int(start), "ApproximateCreationDateTime is the write's second"

        def returns_it(ms: int) -> bool:
            # A raw number, so the fraction reaches the server as written.
            resp = raw_streams_client.get_shard_iterator(
                StreamArn=arn, ShardId=shard, ShardIteratorType="AT_TIMESTAMP", Timestamp=second + ms / 1000.0
            )
            return _values(_drain(streams_client, resp["ShardIterator"])) == ["only"]

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
        # The record's millisecond is inside the write's window (50 ms of
        # clock skew allowed). A seek at a fraction before the window returns
        # the record, one past the window does not.
        window = (int(start * 1000) - second * 1000, int(end * 1000) - second * 1000)
        assert window[0] - 50 <= lo <= window[1] + 50, f"record ms {lo} outside the write window {window}"
        assert returns_it(window[0] - 50)
        assert not returns_it(window[1] + 51)

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
        for ts in (int(g[0]), round(g[0], 3), g[0] + 0.123456, 0.0005, 0.00051):
            resp = raw_streams_client.get_shard_iterator(
                StreamArn=arn, ShardId=shard, ShardIteratorType="AT_TIMESTAMP", Timestamp=ts
            )
            assert "ShardIterator" in resp, ts

    def test_zero_negative_and_null_refuse(self, dynamodb_client, streams_client, raw_streams_client, stream_table, endpoint_url):
        """The Timestamp rounds to a millisecond first: 0.0004 and 0.00049
        round to zero and are refused (0.0005 is accepted above)."""
        table, arn = stream_table
        _write_group(dynamodb_client, table, "g", 1)
        shard = _shard_with_records(streams_client, arn)
        expected = PREFIX + "Valid timestamp must be provided when the iterator type is AT_TIMESTAMP"
        for ts in (0, -1, 0.0004, 0.00049, -0.0004):
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

    def test_ill_typed_members_are_serialization_errors(self, dynamodb_client, streams_client, stream_table, endpoint_url):
        """A member of the wrong JSON type is refused before the enum, null and
        length checks, with the deserializer's text; SequenceNumber is read
        before Timestamp; an array has no message."""
        table, arn = stream_table
        g = _write_group(dynamodb_client, table, "g", 1)
        shard = _shard_with_records(streams_client, arn)
        date_s = "STRING_VALUE cannot be converted to Date"
        structure = "Start of structure or map found where not expected"
        cases = [
            ({"ShardIteratorType": "AT_TIMESTAMP", "Timestamp": f"{int(g[0])}.5"}, date_s),
            ({"ShardIteratorType": "AT_TIMESTAMP", "Timestamp": ""}, date_s),
            ({"ShardIteratorType": "AT_TIMESTAMP", "Timestamp": True}, "TRUE_VALUE cannot be converted to Date"),
            ({"ShardIteratorType": "AT_TIMESTAMP", "Timestamp": {"S": "x"}}, structure),
            ({"ShardIteratorType": "AT_TIMESTAMP", "Timestamp": []}, ""),
            ({"ShardIteratorType": "BOGUS", "Timestamp": "1792764248"}, date_s),
            ({"Timestamp": "1792764248"}, date_s),
            ({"ShardIteratorType": "BOGUS", "SequenceNumber": 5}, "NUMBER_VALUE cannot be converted to String"),
            ({"ShardIteratorType": "TRIM_HORIZON", "SequenceNumber": True}, "TRUE_VALUE cannot be converted to String"),
            ({"ShardIteratorType": "TRIM_HORIZON", "SequenceNumber": {}}, structure),
            ({"ShardIteratorType": "AT_TIMESTAMP", "SequenceNumber": 5, "Timestamp": "x"}, "NUMBER_VALUE cannot be converted to String"),
        ]
        for extra, expected in cases:
            code, msg = _raw_error(endpoint_url, {"StreamArn": arn, "ShardId": shard, **extra})
            assert (code, msg) == ("SerializationException", expected), extra
        # The type check comes before the ShardId length constraint too, and
        # before the StreamArn check (measured 2026-11-23 with no StreamArn
        # and with an empty one).
        for body in (
            {"StreamArn": arn, "ShardId": "shardId-short", "ShardIteratorType": "AT_TIMESTAMP", "Timestamp": "x"},
            {"Timestamp": "x"},
        ):
            code, msg = _raw_error(endpoint_url, body)
            assert (code, msg) == ("SerializationException", date_s), body
        code, msg = _raw_error(endpoint_url, {"StreamArn": "", "SequenceNumber": 5})
        assert (code, msg) == ("SerializationException", "NUMBER_VALUE cannot be converted to String")


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
            (dict(ShardIteratorType="AFTER_SEQUENCE_NUMBER"), "SequenceNumber cannot be null"),
            # The missing SequenceNumber is refused before the Timestamp.
            (dict(ShardIteratorType="AT_SEQUENCE_NUMBER", Timestamp=ts), "SequenceNumber cannot be null"),
            (dict(ShardIteratorType="AFTER_SEQUENCE_NUMBER", Timestamp=ts), "SequenceNumber cannot be null"),
        ]
        for extra, expected in cases:
            code, msg = _error(raw_streams_client.get_shard_iterator, StreamArn=arn, ShardId=shard, **extra)
            assert code == "ValidationException", extra
            assert msg == PREFIX + expected, extra

    def test_positional_types_check_the_shard_before_refusing_a_sequence_number(self, dynamodb_client, streams_client, raw_streams_client, stream_table):
        """On a shard that does not exist, TRIM_HORIZON and LATEST answer the
        shard before the SequenceNumber; AT_TIMESTAMP refuses the
        SequenceNumber first, and a Timestamp is refused first for every type."""
        table, arn = stream_table
        g = _write_group(dynamodb_client, table, "g", 1)
        _shard_with_records(streams_client, arn)
        missing = "shardId-00000000000000000000-ffffffff"
        seq = "000000000000000000001"
        not_found = ("ResourceNotFoundException", "Requested resource not found: Shard does not exist")
        seq_refused = ("ValidationException", PREFIX + "SequenceNumber should not be passed for this iterator type")
        ts_refused = ("ValidationException", PREFIX + "Timestamp should not be passed for this iterator type")
        cases = [
            (dict(ShardIteratorType="TRIM_HORIZON", SequenceNumber=seq), not_found),
            (dict(ShardIteratorType="LATEST", SequenceNumber=seq), not_found),
            (dict(ShardIteratorType="TRIM_HORIZON"), not_found),
            (dict(ShardIteratorType="AT_TIMESTAMP", SequenceNumber=seq, Timestamp=int(g[0])), seq_refused),
            (dict(ShardIteratorType="TRIM_HORIZON", Timestamp=int(g[0])), ts_refused),
            (dict(ShardIteratorType="LATEST", SequenceNumber=seq, Timestamp=int(g[0])), ts_refused),
            (dict(ShardIteratorType="AT_SEQUENCE_NUMBER"), ("ValidationException", PREFIX + "SequenceNumber cannot be null")),
        ]
        for extra, expected in cases:
            got = _error(raw_streams_client.get_shard_iterator, StreamArn=arn, ShardId=missing, **extra)
            assert got == expected, extra

    def test_unknown_or_missing_type_renders_without_the_value(self, dynamodb_client, streams_client, raw_streams_client, stream_table, endpoint_url):
        table, arn = stream_table
        _write_group(dynamodb_client, table, "g", 1)
        shard = _shard_with_records(streams_client, arn)
        members = "[AFTER_SEQUENCE_NUMBER, LATEST, AT_TIMESTAMP, AT_SEQUENCE_NUMBER, TRIM_HORIZON]"
        enum_clause = f"Value at 'shardIteratorType' failed to satisfy constraint: Member must satisfy enum value set: {members}"
        null_clause = "Value at 'shardIteratorType' failed to satisfy constraint: Member must not be null"
        shard_clause = "Value at 'shardId' failed to satisfy constraint: Member must have length greater than or equal to 28"
        seq_clause = "Value at 'sequenceNumber' failed to satisfy constraint: Member must have length greater than or equal to 21"
        # The service omits the offending value from the enum violation.
        for bad in ("BOGUS", "at_timestamp"):
            code, msg = _error(raw_streams_client.get_shard_iterator, StreamArn=arn, ShardId=shard, ShardIteratorType=bad)
            assert (code, msg) == ("ValidationException", f"1 validation error detected: {enum_clause}")
        # A missing or null type or shard id is a ValidationException, not a
        # serialization error, and it joins the other constraint violations in
        # one message: sequenceNumber, shardIteratorType, shardId. An absent,
        # null or empty StreamArn is refused alone, before the clauses
        # (measured 2026-11-23).
        shard_null = "Value at 'shardId' failed to satisfy constraint: Member must not be null"
        cases = [
            ({"StreamArn": arn, "ShardId": shard, "Timestamp": 1792764248.0}, f"1 validation error detected: {null_clause}"),
            ({"StreamArn": arn, "ShardId": shard, "ShardIteratorType": None}, f"1 validation error detected: {null_clause}"),
            ({"StreamArn": arn, "ShardId": "shardId-short"}, f"2 validation errors detected: {null_clause}; {shard_clause}"),
            ({"StreamArn": arn, "ShardId": "shardId-short", "ShardIteratorType": "BOGUS"}, f"2 validation errors detected: {enum_clause}; {shard_clause}"),
            ({"StreamArn": arn, "ShardId": shard, "SequenceNumber": "0"}, f"2 validation errors detected: {seq_clause}; {null_clause}"),
            ({"StreamArn": arn, "ShardId": "shardId-short", "SequenceNumber": "0"}, f"3 validation errors detected: {seq_clause}; {null_clause}; {shard_clause}"),
            ({"StreamArn": arn, "ShardId": "shardId-short", "ShardIteratorType": "TRIM_HORIZON"}, f"1 validation error detected: {shard_clause}"),
            ({"StreamArn": arn, "ShardIteratorType": "TRIM_HORIZON"}, f"1 validation error detected: {shard_null}"),
            ({"StreamArn": arn, "ShardId": None, "ShardIteratorType": "TRIM_HORIZON"}, f"1 validation error detected: {shard_null}"),
            ({"StreamArn": arn, "SequenceNumber": "0"}, f"3 validation errors detected: {seq_clause}; {null_clause}; {shard_null}"),
            ({"ShardId": shard, "ShardIteratorType": "TRIM_HORIZON"}, "Invalid StreamArn"),
            ({"ShardId": "shardId-short"}, "Invalid StreamArn"),
            ({"StreamArn": None, "ShardId": shard, "ShardIteratorType": "TRIM_HORIZON"}, "Invalid StreamArn"),
            ({"StreamArn": "", "ShardId": shard, "ShardIteratorType": "TRIM_HORIZON"}, "Invalid StreamArn"),
            ({"StreamArn": None, "SequenceNumber": "0"}, "Invalid StreamArn"),
            ({"StreamArn": "", "ShardId": "shardId-short"}, "Invalid StreamArn"),
        ]
        for body, expected in cases:
            code, msg = _raw_error(endpoint_url, body)
            assert (code, msg) == ("ValidationException", expected), body

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
        code, msg = _error(
            raw_streams_client.get_shard_iterator,
            StreamArn=arn, ShardId="shardId-00000000000000000000-00000000",
            ShardIteratorType="AT_TIMESTAMP", Timestamp=int(time.time()) - 60,
        )
        assert (code, msg) == ("ResourceNotFoundException", "Requested resource not found: Shard does not exist")
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
        code, msg = _error(
            streams_client.get_shard_iterator,
            StreamArn=arn, ShardId=shard, ShardIteratorType="AT_TIMESTAMP",
            Timestamp=datetime.fromtimestamp(time.time() + 5, tz=timezone.utc),
        )
        assert (code, msg) == ("ValidationException", PREFIX + "Timestamp parameter cannot be greater than the current timestamp")
