# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Disabling a table's stream closes it; enabling it again creates a new one.

One test per row measured on Amazon DynamoDB on 2026-10-24 (us-east-1, client
retries off), on a table with a NEW_AND_OLD_IMAGES stream and three items. The
module fixture runs the whole sequence once, in the order it was measured, and
records every answer; each test asserts one row.

Runs against ExtendDB (DYNAMODB_ENDPOINT) and against the service (streams
client on its default endpoint). On the service the disable completes
asynchronously, so the fixture waits for DISABLED.
"""

from __future__ import annotations

import os
import time

import boto3
import botocore.config
import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

NEW_AND_OLD = {"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"}
ITEMS = ["k0", "k1", "k2"]


def _streams_client(endpoint_url):
    kwargs: dict = {
        "service_name": "dynamodbstreams",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": botocore.config.Config(retries={"max_attempts": 0}),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            ca_cert = os.environ.get("EXTENDDB_CA_CERT", "")
            kwargs["verify"] = ca_cert if ca_cert else False
    return boto3.client(**kwargs)


def _call(fn, **kwargs) -> dict:
    """The response without its metadata, or the error's status, code and message."""
    try:
        resp = fn(**kwargs)
        resp.pop("ResponseMetadata", None)
        return {"status": 200, "response": resp}
    except ClientError as e:
        return {
            "status": e.response["ResponseMetadata"]["HTTPStatusCode"],
            "code": e.response["Error"]["Code"],
            "message": e.response["Error"]["Message"],
        }


def _stream(streams, arn: str, **kwargs) -> dict:
    return streams.describe_stream(StreamArn=arn, **kwargs)["StreamDescription"]


def _wait_disabled(streams, arn: str, timeout: float = 120.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        desc = _stream(streams, arn)
        if desc["StreamStatus"] == "DISABLED" or time.monotonic() > deadline:
            return desc
        time.sleep(3)


def _iterator(streams, arn: str, shard: str, kind: str, seq: str | None = None) -> dict:
    kwargs = {"StreamArn": arn, "ShardId": shard, "ShardIteratorType": kind}
    if seq is not None:
        kwargs["SequenceNumber"] = seq
    return _call(streams.get_shard_iterator, **kwargs)


def _reread(streams, arn: str, shards: list[str]) -> dict:
    """Every record of every shard, read from a fresh TRIM_HORIZON iterator."""
    out = {}
    for shard in shards:
        it = _iterator(streams, arn, shard, "TRIM_HORIZON")["response"]["ShardIterator"]
        got = []
        for _ in range(50):
            page = streams.get_records(ShardIterator=it, Limit=1000)
            got += [r["dynamodb"]["SequenceNumber"] for r in page["Records"]]
            it = page.get("NextShardIterator")
            if not it or not page["Records"]:
                break
        out[shard] = got
    return out


def _closed_shard_page_ends(page: dict) -> None:
    """The page that reaches the end of a closed shard omits NextShardIterator.

    That is PR #372's rule (a closed and drained shard ends the iterator);
    until it lands ExtendDB keeps answering an iterator, so on ExtendDB this
    one assertion is an expected failure. Every other assertion of the row
    stays unconditional.
    """
    if "NextShardIterator" not in page["response"]:
        return
    if os.environ.get("DYNAMODB_ENDPOINT", "").strip():
        pytest.xfail("closed shard terminal GetRecords page: PR #372")
    raise AssertionError(f"NextShardIterator present on a closed shard's final page: {page}")


def _iterate(streams, arn: str, shard: str, kind: str, seq: str | None = None):
    """GetShardIterator, then one GetRecords with the iterator it issued."""
    it = _iterator(streams, arn, shard, kind, seq)
    if it["status"] != 200:
        return it, None
    return it, _call(streams.get_records, ShardIterator=it["response"]["ShardIterator"])


@pytest.fixture(scope="module")
def run(dynamodb_client, endpoint_url):
    """Run the measured sequence once and record every answer by row name."""
    ddb = dynamodb_client
    streams = _streams_client(endpoint_url)
    name = unique_name("t482")
    nostream = unique_name("x482-nostream")
    key = {
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
    }
    ddb.create_table(TableName=name, StreamSpecification=NEW_AND_OLD, **key)
    ddb.create_table(TableName=nostream, **key)
    wait_for_active(ddb, name)
    wait_for_active(ddb, nostream)
    rows: dict = {"name": name, "nostream": nostream}
    try:
        table = ddb.describe_table(TableName=name)["Table"]
        rows["describe_enabled"] = table
        old_arn = table["LatestStreamArn"]
        rows["old_arn"] = old_arn
        for pk in ITEMS:
            ddb.put_item(TableName=name, Item={"pk": {"S": pk}, "v": {"S": pk}})
        shards = [s["ShardId"] for s in _stream(streams, old_arn)["Shards"]]
        rows["old_shards"] = shards
        rows["records"] = {}
        rows["pre_disable_iterators"] = {}
        for shard in shards:
            it = _iterator(streams, old_arn, shard, "TRIM_HORIZON")["response"]["ShardIterator"]
            got = streams.get_records(ShardIterator=it, Limit=1000)["Records"]
            rows["records"][shard] = [r["dynamodb"]["SequenceNumber"] for r in got]
            rows["pre_disable_iterators"][shard] = _iterator(
                streams, old_arn, shard, "TRIM_HORIZON"
            )["response"]["ShardIterator"]

        for view in ["NEW_AND_OLD_IMAGES", "KEYS_ONLY"]:
            rows[f"enable_while_enabled_{view}"] = _call(
                ddb.update_table,
                TableName=name,
                StreamSpecification={"StreamEnabled": True, "StreamViewType": view},
            )
        rows["disable_never_streamed"] = _call(
            ddb.update_table, TableName=nostream, StreamSpecification={"StreamEnabled": False}
        )

        rows["disable"] = _call(
            ddb.update_table, TableName=name, StreamSpecification={"StreamEnabled": False}
        )
        wait_for_active(ddb, name)
        rows["describe_disabled"] = ddb.describe_table(TableName=name)["Table"]
        rows["stream_disabled"] = _wait_disabled(streams, old_arn)
        rows["disable_while_disabled"] = _call(
            ddb.update_table, TableName=name, StreamSpecification={"StreamEnabled": False}
        )

        rows["latest"] = {}
        rows["pre_disable_reads"] = {}
        rows["after_last"] = {}
        rows["at_ending"] = {}
        for s in rows["stream_disabled"]["Shards"]:
            shard = s["ShardId"]
            rows["latest"][shard] = _iterate(streams, old_arn, shard, "LATEST")
            rows["pre_disable_reads"][shard] = _call(
                streams.get_records, ShardIterator=rows["pre_disable_iterators"][shard]
            )
            if rows["records"][shard]:
                last = rows["records"][shard][-1]
                rows["after_last"][shard] = _iterate(
                    streams, old_arn, shard, "AFTER_SEQUENCE_NUMBER", last
                )
                # Absent on a server that does not close the shard; the row
                # for the ending number reports that on its own.
                ending = s["SequenceNumberRange"].get("EndingSequenceNumber", last)
                rows["at_ending"][shard] = _iterate(
                    streams, old_arn, shard, "AT_SEQUENCE_NUMBER", ending
                )

        page1 = _stream(streams, old_arn, Limit=2)
        page2 = _stream(
            streams, old_arn, Limit=2, ExclusiveStartShardId=page1.get("LastEvaluatedShardId", "")
        )
        rows["paged"] = (page1, page2)

        # A write while the stream is disabled records nothing (0 of 1 on the
        # service); every old shard is re-read from TRIM_HORIZON afterwards.
        ddb.put_item(TableName=name, Item={"pk": {"S": "while-disabled"}})
        rows["old_records_after_disabled_write"] = _reread(streams, old_arn, shards)

        rows["reenable"] = _call(ddb.update_table, TableName=name, StreamSpecification=NEW_AND_OLD)
        wait_for_active(ddb, name)
        rows["describe_reenabled"] = ddb.describe_table(TableName=name)["Table"]
        new_arn = rows["describe_reenabled"]["LatestStreamArn"]
        rows["new_arn"] = new_arn
        rows["stream_new"] = _stream(streams, new_arn)
        ddb.put_item(TableName=name, Item={"pk": {"S": "after"}})
        rows["stream_old_after_write"] = _stream(streams, old_arn)
        rows["old_records_after_reenable_write"] = _reread(streams, old_arn, shards)

        rows["list_all"] = streams.list_streams()["Streams"]
        rows["list_table"] = streams.list_streams(TableName=name)["Streams"]
        new_shard = rows["stream_new"]["Shards"][0]["ShardId"]
        rows["cross_old_arn"] = _iterator(streams, old_arn, new_shard, "TRIM_HORIZON")
        rows["cross_new_arn"] = _iterator(streams, new_arn, shards[0], "TRIM_HORIZON")
        yield rows
    finally:
        for t in (name, nostream):
            try:
                ddb.delete_table(TableName=t)
                wait_for_deleted(ddb, t)
            except ClientError:
                pass


def _refused(row: dict, sentence: str) -> None:
    assert (row.get("status"), row.get("code"), row.get("message")) == (
        400,
        "ValidationException",
        sentence,
    ), row


def test_enable_while_enabled_same_view_type_is_refused(run):
    _refused(
        run["enable_while_enabled_NEW_AND_OLD_IMAGES"],
        f"Table already has an enabled stream: TableName: {run['name']}",
    )


def test_enable_while_enabled_other_view_type_is_refused(run):
    _refused(
        run["enable_while_enabled_KEYS_ONLY"],
        f"Table already has an enabled stream: TableName: {run['name']}",
    )


def test_disable_on_a_table_that_never_had_a_stream_is_refused(run):
    _refused(
        run["disable_never_streamed"],
        f"Table has no stream to disable: TableName: {run['nostream']}",
    )


def test_disable_while_disabled_is_refused(run):
    _refused(
        run["disable_while_disabled"],
        f"Table has no stream to disable: TableName: {run['name']}",
    )


def test_disable_response_omits_the_specification_and_keeps_the_latest_stream(run):
    assert run["disable"]["status"] == 200, run["disable"]
    desc = run["disable"]["response"]["TableDescription"]
    # The service answers TableStatus UPDATING here; ExtendDB applies every
    # UpdateTable change at once and answers ACTIVE, for stream changes as for
    # billing-mode changes, so the status is not asserted.
    assert "StreamSpecification" not in desc
    enabled = run["describe_enabled"]
    assert desc["LatestStreamArn"] == enabled["LatestStreamArn"]
    assert desc["LatestStreamLabel"] == enabled["LatestStreamLabel"]


def test_reenable_response_names_a_new_stream(run):
    assert run["reenable"]["status"] == 200, run["reenable"]
    desc = run["reenable"]["response"]["TableDescription"]
    assert desc["StreamSpecification"] == NEW_AND_OLD
    assert desc["LatestStreamArn"] != run["old_arn"]
    assert desc["LatestStreamLabel"] > run["describe_enabled"]["LatestStreamLabel"]


def test_describe_table_while_enabled(run):
    t = run["describe_enabled"]
    assert t["StreamSpecification"] == NEW_AND_OLD
    assert t["LatestStreamArn"].endswith("/stream/" + t["LatestStreamLabel"])


def test_describe_table_while_disabled(run):
    t = run["describe_disabled"]
    assert t["TableStatus"] == "ACTIVE"
    assert "StreamSpecification" not in t
    assert t["LatestStreamArn"] == run["old_arn"]
    assert t["LatestStreamLabel"] == run["describe_enabled"]["LatestStreamLabel"]


def test_describe_table_after_reenable(run):
    t = run["describe_reenabled"]
    assert t["StreamSpecification"] == NEW_AND_OLD
    assert t["LatestStreamArn"] == run["new_arn"] != run["old_arn"]
    assert run["new_arn"].endswith("/stream/" + t["LatestStreamLabel"])


def test_describe_stream_of_a_disabled_stream(run):
    d = run["stream_disabled"]
    assert d["StreamStatus"] == "DISABLED"
    assert d["StreamViewType"] == "NEW_AND_OLD_IMAGES"
    assert d["StreamArn"] == run["old_arn"]
    assert d["TableName"] == run["name"]
    assert d["KeySchema"] == [{"AttributeName": "pk", "KeyType": "HASH"}]
    assert "LastEvaluatedShardId" not in d
    assert [s["ShardId"] for s in d["Shards"]] == run["old_shards"]
    assert all("ParentShardId" not in s for s in d["Shards"])


def test_every_closed_shard_carries_its_ending_sequence_number(run):
    for s in run["stream_disabled"]["Shards"]:
        r = s["SequenceNumberRange"]
        written = run["records"][s["ShardId"]]
        # A shard with records ends at its last record; an empty one ends at
        # its starting number.
        expected = written[-1] if written else r["StartingSequenceNumber"]
        assert r.get("EndingSequenceNumber") == expected, (s, written)
    assert sum(len(v) for v in run["records"].values()) == len(ITEMS)


def test_describe_stream_pages_a_disabled_stream(run):
    page1, page2 = run["paged"]
    assert [s["ShardId"] for s in page1["Shards"]] == run["old_shards"][:2]
    assert page1["LastEvaluatedShardId"] == run["old_shards"][1]
    assert [s["ShardId"] for s in page2["Shards"]] == run["old_shards"][2:]
    assert "LastEvaluatedShardId" not in page2


def test_latest_on_a_closed_shard_reads_an_empty_final_page(run):
    for shard, (it, page) in run["latest"].items():
        assert it["status"] == 200, (shard, it)
        assert page["status"] == 200, (shard, page)
        assert page["response"]["Records"] == [], (shard, page)
        _closed_shard_page_ends(page)


def test_an_iterator_from_before_the_disable_reads_to_the_end(run):
    for shard, page in run["pre_disable_reads"].items():
        assert page["status"] == 200, (shard, page)
        got = [r["dynamodb"]["SequenceNumber"] for r in page["response"]["Records"]]
        assert got == run["records"][shard], (shard, page)
        _closed_shard_page_ends(page)


def test_after_the_last_record_of_a_closed_shard_is_an_empty_final_page(run):
    assert run["after_last"], "no shard held a record"
    for shard, (it, page) in run["after_last"].items():
        assert it["status"] == 200, (shard, it)
        assert page["status"] == 200, (shard, page)
        assert page["response"]["Records"] == [], (shard, page)
        _closed_shard_page_ends(page)


def test_at_the_ending_number_reads_the_last_record_and_ends(run):
    assert run["at_ending"], "no shard held a record"
    for shard, (it, page) in run["at_ending"].items():
        assert it["status"] == 200, (shard, it)
        assert page["status"] == 200, (shard, page)
        got = [r["dynamodb"]["SequenceNumber"] for r in page["response"]["Records"]]
        assert got == run["records"][shard][-1:], (shard, page)
        _closed_shard_page_ends(page)


def test_a_write_while_the_stream_is_disabled_records_nothing(run):
    assert run["old_records_after_disabled_write"] == run["records"]


def test_reenable_creates_a_new_stream_with_new_shards(run):
    new = run["stream_new"]
    assert new["StreamStatus"] == "ENABLED"
    assert new["StreamArn"] == run["new_arn"]
    assert new["StreamLabel"] > run["describe_enabled"]["LatestStreamLabel"]
    ids = [s["ShardId"] for s in new["Shards"]]
    assert ids and not set(ids) & set(run["old_shards"])
    assert all("EndingSequenceNumber" not in s["SequenceNumberRange"] for s in new["Shards"])


def test_the_old_stream_is_unchanged_after_reenable_and_a_write(run):
    before, after = run["stream_disabled"], run["stream_old_after_write"]
    assert after["StreamStatus"] == "DISABLED"
    assert after["Shards"] == before["Shards"]
    # The records of every old shard, read afresh, are the pre-disable set.
    assert run["old_records_after_reenable_write"] == run["records"]


def test_list_streams_lists_both_streams_newest_first(run):
    expected = [
        {"StreamArn": arn, "TableName": run["name"], "StreamLabel": arn.rsplit("/", 1)[1]}
        for arn in (run["new_arn"], run["old_arn"])
    ]
    assert run["list_table"] == expected
    # The account-wide call is paged at 100 streams; only the table's own ARNs
    # are asserted, when the page reached them.
    own = [s for s in run["list_all"] if s["TableName"] == run["name"]]
    assert own in (expected, []), own


def test_a_shard_of_the_other_stream_does_not_exist(run):
    for row in (run["cross_old_arn"], run["cross_new_arn"]):
        assert row == {
            "status": 400,
            "code": "ResourceNotFoundException",
            "message": "Requested resource not found: Shard does not exist",
        }, row
