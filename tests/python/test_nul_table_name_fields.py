# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Table names outside the pattern in every field that carries one, beyond
`TableName` (covered by test_nul_table_names.py), and the stream, backup
and tag ARN fields with U+0000 in a segment.

Measured against the service on 2026-10-24 with SDK parameter validation off.
Every row passed against the service again on 2026-11-04 (us-east-1). Every
operation here looks its table up before its handler runs, so the name has to
be refused before any lookup; on PostgreSQL a U+0000 cannot even be bound as a
parameter. These rows pin the service's sentence and location for each field.
"""

import time

import pytest
from botocore.exceptions import ClientError

from helpers import (
    MEMBER_PATTERN,
    PITR_REFUSAL,
    assert_member_pattern,
    assert_validation,
    unique_name,
    wait_for_active,
    wait_for_deleted,
)

NUL = "\u0000"
BAD = f"t367{NUL}name"


@pytest.fixture()
def nul_table(raw_client):
    """A plain table with a GSI, and its ARN prefix up to `table/`."""
    name = unique_name("nulf")
    raw_client.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    wait_for_active(raw_client, name)
    arn = raw_client.describe_table(TableName=name)["Table"]["TableArn"]
    yield name, arn[: arn.rindex("table/") + len("table/")]
    try:
        raw_client.delete_table(TableName=name)
        wait_for_deleted(raw_client, name)
    except Exception:
        pass


class TestTableNameFieldsBeyondTableName:
    def test_restore_table_from_backup_target_table_name(self, raw_client, nul_table):
        name, _ = nul_table
        # The service refuses CreateBackup for a short while after CreateTable
        # (ContinuousBackupsUnavailableException); retry as the backup tests do.
        deadline = time.monotonic() + 120
        while True:
            try:
                arn = raw_client.create_backup(TableName=name, BackupName="bk367")["BackupDetails"]["BackupArn"]
                break
            except ClientError as e:
                if e.response["Error"]["Code"] != "ContinuousBackupsUnavailableException" or time.monotonic() >= deadline:
                    raise
                time.sleep(2)
        try:
            for bad in [BAD, "a b"]:
                with pytest.raises(ClientError) as exc:
                    raw_client.restore_table_from_backup(BackupArn=arn, TargetTableName=bad)
                assert_member_pattern(exc.value, "targetTableName", bad)
        finally:
            try:
                raw_client.delete_backup(BackupArn=arn)
            except Exception:
                pass

    def test_restore_table_to_point_in_time_names(self, raw_client, nul_table, is_extenddb):
        # The service checks each name; ExtendDB refuses every restore before
        # its names are checked, because point-in-time recovery is not in this
        # release.
        name, _ = nul_table
        for location, params in (
            ("sourceTableName", {"SourceTableName": BAD, "TargetTableName": unique_name("tgt")}),
            ("targetTableName", {"SourceTableName": name, "TargetTableName": BAD}),
        ):
            with pytest.raises(ClientError) as exc:
                raw_client.restore_table_to_point_in_time(UseLatestRestorableTime=True, **params)
            if is_extenddb:
                assert_validation(
                    exc.value, PITR_REFUSAL, code="PointInTimeRecoveryUnavailableException"
                )
            else:
                assert_member_pattern(exc.value, location, BAD)

    def test_import_table_creation_parameters_table_name(self, raw_client):
        with pytest.raises(ClientError) as exc:
            raw_client.import_table(
                S3BucketSource={"S3Bucket": "bucket"},
                InputFormat="DYNAMODB_JSON",
                TableCreationParameters={
                    "TableName": BAD,
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "BillingMode": "PAY_PER_REQUEST",
                },
            )
        assert_member_pattern(exc.value, "tableCreationParameters.tableName", BAD)

    def test_list_tables_exclusive_start_table_name(self, raw_client):
        with pytest.raises(ClientError) as exc:
            raw_client.list_tables(ExclusiveStartTableName=BAD)
        assert_member_pattern(exc.value, "exclusiveStartTableName", BAD)

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(lambda c, t: c.list_backups(TableName=t), id="ListBackups"),
            pytest.param(
                lambda c, t: c.create_backup(TableName=t, BackupName="bk367"), id="CreateBackup"
            ),
            pytest.param(lambda c, t: c.describe_time_to_live(TableName=t), id="DescribeTimeToLive"),
            pytest.param(
                lambda c, t: c.update_time_to_live(
                    TableName=t, TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"}
                ),
                id="UpdateTimeToLive",
            ),
            pytest.param(
                lambda c, t: c.describe_continuous_backups(TableName=t),
                id="DescribeContinuousBackups",
            ),
            pytest.param(
                lambda c, t: c.update_continuous_backups(
                    TableName=t,
                    PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": False},
                ),
                id="UpdateContinuousBackups-disable",
            ),
        ],
    )
    def test_other_operations_with_a_bare_table_name(self, raw_client, call):
        with pytest.raises(ClientError) as exc:
            call(raw_client, BAD)
        assert_member_pattern(exc.value, "tableName", BAD)

    def test_list_streams_has_its_own_sentence(self, raw_streams_client):
        with pytest.raises(ClientError) as exc:
            raw_streams_client.list_streams(TableName=BAD)
        assert_validation(
            exc.value,
            "TableName must be at least 3 characters long and at most 255 characters long",
        )


class TestSeveralConstraintsAtOnce:
    """A name that fails the pattern and a length bound lists both clauses,
    the length first, under `2 validation errors detected:`."""

    @pytest.mark.parametrize(
        ("name", "length_clause"),
        [
            pytest.param(NUL, "Member must have length greater than or equal to 3", id="nul-alone"),
            pytest.param(f"a{NUL}", "Member must have length greater than or equal to 3", id="two"),
            pytest.param(
                "x" * 300 + NUL, "Member must have length less than or equal to 255", id="301"
            ),
        ],
    )
    def test_describe_table(self, raw_client, name, length_clause):
        with pytest.raises(ClientError) as exc:
            raw_client.describe_table(TableName=name)
        assert_validation(
            exc.value,
            f"2 validation errors detected: Value '{name}' at 'tableName' failed to satisfy "
            f"constraint: {length_clause}; Value '{name}' at 'tableName' failed to satisfy "
            f"constraint: {MEMBER_PATTERN}",
        )


SHORT = "Member must have length greater than or equal to 3"
LONG = "Member must have length less than or equal to 255"
EMOJI = "\U0001F600"
VECTOR_UNSUPPORTED = "Vector indexes are not supported by this storage backend"

# Each member that carries an index name: its path in DynamoDB's answer, the
# operation and the request measured with it. A CreateTable names a table
# that it does not create. An UpdateTable names a table that does not exist,
# and DynamoDB refuses the name before it looks the table up.
INDEX_NAME_MEMBERS = {
    "gsi": "globalSecondaryIndexes.1.member.indexName",
    "second-gsi": "globalSecondaryIndexes.2.member.indexName",
    "lsi": "localSecondaryIndexes.1.member.indexName",
    "vector": "vectorIndexes.1.member.indexName",
    "gsi-create": "globalSecondaryIndexUpdates.1.member.create.indexName",
    "gsi-update": "globalSecondaryIndexUpdates.1.member.update.indexName",
    "gsi-delete": "globalSecondaryIndexUpdates.1.member.delete.indexName",
    "vector-create": "vectorIndexUpdates.1.member.create.indexName",
    "vector-delete": "vectorIndexUpdates.1.member.delete.indexName",
}

# Measured in us-east-1: #904 on 2026-11-06, #1021 on 2026-11-14 (the emoji
# names in a CreateTable GSI or LSI and an UpdateTable GSI Create), the other
# rows on 2026-11-18. DynamoDB lists the two clauses in either order.
INDEX_NAME_ROWS = [
    pytest.param(member, name, clauses, id=f"{member}-{label}")
    for member in INDEX_NAME_MEMBERS
    if member != "second-gsi"
    for label, name, clauses in [
        ("ab", "ab", [SHORT]),
        ("256-chars", "a" * 256, [LONG]),
        ("2-emoji", EMOJI * 2, [SHORT, MEMBER_PATTERN]),
        ("128-emoji", EMOJI * 128, [MEMBER_PATTERN]),
    ]
] + [
    pytest.param("gsi", "g i", [MEMBER_PATTERN], id="gsi-space"),
    pytest.param("second-gsi", f"g{NUL}i", [MEMBER_PATTERN], id="second-gsi-nul"),
]


def index_name_request(member: str, name: str) -> tuple[str, dict]:
    keys_only = {"ProjectionType": "KEYS_ONLY"}

    def defs(*names):
        return [{"AttributeName": n, "AttributeType": "S"} for n in names]

    def gsi(index_name):
        return {
            "IndexName": index_name,
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": keys_only,
        }

    vector = {
        "IndexName": name,
        "Dimensions": 4,
        "DistanceFunction": "COSINE",
        "VectorAttribute": {"AttributeName": "emb"},
        "Projection": {"ProjectionType": "ALL"},
    }
    table = {
        "TableName": unique_name("idxn"),
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": defs("pk", "g"),
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
    }
    missing = {"TableName": unique_name("idxn-missing")}
    if member == "gsi":
        return "create_table", {**table, "GlobalSecondaryIndexes": [gsi(name)]}
    if member == "second-gsi":
        return "create_table", {**table, "GlobalSecondaryIndexes": [gsi("gok"), gsi(name)]}
    if member == "lsi":
        return "create_table", {
            **table,
            "AttributeDefinitions": defs("pk", "s", "l"),
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "s", "KeyType": "RANGE"},
            ],
            "LocalSecondaryIndexes": [
                {
                    "IndexName": name,
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "l", "KeyType": "RANGE"},
                    ],
                    "Projection": keys_only,
                }
            ],
        }
    if member == "vector":
        return "create_table", {**table, "AttributeDefinitions": defs("pk"), "VectorIndexes": [vector]}
    if member == "gsi-create":
        return "update_table", {
            **missing,
            "AttributeDefinitions": defs("pk", "g"),
            "GlobalSecondaryIndexUpdates": [{"Create": gsi(name)}],
        }
    if member == "gsi-update":
        throughput = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
        return "update_table", {
            **missing,
            "GlobalSecondaryIndexUpdates": [
                {"Update": {"IndexName": name, "ProvisionedThroughput": throughput}}
            ],
        }
    if member == "gsi-delete":
        return "update_table", {
            **missing,
            "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": name}}],
        }
    if member == "vector-create":
        return "update_table", {**missing, "VectorIndexUpdates": [{"Create": vector}]}
    assert member == "vector-delete", member
    return "update_table", {**missing, "VectorIndexUpdates": [{"Delete": {"IndexName": name}}]}


def refusal(client, operation: str, request: dict) -> ClientError:
    """The error `operation` answers. A CreateTable that is accepted is
    deleted before the test fails."""
    try:
        getattr(client, operation)(**request)
    except ClientError as exc:
        return exc
    if operation == "create_table":
        wait_for_active(client, request["TableName"])
        client.delete_table(TableName=request["TableName"])
    pytest.fail(f"{operation} was accepted")


class TestIndexName:
    def test_query_and_scan_with_nul_index_name(self, raw_client, nul_table):
        name, _ = nul_table
        bad = f"g{NUL}2"
        with pytest.raises(ClientError) as exc:
            raw_client.query(
                TableName=name,
                IndexName=bad,
                KeyConditionExpression="gk = :v",
                ExpressionAttributeValues={":v": {"S": "a"}},
            )
        assert_member_pattern(exc.value, "indexName", bad)
        with pytest.raises(ClientError) as exc:
            raw_client.scan(TableName=name, IndexName=bad)
        assert_member_pattern(exc.value, "indexName", bad)

    def test_update_table_create_and_delete_with_nul_index_name(self, raw_client, nul_table):
        name, _ = nul_table
        bad = f"g{NUL}2"
        with pytest.raises(ClientError) as exc:
            raw_client.update_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "gk", "AttributeType": "S"}],
                GlobalSecondaryIndexUpdates=[
                    {
                        "Create": {
                            "IndexName": bad,
                            "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                            "Projection": {"ProjectionType": "ALL"},
                        }
                    }
                ],
            )
        assert_member_pattern(
            exc.value, "globalSecondaryIndexUpdates.1.member.create.indexName", bad
        )
        with pytest.raises(ClientError) as exc:
            raw_client.update_table(
                TableName=name, GlobalSecondaryIndexUpdates=[{"Delete": {"IndexName": bad}}]
            )
        assert_member_pattern(
            exc.value, "globalSecondaryIndexUpdates.1.member.delete.indexName", bad
        )

    @pytest.mark.parametrize(("member", "name", "clauses"), INDEX_NAME_ROWS)
    def test_index_name_is_refused_at_its_member_path(self, raw_client, member, name, clauses):
        operation, request = index_name_request(member, name)
        resp = refusal(raw_client, operation, request).response
        message = resp["Error"]["Message"]
        if member.startswith("vector") and message == VECTOR_UNSUPPORTED:
            pytest.skip(VECTOR_UNSUPPORTED)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 400, resp
        assert resp["Error"]["Code"] == "ValidationException", resp["Error"]
        path = INDEX_NAME_MEMBERS[member]
        parts = [f"Value '{name}' at '{path}' failed to satisfy constraint: {c}" for c in clauses]
        n = len(parts)
        head = f"{n} validation error{'s' if n > 1 else ''} detected: "
        assert message in {head + "; ".join(parts), head + "; ".join(reversed(parts))}, message


class TestStreamBackupAndTagArns:
    def test_stream_arns_with_nul(self, raw_client, raw_streams_client, nul_table):
        name, prefix = nul_table
        bad = f"{prefix}t367{NUL}name"
        table_seg = f"{bad}/stream/2026-10-24T00:00:00.000"
        # Measured 2026-10-24: a U+0000 inside the label answers Invalid StreamArn;
        # a U+0000 after a complete label reads as a label and answers
        # ResourceNotFoundException, so the character goes inside.
        label = f"{prefix}{name}/stream/2026-10-24T00:00{NUL}:00.000"
        for arn in [table_seg, label]:
            with pytest.raises(ClientError) as exc:
                raw_streams_client.describe_stream(StreamArn=arn)
            assert_validation(exc.value, "Invalid StreamArn")
        with pytest.raises(ClientError) as exc:
            raw_streams_client.get_shard_iterator(
                StreamArn=table_seg, ShardId="shardId-00000001", ShardIteratorType="TRIM_HORIZON"
            )
        assert_validation(exc.value, "Invalid StreamArn")
        with pytest.raises(ClientError) as exc:
            raw_client.tag_resource(ResourceArn=table_seg, Tags=[{"Key": "k", "Value": "v"}])
        assert_validation(exc.value, "Invalid StreamArn")

    def test_backup_arns_with_nul(self, raw_client, nul_table):
        name, prefix = nul_table
        bad = f"{prefix}t367{NUL}name"
        table_seg = f"{bad}/backup/01700000000000-abcdef12"
        for call in [
            lambda: raw_client.describe_backup(BackupArn=table_seg),
            lambda: raw_client.delete_backup(BackupArn=table_seg),
            lambda: raw_client.restore_table_from_backup(
                BackupArn=table_seg, TargetTableName=unique_name("tgt")
            ),
        ]:
            with pytest.raises(ClientError) as exc:
                call()
            assert_validation(exc.value, "Invalid Backup ARN")
        backup_id = f"{prefix}{name}/backup/0170000000{NUL}0000-abcdef12"
        with pytest.raises(ClientError) as exc:
            raw_client.describe_backup(BackupArn=backup_id)
        assert_validation(exc.value, "Invalid Request: BackupArn is not valid")
