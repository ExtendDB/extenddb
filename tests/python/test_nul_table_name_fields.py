"""Table names outside the pattern in every field that carries one, beyond
`TableName` (covered by test_nul_metadata_names.py), and the stream, backup
and tag ARN fields with U+0000 in a segment.

Measured against the service on 2026-10-24 with SDK parameter validation off.
Every operation here looks its table up before its handler runs, so the name
has to be refused before any lookup; on PostgreSQL a U+0000 cannot even be
bound as a parameter. These rows pin the service's sentence and location for
each field.
"""

import time

import pytest
from botocore.exceptions import ClientError

from helpers import (
    MEMBER_PATTERN,
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

    def test_restore_table_to_point_in_time_names(self, raw_client, nul_table):
        name, _ = nul_table
        with pytest.raises(ClientError) as exc:
            raw_client.restore_table_to_point_in_time(
                SourceTableName=BAD, TargetTableName=unique_name("tgt"), UseLatestRestorableTime=True
            )
        assert_member_pattern(exc.value, "sourceTableName", BAD)
        with pytest.raises(ClientError) as exc:
            raw_client.restore_table_to_point_in_time(
                SourceTableName=name, TargetTableName=BAD, UseLatestRestorableTime=True
            )
        assert_member_pattern(exc.value, "targetTableName", BAD)

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
                    PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
                ),
                id="UpdateContinuousBackups",
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
