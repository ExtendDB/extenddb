# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for TableClass, SSESpecification, and OnDemandThroughput round-tripping."""

from __future__ import annotations

import pytest
from conftest import wait_for_active


class TestTableClass:
    """TableClass field round-trips through CreateTable and UpdateTable."""

    def test_table_class_infrequent_access(self, create_and_cleanup_table, dynamodb_client):
        result = create_and_cleanup_table(
            TableClass="STANDARD_INFREQUENT_ACCESS",
        )
        desc = dynamodb_client.describe_table(
            TableName=result["TableDescription"]["TableName"]
        )
        assert desc["Table"]["TableClassSummary"]["TableClass"] == "STANDARD_INFREQUENT_ACCESS"

    def test_table_class_default_standard(self, create_and_cleanup_table, dynamodb_client):
        result = create_and_cleanup_table()
        desc = dynamodb_client.describe_table(
            TableName=result["TableDescription"]["TableName"]
        )
        # STANDARD may be omitted entirely or reported explicitly
        tc = desc["Table"].get("TableClassSummary", {}).get("TableClass", "STANDARD")
        assert tc == "STANDARD"

    def test_update_table_class(self, create_and_cleanup_table, dynamodb_client):
        result = create_and_cleanup_table()
        table_name = result["TableDescription"]["TableName"]
        dynamodb_client.update_table(
            TableName=table_name,
            TableClass="STANDARD_INFREQUENT_ACCESS",
        )
        wait_for_active(dynamodb_client, table_name)
        desc = dynamodb_client.describe_table(TableName=table_name)
        assert desc["Table"]["TableClassSummary"]["TableClass"] == "STANDARD_INFREQUENT_ACCESS"


class TestSSESpecification:
    """SSESpecification round-trips as SSEDescription in DescribeTable."""

    def test_sse_enabled_round_trips(self, create_and_cleanup_table, dynamodb_client):
        result = create_and_cleanup_table(
            SSESpecification={"Enabled": True},
        )
        desc = dynamodb_client.describe_table(
            TableName=result["TableDescription"]["TableName"]
        )
        sse = desc["Table"]["SSEDescription"]
        assert sse["Status"] == "ENABLED"
        assert sse["SSEType"] == "KMS"
        assert "arn:aws:kms:" in sse["KMSMasterKeyArn"]


class TestOnDemandThroughput:
    """OnDemandThroughput round-trips through CreateTable and UpdateTable."""

    def test_on_demand_throughput_create(self, create_and_cleanup_table, dynamodb_client):
        result = create_and_cleanup_table(
            OnDemandThroughput={
                "MaxReadRequestUnits": 10,
                "MaxWriteRequestUnits": 5,
            },
        )
        desc = dynamodb_client.describe_table(
            TableName=result["TableDescription"]["TableName"]
        )
        odt = desc["Table"]["OnDemandThroughput"]
        assert odt["MaxReadRequestUnits"] == 10
        assert odt["MaxWriteRequestUnits"] == 5

    def test_update_on_demand_throughput(self, create_and_cleanup_table, dynamodb_client):
        result = create_and_cleanup_table(
            OnDemandThroughput={
                "MaxReadRequestUnits": 10,
                "MaxWriteRequestUnits": 5,
            },
        )
        table_name = result["TableDescription"]["TableName"]
        dynamodb_client.update_table(
            TableName=table_name,
            OnDemandThroughput={
                "MaxReadRequestUnits": 20,
                "MaxWriteRequestUnits": 15,
            },
        )
        wait_for_active(dynamodb_client, table_name)
        desc = dynamodb_client.describe_table(TableName=table_name)
        odt = desc["Table"]["OnDemandThroughput"]
        assert odt["MaxReadRequestUnits"] == 20
        assert odt["MaxWriteRequestUnits"] == 15
