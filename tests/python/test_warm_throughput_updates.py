# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""UpdateTable shapes around WarmThroughput measured in the review rounds of
2026-10-23: the GlobalSecondaryIndexUpdates element rules, the companions a
table-level WarmThroughput may and may not have, a GSI Create carrying
WarmThroughput, the empty request on CreateTable, and the defaults after
capacity and billing changes. Row labels refer to the service-r1, r2 and r3
runs. The file runs against ExtendDB and the service; only waits and the
pre-existing IndexStatus divergence are gated on `is_extenddb`.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name
from warm_throughput_helpers import (  # noqa: F401  (tables is a fixture)
    ATTRS,
    DECREASE,
    DELETION_PROTECTION_ONLY,
    GSI,
    ON_DEMAND_DEFAULTS,
    ON_DEMAND_MIN,
    ON_DEMAND_NEEDS_BILLING,
    ONLY_OPERATION,
    TABLE_CLASS_ONLY,
    _create,
    _sentence,
    _validation,
    _wait_all_active,
    _wait_warm_active,
    tables,
)

ONE_ACTION = (
    "One or more parameter values were invalid: Only one global secondary index action is "
    "allowed per GlobalSecondaryIndexUpdate object"
)
ONE_UPDATE_PER_INDEX = (
    "One or more parameter values were invalid: Only one global secondary index update per "
    "index is allowed simultaneously. Index: gsi"
)
ON_DEMAND_UPDATE_MEMBERS = (
    "One or more parameter values were invalid: The only Updates for index: gsi when "
    "TableThroughputMode is PAY_PER_REQUEST can be to OnDemandThroughput, WarmThroughput"
)
SSE_ONLY = (
    "One or more parameter values were invalid: Server-Side Encryption modification must be "
    "the only operation in the request"
)
DELETE_WITH_WARM = (
    "One or more parameter values were invalid: Delete global secondary index cannot be "
    "specified when updating WarmThroughput"
)
GSI_TWO = {
    "IndexName": "gsi-two",
    "KeySchema": [{"AttributeName": "G", "KeyType": "HASH"}],
    "Projection": {"ProjectionType": "KEYS_ONLY"},
}


def _on_demand_with_gsi(client, tables) -> str:
    name, _ = _create(client, tables, BillingMode="PAY_PER_REQUEST", GlobalSecondaryIndexes=[GSI])
    _wait_all_active(client, name)
    return name


class TestGsiUpdateElements:
    def test_an_element_with_two_actions_is_refused(self, dynamodb_client, tables):
        """Rows V1a and V1b: Update plus Create, and Create plus Delete (the Create
        keys on a defined attribute, so only the element rule can refuse it)."""
        name = _on_demand_with_gsi(dynamodb_client, tables)
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_table(
                TableName=name,
                AttributeDefinitions=ATTRS,
                GlobalSecondaryIndexUpdates=[{
                    "Update": {"IndexName": "gsi", "WarmThroughput": ON_DEMAND_DEFAULTS},
                    "Create": GSI_TWO,
                }],
            )
        _validation(exc, ONE_ACTION)
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_table(
                TableName=name,
                AttributeDefinitions=ATTRS,
                GlobalSecondaryIndexUpdates=[{"Create": GSI_TWO, "Delete": {"IndexName": "gsi"}}],
            )
        _validation(exc, ONE_ACTION)
        # T3b: Update plus Delete of the same index in one element answers the
        # per-index sentence.
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_table(
                TableName=name,
                GlobalSecondaryIndexUpdates=[{
                    "Update": {"IndexName": "gsi", "WarmThroughput": ON_DEMAND_DEFAULTS},
                    "Delete": {"IndexName": "gsi"},
                }],
            )
        _validation(exc, ONE_UPDATE_PER_INDEX)
        t = dynamodb_client.describe_table(TableName=name)["Table"]
        assert [g["IndexName"] for g in t["GlobalSecondaryIndexes"]] == ["gsi"]

    def test_two_elements_on_one_index_are_refused(self, dynamodb_client, tables):
        """Row V3c: Delete gsi and Update gsi WarmThroughput in two elements."""
        name = _on_demand_with_gsi(dynamodb_client, tables)
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_table(
                TableName=name,
                GlobalSecondaryIndexUpdates=[
                    {"Delete": {"IndexName": "gsi"}},
                    {"Update": {"IndexName": "gsi", "WarmThroughput": ON_DEMAND_DEFAULTS}},
                ],
            )
        _validation(exc, ONE_UPDATE_PER_INDEX)

    def test_an_update_without_warm_throughput_names_the_billing_mode(
        self, dynamodb_client, tables
    ):
        """Rows T3d and T3e: an Update carrying a ProvisionedThroughput, or nothing,
        on an on-demand table."""
        name = _on_demand_with_gsi(dynamodb_client, tables)
        for update in (
            {"IndexName": "gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}},
            {"IndexName": "gsi"},
        ):
            with pytest.raises(ClientError) as exc:
                dynamodb_client.update_table(
                    TableName=name, GlobalSecondaryIndexUpdates=[{"Update": update}]
                )
            _validation(exc, ON_DEMAND_UPDATE_MEMBERS)


INDEX_CHANGE_WITH_STREAM = (
    "One or more parameter values were invalid: You cannot create or delete index while "
    "changing stream status"
)
STREAM_ON = {"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"}
PROVISIONED_5 = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}


def _refused(client, expected: str, **kwargs) -> None:
    with pytest.raises(ClientError) as exc:
        client.update_table(**kwargs)
    _validation(exc, expected)


class TestWarmThroughputCompanions:
    """The companions a table-level WarmThroughput may not have (rows S1a to S1d,
    S9a, T4a to T4d, V2, V3a, V3b, V4). Every request here is refused, so one
    table serves all of them."""

    def test_sse_and_the_ordering_of_the_member_sentences(self, dynamodb_client, tables):
        name = _on_demand_with_gsi(dynamodb_client, tables)
        sse = {"Enabled": True}
        cases = [
            ({"SSESpecification": sse}, SSE_ONLY),
            # SSE outranks TableClass (S1a), DeletionProtection (S1b), Streams (S1c).
            ({"SSESpecification": sse, "TableClass": "STANDARD_INFREQUENT_ACCESS"}, SSE_ONLY),
            ({"SSESpecification": sse, "DeletionProtectionEnabled": False}, SSE_ONLY),
            ({"SSESpecification": sse, "StreamSpecification": STREAM_ON}, SSE_ONLY),
            # DeletionProtection outranks Streams (S1d).
            (
                {"DeletionProtectionEnabled": False, "StreamSpecification": STREAM_ON},
                DELETION_PROTECTION_ONLY,
            ),
            # V4: the triple answers the TableClass sentence.
            (
                {
                    "TableClass": "STANDARD_INFREQUENT_ACCESS",
                    "DeletionProtectionEnabled": False,
                    "StreamSpecification": STREAM_ON,
                },
                TABLE_CLASS_ONLY,
            ),
        ]
        for extra, expected in cases:
            _refused(dynamodb_client, expected, TableName=name, WarmThroughput=ON_DEMAND_DEFAULTS, **extra)

    def test_on_demand_throughput_with_and_without_billing_mode(self, dynamodb_client, tables):
        name = _on_demand_with_gsi(dynamodb_client, tables)
        odt = {"MaxReadRequestUnits": 100000}
        cases = [
            # T4a and T4d: the same sentence with and without BillingMode.
            ({"BillingMode": "PAY_PER_REQUEST", "OnDemandThroughput": odt}, ON_DEMAND_NEEDS_BILLING),
            ({"OnDemandThroughput": odt}, ON_DEMAND_NEEDS_BILLING),
            # T4b and T4c: TableClass and DeletionProtection outrank it.
            ({"TableClass": "STANDARD_INFREQUENT_ACCESS", "OnDemandThroughput": odt}, TABLE_CLASS_ONLY),
            ({"DeletionProtectionEnabled": False, "OnDemandThroughput": odt}, DELETION_PROTECTION_ONLY),
        ]
        for extra, expected in cases:
            _refused(dynamodb_client, expected, TableName=name, WarmThroughput=ON_DEMAND_DEFAULTS, **extra)

    def test_provisioned_throughput_alone_is_not_an_allowed_companion(self, dynamodb_client, tables):
        """Row V2."""
        name = _on_demand_with_gsi(dynamodb_client, tables)
        _refused(
            dynamodb_client,
            ONLY_OPERATION,
            TableName=name,
            WarmThroughput=ON_DEMAND_DEFAULTS,
            ProvisionedThroughput=PROVISIONED_5,
        )

    def test_index_delete_is_refused_and_the_index_stays(self, dynamodb_client, tables):
        """Rows S9a, V3a and V3b: Delete beside a table WarmThroughput; TableClass
        outranks it; with Streams the service names the stream change."""
        name = _on_demand_with_gsi(dynamodb_client, tables)
        delete = [{"Delete": {"IndexName": "gsi"}}]
        _refused(
            dynamodb_client, DELETE_WITH_WARM, TableName=name,
            WarmThroughput=ON_DEMAND_DEFAULTS, GlobalSecondaryIndexUpdates=delete,
        )
        _refused(
            dynamodb_client, TABLE_CLASS_ONLY, TableName=name,
            WarmThroughput=ON_DEMAND_DEFAULTS, GlobalSecondaryIndexUpdates=delete,
            TableClass="STANDARD_INFREQUENT_ACCESS",
        )
        _refused(
            dynamodb_client, INDEX_CHANGE_WITH_STREAM, TableName=name,
            WarmThroughput=ON_DEMAND_DEFAULTS, GlobalSecondaryIndexUpdates=delete,
            StreamSpecification=STREAM_ON,
        )
        t = _wait_all_active(dynamodb_client, name)
        assert [g["IndexName"] for g in t["GlobalSecondaryIndexes"]] == ["gsi"]
        assert t["GlobalSecondaryIndexes"][0]["WarmThroughput"] == {
            **ON_DEMAND_DEFAULTS, "Status": "ACTIVE"
        }

    def test_missing_table(self, dynamodb_client):
        """Rows S10, T5a, T5c: a bad value alone, and an index Update alone, are
        ResourceNotFoundException. Rows S10b, T5b, T5d: a companion is answered
        first. Only the code is compared for the not-found message."""
        missing = unique_name("wt-missing")
        for kwargs in (
            {"WarmThroughput": {"ReadUnitsPerSecond": 11999, "WriteUnitsPerSecond": 4000}},
            {"WarmThroughput": {"ReadUnitsPerSecond": 1}},
            {
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "gsi", "WarmThroughput": ON_DEMAND_DEFAULTS}}
                ]
            },
        ):
            with pytest.raises(ClientError) as exc:
                dynamodb_client.update_table(TableName=missing, **kwargs)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        for extra, expected in (
            ({"TableClass": "STANDARD_INFREQUENT_ACCESS"}, TABLE_CLASS_ONLY),
            ({"StreamSpecification": STREAM_ON}, ONLY_OPERATION),
            ({"DeletionProtectionEnabled": False}, DELETION_PROTECTION_ONLY),
        ):
            _refused(
                dynamodb_client, expected, TableName=missing,
                WarmThroughput=ON_DEMAND_DEFAULTS, **extra,
            )


class TestWarmThroughputCreateShapes:
    def test_create_with_an_empty_warm_throughput_reports_no_member_while_creating(
        self, dynamodb_client, tables
    ):
        """Row T1: `WarmThroughput: {}` on CreateTable is accepted, the response
        while CREATING carries no member, and the defaults read once ACTIVE."""
        name, created = _create(
            dynamodb_client, tables, BillingMode="PAY_PER_REQUEST", WarmThroughput={}
        )
        if created["TableStatus"] == "CREATING":
            assert "WarmThroughput" not in created
        active = _wait_all_active(dynamodb_client, name)
        active = _wait_warm_active(dynamodb_client, name, timeout=900)
        assert active["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "ACTIVE"}

    def test_gsi_create_with_warm_throughput_in_update_table(
        self, dynamodb_client, tables, is_extenddb
    ):
        """Rows S8a and V5: a GSI created through GlobalSecondaryIndexUpdates with
        WarmThroughput reports the values as sent with Status UPDATING, then the
        same values ACTIVE. The service also reports the new index CREATING and the
        table UPDATING; ExtendDB reports both ACTIVE at once (the index build is
        immediate), so those two statuses are asserted on the service only."""
        name = _on_demand_with_gsi(dynamodb_client, tables)
        sent = {"ReadUnitsPerSecond": 30000, "WriteUnitsPerSecond": 8000}
        resp = dynamodb_client.update_table(
            TableName=name,
            AttributeDefinitions=ATTRS,
            GlobalSecondaryIndexUpdates=[{"Create": {**GSI_TWO, "WarmThroughput": sent}}],
        )
        t = resp["TableDescription"]
        by_name = {g["IndexName"]: g for g in t["GlobalSecondaryIndexes"]}
        assert by_name["gsi-two"]["WarmThroughput"] == {**sent, "Status": "UPDATING"}
        assert by_name["gsi"]["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "ACTIVE"}
        if not is_extenddb:
            assert t["TableStatus"] == "UPDATING"
            assert by_name["gsi-two"]["IndexStatus"] == "CREATING"
        described = dynamodb_client.describe_table(TableName=name)["Table"]
        new = {g["IndexName"]: g for g in described["GlobalSecondaryIndexes"]}["gsi-two"]
        if new["WarmThroughput"]["Status"] == "UPDATING":
            assert new["WarmThroughput"] == {**sent, "Status": "UPDATING"}
        _wait_all_active(dynamodb_client, name)
        settled = _wait_warm_active(dynamodb_client, name, timeout=900)
        by_name = {g["IndexName"]: g for g in settled["GlobalSecondaryIndexes"]}
        assert by_name["gsi-two"]["WarmThroughput"] == {**sent, "Status": "ACTIVE"}
        assert by_name["gsi"]["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "ACTIVE"}

    def test_gsi_create_below_the_minimum_names_the_index(self, dynamodb_client, tables):
        """Row S8b."""
        name = _on_demand_with_gsi(dynamodb_client, tables)
        _refused(
            dynamodb_client,
            _sentence("ReadUnitsPerSecond", "index gsi3", ON_DEMAND_MIN),
            TableName=name,
            AttributeDefinitions=ATTRS,
            GlobalSecondaryIndexUpdates=[{"Create": {
                **GSI_TWO,
                "IndexName": "gsi3",
                "WarmThroughput": {"ReadUnitsPerSecond": 11999, "WriteUnitsPerSecond": 4000},
            }}],
        )


class TestWarmThroughputAcrossBillingChanges:
    """The values a provisioned table reports around capacity and billing mode
    changes (rows S3, S7a to S7c, T7a to T7d). Statuses the service reports while
    the table is UPDATING for a capacity or billing change, and the previous
    values it answers with, are asserted on the service only: ExtendDB changes
    capacity and billing mode with the table ACTIVE, and answers a switch to
    PAY_PER_REQUEST with the on-demand values."""

    def test_switch_to_on_demand_raises_a_low_warm_throughput(
        self, dynamodb_client, tables, is_extenddb
    ):
        """Rows T7a to T7d and S7c: a read-only record {100} on a provisioned 5/5
        table reads {100, 5}, follows a capacity change to {100, 10}, and reads
        {12000, 4000} once the table is on-demand; the on-demand values are then
        not a decrease."""
        name, _ = _create(
            dynamodb_client,
            tables,
            BillingMode="PROVISIONED",
            ProvisionedThroughput=PROVISIONED_5,
            WarmThroughput={"ReadUnitsPerSecond": 100},
        )
        _wait_all_active(dynamodb_client, name)
        active = _wait_warm_active(dynamodb_client, name, timeout=900)
        assert active["WarmThroughput"] == {
            "ReadUnitsPerSecond": 100, "WriteUnitsPerSecond": 5, "Status": "ACTIVE"
        }
        dynamodb_client.update_table(
            TableName=name,
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )
        _wait_all_active(dynamodb_client, name)
        assert dynamodb_client.describe_table(TableName=name)["Table"]["WarmThroughput"] == {
            "ReadUnitsPerSecond": 100, "WriteUnitsPerSecond": 10, "Status": "ACTIVE"
        }
        resp = dynamodb_client.update_table(TableName=name, BillingMode="PAY_PER_REQUEST")
        if not is_extenddb:
            assert resp["TableDescription"]["WarmThroughput"] == {
                "ReadUnitsPerSecond": 100, "WriteUnitsPerSecond": 10, "Status": "ACTIVE"
            }
        _wait_all_active(dynamodb_client, name)
        active = _wait_warm_active(dynamodb_client, name, timeout=900)
        assert active["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "ACTIVE"}
        resp = dynamodb_client.update_table(TableName=name, WarmThroughput=ON_DEMAND_DEFAULTS)
        assert resp["TableDescription"]["WarmThroughput"] == {
            **ON_DEMAND_DEFAULTS, "Status": "UPDATING"
        }

    def test_never_set_warm_throughput_across_capacity_and_billing_changes(
        self, dynamodb_client, tables, is_extenddb
    ):
        """Rows S3, S7a and S7b: a provisioned 5/5 table that never set warm
        throughput reads {5, 5}, then {10, 10} after a capacity change, then
        {12000, 4000} on-demand, and keeps {12000, 4000} after the switch back to
        provisioned."""
        name, _ = _create(
            dynamodb_client, tables, BillingMode="PROVISIONED", ProvisionedThroughput=PROVISIONED_5
        )
        _wait_all_active(dynamodb_client, name)
        active = _wait_warm_active(dynamodb_client, name, timeout=900)
        assert active["WarmThroughput"] == {
            "ReadUnitsPerSecond": 5, "WriteUnitsPerSecond": 5, "Status": "ACTIVE"
        }

        # S3: the service answers the capacity change with the old units.
        resp = dynamodb_client.update_table(
            TableName=name,
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )
        if not is_extenddb:
            assert resp["TableDescription"]["WarmThroughput"] == {
                "ReadUnitsPerSecond": 5, "WriteUnitsPerSecond": 5, "Status": "ACTIVE"
            }
        _wait_all_active(dynamodb_client, name)
        assert dynamodb_client.describe_table(TableName=name)["Table"]["WarmThroughput"] == {
            "ReadUnitsPerSecond": 10, "WriteUnitsPerSecond": 10, "Status": "ACTIVE"
        }

        # S7a: on-demand.
        resp = dynamodb_client.update_table(TableName=name, BillingMode="PAY_PER_REQUEST")
        if not is_extenddb:
            assert resp["TableDescription"]["WarmThroughput"] == {
                "ReadUnitsPerSecond": 10, "WriteUnitsPerSecond": 10, "Status": "ACTIVE"
            }
        _wait_all_active(dynamodb_client, name)
        active = _wait_warm_active(dynamodb_client, name, timeout=900)
        assert active["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "ACTIVE"}

        # S7b: back to provisioned; the on-demand values stay.
        resp = dynamodb_client.update_table(
            TableName=name, BillingMode="PROVISIONED", ProvisionedThroughput=PROVISIONED_5
        )
        assert resp["TableDescription"]["WarmThroughput"] == {
            **ON_DEMAND_DEFAULTS, "Status": "ACTIVE"
        }
        _wait_all_active(dynamodb_client, name)
        active = _wait_warm_active(dynamodb_client, name, timeout=900)
        assert active["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "ACTIVE"}
