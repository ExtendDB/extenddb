# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""WarmThroughput on tables and global secondary indexes.

Every assertion here is a row measured against Amazon DynamoDB (us-east-1,
2026-10-23). The file runs against ExtendDB and against the service with the
same assertions; the two differ only in timing, which the `is_extenddb`
fixture gates: ExtendDB reports `Status: UPDATING` for the control-plane delay
(a fraction of a second by default), the service for minutes, so the "returns
to ACTIVE" wait runs only against ExtendDB.

ExtendDB stores and reports the values. Nothing acts on them.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from warm_throughput_helpers import (  # noqa: F401  (tables is a fixture)
    DECREASE,
    DELETION_PROTECTION_ONLY,
    GSI,
    LSI,
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


class TestWarmThroughputLifecycle:
    def test_on_demand_table_absent_while_creating_defaults_once_active(
        self, dynamodb_client, tables
    ):
        """Rows: CreateTable response and first DescribeTable carry no member while
        CREATING; ACTIVE reads {12000, 4000, ACTIVE} on the table and every GSI;
        a local secondary index carries none."""
        name, created = _create(
            dynamodb_client,
            tables,
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[GSI],
            LocalSecondaryIndexes=[LSI],
        )
        if created["TableStatus"] == "CREATING":
            assert "WarmThroughput" not in created
            assert all("WarmThroughput" not in g for g in created["GlobalSecondaryIndexes"])
            first = dynamodb_client.describe_table(TableName=name)["Table"]
            if first["TableStatus"] == "CREATING":
                assert "WarmThroughput" not in first
        active = _wait_all_active(dynamodb_client, name)
        assert active["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "ACTIVE"}
        gsi = active["GlobalSecondaryIndexes"][0]
        assert gsi["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "ACTIVE"}
        assert "WarmThroughput" not in active["LocalSecondaryIndexes"][0]

    def test_provisioned_table_and_index_default_to_their_units(self, dynamodb_client, tables):
        """Row: a 1/1 provisioned table with a 1/1 GSI reads {1, 1, ACTIVE} on both."""
        name, created = _create(
            dynamodb_client,
            tables,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
            GlobalSecondaryIndexes=[
                {**GSI, "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}}
            ],
        )
        if created["TableStatus"] == "CREATING":
            assert "WarmThroughput" not in created
        active = _wait_all_active(dynamodb_client, name)
        expected = {"ReadUnitsPerSecond": 1, "WriteUnitsPerSecond": 1, "Status": "ACTIVE"}
        assert active["WarmThroughput"] == expected
        assert active["GlobalSecondaryIndexes"][0]["WarmThroughput"] == expected

    def test_update_table_without_warm_throughput_reports_it_active(
        self, dynamodb_client, tables
    ):
        """Row: an UpdateTable that only touches deletion protection carries the
        member with Status ACTIVE in its response."""
        name, _ = _create(dynamodb_client, tables, BillingMode="PAY_PER_REQUEST")
        _wait_all_active(dynamodb_client, name)
        resp = dynamodb_client.update_table(TableName=name, DeletionProtectionEnabled=False)
        assert resp["TableDescription"]["WarmThroughput"] == {
            **ON_DEMAND_DEFAULTS,
            "Status": "ACTIVE",
        }

    def test_create_with_warm_throughput_is_present_at_once_and_partial_is_echoed(
        self, dynamodb_client, tables
    ):
        """Rows C1 and C4: a request with WarmThroughput is answered with the member
        at once, Status UPDATING, as requested (a read-only request echoes only the
        read side in the CreateTable response), and reads back filled once ACTIVE."""
        name, created = _create(
            dynamodb_client,
            tables,
            BillingMode="PAY_PER_REQUEST",
            WarmThroughput={"ReadUnitsPerSecond": 20000},
        )
        if created["TableStatus"] == "CREATING":
            assert created["WarmThroughput"] == {"ReadUnitsPerSecond": 20000, "Status": "UPDATING"}
        else:
            assert created["WarmThroughput"]["ReadUnitsPerSecond"] == 20000
        _wait_all_active(dynamodb_client, name)
        active = _wait_warm_active(dynamodb_client, name, timeout=900)
        assert active["WarmThroughput"] == {
            "ReadUnitsPerSecond": 20000,
            "WriteUnitsPerSecond": 4000,
            "Status": "ACTIVE",
        }

        both, created = _create(
            dynamodb_client,
            tables,
            BillingMode="PAY_PER_REQUEST",
            WarmThroughput={"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000},
            GlobalSecondaryIndexes=[
                {**GSI, "WarmThroughput": {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000}}
            ],
        )
        if created["TableStatus"] == "CREATING":
            assert created["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "UPDATING"}
            assert created["GlobalSecondaryIndexes"][0]["WarmThroughput"] == {
                **ON_DEMAND_DEFAULTS,
                "Status": "UPDATING",
            }
        active = _wait_all_active(dynamodb_client, both)
        active = _wait_warm_active(dynamodb_client, both, timeout=900)
        assert active["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "ACTIVE"}
        assert active["GlobalSecondaryIndexes"][0]["WarmThroughput"] == {
            **ON_DEMAND_DEFAULTS,
            "Status": "ACTIVE",
        }

    def test_update_warm_throughput_goes_updating_then_active(
        self, dynamodb_client, tables, is_extenddb
    ):
        """Rows U1, U2, U3, U5, U6, S2a, S2b and S2c: UpdateTable with WarmThroughput
        is accepted and the response reports the PREVIOUS values with Status
        UPDATING while TableStatus and IndexStatus stay ACTIVE; inside the window a
        request carrying those previous values is accepted (U3); once settled the new
        values read ACTIVE and the same request is a decrease (S2b). Only the waits
        are gated: ExtendDB returns to ACTIVE within the control plane delay, the
        service after minutes, so the settled assertions run on ExtendDB only."""
        name, _ = _create(
            dynamodb_client,
            tables,
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[GSI],
        )
        _wait_all_active(dynamodb_client, name)

        # U1: the same values as current, and nothing else in the request.
        resp = dynamodb_client.update_table(TableName=name, WarmThroughput=ON_DEMAND_DEFAULTS)
        t = resp["TableDescription"]
        assert t["TableStatus"] == "ACTIVE"
        assert t["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "UPDATING"}
        if is_extenddb:
            _wait_warm_active(dynamodb_client, name)

        # U2 and S2a: higher values; the response carries the previous pair.
        higher = {"ReadUnitsPerSecond": 24000, "WriteUnitsPerSecond": 8000}
        t = dynamodb_client.update_table(TableName=name, WarmThroughput=higher)["TableDescription"]
        assert t["TableStatus"] == "ACTIVE"
        assert t["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "UPDATING"}
        described = dynamodb_client.describe_table(TableName=name)["Table"]["WarmThroughput"]
        if described["Status"] == "UPDATING":
            assert described == {**ON_DEMAND_DEFAULTS, "Status": "UPDATING"}
            # U3: inside the window the previous values are not a decrease.
            t = dynamodb_client.update_table(TableName=name, WarmThroughput=ON_DEMAND_DEFAULTS)[
                "TableDescription"
            ]
            assert t["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "UPDATING"}
            if is_extenddb:
                _wait_warm_active(dynamodb_client, name)
                dynamodb_client.update_table(TableName=name, WarmThroughput=higher)
        if is_extenddb:
            active = _wait_warm_active(dynamodb_client, name)
            assert active["WarmThroughput"] == {**higher, "Status": "ACTIVE"}

            # S2b: on the settled table the previous values are a decrease.
            with pytest.raises(ClientError) as exc:
                dynamodb_client.update_table(TableName=name, WarmThroughput=ON_DEMAND_DEFAULTS)
            _validation(exc, _sentence("ReadUnitsPerSecond", "table", DECREASE))
            # S2c: the same values re-sent report themselves UPDATING.
            t = dynamodb_client.update_table(TableName=name, WarmThroughput=higher)[
                "TableDescription"
            ]
            assert t["WarmThroughput"] == {**higher, "Status": "UPDATING"}
            _wait_warm_active(dynamodb_client, name)

            # U5: read alone; the write side keeps its value.
            dynamodb_client.update_table(TableName=name, WarmThroughput={"ReadUnitsPerSecond": 30000})
            active = _wait_warm_active(dynamodb_client, name)
            assert active["WarmThroughput"] == {
                "ReadUnitsPerSecond": 30000,
                "WriteUnitsPerSecond": 8000,
                "Status": "ACTIVE",
            }

        # U6: a GSI through GlobalSecondaryIndexUpdates.Update (same values, so
        # the previous and new pairs agree on both targets).
        resp = dynamodb_client.update_table(
            TableName=name,
            GlobalSecondaryIndexUpdates=[
                {"Update": {"IndexName": "gsi", "WarmThroughput": ON_DEMAND_DEFAULTS}}
            ],
        )
        gsi = resp["TableDescription"]["GlobalSecondaryIndexes"][0]
        assert gsi["IndexStatus"] == "ACTIVE"
        assert gsi["WarmThroughput"] == {**ON_DEMAND_DEFAULTS, "Status": "UPDATING"}
        assert resp["TableDescription"]["TableStatus"] == "ACTIVE"
        if is_extenddb:
            active = _wait_warm_active(dynamodb_client, name)
            assert active["GlobalSecondaryIndexes"][0]["WarmThroughput"] == {
                **ON_DEMAND_DEFAULTS,
                "Status": "ACTIVE",
            }

    def test_capacity_change_leaves_a_set_warm_throughput_alone(self, dynamodb_client, tables):
        """Rows C6 and U8: a provisioned 5/5 table created with WarmThroughput 100/50
        keeps 100/50 ACTIVE after its capacity moves to 10/10."""
        name, created = _create(
            dynamodb_client,
            tables,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            WarmThroughput={"ReadUnitsPerSecond": 100, "WriteUnitsPerSecond": 50},
        )
        if created["TableStatus"] == "CREATING":
            assert created["WarmThroughput"] == {
                "ReadUnitsPerSecond": 100,
                "WriteUnitsPerSecond": 50,
                "Status": "UPDATING",
            }
        _wait_all_active(dynamodb_client, name)
        _wait_warm_active(dynamodb_client, name, timeout=900)
        resp = dynamodb_client.update_table(
            TableName=name,
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )
        expected = {"ReadUnitsPerSecond": 100, "WriteUnitsPerSecond": 50, "Status": "ACTIVE"}
        assert resp["TableDescription"]["WarmThroughput"] == expected
        _wait_all_active(dynamodb_client, name)
        assert dynamodb_client.describe_table(TableName=name)["Table"]["WarmThroughput"] == expected


class TestWarmThroughputValidation:
    def test_on_demand_create_below_the_minimum(self, dynamodb_client, tables):
        """Rows C2, C3 and C7: the on-demand minimum, read checked before write."""
        for warm, member in (
            ({"ReadUnitsPerSecond": 11999, "WriteUnitsPerSecond": 4000}, "ReadUnitsPerSecond"),
            ({"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 3999}, "WriteUnitsPerSecond"),
            ({"ReadUnitsPerSecond": 0, "WriteUnitsPerSecond": 0}, "ReadUnitsPerSecond"),
        ):
            with pytest.raises(ClientError) as exc:
                _create(dynamodb_client, tables, BillingMode="PAY_PER_REQUEST", WarmThroughput=warm)
            _validation(exc, _sentence(member, "table", ON_DEMAND_MIN))

    def test_on_demand_gsi_create_below_the_minimum_names_the_index(self, dynamodb_client, tables):
        """Rows G1 and G2: the index form of the on-demand sentence."""
        for warm, member in (
            ({"ReadUnitsPerSecond": 11999, "WriteUnitsPerSecond": 4000}, "ReadUnitsPerSecond"),
            ({"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 3999}, "WriteUnitsPerSecond"),
        ):
            with pytest.raises(ClientError) as exc:
                _create(
                    dynamodb_client,
                    tables,
                    BillingMode="PAY_PER_REQUEST",
                    GlobalSecondaryIndexes=[{**GSI, "WarmThroughput": warm}],
                )
            _validation(exc, _sentence(member, "index gsi", ON_DEMAND_MIN))

    def test_provisioned_create_below_the_units(self, dynamodb_client, tables):
        """Row C5 (read form measured; the write form is the same sentence with the
        member names substituted)."""
        with pytest.raises(ClientError) as exc:
            _create(
                dynamodb_client,
                tables,
                BillingMode="PROVISIONED",
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                WarmThroughput={"ReadUnitsPerSecond": 1, "WriteUnitsPerSecond": 1},
            )
        _validation(
            exc,
            _sentence(
                "ReadUnitsPerSecond", "table",
                " is lower than ReadCapacityUnits of ProvisionedThroughput",
            ),
        )
        # P1: the write form.
        with pytest.raises(ClientError) as exc:
            _create(
                dynamodb_client,
                tables,
                BillingMode="PROVISIONED",
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                WarmThroughput={"ReadUnitsPerSecond": 5, "WriteUnitsPerSecond": 4},
            )
        _validation(
            exc,
            _sentence(
                "WriteUnitsPerSecond", "table",
                " is lower than WriteCapacityUnits of ProvisionedThroughput",
            ),
        )
        # G3: the index form, on a 5/5 GSI.
        with pytest.raises(ClientError) as exc:
            _create(
                dynamodb_client,
                tables,
                BillingMode="PROVISIONED",
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                GlobalSecondaryIndexes=[{
                    **GSI,
                    "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                    "WarmThroughput": {"ReadUnitsPerSecond": 1, "WriteUnitsPerSecond": 1},
                }],
            )
        _validation(
            exc,
            _sentence(
                "ReadUnitsPerSecond", "index gsi",
                " is lower than ReadCapacityUnits of ProvisionedThroughput",
            ),
        )
        # P2: equal to the units is accepted and reads back {5, 5, ACTIVE}.
        name, _ = _create(
            dynamodb_client,
            tables,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            WarmThroughput={"ReadUnitsPerSecond": 5, "WriteUnitsPerSecond": 5},
        )
        _wait_all_active(dynamodb_client, name)
        active = _wait_warm_active(dynamodb_client, name, timeout=900)
        assert active["WarmThroughput"] == {
            "ReadUnitsPerSecond": 5,
            "WriteUnitsPerSecond": 5,
            "Status": "ACTIVE",
        }

    def test_update_below_current_is_a_decrease(self, dynamodb_client, tables):
        """Rows U4 and U7: below the current value on either billing mode."""
        od, _ = _create(dynamodb_client, tables, BillingMode="PAY_PER_REQUEST")
        _wait_all_active(dynamodb_client, od)
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_table(
                TableName=od, WarmThroughput={"ReadUnitsPerSecond": 11999, "WriteUnitsPerSecond": 4000}
            )
        _validation(exc, _sentence("ReadUnitsPerSecond", "table", DECREASE))
        # UT1: the write form.
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_table(
                TableName=od, WarmThroughput={"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 3999}
            )
        _validation(exc, _sentence("WriteUnitsPerSecond", "table", DECREASE))

        pv, _ = _create(
            dynamodb_client,
            tables,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            WarmThroughput={"ReadUnitsPerSecond": 100, "WriteUnitsPerSecond": 50},
        )
        _wait_all_active(dynamodb_client, pv)
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_table(
                TableName=pv, WarmThroughput={"ReadUnitsPerSecond": 2, "WriteUnitsPerSecond": 2}
            )
        _validation(exc, _sentence("ReadUnitsPerSecond", "table", DECREASE))
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_table(TableName=pv, WarmThroughput={"WriteUnitsPerSecond": 49})
        _validation(exc, _sentence("WriteUnitsPerSecond", "table", DECREASE))

    def test_gsi_update_below_current_names_the_index(self, dynamodb_client, tables):
        """Rows UG1 and UG2: the index form of the decrease sentence, both members."""
        name, _ = _create(
            dynamodb_client, tables, BillingMode="PAY_PER_REQUEST", GlobalSecondaryIndexes=[GSI]
        )
        _wait_all_active(dynamodb_client, name)
        for warm, member in (
            ({"ReadUnitsPerSecond": 11999, "WriteUnitsPerSecond": 4000}, "ReadUnitsPerSecond"),
            ({"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 3999}, "WriteUnitsPerSecond"),
        ):
            with pytest.raises(ClientError) as exc:
                dynamodb_client.update_table(
                    TableName=name,
                    GlobalSecondaryIndexUpdates=[{"Update": {"IndexName": "gsi", "WarmThroughput": warm}}],
                )
            _validation(exc, _sentence(member, "index gsi", DECREASE))

    def test_table_warm_throughput_beside_another_change(self, dynamodb_client, tables):
        """The per-companion rules, measured on an on-demand table with one GSI
        (values otherwise valid): TableClass and DeletionProtection companions
        answer their own "must be the only operation" sentence; a
        StreamSpecification companion, and BillingMode with ProvisionedThroughput,
        answer the WarmThroughput sentence; an OnDemandThroughput companion without
        BillingMode answers its own sentence; a GlobalSecondaryIndexUpdates Update
        carrying the index's WarmThroughput is accepted and both go UPDATING."""
        name, _ = _create(
            dynamodb_client, tables, BillingMode="PAY_PER_REQUEST", GlobalSecondaryIndexes=[GSI]
        )
        _wait_all_active(dynamodb_client, name)
        cases = [
            ({"TableClass": "STANDARD"}, TABLE_CLASS_ONLY),
            ({"DeletionProtectionEnabled": False}, DELETION_PROTECTION_ONLY),
            ({"OnDemandThroughput": {"MaxReadRequestUnits": 100000}}, ON_DEMAND_NEEDS_BILLING),
            ({"StreamSpecification": {"StreamEnabled": False}}, ONLY_OPERATION),
            (
                {
                    "BillingMode": "PROVISIONED",
                    "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                },
                ONLY_OPERATION,
            ),
        ]
        for extra, expected in cases:
            with pytest.raises(ClientError) as exc:
                dynamodb_client.update_table(
                    TableName=name, WarmThroughput=ON_DEMAND_DEFAULTS, **extra
                )
            _validation(exc, expected)

        resp = dynamodb_client.update_table(
            TableName=name,
            WarmThroughput=ON_DEMAND_DEFAULTS,
            GlobalSecondaryIndexUpdates=[
                {"Update": {"IndexName": "gsi", "WarmThroughput": ON_DEMAND_DEFAULTS}}
            ],
        )
        t = resp["TableDescription"]
        assert t["TableStatus"] == "ACTIVE"
        assert t["WarmThroughput"]["Status"] == "UPDATING"
        assert t["GlobalSecondaryIndexes"][0]["WarmThroughput"]["Status"] == "UPDATING"
