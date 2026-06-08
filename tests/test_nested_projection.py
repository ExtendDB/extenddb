# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Nested ProjectionExpression correctness (C2).

Amazon DynamoDB projects list elements into a *compacted* list ordered by the
original element index, not by the order the indices appear in the
expression. It also preserves NULL elements that are explicitly projected
and drops the structure of unselected elements. These cases were captured
directly from Amazon DynamoDB via the AWS CLI.
"""

from __future__ import annotations

import pytest

from conftest import scoped_table


class TestNestedProjection:
    """List-element and deep-map projection, matched to Amazon DynamoDB."""

    @pytest.fixture(scope="class")
    def proj_table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "pk": {"S": "p1"},
                    "mylist": {
                        "L": [
                            {"S": "zero"},
                            {"S": "one"},
                            {"S": "two"},
                            {"S": "three"},
                        ]
                    },
                    "listOfMaps": {
                        "L": [
                            {"M": {"val": {"S": "a0"}, "x": {"S": "x0"}}},
                            {"M": {"val": {"S": "a1"}, "x": {"S": "x1"}}},
                            {"M": {"val": {"S": "a2"}, "x": {"S": "x2"}}},
                        ]
                    },
                    "listWithNull": {
                        "L": [{"S": "keep0"}, {"NULL": True}, {"S": "keep2"}]
                    },
                    "deep": {
                        "M": {
                            "l1": {
                                "M": {
                                    "l2": {
                                        "M": {
                                            "l3": {"S": "bottom"},
                                            "sib": {"S": "s"},
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "nestedList": {
                        "L": [
                            {"M": {"inner": {"L": [{"S": "i0"}, {"S": "i1"}]}}}
                        ]
                    },
                },
            )
            yield name

    def _get(self, client, table, projection, names=None):
        kwargs = {
            "TableName": table,
            "Key": {"pk": {"S": "p1"}},
            "ProjectionExpression": projection,
        }
        if names is not None:
            kwargs["ExpressionAttributeNames"] = names
        return client.get_item(**kwargs)["Item"]

    def test_two_list_indices_compacted(self, dynamodb_client, proj_table):
        """mylist[0], mylist[2] -> two-element list, not an overwrite."""
        item = self._get(dynamodb_client, proj_table, "mylist[0], mylist[2]")
        assert item["mylist"]["L"] == [{"S": "zero"}, {"S": "two"}]

    def test_list_indices_ordered_by_index_not_expression(
        self, dynamodb_client, proj_table
    ):
        """mylist[2], mylist[0] -> still ordered by original index."""
        item = self._get(dynamodb_client, proj_table, "mylist[2], mylist[0]")
        assert item["mylist"]["L"] == [{"S": "zero"}, {"S": "two"}]

    def test_list_index_gap_compacted(self, dynamodb_client, proj_table):
        """mylist[1], mylist[3] -> compacted, index order."""
        item = self._get(dynamodb_client, proj_table, "mylist[1], mylist[3]")
        assert item["mylist"]["L"] == [{"S": "one"}, {"S": "three"}]

    def test_list_of_maps_subfield_multi(self, dynamodb_client, proj_table):
        """listOfMaps[0].val, listOfMaps[2].val -> two maps, only val each."""
        item = self._get(
            dynamodb_client, proj_table, "listOfMaps[0].val, listOfMaps[2].val"
        )
        assert item["listOfMaps"]["L"] == [
            {"M": {"val": {"S": "a0"}}},
            {"M": {"val": {"S": "a2"}}},
        ]

    def test_whole_list_preserves_null(self, dynamodb_client, proj_table):
        """Projecting the whole list keeps the NULL element in place."""
        item = self._get(dynamodb_client, proj_table, "listWithNull")
        assert item["listWithNull"]["L"] == [
            {"S": "keep0"},
            {"NULL": True},
            {"S": "keep2"},
        ]

    def test_null_element_projected_by_index(self, dynamodb_client, proj_table):
        """listWithNull[1] -> the NULL element, wrapped in a single-element list."""
        item = self._get(dynamodb_client, proj_table, "listWithNull[1]")
        assert item["listWithNull"]["L"] == [{"NULL": True}]

    def test_unselected_null_dropped(self, dynamodb_client, proj_table):
        """listWithNull[0], listWithNull[2] -> the middle NULL is not included."""
        item = self._get(
            dynamodb_client, proj_table, "listWithNull[0], listWithNull[2]"
        )
        assert item["listWithNull"]["L"] == [{"S": "keep0"}, {"S": "keep2"}]

    def test_deep_map_structure_preserved(self, dynamodb_client, proj_table):
        """deep.l1.l2.l3 -> nested maps preserved, sibling dropped."""
        item = self._get(dynamodb_client, proj_table, "deep.l1.l2.l3")
        assert item["deep"] == {
            "M": {"l1": {"M": {"l2": {"M": {"l3": {"S": "bottom"}}}}}}
        }

    def test_deep_nested_list_element(self, dynamodb_client, proj_table):
        """nestedList[0].#i[1] -> deep list-in-map-in-list, single leaf element."""
        item = self._get(
            dynamodb_client,
            proj_table,
            "nestedList[0].#i[1]",
            names={"#i": "inner"},
        )
        assert item["nestedList"]["L"] == [
            {"M": {"inner": {"L": [{"S": "i1"}]}}}
        ]
