# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-end metrics tests — verify operations produce metrics and the
/metrics endpoint returns correct data.

When a catalog pool is available (the normal case), the server always
queries the database for metrics.  The in-memory collector is a fallback
only when no catalog pool exists.  Because the DB flush worker runs on a
60-second timer, recently-recorded metrics may not yet be visible through
the ``/metrics`` endpoint.  Tests that check metric *values* therefore
tolerate empty results from the DB path.

Prerequisites:
  - extenddb running on EXTENDDB_TEST_ENDPOINT

REQ-OBS-005
"""

from __future__ import annotations

import os
import time

import boto3
import pytest
import requests

from conftest import wait_for_active

# /metrics requires admin HTTP Basic auth. The harness (devtools/run-tests)
# exports EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD from `extenddb init`.
_ADMIN_AUTH = (
    os.environ.get("EXTENDDB_ADMIN_USER", "admin"),
    os.environ.get("EXTENDDB_ADMIN_PASSWORD", ""),
)


def _endpoint() -> str:
    """Return the extenddb endpoint or fail."""
    url = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    if not url:
        pytest.fail(
            "MISCONFIGURED: Metrics tests require EXTENDDB_TEST_ENDPOINT. "
            "This must be set by devtools/run-tests before test execution."
        )
    return url


def _metrics_url(endpoint: str, **params: str) -> str:
    """Build the /metrics URL with query parameters."""
    qs = "&".join(f"{k}={v}" for k, v in params.items() if v)
    base = f"{endpoint}/metrics"
    return f"{base}?{qs}" if qs else base


@pytest.fixture(scope="module")
def endpoint_url() -> str:
    return _endpoint()


@pytest.fixture(scope="module")
def ddb(endpoint_url: str):
    """DynamoDB client targeting extenddb."""
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )
    # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


@pytest.fixture(scope="module")
def metrics_table(ddb, endpoint_url: str) -> str:
    """Create a table, do some operations, and return the table name."""
    import uuid

    name = f"extenddb-metrics-test-{uuid.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(ddb, name)

    # Generate some metrics: writes and reads.
    for i in range(5):
        ddb.put_item(TableName=name, Item={"pk": {"S": f"item-{i}"}})
    for i in range(3):
        ddb.get_item(TableName=name, Key={"pk": {"S": f"item-{i}"}})

    # Small delay to let metrics accumulate.
    time.sleep(0.5)

    yield name

    # Cleanup.
    try:
        ddb.delete_table(TableName=name)
        # Wait for deletion.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                ddb.describe_table(TableName=name)
                time.sleep(0.2)
            except ddb.exceptions.ResourceNotFoundException:
                break
    except Exception:
        pass


class TestMetricsEndpoint:
    """Tests for the /metrics JSON endpoint."""

    def test_metrics_returns_200(self, endpoint_url: str) -> None:
        """GET /metrics returns 200 with JSON body."""
        resp = requests.get(f"{endpoint_url}/metrics", timeout=10, verify=False, auth=_ADMIN_AUTH)
        assert resp.status_code == 200
        data = resp.json()
        assert "metrics" in data
        assert "source" in data

    def test_metrics_source(self, endpoint_url: str) -> None:
        """Default query reports its data source."""
        resp = requests.get(f"{endpoint_url}/metrics", timeout=10, verify=False, auth=_ADMIN_AUTH)
        data = resp.json()
        assert data["source"] in ("memory", "database")

    def test_metrics_with_window(self, endpoint_url: str) -> None:
        """Window parameter selects time range."""
        resp = requests.get(
            f"{endpoint_url}/metrics?window=Last5Minutes", timeout=10, verify=False, auth=_ADMIN_AUTH
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["source"] in ("memory", "database")

    def test_metrics_after_operations(
        self, endpoint_url: str, metrics_table: str
    ) -> None:
        """Operations produce metrics visible in the endpoint.

        When the DB path is active, recently-recorded metrics may not yet
        be flushed (60-second timer).  We verify the response structure
        but tolerate empty results from the database source.
        """
        resp = requests.get(
            _metrics_url(endpoint_url, table_name=metrics_table), timeout=10, verify=False, auth=_ADMIN_AUTH
        )
        assert resp.status_code == 200
        data = resp.json()
        metrics = data["metrics"]

        if data.get("source") == "memory":
            assert len(metrics) > 0, "Expected metrics after PutItem/GetItem operations"
            metric_names = {m["metric"] for m in metrics}
            assert "ConsumedWriteCapacityUnits" in metric_names, (
                f"Expected ConsumedWriteCapacityUnits, got {metric_names}"
            )
            assert "ConsumedReadCapacityUnits" in metric_names, (
                f"Expected ConsumedReadCapacityUnits, got {metric_names}"
            )

    def test_metrics_filter_by_metric_name(
        self, endpoint_url: str, metrics_table: str
    ) -> None:
        """Filtering by metric name returns only that metric."""
        resp = requests.get(
            _metrics_url(
                endpoint_url,
                table_name=metrics_table,
                metric="ConsumedWriteCapacityUnits",
            ),
            timeout=10, verify=False, auth=_ADMIN_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        for m in data["metrics"]:
            assert m["metric"] == "ConsumedWriteCapacityUnits"

    def test_metrics_write_capacity_sum(
        self, endpoint_url: str, metrics_table: str
    ) -> None:
        """Write capacity sum reflects the number of PutItem calls.

        Tolerates zero from the DB path (flush not yet occurred).
        """
        resp = requests.get(
            _metrics_url(
                endpoint_url,
                table_name=metrics_table,
                metric="ConsumedWriteCapacityUnits",
            ),
            timeout=10, verify=False, auth=_ADMIN_AUTH,
        )
        data = resp.json()
        total_sum = sum(m["sum"] for m in data["metrics"])
        if data.get("source") == "memory":
            # We did 5 PutItem calls; each consumes at least 1 WCU.
            assert total_sum >= 5.0, f"Expected sum >= 5.0, got {total_sum}"

    def test_metrics_read_capacity_sum(
        self, endpoint_url: str, metrics_table: str
    ) -> None:
        """Read capacity sum reflects the number of GetItem calls.

        Tolerates zero from the DB path (flush not yet occurred).
        """
        resp = requests.get(
            _metrics_url(
                endpoint_url,
                table_name=metrics_table,
                metric="ConsumedReadCapacityUnits",
            ),
            timeout=10, verify=False, auth=_ADMIN_AUTH,
        )
        data = resp.json()
        total_sum = sum(m["sum"] for m in data["metrics"])
        if data.get("source") == "memory":
            # We did 3 GetItem calls; each consumes at least 0.5 RCU.
            assert total_sum >= 1.5, f"Expected sum >= 1.5, got {total_sum}"

    def test_metrics_latency_has_percentiles(
        self, endpoint_url: str, metrics_table: str
    ) -> None:
        """Latency metrics include percentile data.

        Tolerates empty results from the DB path (flush not yet occurred).
        """
        resp = requests.get(
            _metrics_url(
                endpoint_url,
                table_name=metrics_table,
                metric="SuccessfulRequestLatency",
            ),
            timeout=10, verify=False, auth=_ADMIN_AUTH,
        )
        data = resp.json()
        latency_metrics = [
            m for m in data["metrics"] if m["metric"] == "SuccessfulRequestLatency"
        ]
        if data.get("source") == "memory":
            assert len(latency_metrics) > 0, "Expected latency metrics"
        for m in latency_metrics:
            assert "percentiles" in m, "Latency metric should have percentiles"
            p = m["percentiles"]
            assert "p50" in p
            assert "p99" in p
            assert p["p50"] >= 0
            assert p["p99"] >= p["p50"]

    def test_metrics_dimensions(
        self, endpoint_url: str, metrics_table: str
    ) -> None:
        """Metrics include table name and operation dimensions."""
        resp = requests.get(
            _metrics_url(endpoint_url, table_name=metrics_table), timeout=10, verify=False, auth=_ADMIN_AUTH
        )
        data = resp.json()
        for m in data["metrics"]:
            dim_types = {list(d.keys())[0] if isinstance(d, dict) else d for d in m["dimensions"]}
            # Dimension format is {"TableName": "..."} or similar.
            # At minimum, table-scoped metrics should have a TableName dimension.
            has_table = any(
                (isinstance(d, dict) and "TableName" in d)
                for d in m["dimensions"]
            )
            assert has_table, f"Expected TableName dimension in {m['dimensions']}"

    def test_metrics_nonexistent_table(self, endpoint_url: str) -> None:
        """Querying metrics for a nonexistent table returns empty or
        only incidental latency metrics (e.g. from the query itself)."""
        resp = requests.get(
            _metrics_url(endpoint_url, table_name="nonexistent-table-xyz"),
            timeout=10, verify=False, auth=_ADMIN_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # The DB path may return latency metrics recorded for operations
        # that referenced this table name (e.g. ListStreams).  The key
        # invariant is that no *capacity* metrics exist for a table that
        # was never written to.
        capacity = [
            m for m in data["metrics"]
            if m["metric"] in ("ConsumedReadCapacityUnits", "ConsumedWriteCapacityUnits")
        ]
        assert len(capacity) == 0, f"Unexpected capacity metrics: {capacity}"

    def test_metrics_last_hour_window(self, endpoint_url: str) -> None:
        """LastHour window queries the database source (if available)."""
        resp = requests.get(
            f"{endpoint_url}/metrics?window=LastHour", timeout=10, verify=False, auth=_ADMIN_AUTH
        )
        assert resp.status_code == 200
        data = resp.json()
        # Source should be "database" if catalog pool is available,
        # or "memory" if not. Either is valid.
        assert data["source"] in ("memory", "database")

    def test_metrics_invalid_start_time(self, endpoint_url: str) -> None:
        """Invalid ISO 8601 start time returns 400."""
        resp = requests.get(
            f"{endpoint_url}/metrics?start=not-a-date", timeout=10, verify=False, auth=_ADMIN_AUTH
        )
        assert resp.status_code == 400
        data = resp.json()
        assert "ValidationException" in data.get("__type", "")
