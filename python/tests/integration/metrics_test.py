# Copyright 2023-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

from aerospike_async import (
    ClientPolicy,
    CommandType,
    Key,
    LatencyUnit,
    MetricsPolicy,
    ReadPolicy,
    ResultCode,
    Sampler,
    WritePolicy,
    new_client_blocking,
)
from fixtures import TestFixtureCleanDB

NAMESPACE = "test"
SET_NAME = "metrics"


async def _do_some_ops(client, count=5):
    for i in range(count):
        key = Key(NAMESPACE, SET_NAME, f"metrics-{i}")
        await client.put(key, {"n": i}, policy=WritePolicy())
        await client.get(key, policy=ReadPolicy())


class TestMetricsLifecycle(TestFixtureCleanDB):

    async def test_enable_disable_round_trip(self, client):
        assert client.metrics_enabled() is False
        client.enable_metrics()
        assert client.metrics_enabled() is True
        client.disable_metrics()
        assert client.metrics_enabled() is False

    async def test_metrics_before_enable_is_empty_not_error(self, client):
        m = client.metrics()
        assert m.total_nodes >= 1
        assert m.cluster_aggregated.command_histogram(CommandType.GET).count == 0


class TestMetricsSnapshot(TestFixtureCleanDB):

    async def test_snapshot_after_ops(self, client):
        client.enable_metrics()
        await _do_some_ops(client, count=5)

        m = client.metrics()
        assert m.total_nodes >= 1
        assert m.open_connections >= 1
        assert len(m.nodes) == m.total_nodes

        agg = m.cluster_aggregated
        assert agg.latency_unit == LatencyUnit.MICROSECONDS
        assert agg.command_histogram(CommandType.GET).count >= 5
        assert agg.command_histogram(CommandType.PUT).count >= 5
        # NONE has no histogram of its own.
        assert agg.command_histogram(CommandType.NONE) is None

        assert NAMESPACE in agg.detailed_namespaces()
        detail = agg.detailed_metric(NAMESPACE, CommandType.GET)
        assert detail is not None
        assert detail.latency.count >= 5
        assert detail.bytes_sent.count >= 5
        assert detail.bytes_received.count >= 5

        assert agg.result_code_count(NAMESPACE, CommandType.GET, ResultCode.OK) >= 5
        assert agg.result_code_count(NAMESPACE, CommandType.DELETE, ResultCode.OK) == 0

    async def test_snapshot_accumulates_across_calls(self, client):
        client.enable_metrics()
        await _do_some_ops(client, count=3)
        first = client.metrics().cluster_aggregated.command_histogram(CommandType.GET).count
        await _do_some_ops(client, count=3)
        second = client.metrics().cluster_aggregated.command_histogram(CommandType.GET).count
        # Snapshots are cumulative, not since-last-call.
        assert first >= 3
        assert second >= first + 3

    async def test_default_policy_shapes_histograms(self, client):
        client.enable_metrics()
        await _do_some_ops(client, count=2)

        agg = client.metrics().cluster_aggregated
        hist = agg.command_histogram(CommandType.GET)
        assert len(hist.buckets) == 24
        assert hist.count >= 2
        assert sum(hist.buckets) == hist.count

    async def test_millis_policy_shapes_histograms(self, client):
        client.enable_metrics(MetricsPolicy.millis())
        await _do_some_ops(client, count=2)

        agg = client.metrics().cluster_aggregated
        assert agg.latency_unit == LatencyUnit.MILLISECONDS
        hist = agg.command_histogram(CommandType.GET)
        assert len(hist.buckets) == 7
        assert hist.count >= 2
        assert sum(hist.buckets) == hist.count

    async def test_millis_detailed_metrics_survive_aggregation(self, client):
        client.enable_metrics(MetricsPolicy.millis())
        await _do_some_ops(client, count=3)

        agg = client.metrics().cluster_aggregated
        detail = agg.detailed_metric(NAMESPACE, CommandType.GET)
        assert detail is not None
        assert detail.latency.count >= 3
        assert len(detail.latency.buckets) == 7

    async def test_never_sampler_gates_extended_metrics(self, client):
        policy = MetricsPolicy()
        policy.sampler = Sampler.never()
        client.enable_metrics(policy)
        await _do_some_ops(client, count=3)

        agg = client.metrics().cluster_aggregated
        assert agg.command_histogram(CommandType.GET).count == 0
        assert agg.detailed_metric(NAMESPACE, CommandType.GET) is None

    async def test_to_dict_uses_stable_serialized_names(self, client):
        client.enable_metrics()
        await _do_some_ops(client, count=2)

        d = client.metrics().to_dict()
        assert d["total-nodes"] >= 1
        assert "open-connections" in d
        assert "exceeded-max-retries" in d
        assert "exceeded-total-timeout" in d

        agg = d["cluster-aggregated-metrics"]
        assert agg["latency-unit"] == "us"
        get_hist = agg["get-metrics"]
        assert set(get_hist) == {"buckets", "min", "max", "sum", "count"}
        assert get_hist["count"] >= 2
        assert agg["detailed-metrics"][NAMESPACE]["Get"]["latency"]["count"] >= 2
        assert agg["detailed-resultcode-counts"][NAMESPACE]["Get"]["ok"] >= 2

    async def test_labels_carried_on_snapshot(self, client):
        policy = MetricsPolicy()
        policy.labels = [{"team": "billing"}]
        client.enable_metrics(policy)
        await _do_some_ops(client, count=1)

        agg = client.metrics().cluster_aggregated
        # Snapshot labels are the user labels merged with the reserved
        # per-node identity labels (node/host/cluster/app-id).
        assert any(entry.get("team") == "billing" for entry in agg.labels)
        assert any("node" in entry for entry in agg.labels)


def test_local_client_metrics(aerospike_host, use_services_alternate):
    """The metrics surface works on the per-thread local client."""
    from aerospike_async import _LocalClient

    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    # No explicit close: the local client owns its runtime and shuts down on drop.
    client = _LocalClient(cp, aerospike_host)

    assert client.metrics_enabled() is False
    client.enable_metrics(MetricsPolicy.micros())
    assert client.metrics_enabled() is True

    key = Key(NAMESPACE, SET_NAME, "metrics-local")
    client.put_blocking(key, {"n": 1}, policy=WritePolicy())
    client.get_blocking(key, policy=ReadPolicy())

    agg = client.metrics().cluster_aggregated
    assert agg.latency_unit == LatencyUnit.MICROSECONDS
    assert agg.command_histogram(CommandType.GET).count >= 1
    client.disable_metrics()


def test_blocking_client_metrics(aerospike_host, use_services_alternate):
    """The metrics surface works on blocking-constructed clients too."""
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = new_client_blocking(cp, aerospike_host)
    try:
        assert client.metrics_enabled() is False
        client.enable_metrics()

        key = Key(NAMESPACE, SET_NAME, "metrics-blocking")
        client.put_blocking(key, {"n": 1}, policy=WritePolicy())
        client.get_blocking(key, policy=ReadPolicy())

        agg = client.metrics().cluster_aggregated
        assert agg.command_histogram(CommandType.GET).count >= 1
        client.disable_metrics()
        assert client.metrics_enabled() is False
    finally:
        client.close_blocking()
