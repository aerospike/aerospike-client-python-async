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

"""
Vector search — Top-K ordering and vector-distance expressions, executed
against a live server.

Top-K runs client-side. These tests require VECTOR support.

Generic vector expression reads are not covered here.
"""

import pytest
import pytest_asyncio

from aerospike_async import (
    ClientPolicy,
    ExpOperation,
    ExpReadFlags,
    Key,
    Operation,
    Order,
    OrderByType,
    PartitionFilter,
    QueryPolicy,
    Statement,
    Vector,
    VectorElementType,
    WritePolicy,
    FilterExpression as fe,
    new_client,
)


@pytest_asyncio.fixture(autouse=True)
async def _skip_without_vector_support(supports_vector_bins):
    if not supports_vector_bins:
        pytest.skip("cluster does not support VECTOR bins (requires a dev server build)")


@pytest_asyncio.fixture
async def search_client(aerospike_host, use_services_alternate):
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)
    key = Key("test", "vector_search", "vs")
    wp = WritePolicy()
    await client.delete(key, policy=wp)
    try:
        yield client, key, wp
    finally:
        await client.delete(key, policy=wp)
        await client.close()


async def _drain(recordset):
    out = []
    async for rec in recordset:
        out.append(rec)
    return out


class TestVectorTopKScalar:
    async def test_topk_orders_and_limits_scalar_bin(self, search_client):
        """Top-K returns the client-reduced result in the requested order."""
        client, _key, wp = search_client
        ns, setname = "test", "vector_search"
        keys = [Key(ns, setname, f"topk-{i}") for i in range(5)]
        for i, k in enumerate(keys):
            await client.delete(k, policy=wp)
            await client.put(k, {"score": i * 10}, policy=wp)
        try:
            stmt = Statement(ns, setname, ["score"])
            stmt.set_order_by("score", OrderByType.INTEGER, Order.DESC)
            stmt.set_top_k(3)

            rs = await client.query(stmt, PartitionFilter.all(), policy=QueryPolicy())
            scores = [r.bins["score"] for r in await _drain(rs)]

            # Observed contract: correctly ordered (desc) and limited to k=3.
            assert scores == [40, 30, 20]
        finally:
            for k in keys:
                await client.delete(k, policy=wp)


class TestVectorDistanceExpressions:
    """Distance expressions against VECTOR bins."""

    async def _seed(self, client, key, wp, values=(0.1, 0.2, 0.3, 0.4)):
        await client.put(
            key,
            {"embedding": Vector(list(values), VectorElementType.FLOAT32)},
            policy=wp,
        )

    # Distance of the stored vector to *itself* has a known closed form per
    # metric (mirrors the Rust core's distance-to-self tests):
    #   euclidean_squared_distance -> 0
    #   dot_product                -> sum of squares (0.1^2+..+0.4^2 = 0.30)
    #   cosine_similarity          -> 1
    @pytest.mark.parametrize(
        "metric, expected",
        [
            ("euclidean_squared_distance", 0.0),
            ("dot_product", 0.30),
            ("cosine_similarity", 1.0),
        ],
    )
    async def test_distance_to_self_has_known_value(self, search_client, metric, expected):
        client, key, wp = search_client
        await self._seed(client, key, wp)

        query = Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32)
        distance = getattr(fe, metric)(query, fe.vector_bin("embedding"))

        stmt = Statement("test", "vector_search")
        stmt.set_operations([ExpOperation.read("distance", distance)])

        rs = await client.query(stmt, PartitionFilter.all(), policy=QueryPolicy())
        records = await _drain(rs)
        assert len(records) == 1
        assert records[0].bins["distance"] == pytest.approx(expected, abs=1e-3)

    async def test_euclidean_squared_distance_is_sum_of_squared_diffs(self, search_client):
        """Squared L2 between [0, 0] and [3, 4] is 3^2 + 4^2 = 25 (mirrors the
        Rust core's ``euclidean_squared_distance_is_sum_of_squared_differences``)."""
        client, key, wp = search_client
        await self._seed(client, key, wp, values=(0.0, 0.0))

        query = Vector([3.0, 4.0], VectorElementType.FLOAT32)
        distance = fe.euclidean_squared_distance(query, fe.vector_bin("embedding"))

        stmt = Statement("test", "vector_search")
        stmt.set_operations([ExpOperation.read("distance", distance)])

        rs = await client.query(stmt, PartitionFilter.all(), policy=QueryPolicy())
        records = await _drain(rs)
        assert len(records) == 1
        assert records[0].bins["distance"] == pytest.approx(25.0, abs=1e-3)

    async def test_incomparable_distance_returns_no_result_with_no_fail(self, search_client):
        client, key, wp = search_client
        query = Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32)
        distance = fe.euclidean_squared_distance(query, fe.vector_bin("embedding"))

        for value in (
            1,
            Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT64),
            Vector([0.1, 0.2], VectorElementType.FLOAT32),
        ):
            await client.put(key, {"embedding": value}, policy=wp)
            rec = await client.operate(
                key,
                [ExpOperation.read("distance", distance, ExpReadFlags.EVAL_NO_FAIL)],
                policy=wp,
            )
            assert rec.bins.get("distance") is None


class TestVectorTopKWithDistance:
    """Project squared distance and reduce to the nearest records."""

    async def test_topk_by_euclidean_distance(self, search_client):
        client, _key, wp = search_client
        ns, setname = "test", "vector_search_knn"
        keys = [Key(ns, setname, f"knn-{i}") for i in range(6)]

        for i, key in enumerate(keys):
            await client.delete(key, policy=wp)
            await client.put(
                key,
                {
                    "id": i,
                    "embedding": Vector([float(i), 0.0], VectorElementType.FLOAT32),
                },
                policy=wp,
            )

        try:
            query = Vector([0.0, 0.0], VectorElementType.FLOAT32)
            distance = fe.euclidean_squared_distance(query, fe.vector_bin("embedding"))
            stmt = Statement(ns, setname)
            stmt.set_operations(
                [
                    Operation.get_bin("id"),
                    ExpOperation.read("distance", distance),
                ]
            )
            stmt.set_order_by("distance", OrderByType.DOUBLE, Order.ASC)
            stmt.set_top_k(3)

            rs = await client.query(stmt, PartitionFilter.all(), policy=QueryPolicy())
            results = [(rec.bins["id"], rec.bins["distance"]) for rec in await _drain(rs)]
            assert results == [(0, pytest.approx(0.0)), (1, pytest.approx(1.0)), (2, pytest.approx(4.0))]
        finally:
            for key in keys:
                await client.delete(key, policy=wp)
