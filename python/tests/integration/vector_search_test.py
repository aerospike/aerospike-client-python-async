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

The client-side *build* surface for all of this (constructing distance
expressions and the Top-K statement) is unit-tested separately -- see
``tests/unit/filter_expr_test.py::TestFilterExprVector`` and
``tests/unit/query_test.py::TestStatementTopK``.

Scalar Top-K ("ORDER BY <scalar bin> LIMIT k", no vector expression involved)
runs and passes against the current dev server (8.1.3.0-76), even though
``examples/vector_topk_query.py`` and the README currently document Top-K as
unsupported / failing fast client-side. That contradiction hasn't been
confirmed by the server team, so treat the passing result as informative
rather than a guaranteed contract -- see the comment on
``test_topk_orders_and_limits_scalar_bin`` below.

Everything else in this file -- reading or filtering a vector bin through
*any* expression (a plain read, ``bin_exists``, or a distance metric) --
reaches a real server-side defect: ``rt_bin_translate``
(``aerospike-server/as/src/exp/exp_rt.c``) switches on the stored particle
type and has no ``case AS_PARTICLE_TYPE_VECTOR``, so it falls into
``default: cf_crash(AS_EXP, "unexpected")`` and aborts ``asd``. Every
expression path that loads a vector bin -- a filter, ``bin_exists``,
``bin_type``, or an ``ExpOperation.read`` -- routes through
``rt_load_bin -> rt_bin_translate``, so a plain read and a distance metric hit
the exact same crash; distance is just one more caller on top of it. Per the
server's own ``vector_type_design.md`` the VECTOR particle reuses the BLOB
vtable, and the sibling ``rt_value_translate`` already degrades unknown
particle types gracefully (``AS_EXP_UNK``) instead of crashing, so this looks
like a one-line server fix (give VECTOR the same treatment as BLOB in
``rt_bin_translate``) rather than a fundamental limitation.

Because running any of those tests against an unfixed server takes the whole
node down (not just fails the one request), each is marked
``@pytest.mark.skipif`` on ``AEROSPIKE_RUN_VECTOR_SEARCH=1`` -- the same idea
as the Rust core's own ``#[ignore = "..."]`` on the equivalent tests, just
needing an explicit opt-in env var rather than ``cargo test -- --ignored``.
Enable it only against a server build that carries the ``rt_bin_translate``
fix.
"""

import os

import pytest
import pytest_asyncio

from aerospike_async import (
    ClientPolicy,
    ExpOperation,
    Key,
    Order,
    OrderByType,
    PartitionFilter,
    QueryPolicy,
    ReadPolicy,
    Statement,
    Vector,
    VectorElementType,
    WritePolicy,
    FilterExpression as fe,
    new_client,
)
from aerospike_async.exceptions import FilteredOut


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


_RUN_ENV = "AEROSPIKE_RUN_VECTOR_SEARCH"

# Mirrors the Rust core's `#[ignore = "..."]` on the equivalent tests: these
# reach a server-side crash (see module docstring), so they stay off by
# default and require an explicit opt-in to run against a fixed server build.
_crash_skip = pytest.mark.skipif(
    os.environ.get(_RUN_ENV) != "1",
    reason=(
        "evaluating any expression over a VECTOR bin currently crashes asd "
        "(rt_bin_translate has no AS_PARTICLE_TYPE_VECTOR case, see module "
        f"docstring); set {_RUN_ENV}=1 to run against a server build that "
        "carries the rt_bin_translate fix"
    ),
)


class TestVectorTopKScalar:
    async def test_topk_orders_and_limits_scalar_bin(self, search_client):
        """Scalar-bin Top-K ("ORDER BY <scalar bin> LIMIT k", no vector
        expression involved) is not on the crash path described in the
        module docstring, and passes today: the query returns the correctly
        ordered and limited result set and the node stays healthy.

        This is kept as a normal, un-skipped test, but note its status is
        unconfirmed: the documented contract (``examples/vector_topk_query.py``,
        the README) says Top-K "fails fast client-side with a ValueError,
        regardless of the server," which is empirically false here for scalar
        Top-K. Unknowns pending the server team: whether this is
        *intended*-supported on this build, its minimum version / capability
        gate, and whether the docs are simply stale. Don't treat this as a
        stable guarantee until that's reconciled.
        """
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


@_crash_skip
class TestVectorBinExpressionReadUnsupported:
    """Any expression that evaluates *over* a vector bin -- a plain read, an
    existence check, or a comparison -- crashes an unfixed node. These are
    the general, minimal repros (no distance math involved); see
    ``TestVectorDistanceExpressionsUnsupported`` below for the same defect
    reached via distance metrics instead."""

    async def test_vector_bin_filter_evaluates_to_filtered_out(self, search_client):
        """``eq(vector_bin("v"), blob_val(...))`` against a stored vector must
        evaluate to unknown (FILTERED_OUT) on a fixed server, not abort the
        node. The blob comparand can never equal a VECTOR particle, so the
        correct, non-crashing outcome is FILTERED_OUT."""
        client, key, wp = search_client
        v = Vector([0.1, -2.5, 3.375], VectorElementType.FLOAT32)
        await client.put(key, {"v": v, "scalar": 1}, policy=wp)

        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.vector_bin("v"), fe.blob_val([0]))

        with pytest.raises(FilteredOut):
            await client.get(key, ["scalar"], policy=rp)

        # The node must still be reachable afterwards -- a genuine crash would
        # make this follow-up read fail too.
        rec = await client.get(key, ["scalar"])
        assert rec.bins["scalar"] == 1

    async def test_bin_exists_filter_returns_record(self, search_client):
        """``bin_exists("v")`` is a different entry point into the same
        defect: it compiles to ``bin_type(...) != NULL``, evaluated via
        ``exp_eval_bin_type -> rt_load_bin -> rt_bin_translate``. On a fixed
        server the bin exists, so the predicate is true and the record comes
        back normally (no crash, no FILTERED_OUT)."""
        client, key, wp = search_client
        v = Vector([0.1, -2.5, 3.375], VectorElementType.FLOAT32)
        await client.put(key, {"v": v, "scalar": 3}, policy=wp)

        rp = ReadPolicy()
        rp.filter_expression = fe.bin_exists("v")

        rec = await client.get(key, ["scalar"], policy=rp)
        assert rec.bins["scalar"] == 3

    async def test_expression_read_of_vector_bin_returns_bin(self, search_client):
        """Reading the vector bin back through an ``operate`` read-expression
        (``ExpOperation.read``) exercises the same
        ``rt_load_bin -> rt_bin_translate`` crash from the read side rather
        than a filter. On a fixed server the expression evaluates and the
        result bin comes back."""
        client, key, wp = search_client
        v = Vector([0.5, -1.5, 2.0], VectorElementType.FLOAT32)
        await client.put(key, {"v": v}, policy=wp)

        rec = await client.operate(key, [ExpOperation.read("out", fe.vector_bin("v"))], policy=wp)
        assert "out" in rec.bins


@_crash_skip
class TestVectorDistanceExpressionsUnsupported:
    """Distance expressions used in a query/read. These hit the same
    rt_bin_translate crash as a plain vector-bin read (see module docstring)."""

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

    async def test_distance_filter_expression_on_get(self, search_client):
        client, key, wp = search_client
        await self._seed(client, key, wp)

        query = Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32)
        rp = ReadPolicy()
        rp.filter_expression = fe.gt(
            fe.cosine_similarity(query, fe.vector_bin("embedding")),
            fe.float_val(0.5),
        )
        rec = await client.get(key, policy=rp)
        assert rec is not None


@_crash_skip
class TestVectorTopKWithDistanceUnsupported:
    """The full hybrid flow: project a distance into a bin, then Top-K by it.
    Requires both server-side vector-distance and Top-K support."""

    async def test_topk_by_cosine_similarity(self, search_client):
        client, key, wp = search_client
        await client.put(
            key,
            {"embedding": Vector([0.12, 0.98, 0.44, 0.05], VectorElementType.FLOAT32)},
            policy=wp,
        )

        query = Vector([0.10, 0.95, 0.40, 0.02], VectorElementType.FLOAT32)
        similarity = fe.cosine_similarity(query, fe.vector_bin("embedding"))

        stmt = Statement("test", "vector_search")
        stmt.set_operations([ExpOperation.read("similarity", similarity)])
        stmt.set_order_by("similarity", OrderByType.DOUBLE, Order.DESC)
        stmt.set_top_k(10)

        rs = await client.query(stmt, PartitionFilter.all(), policy=QueryPolicy())
        records = await _drain(rs)
        assert len(records) >= 1
