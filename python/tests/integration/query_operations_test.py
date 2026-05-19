# Copyright 2026 Aerospike, Inc.
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

"""Integration tests for ``Statement.set_operations`` (ops projection).

Backward-compat cases (``Operation.get_bin``) run against any 8.1.x server
via ``tqo_client``. Cases that attach non-basic-read ops
(``ExpOperation.read``, CDT reads) require server >= 8.1.2 and consume
``tqo_client_812``, which auto-routes to ``AEROSPIKE_HOST_8_1_2`` when the
env var is set and skips the dependent test cleanly when it isn't.
Negative cases (write/touch/delete in foreground queries, read-only in
background execute, etc.) are version-agnostic and run on
``AEROSPIKE_HOST``. The pre-8.1.2 client-side gate test inverts the
version check so it only runs when ``AEROSPIKE_HOST`` itself is < 8.1.2.

Setting both ``AEROSPIKE_HOST`` (pre-8.1.2) and ``AEROSPIKE_HOST_8_1_2``
exercises every branch in a single ``pytest`` invocation.
"""

import pytest
import pytest_asyncio

# Fixtures here are session-loop-scoped (clients live longer than one test);
# tests must run on the same session loop or the per-Client owning-loop guard
# in PAC's completion bridge fires.
pytestmark = pytest.mark.asyncio(loop_scope="session")

from aerospike_async import (
    ClientPolicy,
    CollectionIndexType,
    ExpOperation,
    ExpReadFlags,
    ExpWriteFlags,
    Filter,
    FilterExpression as Exp,
    IndexType,
    Key,
    MapOperation,
    MapReturnType,
    Operation,
    PartitionFilter,
    QueryPolicy,
    Statement,
    WritePolicy,
    new_client,
)


_NAMESPACE = "test"
_SET = "tqo"
_INDEX = "tqo_idx_b1"
_KEY_PREFIX = "tqokey"
_BIN1 = "tqobin1"
_BIN2 = "tqobin2"
_BIN3 = "tqobin3"
_MAP_BIN = "tqomapbin"
_SIZE = 20


async def _seed_query_dataset(client):
    """Create the secondary index and the 20-record test dataset on ``client``.

    Used by both ``tqo_client`` and ``tqo_client_812`` so the broad-surface
    seed and the 8.1.2-only seed see the same shape.
    """
    try:
        await client.truncate(_NAMESPACE, _SET)
    except Exception:
        # Truncate may fail on permission-restricted clusters; not fatal here.
        pass

    try:
        task = await client.drop_index(_NAMESPACE, _SET, _INDEX)
        await task.wait_till_complete()
    except Exception:
        pass
    await client.create_index(
        _NAMESPACE,
        _SET,
        _BIN1,
        _INDEX,
        IndexType.NUMERIC,
        cit=CollectionIndexType.DEFAULT,
    )

    wp = WritePolicy()
    for i in range(1, _SIZE + 1):
        key = Key(_NAMESPACE, _SET, f"{_KEY_PREFIX}{i}")
        await client.put(
            wp,
            key,
            {
                _BIN1: i,
                _BIN2: i * 10,
                _BIN3: i * 100,
                _MAP_BIN: {"a": i, "b": i * 10},
            },
        )


async def _drop_query_index(client):
    try:
        task = await client.drop_index(_NAMESPACE, _SET, _INDEX)
        await task.wait_till_complete()
    except Exception:
        pass


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def tqo_client(aerospike_host, use_services_alternate):
    """Module-scoped client + populated dataset on the broad-surface seed.

    A 20-record dataset with three int bins and a small map is created once
    per module; each test reads from it via a secondary-index range over
    ``binName1``. Keeps individual tests fast. Tests that exercise
    server-8.1.2-only ops projection should consume ``tqo_client_812``
    instead so they auto-route to the 8.1.2+ cluster.
    """
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)
    await _seed_query_dataset(client)
    yield client
    await _drop_query_index(client)
    await client.close()


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def tqo_client_812(aerospike_host_8_1_2, use_services_alternate):
    """Module-scoped client + populated dataset on the 8.1.2+ seed.

    Mirrors ``tqo_client`` but connects to ``AEROSPIKE_HOST_8_1_2``. We
    skip at module scope when the env var is unset (rather than going
    through ``aerospike_host_812_required``, which is function-scoped) so
    the dataset isn't seeded against the wrong cluster.
    """
    if not aerospike_host_8_1_2:
        pytest.skip(
            "AEROSPIKE_HOST_8_1_2 is unset; tests in this module that need "
            "an 8.1.2+ cluster require it. Set the env var in aerospike.env "
            "to enable."
        )
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host_8_1_2)
    await _seed_query_dataset(client)
    yield client
    await _drop_query_index(client)
    await client.close()


def _stmt(*, with_filter=None):
    """Build a Statement against the test dataset, optionally with a filter."""
    s = Statement(_NAMESPACE, _SET, None)
    if with_filter is not None:
        s.filters = [with_filter]
    return s


async def _drain(rs):
    """Materialize records from a RecordSet into a list, then close."""
    out = []
    async for rec in rs:
        out.append(rec)
    return out


# =====================================================================
# Backward-compat: basic Operation.get_bin works on any 8.1.x server.
# =====================================================================


class TestQueryOpsBackwardCompat:

    async def test_query_with_get_operation(self, tqo_client, wait_for_index):
        """``Operation.get_bin`` projection over a SI range is the simplest path."""
        begin, end = 1, 5
        flt = Filter.range(_BIN1, begin, end)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([Operation.get_bin(_BIN1)])

        rs = await tqo_client.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) == end - begin + 1
        for r in records:
            v1 = r.bins.get(_BIN1)
            assert begin <= v1 <= end
            # Projection picked up only binName1.
            assert r.bins.get(_BIN2) is None

    async def test_query_operations_take_precedence_over_bin_names(
        self, tqo_client, wait_for_index
    ):
        """When both ``bins`` and ``operations`` are set, operations win."""
        begin, end = 1, 5
        flt = Filter.range(_BIN1, begin, end)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = Statement(_NAMESPACE, _SET, [_BIN1, _BIN2, _BIN3])
        stmt.filters = [flt]
        stmt.set_operations([Operation.get_bin(_BIN1)])

        rs = await tqo_client.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) == end - begin + 1
        for r in records:
            assert r.bins.get(_BIN1) is not None
            # Server honored ops projection, ignored bin name list.
            assert r.bins.get(_BIN2) is None
            assert r.bins.get(_BIN3) is None


# =====================================================================
# 8.1.2+: extended-read ops projection. Skipped on older servers.
# =====================================================================


class TestQueryOpsExt812:

    async def test_query_project_multiple_bins(self, tqo_client_812):
        """Mix basic gets with a CDT read (``MapOperation.get_by_key``)."""
        stmt = _stmt()
        stmt.set_operations([
            Operation.get_bin(_BIN1),
            Operation.get_bin(_BIN2),
            MapOperation.get_by_key(_MAP_BIN, "a", MapReturnType.VALUE),
        ])

        rs = await tqo_client_812.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) >= _SIZE
        for r in records:
            v1 = r.bins[_BIN1]
            v2 = r.bins[_BIN2]
            map_val = r.bins[_MAP_BIN]
            assert v2 == v1 * 10
            assert map_val == v1
            assert r.bins.get(_BIN3) is None

    async def test_query_project_bins_via_expression_read(self, tqo_client_812):
        """All-three projections via ``ExpOperation.read``."""
        stmt = _stmt()
        stmt.set_operations([
            ExpOperation.read("result1", Exp.int_bin(_BIN1), ExpReadFlags.DEFAULT),
            ExpOperation.read("result2", Exp.int_bin(_BIN2), ExpReadFlags.DEFAULT),
            ExpOperation.read("result3", Exp.int_bin(_BIN3), ExpReadFlags.DEFAULT),
        ])
        rs = await tqo_client_812.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) >= _SIZE
        for r in records:
            r1 = r.bins["result1"]
            assert r.bins["result2"] == r1 * 10
            assert r.bins["result3"] == r1 * 100

    async def test_query_project_bins_via_expression_read_with_filter(
        self, tqo_client_812, wait_for_index
    ):
        """ExpOperation.read alongside a SI ``Filter.range``."""
        begin, end = 1, 10
        flt = Filter.range(_BIN1, begin, end)
        await wait_for_index(tqo_client_812, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([
            ExpOperation.read("result1", Exp.int_bin(_BIN1), ExpReadFlags.DEFAULT),
            ExpOperation.read("result2", Exp.int_bin(_BIN2), ExpReadFlags.DEFAULT),
            ExpOperation.read("result3", Exp.int_bin(_BIN3), ExpReadFlags.DEFAULT),
        ])
        rs = await tqo_client_812.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) == end - begin + 1
        for r in records:
            r1 = r.bins["result1"]
            assert begin <= r1 <= end
            assert r.bins["result2"] == r1 * 10
            assert r.bins["result3"] == r1 * 100

    async def test_query_project_mixed_get_and_expression_read(
        self, tqo_client_812, wait_for_index
    ):
        """Mixed Operation.get_bin + ExpOperation.read on a SI range."""
        begin, end = 1, 10
        flt = Filter.range(_BIN1, begin, end)
        await wait_for_index(tqo_client_812, _NAMESPACE, _SET, flt, bins=[_BIN1])
        sum_exp = Exp.num_add([Exp.int_bin(_BIN1), Exp.int_bin(_BIN2)])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([
            Operation.get_bin(_BIN1),
            ExpOperation.read("sum", sum_exp, ExpReadFlags.DEFAULT),
        ])
        rs = await tqo_client_812.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) == end - begin + 1
        for r in records:
            v1 = r.bins[_BIN1]
            assert begin <= v1 <= end
            # binName2 = binName1 * 10, so sum = v1 + v1 * 10
            assert r.bins["sum"] == v1 + v1 * 10
            # Other bins should not have been projected.
            assert r.bins.get(_BIN2) is None
            assert r.bins.get(_BIN3) is None

    async def test_query_with_exp_read_operation(self, tqo_client_812, wait_for_index):
        """ExpOperation.read multiplying a bin by a literal value."""
        begin, end = 1, 10
        flt = Filter.range(_BIN1, begin, end)
        await wait_for_index(tqo_client_812, _NAMESPACE, _SET, flt, bins=[_BIN1])
        exp = Exp.num_mul([Exp.int_bin(_BIN1), Exp.int_val(100)])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([
            Operation.get_bin(_BIN1),
            ExpOperation.read("computed", exp, ExpReadFlags.DEFAULT),
        ])
        rs = await tqo_client_812.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) == end - begin + 1
        for r in records:
            assert r.bins["computed"] == r.bins[_BIN1] * 100

    async def test_query_with_multiple_exp_read_operations(
        self, tqo_client_812, wait_for_index
    ):
        """Two ExpOperation.read ops alongside two Operation.get_bin ops."""
        begin, end = 5, 15
        flt = Filter.range(_BIN1, begin, end)
        await wait_for_index(tqo_client_812, _NAMESPACE, _SET, flt, bins=[_BIN1])
        sum_exp = Exp.num_add([Exp.int_bin(_BIN1), Exp.int_bin(_BIN2)])
        diff_exp = Exp.num_sub([Exp.int_bin(_BIN2), Exp.int_bin(_BIN1)])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([
            Operation.get_bin(_BIN1),
            Operation.get_bin(_BIN2),
            ExpOperation.read("sum", sum_exp, ExpReadFlags.DEFAULT),
            ExpOperation.read("diff", diff_exp, ExpReadFlags.DEFAULT),
        ])
        rs = await tqo_client_812.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) == end - begin + 1
        for r in records:
            v1, v2 = r.bins[_BIN1], r.bins[_BIN2]
            assert r.bins["sum"] == v1 + v2
            assert r.bins["diff"] == v2 - v1

    async def test_query_with_exp_read_and_filter_exp(
        self, tqo_client_812, wait_for_index
    ):
        """ExpOperation.read together with a QueryPolicy filter expression."""
        begin, end = 1, 20
        flt = Filter.range(_BIN1, begin, end)
        await wait_for_index(tqo_client_812, _NAMESPACE, _SET, flt, bins=[_BIN1])

        doubled_exp = Exp.num_mul([Exp.int_bin(_BIN1), Exp.int_val(2)])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([
            Operation.get_bin(_BIN1),
            ExpOperation.read("doubled", doubled_exp, ExpReadFlags.DEFAULT),
        ])

        qp = QueryPolicy()
        qp.filter_expression = Exp.lt(Exp.int_bin(_BIN1), Exp.int_val(6))

        rs = await tqo_client_812.query(qp, PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) == 5
        for r in records:
            v1 = r.bins[_BIN1]
            assert v1 < 6
            assert r.bins["doubled"] == v1 * 2

    async def test_query_with_exp_read_no_filter(self, tqo_client_812):
        """ExpOperation.read without any filter — full-set scan."""
        offset_exp = Exp.num_add([Exp.int_bin(_BIN1), Exp.int_val(1000)])
        stmt = _stmt()
        stmt.set_operations([
            ExpOperation.read("offset", offset_exp, ExpReadFlags.DEFAULT),
        ])
        rs = await tqo_client_812.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) >= _SIZE
        for r in records:
            assert r.bins["offset"] is not None

    async def test_query_with_exp_read_conditional(
        self, tqo_client_812, wait_for_index
    ):
        """Conditional ExpOperation.read produces ``high``/``low`` per record."""
        begin, end = 1, 20
        flt = Filter.range(_BIN1, begin, end)
        await wait_for_index(tqo_client_812, _NAMESPACE, _SET, flt, bins=[_BIN1])

        cond_exp = Exp.cond([
            Exp.gt(Exp.int_bin(_BIN1), Exp.int_val(10)),
            Exp.string_val("high"),
            Exp.string_val("low"),
        ])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([
            Operation.get_bin(_BIN1),
            ExpOperation.read("category", cond_exp, ExpReadFlags.DEFAULT),
        ])

        rs = await tqo_client_812.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        high = sum(1 for r in records if r.bins["category"] == "high")
        low = sum(1 for r in records if r.bins["category"] == "low")
        assert high == 10
        assert low == 10
        for r in records:
            v = r.bins[_BIN1]
            assert r.bins["category"] == ("high" if v > 10 else "low")

    async def test_query_with_exp_read_eval_no_fail(
        self, tqo_client_812, wait_for_index
    ):
        """``ExpReadFlags.EVAL_NO_FAIL`` lets the read tolerate a missing bin."""
        begin, end = 1, 5
        flt = Filter.range(_BIN1, begin, end)
        await wait_for_index(tqo_client_812, _NAMESPACE, _SET, flt, bins=[_BIN1])

        # ``nonexistent`` is not present on any record; without EVAL_NO_FAIL
        # the read would error out.
        nonexistent_exp = Exp.int_bin("nonexistent")
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([
            Operation.get_bin(_BIN1),
            ExpOperation.read("result", nonexistent_exp, ExpReadFlags.EVAL_NO_FAIL),
        ])

        rs = await tqo_client_812.query(QueryPolicy(), PartitionFilter.all(), stmt)
        records = await _drain(rs)
        assert len(records) == end - begin + 1
        for r in records:
            assert r.bins.get(_BIN1) is not None


# =====================================================================
# Negative cases — version-agnostic, always run.
# =====================================================================


class TestQueryOpsRejects:

    async def test_query_rejects_write_operation(self, tqo_client, wait_for_index):
        """``Operation.put`` in a foreground query is rejected."""
        flt = Filter.range(_BIN1, 1, 5)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([Operation.put("foo", "bar")])

        with pytest.raises(Exception) as excinfo:
            rs = await tqo_client.query(QueryPolicy(), PartitionFilter.all(), stmt)
            await _drain(rs)
        # Server returns PARAMETER_ERROR with a "read-only" hint.
        assert "read-only" in str(excinfo.value).lower() or "parameter" in str(excinfo.value).lower()

    async def test_query_rejects_exp_write_operation(
        self, tqo_client, wait_for_index
    ):
        """``ExpOperation.write`` in a foreground query is rejected."""
        flt = Filter.range(_BIN1, 1, 5)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([
            ExpOperation.write("foo", Exp.string_val("bar"), ExpWriteFlags.DEFAULT),
        ])
        with pytest.raises(Exception) as excinfo:
            rs = await tqo_client.query(QueryPolicy(), PartitionFilter.all(), stmt)
            await _drain(rs)
        msg = str(excinfo.value).lower()
        assert "read-only" in msg or "parameter" in msg

    async def test_query_rejects_mixed_read_write_operations(
        self, tqo_client, wait_for_index, server_version
    ):
        """Mixed read+write in a foreground query is rejected.

        On 8.1.2+ the server returns a "read-only" message; on pre-8.1.2 the
        client-side basic-read gate fires first ("basic read operations").
        Either is acceptable.
        """
        flt = Filter.range(_BIN1, 1, 5)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([
            ExpOperation.read("computed", Exp.int_bin(_BIN1), ExpReadFlags.DEFAULT),
            ExpOperation.write("foo", Exp.string_val("updated"), ExpWriteFlags.DEFAULT),
        ])
        with pytest.raises(Exception) as excinfo:
            rs = await tqo_client.query(QueryPolicy(), PartitionFilter.all(), stmt)
            await _drain(rs)
        msg = str(excinfo.value).lower()
        if server_version is not None and server_version >= (8, 1, 2, 0):
            assert "read-only" in msg or "parameter" in msg
        else:
            assert "basic read operations" in msg or "read-only" in msg or "parameter" in msg

    async def test_query_rejects_touch_operation(self, tqo_client, wait_for_index):
        """``Operation.touch()`` in a foreground query is rejected."""
        flt = Filter.range(_BIN1, 1, 5)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([Operation.touch()])
        with pytest.raises(Exception) as excinfo:
            rs = await tqo_client.query(QueryPolicy(), PartitionFilter.all(), stmt)
            await _drain(rs)
        msg = str(excinfo.value).lower()
        assert "read-only" in msg or "parameter" in msg

    async def test_query_rejects_delete_operation(self, tqo_client, wait_for_index):
        """``Operation.delete()`` in a foreground query is rejected."""
        flt = Filter.range(_BIN1, 1, 5)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)
        stmt.set_operations([Operation.delete()])
        with pytest.raises(Exception) as excinfo:
            rs = await tqo_client.query(QueryPolicy(), PartitionFilter.all(), stmt)
            await _drain(rs)
        msg = str(excinfo.value).lower()
        assert "read-only" in msg or "parameter" in msg

    async def test_execute_rejects_read_only_operations(
        self, tqo_client, wait_for_index
    ):
        """``client.query_operate`` (background) rejects read-only ops."""
        flt = Filter.range(_BIN1, 1, 5)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)
        with pytest.raises(Exception) as excinfo:
            task = await tqo_client.query_operate(
                WritePolicy(),
                stmt,
                [
                    ExpOperation.read(
                        "computed", Exp.int_bin(_BIN1), ExpReadFlags.DEFAULT
                    )
                ],
            )
            # If the client returns a task instead of raising, await its
            # completion to surface the server-side error.
            if task is not None:
                await task.wait_till_complete()
        msg = str(excinfo.value).lower()
        assert "write" in msg or "parameter" in msg

    async def test_execute_rejects_get_operation(self, tqo_client, wait_for_index):
        """``client.query_operate`` rejects basic ``Operation.get_bin`` too."""
        flt = Filter.range(_BIN1, 1, 5)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)
        with pytest.raises(Exception) as excinfo:
            task = await tqo_client.query_operate(
                WritePolicy(), stmt, [Operation.get_bin(_BIN1)]
            )
            if task is not None:
                await task.wait_till_complete()
        msg = str(excinfo.value).lower()
        assert "write" in msg or "parameter" in msg

    async def test_execute_rejects_mixed_read_write_operations(
        self, tqo_client, wait_for_index
    ):
        """Mixed read+write in background execute is rejected."""
        flt = Filter.range(_BIN1, 1, 5)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)
        with pytest.raises(Exception) as excinfo:
            task = await tqo_client.query_operate(
                WritePolicy(),
                stmt,
                [
                    ExpOperation.read(
                        "computed", Exp.int_bin(_BIN1), ExpReadFlags.DEFAULT
                    ),
                    ExpOperation.write(
                        "tag", Exp.string_val("mixed"), ExpWriteFlags.DEFAULT
                    ),
                ],
            )
            if task is not None:
                await task.wait_till_complete()
        msg = str(excinfo.value).lower()
        assert "write-only" in msg or "write" in msg or "parameter" in msg

    async def test_execute_with_write_operation_succeeds(
        self, tqo_client, wait_for_index
    ):
        """Background execute with a write op succeeds and applies."""
        begin, end = 1, 3
        flt = Filter.range(_BIN1, begin, end)
        await wait_for_index(tqo_client, _NAMESPACE, _SET, flt, bins=[_BIN1])
        stmt = _stmt(with_filter=flt)

        task = await tqo_client.query_operate(
            WritePolicy(),
            stmt,
            [
                ExpOperation.write(
                    "marker", Exp.string_val("executed"), ExpWriteFlags.DEFAULT
                )
            ],
        )
        await task.wait_till_complete()

        # Verify each touched record now has the marker bin.
        from aerospike_async import ReadPolicy
        rp = ReadPolicy()
        for i in range(begin, end + 1):
            key = Key(_NAMESPACE, _SET, f"{_KEY_PREFIX}{i}")
            rec = await tqo_client.get(rp, key)
            assert rec.bins.get("marker") == "executed"

        # Cleanup: remove the marker so subsequent tests/runs see clean data.
        wp = WritePolicy()
        for i in range(begin, end + 1):
            key = Key(_NAMESPACE, _SET, f"{_KEY_PREFIX}{i}")
            await tqo_client.put(wp, key, {"marker": None})


# =====================================================================
# Pre-8.1.2 client-side gate (driven by the Rust core's per-node version
# check). Only meaningful on a pre-8.1.2 cluster; skipped on 8.1.2+.
# =====================================================================


class TestQueryOpsPre812Gate:

    async def test_extended_read_rejected_on_pre_8_1_2(
        self, tqo_client, server_version, supports_query_ops_projection_ext
    ):
        """The core's wire encoder rejects extended reads with a clear message.

        Negative companion to :class:`TestQueryOpsExt812`; only runs against
        a server that doesn't yet support the extended projection.
        """
        if server_version is None:
            pytest.skip("Could not detect server version")
        if supports_query_ops_projection_ext:
            pytest.skip(
                "Server >= 8.1.2 accepts extended reads; "
                "this test exercises the pre-8.1.2 gate"
            )

        stmt = _stmt()
        stmt.set_operations([
            ExpOperation.read("computed", Exp.int_bin(_BIN1), ExpReadFlags.DEFAULT),
        ])
        with pytest.raises(Exception) as excinfo:
            rs = await tqo_client.query(QueryPolicy(), PartitionFilter.all(), stmt)
            await _drain(rs)
        msg = str(excinfo.value)
        assert "basic read operations" in msg.lower()
        assert "8.1.2" in msg
