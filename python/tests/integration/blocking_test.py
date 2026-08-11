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

"""Integration tests for `_blocking` entry points in PAC.

These tests are intentionally synchronous (`def`, not `async def`).
`new_client_blocking()` does not require a running asyncio event loop, and
the `_blocking` siblings (put/get/delete/close/commit/abort/is_connected)
must work the same way.

Covers:
  - Happy path: connect → put → get → delete → close, all blocking.
  - Async-context guard: calling a `_blocking` method from inside a running
    asyncio event loop raises `RuntimeError`.
  - Bridge mismatch guard: a client created via `new_client_blocking` raises
    `RuntimeError` when an async method (e.g. `put`) is used on it.
  - Async client + sync method: a client created via `new_client` (async) can
    still have its `_blocking` siblings called from a sync context. This
    proves the bridge is orthogonal to the blocking surface.
"""

import asyncio
import time

import pytest

from aerospike_async import (
    BatchDeleteOp,
    BatchPolicy,
    BatchReadOp,
    BatchWriteOp,
    ClientPolicy,
    CollectionIndexType,
    Filter,
    IndexType,
    Key,
    Operation,
    PartitionFilter,
    QueryPolicy,
    QuerySelection,
    ReadPolicy,
    Statement,
    WritePolicy,
    new_client,
    new_client_blocking,
)

QSEL_NAMESPACE = "test"
QSEL_SET_NAME = "qsel_blk"
QSEL_AGE_BIN = "age"
QSEL_SCORE_BIN = "score"
QSEL_COUNTRY_BIN = "country"
QSEL_AGE_INDEX = "qsel_blk_age_idx"
QSEL_SCORE_INDEX = "qsel_blk_score_idx"
QSEL_DATASET_SIZE = 50


def _connect_blocking(aerospike_host, use_services_alternate):
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    return new_client_blocking(cp, aerospike_host)


def test_blocking_round_trip(aerospike_host, use_services_alternate):
    """connect → put → get → delete → close, single-threaded blocking."""
    client = _connect_blocking(aerospike_host, use_services_alternate)
    try:
        assert client.is_connected_blocking() is True

        key = Key("test", "blocking", "rt-1")

        client.put_blocking(key, {"name": "alice", "age": 30}, policy=WritePolicy())

        rec = client.get_blocking(key, policy=ReadPolicy())
        assert rec.bins["name"] == "alice"
        assert rec.bins["age"] == 30

        existed = client.delete_blocking(key, policy=WritePolicy())
        assert existed is True

        existed_again = client.delete_blocking(key, policy=WritePolicy())
        assert existed_again is False
    finally:
        client.close_blocking()
        assert client.is_connected_blocking() is False


def test_blocking_async_context_guard(aerospike_host, use_services_alternate):
    """Calling a `_blocking` method from inside `asyncio.run()` raises."""
    client = _connect_blocking(aerospike_host, use_services_alternate)
    try:
        key = Key("test", "blocking", "guard-1")

        async def misuse():
            # We're inside a running asyncio loop now — every `_blocking`
            # variant must refuse to run rather than calling `block_on` from
            # within asyncio (which would deadlock or silently dispatch on a
            # foreign runtime).
            client.put_blocking(key, {"x": 1}, policy=WritePolicy())

        with pytest.raises(RuntimeError, match="async context"):
            asyncio.run(misuse())

        async def misuse_connect():
            new_client_blocking(ClientPolicy(), aerospike_host)

        with pytest.raises(RuntimeError, match="async context"):
            asyncio.run(misuse_connect())
    finally:
        client.close_blocking()


def test_blocking_client_rejects_async_methods(aerospike_host, use_services_alternate):
    """A client built via `new_client_blocking` has no completion bridge,
    so async methods (e.g. `put`) raise a clear RuntimeError instead of
    panicking or hanging."""
    client = _connect_blocking(aerospike_host, use_services_alternate)
    try:
        key = Key("test", "blocking", "bridge-1")

        async def use_async():
            # `client.put(...)` returns an awaitable — but the bridge guard
            # fires synchronously when we call the method, so we never reach
            # the `await`.
            client.put(key, {"x": 1}, policy=WritePolicy())

        with pytest.raises(RuntimeError, match="new_client_blocking"):
            asyncio.run(use_async())
    finally:
        client.close_blocking()


def test_async_client_supports_blocking_methods(aerospike_host, use_services_alternate):
    """A client created via `new_client` (async) can be used with `_blocking`
    methods from a sync context. The bridge is for the async surface; the
    blocking surface bypasses it entirely.

    This test runs the async constructor inside `asyncio.run()`, then exits
    the loop and uses the resulting client from plain sync code.
    """
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate

    # Build via the async constructor inside a loop, then return the client
    # back into sync land for blocking-method usage.
    async def _build():
        return await new_client(cp, aerospike_host)

    client = asyncio.run(_build())
    try:
        key = Key("test", "blocking", "mixed-1")
        client.put_blocking(key, {"v": 42}, policy=WritePolicy())
        rec = client.get_blocking(key, policy=ReadPolicy())
        assert rec.bins["v"] == 42
        client.delete_blocking(key, policy=WritePolicy())
    finally:
        client.close_blocking()


def test_blocking_extended_ops(aerospike_host, use_services_alternate):
    """Sweep across the rest of the blocking surface.

    Touches: add, append, prepend, touch, exists, batch_read, batch_write,
    batch_delete, batch_exists, batch_get_header, query (iter), info,
    node_names, nodes. UDF + admin-only methods are exercised by
    feature-specific suites; this is just a smoke that every variant
    completes a round-trip against a live cluster.
    """
    client = _connect_blocking(aerospike_host, use_services_alternate)
    try:
        wp = WritePolicy()
        rp = ReadPolicy()

        # cluster introspection
        names = client.node_names_blocking()
        assert isinstance(names, list) and len(names) > 0
        nodes = client.nodes_blocking()
        assert len(nodes) == len(names)
        info = client.info_blocking("build")
        assert info  # any non-empty response is fine

        # add / append / prepend / touch / exists
        k = Key("test", "blocking", "ext-1")
        client.put_blocking(k, {"counter": 1, "label": "alpha"}, policy=wp)
        client.add_blocking(k, {"counter": 10}, policy=wp)
        client.append_blocking(k, {"label": "-end"}, policy=wp)
        client.prepend_blocking(k, {"label": "start-"}, policy=wp)
        client.touch_blocking(k, policy=wp)
        rec = client.get_blocking(k, policy=rp)
        assert rec.bins["counter"] == 11
        assert rec.bins["label"] == "start-alpha-end"
        assert client.exists_blocking(k, policy=rp) is True

        # batch read/write/delete/exists/get_header
        keys = [Key("test", "blocking", f"batch-{i}") for i in range(4)]
        bins_list = [{"i": i} for i in range(4)]
        bp = BatchPolicy()
        client.batch_write_blocking([pytest_pyref(k) for k in keys], bins_list, batch_policy=bp, write_policy=None)
        batched = client.batch_read_blocking([pytest_pyref(k) for k in keys], None, batch_policy=bp, read_policy=None)
        assert len(batched) == 4
        for i, br in enumerate(batched):
            assert br.record is not None
            assert br.record.bins["i"] == i
        exists = client.batch_exists_blocking([pytest_pyref(k) for k in keys], batch_policy=bp, read_policy=None)
        assert exists == [True, True, True, True]
        headers = client.batch_get_header_blocking([pytest_pyref(k) for k in keys], batch_policy=bp, read_policy=None)
        assert all(h is not None for h in headers)
        client.batch_delete_blocking([pytest_pyref(k) for k in keys], batch_policy=bp, delete_policy=None)
        exists_after = client.batch_exists_blocking([pytest_pyref(k) for k in keys], batch_policy=bp, read_policy=None)
        assert exists_after == [False, False, False, False]

        # query (iter) — primary-key scan with no filter, just sanity-check
        # that __iter__/__next__ work and yield records we wrote.
        client.put_blocking(Key("test", "blocking", "scan-1"), {"x": 1}, policy=wp)
        client.put_blocking(Key("test", "blocking", "scan-2"), {"x": 2}, policy=wp)
        stmt = Statement("test", "blocking", ["x"])
        recordset = client.query_blocking(stmt, PartitionFilter.all(), policy=QueryPolicy())
        seen = 0
        for _record in recordset:
            seen += 1
            if seen >= 2:
                break
        assert seen >= 2

        # cleanup
        client.delete_blocking(k, policy=wp)
        for kk in keys:
            client.delete_blocking(kk, policy=wp)
        client.delete_blocking(Key("test", "blocking", "scan-1"), policy=wp)
        client.delete_blocking(Key("test", "blocking", "scan-2"), policy=wp)
    finally:
        client.close_blocking()


def pytest_pyref(k):
    """Pass-through helper — PyO3's `Vec<PyRef<Key>>` accepts a list of Key
    directly from Python; this exists only so the call sites read clearly.
    """
    return k


@pytest.mark.skipif(
    __import__("os").environ.get("BLOCKING_PERF_SMOKE") != "1",
    reason="set BLOCKING_PERF_SMOKE=1 to run the 10K put/get latency smoke",
)
def test_blocking_latency_smoke(aerospike_host, use_services_alternate):
    """10K sequential put+get measuring per-op latency.

    Gated by env var because it depends on cluster reachability and the
    timing target is informational, not pass/fail.
    """
    client = _connect_blocking(aerospike_host, use_services_alternate)
    try:
        wp = WritePolicy()
        rp = ReadPolicy()
        key = Key("test", "blocking", "perf-1")

        # Warmup
        for _ in range(100):
            client.put_blocking(key, {"v": 1}, policy=wp)
            client.get_blocking(key, policy=rp)

        n = 10_000
        t0 = time.perf_counter()
        for i in range(n):
            client.put_blocking(key, {"v": i}, policy=wp)
            client.get_blocking(key, policy=rp)
        elapsed = time.perf_counter() - t0

        ops = 2 * n
        per_op_us = (elapsed / ops) * 1_000_000
        tps = ops / elapsed
        print(
            f"\nblocking smoke: {ops} ops in {elapsed:.2f}s — "
            f"{tps:,.0f} TPS, {per_op_us:.1f} µs/op"
        )

        client.delete_blocking(key, policy=wp)
    finally:
        client.close_blocking()


def test_blocking_batch_stream(aerospike_host, use_services_alternate):
    """`batch_stream_blocking` yields each input op's BatchRecord on a sync
    iterator. Items arrive in completion order; we assert set-equality on
    indices and per-key result codes."""
    from aerospike_async.exceptions import ResultCode

    client = _connect_blocking(aerospike_host, use_services_alternate)
    try:
        ns, set_name = "test", "batchstream_blocking"
        keys = [Key(ns, set_name, f"sk{i}") for i in range(4)]
        wp = WritePolicy()
        for i, k in enumerate(keys):
            client.put_blocking(k, {"v": i}, policy=wp)

        ops = [
            BatchReadOp(keys[0], ["v"]),
            BatchWriteOp(keys[1], [Operation.put("v", 99)]),
            BatchReadOp(keys[2], ["v"]),
            BatchDeleteOp(keys[3]),
        ]

        stream = client.batch_stream_blocking(ops)
        yielded = list(stream)

        assert len(yielded) == 4
        by_idx = dict(yielded)
        assert set(by_idx.keys()) == {0, 1, 2, 3}
        for br in by_idx.values():
            assert br.result_code == ResultCode.OK

        # Reads return bins; the write op's record has the post-op bin shape;
        # the delete returns no bins. Just confirm read[0] sees the seed value.
        assert by_idx[0].record.bins["v"] == 0
        assert by_idx[2].record.bins["v"] == 2

        # Cleanup
        for k in keys[:3]:
            client.delete_blocking(k, policy=wp)
    finally:
        client.close_blocking()


def test_blocking_batch_stream_rejects_async_iteration(
    aerospike_host, use_services_alternate,
):
    """A stream built via `batch_stream_blocking` has no CompletionBridge
    (no owning event loop / no per-Client runtime to route through). Async
    iteration on it must refuse rather than silently routing through the
    global Tokio runtime — which would break AsyncPool's per-Client
    runtime isolation invariant for any caller that opted into it."""
    client = _connect_blocking(aerospike_host, use_services_alternate)
    try:
        k = Key("test", "batchstream_blocking", "no_bridge")
        client.put_blocking(k, {"v": 1}, policy=WritePolicy())

        # Build the stream sync-side (outside any event loop). Then attempt
        # async iteration — the stream carries no bridge, so __anext__ must
        # refuse explicitly.
        stream = client.batch_stream_blocking([BatchReadOp(k, None)])

        async def misuse():
            async for _ in stream:
                pass

        with pytest.raises(RuntimeError, match="batch_stream_blocking"):
            asyncio.run(misuse())

        client.delete_blocking(k, policy=WritePolicy())
    finally:
        client.close_blocking()


def test_blocking_batch_stream_async_context_guard(
    aerospike_host, use_services_alternate,
):
    """Iterating a `BatchRecordStream` from inside asyncio.run() raises."""
    client = _connect_blocking(aerospike_host, use_services_alternate)
    try:
        k = Key("test", "batchstream_blocking", "guard")
        client.put_blocking(k, {"v": 1}, policy=WritePolicy())

        # Construct the stream while NOT in an async context (allowed).
        stream = client.batch_stream_blocking([BatchReadOp(k, None)])

        async def misuse():
            # Now iterate from inside an async loop — must refuse.
            for _ in stream:
                pass

        with pytest.raises(RuntimeError, match="async context"):
            asyncio.run(misuse())

        client.delete_blocking(k, policy=WritePolicy())
    finally:
        client.close_blocking()


def test_blocking_batch_stream_exhausted_stops(
    aerospike_host, use_services_alternate,
):
    """Sync sibling of `test_batch_stream_async_exhausted_stops`.

    After the last record, ``__next__`` raises ``StopIteration`` (the
    `for ... in stream:` loop ends naturally). A second iteration over
    the same stream object yields nothing — items already consumed and
    the underlying channel is closed.
    """
    client = _connect_blocking(aerospike_host, use_services_alternate)
    try:
        k = Key("test", "batchstream_blocking_exhaust", "k0")
        client.put_blocking(k, {"v": 1}, policy=WritePolicy())

        stream = client.batch_stream_blocking([BatchReadOp(k, None)])

        count = 0
        for _idx, _br in stream:
            count += 1
        assert count == 1

        # Second pass on the same stream — channel closed; yields nothing.
        second_pass = list(stream)
        assert second_pass == []

        client.delete_blocking(k, policy=WritePolicy())
    finally:
        client.close_blocking()


def _wait_for_index_blocking(client, ns, set_name, sindex_filter, *, bins=None,
                             timeout=5.0, interval=0.25):
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            stmt = Statement(ns, set_name, bins or [])
            stmt.filters = [sindex_filter]
            recordset = client.query_blocking(
                stmt,
                PartitionFilter.all(),
                policy=QueryPolicy(),
            )
            for _ in recordset:
                break
            return
        except Exception as exc:
            if "IndexNotReadable" not in str(exc):
                raise
            last_err = exc
            time.sleep(interval)
    raise last_err  # type: ignore[misc]


def _collect_int_bin_blocking(recordset, bin_name: str) -> list[int]:
    values = []
    for record in recordset:
        values.append(record.bins[bin_name])
    values.sort()
    return values


@pytest.fixture
def qsel_blocking_fixture(aerospike_host, use_services_alternate, supports_query_selection_sync):
    if not supports_query_selection_sync:
        pytest.skip(
            "cluster lacks query selection "
            "(Node.version.supports_query_selection() is False on one or more nodes)"
        )

    client = _connect_blocking(aerospike_host, use_services_alternate)
    wp = WritePolicy()

    try:
        for i in range(1, QSEL_DATASET_SIZE + 1):
            country = "US" if i % 2 == 0 else "CA"
            key = Key(QSEL_NAMESPACE, QSEL_SET_NAME, i)
            client.put_blocking(
                key,
                {QSEL_AGE_BIN: i, QSEL_SCORE_BIN: i, QSEL_COUNTRY_BIN: country},
                policy=wp,
            )

        for bin_name, index_name in (
            (QSEL_AGE_BIN, QSEL_AGE_INDEX),
            (QSEL_SCORE_BIN, QSEL_SCORE_INDEX),
        ):
            client.create_index_blocking(
                QSEL_NAMESPACE,
                QSEL_SET_NAME,
                bin_name,
                index_name,
                IndexType.NUMERIC,
                cit=CollectionIndexType.DEFAULT,
            )

        _wait_for_index_blocking(
            client,
            QSEL_NAMESPACE,
            QSEL_SET_NAME,
            Filter.range(QSEL_AGE_BIN, 0, 100),
            bins=[QSEL_AGE_BIN],
        )

        yield {
            "client": client,
            "set_name": QSEL_SET_NAME,
            "age_index_name": QSEL_AGE_INDEX,
        }
    finally:
        for index_name in (QSEL_AGE_INDEX, QSEL_SCORE_INDEX):
            try:
                task = client.drop_index_blocking(
                    QSEL_NAMESPACE, QSEL_SET_NAME, index_name,
                )
                task.wait_till_complete_blocking()
            except Exception:
                pass
        client.close_blocking()


def test_blocking_explain_selects_secondary_index(qsel_blocking_fixture):
    client = qsel_blocking_fixture["client"]
    set_name = qsel_blocking_fixture["set_name"]
    age_index_name = qsel_blocking_fixture["age_index_name"]

    plan = client.query_explain_blocking(
        QSEL_NAMESPACE,
        "$.age >= 14 and $.age <= 18",
        set_name=set_name,
    )

    assert plan.selection == QuerySelection.SECONDARY_INDEX
    assert plan.is_secondary_index
    assert plan.index_name == age_index_name
    assert isinstance(plan.ael, str) and len(plan.ael) > 0


def test_blocking_execute_returns_matching_records(qsel_blocking_fixture):
    client = qsel_blocking_fixture["client"]
    set_name = qsel_blocking_fixture["set_name"]

    plan = client.query_explain_blocking(
        QSEL_NAMESPACE,
        "$.age >= 14 and $.age <= 18",
        set_name=set_name,
    )
    stmt = Statement(QSEL_NAMESPACE, set_name, [QSEL_AGE_BIN])
    recordset = client.query_with_plan_blocking(
        stmt,
        PartitionFilter.all(),
        plan,
        policy=QueryPolicy(),
    )

    ages = _collect_int_bin_blocking(recordset, QSEL_AGE_BIN)
    assert ages == [14, 15, 16, 17, 18]


def test_blocking_execute_statement_with_filters_raises(qsel_blocking_fixture):
    from aerospike_async.exceptions import ValueError

    client = qsel_blocking_fixture["client"]
    set_name = qsel_blocking_fixture["set_name"]

    plan = client.query_explain_blocking(
        QSEL_NAMESPACE,
        "$.age >= 14 and $.age <= 18",
        set_name=set_name,
    )
    stmt = Statement(QSEL_NAMESPACE, set_name, [QSEL_AGE_BIN])
    stmt.filters = [Filter.range(QSEL_AGE_BIN, 14, 18)]
    with pytest.raises(ValueError, match="plan supplies the index filter"):
        client.query_with_plan_blocking(
            stmt,
            PartitionFilter.all(),
            plan,
            policy=QueryPolicy(),
        )

