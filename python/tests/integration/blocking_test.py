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
    BatchPolicy,
    ClientPolicy,
    Key,
    PartitionFilter,
    QueryPolicy,
    ReadPolicy,
    Statement,
    WritePolicy,
    new_client,
    new_client_blocking,
)


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
