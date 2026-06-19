# Copyright 2025-2026 Aerospike, Inc.
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

"""Multi-loop completion bridge correctness.

Each ``Client`` owns a ``CompletionBridge`` paired to the event loop that
constructed it. Two properties exercised here:

1. Many independent ``(loop, client)`` pairs running in parallel OS threads
   all receive their completions on the correct loop — completions never
   cross loops.
2. Sharing one ``Client`` across two event loops fails loud with a
   ``RuntimeError`` at the first cross-loop call instead of silently routing
   loop B's future through loop A's drainer (a hazard the GIL masks today but
   that becomes a real data race under free-threading).
"""

import asyncio
import threading

import pytest

from aerospike_async import ClientPolicy, Key, ReadPolicy, WritePolicy, new_client


def _run_loop_in_thread():
    """Spin up an asyncio loop in a daemon thread; return (loop, thread).

    The loop runs forever until ``loop.call_soon_threadsafe(loop.stop)``;
    the caller is responsible for tearing it down.
    """
    loop = asyncio.new_event_loop()
    ready = threading.Event()

    def _runner():
        asyncio.set_event_loop(loop)
        ready.set()
        loop.run_forever()

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    ready.wait()
    return loop, t


def _submit(loop, coro, timeout=10.0):
    """Run ``coro`` on ``loop`` from the calling thread; block for the result."""
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    return fut.result(timeout=timeout)


def _stop_loop(loop, thread):
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5.0)
    loop.close()


class TestMultiLoopCompletion:

    @pytest.mark.parametrize("n_loops", [4])
    def test_parallel_loops_each_with_own_client(
        self, aerospike_host, use_services_alternate, n_loops
    ):
        """N loops on N threads, each with its own Client, all complete cleanly.

        Each loop does a put + get round trip; the test fails if any loop's
        completion is lost, mis-routed, or hangs.
        """
        if not aerospike_host:
            pytest.skip("AEROSPIKE_HOST not set")

        loops_and_threads = [_run_loop_in_thread() for _ in range(n_loops)]

        async def make_client():
            cp = ClientPolicy()
            cp.use_services_alternate = use_services_alternate
            return await new_client(cp, aerospike_host)

        clients = [_submit(loop, make_client()) for loop, _ in loops_and_threads]

        try:
            async def roundtrip(client, i):
                key = Key("test", "test", f"multiloop_{i}")
                await client.put(key, {"bin": i}, policy=WritePolicy())
                rec = await client.get(key, policy=ReadPolicy())
                return rec.bins["bin"] if rec is not None else None

            results = []
            for i, (loop, _) in enumerate(loops_and_threads):
                results.append(_submit(loop, roundtrip(clients[i], i)))

            assert results == list(range(n_loops)), (
                f"completion routing broken: got {results}, expected {list(range(n_loops))}"
            )
        finally:
            for i, (loop, _) in enumerate(loops_and_threads):
                try:
                    _submit(loop, clients[i].close())
                except Exception:
                    pass
            for loop, thread in loops_and_threads:
                _stop_loop(loop, thread)

    def test_cross_loop_misuse_raises(
        self, aerospike_host, use_services_alternate
    ):
        """Calling a Client from a loop other than its owning one raises RuntimeError.

        Without this guard the call would silently enqueue into the owning
        loop's bridge and call ``set_result`` on the caller-loop's future from
        the wrong thread — a violation masked by the GIL but real under
        free-threading.
        """
        if not aerospike_host:
            pytest.skip("AEROSPIKE_HOST not set")

        loop_a, thread_a = _run_loop_in_thread()
        loop_b, thread_b = _run_loop_in_thread()

        async def make_client():
            cp = ClientPolicy()
            cp.use_services_alternate = use_services_alternate
            return await new_client(cp, aerospike_host)

        client = _submit(loop_a, make_client())

        try:
            async def use_client_on_wrong_loop():
                # The Client was bound to loop_a; calling .get() while
                # running on loop_b must fail.  In debug builds the failure
                # is the explicit upfront check in `batched_future_into_py`
                # ("...different event loop than the one that created it").
                # In release builds that diagnostic is gated out
                # (`#[cfg(debug_assertions)]`) and the misuse surfaces
                # downstream when asyncio tries to resolve a future
                # attached to the wrong loop ("...attached to a different
                # loop"). Either is acceptable — both are loud,
                # raise-immediately failures.
                return await client.get(Key("test", "test", "x"), policy=ReadPolicy())

            with pytest.raises(RuntimeError, match=r"different .*loop"):
                _submit(loop_b, use_client_on_wrong_loop())
        finally:
            try:
                _submit(loop_a, client.close())
            except Exception:
                pass
            _stop_loop(loop_a, thread_a)
            _stop_loop(loop_b, thread_b)
