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

"""Pipe-wake wake transport: forced-on delivery + clean teardown.

Exercises PAC's alternate cross-thread wake path (``AEROSPIKE_PIPE_WAKE=1``,
which forces the transport on regardless of loop implementation) end to end: a
concurrent burst delivers through the self-pipe reader, ``close()`` tears the
reader down, and a fresh client on the same loop still works afterward (proving
teardown left the loop healthy — no dangling reader, no leak-induced wedge).
"""

import asyncio
import os

from aerospike_async import ClientPolicy, Key, new_client

NS, SET = "test", "pipe_wake_test"


def _host() -> str:
    return os.environ.get("AEROSPIKE_HOST", "localhost:3000")


async def test_pipe_wake_burst_delivery(monkeypatch):
    """A concurrent burst delivers correctly through the pipe drain."""
    monkeypatch.setenv("AEROSPIKE_PIPE_WAKE", "1")
    client = await new_client(ClientPolicy(), _host())
    try:
        keys = [Key(NS, SET, i) for i in range(64)]
        await asyncio.gather(*(client.put(k, {"v": i}) for i, k in enumerate(keys)))
        records = await asyncio.gather(*(client.get(k, None) for k in keys))
        assert [r.bins["v"] for r in records] == list(range(64))
    finally:
        await client.close()


async def test_pipe_wake_usable_after_close(monkeypatch):
    """A closed pipe-backed client still delivers completions (does not hang).

    The pipe reader is intentionally NOT torn down on ``close()`` — a closed
    client stays usable (``is_connected()`` returns False), and that call must
    deliver through the reader rather than write to an unwatched pipe and hang.
    Regression for the post-close hang.
    """
    monkeypatch.setenv("AEROSPIKE_PIPE_WAKE", "1")
    client = await new_client(ClientPolicy(), _host())
    assert await client.is_connected() is True
    await client.close()
    # Must return (not hang) even though the client is closed.
    assert await client.is_connected() is False


async def test_pipe_wake_teardown_leaves_loop_healthy(monkeypatch):
    """After a pipe-backed client closes, a new client on the loop still works."""
    monkeypatch.setenv("AEROSPIKE_PIPE_WAKE", "1")
    first = await new_client(ClientPolicy(), _host())
    await first.put(Key(NS, SET, 1), {"v": 1})
    await first.close()

    second = await new_client(ClientPolicy(), _host())
    try:
        await second.put(Key(NS, SET, 2), {"v": 2})
        record = await second.get(Key(NS, SET, 2), None)
        assert record.bins["v"] == 2
    finally:
        await second.close()
