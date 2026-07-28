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

"""``_submit_coalesced_write``: per-op-delivery coalesced write.

One crossing submits N independent writes, each with its own bin payload, and
resolves each caller's pre-created future the instant its key completes.
Verifies per-key payloads land distinctly (the property ``_submit_many_write``
cannot provide, since it broadcasts one payload), per-op error isolation, and
the length guard.
"""

import asyncio
import os

import pytest

from aerospike_async import ClientPolicy, Key, new_client

NS, SET = "test", "coalesced_write_test"


def _host() -> str:
    return os.environ.get("AEROSPIKE_HOST", "localhost:3000")


async def test_each_key_gets_its_own_payload():
    """``bins_list[i]`` lands on ``keys[i]``; futures resolve to ``None``."""
    client = await new_client(ClientPolicy(), _host())
    try:
        keys = [Key(NS, SET, i) for i in range(32)]
        payloads = [{"v": i, "name": f"user-{i}"} for i in range(32)]

        loop = asyncio.get_running_loop()
        futures = [loop.create_future() for _ in keys]
        assert client._submit_coalesced_write(keys, futures, payloads) is None
        assert await asyncio.gather(*futures) == [None] * 32

        records = await asyncio.gather(*(client.get(k) for k in keys))
        assert [r.bins["v"] for r in records] == list(range(32))
        assert [r.bins["name"] for r in records] == [f"user-{i}" for i in range(32)]
    finally:
        await client.close()

async def test_heterogeneous_bin_sets():
    """Payloads need not share bin names — each is converted independently."""
    client = await new_client(ClientPolicy(), _host())
    try:
        keys = [Key(NS, SET, f"het-{i}") for i in range(3)]
        payloads = [{"a": 1}, {"b": "two", "c": 3.5}, {"d": [1, 2], "e": {"f": 1}}]

        loop = asyncio.get_running_loop()
        futures = [loop.create_future() for _ in keys]
        client._submit_coalesced_write(keys, futures, payloads)
        await asyncio.gather(*futures)

        assert (await client.get(keys[0])).bins == {"a": 1}
        assert (await client.get(keys[1])).bins == {"b": "two", "c": 3.5}
        assert (await client.get(keys[2])).bins == {"d": [1, 2], "e": {"f": 1}}
    finally:
        await client.close()

async def test_one_failed_key_does_not_fail_its_window_mates():
    """A per-key error raises on its own future only."""
    client = await new_client(ClientPolicy(), _host())
    try:
        good, bad = Key(NS, SET, "iso-ok"), Key("no_such_ns", SET, "iso-bad")

        loop = asyncio.get_running_loop()
        futures = [loop.create_future(), loop.create_future()]
        client._submit_coalesced_write(
            [good, bad], futures, [{"v": 1}, {"v": 2}],
        )
        results = await asyncio.gather(*futures, return_exceptions=True)
        assert results[0] is None
        assert isinstance(results[1], Exception)
        assert (await client.get(good)).bins["v"] == 1
    finally:
        await client.close()

async def test_length_mismatch_is_rejected():
    """keys, futures, and bins_list must all be the same length."""
    client = await new_client(ClientPolicy(), _host())
    try:
        loop = asyncio.get_running_loop()
        with pytest.raises(ValueError):
            client._submit_coalesced_write(
                [Key(NS, SET, 1)],
                [loop.create_future(), loop.create_future()],
                [{"v": 1}, {"v": 2}],
            )
        with pytest.raises(ValueError):
            client._submit_coalesced_write(
                [Key(NS, SET, 1)], [loop.create_future()], [{"v": 1}, {"v": 2}],
            )
    finally:
        await client.close()
