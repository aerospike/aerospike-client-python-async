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

"""``_submit_coalesced_read``: per-op-delivery coalesced read.

One crossing submits N independent reads and resolves each caller's own
pre-created future the instant its key completes. Verifies positional delivery,
not-found raising (byte-identical to a direct ``get``), and the keys/futures
length guard. This is the backend for PSDK's transparent read coalescer.
"""

import asyncio
import os

import pytest

from aerospike_async import ClientPolicy, Key, new_client
from aerospike_async.exceptions import RecordNotFound

NS, SET = "test", "coalesced_read_test"


def _host() -> str:
    return os.environ.get("AEROSPIKE_HOST", "localhost:3000")


async def test_delivers_each_future_positionally():
    """Every key's own future receives that key's record, in input order."""
    client = await new_client(ClientPolicy(), _host())
    try:
        keys = [Key(NS, SET, i) for i in range(32)]
        await asyncio.gather(*(client.put(k, {"v": i}) for i, k in enumerate(keys)))

        loop = asyncio.get_running_loop()
        futures = [loop.create_future() for _ in keys]
        assert client._submit_coalesced_read(keys, futures, None) is None
        records = await asyncio.gather(*futures)
        assert [r.bins["v"] for r in records] == list(range(32))
    finally:
        await client.close()


async def test_not_found_raises_on_its_own_future():
    """A missing key raises on its future — identical to a direct ``get``."""
    client = await new_client(ClientPolicy(), _host())
    try:
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        client._submit_coalesced_read([Key(NS, SET, 10_000_001)], [fut], None)
        with pytest.raises(RecordNotFound):
            await fut
    finally:
        await client.close()


async def test_length_mismatch_is_rejected():
    """keys and futures must be the same length."""
    client = await new_client(ClientPolicy(), _host())
    try:
        loop = asyncio.get_running_loop()
        with pytest.raises(ValueError):
            client._submit_coalesced_read(
                [Key(NS, SET, 1)],
                [loop.create_future(), loop.create_future()],
                None,
            )
    finally:
        await client.close()
