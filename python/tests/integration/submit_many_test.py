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

"""Window submission: ``_submit_many_read`` / ``_submit_many_write``.

Each resolves to ``(slots, failure_count)``. Slots are positional and hold the
record (reads) or ``None`` (writes), or the exception instance for that key;
``failure_count`` is the number of exception slots, so a consumer can skip
post-processing a failure-free window without scanning it.
"""

import os

from aerospike_async import ClientPolicy, Key, new_client
from aerospike_async.exceptions import RecordNotFound


def _host() -> str:
    return os.environ.get("AEROSPIKE_HOST", "localhost:3000")


async def test_read_window_reports_failure_count():
    client = await new_client(ClientPolicy(), _host())
    try:
        present = [Key("test", "submit_many", f"present-{i}") for i in range(3)]
        missing = Key("test", "submit_many", "missing")
        for i, k in enumerate(present):
            await client.put(k, {"v": i})

        slots, failures = await client._submit_many_read(present)
        assert failures == 0
        assert [s.bins["v"] for s in slots] == [0, 1, 2]

        slots, failures = await client._submit_many_read(present + [missing])
        assert failures == 1
        assert [s.bins["v"] for s in slots[:3]] == [0, 1, 2]
        assert isinstance(slots[3], RecordNotFound)
    finally:
        await client.close()


async def test_write_window_reports_failure_count():
    client = await new_client(ClientPolicy(), _host())
    try:
        keys = [Key("test", "submit_many", f"write-{i}") for i in range(3)]

        slots, failures = await client._submit_many_write(keys, {"v": 1})
        assert failures == 0
        assert slots == [None, None, None]

        # Well past any configured write-block-size, so the server rejects it.
        slots, failures = await client._submit_many_write(
            keys[:1], {"blob": b"x" * (16 * 1024 * 1024)})
        assert failures == 1
        assert isinstance(slots[0], Exception)
    finally:
        await client.close()
