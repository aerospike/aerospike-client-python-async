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

import asyncio
import sys
import time
from pathlib import Path

import pytest
from aerospike_async import PartitionFilter, QueryPolicy, Statement

# Ensure this directory is on sys.path so "from fixtures import ..." works
_this_dir = str(Path(__file__).parent)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)


@pytest.fixture
def wait_for_index():
    """Async helper: poll until a secondary index is queryable.

    Server-side SI build completes asynchronously after ``create_index`` even
    when the task reports done, so ``asyncio.sleep(N)`` is inherently flaky
    on loaded hosts. This fixture issues the same query the caller is about
    to run, retrying while ``IndexNotReadable`` is reported.

    Usage::

        await wait_for_index(client, "test", "my_set", Filter.range("age", 0, 100))
    """
    async def _wait(client, ns, set_name, sindex_filter, *,
                    bins=None, timeout=5.0, interval=0.25):
        deadline = time.monotonic() + timeout
        last_err = None
        while time.monotonic() < deadline:
            try:
                stmt = Statement(ns, set_name, bins or [])
                stmt.filters = [sindex_filter]
                records = await client.query(
                    QueryPolicy(), PartitionFilter.all(), stmt)
                async for _ in records:
                    break
                return
            except Exception as exc:
                if "IndexNotReadable" not in str(exc):
                    raise
                last_err = exc
                await asyncio.sleep(interval)
        raise last_err  # type: ignore[misc]

    return _wait
