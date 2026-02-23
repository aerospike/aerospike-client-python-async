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

import pytest

from aerospike_async import (
    new_client, ClientPolicy, Key, QueryPolicy, ReadPolicy, Statement,
    WritePolicy, PartitionFilter,
)
from aerospike_async.exceptions import ClientError, TimeoutError


class TestSocketTimeout:
    """Test that socket_timeout actually enforces socket I/O timeouts."""

    @pytest.mark.asyncio
    async def test_socket_timeout_raises_timeout_error(self, aerospike_host):
        """Test that socket_timeout raises TimeoutError on slow socket operations.

        Note: This test may not always timeout on fast networks (e.g., localhost).
        socket_timeout applies to individual socket read/write operations, which
        can complete very quickly on local networks.
        """
        cp = ClientPolicy()
        cp.use_services_alternate = True
        client = await new_client(cp, aerospike_host)

        try:
            qp = QueryPolicy()
            qp.socket_timeout = 1  # 1ms - extremely short, may timeout on socket I/O
            qp.total_timeout = 100  # 100ms - hard cap so the test can't hang
            qp.max_retries = 0

            stmt = Statement("test", "test", None)

            try:
                recordset = await client.query(qp, PartitionFilter.all(), stmt)
                async for _ in recordset:
                    pass
                recordset.close()
                pytest.skip("Socket operations completed faster than 1ms timeout - network too fast to verify timeout")
            except (TimeoutError, ClientError):
                pass

        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_socket_timeout_not_triggered_on_fast_operation(self, aerospike_host):
        """Test that socket_timeout doesn't trigger on fast socket operations."""
        cp = ClientPolicy()
        cp.use_services_alternate = True
        client = await new_client(cp, aerospike_host)

        try:
            wp = WritePolicy()
            wp.socket_timeout = 1000
            rp = ReadPolicy()
            rp.socket_timeout = 1000

            key = Key("test", "test", "socket_timeout_fast_test")

            await client.put(wp, key, {"test": "value"})

            record = await client.get(rp, key, None)
            assert record is not None
            assert record.bins["test"] == "value"

            await client.delete(wp, key)

        finally:
            await client.close()


class TestTotalTimeout:
    """Test that total_timeout actually enforces operation timeouts (client-side TimeoutError)."""

    @pytest.mark.asyncio
    async def test_total_timeout_raises_timeout_error(self, aerospike_host):
        """Test that total_timeout raises TimeoutError (client-side timeout)."""
        cp = ClientPolicy()
        cp.use_services_alternate = True
        client = await new_client(cp, aerospike_host)

        try:
            qp = QueryPolicy()
            qp.total_timeout = 1  # 1ms - extremely short, should definitely timeout

            stmt = Statement("test", "test", None)

            with pytest.raises(TimeoutError):
                recordset = await client.query(qp, PartitionFilter.all(), stmt)
                async for _ in recordset:
                    pass
                recordset.close()

        finally:
            await client.close()
