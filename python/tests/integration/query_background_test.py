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

"""Tests for background query operations: query_operate and query_execute_udf.

Covers scan-mode put, filter-based put, touch, ExecuteTask.wait_till_complete
and query_status.
"""

import asyncio
import pytest
from aerospike_async import (
    WritePolicy,
    ReadPolicy,
    Key,
    Statement,
    Operation,
    Filter,
    QueryPolicy,
    PartitionFilter,
    IndexType,
)
from aerospike_async.exceptions import ServerError
from fixtures import TestFixtureConnection


class TestQueryBackground(TestFixtureConnection):
    """Test query_operate and ExecuteTask."""

    SET_NAME = "bg_op_test"
    SET_NAME_FILTER = "bg_op_filter_test"  # Dedicated set for filter test to avoid cross-test data
    BIN_NAME = "bin"
    NAMESPACE = "test"
    INDEX_NAME = "bg_op_filter_idx"

    @pytest.fixture
    async def client_and_data(self, client):
        """Create a few records in a dedicated set for background operate tests."""
        wp = WritePolicy()
        for i in range(10):
            key = Key(self.NAMESPACE, self.SET_NAME, i)
            await client.put(wp, key, {self.BIN_NAME: i})
        yield client
        # Teardown: delete test records (optional, next run overwrites)

    @pytest.fixture
    async def client_index_and_data(self, client):
        """Create index and records for filter-based query_operate."""
        try:
            await client.create_index(
                self.NAMESPACE,
                self.SET_NAME_FILTER,
                self.BIN_NAME,
                self.INDEX_NAME,
                IndexType.NUMERIC,
            )
            await asyncio.sleep(1.5)
        except ServerError as e:
            if "INDEX_ALREADY_EXISTS" not in str(e) and "200" not in str(e):
                raise
        wp = WritePolicy()
        for i in range(1, 11):
            key = Key(self.NAMESPACE, self.SET_NAME_FILTER, f"key_{i}")
            await client.put(wp, key, {self.BIN_NAME: i})
        yield client

    async def test_query_operate_scan_put(self, client_and_data):
        """Test query_operate in scan mode: put a bin on all records in the set."""
        client = client_and_data
        wp = WritePolicy()
        rp = ReadPolicy()

        # No filter: scan all records in namespace/set
        statement = Statement(self.NAMESPACE, self.SET_NAME, None)
        task = await client.query_operate(
            wp,
            statement,
            [Operation.put("marker_bin", 1)],
        )
        assert task is not None

        done = await task.wait_till_complete(sleep_time=0.2, max_attempts=50)
        assert done is True

        # Verify a couple of records have the new bin
        key0 = Key(self.NAMESPACE, self.SET_NAME, 0)
        rec0 = await client.get(rp, key0, ["marker_bin"])
        assert rec0 is not None
        assert rec0.bins.get("marker_bin") == 1

        key5 = Key(self.NAMESPACE, self.SET_NAME, 5)
        rec5 = await client.get(rp, key5, ["marker_bin"])
        assert rec5 is not None
        assert rec5.bins.get("marker_bin") == 1

    async def test_query_operate_with_filter(self, client_index_and_data):
        """Test query_operate with filter.

        Filter range(3, 9), put foo=bar, wait, then query and verify all 7 records
        have foo=='bar' and count is 7.
        """
        client = client_index_and_data
        wp = WritePolicy()
        begin, end = 3, 9
        expected_count = end - begin + 1

        statement = Statement(self.NAMESPACE, self.SET_NAME_FILTER, None)
        statement.filters = [Filter.range(self.BIN_NAME, begin, end)]

        task = await client.query_operate(
            wp,
            statement,
            [Operation.put("foo", "bar")],
        )
        assert task is not None
        done = await task.wait_till_complete(sleep_time=0.2, max_attempts=50)
        assert done is True

        # Query and verify all records in range have the new bin
        stmt2 = Statement(self.NAMESPACE, self.SET_NAME_FILTER, ["foo", self.BIN_NAME])
        stmt2.filters = [Filter.range(self.BIN_NAME, begin, end)]
        records = await client.query(QueryPolicy(), PartitionFilter.all(), stmt2)
        count = 0
        async for record in records:
            assert record.bins.get("foo") == "bar"
            count += 1
        records.close()
        assert count == expected_count

    async def test_query_operate_touch(self, client_and_data):
        """Test query_operate with Operation.touch()."""
        client = client_and_data
        wp = WritePolicy()
        statement = Statement(self.NAMESPACE, self.SET_NAME, None)
        task = await client.query_operate(wp, statement, [Operation.touch()])
        assert task is not None
        done = await task.wait_till_complete(sleep_time=0.2, max_attempts=50)
        assert done is True

    async def test_execute_task_query_status(self, client_and_data):
        """Test ExecuteTask.query_status() returns a status."""
        client = client_and_data
        wp = WritePolicy()
        statement = Statement(self.NAMESPACE, self.SET_NAME, None)
        task = await client.query_operate(wp, statement, [Operation.put("status_bin", 2)])
        assert task is not None

        status = await task.query_status()
        assert status is not None
        # Status may be InProgress or Complete depending on timing

        await task.wait_till_complete(sleep_time=0.2, max_attempts=50)
