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

"""Tests for query aggregation functionality.


Note: The Rust core currently (1/30/2026) returns per-partition mapped results from aggregation UDFs.
Client-side reduction must be performed to get final aggregated values.
"""

import asyncio
import os
import pytest
from aerospike_async import (
    Client, ClientPolicy, Statement, Filter, QueryPolicy,
    PartitionFilter, IndexType, WritePolicy, Key, UDFLang
)
from aerospike_async.exceptions import ServerError
from fixtures import TestFixtureConnection


class TestQueryAggregate(TestFixtureConnection):
    """Test query aggregation with UDF functions.

    The Rust core streams per-partition results from aggregation UDFs.
    Final reduction must be done client-side by summing the streamed values.
    """

    INDEX_NAME = "agg_test_idx"
    BIN_NAME = "value"
    SET_NAME = "agg_test"
    SIZE = 10

    @pytest.fixture(autouse=True)
    async def setup_aggregation(self, client):
        """Set up UDF, index, and test data for aggregation tests."""
        # Register the UDF
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "sum_example.lua")
        server_path = "sum_example.lua"

        try:
            task = await client.register_udf_from_file(None, udf_path, server_path, UDFLang.LUA)
            await task.wait_till_complete()
        except ServerError:
            pass  # UDF may already be registered

        # Create index
        try:
            await client.create_index(
                "test", self.SET_NAME, self.BIN_NAME,
                self.INDEX_NAME, IndexType.NUMERIC
            )
            await asyncio.sleep(1)
        except ServerError as e:
            if "INDEX_ALREADY_EXISTS" not in str(e) and "200" not in str(e):
                raise

        # Insert test data: values 1 through SIZE
        wp = WritePolicy()
        for i in range(1, self.SIZE + 1):
            key = Key("test", self.SET_NAME, f"agg_key_{i}")
            await client.put(wp, key, {self.BIN_NAME: i})

        yield

        # Cleanup: delete test records
        for i in range(1, self.SIZE + 1):
            key = Key("test", self.SET_NAME, f"agg_key_{i}")
            try:
                await client.delete(wp, key)
            except ServerError:
                pass

    async def test_aggregation_with_map_function(self, client):
        """Test aggregation UDF with map function returns mapped values.

        The UDF's map function transforms each record, and results are streamed
        back. Client-side reduction sums the values.
        """
        begin = 4
        end = 7

        stmt = Statement("test", self.SET_NAME, [self.BIN_NAME])
        stmt.filters = [Filter.range(self.BIN_NAME, begin, end)]
        stmt.set_aggregate_function("sum_example", "sum_single_bin", [self.BIN_NAME])

        qp = QueryPolicy()
        recordset = await client.query(qp, PartitionFilter.all(), stmt)

        # Collect streamed results and perform client-side reduction
        results = []
        async for record in recordset:
            if record.bins:
                for bin_name, value in record.bins.items():
                    if isinstance(value, (int, float)):
                        results.append(int(value))

        # Should get the mapped values (4, 5, 6, 7) streamed back
        assert len(results) > 0, "Should have aggregation results"

        # Client-side reduction: sum all streamed values
        client_side_sum = sum(results)
        expected_sum = sum(range(begin, end + 1))  # 4 + 5 + 6 + 7 = 22
        assert client_side_sum == expected_sum, f"Expected sum {expected_sum}, got {client_side_sum}"

    async def test_aggregation_returns_success_bin(self, client):
        """Test that aggregation results come in 'SUCCESS' bin."""
        begin = 4
        end = 7

        stmt = Statement("test", self.SET_NAME, [self.BIN_NAME])
        stmt.filters = [Filter.range(self.BIN_NAME, begin, end)]
        stmt.set_aggregate_function("sum_example", "sum_single_bin", [self.BIN_NAME])

        qp = QueryPolicy()
        recordset = await client.query(qp, PartitionFilter.all(), stmt)

        found_success_bin = False
        async for record in recordset:
            if record.bins and "SUCCESS" in record.bins:
                found_success_bin = True
                break

        assert found_success_bin, "Aggregation results should have 'SUCCESS' bin"

    async def test_aggregation_empty_result(self, client):
        """Test aggregation with no matching records returns no results."""
        stmt = Statement("test", self.SET_NAME, [self.BIN_NAME])
        stmt.filters = [Filter.range(self.BIN_NAME, 100, 200)]
        stmt.set_aggregate_function("sum_example", "sum_single_bin", [self.BIN_NAME])

        qp = QueryPolicy()
        recordset = await client.query(qp, PartitionFilter.all(), stmt)

        results = []
        async for record in recordset:
            if record.bins:
                for bin_name, value in record.bins.items():
                    if isinstance(value, (int, float)):
                        results.append(value)

        # Empty range should return no results
        assert len(results) == 0, "Empty range should return no aggregation results"

    async def test_aggregation_all_records(self, client):
        """Test aggregation over all records with client-side reduction."""
        stmt = Statement("test", self.SET_NAME, [self.BIN_NAME])
        stmt.filters = [Filter.range(self.BIN_NAME, 1, self.SIZE)]
        stmt.set_aggregate_function("sum_example", "sum_single_bin", [self.BIN_NAME])

        qp = QueryPolicy()
        recordset = await client.query(qp, PartitionFilter.all(), stmt)

        results = []
        async for record in recordset:
            if record.bins:
                for bin_name, value in record.bins.items():
                    if isinstance(value, (int, float)):
                        results.append(int(value))

        assert len(results) > 0, "Should have aggregation results"

        # Client-side reduction
        client_side_sum = sum(results)
        expected_sum = self.SIZE * (self.SIZE + 1) // 2  # 1+2+...+10 = 55
        assert client_side_sum == expected_sum, f"Expected sum {expected_sum}, got {client_side_sum}"

    async def test_count_aggregation(self, client):
        """Test count aggregation with client-side reduction."""
        begin = 3
        end = 8

        stmt = Statement("test", self.SET_NAME, [self.BIN_NAME])
        stmt.filters = [Filter.range(self.BIN_NAME, begin, end)]
        stmt.set_aggregate_function("sum_example", "count_records")

        qp = QueryPolicy()
        recordset = await client.query(qp, PartitionFilter.all(), stmt)

        results = []
        async for record in recordset:
            if record.bins:
                for bin_name, value in record.bins.items():
                    if isinstance(value, (int, float)):
                        results.append(int(value))

        # Each streamed result is 1, so sum equals count
        client_side_count = sum(results)
        expected_count = end - begin + 1  # values 3,4,5,6,7,8 = 6 records
        assert client_side_count == expected_count, f"Expected count {expected_count}, got {client_side_count}"
