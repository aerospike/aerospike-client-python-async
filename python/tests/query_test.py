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
import pytest
from aerospike_async import Statement, Filter, Recordset, Record, QueryPolicy, PartitionFilter
from aerospike_async.exceptions import InvalidNodeError
from fixtures import TestFixtureInsertRecord, TestFixtureConnection


class TestStatement:
    """Test Statement class functionality."""

    bin_name = "bin"

    def test_new(self):
        """Test creating a new Statement."""
        stmt = Statement(namespace="test", set_name="test", bins=["test_bin"])
        # Test defaults
        assert stmt.filters is None
        assert stmt.index_name is None

    def test_set_filters(self):
        """Test setting filters on Statement."""
        stmt = Statement("test", "test", [self.bin_name])
        a_filter = Filter.range(self.bin_name, 1, 3)
        stmt.filters = [a_filter]
        assert isinstance(stmt.filters, list)

        stmt.filters = None
        assert stmt.filters is None


class TestQuery(TestFixtureInsertRecord):
    """Test client.query() method functionality."""
    bin_name = "bin"

    @pytest.fixture
    def stmt(self):
        """Create a test statement."""
        return Statement("test", "test", [self.bin_name])

    async def test_query_and_recordset(self, client, stmt):
        """Test basic query operation and Recordset functionality."""
        records = await client.query(QueryPolicy(), PartitionFilter.all(), stmt)
        assert isinstance(records, Recordset)

        async for record in records:
            assert isinstance(record, Record)

        # Wait for the recordset to become inactive (query finished processing)
        # This ensures the recordset is properly closed after consuming all records
        max_wait = 10  # Maximum 1 second wait
        for _ in range(max_wait):
            if not records.active:
                break
            await asyncio.sleep(0.1)
        
        # Query finished - recordset should be inactive after consuming all records
        assert records.active is False

        # Check that we can call close()
        records.close()

    async def test_with_policy(self, client, stmt):
        """Test query operation with query policy."""
        qp = QueryPolicy()
        records = await client.query(qp, PartitionFilter.all(), stmt)
        assert isinstance(records, Recordset)

    async def test_fail(self, client):
        """Test query operation with invalid parameters raises TypeError."""
        # Test with invalid partition filter type to trigger TypeError
        with pytest.raises(TypeError):
            records = await client.query(QueryPolicy(), "invalid_filter", Statement("test", "test", ["bin1"]))

    async def test_invalid_node_error(self, client):
        """Test query operation with invalid namespace raises InvalidNodeError during iteration."""
        stmt_invalid_namespace = Statement("bad_ns", "test", ["bin1"])
        records = await client.query(QueryPolicy(), PartitionFilter.all(), stmt_invalid_namespace)
        
        # Wait for the recordset to become inactive (query finished processing)
        # This ensures the error is properly raised during iteration
        max_wait = 10  # Maximum 1 second wait
        for _ in range(max_wait):
            if not records.active:
                break
            await asyncio.sleep(0.1)
        
        # The error occurs during iteration, not during the query call
        with pytest.raises(InvalidNodeError):
            # Force iteration to trigger the error
            async for _ in records:
                pass


class TestQueryEmptySet(TestFixtureConnection):
    """Test query with empty set name."""

    async def test_query_empty_set_name_none(self, client):
        """Test query operation with None set name (queries all sets in namespace)."""
        stmt = Statement("test", set_name=None, bins=None)
        qp = QueryPolicy()
        pf = PartitionFilter.all()

        assert stmt.set_name is None

        rs = await client.query(qp, pf, stmt)
        # Empty set name should not raise an error - it queries all sets in namespace
        assert isinstance(rs, Recordset)
        record_count = 0
        async for _ in rs:
            record_count += 1
        # Query completed successfully (may return 0 or more records depending on namespace)
        assert record_count >= 0

    async def test_query_empty_set_name_empty_string(self, client):
        """Test query operation with empty string set name (queries all sets in namespace)."""
        stmt = Statement("test", set_name="", bins=None)
        qp = QueryPolicy()
        pf = PartitionFilter.all()

        # Assert that empty string is converted to None
        assert stmt.set_name is None

        rs = await client.query(qp, pf, stmt)
        # Empty set name should not raise an error - it queries all sets in namespace
        assert isinstance(rs, Recordset)
        record_count = 0
        async for _ in rs:
            record_count += 1
        # Query completed successfully (may return 0 or more records depending on namespace)
        assert record_count >= 0

    async def test_query_empty_set_name_equivalence(self, client):
        """Test that None and empty string are equivalent for set_name."""
        stmt1 = Statement("test", set_name=None, bins=None)
        stmt2 = Statement("test", set_name="", bins=None)

        # Both should result in None
        assert stmt1.set_name is None
        assert stmt2.set_name is None
        assert stmt1.set_name == stmt2.set_name
