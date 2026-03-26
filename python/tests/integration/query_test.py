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
from aerospike_async import (
    Statement,
    Recordset,
    Record,
    QueryPolicy,
    PartitionFilter,
    Filter,
    IndexType,
    CollectionIndexType,
    FilterExpression,
    CTX,
    Key,
    WritePolicy,
)
from aerospike_async.exceptions import InvalidNodeError
from fixtures import TestFixtureInsertRecord, TestFixtureConnection


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
        assert isinstance(rs, Recordset)

    async def test_query_empty_set_name_empty_string(self, client):
        """Test query operation with empty string set name (queries all sets in namespace)."""
        stmt = Statement("test", set_name="", bins=None)
        qp = QueryPolicy()
        pf = PartitionFilter.all()

        assert stmt.set_name is None

        rs = await client.query(qp, pf, stmt)
        assert isinstance(rs, Recordset)

    async def test_query_empty_set_name_equivalence(self, client):
        """Test that None and empty string are equivalent for set_name."""
        stmt1 = Statement("test", set_name=None, bins=None)
        stmt2 = Statement("test", set_name="", bins=None)

        # Both should result in None
        assert stmt1.set_name is None
        assert stmt2.set_name is None
        assert stmt1.set_name == stmt2.set_name


class TestQueryEqualByIndex(TestFixtureInsertRecord):
    """Query using a filter that names the secondary index (not Statement.index_name)."""

    idx = "pac_it_query_equal_by_index"

    async def cleanup_index(self, client):
        try:
            task = await client.drop_index("test", "test", self.idx)
            await task.wait_till_complete()
        except Exception:
            pass

    async def test_query_equal_by_index_returns_record(self, client):
        await self.cleanup_index(client)
        await client.create_index(
            "test",
            "test",
            "year",
            self.idx,
            IndexType.NUMERIC,
            cit=CollectionIndexType.DEFAULT,
        )
        await asyncio.sleep(1.0)

        stmt = Statement("test", "test", ["year"])
        stmt.filters = [Filter.equal_by_index(self.idx, 1964)]

        records = await client.query(QueryPolicy(), PartitionFilter.all(), stmt)
        found = False
        async for record in records:
            assert isinstance(record, Record)
            assert record.bins.get("year") == 1964
            found = True
        assert found

        await self.cleanup_index(client)


class TestQueryFilterContext(TestFixtureConnection):
    """Query with Filter.context for a secondary index on a nested list element."""

    set_name = "flt_ctx_set"
    idx_name = "pac_it_nested_list_elem"
    bin_name = "nested"

    async def cleanup(self, client):
        try:
            await client.truncate("test", self.set_name)
        except Exception:
            pass
        try:
            task = await client.drop_index("test", self.set_name, self.idx_name)
            await task.wait_till_complete()
        except Exception:
            pass

    async def test_query_list_element_context_filter(self, client):
        await self.cleanup(client)
        wp = WritePolicy()
        for i in range(5):
            key = Key("test", self.set_name, i)
            await client.put(wp, key, {self.bin_name: [i]})

        await client.create_index(
            "test",
            self.set_name,
            self.bin_name,
            self.idx_name,
            IndexType.NUMERIC,
            cit=CollectionIndexType.DEFAULT,
            ctx=[CTX.list_index(0)],
        )
        await asyncio.sleep(1.5)

        stmt = Statement("test", self.set_name, [self.bin_name])
        stmt.filters = [
            Filter.range(self.bin_name, 0, 4).context([CTX.list_index(0)])
        ]

        records = await client.query(QueryPolicy(), PartitionFilter.all(), stmt)
        count = 0
        async for _ in records:
            count += 1
        assert count == 5

        await self.cleanup(client)


class TestQueryFilterExpressionAttach(TestFixtureInsertRecord):
    """Query using Filter.expression to select an expression-based secondary index."""

    idx = "pac_it_flt_expr_attach"

    async def cleanup_index(self, client):
        try:
            task = await client.drop_index("test", "test", self.idx)
            await task.wait_till_complete()
        except Exception:
            pass

    async def test_query_range_with_expression_on_filter(self, client):
        await self.cleanup_index(client)
        expr = FilterExpression.int_bin("year")
        task = await client.create_index_using_expression(
            namespace="test",
            set_name="test",
            index_name=self.idx,
            index_type=IndexType.NUMERIC,
            expression=expr,
        )
        assert await task.wait_till_complete()

        stmt = Statement("test", "test", ["year"])
        stmt.filters = [Filter.range("year", 1960, 1970).expression(expr)]

        records = await client.query(QueryPolicy(), PartitionFilter.all(), stmt)
        found = False
        async for record in records:
            assert isinstance(record, Record)
            assert record.bins.get("year") == 1964
            found = True
        assert found

        await self.cleanup_index(client)
