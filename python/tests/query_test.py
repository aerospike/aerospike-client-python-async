import asyncio
import pytest
from aerospike_async import Statement, Filter, Recordset, Record, QueryPolicy, PartitionFilter
from aerospike_async.exceptions import ServerError, InvalidNodeError
from fixtures import TestFixtureInsertRecord


class TestStatement:
    """Test Statement class functionality."""

    bin_name = "bin"

    def test_new(self):
        """Test creating a new Statement."""
        stmt = Statement(namespace="test", set_name="test", bins=["test_bin"])
        # Test defaults
        assert stmt.filters is None

    def test_set_filters(self):
        """Test setting filters on Statement."""
        stmt = Statement("test", "test", [self.bin_name])
        filter = Filter.range(self.bin_name, 1, 3)
        stmt.filters = [filter]
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

        for record in records:
            assert isinstance(record, Record)

        # Close the recordset
        #records.close()
        
        # Query finished - recordset should be inactive after consuming all records
        # Note: There may be timing differences between IDE and command line execution
        import time
        time.sleep(0.1)  # Small delay to allow recordset to become inactive
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
        
        # Add a small delay to allow the server to process the request
        await asyncio.sleep(0.1)
        
        # The error occurs during iteration, not during the query call
        with pytest.raises(InvalidNodeError):
            # Force iteration to trigger the error
            list(records)
