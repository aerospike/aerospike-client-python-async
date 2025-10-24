import pytest
from aerospike_async import Recordset, ScanPolicy, Record, PartitionFilter
from aerospike_async.exceptions import ServerError, InvalidNodeError
from fixtures import TestFixtureInsertRecord


class TestScan(TestFixtureInsertRecord):
    """Test client.scan() method functionality."""

    async def test_basic_usage(self, client):
        """Test basic scan operation."""
        records = await client.scan(ScanPolicy(), PartitionFilter.all(), "test", "test", None)
        assert isinstance(records, Recordset)

        # RecordSet tests are in the query basic usage test
        for record in records:
            assert isinstance(record, Record)


    async def test_with_bins(self, client):
        """Test scan operation with specific bins."""
        records = await client.scan(ScanPolicy(), PartitionFilter.all(), "test", "test", [])
        assert isinstance(records, Recordset)


    async def test_with_policy(self, client):
        """Test scan operation with scan policy."""
        sp = ScanPolicy()
        records = await client.scan(sp, PartitionFilter.all(), "test", "test", None)
        assert isinstance(records, Recordset)


    async def test_fail(self, client):
        """Test scan operation with invalid namespace raises InvalidNodeError during iteration."""
        records = await client.scan(ScanPolicy(), PartitionFilter.all(), "test1", "test", None)
        
        # The error occurs during iteration, not during the scan call
        with pytest.raises(InvalidNodeError):
            # Force iteration to trigger the error
            list(records)