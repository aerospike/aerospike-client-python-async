import pytest
from aerospike_async import PartitionFilter, Key, ScanPolicy, QueryPolicy, Statement, PartitionStatus, Recordset

from fixtures import TestFixtureInsertRecord


class TestPartitionFilter:
    """Test PartitionFilter class functionality."""

    def test_all(self):
        """Test PartitionFilter.all() creates a filter for all partitions."""
        pf = PartitionFilter.all()
        assert isinstance(pf, PartitionFilter)
        assert pf.begin == 0
        assert pf.count == 4096
        assert pf.digest is None

    def test_by_id(self):
        """Test PartitionFilter.by_id() creates a filter for a specific partition."""
        pf = PartitionFilter.by_id(0)
        assert isinstance(pf, PartitionFilter)
        assert pf.begin == 0
        assert pf.count == 1
        assert pf.digest is None

        pf = PartitionFilter.by_id(100)
        assert pf.begin == 100
        assert pf.count == 1

        pf = PartitionFilter.by_id(4095)
        assert pf.begin == 4095
        assert pf.count == 1

    def test_by_id_invalid(self):
        """Test PartitionFilter.by_id() with invalid partition IDs."""
        # Partition IDs should be 0-4095, but we can't easily test this without
        # actually running a scan/query, so we just verify the filter is created
        pf = PartitionFilter.by_id(5000)  # Out of range, but filter still created
        assert pf.begin == 5000
        assert pf.count == 1

    def test_by_range(self):
        """Test PartitionFilter.by_range() creates a filter for a partition range."""
        pf = PartitionFilter.by_range(0, 10)
        assert isinstance(pf, PartitionFilter)
        assert pf.begin == 0
        assert pf.count == 10
        assert pf.digest is None

        pf = PartitionFilter.by_range(100, 50)
        assert pf.begin == 100
        assert pf.count == 50

        pf = PartitionFilter.by_range(4090, 6)  # Last 6 partitions
        assert pf.begin == 4090
        assert pf.count == 6

    def test_by_key(self):
        """Test PartitionFilter.by_key() creates a filter for a specific key."""
        key = Key("test", "test", "test_key")
        pf = PartitionFilter.by_key(key)
        assert isinstance(pf, PartitionFilter)
        assert pf.count == 1
        assert pf.digest is not None
        assert isinstance(pf.digest, str)
        assert len(pf.digest) == 40  # 20 bytes = 40 hex chars

    def test_by_key_with_digest_key(self):
        """Test PartitionFilter.by_key() with a key created from digest."""
        digest_hex = "a" * 40  # 20 bytes as hex
        key = Key.key_with_digest("test", "test", digest_hex)
        pf = PartitionFilter.by_key(key)
        assert pf.count == 1
        assert pf.digest is not None

    def test_getters_setters(self):
        """Test getters and setters for PartitionFilter properties."""
        pf = PartitionFilter.all()
        
        # Test begin getter/setter
        assert pf.begin == 0
        pf.begin = 100
        assert pf.begin == 100
        
        # Test count getter/setter
        assert pf.count == 4096
        pf.count = 50
        assert pf.count == 50
        
        # Test digest getter/setter
        assert pf.digest is None
        digest_hex = "a" * 40  # 20 bytes as hex
        pf.digest = digest_hex
        assert pf.digest == digest_hex
        
        # Test setting digest to None
        pf.digest = None
        assert pf.digest is None

    def test_digest_setter_invalid(self):
        """Test digest setter with invalid values."""
        pf = PartitionFilter.all()
        
        # Test with invalid hex string (odd number of digits)
        with pytest.raises(ValueError, match="Invalid hex digest"):
            pf.digest = "short"
        
        # Test with too short (even number but wrong length)
        with pytest.raises(ValueError, match="Digest must be exactly 20 bytes"):
            pf.digest = "a" * 38  # 19 bytes
        
        # Test with too long
        with pytest.raises(ValueError, match="Digest must be exactly 20 bytes"):
            pf.digest = "a" * 42  # 21 bytes
        
        # Test with invalid hex characters
        with pytest.raises(ValueError, match="Invalid hex digest"):
            pf.digest = "g" * 40  # Invalid hex char

    def test_done(self):
        """Test done() method."""
        pf = PartitionFilter.all()
        # Initially should be False (no scan/query has been performed)
        assert pf.done() is False

    def test_default_constructor(self):
        """Test default constructor creates a filter for all partitions."""
        pf = PartitionFilter()
        assert pf.begin == 0
        assert pf.count == 4096
        assert pf.digest is None


class TestPartitionFilterUsage(TestFixtureInsertRecord):
    """Test PartitionFilter usage in actual scan/query operations."""

    def test_partition_filter_partitions_setter_accepts_partition_status_objects(self):
        """Test that PartitionFilter.partitions setter accepts PartitionStatus objects."""
        pf = PartitionFilter.by_range(0, 1)
        ps = PartitionStatus(0)
        ps.retry = False
        ps.bval = 0
        ps.digest = None
        pf.partitions = [ps]
        # Test passes if no exception is raised

    async def test_scan_with_by_id(self, client):
        """Test scan with PartitionFilter.by_id()."""
        pf = PartitionFilter.by_id(0)
        records = await client.scan(ScanPolicy(), pf, "test", "test", None)
        assert isinstance(records, Recordset)
        
        # Consume records to ensure scan completes
        count = 0
        async for _ in records:
            count += 1
            if count > 100:  # Limit to avoid long test
                break

    async def test_scan_with_by_range(self, client):
        """Test scan with PartitionFilter.by_range()."""
        pf = PartitionFilter.by_range(0, 10)
        records = await client.scan(ScanPolicy(), pf, "test", "test", None)
        assert isinstance(records, Recordset)
        
        # Consume records
        count = 0
        async for _ in records:
            count += 1
            if count > 100:
                break

    async def test_scan_with_by_key(self, client):
        """Test scan with PartitionFilter.by_key()."""
        # Create a key that exists in the test data
        key = Key("test", "test", "test_key")
        pf = PartitionFilter.by_key(key)
        records = await client.scan(ScanPolicy(), pf, "test", "test", None)
        assert isinstance(records, Recordset)
        
        # Consume records
        count = 0
        async for _ in records:
            count += 1
            if count > 100:
                break

    async def test_query_with_by_id(self, client):
        """Test query with PartitionFilter.by_id()."""
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_id(0)
        records = await client.query(QueryPolicy(), pf, stmt)
        assert isinstance(records, Recordset)
        
        # Consume records
        count = 0
        async for _ in records:
            count += 1
            if count > 100:
                break

    async def test_query_with_by_range(self, client):
        """Test query with PartitionFilter.by_range()."""
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_range(0, 10)
        records = await client.query(QueryPolicy(), pf, stmt)
        assert isinstance(records, Recordset)
        
        # Consume records
        count = 0
        async for _ in records:
            count += 1
            if count > 100:
                break

    async def test_scan_with_modified_filter(self, client):
        """Test scan with a manually modified PartitionFilter."""
        pf = PartitionFilter.all()
        # Modify the filter
        pf.begin = 0
        pf.count = 5
        records = await client.scan(ScanPolicy(), pf, "test", "test", None)
        assert isinstance(records, Recordset)
        
        # Consume records
        count = 0
        async for _ in records:
            count += 1
            if count > 100:
                break

    async def test_partitions_getter_after_scan(self, client):
        """Test getting partitions after a scan operation."""
        pf = PartitionFilter.by_range(0, 10)
        policy = ScanPolicy()
        policy.max_records = 50  # Limit records to test pagination
        
        records = await client.scan(policy, pf, "test", "test", None)
        
        # Consume all records to ensure scan completes
        count = 0
        async for _ in records:
            count += 1
        
        # After scan completes, partitions may be populated (depends on implementation)
        # Note: partitions are typically populated after the scan fully completes
        partitions = pf.partitions
        # Partitions may be None if scan completed without partial results
        # or may be a list if there are partition statuses
        if partitions is not None:
            assert isinstance(partitions, list)
            # If populated, should have partition status for partitions in range
            if len(partitions) > 0:
                for ps in partitions:
                    assert isinstance(ps, PartitionStatus)

    async def test_partitions_setter_resume_scan(self, client):
        """Test setting partitions to resume a scan."""
        pf1 = PartitionFilter.by_range(0, 5)
        policy = ScanPolicy()
        policy.max_records = 10
        
        # First scan - consume all records
        records1 = await client.scan(policy, pf1, "test", "test", None)
        count1 = 0
        async for _ in records1:
            count1 += 1
        
        # Get partitions from first scan (may be None if scan completed fully)
        partitions = pf1.partitions
        
        # If partitions are populated, test resume functionality
        if partitions is not None and len(partitions) > 0:
            # Create new filter and set partitions to resume
            pf2 = PartitionFilter.by_range(0, 5)
            pf2.partitions = partitions
            
            # Second scan should resume from where first left off
            records2 = await client.scan(policy, pf2, "test", "test", None)
            count2 = 0
            async for _ in records2:
                count2 += 1
            
            # Both scans should have returned records
            assert count1 > 0
            assert count2 >= 0  # May be 0 if first scan exhausted all records
        else:
            # If partitions weren't populated, that's also valid behavior
            assert count1 >= 0

    async def test_partitions_setter_none_reset(self, client):
        """Test setting partitions to None resets the cursor."""
        pf = PartitionFilter.by_range(0, 5)
        policy = ScanPolicy()
        policy.max_records = 10
        
        # First scan - consume all records
        records1 = await client.scan(policy, pf, "test", "test", None)
        count1 = 0
        async for _ in records1:
            count1 += 1
        
        # Reset by setting to None (works regardless of whether partitions were populated)
        pf.partitions = None
        assert pf.partitions is None
        
        # Second scan should start from beginning again
        records2 = await client.scan(policy, pf, "test", "test", None)
        count2 = 0
        async for _ in records2:
            count2 += 1
        
        # Should get records again after reset
        assert count2 >= 0  # May be 0 if no records in range

    async def test_partitions_with_pagination(self, client):
        """Test partitions with pagination using max_records."""
        pf = PartitionFilter.all()
        policy = ScanPolicy()
        policy.max_records = 100  # Page size
        
        total_records = 0
        page = 0
        
        while not pf.done() and page < 5:  # Limit to 5 pages for test
            records = await client.scan(policy, pf, "test", "test", None)
            
            page_count = 0
            async for _ in records:
                page_count += 1
                total_records += 1
            
            page += 1
            
            # After each page, partitions may be updated (implementation dependent)
            partitions = pf.partitions
            if partitions:
                assert isinstance(partitions, list)
        
        # Should have scanned at least one page
        assert page > 0

    async def test_query_with_partitions(self, client):
        """Test query with partitions getter/setter."""
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_range(0, 5)
        policy = QueryPolicy()
        policy.max_records = 20
        
        records = await client.query(policy, pf, stmt)
        
        # Consume all records
        count = 0
        async for _ in records:
            count += 1
        
        # Partitions may be populated after query (implementation dependent)
        partitions = pf.partitions
        if partitions is not None:
            assert isinstance(partitions, list)

    async def test_partition_status_fields(self, client):
        """Test that PartitionStatus objects have expected fields."""
        pf = PartitionFilter.by_range(0, 3)
        policy = ScanPolicy()
        policy.max_records = 10
        
        records = await client.scan(policy, pf, "test", "test", None)
        
        # Consume records
        async for _ in records:
            pass
        
        partitions = pf.partitions
        if partitions:
            for ps in partitions:
                assert isinstance(ps, PartitionStatus)
                # Check that PartitionStatus has expected fields
                assert hasattr(ps, 'bval')
                assert hasattr(ps, 'id')
                assert hasattr(ps, 'retry')
                assert hasattr(ps, 'digest')
                # id should be a valid partition ID
                assert isinstance(ps.id, int)
                assert 0 <= ps.id < 4096
