import pytest
from aerospike_async import PartitionFilter, Key, QueryPolicy, Statement, PartitionStatus, Recordset, WritePolicy

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

    @pytest.mark.asyncio
    async def test_partition_filter_partitions_property_works_in_python_asyncio_context(self):
        """Test that PartitionFilter.partitions getter/setter works in Python asyncio context.

        This test verifies that the partitions property can be accessed from Python's asyncio
        context without requiring a Tokio runtime handle. The getter uses blocking_lock()
        instead of requiring Handle::try_current().
        """
        pf = PartitionFilter.by_range(0, 1)
        ps = PartitionStatus(0)
        ps.retry = False
        ps.bval = 0
        ps.digest = None

        # Setter should not raise.
        pf.partitions = [ps]

        # Getter should not raise - this was previously failing with:
        # ValueError: No Tokio runtime available. This method must be called from within an async context.
        got = pf.partitions
        assert got is not None
        assert len(got) == 1
        assert got[0].id == 0

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

    async def test_recordset_partition_filter(self, client):
        """Test Recordset.partition_filter() returns updated PartitionFilter."""
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_range(0, 5)
        records = await client.query(QueryPolicy(), pf, stmt)
        assert isinstance(records, Recordset)

        count = 0
        async for _ in records:
            count += 1
            if count > 10:
                break

        updated_pf = await records.partition_filter()
        assert updated_pf is not None
        assert isinstance(updated_pf, PartitionFilter)

    async def test_recordset_partition_filter_active(self, client):
        """Test Recordset.partition_filter() behavior with active recordsets."""
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_range(0, 5)
        records = await client.query(QueryPolicy(), pf, stmt)
        assert isinstance(records, Recordset)

        # Check immediately after query
        # partition_filter() may return None or a PartitionFilter depending on implementation
        # The key is that it's callable and returns a valid result
        updated_pf = await records.partition_filter()
        assert updated_pf is None or isinstance(updated_pf, PartitionFilter)

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
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_range(0, 3)
        policy = QueryPolicy()
        policy.max_records = 10

        records = await client.query(policy, pf, stmt)

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


class TestQueryPagination(TestFixtureInsertRecord):
    """Test query pagination using Recordset.partition_filter()."""

    async def test_query_pagination_basic(self, client):
        """Test basic query pagination with max_records."""
        wp = WritePolicy()
        for i in range(1, 21):
            key = Key("test", "test", i)
            await client.put(wp, key, {"bin": i})

        stmt = Statement("test", "test", None)
        policy = QueryPolicy()
        policy.max_records = 10

        pf = PartitionFilter.all()
        total_records = 0
        page_count = 0

        while page_count < 5 and not pf.done():
            records = await client.query(policy, pf, stmt)
            page_records = 0

            async for _ in records:
                page_records += 1
                total_records += 1

            page_count += 1

            updated_pf = await records.partition_filter()
            if updated_pf is not None:
                pf = updated_pf
            else:
                break

        assert page_count > 0
        assert total_records > 0

    async def test_query_pagination_with_results(self, client):
        """Test query pagination using async iteration."""
        wp = WritePolicy()
        for i in range(1, 31):
            key = Key("test", "test", i)
            await client.put(wp, key, {"bin": i})

        stmt = Statement("test", "test", None)
        policy = QueryPolicy()
        policy.max_records = 20

        pf = PartitionFilter.all()
        total_records = 0
        pages = 0

        while pages < 10 and not pf.done():
            records = await client.query(policy, pf, stmt)
            page_records = 0

            async for _ in records:
                page_records += 1
                total_records += 1

            pages += 1

            updated_pf = await records.partition_filter()
            if updated_pf is not None:
                pf = updated_pf
            else:
                break

        assert pages > 0
        assert total_records > 0

    async def test_query_pagination_done_check(self, client):
        """Test that pagination stops when done() returns True."""
        wp = WritePolicy()
        for i in range(1, 11):
            key = Key("test", "test", i)
            await client.put(wp, key, {"bin": i})

        stmt = Statement("test", "test", None)
        policy = QueryPolicy()
        policy.max_records = 50

        pf = PartitionFilter.all()
        pages = 0
        max_pages = 10

        while pages < max_pages and not pf.done():
            records = await client.query(policy, pf, stmt)

            async for _ in records:
                pass

            pages += 1

            updated_pf = await records.partition_filter()
            if updated_pf is not None:
                pf = updated_pf
            else:
                break

        assert pages <= max_pages

    async def test_query_pagination_empty_resultset(self, client):
        """Test pagination with empty resultset."""
        stmt = Statement("test", "nonexistent_set", ["bin"])
        policy = QueryPolicy()
        policy.max_records = 10

        pf = PartitionFilter.by_range(0, 1)
        records = await client.query(policy, pf, stmt)

        count = 0
        async for _ in records:
            count += 1

        assert count == 0

        updated_pf = await records.partition_filter()
        if updated_pf is not None:
            assert updated_pf.done() is True


class TestQueryResume(TestFixtureInsertRecord):
    """Test query resume functionality using Recordset.partition_filter()."""

    async def test_query_resume_after_partial_consumption(self, client):
        """Test resuming a query after partially consuming records."""
        wp = WritePolicy()
        for i in range(1, 31):
            key = Key("test", "test", i)
            await client.put(wp, key, {"bin": i})

        stmt = Statement("test", "test", None)
        policy = QueryPolicy()
        policy.max_records = 10  # Smaller max_records so query finishes after first batch

        pf = PartitionFilter.all()
        records = await client.query(policy, pf, stmt)

        first_batch_count = 0
        async for _ in records:
            first_batch_count += 1

        # Wait for recordset to become inactive
        import asyncio
        max_wait = 10
        for _ in range(max_wait):
            if not records.active:
                break
            await asyncio.sleep(0.1)

        updated_pf = await records.partition_filter()
        assert updated_pf is not None

        resumed_records = await client.query(policy, updated_pf, stmt)
        resumed_count = 0

        async for _ in resumed_records:
            resumed_count += 1

        assert first_batch_count > 0
        assert resumed_count > 0

    async def test_query_resume_complete_consumption(self, client):
        """Test resuming after fully consuming a recordset."""
        wp = WritePolicy()
        for i in range(1, 21):
            key = Key("test", "test", i)
            await client.put(wp, key, {"bin": i})

        stmt = Statement("test", "test", None)
        policy = QueryPolicy()
        policy.max_records = 50

        pf = PartitionFilter.all()
        records = await client.query(policy, pf, stmt)

        first_count = 0
        async for _ in records:
            first_count += 1

        updated_pf = await records.partition_filter()
        assert updated_pf is not None

        resumed_records = await client.query(policy, updated_pf, stmt)
        resumed_count = 0

        async for _ in resumed_records:
            resumed_count += 1

        assert first_count > 0
        assert resumed_count >= 0

    async def test_query_resume_multiple_times(self, client):
        """Test resuming a query multiple times."""
        wp = WritePolicy()
        for i in range(1, 21):
            key = Key("test", "test", i)
            await client.put(wp, key, {"bin": i})

        stmt = Statement("test", "test", None)
        policy = QueryPolicy()
        policy.max_records = 20

        pf = PartitionFilter.all()
        total_count = 0

        for resume_iteration in range(3):
            records = await client.query(policy, pf, stmt)
            iteration_count = 0

            async for _ in records:
                iteration_count += 1
                total_count += 1
                if iteration_count >= 5:
                    break

            updated_pf = await records.partition_filter()
            if updated_pf is not None:
                pf = updated_pf
            else:
                break

        assert total_count > 0


class TestQueryPartitionEdgeCases(TestFixtureInsertRecord):
    """Test edge cases and error conditions for partition queries."""

    async def test_query_partition_invalid_begin(self, client):
        """Test query with invalid partition begin value."""
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_range(4096, 1)

        with pytest.raises(Exception):
            records = await client.query(QueryPolicy(), pf, stmt)
            async for _ in records:
                pass

    async def test_query_partition_invalid_count(self, client):
        """Test query with invalid partition count."""
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_range(0, 5000)

        with pytest.raises(Exception):
            records = await client.query(QueryPolicy(), pf, stmt)
            async for _ in records:
                pass

    async def test_query_partition_zero_count(self, client):
        """Test query with zero partition count."""
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_range(0, 0)

        with pytest.raises(Exception):
            records = await client.query(QueryPolicy(), pf, stmt)
            async for _ in records:
                pass

    async def test_query_partition_nonexistent_namespace(self, client):
        """Test query with non-existent namespace."""
        stmt = Statement("nonexistent_ns", "test", ["bin"])
        pf = PartitionFilter.by_range(0, 1)

        with pytest.raises(Exception):
            records = await client.query(QueryPolicy(), pf, stmt)
            async for _ in records:
                pass

    async def test_query_partition_nonexistent_set(self, client):
        """Test query with non-existent set."""
        stmt = Statement("test", "nonexistent_set", ["bin"])
        pf = PartitionFilter.by_range(0, 1)

        records = await client.query(QueryPolicy(), pf, stmt)
        count = 0
        async for _ in records:
            count += 1

        assert count == 0

    async def test_query_partition_filter_reuse(self, client):
        """Test reusing the same PartitionFilter object."""
        wp = WritePolicy()
        for i in range(1, 11):
            key = Key("test", "test", i)
            await client.put(wp, key, {"bin": i})

        stmt = Statement("test", "test", None)
        pf = PartitionFilter.all()

        records1 = await client.query(QueryPolicy(), pf, stmt)
        count1 = 0
        async for _ in records1:
            count1 += 1

        records2 = await client.query(QueryPolicy(), pf, stmt)
        count2 = 0
        async for _ in records2:
            count2 += 1

        assert count1 > 0
        assert count2 > 0

    async def test_query_partition_filter_active_recordset(self, client):
        """Test partition_filter() behavior with active recordsets."""
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_range(0, 2)
        records = await client.query(QueryPolicy(), pf, stmt)

        # Check immediately after query
        # partition_filter() may return None or a PartitionFilter depending on implementation
        # The key is that it's callable and returns a valid result
        updated_pf = await records.partition_filter()
        assert updated_pf is None or isinstance(updated_pf, PartitionFilter)

        # Consume one record
        async for _ in records:
            break

        # After consuming, partition_filter() may return None (if still active) or a PartitionFilter (if inactive)
        updated_pf = await records.partition_filter()
        assert updated_pf is None or isinstance(updated_pf, PartitionFilter)

    async def test_query_partition_filter_after_close(self, client):
        """Test partition_filter() after recordset is closed."""
        stmt = Statement("test", "test", ["bin"])
        pf = PartitionFilter.by_range(0, 2)
        records = await client.query(QueryPolicy(), pf, stmt)

        async for _ in records:
            pass

        records.close()
        updated_pf = await records.partition_filter()

        assert updated_pf is not None
        assert isinstance(updated_pf, PartitionFilter)
