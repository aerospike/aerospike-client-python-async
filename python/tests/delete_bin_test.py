"""Tests for deleting specific bins from records."""

import pytest
from aerospike_async import WritePolicy, ReadPolicy, Key
from fixtures import TestFixtureConnection


class TestDeleteBin(TestFixtureConnection):
    """Test deleting specific bins by setting them to None."""

    async def test_delete_single_bin(self, client):
        """Test deleting a single bin by setting it to None."""
        key = Key("test", "test", "delete_bin_single")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            # Create record with two bins
            await client.put(wp, key, {"bin1": "value1", "bin2": "value2"})

            # Verify both bins exist
            rec = await client.get(rp, key)
            assert "bin1" in rec.bins
            assert "bin2" in rec.bins

            # Delete bin1 by setting to None
            await client.put(wp, key, {"bin1": None})

            # Verify bin1 is deleted, bin2 remains
            rec = await client.get(rp, key)
            assert "bin1" not in rec.bins, "bin1 should be deleted"
            assert rec.bins.get("bin2") == "value2", "bin2 should remain"
        finally:
            try:
                await client.delete(wp, key)
            except Exception:
                pass

    async def test_delete_multiple_bins(self, client):
        """Test deleting multiple bins in one operation."""
        key = Key("test", "test", "delete_bin_multi")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            # Create record with three bins
            await client.put(wp, key, {"bin1": "v1", "bin2": "v2", "bin3": "v3"})

            # Delete bin1 and bin2
            await client.put(wp, key, {"bin1": None, "bin2": None})

            # Verify only bin3 remains
            rec = await client.get(rp, key)
            assert "bin1" not in rec.bins
            assert "bin2" not in rec.bins
            assert rec.bins.get("bin3") == "v3"
        finally:
            try:
                await client.delete(wp, key)
            except Exception:
                pass

    async def test_delete_all_bins_removes_record(self, client):
        """Test that deleting all bins removes the record entirely.

        Aerospike server behavior: a record with no bins is automatically deleted.
        """
        key = Key("test", "test", "delete_bin_all")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            # Create record with two bins
            await client.put(wp, key, {"bin1": "v1", "bin2": "v2"})

            # Verify record exists
            exists = await client.exists(rp, key)
            assert exists, "Record should exist initially"

            # Delete all bins
            await client.put(wp, key, {"bin1": None, "bin2": None})

            # Record should be automatically deleted by server
            exists = await client.exists(rp, key)
            assert not exists, "Record should be deleted when all bins are removed"
        finally:
            try:
                await client.delete(wp, key)
            except Exception:
                pass

    async def test_delete_nonexistent_bin(self, client):
        """Test deleting a bin that doesn't exist is a no-op."""
        key = Key("test", "test", "delete_bin_noexist")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            # Create record with one bin
            await client.put(wp, key, {"bin1": "value1"})

            # Try to delete nonexistent bin
            await client.put(wp, key, {"nonexistent": None})

            # Original bin should still exist
            rec = await client.get(rp, key)
            assert rec.bins.get("bin1") == "value1"
        finally:
            try:
                await client.delete(wp, key)
            except Exception:
                pass

    async def test_delete_and_add_bin_same_operation(self, client):
        """Test deleting one bin while adding another in same operation."""
        key = Key("test", "test", "delete_bin_add")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            # Create record with bin1
            await client.put(wp, key, {"bin1": "value1"})

            # Delete bin1 and add bin2 in same operation
            await client.put(wp, key, {"bin1": None, "bin2": "value2"})

            # Verify bin1 deleted, bin2 added
            rec = await client.get(rp, key)
            assert "bin1" not in rec.bins
            assert rec.bins.get("bin2") == "value2"
        finally:
            try:
                await client.delete(wp, key)
            except Exception:
                pass

    async def test_delete_bin_different_types(self, client):
        """Test deleting bins of various types."""
        key = Key("test", "test", "delete_bin_types")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            # Create record with bins of different types
            await client.put(wp, key, {
                "str_bin": "string",
                "int_bin": 42,
                "float_bin": 3.14,
                "list_bin": [1, 2, 3],
                "map_bin": {"a": 1}
            })

            # Delete string and int bins
            await client.put(wp, key, {"str_bin": None, "int_bin": None})

            # Verify deletions
            rec = await client.get(rp, key)
            assert "str_bin" not in rec.bins
            assert "int_bin" not in rec.bins
            assert rec.bins.get("float_bin") == 3.14
            assert rec.bins.get("list_bin") == [1, 2, 3]
            assert rec.bins.get("map_bin") == {"a": 1}
        finally:
            try:
                await client.delete(wp, key)
            except Exception:
                pass
