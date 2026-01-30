"""Tests for RecordExistsAction policies.

Tests different write behaviors based on record existence.
"""

import pytest
from aerospike_async import Key, WritePolicy, ReadPolicy, RecordExistsAction
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureConnection


class TestReplace(TestFixtureConnection):
    """Test REPLACE action - replaces entire record, removing other bins."""

    async def test_replace_removes_other_bins(self, client):
        """Test that REPLACE removes bins not included in the new write."""
        key = Key("test", "test", "replace_1")

        # Clean up
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        # Create record with two bins
        await client.put(WritePolicy(), key, {"bin1": "value1", "bin2": "value2"})

        # Verify both bins exist
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin1"] == "value1"
        assert record.bins["bin2"] == "value2"

        # Replace with only bin3
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.REPLACE
        await client.put(wp, key, {"bin3": "value3"})

        # Verify bin1 and bin2 are gone, only bin3 exists
        record = await client.get(ReadPolicy(), key)
        assert "bin1" not in record.bins
        assert "bin2" not in record.bins
        assert record.bins["bin3"] == "value3"

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_replace_on_nonexistent_creates_record(self, client):
        """Test that REPLACE on non-existent record creates it."""
        key = Key("test", "test", "replace_2")

        # Ensure record doesn't exist
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        # REPLACE on non-existent should create the record
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.REPLACE
        await client.put(wp, key, {"bin": "value"})

        # Verify record was created
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin"] == "value"

        # Cleanup
        await client.delete(WritePolicy(), key)


class TestReplaceOnly(TestFixtureConnection):
    """Test REPLACE_ONLY action - replace only if record exists."""

    async def test_replace_only_succeeds_when_exists(self, client):
        """Test that REPLACE_ONLY succeeds when record exists."""
        key = Key("test", "test", "replace_only_1")

        # Clean up and create record
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        await client.put(WritePolicy(), key, {"bin1": "value1", "bin2": "value2"})

        # REPLACE_ONLY should succeed and replace all bins
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.REPLACE_ONLY
        await client.put(wp, key, {"bin3": "value3"})

        # Verify replacement
        record = await client.get(ReadPolicy(), key)
        assert "bin1" not in record.bins
        assert "bin2" not in record.bins
        assert record.bins["bin3"] == "value3"

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_replace_only_fails_when_not_exists(self, client):
        """Test that REPLACE_ONLY fails when record doesn't exist."""
        key = Key("test", "test", "replace_only_2")

        # Ensure record doesn't exist
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        # REPLACE_ONLY on non-existent should fail
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.REPLACE_ONLY

        with pytest.raises(ServerError) as exc_info:
            await client.put(wp, key, {"bin": "value"})

        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR


class TestUpdate(TestFixtureConnection):
    """Test UPDATE action (default) - merges bins."""

    async def test_update_merges_bins(self, client):
        """Test that UPDATE (default) merges bins, keeping existing ones."""
        key = Key("test", "test", "update_1")

        # Clean up
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        # Create record with two bins
        await client.put(WritePolicy(), key, {"bin1": "value1", "bin2": "value2"})

        # Update with bin3 (default UPDATE action)
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.UPDATE
        await client.put(wp, key, {"bin3": "value3"})

        # Verify all bins exist
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin1"] == "value1"
        assert record.bins["bin2"] == "value2"
        assert record.bins["bin3"] == "value3"

        # Cleanup
        await client.delete(WritePolicy(), key)


class TestUpdateOnly(TestFixtureConnection):
    """Test UPDATE_ONLY action - update only if record exists."""

    async def test_update_only_succeeds_when_exists(self, client):
        """Test that UPDATE_ONLY succeeds when record exists."""
        key = Key("test", "test", "update_only_1")

        # Clean up and create record
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        await client.put(WritePolicy(), key, {"bin1": "value1"})

        # UPDATE_ONLY should succeed
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.UPDATE_ONLY
        await client.put(wp, key, {"bin2": "value2"})

        # Verify both bins exist (merged)
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin1"] == "value1"
        assert record.bins["bin2"] == "value2"

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_update_only_fails_when_not_exists(self, client):
        """Test that UPDATE_ONLY fails when record doesn't exist."""
        key = Key("test", "test", "update_only_2")

        # Ensure record doesn't exist
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        # UPDATE_ONLY on non-existent should fail
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.UPDATE_ONLY

        with pytest.raises(ServerError) as exc_info:
            await client.put(wp, key, {"bin": "value"})

        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR


class TestCreateOnly(TestFixtureConnection):
    """Test CREATE_ONLY action - create only if record doesn't exist."""

    async def test_create_only_succeeds_when_not_exists(self, client):
        """Test that CREATE_ONLY succeeds when record doesn't exist."""
        key = Key("test", "test", "create_only_1")

        # Ensure record doesn't exist
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        # CREATE_ONLY should succeed
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.CREATE_ONLY
        await client.put(wp, key, {"bin": "value"})

        # Verify record was created
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin"] == "value"

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_create_only_fails_when_exists(self, client):
        """Test that CREATE_ONLY fails when record already exists."""
        key = Key("test", "test", "create_only_2")

        # Clean up and create record
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        await client.put(WritePolicy(), key, {"bin1": "value1"})

        # CREATE_ONLY on existing should fail
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.CREATE_ONLY

        with pytest.raises(ServerError) as exc_info:
            await client.put(wp, key, {"bin2": "value2"})

        assert exc_info.value.result_code == ResultCode.KEY_EXISTS_ERROR

        # Verify original record is unchanged
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin1"] == "value1"
        assert "bin2" not in record.bins

        # Cleanup
        await client.delete(WritePolicy(), key)
