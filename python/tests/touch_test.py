import pytest
from aerospike_async import WritePolicy, ReadPolicy
from aerospike_async.exceptions import ServerError
from fixtures import TestFixtureInsertRecord


class TestTouch(TestFixtureInsertRecord):
    """Test client.touch() method functionality."""

    async def test_existing_record(self, client, key):
        """Test touching an existing record."""
        retval = await client.touch(WritePolicy(), key)
        assert retval is None

        rec = await client.get(ReadPolicy(), key)
        assert rec.generation == 2

    async def test_touch_with_policy(self, client, key):
        """Test touch operation with write policy."""
        wp = WritePolicy()
        retval = await client.touch(wp, key)
        assert retval is None

    async def test_nonexistent_record(self, client, key_invalid_primary_key):
        """Test touching a non-existent record raises ServerError."""
        with pytest.raises(ServerError):
            await client.touch(WritePolicy(), key_invalid_primary_key)
