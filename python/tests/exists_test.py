import pytest
from aerospike_async import ReadPolicy
from aerospike_async.exceptions import ServerError
from fixtures import TestFixtureInsertRecord


class TestExists(TestFixtureInsertRecord):
    """Test client.exists() method functionality."""

    async def test_existing_record(self, client, key):
        """Test checking existence of an existing record."""
        retval = await client.exists(ReadPolicy(), key)
        assert retval is True


    async def test_nonexistent_record(self, client, key_invalid_primary_key):
        """Test checking existence of a non-existent record."""
        retval = await client.exists(ReadPolicy(), key_invalid_primary_key)
        assert retval is False


    async def test_exists_with_policy(self, client, key):
        """Test exists operation with read policy."""
        rp = ReadPolicy()
        retval = await client.exists(rp, key)
        assert retval is True


    async def test_exists_fail(self, client, key_invalid_namespace):
        """Test exists operation with invalid namespace raises ConnectionError (timeout)."""
        from aerospike_async.exceptions import ConnectionError
        with pytest.raises(ConnectionError):
            await client.exists(ReadPolicy(), key_invalid_namespace)
