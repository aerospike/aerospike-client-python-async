import pytest
from aerospike_async import WritePolicy, Expiration
from aerospike_async.exceptions import TimeoutError
from fixtures import TestFixtureInsertRecord


class TestDelete(TestFixtureInsertRecord):
    """Test client.delete() method functionality."""

    async def test_delete_existing_record(self, client, key):
        """Test deleting an existing record."""
        rec_existed = await client.delete(WritePolicy(), key)
        assert rec_existed is True

    async def test_delete_nonexistent_record(self, client, key_invalid_primary_key):
        """Test deleting a non-existent record."""
        rec_existed = await client.delete(WritePolicy(), key_invalid_primary_key)
        assert rec_existed is False

    async def test_delete_with_policy(self, client, key):
        """Test delete operation with write policy."""
        wp = WritePolicy()
        rec_existed = await client.delete(wp, key)
        assert rec_existed is True

    async def test_delete_with_nonexistent_namespace(self, client, key_invalid_namespace):
        """Test delete operation with invalid namespace raises TimeoutError."""
        wp = WritePolicy()
        wp.expiration = Expiration.NEVER_EXPIRE
        with pytest.raises(TimeoutError) as exi:
            await client.delete(wp, key_invalid_namespace)
        assert "Timeout" in str(exi.value)