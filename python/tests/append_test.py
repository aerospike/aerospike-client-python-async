import pytest
from aerospike_async import WritePolicy, ReadPolicy
from aerospike_async.exceptions import ServerError
from fixtures import TestFixtureInsertRecord


class TestAppend(TestFixtureInsertRecord):
    """Test client.append() method functionality."""

    async def test_append(self, client, key):
        """Test basic append operation."""
        retval = await client.append(WritePolicy(), key, {"brand": "d"})
        assert retval is None

        rec = await client.get(ReadPolicy(), key)
        assert rec.bins["brand"] == "Fordd"


    async def test_append_with_policy(self, client, key):
        """Test append operation with write policy."""
        wp = WritePolicy()
        retval = await client.append(wp, key, {"brand": "d"})
        assert retval is None


    async def test_append_unsupported_bin_type(self, client, key):
        """Test append operation with unsupported bin type raises ServerError."""
        with pytest.raises(ServerError):
            await client.append(WritePolicy(), key, {"year": "d"})
