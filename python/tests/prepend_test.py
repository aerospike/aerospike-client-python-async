import pytest
from aerospike_async import WritePolicy, ReadPolicy
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureInsertRecord


class TestPrepend(TestFixtureInsertRecord):
    """Test client.prepend() method functionality."""

    async def test_prepend(self, client, key):
        """Test basic prepend operation."""
        retval = await client.prepend(WritePolicy(), key, {"brand": "F"})
        assert retval is None

        rec = await client.get(ReadPolicy(), key)
        assert rec.bins["brand"] == "FFord"

    async def test_prepend_with_policy(self, client, key):
        """Test prepend operation with write policy."""
        wp = WritePolicy()
        retval = await client.prepend(wp, key, {"brand": "F"})
        assert retval is None

        rec = await client.get(ReadPolicy(), key)
        assert rec.bins["brand"] == "FFord"

    async def test_prepend_nonexistent_bin(self, client, key):
        """Test prepend operation on non-existent bin."""
        await client.append(WritePolicy(), key, {"brand1": "F"})
        rec = await client.get(ReadPolicy(), key)
        assert rec.bins["brand1"] == "F"

    async def test_prepend_unsupported_type(self, client, key):
        """Test prepend operation with unsupported type raises ServerError."""
        with pytest.raises(ServerError) as exc_info:
            await client.prepend(WritePolicy(), key, {"year": "d"})
        assert exc_info.value.result_code == ResultCode.BIN_TYPE_ERROR
