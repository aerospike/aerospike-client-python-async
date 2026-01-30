import pytest
import time
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureInsertRecord


class TestTruncate(TestFixtureInsertRecord):
    """Test client.truncate() method functionality."""

    async def test_truncate(self, client):
        """Test basic truncate operation."""
        retval = await client.truncate("test", "test", None)
        assert retval is None

    async def test_truncate_before_nanos(self, client):
        """Test truncate operation with before_nanos parameter."""
        retval = await client.truncate("test", "test", before_nanos=0)
        assert retval is None

    async def test_truncate_fail(self, client):
        """Test truncate operation with future timestamp raises ServerError."""
        seconds_in_future = 1000
        future_threshold = time.time_ns() + seconds_in_future * 10**9
        with pytest.raises(ServerError) as exc_info:
            await client.truncate("test", "test", future_threshold)
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
