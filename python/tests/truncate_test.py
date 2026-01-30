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

    async def test_truncate_future_timestamp(self, client):
        """Test truncate operation with future timestamp.

        Uses isolated set name to avoid polluting other tests.
        """
        seconds_in_future = 1000
        future_threshold = time.time_ns() + seconds_in_future * 10**9
        # Use isolated set name to avoid affecting the shared "test" set
        isolated_set = "test_truncate_future_isolated"
        
        try:
            await client.truncate("test", isolated_set, future_threshold)
            # Server accepted it (newer behavior) - that's fine, test passes
        except ServerError as e:
            # Server rejected it (older behavior) - verify it's PARAMETER_ERROR
            assert e.result_code == ResultCode.PARAMETER_ERROR
