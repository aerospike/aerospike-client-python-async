import os
import time
import pytest
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

    @pytest.mark.skipif(
        os.environ.get("CI") == "true",
        reason="Future timestamp truncate can block namespace writes in CI"
    )
    async def test_truncate_future_timestamp(self, client):
        """Test truncate operation with future timestamp."""

        seconds_in_future = 1000
        future_threshold = time.time_ns() + seconds_in_future * 10**9
        isolated_set = "test_truncate_future_isolated"

        try:
            await client.truncate("test", isolated_set, future_threshold)
        except ServerError as e:
            assert e.result_code == ResultCode.PARAMETER_ERROR
