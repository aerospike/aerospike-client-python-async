"""Tests for batch_apply functionality."""
import os
import pytest
from aerospike_async import (
    WritePolicy,
    ReadPolicy,
    Key,
    BatchPolicy,
    BatchUDFPolicy,
    UDFLang,
    BatchRecord,
)
from aerospike_async.exceptions import UDFBadResponse, ResultCode
from fixtures import TestFixtureConnection


class TestBatchApply(TestFixtureConnection):
    """Test batch_apply functionality."""

    @pytest.fixture
    async def client_with_udf(self, client):
        """Register a test UDF for batch tests."""
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "record_example.lua")
        server_path = "record_example.lua"

        # Clean up any existing UDF first
        try:
            remove_task = await client.remove_udf(None, server_path)
            await remove_task.wait_till_complete()
        except Exception:
            pass

        # Register the UDF
        task = await client.register_udf_from_file(None, udf_path, server_path, UDFLang.LUA)
        completed = await task.wait_till_complete()
        assert completed, f"UDF registration did not complete. Final status: {await task.query_status()}"

        yield client

        # Clean up
        try:
            remove_task = await client.remove_udf(None, server_path)
            await remove_task.wait_till_complete()
        except Exception:
            pass

    async def test_batch_apply_basic(self, client_with_udf):
        """Test basic batch UDF execution on multiple keys."""
        keys = [
            Key("test", "test", "batchudf1"),
            Key("test", "test", "batchudf2"),
        ]
        wp = WritePolicy()
        rp = ReadPolicy()

        # Clean up keys
        for key in keys:
            try:
                await client_with_udf.delete(wp, key)
            except Exception:
                pass

        # Execute batch UDF
        results = await client_with_udf.batch_apply(
            None, None, keys, "record_example", "writeBin", ["B5", "value5"]
        )

        assert len(results) == 2
        for result in results:
            assert result.result_code == ResultCode.OK

        # Verify records were written
        for key in keys:
            record = await client_with_udf.get(rp, key, ["B5"])
            assert record is not None
            assert record.bins["B5"] == "value5"

    async def test_batch_apply_with_policies(self, client_with_udf):
        """Test batch_apply with explicit policies."""
        keys = [
            Key("test", "test", "batchudf3"),
            Key("test", "test", "batchudf4"),
        ]
        wp = WritePolicy()
        rp = ReadPolicy()
        bp = BatchPolicy()
        udfp = BatchUDFPolicy()

        # Clean up keys
        for key in keys:
            try:
                await client_with_udf.delete(wp, key)
            except Exception:
                pass

        # Execute batch UDF with policies
        results = await client_with_udf.batch_apply(
            bp, udfp, keys, "record_example", "writeBin", ["B6", "value6"]
        )

        assert len(results) == 2
        for result in results:
            assert result.result_code == ResultCode.OK

        # Verify records were written
        for key in keys:
            record = await client_with_udf.get(rp, key, ["B6"])
            assert record is not None
            assert record.bins["B6"] == "value6"

    async def test_batch_apply_error(self, client_with_udf):
        """Test batch_apply with UDF validation errors."""
        keys = [
            Key("test", "test", "batchudf5"),
            Key("test", "test", "batchudf6"),
        ]
        wp = WritePolicy()

        # Clean up keys
        for key in keys:
            try:
                await client_with_udf.delete(wp, key)
            except Exception:
                pass

        # Execute batch UDF with invalid value (should trigger validation error)
        results = await client_with_udf.batch_apply(
            None, None, keys, "record_example", "writeWithValidation", ["B5", 999]
        )

        assert len(results) == 2
        for result in results:
            assert result.result_code == ResultCode.UDF_BAD_RESPONSE
            assert result.record is not None

    async def test_batch_apply_no_args(self, client_with_udf):
        """Test batch_apply with no arguments."""
        keys = [
            Key("test", "test", "batchudf7"),
            Key("test", "test", "batchudf8"),
        ]
        wp = WritePolicy()

        # Clean up keys
        for key in keys:
            try:
                await client_with_udf.delete(wp, key)
            except Exception:
                pass

        # Execute batch UDF with no args
        results = await client_with_udf.batch_apply(
            None, None, keys, "record_example", "getGeneration", None
        )

        assert len(results) == 2
        for result in results:
            assert result.result_code == ResultCode.OK
            assert result.record is not None
            assert "SUCCESS" in result.record.bins
            assert isinstance(result.record.bins["SUCCESS"], int)
