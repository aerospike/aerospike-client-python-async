# Copyright 2023-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Tests for execute_udf functionality."""
import os
import pytest
from aerospike_async import WritePolicy, ReadPolicy, Key, UDFLang
from aerospike_async.exceptions import ServerError, ResultCode, UDFBadResponse
from fixtures import TestFixtureConnection


class TestExecuteUDF(TestFixtureConnection):
    """Test execute_udf functionality."""

    @pytest.fixture
    async def client_with_udf(self, client):
        """Register a test UDF for execute tests."""
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

    async def test_execute_udf_write_bin(self, client_with_udf):
        """Test executing UDF to write a bin."""
        key = Key("test", "test", "udfkey1")
        wp = WritePolicy()
        rp = ReadPolicy()

        await client_with_udf.delete(wp, key)

        result = await client_with_udf.execute_udf(
            wp, key, "record_example", "writeBin", ["udfbin1", "string value"]
        )
        assert result is None

        record = await client_with_udf.get(rp, key, ["udfbin1"])
        assert record is not None
        assert record.bins["udfbin1"] == "string value"

    async def test_execute_udf_read_bin(self, client_with_udf):
        """Test executing UDF to read a bin."""
        key = Key("test", "test", "udfkey2")
        wp = WritePolicy()

        await client_with_udf.put(wp, key, {"udfbin2": "test value"})

        result = await client_with_udf.execute_udf(
            wp, key, "record_example", "readBin", ["udfbin2"]
        )
        assert result == "test value"

    async def test_execute_udf_get_generation(self, client_with_udf):
        """Test executing UDF to get record generation."""
        key = Key("test", "test", "udfkey3")
        wp = WritePolicy()

        await client_with_udf.put(wp, key, {"bin": "value"})

        result = await client_with_udf.execute_udf(wp, key, "record_example", "getGeneration", None)
        assert isinstance(result, int)
        assert result >= 0

    async def test_execute_udf_with_validation_success(self, client_with_udf):
        """Test executing UDF with validation that succeeds."""
        key = Key("test", "test", "udfkey4")
        wp = WritePolicy()

        await client_with_udf.delete(wp, key)

        result = await client_with_udf.execute_udf(
            wp, key, "record_example", "writeWithValidation", ["udfbin4", 5]
        )
        assert result is None

    async def test_execute_udf_with_validation_failure(self, client_with_udf):
        """Test executing UDF with validation that fails."""
        key = Key("test", "test", "udfkey5")
        wp = WritePolicy()

        await client_with_udf.delete(wp, key)

        with pytest.raises(UDFBadResponse):
            await client_with_udf.execute_udf(
                wp, key, "record_example", "writeWithValidation", ["udfbin5", 11]
            )

    async def test_execute_udf_write_unique_success(self, client_with_udf):
        """Test executing UDF writeUnique when record doesn't exist."""
        key = Key("test", "test", "udfkey6")
        wp = WritePolicy()
        rp = ReadPolicy()

        await client_with_udf.delete(wp, key)

        result = await client_with_udf.execute_udf(
            wp, key, "record_example", "writeUnique", ["udfbin6", "first"]
        )
        assert result is None

        record = await client_with_udf.get(rp, key, ["udfbin6"])
        assert record is not None
        assert record.bins["udfbin6"] == "first"

    async def test_execute_udf_write_unique_failure(self, client_with_udf):
        """Test executing UDF writeUnique when record already exists."""
        key = Key("test", "test", "udfkey7")
        wp = WritePolicy()
        rp = ReadPolicy()

        await client_with_udf.delete(wp, key)
        await client_with_udf.put(wp, key, {"udfbin7": "first"})

        result = await client_with_udf.execute_udf(
            wp, key, "record_example", "writeUnique", ["udfbin7", "second"]
        )
        assert result is None

        record = await client_with_udf.get(rp, key, ["udfbin7"])
        assert record.bins["udfbin7"] == "first"

    async def test_execute_udf_no_args(self, client_with_udf):
        """Test executing UDF with no arguments."""
        key = Key("test", "test", "udfkey8")
        wp = WritePolicy()

        await client_with_udf.delete(wp, key)

        result = await client_with_udf.execute_udf(
            wp, key, "record_example", "getGeneration", None
        )
        assert isinstance(result, int)

    async def test_execute_udf_nonexistent_function(self, client_with_udf):
        """Test executing UDF with non-existent function name."""
        key = Key("test", "test", "udfkey9")
        wp = WritePolicy()

        with pytest.raises(UDFBadResponse):
            await client_with_udf.execute_udf(
                wp, key, "record_example", "nonexistentFunction", None
            )

    async def test_execute_udf_nonexistent_module(self, client_with_udf):
        """Test executing UDF with non-existent module."""
        key = Key("test", "test", "udfkey10")
        wp = WritePolicy()

        with pytest.raises(UDFBadResponse):
            await client_with_udf.execute_udf(
                wp, key, "nonexistent", "writeBin", ["bin", "value"]
            )

    @pytest.fixture
    async def client_with_sleep_udf(self, client):
        """Register sleep UDF for timeout tests."""
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "sleep_example.lua")
        server_path = "sleep_example.lua"

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

    async def test_udf_timeout_handling(self, client_with_sleep_udf):
        """Test that total_timeout handles UDF timeouts (may be server-side UDFBadResponse or client-side TimeoutError)."""
        from aerospike_async.exceptions import TimeoutError, UDFBadResponse

        key = Key("test", "test", "timeout_test_key")
        wp = WritePolicy()

        # Set a short total_timeout (500ms)
        wp.total_timeout = 500

        # Execute UDF that sleeps for longer than the timeout (2000ms = 2 seconds)
        # This should trigger a timeout error
        # Note: Server may return UDFBadResponse with "UDF Execution Timeout" message
        # or client may raise TimeoutError - both indicate timeout enforcement
        with pytest.raises((TimeoutError, UDFBadResponse)):
            await client_with_sleep_udf.execute_udf(
                wp, key, "sleep_example", "sleep", [2000]
            )

    async def test_udf_timeout_not_triggered_on_fast_operation(self, client_with_sleep_udf):
        """Test that total_timeout doesn't trigger on fast UDF operations."""
        key = Key("test", "test", "timeout_test_key2")
        wp = WritePolicy()

        # Set a total_timeout (1000ms)
        wp.total_timeout = 1000

        # Execute UDF that sleeps for shorter than the timeout (100ms)
        # This should complete successfully
        result = await client_with_sleep_udf.execute_udf(
            wp, key, "sleep_example", "sleep", [100]
        )
        assert result == "slept"
