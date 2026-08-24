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
from aerospike_async.exceptions import BatchFailedError, ClientError, UDFBadResponse, ResultCode
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
            remove_task = await client.remove_udf(server_path)
            await remove_task.wait_till_complete()
        except Exception:
            pass

        # Register the UDF
        task = await client.register_udf_from_file(udf_path, server_path, UDFLang.LUA)
        completed = await task.wait_till_complete()
        assert completed, f"UDF registration did not complete. Final status: {await task.query_status()}"

        yield client

        # Clean up
        try:
            remove_task = await client.remove_udf(server_path)
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
                await client_with_udf.delete(key, policy=wp)
            except Exception:
                pass

        # Execute batch UDF
        results = await client_with_udf.batch_apply(
            keys,
            "record_example",
            "writeBin",
            ["B5", "value5"],
            batch_policy=None,
            udf_policy=None,
        )

        assert len(results) == 2
        for result in results:
            assert result.result_code == ResultCode.OK

        # Verify records were written
        for key in keys:
            record = await client_with_udf.get(key, ["B5"], policy=rp)
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
                await client_with_udf.delete(key, policy=wp)
            except Exception:
                pass

        # Execute batch UDF with policies
        results = await client_with_udf.batch_apply(
            keys,
            "record_example",
            "writeBin",
            ["B6", "value6"],
            batch_policy=bp,
            udf_policy=udfp,
        )

        assert len(results) == 2
        for result in results:
            assert result.result_code == ResultCode.OK

        # Verify records were written
        for key in keys:
            record = await client_with_udf.get(key, ["B6"], policy=rp)
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
                await client_with_udf.delete(key, policy=wp)
            except Exception:
                pass

        # Execute batch UDF with invalid value (should trigger validation error)
        results = await client_with_udf.batch_apply(
            keys,
            "record_example",
            "writeWithValidation",
            ["B5", 999],
            batch_policy=None,
            udf_policy=None,
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
                await client_with_udf.delete(key, policy=wp)
            except Exception:
                pass

        # Execute batch UDF with no args
        results = await client_with_udf.batch_apply(
            keys,
            "record_example",
            "getGeneration",
            None,
            batch_policy=None,
            udf_policy=None,
        )

        assert len(results) == 2
        for result in results:
            assert result.result_code == ResultCode.OK
            assert result.record is not None
            assert "SUCCESS" in result.record.bins
            assert isinstance(result.record.bins["SUCCESS"], int)


class TestBatchFailedError(TestFixtureConnection):
    """A batch-wide failure raises BatchFailedError carrying per-key outcomes."""

    @pytest.fixture
    async def client_with_sleep_udf(self, client):
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "sleep_example.lua")
        server_path = "sleep_example.lua"
        try:
            remove_task = await client.remove_udf(server_path)
            await remove_task.wait_till_complete()
        except Exception:
            pass
        task = await client.register_udf_from_file(udf_path, server_path, UDFLang.LUA)
        assert await task.wait_till_complete()
        yield client
        try:
            remove_task = await client.remove_udf(server_path)
            await remove_task.wait_till_complete()
        except Exception:
            pass

    async def test_batch_udf_client_timeout_raises_batch_failed_with_rows(
        self, client_with_sleep_udf
    ):
        """Client socket timeout racing a longer server-side UDF sleep.

        total_timeout must stay 0: a nonzero total makes the client send
        min(socket, total) as the server-side deadline and the server's own
        abort then beats the client timer. With no server deadline the 250ms
        socket timer is the only one racing the 1000ms sleep, so the
        client-side timeout is deterministic. The writes reached the wire,
        so the aggregate and every row are in-doubt, and unanswered rows are
        stamped TIMEOUT.
        """
        keys = [Key("test", "test", f"pac_budf_fail_{i}") for i in range(6)]
        bp = BatchPolicy()
        bp.socket_timeout = 250
        bp.total_timeout = 0
        bp.max_retries = 0

        with pytest.raises(BatchFailedError) as exc_info:
            await client_with_sleep_udf.batch_apply(
                keys,
                "sleep_example",
                "sleep",
                [1000],
                batch_policy=bp,
                udf_policy=None,
            )
        err = exc_info.value
        assert isinstance(err, ClientError)
        assert err.in_doubt is True
        assert err.records is not None and len(err.records) == len(keys)
        for row in err.records:
            assert isinstance(row, BatchRecord)
            assert row.result_code == ResultCode.TIMEOUT
            assert row.in_doubt is True
        assert sorted(r.key.digest for r in err.records) == sorted(k.digest for k in keys)
