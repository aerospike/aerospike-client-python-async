"""Tests for register_udf and register_udf_from_file functionality."""
import os
import pytest
from aerospike_async import AdminPolicy, UDFLang, TaskStatus
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureConnection


class TestRegisterUDF(TestFixtureConnection):
    """Test register_udf functionality."""

    async def test_register_udf_from_bytes(self, client):
        """Test registering UDF from bytes."""
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "record_example.lua")
        with open(udf_path, "rb") as f:
            udf_body = f.read()

        server_path = "test_register_bytes.lua"

        try:
            task = await client.register_udf(None, udf_body, server_path, UDFLang.LUA)
            assert task is not None

            status = await task.wait_till_complete()
            assert status is True

            final_status = await task.query_status()
            assert final_status == TaskStatus.COMPLETE
        finally:
            try:
                remove_task = await client.remove_udf(None, server_path)
                await remove_task.wait_till_complete()
            except Exception:
                pass

    async def test_register_udf_from_file(self, client):
        """Test registering UDF from file."""
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "record_example.lua")
        server_path = "test_register_file.lua"

        try:
            task = await client.register_udf_from_file(None, udf_path, server_path, UDFLang.LUA)
            assert task is not None

            status = await task.wait_till_complete()
            assert status is True

            final_status = await task.query_status()
            assert final_status == TaskStatus.COMPLETE
        finally:
            try:
                remove_task = await client.remove_udf(None, server_path)
                await remove_task.wait_till_complete()
            except Exception:
                pass

    async def test_register_udf_with_policy(self, client):
        """Test registering UDF with AdminPolicy."""
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "record_example.lua")
        with open(udf_path, "rb") as f:
            udf_body = f.read()

        server_path = "test_register_policy.lua"
        policy = AdminPolicy()
        policy.timeout = 30000

        try:
            task = await client.register_udf(policy, udf_body, server_path, UDFLang.LUA)
            assert task is not None

            status = await task.wait_till_complete()
            assert status is True
        finally:
            try:
                remove_task = await client.remove_udf(None, server_path)
                await remove_task.wait_till_complete()
            except Exception:
                pass

    async def test_register_udf_nonexistent_file(self, client):
        """Test registering UDF from non-existent file."""
        udf_path = "nonexistent_file.lua"
        server_path = "test_nonexistent.lua"

        with pytest.raises(Exception):
            await client.register_udf_from_file(None, udf_path, server_path, UDFLang.LUA)

    async def test_register_udf_empty_file(self, client):
        """Test registering UDF from empty file raises error."""
        empty_file = os.path.join(os.path.dirname(__file__), "udf", "empty.lua")
        if not os.path.exists(empty_file):
            with open(empty_file, "w") as f:
                f.write("")

        server_path = "test_empty.lua"

        try:
            with pytest.raises(ServerError) as exc_info:
                await client.register_udf_from_file(None, empty_file, server_path, UDFLang.LUA)
            assert exc_info.value.result_code == ResultCode.SERVER_ERROR
        finally:
            if os.path.exists(empty_file):
                os.remove(empty_file)

    async def test_register_udf_duplicate(self, client):
        """Test registering the same UDF twice."""
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "record_example.lua")
        server_path = "test_duplicate.lua"

        try:
            task1 = await client.register_udf_from_file(None, udf_path, server_path, UDFLang.LUA)
            await task1.wait_till_complete()

            task2 = await client.register_udf_from_file(None, udf_path, server_path, UDFLang.LUA)
            await task2.wait_till_complete()

            status1 = await task1.query_status()
            status2 = await task2.query_status()
            assert status1 == TaskStatus.COMPLETE
            assert status2 == TaskStatus.COMPLETE
        finally:
            try:
                remove_task = await client.remove_udf(None, server_path)
                await remove_task.wait_till_complete()
            except Exception:
                pass
