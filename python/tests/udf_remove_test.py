"""Tests for remove_udf functionality."""
import os
import pytest
from aerospike_async import AdminPolicy, UDFLang, TaskStatus
from aerospike_async.exceptions import ServerError
from fixtures import TestFixtureConnection


class TestRemoveUDF(TestFixtureConnection):
    """Test remove_udf functionality."""

    async def test_remove_udf_basic(self, client):
        """Test removing a UDF."""
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "record_example.lua")
        server_path = "test_remove_basic.lua"

        try:
            register_task = await client.register_udf_from_file(None, udf_path, server_path, UDFLang.LUA)
            await register_task.wait_till_complete()

            task = await client.remove_udf(None, server_path)
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

    async def test_remove_udf_with_policy(self, client):
        """Test removing UDF with AdminPolicy."""
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "record_example.lua")
        server_path = "test_remove_policy.lua"

        try:
            register_task = await client.register_udf_from_file(None, udf_path, server_path, UDFLang.LUA)
            await register_task.wait_till_complete()

            policy = AdminPolicy()
            policy.timeout = 30000

            task = await client.remove_udf(policy, server_path)
            assert task is not None

            status = await task.wait_till_complete()
            assert status is True
        finally:
            try:
                remove_task = await client.remove_udf(None, server_path)
                await remove_task.wait_till_complete()
            except Exception:
                pass

    async def test_remove_udf_nonexistent(self, client):
        """Test removing a non-existent UDF."""
        task = await client.remove_udf(None, "nonexistent_udf.lua")
        assert task is not None

        status = await task.wait_till_complete()
        assert status is True

        final_status = await task.query_status()
        assert final_status in (TaskStatus.COMPLETE, TaskStatus.NOT_FOUND)

    async def test_remove_udf_twice(self, client):
        """Test removing the same UDF twice."""
        udf_path = os.path.join(os.path.dirname(__file__), "udf", "record_example.lua")
        server_path = "test_remove_twice.lua"

        try:
            register_task = await client.register_udf_from_file(None, udf_path, server_path, UDFLang.LUA)
            await register_task.wait_till_complete()

            task1 = await client.remove_udf(None, server_path)
            await task1.wait_till_complete()

            task2 = await client.remove_udf(None, server_path)
            await task2.wait_till_complete()

            status1 = await task1.query_status()
            status2 = await task2.query_status()
            assert status1 == TaskStatus.COMPLETE
            assert status2 in (TaskStatus.COMPLETE, TaskStatus.NOT_FOUND)
        finally:
            try:
                remove_task = await client.remove_udf(None, server_path)
                await remove_task.wait_till_complete()
            except Exception:
                pass
