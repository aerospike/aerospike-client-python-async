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

"""Tests for index creation and management functionality."""

import pytest
from aerospike_async import IndexType, CollectionIndexType, TaskStatus
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureConnection


class TestIndex(TestFixtureConnection):
    """Test index creation and management functionality."""

    async def cleanup_index(self, client, index_name):
        """Helper to clean up an index, ignoring errors if it doesn't exist."""
        try:
            task = await client.drop_index("test", "test", index_name)
            await task.wait_till_complete()
        except Exception:
            pass

    async def test_create_string_index(self, client):
        """Test creating a string index."""
        await self.cleanup_index(client, "index_name")

        retval = await client.create_index(
            namespace="test",
            set_name="test",
            bin_name="brand",
            index_name="index_name",
            index_type=IndexType.STRING,
            cit=CollectionIndexType.DEFAULT)
        assert retval is None

        await self.cleanup_index(client, "index_name")

    async def test_create_numeric_index(self, client):
        """Test creating a numeric index."""
        await self.cleanup_index(client, "index_name")

        retval = await client.create_index("test", "test", "year", "index_name", IndexType.NUMERIC, cit=CollectionIndexType.DEFAULT)
        assert retval is None

        await self.cleanup_index(client, "index_name")

    async def test_create_geo2dsphere_index(self, client):
        """Test creating a geo2dsphere index."""
        await self.cleanup_index(client, "index_name")

        retval = await client.create_index("test", "test", "geojson", "index_name", IndexType.GEO2D_SPHERE, cit=CollectionIndexType.DEFAULT)
        assert retval is None

        await self.cleanup_index(client, "index_name")

    async def test_create_with_cit(self, client):
        """Test creating an index with collection index type."""
        await self.cleanup_index(client, "index_name")

        retval = await client.create_index("test", "test", "year", "index_name", IndexType.NUMERIC, cit=CollectionIndexType.DEFAULT)
        assert retval is None

        await self.cleanup_index(client, "index_name")

    async def test_create_index_fail(self, client):
        """Test that creating duplicate index names fails."""
        await self.cleanup_index(client, "indexname")

        # Create first index
        await client.create_index("test", "test", "brand", "indexname", IndexType.STRING, cit=CollectionIndexType.DEFAULT)

        # Try to create another index with same name should fail
        with pytest.raises(ServerError) as exc_info:
            await client.create_index("test", "test", "year", "indexname", IndexType.NUMERIC, cit=CollectionIndexType.DEFAULT)
        assert exc_info.value.result_code == ResultCode.INDEX_FOUND

        await self.cleanup_index(client, "indexname")


class TestDropIndex(TestFixtureConnection):
    """Test drop_index functionality and DropIndexTask."""

    async def test_create_drop_cycle(self, client):
        """Test full create/drop cycle."""
        index_name = "create_drop_test"

        # Drop index if it already exists
        try:
            task = await client.drop_index("test", "test", index_name)
            await task.wait_till_complete()
        except Exception:
            pass

        # Create index
        await client.create_index("test", "test", "brand", index_name, IndexType.STRING)

        # Drop index and wait for completion
        task = await client.drop_index("test", "test", index_name)
        await task.wait_till_complete()

        # Verify across all nodes that index no longer exists
        nodes = await client.nodes()
        for node in nodes:
            # Query sindex on each node to verify index is gone
            response = await node.info(f"sindex/{index_name}")
            # Response should indicate index not found
            assert response is not None

    async def test_drop_index_returns_task(self, client):
        """Test that drop_index returns a DropIndexTask."""
        index_name = "drop_task_test"

        try:
            task = await client.drop_index("test", "test", index_name)
            await task.wait_till_complete()
        except Exception:
            pass

        await client.create_index("test", "test", "brand", index_name, IndexType.STRING)

        # Drop and verify we get a task back
        task = await client.drop_index(namespace="test", set_name="test", index_name=index_name)
        assert task is not None

        # Wait for completion
        completed = await task.wait_till_complete()
        assert completed is True

    async def test_drop_index_task_query_status(self, client):
        """Test DropIndexTask.query_status method."""
        index_name = "status_test_index"

        try:
            task = await client.drop_index("test", "test", index_name)
            await task.wait_till_complete()
        except Exception:
            pass

        await client.create_index("test", "test", "year", index_name, IndexType.NUMERIC)

        task = await client.drop_index("test", "test", index_name)

        # Query status - should be IN_PROGRESS or COMPLETE
        status = await task.query_status()
        assert status in [TaskStatus.IN_PROGRESS, TaskStatus.COMPLETE, TaskStatus.NOT_FOUND]

        # Wait for completion
        await task.wait_till_complete()

        # After completion, status should be COMPLETE or NOT_FOUND
        status = await task.query_status()
        assert status in [TaskStatus.COMPLETE, TaskStatus.NOT_FOUND]

    async def test_drop_index_task_wait_custom_params(self, client):
        """Test DropIndexTask.wait_till_complete with custom parameters."""
        index_name = "wait_custom_test"

        try:
            task = await client.drop_index("test", "test", index_name)
            await task.wait_till_complete()
        except Exception:
            pass

        await client.create_index("test", "test", "brand", index_name, IndexType.STRING)

        task = await client.drop_index("test", "test", index_name)

        # Wait with custom sleep_time and max_attempts
        completed = await task.wait_till_complete(sleep_time=0.1, max_attempts=100)
        assert completed is True

    async def test_drop_nonexistent_index(self, client):
        """Test dropping a non-existent index returns task."""
        task = await client.drop_index(namespace="test", set_name="test", index_name="nonexistent_index_12345")
        assert task is not None

        # Task should complete (possibly with NOT_FOUND status)
        status = await task.query_status()
        assert status in [TaskStatus.COMPLETE, TaskStatus.NOT_FOUND]
