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

"""Tests for create_index_using_expression and IndexTask functionality."""

import pytest
from aerospike_async import (
    IndexType,
    CollectionIndexType,
    FilterExpression,
    TaskStatus,
)
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureConnection


class TestCreateIndexUsingExpression(TestFixtureConnection):
    """Test create_index_using_expression and IndexTask functionality."""

    @pytest.fixture
    def index_name(self):
        return "test_expr_index"

    async def cleanup_index(self, client, index_name):
        """Helper to clean up an index, ignoring errors if it doesn't exist."""
        try:
            await client.drop_index("test", "test", index_name)
        except Exception:
            pass

    async def test_create_index_using_expression_numeric(self, client, index_name):
        """Test creating an expression-based numeric index."""
        await self.cleanup_index(client, index_name)

        # Create expression for integer bin
        expr = FilterExpression.int_bin("year")

        task = await client.create_index_using_expression(
            namespace="test",
            set_name="test",
            index_name=index_name,
            index_type=IndexType.NUMERIC,
            expression=expr,
        )

        assert task is not None

        # Wait for completion
        completed = await task.wait_till_complete()
        assert completed is True

        # Clean up
        await self.cleanup_index(client, index_name)

    async def test_create_index_using_expression_string(self, client, index_name):
        """Test creating an expression-based string index."""
        await self.cleanup_index(client, index_name)

        # Create expression for string bin
        expr = FilterExpression.string_bin("brand")

        task = await client.create_index_using_expression(
            namespace="test",
            set_name="test",
            index_name=index_name,
            index_type=IndexType.STRING,
            expression=expr,
        )

        assert task is not None

        # Wait for completion
        completed = await task.wait_till_complete()
        assert completed is True

        # Clean up
        await self.cleanup_index(client, index_name)

    async def test_index_task_query_status(self, client, index_name):
        """Test IndexTask.query_status method."""
        await self.cleanup_index(client, index_name)

        expr = FilterExpression.int_bin("year")

        task = await client.create_index_using_expression(
            namespace="test",
            set_name="test",
            index_name=index_name,
            index_type=IndexType.NUMERIC,
            expression=expr,
        )

        # Query status - should be IN_PROGRESS or COMPLETE
        status = await task.query_status()
        assert status in [TaskStatus.IN_PROGRESS, TaskStatus.COMPLETE]

        # Wait for completion then check status again
        await task.wait_till_complete()
        status = await task.query_status()
        assert status == TaskStatus.COMPLETE

        # Clean up
        await self.cleanup_index(client, index_name)

    async def test_index_task_wait_till_complete_custom_params(self, client, index_name):
        """Test IndexTask.wait_till_complete with custom parameters."""
        await self.cleanup_index(client, index_name)

        expr = FilterExpression.int_bin("year")

        task = await client.create_index_using_expression(
            namespace="test",
            set_name="test",
            index_name=index_name,
            index_type=IndexType.NUMERIC,
            expression=expr,
        )

        # Wait with custom sleep time and max attempts
        completed = await task.wait_till_complete(sleep_time=0.1, max_attempts=100)
        assert completed is True

        # Clean up
        await self.cleanup_index(client, index_name)

    async def test_create_index_using_expression_with_cit(self, client, index_name):
        """Test creating an expression-based index with collection index type."""
        await self.cleanup_index(client, index_name)

        expr = FilterExpression.int_bin("year")

        task = await client.create_index_using_expression(
            namespace="test",
            set_name="test",
            index_name=index_name,
            index_type=IndexType.NUMERIC,
            expression=expr,
            cit=CollectionIndexType.DEFAULT,
        )

        completed = await task.wait_till_complete()
        assert completed is True

        # Clean up
        await self.cleanup_index(client, index_name)

    async def test_create_duplicate_index_name_fails(self, client, index_name):
        """Test that creating an index with duplicate name fails."""
        await self.cleanup_index(client, index_name)

        expr = FilterExpression.int_bin("year")

        # Create first index
        task = await client.create_index_using_expression(
            namespace="test",
            set_name="test",
            index_name=index_name,
            index_type=IndexType.NUMERIC,
            expression=expr,
        )
        await task.wait_till_complete()

        # Try to create another index with same name - should fail
        with pytest.raises(ServerError) as exc_info:
            await client.create_index_using_expression(
                namespace="test",
                set_name="test",
                index_name=index_name,
                index_type=IndexType.STRING,
                expression=FilterExpression.string_bin("brand"),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_FOUND

        # Clean up
        await self.cleanup_index(client, index_name)
