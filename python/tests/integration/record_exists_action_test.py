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

"""Tests for RecordExistsAction policies.

Tests different write behaviors based on record existence.
"""

import pytest
from aerospike_async import Key, WritePolicy, ReadPolicy, RecordExistsAction
from aerospike_async.exceptions import ServerError, ResultCode, RecordNotFound, RecordExistsError
from fixtures import TestFixtureConnection


class TestReplace(TestFixtureConnection):
    """Test REPLACE action - replaces entire record, removing other bins."""

    async def test_replace_removes_other_bins(self, client):
        """Test that REPLACE removes bins not included in the new write."""
        key = Key("test", "test", "replace_1")

        # Clean up
        try:
            await client.delete(key, policy=WritePolicy())
        except Exception:
            pass

        # Create record with two bins
        await client.put(key, {"bin1": "value1", "bin2": "value2"}, policy=WritePolicy())

        # Verify both bins exist
        record = await client.get(key, policy=ReadPolicy())
        assert record.bins["bin1"] == "value1"
        assert record.bins["bin2"] == "value2"

        # Replace with only bin3
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.REPLACE
        await client.put(key, {"bin3": "value3"}, policy=wp)

        # Verify bin1 and bin2 are gone, only bin3 exists
        record = await client.get(key, policy=ReadPolicy())
        assert "bin1" not in record.bins
        assert "bin2" not in record.bins
        assert record.bins["bin3"] == "value3"

        # Cleanup
        await client.delete(key, policy=WritePolicy())

    async def test_replace_on_nonexistent_creates_record(self, client):
        """Test that REPLACE on non-existent record creates it."""
        key = Key("test", "test", "replace_2")

        # Ensure record doesn't exist
        try:
            await client.delete(key, policy=WritePolicy())
        except Exception:
            pass

        # REPLACE on non-existent should create the record
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.REPLACE
        await client.put(key, {"bin": "value"}, policy=wp)

        # Verify record was created
        record = await client.get(key, policy=ReadPolicy())
        assert record.bins["bin"] == "value"

        # Cleanup
        await client.delete(key, policy=WritePolicy())


class TestReplaceOnly(TestFixtureConnection):
    """Test REPLACE_ONLY action - replace only if record exists."""

    async def test_replace_only_succeeds_when_exists(self, client):
        """Test that REPLACE_ONLY succeeds when record exists."""
        key = Key("test", "test", "replace_only_1")

        # Clean up and create record
        try:
            await client.delete(key, policy=WritePolicy())
        except Exception:
            pass

        await client.put(key, {"bin1": "value1", "bin2": "value2"}, policy=WritePolicy())

        # REPLACE_ONLY should succeed and replace all bins
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.REPLACE_ONLY
        await client.put(key, {"bin3": "value3"}, policy=wp)

        # Verify replacement
        record = await client.get(key, policy=ReadPolicy())
        assert "bin1" not in record.bins
        assert "bin2" not in record.bins
        assert record.bins["bin3"] == "value3"

        # Cleanup
        await client.delete(key, policy=WritePolicy())

    async def test_replace_only_fails_when_not_exists(self, client):
        """Test that REPLACE_ONLY fails when record doesn't exist."""
        key = Key("test", "test", "replace_only_2")

        # Ensure record doesn't exist
        try:
            await client.delete(key, policy=WritePolicy())
        except Exception:
            pass

        # REPLACE_ONLY on non-existent should fail
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.REPLACE_ONLY

        with pytest.raises(RecordNotFound) as exc_info:
            await client.put(key, {"bin": "value"}, policy=wp)

        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR


class TestUpdate(TestFixtureConnection):
    """Test UPDATE action (default) - merges bins."""

    async def test_update_merges_bins(self, client):
        """Test that UPDATE (default) merges bins, keeping existing ones."""
        key = Key("test", "test", "update_1")

        # Clean up
        try:
            await client.delete(key, policy=WritePolicy())
        except Exception:
            pass

        # Create record with two bins
        await client.put(key, {"bin1": "value1", "bin2": "value2"}, policy=WritePolicy())

        # Update with bin3 (default UPDATE action)
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.UPDATE
        await client.put(key, {"bin3": "value3"}, policy=wp)

        # Verify all bins exist
        record = await client.get(key, policy=ReadPolicy())
        assert record.bins["bin1"] == "value1"
        assert record.bins["bin2"] == "value2"
        assert record.bins["bin3"] == "value3"

        # Cleanup
        await client.delete(key, policy=WritePolicy())


class TestUpdateOnly(TestFixtureConnection):
    """Test UPDATE_ONLY action - update only if record exists."""

    async def test_update_only_succeeds_when_exists(self, client):
        """Test that UPDATE_ONLY succeeds when record exists."""
        key = Key("test", "test", "update_only_1")

        # Clean up and create record
        try:
            await client.delete(key, policy=WritePolicy())
        except Exception:
            pass

        await client.put(key, {"bin1": "value1"}, policy=WritePolicy())

        # UPDATE_ONLY should succeed
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.UPDATE_ONLY
        await client.put(key, {"bin2": "value2"}, policy=wp)

        # Verify both bins exist (merged)
        record = await client.get(key, policy=ReadPolicy())
        assert record.bins["bin1"] == "value1"
        assert record.bins["bin2"] == "value2"

        # Cleanup
        await client.delete(key, policy=WritePolicy())

    async def test_update_only_fails_when_not_exists(self, client):
        """Test that UPDATE_ONLY fails when record doesn't exist."""
        key = Key("test", "test", "update_only_2")

        # Ensure record doesn't exist
        try:
            await client.delete(key, policy=WritePolicy())
        except Exception:
            pass

        # UPDATE_ONLY on non-existent should fail
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.UPDATE_ONLY

        with pytest.raises(RecordNotFound) as exc_info:
            await client.put(key, {"bin": "value"}, policy=wp)

        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR


class TestCreateOnly(TestFixtureConnection):
    """Test CREATE_ONLY action - create only if record doesn't exist."""

    async def test_create_only_succeeds_when_not_exists(self, client):
        """Test that CREATE_ONLY succeeds when record doesn't exist."""
        key = Key("test", "test", "create_only_1")

        # Ensure record doesn't exist
        try:
            await client.delete(key, policy=WritePolicy())
        except Exception:
            pass

        # CREATE_ONLY should succeed
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.CREATE_ONLY
        await client.put(key, {"bin": "value"}, policy=wp)

        # Verify record was created
        record = await client.get(key, policy=ReadPolicy())
        assert record.bins["bin"] == "value"

        # Cleanup
        await client.delete(key, policy=WritePolicy())

    async def test_create_only_fails_when_exists(self, client):
        """Test that CREATE_ONLY fails when record already exists."""
        key = Key("test", "test", "create_only_2")

        # Clean up and create record
        try:
            await client.delete(key, policy=WritePolicy())
        except Exception:
            pass

        await client.put(key, {"bin1": "value1"}, policy=WritePolicy())

        # CREATE_ONLY on existing should fail
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.CREATE_ONLY

        with pytest.raises(RecordExistsError) as exc_info:
            await client.put(key, {"bin2": "value2"}, policy=wp)

        assert exc_info.value.result_code == ResultCode.KEY_EXISTS_ERROR

        # Verify original record is unchanged
        record = await client.get(key, policy=ReadPolicy())
        assert record.bins["bin1"] == "value1"
        assert "bin2" not in record.bins

        # Cleanup
        await client.delete(key, policy=WritePolicy())
