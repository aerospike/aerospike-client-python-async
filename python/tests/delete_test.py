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

import pytest
from aerospike_async import WritePolicy, Expiration
from aerospike_async.exceptions import TimeoutError
from fixtures import TestFixtureInsertRecord


class TestDelete(TestFixtureInsertRecord):
    """Test client.delete() method functionality."""

    async def test_delete_existing_record(self, client, key):
        """Test deleting an existing record."""
        rec_existed = await client.delete(WritePolicy(), key)
        assert rec_existed is True

    async def test_delete_nonexistent_record(self, client, key_invalid_primary_key):
        """Test deleting a non-existent record."""
        rec_existed = await client.delete(WritePolicy(), key_invalid_primary_key)
        assert rec_existed is False

    async def test_delete_with_policy(self, client, key):
        """Test delete operation with write policy."""
        wp = WritePolicy()
        rec_existed = await client.delete(wp, key)
        assert rec_existed is True

    async def test_delete_with_nonexistent_namespace(self, client, key_invalid_namespace):
        """Test delete operation with invalid namespace raises TimeoutError."""
        wp = WritePolicy()
        wp.expiration = Expiration.NEVER_EXPIRE
        with pytest.raises(TimeoutError) as exi:
            await client.delete(wp, key_invalid_namespace)
        assert "Timeout" in str(exi.value)