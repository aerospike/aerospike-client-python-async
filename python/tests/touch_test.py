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
from aerospike_async import WritePolicy, ReadPolicy
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureInsertRecord


class TestTouch(TestFixtureInsertRecord):
    """Test client.touch() method functionality."""

    async def test_existing_record(self, client, key):
        """Test touching an existing record."""
        retval = await client.touch(WritePolicy(), key)
        assert retval is None

        rec = await client.get(ReadPolicy(), key)
        assert rec.generation == 2

    async def test_touch_with_policy(self, client, key):
        """Test touch operation with write policy."""
        wp = WritePolicy()
        retval = await client.touch(wp, key)
        assert retval is None

    async def test_nonexistent_record(self, client, key_invalid_primary_key):
        """Test touching a non-existent record raises ServerError."""
        with pytest.raises(ServerError) as exc_info:
            await client.touch(WritePolicy(), key_invalid_primary_key)
        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR
