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


class TestAppend(TestFixtureInsertRecord):
    """Test client.append() method functionality."""

    async def test_append(self, client, key):
        """Test basic append operation."""
        retval = await client.append(WritePolicy(), key, {"brand": "d"})
        assert retval is None

        rec = await client.get(ReadPolicy(), key)
        assert rec.bins["brand"] == "Fordd"

    async def test_append_with_policy(self, client, key):
        """Test append operation with write policy."""
        wp = WritePolicy()
        retval = await client.append(wp, key, {"brand": "d"})
        assert retval is None

    async def test_append_unsupported_bin_type(self, client, key):
        """Test append operation with unsupported bin type raises ServerError."""
        with pytest.raises(ServerError) as exc_info:
            await client.append(WritePolicy(), key, {"year": "d"})
        assert exc_info.value.result_code == ResultCode.BIN_TYPE_ERROR
