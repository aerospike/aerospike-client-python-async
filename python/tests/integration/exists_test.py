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
from aerospike_async import ReadPolicy
from aerospike_async.exceptions import TimeoutError
from fixtures import TestFixtureInsertRecord


class TestExists(TestFixtureInsertRecord):
    """Test client.exists() method functionality."""

    async def test_existing_record(self, client, key):
        """Test checking existence of an existing record."""
        retval = await client.exists(ReadPolicy(), key)
        assert retval is True

    async def test_nonexistent_record(self, client, key_invalid_primary_key):
        """Test checking existence of a non-existent record."""
        retval = await client.exists(ReadPolicy(), key_invalid_primary_key)
        assert retval is False

    async def test_exists_with_policy(self, client, key):
        """Test exists operation with read policy."""
        rp = ReadPolicy()
        retval = await client.exists(rp, key)
        assert retval is True

    async def test_exists_fail(self, client, key_invalid_namespace):
        """Test exists operation with invalid namespace raises TimeoutError."""
        with pytest.raises(TimeoutError):
            await client.exists(ReadPolicy(), key_invalid_namespace)


class TestExistsLegacy(TestFixtureInsertRecord):
    """Test client.exists_legacy() method functionality - returns (key, meta) tuple like legacy client."""

    async def test_existing_record(self, client, key):
        """Test checking existence of an existing record returns (key, meta) tuple with metadata."""
        retval = await client.exists_legacy(ReadPolicy(), key)
        assert isinstance(retval, tuple)
        assert len(retval) == 2
        assert retval[0] == key
        # When record exists, meta should be a dict with generation and ttl
        assert retval[1] is not None
        assert isinstance(retval[1], dict)
        assert "gen" in retval[1]
        assert "ttl" in retval[1]

    async def test_nonexistent_record(self, client, key_invalid_primary_key):
        """Test checking existence of a non-existent record returns (key, None)."""
        retval = await client.exists_legacy(ReadPolicy(), key_invalid_primary_key)
        assert isinstance(retval, tuple)
        assert len(retval) == 2
        assert retval[0] == key_invalid_primary_key
        # Legacy contract: meta=None when record not found
        assert retval[1] is None

    async def test_exists_legacy_with_policy(self, client, key):
        """Test exists_legacy operation with read policy."""
        rp = ReadPolicy()
        retval = await client.exists_legacy(rp, key)
        assert isinstance(retval, tuple)
        assert len(retval) == 2
        assert retval[0] == key

    async def test_exists_legacy_fail(self, client, key_invalid_namespace):
        """Test exists_legacy operation with invalid namespace raises TimeoutError."""
        with pytest.raises(TimeoutError):
            await client.exists_legacy(ReadPolicy(), key_invalid_namespace)
