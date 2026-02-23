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
from aerospike_async import ReadPolicy, Record
from aerospike_async.exceptions import ServerError, ResultCode
from aerospike_async import FilterExpression as fe
from fixtures import TestFixtureInsertRecord


class TestFilterExprUsage(TestFixtureInsertRecord):
    """Test FilterExpression usage in actual operations."""

    async def test_matching_filter_exp(self, client, key):
        """Test using a matching filter expression."""
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Ford"))
        rec = await client.get(rp, key, ["brand", "year"])
        assert isinstance(rec, Record)
        assert rec.bins == {"brand": "Ford", "year": 1964}

    async def test_non_matching_filter_exp(self, client, key):
        """Test using a non-matching filter expression raises ServerError."""
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))

        with pytest.raises(ServerError) as exc_info:
            await client.get(rp, key, ["brand", "year"])
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT


class TestFilterExprListVal(TestFixtureInsertRecord):
    """Test list_val filter expression usage."""

    async def test_list_val_equality(self, client, key):
        """Test comparing a list bin to a list value in filter expression."""
        # Create a test list
        test_list = [1, -1, 3, 5]

        # Put the list in a bin
        from aerospike_async import WritePolicy
        wp = WritePolicy()
        await client.put(wp, key, {"listbin": test_list})

        # Use filter expression to compare list bin to list value
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.list_bin("listbin"), fe.list_val(test_list))

        # Should match and return the record
        rec = await client.get(rp, key, ["listbin"])
        assert isinstance(rec, Record)
        assert rec.bins["listbin"] == test_list

    async def test_list_val_non_matching(self, client, key):
        """Test list_val with non-matching list raises ServerError."""
        test_list = [1, 2, 3]
        different_list = [4, 5, 6]

        from aerospike_async import WritePolicy
        wp = WritePolicy()
        await client.put(wp, key, {"listbin": test_list})

        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.list_bin("listbin"), fe.list_val(different_list))

        with pytest.raises(ServerError) as exc_info:
            await client.get(rp, key, ["listbin"])
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT


class TestFilterExprMapVal(TestFixtureInsertRecord):
    """Test map_val filter expression usage."""

    async def test_map_val_equality(self, client, key):
        """Test comparing a map bin to a map value in filter expression.

        Uses MapPolicy(MapOrder.KEY_ORDERED) to store the map as ordered, which
        ensures deterministic key ordering for exact byte-level matching in filter expressions.
        """
        # Create a test map
        test_map = {
            "key1": "e",
            "key2": "d",
            "key3": "c",
            "key4": "b",
            "key5": "a",
        }

        # Put the map in a bin with KEY_ORDERED policy to ensure deterministic ordering
        from aerospike_async import WritePolicy, MapPolicy, MapOrder, MapOperation
        wp = WritePolicy()
        map_policy = MapPolicy(MapOrder.KEY_ORDERED, None)
        # Use put_items to store the entire map with KEY_ORDERED policy
        await client.operate(wp, key, [MapOperation.put_items("mapbin", list(test_map.items()), map_policy)])

        # Retrieve the map as stored by the server to get exact serialization format
        # This ensures we use the same byte-level representation for comparison
        rp_no_filter = ReadPolicy()
        rec_stored = await client.get(rp_no_filter, key, ["mapbin"])
        stored_map = rec_stored.bins["mapbin"]

        # Use filter expression to compare map bin to the exact stored map value
        # The filter expression requires exact byte-level matching
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.map_bin("mapbin"), fe.map_val(stored_map))

        # Should match and return the record (not filtered out)
        rec = await client.get(rp, key, ["mapbin"])
        assert isinstance(rec, Record)
        # Verify the map contents match
        assert rec.bins["mapbin"] == stored_map

    async def test_map_val_non_matching(self, client, key):
        """Test map_val with non-matching map raises ServerError."""
        test_map = {"a": 1, "b": 2}
        different_map = {"c": 3, "d": 4}

        from aerospike_async import WritePolicy
        wp = WritePolicy()
        await client.put(wp, key, {"mapbin": test_map})

        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.map_bin("mapbin"), fe.map_val(different_map))

        with pytest.raises(ServerError) as exc_info:
            await client.get(rp, key, ["mapbin"])
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT
