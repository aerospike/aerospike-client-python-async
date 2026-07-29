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

import uuid

import pytest
from aerospike_async import (
    ReadPolicy,
    Record,
    Key,
    WritePolicy,
    Statement,
    QueryPolicy,
    PartitionFilter,
    FilterExpression as fe,
)
from aerospike_async.exceptions import ServerError, ResultCode, FilteredOut, InvalidRequest
from fixtures import TestFixtureInsertRecord, TestFixtureConnection


class TestFilterExprUsage(TestFixtureInsertRecord):
    """Test FilterExpression usage in actual operations."""

    async def test_matching_filter_exp(self, client, key):
        """Test using a matching filter expression."""
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Ford"))
        rec = await client.get(key, ["brand", "year"], policy=rp)
        assert isinstance(rec, Record)
        assert rec.bins == {"brand": "Ford", "year": 1964}

    async def test_non_matching_filter_exp(self, client, key):
        """Test using a non-matching filter expression raises ServerError."""
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))

        with pytest.raises(FilteredOut) as exc_info:
            await client.get(key, ["brand", "year"], policy=rp)
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
        await client.put(key, {"listbin": test_list}, policy=wp)

        # Use filter expression to compare list bin to list value
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.list_bin("listbin"), fe.list_val(test_list))

        # Should match and return the record
        rec = await client.get(key, ["listbin"], policy=rp)
        assert isinstance(rec, Record)
        assert rec.bins["listbin"] == test_list

    async def test_list_val_non_matching(self, client, key):
        """Test list_val with non-matching list raises ServerError."""
        test_list = [1, 2, 3]
        different_list = [4, 5, 6]

        from aerospike_async import WritePolicy
        wp = WritePolicy()
        await client.put(key, {"listbin": test_list}, policy=wp)

        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.list_bin("listbin"), fe.list_val(different_list))

        with pytest.raises(FilteredOut) as exc_info:
            await client.get(key, ["listbin"], policy=rp)
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
        await client.operate(key, [MapOperation.put_items("mapbin", list(test_map.items()), map_policy)], policy=wp)

        # Retrieve the map as stored by the server to get exact serialization format
        # This ensures we use the same byte-level representation for comparison
        rp_no_filter = ReadPolicy()
        rec_stored = await client.get(key, ["mapbin"], policy=rp_no_filter)
        stored_map = rec_stored.bins["mapbin"]

        # Use filter expression to compare map bin to the exact stored map value
        # The filter expression requires exact byte-level matching
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.map_bin("mapbin"), fe.map_val(stored_map))

        # Should match and return the record (not filtered out)
        rec = await client.get(key, ["mapbin"], policy=rp)
        assert isinstance(rec, Record)
        # Verify the map contents match
        assert rec.bins["mapbin"] == stored_map

    async def test_map_val_non_matching(self, client, key):
        """Test map_val with non-matching map raises ServerError."""
        test_map = {"a": 1, "b": 2}
        different_map = {"c": 3, "d": 4}

        from aerospike_async import WritePolicy
        wp = WritePolicy()
        await client.put(key, {"mapbin": test_map}, policy=wp)

        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.map_bin("mapbin"), fe.map_val(different_map))

        with pytest.raises(FilteredOut) as exc_info:
            await client.get(key, ["mapbin"], policy=rp)
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT


class TestFilterExprBase64Query(TestFixtureConnection):
    """Use FilterExpression restored from base64 in a query and verify result count."""

    NAMESPACE = "test"
    BIN_NAME = "bin"

    @pytest.fixture
    async def client_and_data(self, client):
        """Create a set with records bin=0..19 for expression query tests. Unique set per run to avoid CI collisions."""
        set_name = f"base64_expr_{uuid.uuid4().hex[:8]}"
        wp = WritePolicy()
        for i in range(20):
            key = Key(self.NAMESPACE, set_name, i)
            await client.put(key, {self.BIN_NAME: i}, policy=wp)
        yield client, set_name

    async def test_query_with_restored_expression_single_match(self, client_and_data):
        """Round-trip expression to base64, use restored expr in query; expect count 1."""
        client, set_name = client_and_data
        expr = fe.eq(fe.int_bin(self.BIN_NAME), fe.int_val(1))
        b64 = expr.base64()
        restored = fe.from_base64(b64)

        qp = QueryPolicy()
        qp.filter_expression = restored
        stmt = Statement(self.NAMESPACE, set_name, None)
        records = await client.query(stmt, PartitionFilter.all(), policy=qp)
        count = 0
        async for _ in records:
            count += 1
        records.close()
        assert count == 1

    async def test_query_with_restored_expression_range(self, client_and_data):
        """Round-trip range expression (bin >= 10 and bin < 20), query; expect count 10."""
        client, set_name = client_and_data
        expr = fe.and_(
            exps=[
                fe.ge(fe.int_bin(self.BIN_NAME), fe.int_val(10)),
                fe.lt(fe.int_bin(self.BIN_NAME), fe.int_val(20)),
            ]
        )
        b64 = expr.base64()
        restored = fe.from_base64(b64)

        qp = QueryPolicy()
        qp.filter_expression = restored
        stmt = Statement(self.NAMESPACE, set_name, None)
        records = await client.query(stmt, PartitionFilter.all(), policy=qp)
        count = 0
        async for _ in records:
            count += 1
        records.close()
        assert count == 10


class TestFilterExprServerCompiledAelQuery(TestFixtureConnection):
    """Query with server-compiled AEL filters (server >= 8.1.3)."""

    NAMESPACE = "test"
    BIN_NAME = "bin"

    @pytest.fixture
    async def client_and_data(self, client, supports_server_compiled_ael):
        """Create a set with records bin=0..19 for server-compiled AEL query tests."""
        if not supports_server_compiled_ael:
            pytest.skip(
                "server-compiled AEL filters require server >= 8.1.3; point "
                "AEROSPIKE_HOST at an 8.1.3+ build to run these"
            )
        set_name = f"sc_ael_{uuid.uuid4().hex[:8]}"
        wp = WritePolicy()
        for i in range(20):
            key = Key(self.NAMESPACE, set_name, i)
            await client.put(key, {self.BIN_NAME: i}, policy=wp)
        yield client, set_name

    async def test_query_with_server_compiled_ael_single_match(self, client_and_data):
        """Server-compiled AEL filter on a query returns the matching record count."""
        client, set_name = client_and_data
        qp = QueryPolicy()
        qp.filter_expression = fe.from_server_compiled_ael(f"$.{self.BIN_NAME} == 1")

        stmt = Statement(self.NAMESPACE, set_name, None)
        records = await client.query(stmt, PartitionFilter.all(), policy=qp)
        count = 0
        async for _ in records:
            count += 1
        records.close()
        assert count == 1

    async def test_query_with_invalid_server_compiled_ael(self, client_and_data):
        """Invalid server-compiled AEL surfaces as PARAMETER_ERROR from the server."""
        client, set_name = client_and_data
        qp = QueryPolicy()
        qp.filter_expression = fe.from_server_compiled_ael("this is not valid AEL !!!")

        stmt = Statement(self.NAMESPACE, set_name, None)
        records = await client.query(stmt, PartitionFilter.all(), policy=qp)
        try:
            with pytest.raises(InvalidRequest) as exc_info:
                async for _ in records:
                    pass
        finally:
            records.close()
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
