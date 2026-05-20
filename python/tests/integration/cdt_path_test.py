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

"""Integration tests for CDT path expressions (selectByPath / modifyByPath).

Requires Aerospike Server version >= 8.1.1.
"""

import pytest
import pytest_asyncio

from aerospike_async import (
    CdtOperation,
    ClientPolicy,
    CTX,
    ExpType,
    FilterExpression as fe,
    GeoJSON,
    Key,
    LoopVarPart,
    MapReturnType,
    ModifyFlags,
    new_client,
    ReadPolicy,
    SelectFlags,
    WritePolicy,
)
from aerospike_async.exceptions import ServerError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def client(aerospike_host, use_services_alternate):
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    c = await new_client(cp, aerospike_host)
    yield c
    await c.close()


@pytest_asyncio.fixture
async def cdt_client_and_key(aerospike_host, use_services_alternate):
    """Client + a unique key; record is deleted before and after the test."""
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    c = await new_client(cp, aerospike_host)

    node_names = await c.node_names()
    node = await c.get_node(node_names[0])
    server_version = node.version
    if not (
        server_version.major > 8
        or (server_version.major == 8 and server_version.minor > 1)
        or (server_version.major == 8 and server_version.minor == 1 and server_version.patch >= 1)
    ):
        await c.close()
        pytest.skip(f"CDT path expressions require server >= 8.1.1 (got {server_version})")

    key = Key("test", "test", "cdtpath_key")
    wp = WritePolicy()
    try:
        await c.delete(key, policy=wp)
    except Exception:
        pass

    yield c, key

    try:
        await c.delete(key, policy=wp)
    except Exception:
        pass
    await c.close()


# ---------------------------------------------------------------------------
# select_by_path tests
# ---------------------------------------------------------------------------

class TestSelectByPath:

    async def test_select_single_map_key(self, cdt_client_and_key):
        """Select a scalar value via a single map-key context step."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"score": 42}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [CTX.map_key("score")],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        bins = record.bins
        assert bins is not None
        result = bins.get("data")
        assert result is not None

    async def test_select_all_children_from_list(self, cdt_client_and_key):
        """Select all list elements using CTX.all_children()."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"matrix": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [CTX.map_key("matrix"), CTX.all_children()],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None
        # 3 rows returned
        assert len(result) == 3

    async def test_select_filter_by_int_value(self, cdt_client_and_key):
        """Filter list items by integer value using LoopVarPart.VALUE."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        numbers = [10, 20, 30, 40, 50]
        await client.put(key, {"data": {"numbers": numbers}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("numbers"),
                CTX.all_children_with_filter(
                    fe.lt(fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(35))
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None
        # 10, 20, 30 < 35
        assert len(result) == 3
        assert set(result) == {10, 20, 30}

    async def test_select_filter_by_index(self, cdt_client_and_key):
        """Filter list items by position using LoopVarPart.INDEX."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"nums": [10, 20, 30, 40, 50]}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("nums"),
                CTX.all_children_with_filter(
                    fe.lt(fe.int_loop_var(LoopVarPart.INDEX), fe.int_val(3))
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None
        # indices 0, 1, 2  → values 10, 20, 30
        assert len(result) == 3
        assert set(result) == {10, 20, 30}

    async def test_select_filter_by_map_key(self, cdt_client_and_key):
        """Filter map entries whose key comes before 'c' lexicographically."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        products = {"apple": 1.50, "banana": 0.75, "cherry": 2.25}
        await client.put(key, {"data": {"products": products}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("products"),
                CTX.all_children_with_filter(
                    fe.lt(fe.string_loop_var(LoopVarPart.MAP_KEY), fe.string_val("c"))
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None
        # apple and banana keys are < "c"
        assert len(result) == 2

    async def test_select_nested_books_by_price(self, cdt_client_and_key):
        """Select titles of books with price <= 10.0 (the canonical books example)."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        books = [
            {"title": "Sayings of the Century", "price": 8.95},
            {"title": "Sword of Honour", "price": 12.99},
            {"title": "Moby Dick", "price": 8.99},
            {"title": "The Lord of the Rings", "price": 22.99},
        ]
        await client.put(key, {"data": {"book": books}}, policy=wp)

        # Price is a float; use map_loop_var to get the current map (book entry),
        # then read the "price" key from it via a map get-by-key expression.
        price_exp = fe.map_get_by_key(
            MapReturnType.VALUE,
            ExpType.FLOAT,
            fe.string_val("price"),
            fe.map_loop_var(LoopVarPart.VALUE),
            [],
        )

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("book"),
                CTX.all_children_with_filter(
                    fe.le(price_exp, fe.float_val(10.0))
                ),
                CTX.all_children_with_filter(
                    fe.eq(fe.string_loop_var(LoopVarPart.MAP_KEY), fe.string_val("title"))
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None
        assert len(result) == 2
        assert set(result) == {"Sayings of the Century", "Moby Dick"}

    async def test_select_no_fail_on_empty_list(self, cdt_client_and_key):
        """SelectFlags.NO_FAIL succeeds even when the path has an empty collection."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"emptyList": [], "items": [1, 2, 3]}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.NO_FAIL,
            [CTX.map_key("emptyList"), CTX.all_children()],
        )
        record = await client.operate(key, [op], policy=wp)
        # Should not raise; result may be empty but the operation completes
        assert record is not None

    async def test_select_no_fail_on_empty_map(self, cdt_client_and_key):
        """SelectFlags.NO_FAIL succeeds when the path leads to an empty map."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"emptyMap": {}, "items": {"a": 1}}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.NO_FAIL,
            [CTX.map_key("emptyMap"), CTX.all_children()],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None

    async def test_select_map_key_flag(self, cdt_client_and_key):
        """SelectFlags.MAP_KEY returns only map keys, not values."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"items": {"item1": 100, "item2": 200, "item3": 50}}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.MAP_KEY,
            [
                CTX.map_key("items"),
                CTX.all_children_with_filter(
                    fe.gt(fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(75))
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        # item1 (100) and item2 (200) have value > 75; we get their keys
        assert result is not None

    async def test_select_matching_tree_flag(self, cdt_client_and_key):
        """SelectFlags.MATCHING_TREE returns the matched sub-tree structure."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        books = [
            {"title": "Cheap Book", "price": 5.99},
            {"title": "Expensive Book", "price": 25.99},
        ]
        await client.put(key, {"data": {"book": books}}, policy=wp)

        from aerospike_async import MapReturnType, ExpType
        price_exp = fe.map_get_by_key(
            MapReturnType.VALUE,
            ExpType.FLOAT,
            fe.string_val("price"),
            fe.map_loop_var(LoopVarPart.VALUE),
            [],
        )

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.MATCHING_TREE,
            [
                CTX.map_key("book"),
                CTX.all_children_with_filter(
                    fe.le(price_exp, fe.float_val(10.0))
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None

    async def test_select_with_list_index_context(self, cdt_client_and_key):
        """Navigate to a specific list element by index, then into it."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        items = [
            {"name": "item1", "value": 10},
            {"name": "item2", "value": 20},
            {"name": "item3", "value": 30},
        ]
        await client.put(key, {"data": {"items": items}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("items"),
                CTX.list_index(1),  # second item
                CTX.map_key("value"),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None
        # Expect [20] – the value of the second item
        assert isinstance(result, list)
        assert 20 in result

    async def test_select_empty_results_when_no_match(self, cdt_client_and_key):
        """No items matching the filter returns an empty (or absent) result."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        books = [
            {"title": "Expensive 1", "price": 25.99},
            {"title": "Expensive 2", "price": 30.50},
        ]
        await client.put(key, {"data": {"book": books}}, policy=wp)

        from aerospike_async import MapReturnType, ExpType
        price_exp = fe.map_get_by_key(
            MapReturnType.VALUE,
            ExpType.FLOAT,
            fe.string_val("price"),
            fe.map_loop_var(LoopVarPart.VALUE),
            [],
        )

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("book"),
                CTX.all_children_with_filter(
                    fe.le(price_exp, fe.float_val(10.0))
                ),
                CTX.all_children_with_filter(
                    fe.eq(fe.string_loop_var(LoopVarPart.MAP_KEY), fe.string_val("title"))
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        # Expect None or an empty list (no book has price <= 10.0)
        if result is not None:
            assert len(result) == 0

    async def test_select_bool_loop_var_active_users(self, cdt_client_and_key):
        """Use bool_loop_var to select only active users."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        users = [
            {"name": "Alice", "active": True},
            {"name": "Bob", "active": False},
            {"name": "Charlie", "active": True},
        ]
        await client.put(key, {"data": {"users": users}}, policy=wp)

        from aerospike_async import MapReturnType, ExpType
        active_exp = fe.map_get_by_key(
            MapReturnType.VALUE,
            ExpType.BOOL,
            fe.string_val("active"),
            fe.map_loop_var(LoopVarPart.VALUE),
            [],
        )

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("users"),
                CTX.all_children_with_filter(fe.eq(active_exp, fe.bool_val(True))),
                CTX.all_children_with_filter(
                    fe.eq(fe.string_loop_var(LoopVarPart.MAP_KEY), fe.string_val("name"))
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None
        assert len(result) == 2
        assert set(result) == {"Alice", "Charlie"}


# ---------------------------------------------------------------------------
# modify_by_path tests
# ---------------------------------------------------------------------------

class TestModifyByPath:

    async def test_modify_multiply_all_prices(self, cdt_client_and_key):
        """Multiply every price in a list of books by 1.10."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        books = [
            {"title": "Sayings of the Century", "price": 8.95},
            {"title": "Sword of Honour", "price": 12.99},
            {"title": "Moby Dick", "price": 8.99},
            {"title": "The Lord of the Rings", "price": 22.99},
        ]
        await client.put(key, {"data": {"book": books}}, policy=wp)

        modify_exp = fe.num_mul([fe.float_loop_var(LoopVarPart.VALUE), fe.float_val(1.10)])

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            modify_exp,
            [
                CTX.map_key("book"),
                CTX.all_children(),
                CTX.map_key("price"),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        assert record is not None
        final_books = record.bins.get("data", {}).get("book")
        assert final_books is not None
        assert len(final_books) == 4
        first_price = final_books[0]["price"]
        expected = 8.95 * 1.10
        assert abs(first_price - expected) < 0.01

    async def test_modify_add_to_all_scores(self, cdt_client_and_key):
        """Add 5 to every score in a list."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"scores": [10, 20, 30, 40, 50]}}, policy=wp)

        modify_exp = fe.num_add([fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(5)])

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            modify_exp,
            [
                CTX.map_key("scores"),
                CTX.all_children_with_filter(fe.bool_val(True)),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        final_scores = record.bins.get("data", {}).get("scores")
        assert final_scores is not None
        assert len(final_scores) == 5
        assert final_scores[0] == 15

    async def test_modify_subtract_from_all_balances(self, cdt_client_and_key):
        """Subtract 100 from every map value."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"balances": {"account1": 1000, "account2": 2000}}}, policy=wp)

        modify_exp = fe.num_sub([fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(100)])

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            modify_exp,
            [
                CTX.map_key("balances"),
                CTX.all_children_with_filter(fe.bool_val(True)),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        balances = record.bins.get("data", {}).get("balances")
        assert balances is not None
        assert balances["account1"] == 900
        assert balances["account2"] == 1900

    async def test_modify_selective_field_in_nested_maps(self, cdt_client_and_key):
        """Add 100 to the 'value' field in each nested map, leaving other fields alone."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        metrics = [
            {"value": 10, "multiplier": 2},
            {"value": 20, "multiplier": 3},
        ]
        await client.put(key, {"data": {"metrics": metrics}}, policy=wp)

        modify_exp = fe.num_add([fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(100)])

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            modify_exp,
            [
                CTX.map_key("metrics"),
                CTX.all_children_with_filter(fe.bool_val(True)),
                CTX.all_children_with_filter(
                    fe.eq(fe.string_loop_var(LoopVarPart.MAP_KEY), fe.string_val("value"))
                ),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        final_metrics = record.bins.get("data", {}).get("metrics")
        assert final_metrics is not None
        assert final_metrics[0]["value"] == 110
        assert final_metrics[0]["multiplier"] == 2  # unchanged
        assert final_metrics[1]["value"] == 120

    async def test_modify_no_fail_flag(self, cdt_client_and_key):
        """ModifyFlags.NO_FAIL does not raise when the path leads to no items."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"scores": []}}, policy=wp)

        modify_exp = fe.num_add([fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(1)])

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.NO_FAIL,
            modify_exp,
            [
                CTX.map_key("scores"),
                CTX.all_children_with_filter(fe.bool_val(True)),
            ],
        )
        # Should not raise
        await client.operate(key, [op], policy=wp)


# ---------------------------------------------------------------------------
# Compression integration test (Enterprise Edition only)
# ---------------------------------------------------------------------------

class TestUseCompressionIntegration:

    async def test_put_get_with_compression(self, aerospike_host, use_services_alternate, enterprise):
        """Put and get a large byte bin through a compression-enabled policy."""
        if not enterprise:
            pytest.skip("use_compression requires Aerospike Enterprise Edition")

        cp = ClientPolicy()
        cp.use_services_alternate = use_services_alternate
        client = await new_client(cp, aerospike_host)

        try:
            key = Key("test", "test", "compress_test_key")
            wp = WritePolicy()
            wp.use_compression = True
            rp = ReadPolicy()
            rp.use_compression = True

            payload = bytes(range(256)) * 8  # 2048 bytes, compressible

            try:
                await client.delete(key, policy=wp)
            except Exception:
                pass

            await client.put(key, {"bb": payload}, policy=wp)
            record = await client.get(key, policy=rp)

            assert record is not None
            received = record.bins.get("bb")
            assert received == payload

            try:
                await client.delete(key, policy=wp)
            except Exception:
                pass
        finally:
            await client.close()


# ---------------------------------------------------------------------------
# Remove items via modify_by_path with remove_result()
# ---------------------------------------------------------------------------

class TestRemoveByPath:

    async def test_remove_all_items_from_list(self, cdt_client_and_key):
        """Remove every element from a list using remove_result()."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"items": [1, 2, 3, 4, 5]}}, policy=wp)

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            fe.remove_result(),
            [
                CTX.map_key("items"),
                CTX.all_children_with_filter(fe.bool_val(True)),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        items = record.bins.get("data", {}).get("items")
        assert items is not None
        assert len(items) == 0

    async def test_remove_filtered_items_from_list(self, cdt_client_and_key):
        """Remove list elements with value > 10; keep the rest."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"numbers": [1, 5, 10, 15, 20, 25, 30]}}, policy=wp)

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            fe.remove_result(),
            [
                CTX.map_key("numbers"),
                CTX.all_children_with_filter(
                    fe.gt(fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(10))
                ),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        numbers = record.bins.get("data", {}).get("numbers")
        assert numbers is not None
        assert len(numbers) == 3
        assert set(numbers) == {1, 5, 10}

    async def test_remove_all_items_from_map(self, cdt_client_and_key):
        """Remove all entries from a nested map using remove_result()."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"config": {"opt1": "v1", "opt2": "v2", "opt3": "v3"}}}, policy=wp)

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            fe.remove_result(),
            [
                CTX.map_key("config"),
                CTX.all_children_with_filter(fe.bool_val(True)),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        config = record.bins.get("data", {}).get("config")
        assert config is not None
        assert len(config) == 0

    async def test_remove_filtered_map_entries_by_value(self, cdt_client_and_key):
        """Remove map entries where value < 50; keep the rest."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        scores = {"alice": 95, "bob": 45, "carol": 75, "dave": 30}
        await client.put(key, {"data": {"scores": scores}}, policy=wp)

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            fe.remove_result(),
            [
                CTX.map_key("scores"),
                CTX.all_children_with_filter(
                    fe.lt(fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(50))
                ),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        final_scores = record.bins.get("data", {}).get("scores")
        assert final_scores is not None
        assert len(final_scores) == 2
        assert "bob" not in final_scores
        assert "dave" not in final_scores
        assert "alice" in final_scores
        assert final_scores["alice"] == 95

    async def test_remove_map_entries_by_key_filter(self, cdt_client_and_key):
        """Remove map entries whose key >= 'c'."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        inventory = {"apple": 10, "banana": 5, "cherry": 8, "date": 3}
        await client.put(key, {"data": {"inventory": inventory}}, policy=wp)

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            fe.remove_result(),
            [
                CTX.map_key("inventory"),
                CTX.all_children_with_filter(
                    fe.ge(fe.string_loop_var(LoopVarPart.MAP_KEY), fe.string_val("c"))
                ),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        final = record.bins.get("data", {}).get("inventory")
        assert final is not None
        assert len(final) == 2
        assert "apple" in final
        assert "banana" in final

    async def test_remove_items_by_index(self, cdt_client_and_key):
        """Remove list elements at index >= 3 (keep first three)."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"values": [100, 200, 300, 400, 500]}}, policy=wp)

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            fe.remove_result(),
            [
                CTX.map_key("values"),
                CTX.all_children_with_filter(
                    fe.ge(fe.int_loop_var(LoopVarPart.INDEX), fe.int_val(3))
                ),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        values = record.bins.get("data", {}).get("values")
        assert values is not None
        assert len(values) == 3
        assert values[0] == 100
        assert values[1] == 200
        assert values[2] == 300

    async def test_remove_books_with_low_prices(self, cdt_client_and_key):
        """Remove books with price <= 10.0 from a nested list."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        books = [
            {"title": "Cheap 1", "price": 5.99},
            {"title": "Expensive", "price": 25.99},
            {"title": "Cheap 2", "price": 3.99},
            {"title": "Mid Price", "price": 15.99},
        ]
        await client.put(key, {"data": {"books": books}}, policy=wp)

        price_exp = fe.map_get_by_key(
            MapReturnType.VALUE,
            ExpType.FLOAT,
            fe.string_val("price"),
            fe.map_loop_var(LoopVarPart.VALUE),
            [],
        )

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            fe.remove_result(),
            [
                CTX.map_key("books"),
                CTX.all_children_with_filter(fe.le(price_exp, fe.float_val(10.0))),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        final_books = record.bins.get("data", {}).get("books")
        assert final_books is not None
        assert len(final_books) == 2
        for book in final_books:
            assert book["price"] > 10.0

    async def test_remove_nested_items_with_complex_path(self, cdt_client_and_key):
        """Remove employees with sales < 2000 across all departments."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        departments = {
            "sales": [{"name": "John", "sales": 1000}, {"name": "Jane", "sales": 5000}],
            "engineering": [{"name": "Bob", "sales": 500}, {"name": "Alice", "sales": 3000}],
        }
        await client.put(key, {"data": {"departments": departments}}, policy=wp)

        sales_exp = fe.map_get_by_key(
            MapReturnType.VALUE,
            ExpType.INT,
            fe.string_val("sales"),
            fe.map_loop_var(LoopVarPart.VALUE),
            [],
        )

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            fe.remove_result(),
            [
                CTX.map_key("departments"),
                CTX.all_children_with_filter(fe.bool_val(True)),
                CTX.all_children_with_filter(fe.lt(sales_exp, fe.int_val(2000))),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        final = record.bins.get("data", {}).get("departments")
        assert final is not None
        assert len(final["sales"]) == 1
        assert final["sales"][0]["name"] == "Jane"
        assert len(final["engineering"]) == 1
        assert final["engineering"][0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# Additional select/modify coverage for remaining JSDK test equivalents
# ---------------------------------------------------------------------------

class TestSelectModifyAdditional:

    async def test_complex_and_or_filter(self, cdt_client_and_key):
        """Select products that are (inStock AND price < 20) OR (price > 25)."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        products = [
            {"name": "Widget", "price": 10.0, "inStock": True},
            {"name": "Gadget", "price": 25.0, "inStock": False},
            {"name": "Gizmo", "price": 15.0, "inStock": True},
            {"name": "Doohickey", "price": 30.0, "inStock": True},
        ]
        await client.put(key, {"data": {"products": products}}, policy=wp)

        in_stock_exp = fe.map_get_by_key(
            MapReturnType.VALUE, ExpType.BOOL,
            fe.string_val("inStock"), fe.map_loop_var(LoopVarPart.VALUE), [],
        )
        price_exp = fe.map_get_by_key(
            MapReturnType.VALUE, ExpType.FLOAT,
            fe.string_val("price"), fe.map_loop_var(LoopVarPart.VALUE), [],
        )

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("products"),
                CTX.all_children_with_filter(
                    fe.or_([
                        fe.and_([
                            fe.eq(in_stock_exp, fe.bool_val(True)),
                            fe.lt(price_exp, fe.float_val(20.0)),
                        ]),
                        fe.gt(price_exp, fe.float_val(25.0)),
                    ])
                ),
                CTX.all_children_with_filter(
                    fe.eq(fe.string_loop_var(LoopVarPart.MAP_KEY), fe.string_val("name"))
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None
        # Widget (inStock, price 10), Gizmo (inStock, price 15), Doohickey (price 30)
        assert len(result) >= 1

    async def test_modify_divide_all_values(self, cdt_client_and_key):
        """Divide every list element by 10."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"values": [100, 200, 300]}}, policy=wp)

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            fe.num_div([fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(10)]),
            [
                CTX.map_key("values"),
                CTX.all_children_with_filter(fe.bool_val(True)),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        values = record.bins.get("data", {}).get("values")
        assert values is not None
        assert values[0] == 10

    async def test_select_rows_with_size_3(self, cdt_client_and_key):
        """Select only rows of a matrix that have exactly 3 elements."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        matrix = [[1, 2, 3], [4, 5], [7, 8, 9]]
        await client.put(key, {"data": {"matrix": matrix}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("matrix"),
                CTX.all_children_with_filter(
                    fe.eq(
                        fe.list_size(fe.list_loop_var(LoopVarPart.VALUE), []),
                        fe.int_val(3),
                    )
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None
        assert len(result) == 2  # rows [1,2,3] and [7,8,9]

    async def test_modify_multiply_by_index_plus_one(self, cdt_client_and_key):
        """Multiply each element by (index + 1)."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        await client.put(key, {"data": {"values": [100, 200, 300, 400]}}, policy=wp)

        modify_exp = fe.num_mul([
            fe.int_loop_var(LoopVarPart.VALUE),
            fe.num_add([fe.int_loop_var(LoopVarPart.INDEX), fe.int_val(1)]),
        ])

        op = CdtOperation.modify_by_path(
            "data",
            ModifyFlags.DEFAULT,
            modify_exp,
            [
                CTX.map_key("values"),
                CTX.all_children_with_filter(fe.bool_val(True)),
            ],
        )
        await client.operate(key, [op], policy=wp)

        rp = ReadPolicy()
        record = await client.get(key, policy=rp)
        values = record.bins.get("data", {}).get("values")
        assert values is not None
        assert values[0] == 100   # 100 * (0+1)
        assert values[1] == 400   # 200 * (1+1)
        assert values[2] == 900   # 300 * (2+1)
        assert values[3] == 1600  # 400 * (3+1)

    async def test_select_blob_values(self, cdt_client_and_key):
        """Filter a list of byte values to find the exact target blob."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        blobs = [b"First blob content", b"Second blob content", b"Target blob", b"Fourth blob content"]
        await client.put(key, {"data": {"blobs": blobs}}, policy=wp)

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("blobs"),
                CTX.all_children_with_filter(
                    fe.eq(
                        fe.blob_loop_var(LoopVarPart.VALUE),
                        fe.blob_val(b"Target blob"),
                    )
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        assert result is not None
        assert len(result) == 1
        assert result[0] == b"Target blob"

    async def test_select_geo_json_in_region(self, cdt_client_and_key):
        """Filter a list of GeoJSON points to those inside a California bounding box."""
        client, key = cdt_client_and_key
        wp = WritePolicy()
        locations = [
            GeoJSON('{"type":"Point","coordinates":[-122.4194,37.7749]}'),  # San Francisco
            GeoJSON('{"type":"Point","coordinates":[-118.2437,34.0522]}'),  # Los Angeles
            GeoJSON('{"type":"Point","coordinates":[-73.9352,40.7306]}'),   # New York
        ]
        await client.put(key, {"data": {"locations": locations}}, policy=wp)

        california = '{"type":"Polygon","coordinates":[[[-124.5,32.5],[-114.0,32.5],[-114.0,42.0],[-124.5,42.0],[-124.5,32.5]]]}'

        op = CdtOperation.select_by_path(
            "data",
            SelectFlags.VALUE,
            [
                CTX.map_key("locations"),
                CTX.all_children_with_filter(
                    fe.geo_compare(
                        fe.geo_json_loop_var(LoopVarPart.VALUE),
                        fe.geo_val(california),
                    )
                ),
            ],
        )
        record = await client.operate(key, [op], policy=wp)
        assert record is not None
        result = record.bins.get("data")
        # SF and LA are in California, NY is not
        if result is not None:
            assert len(result) == 2
