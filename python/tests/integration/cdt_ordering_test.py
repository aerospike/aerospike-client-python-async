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

"""Tests proving K-ordered map key ordering is preserved through native Python dict."""

import pytest
import pytest_asyncio

from aerospike_async import (
    new_client, ClientPolicy, WritePolicy, ReadPolicy, Key,
    MapOperation, MapOrder, MapPolicy, MapReturnType, Operation,
)
from aerospike_async.exceptions import InvalidRequest, ResultCode


BIN = "mapbin"


@pytest_asyncio.fixture
async def client_and_key(aerospike_host, use_services_alternate):
    """Setup client and prepare test keys."""
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)

    wp = WritePolicy()
    for i in range(1, 25):
        try:
            await client.delete(Key("test", "test", f"cdt_ord_{i}"), policy=wp)
        except Exception:
            pass

    return client


def _key(n: int) -> Key:
    return Key("test", "test", f"cdt_ord_{n}")


class TestKOrderedMapOrdering:
    """K-ordered maps return dict with keys in sorted iteration order."""

    async def test_string_keys_sorted(self, client_and_key):
        """Insert string keys out of order into a K-ordered map, read back sorted."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(1)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "cherry", 3, policy),
            MapOperation.put(BIN, "apple", 1, policy),
            MapOperation.put(BIN, "banana", 2, policy),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == ["apple", "banana", "cherry"]

    async def test_integer_keys_sorted(self, client_and_key):
        """Insert integer keys out of order into a K-ordered map, read back sorted."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(2)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, 50, "fifty", policy),
            MapOperation.put(BIN, 10, "ten", policy),
            MapOperation.put(BIN, 30, "thirty", policy),
            MapOperation.put(BIN, 20, "twenty", policy),
            MapOperation.put(BIN, 40, "forty", policy),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == [10, 20, 30, 40, 50]

    async def test_many_keys_sorted(self, client_and_key):
        """K-ordered map with 100 keys preserves sorted order."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(3)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        keys_reversed = list(range(100, 0, -1))
        ops = [MapOperation.put(BIN, k, k * 10, policy) for k in keys_reversed]
        await client.operate(key, ops, policy=wp)

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert list(m.keys()) == list(range(1, 101))

    async def test_ordering_after_add(self, client_and_key):
        """Adding a key to a K-ordered map keeps all keys sorted."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(4)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "b", 2, policy),
            MapOperation.put(BIN, "d", 4, policy),
        ],
            policy=wp,
        )

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "c", 3, policy),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert list(m.keys()) == ["a", "b", "c", "d"]

    async def test_ordering_after_remove(self, client_and_key):
        """Removing keys from a K-ordered map keeps remaining keys sorted."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(5)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "b", 2, policy),
            MapOperation.put(BIN, "c", 3, policy),
            MapOperation.put(BIN, "d", 4, policy),
        ],
            policy=wp,
        )

        await client.operate(
            key,
            [
            MapOperation.remove_by_key(BIN, "b", MapReturnType.NONE),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert list(m.keys()) == ["a", "c", "d"]

    async def test_ordering_after_remove_by_value(self, client_and_key):
        """Removing entries by value from a K-ordered map keeps remaining keys sorted."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(9)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "a", 100, policy),
            MapOperation.put(BIN, "b", 200, policy),
            MapOperation.put(BIN, "c", 100, policy),
            MapOperation.put(BIN, "d", 300, policy),
            MapOperation.put(BIN, "e", 200, policy),
        ],
            policy=wp,
        )

        await client.operate(
            key,
            [
            MapOperation.remove_by_value(BIN, 200, MapReturnType.NONE),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert list(m.keys()) == ["a", "c", "d"]
        assert list(m.values()) == [100, 100, 300]

    async def test_round_trip_preserves_order(self, client_and_key):
        """Read an ordered map, clear it, re-insert via MapOperation — order preserved."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(6)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "z", 26, policy),
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "m", 13, policy),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        original = record.bins[BIN]
        assert list(original.keys()) == ["a", "m", "z"]

        # Clear and re-insert using MapOperation to preserve K-ordered policy
        items = list(original.items())
        await client.operate(
            key,
            [
            MapOperation.clear(BIN),
            MapOperation.put_items(BIN, items, policy),
        ],
            policy=wp,
        )
        record2 = await client.get(key, [BIN], policy=rp)
        assert list(record2.bins[BIN].keys()) == ["a", "m", "z"]


class TestKVOrderedMapOrdering:
    """KV-ordered maps return dict with keys in sorted iteration order."""

    async def test_kv_ordered_string_keys_sorted(self, client_and_key):
        """KV-ordered map keys iterate in sorted order, same as K-ordered."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(7)
        policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "cherry", 30, policy),
            MapOperation.put(BIN, "apple", 10, policy),
            MapOperation.put(BIN, "banana", 20, policy),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == ["apple", "banana", "cherry"]

    async def test_kv_ordered_integer_keys_sorted(self, client_and_key):
        """KV-ordered map with integer keys returns them in sorted order."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(8)
        policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, 50, "fifty", policy),
            MapOperation.put(BIN, 10, "ten", policy),
            MapOperation.put(BIN, 30, "thirty", policy),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == [10, 30, 50]


class TestUnorderedMap:
    """Unordered maps return dict with no guaranteed key order."""

    async def test_unordered_map_has_no_key_order(self, client_and_key):
        """Unordered maps return dict; key iteration order is not guaranteed."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(10)

        await client.put(key, {BIN: {"x": 1, "y": 2, "z": 3}}, policy=wp)
        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert set(m.keys()) == {"x", "y", "z"}
        assert m["x"] == 1


class TestNestedOrderedMaps:
    """Nested K-ordered maps should preserve ordering at every level."""

    async def test_nested_ordered_maps(self, client_and_key):
        """Outer K-ordered map preserves key order; inner maps are unordered
        unless explicitly created with K-ordered policy."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(11)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        inner = {"c": 3, "a": 1, "b": 2}
        await client.operate(
            key,
            [
            MapOperation.put(BIN, "z_outer", inner, policy),
            MapOperation.put(BIN, "a_outer", inner, policy),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]

        # Outer keys are K-ordered → sorted
        assert list(m.keys()) == ["a_outer", "z_outer"]

        # Inner maps were sent as plain dicts (unordered HashMap) —
        # ordering policy is NOT inherited from the parent map.
        for inner_map in m.values():
            assert isinstance(inner_map, dict)
            assert set(inner_map.keys()) == {"a", "b", "c"}


class TestEdgeCases:
    """Edge cases for ordered map conversion through PythonValue::OrderedMap."""

    async def test_mixed_key_types_sorted(self, client_and_key):
        """Aerospike sorts by type first (int before string), then by value."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(14)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "banana", "s2", policy),
            MapOperation.put(BIN, 99, "i3", policy),
            MapOperation.put(BIN, "apple", "s1", policy),
            MapOperation.put(BIN, 1, "i1", policy),
            MapOperation.put(BIN, 50, "i2", policy),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        keys = list(m.keys())
        int_keys = [k for k in keys if isinstance(k, int)]
        str_keys = [k for k in keys if isinstance(k, str)]
        assert int_keys == sorted(int_keys)
        assert str_keys == sorted(str_keys)
        # Integers sort before strings in Aerospike's type ordering
        assert keys == int_keys + str_keys

    async def test_bytes_keys_sorted(self, client_and_key):
        """Bytes keys in a K-ordered map preserve sorted order."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(15)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, b"\x03", "third", policy),
            MapOperation.put(BIN, b"\x01", "first", policy),
            MapOperation.put(BIN, b"\x02", "second", policy),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert list(m.keys()) == [b"\x01", b"\x02", b"\x03"]

    async def test_empty_ordered_map(self, client_and_key):
        """Empty K-ordered map returns an empty dict."""
        client = client_and_key
        wp = WritePolicy()
        rp = ReadPolicy()
        key = _key(17)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "a", 1, policy),
        ],
            policy=wp,
        )
        await client.operate(
            key,
            [
            MapOperation.remove_by_key(BIN, "a", MapReturnType.NONE),
        ],
            policy=wp,
        )

        record = await client.get(key, [BIN], policy=rp)
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert len(m) == 0

    async def test_get_by_rank_range_ordered(self, client_and_key):
        """get_by_rank_range on K-ordered map returns values in rank order."""
        client = client_and_key
        wp = WritePolicy()
        key = _key(18)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "c", 300, policy),
            MapOperation.put(BIN, "a", 100, policy),
            MapOperation.put(BIN, "b", 200, policy),
            MapOperation.put(BIN, "d", 400, policy),
        ],
            policy=wp,
        )

        # Rank 0 = smallest value (100), get 3 entries by rank
        record = await client.operate(
            key,
            [
            MapOperation.get_by_rank_range(BIN, 0, 3, MapReturnType.VALUE),
        ],
            policy=wp,
        )
        values = record.bins[BIN]
        assert values == [100, 200, 300]


class TestOrderedMapReturnTypes:
    """MapReturnType.ORDERED_MAP should return keys in order."""

    async def test_ordered_map_return_type(self, client_and_key):
        """get_by_key_range with ORDERED_MAP return type preserves key order."""
        client = client_and_key
        wp = WritePolicy()
        key = _key(12)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "e", 5, policy),
            MapOperation.put(BIN, "c", 3, policy),
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "d", 4, policy),
            MapOperation.put(BIN, "b", 2, policy),
        ],
            policy=wp,
        )

        record = await client.operate(
            key,
            [
            MapOperation.get_by_key_range(BIN, "b", "e", MapReturnType.ORDERED_MAP),
        ],
            policy=wp,
        )

        result = record.bins[BIN]
        assert isinstance(result, dict)
        assert list(result.keys()) == ["b", "c", "d"]

    async def test_unordered_map_return_type(self, client_and_key):
        """get_by_key_range with UNORDERED_MAP return type still returns dict."""
        client = client_and_key
        wp = WritePolicy()
        key = _key(13)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.operate(
            key,
            [
            MapOperation.put(BIN, "e", 5, policy),
            MapOperation.put(BIN, "c", 3, policy),
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "d", 4, policy),
            MapOperation.put(BIN, "b", 2, policy),
        ],
            policy=wp,
        )

        record = await client.operate(
            key,
            [
            MapOperation.get_by_key_range(BIN, "b", "e", MapReturnType.UNORDERED_MAP),
        ],
            policy=wp,
        )

        result = record.bins[BIN]
        assert isinstance(result, dict)
        assert set(result.keys()) == {"b", "c", "d"}


class TestKOrderedMapKeyConstraints:
    """K-ordered map operations that the server rejects."""

    async def test_non_scalar_map_key_raises_parameter_error(self, client_and_key):
        """List (non-scalar) key in a KEY_ORDERED map put is rejected with PARAMETER_ERROR."""
        client = client_and_key
        wp = WritePolicy()
        key = _key(19)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await client.delete(key, policy=wp)
        with pytest.raises(InvalidRequest) as exc_info:
            await client.operate(
                key,
                [MapOperation.put(BIN, [1, 2], "value", policy)],
                policy=wp,
            )
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
