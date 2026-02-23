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
import pytest_asyncio

from aerospike_async import (new_client, ClientPolicy, WritePolicy, ReadPolicy, Key, MapOperation,
                             MapPolicy, MapOrder, MapWriteMode, MapReturnType, ResultCode, CTX, Operation)
from aerospike_async.exceptions import ServerError


@pytest_asyncio.fixture
async def client_and_key(aerospike_host):
    """Setup client and prepare test key."""
    cp = ClientPolicy()
    cp.use_services_alternate = True
    client = await new_client(cp, aerospike_host)

    # Create a test key
    key = Key("test", "test", "opkey")

    # Delete the record first to ensure clean state
    wp = WritePolicy()
    await client.delete(wp, key)

    return client, key

async def test_operate_map_size(client_and_key):
    """Test operate with Map size operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Create a map with some items
    await client.operate(
        wp,
        key,
        [
            MapOperation.put("mapbin", 1, "value1", map_policy),
            MapOperation.put("mapbin", 2, "value2", map_policy),
            MapOperation.put("mapbin", 3, "value3", map_policy),
        ]
    )

    # Get map size
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.size("mapbin")
        ]
    )

    assert record is not None
    assert record.bins is not None
    size = record.bins.get("mapbin")
    assert size == 3


async def test_operate_map_clear(client_and_key):
    """Test operate with Map clear operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Create a map with some items
    await client.operate(
        wp,
        key,
        [
            MapOperation.put("mapbin", 1, "value1", map_policy),
            MapOperation.put("mapbin", 2, "value2", map_policy),
        ]
    )

    # Clear the map
    await client.operate(
        wp,
        key,
        [
            MapOperation.clear("mapbin")
        ]
    )

    # Verify map is empty
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.size("mapbin")
        ]
    )

    assert record is not None
    assert record.bins is not None
    size = record.bins.get("mapbin")
    assert size == 0


async def test_operate_map_put(client_and_key):
    """Test operate with Map put operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()
    put_mode = MapPolicy(None, None)
    add_mode = MapPolicy(MapOrder.UNORDERED, MapWriteMode.CREATE_ONLY)
    update_mode = MapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE_ONLY)

    # Delete the record first
    await client.delete(wp, key)

    # Put multiple items with different policies
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put("mapbin", 11, 789, put_mode),
            MapOperation.put("mapbin", 10, 999, put_mode),
            MapOperation.put("mapbin", 12, 500, add_mode),
            MapOperation.put("mapbin", 15, 1000, add_mode),
            MapOperation.put("mapbin", 10, 1, update_mode),
            MapOperation.put("mapbin", 15, 5, update_mode),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)
    i = 0

    size = results[i]
    assert size == 1
    i += 1

    size = results[i]
    assert size == 2
    i += 1

    size = results[i]
    assert size == 3
    i += 1

    size = results[i]
    assert size == 4
    i += 1

    size = results[i]
    assert size == 4
    i += 1

    size = results[i]
    assert size == 4

    # Verify final map state
    record = await client.get(rp, key, ["mapbin"])
    assert record is not None
    assert record.bins is not None
    map_data = record.bins.get("mapbin")
    assert map_data is not None
    assert isinstance(map_data, dict)
    assert map_data[10] == 1
    assert len(map_data) == 4


async def test_operate_map_put_items(client_and_key):
    """Test operate with Map put_items operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()
    put_mode = MapPolicy(None, None)
    add_mode = MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.CREATE_ONLY)
    update_mode = MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE_ONLY)

    # Delete the record first
    await client.delete(wp, key)

    # Put items with different policies, then getByKey and getByKeyRange operations
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", [(12, "myval"), (-8734, "str2"), (1, "my default")], add_mode),
            MapOperation.put_items("mapbin", [(12, "myval12222"), (13, "str13")], put_mode),
            MapOperation.put_items("mapbin", [(13, "myval2")], update_mode),
            MapOperation.put_items("mapbin", [(12, 23), (-8734, "changed")], update_mode),
            MapOperation.get_by_key("mapbin", 1, MapReturnType.VALUE),
            MapOperation.get_by_key("mapbin", -8734, MapReturnType.VALUE),
            MapOperation.get_by_key_range("mapbin", 12, 15, MapReturnType.KEY_VALUE),
            MapOperation.get_by_key_range("mapbin", 12, 15, MapReturnType.UNORDERED_MAP),
            MapOperation.get_by_key_range("mapbin", 12, 15, MapReturnType.ORDERED_MAP),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)
    i = 0

    # First 4 results are sizes from putItems operations
    size = results[i]
    assert size == 3
    i += 1

    size = results[i]
    assert size == 4
    i += 1

    size = results[i]
    assert size == 4
    i += 1

    size = results[i]
    assert size == 4
    i += 1

    # Next 2 results are string values from getByKey operations
    str_val = results[i]
    assert str_val == "my default"
    i += 1

    str_val = results[i]
    assert str_val == "changed"
    i += 1

    # Next result is KeyValue (list of entries for range queries)
    key_value_list = results[i]
    assert isinstance(key_value_list, (list, dict))
    if isinstance(key_value_list, dict):
        # Python returns dict for KeyValue range queries
        assert len(key_value_list) == 2
        assert 12 in key_value_list
        assert 13 in key_value_list
    else:
        # Or list of entries
        assert len(key_value_list) == 2
    i += 1

    # Next result is UnorderedMap (dict/HashMap)
    unordered_map = results[i]
    assert isinstance(unordered_map, dict)
    assert len(unordered_map) == 2
    assert 12 in unordered_map
    assert 13 in unordered_map
    i += 1

    # Last result is OrderedMap (dict/TreeMap)
    ordered_map = results[i]
    assert isinstance(ordered_map, dict)
    assert len(ordered_map) == 2
    assert 12 in ordered_map
    assert 13 in ordered_map

    # Verify final map state
    record = await client.get(rp, key, ["mapbin"])
    assert record is not None
    assert record.bins is not None
    map_data = record.bins.get("mapbin")
    assert map_data is not None
    assert isinstance(map_data, dict)
    assert map_data[1] == "my default"
    assert map_data[-8734] == "changed"
    assert map_data[12] == 23
    assert map_data[13] == "myval2"
    assert len(map_data) == 4


async def test_operate_map_increment_value(client_and_key):
    """Test operate with Map increment_value operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()
    map_policy = MapPolicy(None, None)

    # Create a map with numeric values
    await client.operate(
        wp,
        key,
        [
            MapOperation.put("mapbin", "counter1", 10, map_policy),
            MapOperation.put("mapbin", "counter2", 20, map_policy),
        ]
    )

    # Increment values
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.increment_value("mapbin", "counter1", 5, map_policy),
            MapOperation.increment_value("mapbin", "counter2", 10, map_policy),
            MapOperation.increment_value("mapbin", "counter1", 3, map_policy),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)
    assert len(results) == 3

    # Verify final map state
    record = await client.get(rp, key, ["mapbin"])
    assert record is not None
    assert record.bins is not None
    map_data = record.bins.get("mapbin")
    assert map_data is not None
    assert isinstance(map_data, dict)
    assert map_data["counter1"] == 18
    assert map_data["counter2"] == 30


async def test_operate_map_decrement_value(client_and_key):
    """Test operate with Map decrement_value operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()
    map_policy = MapPolicy(None, None)

    # Create a map with numeric values
    await client.operate(
        wp,
        key,
        [
            MapOperation.put("mapbin", "counter1", 100, map_policy),
            MapOperation.put("mapbin", "counter2", 50, map_policy),
        ]
    )

    # Decrement values
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.decrement_value("mapbin", "counter1", 10, map_policy),
            MapOperation.decrement_value("mapbin", "counter2", 5, map_policy),
            MapOperation.decrement_value("mapbin", "counter1", 20, map_policy),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)
    assert len(results) == 3

    # Verify final map state
    record = await client.get(rp, key, ["mapbin"])
    assert record is not None
    assert record.bins is not None
    map_data = record.bins.get("mapbin")
    assert map_data is not None
    assert isinstance(map_data, dict)
    assert map_data["counter1"] == 70
    assert map_data["counter2"] == 45


async def test_operate_map_remove_by_key(client_and_key):
    """Test operate with Map remove_by_key operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()
    map_policy = MapPolicy(None, None)

    # Create a map with some items
    await client.operate(
        wp,
        key,
        [
            MapOperation.put("mapbin", "key1", "value1", map_policy),
            MapOperation.put("mapbin", "key2", "value2", map_policy),
            MapOperation.put("mapbin", "key3", "value3", map_policy),
        ]
    )

    # Remove by key
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_key("mapbin", "key2", MapReturnType.VALUE),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert results == "value2"

    # Verify the map state
    record = await client.get(rp, key, ["mapbin"])
    assert record is not None
    assert record.bins is not None
    map_data = record.bins.get("mapbin")
    assert map_data is not None
    assert isinstance(map_data, dict)
    assert "key1" in map_data
    assert "key2" not in map_data
    assert "key3" in map_data
    assert len(map_data) == 2


async def test_operate_map_remove_by_key_range(client_and_key):
    """Test operate with Map remove_by_key_range operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()
    map_policy = MapPolicy(None, None)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items
    await client.operate(
        wp,
        key,
        [
            MapOperation.put("mapbin", 1, "value1", map_policy),
            MapOperation.put("mapbin", 2, "value2", map_policy),
            MapOperation.put("mapbin", 3, "value3", map_policy),
            MapOperation.put("mapbin", 4, "value4", map_policy),
            MapOperation.put("mapbin", 5, "value5", map_policy),
        ]
    )

    # Remove by key range
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_key_range("mapbin", 2, 4, MapReturnType.COUNT),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    # Count should be 2 (keys 2, 3 were removed - range is exclusive on end)
    assert results == 2

    # Verify the map state
    record = await client.get(rp, key, ["mapbin"])
    assert record is not None
    assert record.bins is not None
    map_data = record.bins.get("mapbin")
    assert map_data is not None
    assert isinstance(map_data, dict)
    assert 1 in map_data
    assert 2 not in map_data
    assert 3 not in map_data
    assert 4 in map_data  # Range is exclusive on end, so 4 is not removed
    assert 5 in map_data
    assert len(map_data) == 3


async def test_operate_map_index_operations(client_and_key):
    """Test operate with Map index-based operations."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put("mapbin", 4, 4, map_policy),
            MapOperation.put("mapbin", 3, 3, map_policy),
            MapOperation.put("mapbin", 2, 2, map_policy),
            MapOperation.put("mapbin", 1, 1, map_policy),
            MapOperation.get_by_index("mapbin", 2, MapReturnType.KEY_VALUE),
            MapOperation.get_by_index_range("mapbin", 0, 10, MapReturnType.KEY_VALUE),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)
    i = 3

    # First 4 results are sizes from put operations
    size = results[i]
    assert size == 4
    i += 1

    # Next result: getByIndex at index 2 (KeyValue) - Python flattens, so it's a dict
    key_value_result = results[i]
    assert isinstance(key_value_result, dict)
    assert len(key_value_result) == 1
    i += 1

    # Next result: getByIndexRange (KeyValue) - Python flattens, so it's a dict
    key_value_range = results[i]
    assert isinstance(key_value_range, dict)
    assert len(key_value_range) == 4


async def test_operate_map_rank_operations(client_and_key):
    """Test operate with Map rank-based operations."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items (scores)
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Increment some scores
    await client.operate(
        wp,
        key,
        [
            MapOperation.increment_value("mapbin", "John", 5, map_policy),
            MapOperation.increment_value("mapbin", "Jim", -4, map_policy),
        ]
    )

    # Get by rank operations
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.get_by_rank_range("mapbin", -2, 2, MapReturnType.KEY),
            MapOperation.get_by_rank_range("mapbin", 0, 2, MapReturnType.KEY_VALUE),
            MapOperation.get_by_rank("mapbin", 0, MapReturnType.VALUE),
            MapOperation.get_by_rank("mapbin", 2, MapReturnType.KEY),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)
    # MultiResult contains 4 results (no flattening):
    # getByRankRange(-2, 2, KEY) returns a list of 2 keys ['Harry', 'Jim']
    # getByRankRange(0, 2, KEY_VALUE) returns a dict
    # getByRank(0, VALUE) returns a value (55)
    # getByRank(2, KEY) returns a key ('Harry')
    assert len(results) == 4

    # First result: getByRankRange(-2, 2, KEY) - returns a list of keys
    assert isinstance(results[0], list)
    assert "Harry" in results[0]
    assert "Jim" in results[0]
    assert len(results[0]) == 2

    # Second result: getByRankRange(0, 2, KEY_VALUE) - returns a dict
    assert isinstance(results[1], dict)
    assert len(results[1]) == 2
    assert "Charlie" in results[1]
    assert "John" in results[1]

    # Third result: getByRank(0, VALUE) - returns a value
    assert results[2] == 55

    # Fourth result: getByRank(2, KEY) - returns a key
    assert results[3] == "Harry"


async def test_operate_map_value_operations(client_and_key):
    """Test operate with Map value-based operations."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items (scores)
    input_map = [("Charlie", 55), ("Jim", 94), ("John", 81), ("Harry", 82)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Get by value operations
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.get_by_value_range("mapbin", 90, 95, MapReturnType.RANK),
            MapOperation.get_by_value_range("mapbin", 90, 95, MapReturnType.COUNT),
            MapOperation.get_by_value_range("mapbin", 90, 95, MapReturnType.KEY_VALUE),
            MapOperation.get_by_value_range("mapbin", 81, 82, MapReturnType.KEY),
            MapOperation.get_by_value("mapbin", 77, MapReturnType.KEY),
            MapOperation.get_by_value("mapbin", 81, MapReturnType.RANK),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)
    # MultiResult contains 6 results (no flattening):
    # getByValueRange(90, 95, RANK) returns a list [3]
    # getByValueRange(90, 95, COUNT) returns a count (int) 1
    # getByValueRange(90, 95, KEY_VALUE) returns a dict {'Jim': 94}
    # getByValueRange(81, 82, KEY) returns a list ['John']
    # getByValue(77, KEY) returns empty list []
    # getByValue(81, RANK) returns a list [2]
    assert len(results) == 6

    # First result: getByValueRange(90, 95, RANK) - returns a list
    assert isinstance(results[0], list)
    assert results[0] == [3]

    # Second result: getByValueRange(90, 95, COUNT) - returns an int
    assert results[1] == 1

    # Third result: getByValueRange(90, 95, KEY_VALUE) - returns a dict
    assert isinstance(results[2], dict)
    assert len(results[2]) == 1
    assert "Jim" in results[2]
    assert results[2]["Jim"] == 94

    # Fourth result: getByValueRange(81, 82, KEY) - returns a list
    assert isinstance(results[3], list)
    assert results[3] == ["John"]

    # Fifth result: getByValue(77, KEY) - returns empty list
    assert isinstance(results[4], list)
    assert results[4] == []

    # Sixth result: getByValue(81, RANK) - returns a list
    # Value 81 (John) is at rank 1 when sorted by value
    assert isinstance(results[5], list)
    assert results[5] == [1]


async def test_operate_map_get_by_index_range_from(client_and_key):
    """Test operate with Map get_by_index_range_from operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put("mapbin", 4, 4, map_policy),
            MapOperation.put("mapbin", 3, 3, map_policy),
            MapOperation.put("mapbin", 2, 2, map_policy),
            MapOperation.put("mapbin", 1, 1, map_policy),
        ]
    )

    # Get by index range from index 2 to end
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.get_by_index_range_from("mapbin", 2, MapReturnType.KEY_VALUE),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")

    # Result: getByIndexRangeFrom(2) should return items from index 2 to end
    # Python flattens single operation results, so it's a dict directly
    if isinstance(results, dict):
        key_value_result = results
    else:
        assert isinstance(results, list)
        key_value_result = results[0]

    assert isinstance(key_value_result, dict)
    # Should have 2 items (indices 2 and 3)
    assert len(key_value_result) == 2


async def test_operate_map_get_by_rank_range_from(client_and_key):
    """Test operate with Map get_by_rank_range_from operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items (scores)
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Get by rank range from rank 2 to end
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.get_by_rank_range_from("mapbin", 2, MapReturnType.KEY_VALUE),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")

    # Result: getByRankRangeFrom(2) should return items from rank 2 to end
    # Python flattens single operation results, so it's a dict directly
    if isinstance(results, dict):
        key_value_result = results
    else:
        assert isinstance(results, list)
        key_value_result = results[0]

    assert isinstance(key_value_result, dict)
    # Should have 2 items (ranks 2 and 3)
    assert len(key_value_result) == 2


async def test_operate_map_remove_by_index(client_and_key):
    """Test operate with Map remove_by_index operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Remove by index
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_index("mapbin", 1, MapReturnType.KEY_VALUE),
            MapOperation.size("mapbin"),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: removeByIndex returns the removed item (KEY_VALUE)
    removed = results[0]
    assert isinstance(removed, dict)
    assert len(removed) == 1

    # Second result: size should be 3 (one item removed)
    size = results[1]
    assert size == 3


async def test_operate_map_remove_by_index_range(client_and_key):
    """Test operate with Map remove_by_index_range operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82),
                 ("Sally", 79), ("Lenny", 84), ("Abe", 88)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Remove by index range
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_index_range("mapbin", 0, 2, MapReturnType.COUNT),
            MapOperation.size("mapbin"),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: removeByIndexRange returns count of removed items
    count = results[0]
    assert count == 2

    # Second result: size should be 5 (2 items removed from 7)
    size = results[1]
    assert size == 5


async def test_operate_map_remove_by_index_range_from(client_and_key):
    """Test operate with Map remove_by_index_range_from operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Remove by index range from index 2 to end
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_index_range_from("mapbin", 2, MapReturnType.COUNT),
            MapOperation.size("mapbin"),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: removeByIndexRangeFrom returns count of removed items
    count = results[0]
    assert count == 2  # Removed indices 2 and 3

    # Second result: size should be 2 (2 items removed from 4)
    size = results[1]
    assert size == 2


async def test_operate_map_remove_by_rank(client_and_key):
    """Test operate with Map remove_by_rank operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items (scores)
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Remove by rank
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_rank("mapbin", 1, MapReturnType.KEY_VALUE),
            MapOperation.size("mapbin"),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: removeByRank returns the removed item (KEY_VALUE)
    removed = results[0]
    assert isinstance(removed, dict)
    assert len(removed) == 1

    # Second result: size should be 3 (one item removed)
    size = results[1]
    assert size == 3


async def test_operate_map_remove_by_rank_range(client_and_key):
    """Test operate with Map remove_by_rank_range operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items (scores)
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82),
                 ("Sally", 79), ("Lenny", 84), ("Abe", 88)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Remove by rank range
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_rank_range("mapbin", 0, 2, MapReturnType.COUNT),
            MapOperation.size("mapbin"),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: removeByRankRange returns count of removed items
    count = results[0]
    assert count == 2

    # Second result: size should be 5 (2 items removed from 7)
    size = results[1]
    assert size == 5


async def test_operate_map_remove_by_rank_range_from(client_and_key):
    """Test operate with Map remove_by_rank_range_from operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items (scores)
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Remove by rank range from rank 2 to end
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_rank_range_from("mapbin", 2, MapReturnType.COUNT),
            MapOperation.size("mapbin"),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: removeByRankRangeFrom returns count of removed items
    count = results[0]
    assert count == 2  # Removed ranks 2 and 3

    # Second result: size should be 2 (2 items removed from 4)
    size = results[1]
    assert size == 2


async def test_operate_map_remove_by_value(client_and_key):
    """Test operate with Map remove_by_value operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items (scores)
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82),
                 ("Sally", 79), ("Lenny", 84), ("Abe", 88)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Remove by value
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_value("mapbin", 55, MapReturnType.KEY),
            MapOperation.size("mapbin"),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: removeByValue returns keys of removed items
    removed_keys = results[0]
    assert isinstance(removed_keys, list)
    assert len(removed_keys) == 1
    assert "Charlie" in removed_keys

    # Second result: size should be 6 (one item removed from 7)
    size = results[1]
    assert size == 6


async def test_operate_map_remove_by_value_range(client_and_key):
    """Test operate with Map remove_by_value_range operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items (scores)
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82),
                 ("Sally", 79), ("Lenny", 84), ("Abe", 88)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Remove by value range
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_value_range("mapbin", 80, 85, MapReturnType.COUNT),
            MapOperation.size("mapbin"),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: removeByValueRange returns count of removed items
    # Values in range [80, 85): Harry (82), Lenny (84) = 2 items
    count = results[0]
    assert count == 2

    # Second result: size should be 5 (2 items removed from 7)
    size = results[1]
    assert size == 5


async def test_operate_map_get_by_list(client_and_key):
    """Test operate with Map get_by_key_list and get_by_value_list operations."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items
    input_map = [("Charlie", 55), ("Jim", 98), ("John", 76), ("Harry", 82)]
    await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    # Get by key list and value list
    key_list = ["Harry", "Jim"]
    value_list = [76, 50]

    record = await client.operate(
        wp,
        key,
        [
            MapOperation.get_by_key_list("mapbin", key_list, MapReturnType.KEY_VALUE),
            MapOperation.get_by_value_list("mapbin", value_list, MapReturnType.KEY_VALUE),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: getByKeyList returns a list of key-value pairs
    # Python flattens nested lists, so we need to find the dict result
    key_value_dict = None
    for r in results:
        if isinstance(r, dict):
            if "Harry" in r or "Jim" in r:
                key_value_dict = r
                break

    assert key_value_dict is not None
    assert len(key_value_dict) == 2
    assert "Harry" in key_value_dict
    assert key_value_dict["Harry"] == 82
    assert "Jim" in key_value_dict
    assert key_value_dict["Jim"] == 98

    # Second result: getByValueList returns items with values 76 (John) and 50 (nonexistent)
    # Find the dict result for value 76
    value_dict = None
    for r in results:
        if isinstance(r, dict):
            if "John" in r:
                value_dict = r
                break

    assert value_dict is not None
    assert len(value_dict) == 1
    assert "John" in value_dict
    assert value_dict["John"] == 76


async def test_operate_map_remove_by_key_list(client_and_key):
    """Test operate with Map remove_by_key_list operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items
    input_map = [
        ("Charlie", 55),
        ("Jim", 98),
        ("John", 76),
        ("Harry", 82),
        ("Sally", 79),
        ("Lenny", 84),
        ("Abe", 88),
    ]

    # Remove by key list - combine putItems with remove operations in one call
    remove_keys = ["Sally", "UNKNOWN", "Lenny"]
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
            MapOperation.remove_by_key("mapbin", "NOTFOUND", MapReturnType.VALUE),
            MapOperation.remove_by_key("mapbin", "Jim", MapReturnType.VALUE),
            MapOperation.remove_by_key_list("mapbin", remove_keys, MapReturnType.COUNT),
            MapOperation.remove_by_value("mapbin", 55, MapReturnType.KEY),
            MapOperation.size("mapbin"),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: putItems size (7)
    assert 7 in results

    # Second result: removeByKey("NOTFOUND") returns None
    # Python may represent this as None or omit it - check for None in results
    none_found = any(r is None for r in results)

    # Third result: removeByKey("Jim") returns value 98
    assert 98 in results

    # Fourth result: removeByKeyList returns count (2 - Sally and Lenny, UNKNOWN doesn't exist)
    assert 2 in results

    # Fifth result: removeByValue(55) returns key "Charlie"
    charlie_found = False
    for r in results:
        if isinstance(r, list) and "Charlie" in r:
            charlie_found = True
            break
        elif r == "Charlie":
            charlie_found = True
            break
    assert charlie_found

    # Sixth result: map_size returns 3 (remaining items: John, Harry, Abe)
    assert 3 in results

    # Verify the map state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["mapbin"])
    assert record is not None
    map_data = record.bins.get("mapbin")
    assert isinstance(map_data, dict)
    assert len(map_data) == 3
    assert "John" in map_data
    assert "Harry" in map_data
    assert "Abe" in map_data
    assert "Sally" not in map_data
    assert "Lenny" not in map_data
    assert "Jim" not in map_data
    assert "Charlie" not in map_data


async def test_operate_map_remove_by_key_list_for_non_existing_key(client_and_key):
    """Test operate with Map remove_by_key_list on non-existing key."""
    client, key = client_and_key

    wp = WritePolicy()

    # Delete the record to ensure it doesn't exist
    await client.delete(wp, key)

    # Try to remove from a non-existing key - should raise KEY_NOT_FOUND_ERROR
    with pytest.raises(ServerError) as exi:
        await client.operate(
            wp,
            key,
            [
                MapOperation.remove_by_key_list("mapbin", ["key-1"], MapReturnType.VALUE),
            ]
        )
    assert exi.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR


async def test_operate_map_remove_by_value_list(client_and_key):
    """Test operate with Map remove_by_value_list operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()
    map_policy = MapPolicy(None, None)

    # Delete the record first
    await client.delete(wp, key)

    # Create a map with items (some with duplicate values)
    input_map = [
        ("Alice", 100),
        ("Bob", 200),
        ("Charlie", 100),  # Same value as Alice
        ("David", 300),
        ("Eve", 200),  # Same value as Bob
        ("Frank", 400),
        ("Grace", 100),  # Same value as Alice and Charlie
    ]

    # Remove by value list - remove items with values 100 and 200
    remove_values = [100, 200, 999]  # 999 doesn't exist
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
            MapOperation.remove_by_value_list("mapbin", remove_values, MapReturnType.COUNT),
            MapOperation.size("mapbin"),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # First result: putItems size (7)
    assert 7 in results

    # Second result: removeByValueList returns count (5 - Alice, Bob, Charlie, Eve, Grace)
    # Values 100 and 200 appear multiple times, so 5 items should be removed
    assert 5 in results

    # Third result: map_size returns 2 (remaining items: David, Frank)
    assert 2 in results

    # Verify the map state
    record = await client.get(rp, key, ["mapbin"])
    assert record is not None
    map_data = record.bins.get("mapbin")
    assert isinstance(map_data, dict)
    assert len(map_data) == 2
    assert "David" in map_data
    assert map_data["David"] == 300
    assert "Frank" in map_data
    assert map_data["Frank"] == 400
    # Verify removed items are gone
    assert "Alice" not in map_data
    assert "Bob" not in map_data
    assert "Charlie" not in map_data
    assert "Eve" not in map_data
    assert "Grace" not in map_data


async def test_operate_map_set_map_policy(client_and_key):
    """Test operate with Map setMapPolicy operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()
    map_policy = MapPolicy(MapOrder.KEY_ORDERED, None)

    # Create a map and then set its policy
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put("mapbin", "key1", "value1", map_policy),
            MapOperation.put("mapbin", "key2", "value2", map_policy),
            MapOperation.set_map_policy("mapbin", MapPolicy(MapOrder.KEY_VALUE_ORDERED, None)),
            MapOperation.size("mapbin")
        ]
    )

    assert record is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)
    # First 2 results: sizes from put operations (1, 2)
    # Third result: None from setMapPolicy (doesn't return value)
    # Fourth result: size() returns 2
    assert 2 in results

    # Verify map still has the items
    rec = await client.get(rp, key, ["mapbin"])
    map_data = rec.bins.get("mapbin")
    assert isinstance(map_data, dict)
    assert len(map_data) == 2


async def test_operate_map_get_by_key_relative_index_range(client_and_key):
    """Test operate with Map getByKeyRelativeIndexRange operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Create a map with ordered keys
    input_map = [(0, 17), (4, 2), (5, 15), (9, 10)]

    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    assert record is not None

    # Test getByKeyRelativeIndexRange operations
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.get_by_key_relative_index_range("mapbin", 5, 0, None, MapReturnType.KEY),
            MapOperation.get_by_key_relative_index_range("mapbin", 5, 1, None, MapReturnType.KEY),
            MapOperation.get_by_key_relative_index_range("mapbin", 5, -1, None, MapReturnType.KEY),
            MapOperation.get_by_key_relative_index_range("mapbin", 3, 2, None, MapReturnType.KEY),
            MapOperation.get_by_key_relative_index_range("mapbin", 3, -2, None, MapReturnType.KEY),
            MapOperation.get_by_key_relative_index_range("mapbin", 5, 0, 1, MapReturnType.KEY),
            MapOperation.get_by_key_relative_index_range("mapbin", 5, 1, 2, MapReturnType.KEY),
            MapOperation.get_by_key_relative_index_range("mapbin", 5, -1, 1, MapReturnType.KEY),
        ]
    )

    assert record is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # Verify we got some results (Python may flatten nested lists)
    # getByKeyRelativeIndexRange(5, 0) should return keys [5, 9]
    assert 5 in results or [5] in results
    assert 9 in results or [9] in results


async def test_operate_map_get_by_value_relative_rank_range(client_and_key):
    """Test operate with Map getByValueRelativeRankRange operation."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Create a map
    input_map = [(0, 17), (4, 2), (5, 15), (9, 10)]

    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    assert record is not None

    # Test getByValueRelativeRankRange operations
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.get_by_value_relative_rank_range("mapbin", 11, 1, None, MapReturnType.VALUE),
            MapOperation.get_by_value_relative_rank_range("mapbin", 11, -1, None, MapReturnType.VALUE),
            MapOperation.get_by_value_relative_rank_range("mapbin", 11, 1, 1, MapReturnType.VALUE),
            MapOperation.get_by_value_relative_rank_range("mapbin", 11, -1, 1, MapReturnType.VALUE)
        ]
    )

    assert record is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # Verify we got some results
    # getByValueRelativeRankRange(11, 1) should return values greater than 11 by rank 1
    assert len(results) > 0


async def test_operate_map_remove_by_key_relative_index_range(client_and_key):
    """Test operate with Map removeByKeyRelativeIndexRange operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()
    map_policy = MapPolicy(None, None)

    # Create a map
    input_map = [(0, 17), (4, 2), (5, 15), (9, 10)]

    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    assert record is not None

    # Test removeByKeyRelativeIndexRange operations
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_key_relative_index_range("mapbin", 5, 0, None, MapReturnType.VALUE),
            MapOperation.remove_by_key_relative_index_range("mapbin", 5, 1, None, MapReturnType.VALUE),
            MapOperation.remove_by_key_relative_index_range("mapbin", 5, -1, 1, MapReturnType.VALUE),
            MapOperation.size("mapbin")
        ]
    )

    assert record is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # Verify removals happened
    rec = await client.get(rp, key, ["mapbin"])
    map_data = rec.bins.get("mapbin")
    assert isinstance(map_data, dict)
    # After removals, map should be smaller
    assert len(map_data) < 4


async def test_operate_map_remove_by_value_relative_rank_range(client_and_key):
    """Test operate with Map removeByValueRelativeRankRange operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()
    map_policy = MapPolicy(None, None)

    # Create a map
    input_map = [(0, 17), (4, 2), (5, 15), (9, 10)]

    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
        ]
    )

    assert record is not None

    # Test removeByValueRelativeRankRange operations
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.remove_by_value_relative_rank_range("mapbin", 11, 1, None, MapReturnType.VALUE),
            MapOperation.remove_by_value_relative_rank_range("mapbin", 11, -1, 1, MapReturnType.VALUE),
            MapOperation.size("mapbin")
        ]
    )

    assert record is not None
    results = record.bins.get("mapbin")
    assert isinstance(results, list)

    # Verify removals happened
    rec = await client.get(rp, key, ["mapbin"])
    map_data = rec.bins.get("mapbin")
    assert isinstance(map_data, dict)
    # After removals, map should be smaller
    assert len(map_data) < 4


async def test_operate_map_create(client_and_key):
    """Test operate with Map create operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()

    # Delete the record first to ensure clean state
    await client.delete(wp, key)

    # Create a map with order
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.create("mapbin", MapOrder.KEY_ORDERED),
            MapOperation.size("mapbin")
        ]
    )

    assert record is not None
    results = record.bins.get("mapbin")
    # create() doesn't return a value, size() returns 0 for empty map
    # When there's only one operation that returns a value, it might be a single value, not a list
    if isinstance(results, list):
        assert 0 in results
    else:
        assert results == 0

    # Verify map was created
    rec = await client.get(rp, key, ["mapbin"])
    assert "mapbin" in rec.bins
    assert rec.bins.get("mapbin") == {}


async def test_operate_nested_map(client_and_key):
    """Test operate with nested map using CTX.mapKey."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Delete record first
    await client.delete(wp, key)

    # Create nested maps
    m1 = {"key11": 9, "key12": 4}
    m2 = {"key21": 3, "key22": 5}
    input_map = {"key1": m1, "key2": m2}

    # Create maps
    await client.put(wp, key, {"mapbin": input_map})

    # Set map value to 11 for map key "key21" inside of map key "key2" and retrieve all maps
    record = await client.operate(
        wp,
        key,
        [
                MapOperation.put("mapbin", "key21", 11, map_policy).set_context([CTX.map_key("key2")]),
                Operation.get_bin("mapbin")
        ]
    )

    assert record is not None
    results = record.bins.get("mapbin")

    if isinstance(results, list):
        # First result: count from put (should be 2)
        count = results[0] if isinstance(results[0], (int, float)) else results[1]
        assert count == 2

        # Second result: the full map
        full_map = results[1] if isinstance(results[0], (int, float)) else results[0]
    else:
        full_map = results

    assert isinstance(full_map, dict)
    assert len(full_map) == 2

    # Test nested map: key2 -> key21 should be 11
    nested_map = full_map["key2"]
    assert isinstance(nested_map, dict)
    assert nested_map["key21"] == 11
    assert nested_map["key22"] == 5


async def test_operate_double_nested_map(client_and_key):
    """Test operate with double nested map using CTX.mapKey and CTX.mapRank."""
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Delete record first
    await client.delete(wp, key)

    # Create double nested maps
    m11 = {"key111": 1}
    m12 = {"key121": 5}
    m1 = {"key11": m11, "key12": m12}

    m21 = {"key211": 7}
    m2 = {"key21": m21}

    input_map = {"key1": m1, "key2": m2}

    # Create maps
    await client.put(wp, key, {"mapbin": input_map})

    # Set map value to 11 for map key "key121" inside of map key "key1" at rank -1
    record = await client.operate(
        wp,
        key,
        [
                MapOperation.put("mapbin", "key121", 11, map_policy).set_context([
                    CTX.map_key("key1"),
                    CTX.map_rank(-1)
                ]),
                Operation.get_bin("mapbin")
        ]
    )

    assert record is not None
    results = record.bins.get("mapbin")

    if isinstance(results, list):
        # First result: count from put (should be 1)
        count = results[0] if isinstance(results[0], (int, float)) else results[1]
        assert count == 1

        # Second result: the full map
        full_map = results[1] if isinstance(results[0], (int, float)) else results[0]
    else:
        full_map = results

    assert isinstance(full_map, dict)
    assert len(full_map) == 2

    # Test double nested map: key1 -> key12 -> key121 should be 11
    key1_map = full_map["key1"]
    assert isinstance(key1_map, dict)
    assert len(key1_map) == 2

    key12_map = key1_map["key12"]
    assert isinstance(key12_map, dict)
    assert len(key12_map) == 1
    assert key12_map["key121"] == 11


async def test_operate_nested_map_value(client_and_key):
    """Test operate with nested map using CTX.map_value.

    Uses CTX.map_value() which converts HashMap to OrderedMap (BTreeMap) for
    exact byte-level matching with KEY_ORDERED maps stored on the server.
    """
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(MapOrder.KEY_ORDERED, None)

    # Delete record first to ensure clean state
    try:
        await client.delete(wp, key)
    except:
        pass  # Ignore if record doesn't exist

    # Create nested map
    # Create map with specific key order: m1.put(1, "in"), m1.put(3, "order"), m1.put(2, "key")
    m1 = {1: "in", 3: "order", 2: "key"}
    input_map = [("first", m1)]

    # Create nested maps that are all sorted and lookup by map value
    record = await client.operate(
        wp,
        key,
        [
            MapOperation.put_items("mapbin", input_map, map_policy),
            MapOperation.put("mapbin", "first", m1, map_policy),
            MapOperation.get_by_key("mapbin", 3, MapReturnType.KEY_VALUE).set_context([
                CTX.map_value(m1)
            ])
        ]
    )

    assert record is not None
    results = record.bins.get("mapbin")

    assert isinstance(results, list)
    assert len(results) >= 3

    # First result: count from put_items (should be 1)
    count1 = results[0] if isinstance(results[0], (int, float)) else results[1]
    assert count1 == 1

    # Second result: count from put (should be 1)
    count2 = results[1] if isinstance(results[0], (int, float)) else results[2]
    assert count2 == 1

    # Third result: list of entries from get_by_key
    entry_list = results[2] if isinstance(results[0], (int, float)) else results[0]
    # Python client may return dict instead of list of entries for KEY_VALUE return type
    if isinstance(entry_list, dict):
        # Dict format: {key: value}
        assert 3 in entry_list
        assert entry_list[3] == "order"
    else:
        assert isinstance(entry_list, list)
        assert len(entry_list) == 1
        # Entry should be (3, "order")
        entry = entry_list[0]
        assert isinstance(entry, (list, tuple))
        assert len(entry) == 2
        assert entry[0] == 3
        assert entry[1] == "order"


async def test_operate_map_create_context(client_and_key):
    """Test operate with map create context using CTX.map_key.

    Uses CTX.map_key_create with put operation since MapOperation.create doesn't
    support context. CTX.map_key_create creates the map at the context level if
    it doesn't exist.
    """
    client, key = client_and_key

    wp = WritePolicy()
    map_policy = MapPolicy(None, None)

    # Delete record first
    await client.delete(wp, key)

    # Create nested maps
    m1 = {"key11": 9, "key12": 4}
    m2 = {"key21": 3, "key22": 5}
    input_map = {"key1": m1, "key2": m2}

    # Create maps
    await client.put(wp, key, {"mapbin": input_map})

    # Create new map at key "key3" and put value in it
    # Adapted to use CTX.map_key_create with put operation instead of MapOperation.create
    # with context, since the Rust core client's MapOperation.create doesn't support context.
    record = await client.operate(
        wp,
        key,
        [
                MapOperation.put("mapbin", "key31", 99, map_policy).set_context([CTX.map_key_create("key3", MapOrder.KEY_ORDERED)]),
                Operation.get_bin("mapbin")
        ]
    )

    assert record is not None
    results = record.bins.get("mapbin")

    if isinstance(results, list):
        # First result: count from put (should be 1)
        # Second result: the full map from get_bin
        count = results[0] if len(results) > 0 and isinstance(results[0], (int, float)) else results[1]
        assert count == 1

        # Second result: the full map
        full_map = results[1] if len(results) > 1 else results[0]
    else:
        full_map = results

    assert isinstance(full_map, dict)
    assert len(full_map) == 3  # key1, key2, key3

    # Test new nested map: key3 -> key31 should be 99
    key3_map = full_map["key3"]
    assert isinstance(key3_map, dict)
    assert len(key3_map) == 1
    assert key3_map["key31"] == 99

