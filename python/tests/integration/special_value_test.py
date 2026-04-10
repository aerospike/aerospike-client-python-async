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

from aerospike_async import (
    new_client, ClientPolicy, WritePolicy, ReadPolicy, Key,
    MapOperation, MapPolicy, MapReturnType,
    ListOperation, ListPolicy, ListReturnType,
    SpecialValue,
)


@pytest_asyncio.fixture
async def client_and_key(aerospike_host):
    cp = ClientPolicy()
    cp.use_services_alternate = True
    client = await new_client(cp, aerospike_host)
    key = Key("test", "test", "sv_test")
    wp = WritePolicy()
    await client.delete(wp, key)
    yield client, key, wp
    await client.delete(wp, key)
    await client.close()


# ---------------------------------------------------------------------------
# 1. CDT range operations with SpecialValue (happy-path)
# ---------------------------------------------------------------------------

async def test_map_get_by_key_range_null_to_infinity(client_and_key):
    """NULL-to-INFINITY key range returns all map entries."""
    client, key, wp = client_and_key
    mp = MapPolicy(None, None)

    await client.operate(wp, key, [
        MapOperation.put("m", 1, "a", mp),
        MapOperation.put("m", 2, "b", mp),
        MapOperation.put("m", 3, "c", mp),
    ])

    record = await client.operate(wp, key, [
        MapOperation.get_by_key_range(
            "m", SpecialValue.NULL, SpecialValue.INFINITY, MapReturnType.KEY,
        ),
    ])
    keys = record.bins["m"]
    assert sorted(keys) == [1, 2, 3]


async def test_map_get_by_key_range_partial_to_infinity(client_and_key):
    """Partial key range (2 to INFINITY) returns keys >= 2."""
    client, key, wp = client_and_key
    mp = MapPolicy(None, None)

    await client.operate(wp, key, [
        MapOperation.put("m", 1, "a", mp),
        MapOperation.put("m", 2, "b", mp),
        MapOperation.put("m", 3, "c", mp),
    ])

    record = await client.operate(wp, key, [
        MapOperation.get_by_key_range(
            "m", 2, SpecialValue.INFINITY, MapReturnType.KEY,
        ),
    ])
    keys = record.bins["m"]
    assert sorted(keys) == [2, 3]


async def test_map_get_by_value_range_null_to_infinity(client_and_key):
    """NULL-to-INFINITY value range returns all map values."""
    client, key, wp = client_and_key
    mp = MapPolicy(None, None)

    await client.operate(wp, key, [
        MapOperation.put("m", "x", 10, mp),
        MapOperation.put("m", "y", 20, mp),
        MapOperation.put("m", "z", 30, mp),
    ])

    record = await client.operate(wp, key, [
        MapOperation.get_by_value_range(
            "m", SpecialValue.NULL, SpecialValue.INFINITY, MapReturnType.VALUE,
        ),
    ])
    values = record.bins["m"]
    assert sorted(values) == [10, 20, 30]


async def test_list_get_by_value_range_null_to_infinity(client_and_key):
    """NULL-to-INFINITY value range returns all list elements."""
    client, key, wp = client_and_key

    await client.put(wp, key, {"lst": [10, 20, 30]})

    record = await client.operate(wp, key, [
        ListOperation.get_by_value_range(
            "lst", SpecialValue.NULL, SpecialValue.INFINITY, ListReturnType.VALUE,
        ),
    ])
    values = record.bins["lst"]
    assert sorted(values) == [10, 20, 30]


async def test_list_get_by_value_range_partial_to_infinity(client_and_key):
    """Partial value range (25 to INFINITY) returns list values >= 25."""
    client, key, wp = client_and_key

    await client.put(wp, key, {"lst": [10, 20, 30, 40]})

    record = await client.operate(wp, key, [
        ListOperation.get_by_value_range(
            "lst", 25, SpecialValue.INFINITY, ListReturnType.VALUE,
        ),
    ])
    values = record.bins["lst"]
    assert sorted(values) == [30, 40]


async def test_map_get_by_value_list_wildcard(client_and_key):
    """WILDCARD in a value list matches all map entries."""
    client, key, wp = client_and_key
    mp = MapPolicy(None, None)

    await client.operate(wp, key, [
        MapOperation.put("m", "a", 1, mp),
        MapOperation.put("m", "b", 2, mp),
        MapOperation.put("m", "c", 3, mp),
    ])

    record = await client.operate(wp, key, [
        MapOperation.get_by_value_list(
            "m", [SpecialValue.WILDCARD], MapReturnType.KEY,
        ),
    ])
    keys = record.bins["m"]
    assert sorted(keys) == ["a", "b", "c"]


async def test_null_equivalent_to_none(client_and_key):
    """SpecialValue.NULL and Python None produce identical key-range results."""
    client, key, wp = client_and_key
    mp = MapPolicy(None, None)

    await client.operate(wp, key, [
        MapOperation.put("m", 1, "a", mp),
        MapOperation.put("m", 2, "b", mp),
        MapOperation.put("m", 3, "c", mp),
    ])

    rec_null = await client.operate(wp, key, [
        MapOperation.get_by_key_range(
            "m", SpecialValue.NULL, 3, MapReturnType.KEY,
        ),
    ])

    rec_none = await client.operate(wp, key, [
        MapOperation.get_by_key_range(
            "m", None, 3, MapReturnType.KEY,
        ),
    ])

    assert sorted(rec_null.bins["m"]) == sorted(rec_none.bins["m"])


# ---------------------------------------------------------------------------
# 2. SpecialValue round-trip fidelity
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    reason="Server rejects SpecialValue sentinels as stored data values "
           "(ParameterError); sentinels are wire-protocol markers for CDT "
           "range queries only",
    raises=Exception,
    strict=True,
)
async def test_infinity_survives_list_round_trip(client_and_key):
    """INFINITY stored inside a CDT list should read back as SpecialValue.INFINITY."""
    client, key, wp = client_and_key
    rp = ReadPolicy()
    lp = ListPolicy(None, None)

    await client.operate(wp, key, [
        ListOperation.append("lst", 1, lp),
        ListOperation.append("lst", SpecialValue.INFINITY, lp),
        ListOperation.append("lst", "x", lp),
    ])

    record = await client.get(rp, key, ["lst"])
    lst = record.bins["lst"]
    assert lst[1] is SpecialValue.INFINITY


@pytest.mark.xfail(
    reason="Server rejects SpecialValue sentinels as stored data values "
           "(ParameterError); sentinels are wire-protocol markers for CDT "
           "range queries only",
    raises=Exception,
    strict=True,
)
async def test_wildcard_survives_list_round_trip(client_and_key):
    """WILDCARD stored inside a CDT list should read back as SpecialValue.WILDCARD."""
    client, key, wp = client_and_key
    rp = ReadPolicy()
    lp = ListPolicy(None, None)

    await client.operate(wp, key, [
        ListOperation.append("lst", 1, lp),
        ListOperation.append("lst", SpecialValue.WILDCARD, lp),
        ListOperation.append("lst", "x", lp),
    ])

    record = await client.get(rp, key, ["lst"])
    lst = record.bins["lst"]
    assert lst[1] is SpecialValue.WILDCARD


# ---------------------------------------------------------------------------
# 3. Known core bug: xfail
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    reason="Rust core panics at value.rs:254 when serializing SpecialValue "
           "as a top-level bin value; should return an error instead",
    raises=Exception,
    strict=True,
)
async def test_put_special_value_as_bin_panics(client_and_key):
    """Storing a SpecialValue sentinel as a top-level bin value triggers a core panic."""
    client, key, wp = client_and_key
    await client.put(wp, key, {"bad": SpecialValue.INFINITY})
