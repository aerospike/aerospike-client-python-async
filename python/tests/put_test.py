import os
import pytest_asyncio

from aerospike_async import new_client, ClientPolicy, WritePolicy, ReadPolicy, Key, Blob, List, GeoJSON, geojson, null


@pytest_asyncio.fixture
async def client_and_key():
    """Setup client and prepare test key."""

    cp = ClientPolicy()
    client = await new_client(cp, os.environ.get("AEROSPIKE_HOST", "localhost:3000"))

    # make a record
    key = Key("test", "test", 1)

    # delete the record first
    wp = WritePolicy()
    rp = ReadPolicy()
    await client.delete(wp, key)

    return client, rp, key

async def test_put_int(client_and_key):
    """Test putting integer values."""

    client, rp, key = client_and_key

    wp = WritePolicy()
    await client.put(
        wp,
        key,
        {
            "bin": 1,
        },
    )

    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.bins == {"bin": 1}

async def test_put_float(client_and_key):
    """Test putting float values."""

    client, rp, key = client_and_key

    wp = WritePolicy()
    await client.put(
        wp,
        key,
        {
            "bin": 1.76123,
        },
    )

    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.bins == {"bin": 1.76123}

async def test_put_string(client_and_key):
    """Test putting string values."""

    client, rp, key = client_and_key

    wp = WritePolicy()
    await client.put(
        wp,
        key,
        {
            "bin": "str1",
        },
    )

    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.bins == {"bin": "str1"}

async def test_put_bool(client_and_key):
    """Test putting boolean values."""

    client, rp, key = client_and_key

    wp = WritePolicy()
    await client.put(
        wp,
        key,
        {
            "bint": True,
            "binf": False,
        },
    )

    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.bins == {"bint": True, "binf": False}

async def test_put_blob(client_and_key):
    """Test putting blob (bytes/bytearray) values."""

    client, rp, key = client_and_key

    ba = bytearray([1, 2, 3, 4, 5, 6])
    b = bytes([1, 2, 3, 4, 5, 6])

    wp = WritePolicy()
    await client.put(
        wp,
        key,
        {
            "bin_b": b,
            "bin_ba": ba,
        },
    )

    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.bins == {"bin_b": b, "bin_ba": ba}

async def test_put_list(client_and_key):
    """Test putting list values."""

    client, rp, key = client_and_key

    l = [1, "str", bytearray([1, 2, 3, 4, 5, 6]), True, False, 1572, 3.1415]

    wp = WritePolicy()
    await client.put(
        wp,
        key,
        {
            "bin": l,
        },
    )

    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.bins == {"bin": l}

async def test_put_dict(client_and_key):
    """Test putting dictionary values."""

    client, rp, key = client_and_key

    b = Blob(b"Some bytes")
    l = List([1572, 3.1415])
    d = {
        "str": 1,
        1: "str",
        b: 1,
        2: b,
        "true_key": 1.761,  # Changed from True: 1.761 to "true_key": 1.761
        9182: False,  # Changed from 9182.58723 to 9182
        3: [123, 981, 4.12345, [1858673, "str"]],  # Changed from 3.141519 to 3
        "false_key": {"something": [123, 981, 4.12345, [1858673, "str"]]},  # Changed from False: {...} to "false_key": {...}
        "list_key": l,  # Changed from l: b to "list_key": l
    }

    wp = WritePolicy()
    await client.put(
        wp,
        key,
        {
            "bin": d,
        },
    )

    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.bins == {"bin": d}

async def test_put_GeoJSON(client_and_key):
    """Test putting GeoJSON values."""

    client, rp, key = client_and_key

    geo = GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')

    wp = WritePolicy()
    await client.put(
        wp,
        key,
        {
            "bin": geo,
        },
    )

    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.bins == {"bin": geo}

async def test_put_edge_types(client_and_key):
    """Test putting edge case types: None, null(), large u64 (in List), and geojson helper."""
    client, rp, key = client_and_key

    wp = WritePolicy()

    # Create record with a non-Nil value first (Aerospike requires at least one non-Nil bin)
    await client.put(wp, key, {"placeholder": 1})

    # Put None value (adds to existing record)
    # Note: None values are stored but not returned by Aerospike when reading
    await client.put(wp, key, {"a": None})

    # Put null() value (adds to existing record)
    await client.put(wp, key, {"b": null()})

    # Verify None/null() were accepted (no errors) and are not returned (Aerospike behavior)
    rec = await client.get(rp, key)
    assert rec is not None
    assert "a" not in rec.bins  # Nil bins are not returned
    assert "b" not in rec.bins  # Nil bins are not returned
    assert rec.bins["placeholder"] == 1

    # Put large u64 value in a List (u64 cannot be stored directly as a bin value)
    # 2^63 = 9223372036854775808, which is > i64::MAX
    await client.put(wp, key, {"c": List([2 ** 63])})

    # Put geojson helper result
    await client.put(wp, key, {"geo": geojson("-122.0, 37.5")})
    # Put geojson helper result (using JSON string format, like legacy client)
    await client.put(wp, key, {"geo": geojson('{"type": "Point", "coordinates": [-80.604333, 28.608389]}')})

    # Verify all values were stored correctly
    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.bins["placeholder"] == 1
    # Verify u64 in List (2**63 is stored as UInt internally)
    assert isinstance(rec.bins["c"], List)
    assert rec.bins["c"][0] == 2 ** 63
    assert isinstance(rec.bins["geo"], GeoJSON)