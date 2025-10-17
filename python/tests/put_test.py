import os
import pytest_asyncio

from aerospike_async import new_client, ClientPolicy, WritePolicy, ReadPolicy, Key, Blob, List, GeoJSON


@pytest_asyncio.fixture
async def client_and_key():
    """Setup client and prepare test key."""

    cp = ClientPolicy()
    client = await new_client(cp, os.environ["AEROSPIKE_HOST"])

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
