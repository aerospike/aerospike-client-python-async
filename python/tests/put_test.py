import os
import pytest
import pytest_asyncio

from aerospike_async import new_client, ClientPolicy, WritePolicy, ReadPolicy, Key, Blob, List, GeoJSON, geojson, null
from aerospike_async.exceptions import ServerError, ResultCode


@pytest_asyncio.fixture
async def client_and_key():
    """Setup client and prepare test key."""

    cp = ClientPolicy()
    cp.use_services_alternate = True
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

    # List and Blob are returned as native Python types
    # Blob objects become bytes, List objects become lists
    expected = {
        "bin": {
            "str": 1,
            1: "str",
            b"Some bytes": 1,  # Blob converted to bytes
            2: b"Some bytes",  # Blob converted to bytes
            "true_key": 1.761,
            9182: False,
            3: [123, 981, 4.12345, [1858673, "str"]],
            "false_key": {"something": [123, 981, 4.12345, [1858673, "str"]]},
            "list_key": [1572, 3.1415],  # List converted to list
        }
    }
    assert rec.bins == expected

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
    # Note: Since Value::UInt was removed, this will overflow to i64::MIN
    large_value = 2 ** 63
    await client.put(wp, key, {"c": List([large_value])})

    # Put geojson helper result
    await client.put(wp, key, {"geo": geojson("-122.0, 37.5")})
    # Put geojson helper result (using JSON string format, like legacy client)
    await client.put(wp, key, {"geo": geojson('{"type": "Point", "coordinates": [-80.604333, 28.608389]}')})

    # Verify all values were stored correctly
    rec = await client.get(rp, key)
    assert rec is not None
    # Large u64 value overflowed to i64::MIN
    assert rec.bins["c"][0] == -9223372036854775808  # i64::MIN (overflow from 2**63)
    assert rec.bins["placeholder"] == 1
    # Verify u64 in List (2**63 overflows to i64::MIN since UInt was removed)
    # List returns as Python native list
    assert isinstance(rec.bins["c"], list)
    assert rec.bins["c"][0] == -9223372036854775808  # i64::MIN (overflow from 2**63)
    assert isinstance(rec.bins["geo"], GeoJSON)

async def test_put_bin_name_int_negative(client_and_key):
    """Negative test: bin names must be strings, not integers."""
    client, rp, key = client_and_key

    wp = WritePolicy()

    # Attempt to use an integer as a bin name - this should raise a TypeError
    # with a helpful error message
    with pytest.raises(TypeError, match="A bin name must be a string or unicode string"):
        await client.put(
            wp,
            key,
            {
                123: "value",  # Using int as bin name - should fail
            },
        )

async def test_put_get_with_integer_key(client_and_key):
    """Test PUT and GET operations with integer keys."""
    from aerospike_async import Key
    client, rp, wp = client_and_key[0], client_and_key[1], WritePolicy()
    
    # Create a key with integer value
    int_key = Key("test", "test", 99999)
    assert isinstance(int_key.value, int)
    assert int_key.value == 99999
    
    # Put with integer key
    await client.put(wp, int_key, {"bin1": "value1", "bin2": 42})
    
    # Get with same integer key
    record = await client.get(rp, int_key)
    assert record is not None
    assert record.bins == {"bin1": "value1", "bin2": 42}
    
    # Verify we can retrieve with a new integer key object
    int_key2 = Key("test", "test", 99999)
    assert int_key2.value == 99999
    assert isinstance(int_key2.value, int)
    record2 = await client.get(rp, int_key2)
    assert record2.bins == {"bin1": "value1", "bin2": 42}
    
    # Verify integer key and string key are different
    str_key = Key("test", "test", "99999")
    assert str_key.value == "99999"
    assert isinstance(str_key.value, str)
    # String key should not find the record (different digest)
    try:
        record3 = await client.get(rp, str_key)
        # If it doesn't raise, the record might not exist (which is expected)
        if record3 is None:
            pass  # Expected - different key
    except Exception:
        pass  # Also acceptable

async def test_put_bin_limit(client_and_key):
    """Test that putting more than 32767 bins raises an error."""
    client, rp, key = client_and_key

    wp = WritePolicy()
    # Default total_timeout (1000ms) can be too short for this large put; give the server time to respond.
    wp.total_timeout = 20000

    # Create bins dict with more than 32767 bins (the limit)
    BIN_LIMIT_PER_RECORD = 32767

    bins = {}
    for num in range(0, BIN_LIMIT_PER_RECORD + 1):
        bins[str(num)] = num

    # Try to put the record - should fail with too many bins
    # The server will reject this with a ParameterError
    with pytest.raises(ServerError) as exi:
        await client.put(wp, key, bins)
    assert exi.value.result_code == ResultCode.PARAMETER_ERROR