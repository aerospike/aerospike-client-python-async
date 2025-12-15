import pytest
import pytest_asyncio

from aerospike_async import (new_client, ClientPolicy, WritePolicy, ReadPolicy, Key, BitOperation,
                             BitPolicy, BitwiseWriteFlags, BitwiseResizeFlags, BitwiseOverflowActions)
from aerospike_async.exceptions import ServerError, ResultCode


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

async def test_operate_bit_set_and_get(client_and_key):
    """Test operate with Bit set and get operations."""
    client, key = client_and_key

    wp = WritePolicy()
    bit_policy = BitPolicy(None)

    # Delete the record first
    await client.delete(wp, key)

    # Set initial bytes
    initial_bytes = bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
    await client.put(wp, key, {"bitbin": initial_bytes})

    # Set bits: set bit at offset 1, size 1, value 0x80
    bit0 = bytes([0x80])
    record = await client.operate(
        wp,
        key,
        [
            BitOperation.set("bitbin", 1, 1, bit0, bit_policy),
            BitOperation.get("bitbin", 0, 8),
        ]
    )

    assert record is not None
    assert record.bins is not None
    results = record.bins.get("bitbin")

    # Results can be a list or bytes depending on how the client returns them
    if isinstance(results, bytes):
        # Single result (bit_get)
        get_result = results
    elif isinstance(results, list):
        # Multiple results
        get_result = None
        for r in results:
            if isinstance(r, bytes):
                get_result = r
                break
        assert get_result is not None
    else:
        assert False, f"Unexpected result type: {type(results)}"

    # Verify we got bytes back
    assert isinstance(get_result, bytes)
    assert len(get_result) > 0

    # Verify the final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    assert record is not None
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # The bytes should be modified after bit_set
    assert final_bytes != initial_bytes


async def test_operate_bit_bin(client_and_key):
    """Test operate with Bit bin operations (set, remove, insert)."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)
    update_mode = BitPolicy(BitwiseWriteFlags.UpdateOnly)
    add_mode = BitPolicy(BitwiseWriteFlags.CreateOnly)

    # Delete the record first
    await client.delete(wp, key)

    # Test set, remove operations
    initial_bytes = bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
    await client.put(wp, key, {"bitbin": initial_bytes})

    bit0 = bytes([0x80])
    record = await client.operate(
        wp,
        key,
        [
            BitOperation.set("bitbin", 1, 1, bit0, put_mode),
            BitOperation.set("bitbin", 3, 1, bit0, update_mode),
            BitOperation.remove("bitbin", 6, 2, update_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    assert len(final_bytes) == 6  # Removed 2 bytes

    # Test error cases
    # Bin doesn't exist
    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.set("b", 1, 1, bit0, put_mode)])
    assert exi.value.result_code == ResultCode.BIN_NOT_FOUND

    # CREATE_ONLY on existing bin
    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.set("bitbin", 1, 1, bit0, add_mode)])
    assert exi.value.result_code == ResultCode.PARAMETER_ERROR

    # Test insert operation
    await client.delete(wp, key)
    bytes1 = bytes([0x0A])
    record = await client.operate(
        wp,
        key,
        [
            BitOperation.insert("bitbin", 1, bytes1, add_mode),
        ]
    )

    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    assert len(final_bytes) == 2
    assert final_bytes[0] == 0x00
    assert final_bytes[1] == 0x0A


async def test_operate_bit_set(client_and_key):
    """Test operate with Bit set operations."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)
    bit0 = bytes([0x80])
    bits1 = bytes([0x11, 0x22, 0x33])

    # Delete the record first
    await client.delete(wp, key)

    initial_bytes = bytes([0x01, 0x12, 0x02, 0x03, 0x04, 0x05, 0x06,
                          0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
                          0x0E, 0x0F, 0x10, 0x11, 0x41])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.set("bitbin", 1, 1, bit0, put_mode),
            BitOperation.set("bitbin", 15, 1, bit0, put_mode),
            BitOperation.set("bitbin", 16, 24, bits1, put_mode),
            BitOperation.set("bitbin", 40, 22, bits1, put_mode),
            BitOperation.set("bitbin", 73, 21, bits1, put_mode),
            BitOperation.set("bitbin", 100, 20, bits1, put_mode),
            BitOperation.set("bitbin", 120, 17, bits1, put_mode),
            BitOperation.set("bitbin", 144, 1, bit0, put_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # Bytes should be modified
    assert final_bytes != initial_bytes


async def test_operate_bit_lshift(client_and_key):
    """Test operate with Bit left shift operations."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)

    # Delete the record first
    await client.delete(wp, key)

    initial_bytes = bytes([0x01, 0x01, 0x00, 0x80,
                          0xFF, 0x01, 0x01,
                          0x18, 0x01])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.lshift("bitbin", 0, 8, 1, put_mode),
            BitOperation.lshift("bitbin", 9, 7, 6, put_mode),
            BitOperation.lshift("bitbin", 23, 2, 1, put_mode),
            BitOperation.lshift("bitbin", 37, 18, 3, put_mode),
            BitOperation.lshift("bitbin", 58, 2, 1, put_mode),
            BitOperation.lshift("bitbin", 64, 4, 7, put_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # Bytes should be modified after left shifts
    assert final_bytes != initial_bytes


async def test_operate_bit_rshift(client_and_key):
    """Test operate with Bit right shift operations."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)

    # Delete the record first
    await client.delete(wp, key)

    initial_bytes = bytes([0x80, 0x40, 0x01, 0x00,
                          0xFF, 0x01, 0x01,
                          0x18, 0x80])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.rshift("bitbin", 0, 8, 1, put_mode),
            BitOperation.rshift("bitbin", 9, 7, 6, put_mode),
            BitOperation.rshift("bitbin", 23, 2, 1, put_mode),
            BitOperation.rshift("bitbin", 37, 18, 3, put_mode),
            BitOperation.rshift("bitbin", 60, 2, 1, put_mode),
            BitOperation.rshift("bitbin", 68, 4, 7, put_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # Bytes should be modified after right shifts
    assert final_bytes != initial_bytes


async def test_operate_bit_or(client_and_key):
    """Test operate with Bit OR operations."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)
    bits1 = bytes([0x11, 0x22, 0x33])

    # Delete the record first
    await client.delete(wp, key)

    initial_bytes = bytes([0x80, 0x40, 0x01, 0x00, 0x00,
                          0x01, 0x02, 0x03])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            getattr(BitOperation, "or")("bitbin", 0, 5, bits1, put_mode),
            getattr(BitOperation, "or")("bitbin", 9, 7, bits1, put_mode),
            getattr(BitOperation, "or")("bitbin", 23, 6, bits1, put_mode),
            getattr(BitOperation, "or")("bitbin", 32, 8, bits1, put_mode),
            getattr(BitOperation, "or")("bitbin", 40, 24, bits1, put_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # Bytes should be modified after OR operations
    assert final_bytes != initial_bytes


async def test_operate_bit_xor(client_and_key):
    """Test operate with Bit XOR operations."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)
    bits1 = bytes([0x11, 0x22, 0x33])

    # Delete the record first
    await client.delete(wp, key)

    initial_bytes = bytes([0x80, 0x40, 0x01, 0x00, 0x00,
                          0x01, 0x02, 0x03])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.xor("bitbin", 0, 5, bits1, put_mode),
            BitOperation.xor("bitbin", 9, 7, bits1, put_mode),
            BitOperation.xor("bitbin", 23, 6, bits1, put_mode),
            BitOperation.xor("bitbin", 32, 8, bits1, put_mode),
            BitOperation.xor("bitbin", 40, 24, bits1, put_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # Bytes should be modified after XOR operations
    assert final_bytes != initial_bytes


async def test_operate_bit_and(client_and_key):
    """Test operate with Bit AND operations."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)
    bits1 = bytes([0x11, 0x22, 0x33])

    # Delete the record first
    await client.delete(wp, key)

    initial_bytes = bytes([0x80, 0x40, 0x01, 0x00, 0x00,
                          0x01, 0x02, 0x03])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            getattr(BitOperation, "and")("bitbin", 0, 5, bits1, put_mode),
            getattr(BitOperation, "and")("bitbin", 9, 7, bits1, put_mode),
            getattr(BitOperation, "and")("bitbin", 23, 6, bits1, put_mode),
            getattr(BitOperation, "and")("bitbin", 32, 8, bits1, put_mode),
            getattr(BitOperation, "and")("bitbin", 40, 24, bits1, put_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # Bytes should be modified after AND operations
    assert final_bytes != initial_bytes


async def test_operate_bit_not(client_and_key):
    """Test operate with Bit NOT operations."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)

    # Delete the record first
    await client.delete(wp, key)

    initial_bytes = bytes([0x80, 0x40, 0x01, 0x00, 0x00,
                          0x01, 0x02, 0x03])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            getattr(BitOperation, "not")("bitbin", 0, 5, put_mode),
            getattr(BitOperation, "not")("bitbin", 9, 7, put_mode),
            getattr(BitOperation, "not")("bitbin", 23, 6, put_mode),
            getattr(BitOperation, "not")("bitbin", 32, 8, put_mode),
            getattr(BitOperation, "not")("bitbin", 40, 24, put_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # Bytes should be modified after NOT operations
    assert final_bytes != initial_bytes


async def test_operate_bit_add(client_and_key):
    """Test operate with Bit add operations."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)

    # Delete the record first
    await client.delete(wp, key)

    initial_bytes = bytes([0x38, 0x1F, 0x00, 0xE8, 0x7F,
                          0x00, 0x00, 0x00,
                          0x01, 0x01, 0x01,
                          0x01, 0x01, 0x01,
                          0x02, 0x02, 0x02,
                          0x03, 0x03, 0x03])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.add("bitbin", 0, 5, 1, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.add("bitbin", 9, 7, 1, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.add("bitbin", 23, 6, 0x21, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.add("bitbin", 32, 8, 1, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.add("bitbin", 40, 24, 0x7F7F7F, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.add("bitbin", 64, 20, 0x01010, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.add("bitbin", 92, 20, 0x10101, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.add("bitbin", 113, 22, 0x8082, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.add("bitbin", 136, 23, 0x20202, False, BitwiseOverflowActions.Fail, put_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # Bytes should be modified after add operations
    assert final_bytes != initial_bytes

    # Test overflow actions: WRAP and SATURATE
    await client.delete(wp, key)
    initial_bytes = bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.add("bitbin", 0, 8, 0xFF, False, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.add("bitbin", 0, 8, 0xFF, False, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.add("bitbin", 8, 8, 0x7F, True, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.add("bitbin", 8, 8, 0x7F, True, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.add("bitbin", 16, 8, 0x80, True, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.add("bitbin", 16, 8, 0xFF, True, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.add("bitbin", 24, 8, 0x80, False, BitwiseOverflowActions.Saturate, put_mode),
            BitOperation.add("bitbin", 24, 8, 0x80, False, BitwiseOverflowActions.Saturate, put_mode),
            BitOperation.add("bitbin", 32, 8, 0x77, True, BitwiseOverflowActions.Saturate, put_mode),
            BitOperation.add("bitbin", 32, 8, 0x77, True, BitwiseOverflowActions.Saturate, put_mode),
            BitOperation.add("bitbin", 40, 8, 0x8F, True, BitwiseOverflowActions.Saturate, put_mode),
            BitOperation.add("bitbin", 40, 8, 0x8F, True, BitwiseOverflowActions.Saturate, put_mode),
        ]
    )

    # Test overflow FAIL - should raise exception
    await client.delete(wp, key)
    initial_bytes = bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    await client.put(wp, key, {"bitbin": initial_bytes})

    with pytest.raises(ServerError) as exi:
        await client.operate(
            wp,
            key,
            [
                BitOperation.add("bitbin", 0, 8, 0xFF, False, BitwiseOverflowActions.Fail, put_mode),
                BitOperation.add("bitbin", 0, 8, 0xFF, False, BitwiseOverflowActions.Fail, put_mode),
            ]
        )
    # Operation cannot be applied to current bin value
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE


async def test_operate_bit_subtract(client_and_key):
    """Test operate with Bit subtract operations."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)

    # Delete the record first
    await client.delete(wp, key)

    initial_bytes = bytes([0x38, 0x1F, 0x00, 0xE8, 0x7F,
                          0x80, 0x80, 0x80,
                          0x01, 0x01, 0x01,
                          0x01, 0x01, 0x01,
                          0x02, 0x02, 0x02,
                          0x03, 0x03, 0x03])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.subtract("bitbin", 0, 5, 0x01, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.subtract("bitbin", 9, 7, 0x01, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.subtract("bitbin", 23, 6, 0x03, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.subtract("bitbin", 32, 8, 0x01, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.subtract("bitbin", 40, 24, 0x10101, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.subtract("bitbin", 64, 20, 0x101, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.subtract("bitbin", 92, 20, 0x10101, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.subtract("bitbin", 113, 21, 0x101, False, BitwiseOverflowActions.Fail, put_mode),
            BitOperation.subtract("bitbin", 136, 23, 0x11111, False, BitwiseOverflowActions.Fail, put_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # Bytes should be modified after subtract operations
    assert final_bytes != initial_bytes

    # Test overflow actions: WRAP and SATURATE
    await client.delete(wp, key)
    initial_bytes = bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.subtract("bitbin", 0, 8, 0x01, False, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.subtract("bitbin", 8, 8, 0x80, True, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.subtract("bitbin", 8, 8, 0x8A, True, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.subtract("bitbin", 16, 8, 0x7F, True, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.subtract("bitbin", 16, 8, 0x02, True, BitwiseOverflowActions.Wrap, put_mode),
            BitOperation.subtract("bitbin", 24, 8, 0xAA, False, BitwiseOverflowActions.Saturate, put_mode),
            BitOperation.subtract("bitbin", 32, 8, 0x77, True, BitwiseOverflowActions.Saturate, put_mode),
            BitOperation.subtract("bitbin", 32, 8, 0x77, True, BitwiseOverflowActions.Saturate, put_mode),
            BitOperation.subtract("bitbin", 40, 8, 0x81, True, BitwiseOverflowActions.Saturate, put_mode),
            BitOperation.subtract("bitbin", 40, 8, 0x8F, True, BitwiseOverflowActions.Saturate, put_mode),
        ]
    )

    # Test overflow FAIL - should raise exception
    await client.delete(wp, key)
    initial_bytes = bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    await client.put(wp, key, {"bitbin": initial_bytes})

    with pytest.raises(ServerError) as exi:
        await client.operate(
            wp,
            key,
            [
                BitOperation.subtract("bitbin", 0, 8, 1, False, BitwiseOverflowActions.Fail, put_mode),
            ]
        )
    # Operation cannot be applied to current bin value
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE


async def test_operate_bit_set_int(client_and_key):
    """Test operate with Bit setInt operations."""
    client, key = client_and_key

    wp = WritePolicy()
    put_mode = BitPolicy(None)

    # Delete the record first
    await client.delete(wp, key)

    initial_bytes = bytes([0x38, 0x1F, 0x00, 0xE8, 0x7F,
                          0x80, 0x80, 0x80,
                          0x01, 0x01, 0x01,
                          0x01, 0x01, 0x01,
                          0x02, 0x02, 0x02,
                          0x03, 0x03, 0x03])
    await client.put(wp, key, {"bitbin": initial_bytes})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.set_int("bitbin", 0, 5, 0x01, put_mode),
            BitOperation.set_int("bitbin", 9, 7, 0x01, put_mode),
            BitOperation.set_int("bitbin", 23, 6, 0x03, put_mode),
            BitOperation.set_int("bitbin", 32, 8, 0x01, put_mode),
            BitOperation.set_int("bitbin", 40, 24, 0x10101, put_mode),
            BitOperation.set_int("bitbin", 64, 20, 0x101, put_mode),
            BitOperation.set_int("bitbin", 92, 20, 0x10101, put_mode),
            BitOperation.set_int("bitbin", 113, 21, 0x101, put_mode),
            BitOperation.set_int("bitbin", 136, 23, 0x11111, put_mode),
        ]
    )

    # Verify final state
    rp = ReadPolicy()
    record = await client.get(rp, key, ["bitbin"])
    final_bytes = record.bins.get("bitbin")
    assert isinstance(final_bytes, bytes)
    # Bytes should be modified after setInt operations
    assert final_bytes != initial_bytes


async def test_operate_bit_get(client_and_key):
    """Test operate with Bit get operations (read-only)."""
    client, key = client_and_key

    wp = WritePolicy()

    # Delete the record first
    await client.delete(wp, key)

    bytes_data = bytes([0xC1, 0xAA, 0xAA])
    await client.put(wp, key, {"bitbin": bytes_data})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.get("bitbin", 0, 1),
            BitOperation.get("bitbin", 1, 1),
            BitOperation.get("bitbin", 7, 1),
            BitOperation.get("bitbin", 0, 8),
            BitOperation.get("bitbin", 8, 16),
            BitOperation.get("bitbin", 9, 15),
            BitOperation.get("bitbin", 9, 14),
        ]
    )

    assert record is not None
    results = record.bins.get("bitbin")
    assert isinstance(results, list)
    # Should have 7 results
    assert len(results) == 7
    # All results should be bytes
    for r in results:
        assert isinstance(r, bytes)


async def test_operate_bit_count(client_and_key):
    """Test operate with Bit count operations (read-only)."""
    client, key = client_and_key

    wp = WritePolicy()

    # Delete the record first
    await client.delete(wp, key)

    bytes_data = bytes([0xC1, 0xAA, 0xAB])
    await client.put(wp, key, {"bitbin": bytes_data})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.count("bitbin", 0, 1),
            BitOperation.count("bitbin", 1, 1),
            BitOperation.count("bitbin", 7, 1),
            BitOperation.count("bitbin", 0, 8),
            BitOperation.count("bitbin", 8, 16),
            BitOperation.count("bitbin", 9, 15),
            BitOperation.count("bitbin", 9, 14),
        ]
    )

    assert record is not None
    results = record.bins.get("bitbin")
    assert isinstance(results, list)
    # Should have 7 results (counts)
    assert len(results) == 7
    # All results should be integers
    for r in results:
        assert isinstance(r, int)
        assert r >= 0
    # Verify specific counts
    assert results[0] == 1  # First bit
    assert results[1] == 1  # Second bit
    assert results[2] == 1  # Eighth bit
    assert results[3] == 3  # First byte


async def test_operate_bit_lscan(client_and_key):
    """Test operate with Bit lscan operations (read-only)."""
    client, key = client_and_key

    wp = WritePolicy()

    # Delete the record first
    await client.delete(wp, key)

    bytes_data = bytes([0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x01])
    await client.put(wp, key, {"bitbin": bytes_data})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.lscan("bitbin", 0, 1, True),
            BitOperation.lscan("bitbin", 0, 8, True),
            BitOperation.lscan("bitbin", 0, 9, True),
            BitOperation.lscan("bitbin", 0, 32, True),
            BitOperation.lscan("bitbin", 0, 32, False),
            BitOperation.lscan("bitbin", 1, 30, False),
            BitOperation.lscan("bitbin", 32, 40, True),
            BitOperation.lscan("bitbin", 33, 38, True),
            BitOperation.lscan("bitbin", 32, 40, False),
            BitOperation.lscan("bitbin", 33, 38, False),
            BitOperation.lscan("bitbin", 0, 72, True),
            BitOperation.lscan("bitbin", 0, 72, False),
        ]
    )

    assert record is not None
    results = record.bins.get("bitbin")
    assert isinstance(results, list)
    # Should have 12 results (bit positions)
    assert len(results) == 12
    # All results should be integers (bit positions, or -1 if not found)
    for r in results:
        assert isinstance(r, int)
    # First few should find bits (0 or positive)
    assert results[0] >= 0
    assert results[1] >= 0
    assert results[2] >= 0


async def test_operate_bit_rscan(client_and_key):
    """Test operate with Bit rscan operations (read-only)."""
    client, key = client_and_key

    wp = WritePolicy()

    # Delete the record first
    await client.delete(wp, key)

    bytes_data = bytes([0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x01])
    await client.put(wp, key, {"bitbin": bytes_data})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.rscan("bitbin", 0, 1, True),
            BitOperation.rscan("bitbin", 0, 8, True),
            BitOperation.rscan("bitbin", 0, 9, True),
            BitOperation.rscan("bitbin", 0, 32, True),
            BitOperation.rscan("bitbin", 0, 32, False),
            BitOperation.rscan("bitbin", 1, 30, False),
            BitOperation.rscan("bitbin", 32, 40, True),
            BitOperation.rscan("bitbin", 33, 38, True),
            BitOperation.rscan("bitbin", 32, 40, False),
            BitOperation.rscan("bitbin", 33, 38, False),
            BitOperation.rscan("bitbin", 0, 72, True),
            BitOperation.rscan("bitbin", 0, 72, False),
        ]
    )

    assert record is not None
    results = record.bins.get("bitbin")
    assert isinstance(results, list)
    # Should have 12 results (bit positions)
    assert len(results) == 12
    # All results should be integers (bit positions, or -1 if not found)
    for r in results:
        assert isinstance(r, int)
    # First few should find bits (0 or positive)
    assert results[0] >= 0
    assert results[1] >= 0
    assert results[2] >= 0


async def test_operate_bit_get_int(client_and_key):
    """Test operate with Bit getInt operations (read-only)."""
    client, key = client_and_key

    wp = WritePolicy()

    # Delete the record first
    await client.delete(wp, key)

    bytes_data = bytes([0x0F, 0x0F, 0x00])
    await client.put(wp, key, {"bitbin": bytes_data})

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.get_int("bitbin", 4, 4, False),
            BitOperation.get_int("bitbin", 4, 4, True),
            BitOperation.get_int("bitbin", 0, 8, False),
            BitOperation.get_int("bitbin", 0, 8, True),
            BitOperation.get_int("bitbin", 7, 4, False),
            BitOperation.get_int("bitbin", 7, 4, True),
            BitOperation.get_int("bitbin", 8, 16, False),
            BitOperation.get_int("bitbin", 8, 16, True),
            BitOperation.get_int("bitbin", 9, 15, False),
            BitOperation.get_int("bitbin", 9, 15, True),
            BitOperation.get_int("bitbin", 9, 14, False),
            BitOperation.get_int("bitbin", 9, 14, True),
            BitOperation.get_int("bitbin", 5, 17, False),
            BitOperation.get_int("bitbin", 5, 17, True),
        ]
    )

    assert record is not None
    results = record.bins.get("bitbin")
    assert isinstance(results, list)
    # Should have 14 results (integer values)
    assert len(results) == 14
    # All results should be integers
    for r in results:
        assert isinstance(r, int)
    # Verify specific values
    assert results[0] == 15  # 4 bits from offset 4, unsigned
    assert results[2] == 15  # 8 bits from offset 0, unsigned
    assert results[3] == 15  # 8 bits from offset 0, signed


async def test_operate_bit_resize(client_and_key):
    """Test operate with Bit resize operations."""
    client, key = client_and_key

    wp = WritePolicy()
    policy = BitPolicy(None)
    no_fail = BitPolicy(BitwiseWriteFlags.NoFail)

    # Delete the record first
    await client.delete(wp, key)

    record = await client.operate(
        wp,
        key,
        [
            BitOperation.resize("bitbin", 20, BitwiseResizeFlags.Default, policy),
            BitOperation.get("bitbin", 19 * 8, 8),
            BitOperation.resize("bitbin", 10, BitwiseResizeFlags.GrowOnly, no_fail),
            BitOperation.get("bitbin", 19 * 8, 8),
            BitOperation.resize("bitbin", 10, BitwiseResizeFlags.ShrinkOnly, policy),
            BitOperation.get("bitbin", 9 * 8, 8),
            BitOperation.resize("bitbin", 30, BitwiseResizeFlags.ShrinkOnly, no_fail),
            BitOperation.get("bitbin", 9 * 8, 8),
            BitOperation.resize("bitbin", 19, BitwiseResizeFlags.GrowOnly, policy),
            BitOperation.get("bitbin", 18 * 8, 8),
            BitOperation.resize("bitbin", 0, BitwiseResizeFlags.GrowOnly, no_fail),
            BitOperation.resize("bitbin", 0, BitwiseResizeFlags.ShrinkOnly, policy),
        ]
    )

    assert record is not None
    results = record.bins.get("bitbin")
    assert isinstance(results, list)
    # Should have results including get operations
    assert len(results) >= 5
    # Get results should be bytes
    get_results = [r for r in results if isinstance(r, bytes)]
    assert len(get_results) == 5
    # All get results should be 0x00 (empty bytes)
    for r in get_results:
        assert r == bytes([0x00])


async def test_operate_bit_null_blob(client_and_key):
    """Test operate with Bit operations on null/empty blob (error handling)."""
    client, key = client_and_key

    wp = WritePolicy()
    policy = BitPolicy(None)
    buf = bytes([0x80])

    # Delete the record first
    await client.delete(wp, key)

    # Put empty blob
    initial_bytes = bytes([])
    await client.put(wp, key, {"bitbin": initial_bytes})

    # All these operations should fail with ServerError
    # Most operations fail with OP_NOT_APPLICABLE, remove fails with PARAMETER_ERROR
    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.set("bitbin", 0, 1, buf, policy)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [getattr(BitOperation, "or")("bitbin", 0, 1, buf, policy)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.xor("bitbin", 0, 1, buf, policy)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [getattr(BitOperation, "and")("bitbin", 0, 1, buf, policy)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [getattr(BitOperation, "not")("bitbin", 0, 1, policy)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.lshift("bitbin", 0, 1, 1, policy)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.rshift("bitbin", 0, 1, 1, policy)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    # Remove should fail with PARAMETER_ERROR
    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.remove("bitbin", 0, 1, policy)])
    assert exi.value.result_code == ResultCode.PARAMETER_ERROR

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.add("bitbin", 0, 1, 1, False, BitwiseOverflowActions.Fail, policy)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.subtract("bitbin", 0, 1, 1, False, BitwiseOverflowActions.Fail, policy)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.set_int("bitbin", 0, 1, 1, policy)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    # Read operations should also fail with OP_NOT_APPLICABLE
    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.get("bitbin", 0, 1)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.count("bitbin", 0, 1)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.lscan("bitbin", 0, 1, True)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.rscan("bitbin", 0, 1, True)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE

    with pytest.raises(ServerError) as exi:
        await client.operate(wp, key, [BitOperation.get_int("bitbin", 0, 1, False)])
    assert exi.value.result_code == ResultCode.OP_NOT_APPLICABLE


async def test_operate_bit_set_exhaustive(client_and_key):
    """Test exhaustive Bit set operations with various sizes and offsets."""
    client, key = client_and_key

    wp = WritePolicy()
    policy = BitPolicy(None)
    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    # Test with set sizes from 1 to 10 bits
    for set_sz in range(1, 11):
        set_data = bytes([0x00] * ((set_sz + 7) // 8))
        
        # Test various offsets
        for offset in range(0, bin_bit_sz - set_sz + 1, max(1, (bin_bit_sz - set_sz) // 10)):
            await client.delete(wp, key)
            initial = bytes([0xFF] * bin_sz)
            await client.put(wp, key, {"bitbin": initial})
            
            # Set bits and verify
            await client.operate(wp, key, [
                BitOperation.set("bitbin", offset, set_sz, set_data, policy),
                BitOperation.get("bitbin", offset, set_sz),
            ])
            
            rp = ReadPolicy()
            record = await client.get(rp, key, ["bitbin"])
            assert record is not None


async def test_operate_bit_lshift_exhaustive(client_and_key):
    """Test exhaustive Bit left shift operations."""
    client, key = client_and_key

    wp = WritePolicy()
    policy = BitPolicy(None)
    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    # Test with set sizes from 1 to 4 bits
    for set_sz in range(1, 5):
        set_data = bytes([0x00] * ((set_sz + 7) // 8))
        
        # Test various offsets
        for offset in range(0, min(bin_bit_sz - set_sz + 1, 50), 10):
            limit = min(set_sz + 1, 16) if set_sz < 16 else 16
            
            # Test various shift amounts
            for n_bits in range(0, limit + 1, max(1, limit // 5)):
                await client.delete(wp, key)
                initial = bytes([0xFF] * bin_sz)
                await client.put(wp, key, {"bitbin": initial})
                
                # Set bits, then shift
                await client.operate(wp, key, [
                    BitOperation.set("bitbin", offset, set_sz, set_data, policy),
                    BitOperation.lshift("bitbin", offset, set_sz, n_bits, policy),
                ])
                
                rp = ReadPolicy()
                record = await client.get(rp, key, ["bitbin"])
                assert record is not None


async def test_operate_bit_rshift_exhaustive(client_and_key):
    """Test exhaustive Bit right shift operations."""
    client, key = client_and_key

    wp = WritePolicy()
    policy = BitPolicy(None)
    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    # Test with set sizes from 1 to 4 bits
    for set_sz in range(1, 5):
        set_data = bytes([0x00] * ((set_sz + 7) // 8))
        
        # Test various offsets
        for offset in range(0, min(bin_bit_sz - set_sz + 1, 50), 10):
            limit = min(set_sz + 1, 16) if set_sz < 16 else 16
            
            # Test various shift amounts
            for n_bits in range(0, limit + 1, max(1, limit // 5)):
                await client.delete(wp, key)
                initial = bytes([0xFF] * bin_sz)
                await client.put(wp, key, {"bitbin": initial})
                
                # Set bits, then shift
                await client.operate(wp, key, [
                    BitOperation.set("bitbin", offset, set_sz, set_data, policy),
                    BitOperation.rshift("bitbin", offset, set_sz, n_bits, policy),
                ])
                
                rp = ReadPolicy()
                record = await client.get(rp, key, ["bitbin"])
                assert record is not None


async def test_operate_bit_and_exhaustive(client_and_key):
    """Test exhaustive Bit AND operations."""
    client, key = client_and_key

    wp = WritePolicy()
    policy = BitPolicy(None)
    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    # Test with set sizes from 1 to 10 bits
    for set_sz in range(1, 11):
        set_data = bytes([0x00] * ((set_sz + 7) // 8))
        
        # Test various offsets
        for offset in range(0, bin_bit_sz - set_sz + 1, max(1, (bin_bit_sz - set_sz) // 10)):
            await client.delete(wp, key)
            initial = bytes([0xFF] * bin_sz)
            await client.put(wp, key, {"bitbin": initial})
            
            # AND operation
            await client.operate(wp, key, [
                getattr(BitOperation, "and")("bitbin", offset, set_sz, set_data, policy),
            ])
            
            rp = ReadPolicy()
            record = await client.get(rp, key, ["bitbin"])
            assert record is not None


async def test_operate_bit_not_exhaustive(client_and_key):
    """Test exhaustive Bit NOT operations."""
    client, key = client_and_key

    wp = WritePolicy()
    policy = BitPolicy(None)
    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    # Test with set sizes from 1 to 10 bits
    for set_sz in range(1, 11):
        # Test various offsets
        for offset in range(0, bin_bit_sz - set_sz + 1, max(1, (bin_bit_sz - set_sz) // 10)):
            await client.delete(wp, key)
            initial = bytes([0xFF] * bin_sz)
            await client.put(wp, key, {"bitbin": initial})
            
            # NOT operation
            await client.operate(wp, key, [
                getattr(BitOperation, "not")("bitbin", offset, set_sz, policy),
            ])
            
            rp = ReadPolicy()
            record = await client.get(rp, key, ["bitbin"])
            assert record is not None


async def test_operate_bit_insert_exhaustive(client_and_key):
    """Test exhaustive Bit insert operations."""
    client, key = client_and_key

    wp = WritePolicy()
    policy = BitPolicy(None)
    bin_sz = 15

    # Test with insert sizes from 1 to 10 bytes
    for set_sz in range(1, 11):
        set_data = bytes([0x0A] * set_sz)
        
        # Test various byte offsets
        for offset in range(0, bin_sz + 1, max(1, bin_sz // 10)):
            await client.delete(wp, key)
            initial = bytes([0xFF] * bin_sz)
            await client.put(wp, key, {"bitbin": initial})
            
            # Insert operation
            await client.operate(wp, key, [
                BitOperation.insert("bitbin", offset, set_data, policy),
            ])
            
            rp = ReadPolicy()
            record = await client.get(rp, key, ["bitbin"])
            assert record is not None
            # Size should increase
            assert len(record.bins.get("bitbin")) >= bin_sz


async def test_operate_bit_add_exhaustive(client_and_key):
    """Test exhaustive Bit add operations."""
    client, key = client_and_key

    wp = WritePolicy()
    policy = BitPolicy(None)
    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    # Test with set sizes from 1 to 10 bits
    for set_sz in range(1, 11):
        # Test various offsets
        for offset in range(0, bin_bit_sz - set_sz + 1, max(1, (bin_bit_sz - set_sz) // 10)):
            await client.delete(wp, key)
            initial = bytes([0x00] * bin_sz)
            await client.put(wp, key, {"bitbin": initial})
            
            # Add operation with WRAP to avoid overflow errors
            await client.operate(wp, key, [
                BitOperation.add("bitbin", offset, set_sz, 1, False, BitwiseOverflowActions.Wrap, policy),
            ])
            
            rp = ReadPolicy()
            record = await client.get(rp, key, ["bitbin"])
            assert record is not None


async def test_operate_bit_subtract_exhaustive(client_and_key):
    """Test exhaustive Bit subtract operations."""
    client, key = client_and_key

    wp = WritePolicy()
    policy = BitPolicy(None)
    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    # Test with set sizes from 1 to 10 bits
    for set_sz in range(1, 11):
        # Calculate max value for this size
        max_value = (1 << set_sz) - 1
        
        # Test various offsets
        for offset in range(0, bin_bit_sz - set_sz + 1, max(1, (bin_bit_sz - set_sz) // 10)):
            await client.delete(wp, key)
            # Set to max value so subtract won't underflow
            initial = bytes([0xFF] * bin_sz)
            await client.put(wp, key, {"bitbin": initial})
            
            # Subtract operation with WRAP to avoid underflow errors
            await client.operate(wp, key, [
                BitOperation.subtract("bitbin", offset, set_sz, max_value, False, BitwiseOverflowActions.Wrap, policy),
            ])
            
            rp = ReadPolicy()
            record = await client.get(rp, key, ["bitbin"])
            assert record is not None


