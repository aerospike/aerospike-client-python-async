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

import asyncio
import pytest
import pytest_asyncio

from aerospike_async import new_client, ClientPolicy, WritePolicy, ReadPolicy, Key, Operation, Expiration
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

async def test_operate_put_and_get(client_and_key):
    """Test operate with Put and Get operations."""
    client, key = client_and_key

    wp = WritePolicy()

    # Write initial record
    await client.put(wp, key, {
        "optintbin": 7,
        "optstringbin": "string value"
    })

    # Use operate to put a new value and get the record
    # This is similar to: client.operate(null, key, Operation.put(bin4), Operation.get())
    record = await client.operate(
        wp,
        key,
        [
            Operation.put("optstringbin", "new string"),
            Operation.get()
        ]
    )

    # Verify the record was returned
    assert record is not None
    assert record.bins is not None

    # Verify the new string value was written
    assert record.bins.get("optstringbin") == "new string"

    # Verify the original int bin is still there
    assert record.bins.get("optintbin") == 7


async def test_operate_get_only(client_and_key):
    """Test operate with Get operation only."""
    client, key = client_and_key

    wp = WritePolicy()

    # Write initial record
    await client.put(wp, key, {
        "bin1": "value1",
        "bin2": 42
    })

    # Use operate to get the record
    record = await client.operate(
        wp,
        key,
        [Operation.get()]
    )

    # Verify the record was returned
    assert record is not None
    assert record.bins is not None
    assert record.bins.get("bin1") == "value1"
    assert record.bins.get("bin2") == 42


async def test_operate_multiple_puts(client_and_key):
    """Test operate with multiple Put operations."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()

    # Write initial record
    await client.put(wp, key, {
        "bin1": "original1",
        "bin2": "original2"
    })

    # Use operate to put multiple bins and get the record
    record = await client.operate(
        wp,
        key,
        [
            Operation.put("bin1", "updated1"),
            Operation.put("bin2", "updated2"),
            Operation.get()
        ]
    )

    # Verify the record was returned
    assert record is not None
    assert record.bins is not None

    # Verify both bins were updated
    assert record.bins.get("bin1") == "updated1"
    assert record.bins.get("bin2") == "updated2"

    # Verify by reading the record again
    rec = await client.get(rp, key)
    assert rec.bins.get("bin1") == "updated1"
    assert rec.bins.get("bin2") == "updated2"


async def test_operate_add_and_put(client_and_key):
    """Test operate with Add and Put operations."""
    client, key = client_and_key

    wp = WritePolicy()

    # Write initial record
    await client.put(wp, key, {
        "optintbin": 7,
        "optstringbin": "string value"
    })

    # Add integer, write new string and read record
    record = await client.operate(
        wp,
        key,
        [
            Operation.add("optintbin", 4),
            Operation.put("optstringbin", "new string"),
            Operation.get()
        ]
    )

    # Verify the record was returned
    assert record is not None
    assert record.bins is not None

    # Verify the integer was added (7 + 4 = 11)
    assert record.bins.get("optintbin") == 11

    # Verify the new string value was written
    assert record.bins.get("optstringbin") == "new string"


async def test_operate_add_and_get(client_and_key):
    """Test operate with Add and Get operations."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()

    # Delete record first to ensure clean state
    await client.delete(wp, key)

    # Perform some adds
    await client.add(wp, key, {"addbin": 10})
    await client.add(wp, key, {"addbin": 5})

    # Verify the result
    rec = await client.get(rp, key, ["addbin"])
    assert rec.bins.get("addbin") == 15

    # Test add and get combined
    record = await client.operate(
        wp,
        key,
        [
            Operation.add("addbin", 30),
            Operation.get_bin("addbin")
        ]
    )

    # Verify the result (15 + 30 = 45)
    assert record is not None
    assert record.bins is not None
    assert record.bins.get("addbin") == 45


async def test_operate_append(client_and_key):
    """Test operate with Append operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()

    # Delete record first to ensure clean state
    await client.delete(wp, key)

    # Append operations
    await client.append(wp, key, {"appendbin": "Hello"})
    await client.append(wp, key, {"appendbin": " World"})

    # Verify the result
    rec = await client.get(rp, key, ["appendbin"])
    assert rec.bins.get("appendbin") == "Hello World"

    # Test append in operate
    record = await client.operate(
        wp,
        key,
        [
            Operation.append("appendbin", "!"),
            Operation.get_bin("appendbin")
        ]
    )

    # Verify the result
    assert record is not None
    assert record.bins is not None
    assert record.bins.get("appendbin") == "Hello World!"


async def test_operate_prepend(client_and_key):
    """Test operate with Prepend operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()

    # Delete record first to ensure clean state
    await client.delete(wp, key)

    # Prepend operations
    await client.prepend(wp, key, {"prependbin": "World"})
    await client.prepend(wp, key, {"prependbin": "Hello "})

    # Verify the result
    rec = await client.get(rp, key, ["prependbin"])
    assert rec.bins.get("prependbin") == "Hello World"

    # Test prepend in operate
    record = await client.operate(
        wp,
        key,
        [
            Operation.prepend("prependbin", "Say: "),
            Operation.get_bin("prependbin")
        ]
    )

    # Verify the result
    assert record is not None
    assert record.bins is not None
    assert record.bins.get("prependbin") == "Say: Hello World"


async def test_operate_get_header(client_and_key):
    """Test operate with GetHeader operation."""
    client, key = client_and_key

    wp = WritePolicy()

    # Write initial record
    await client.put(wp, key, {
        "mybin": "myvalue"
    })

    # Use operate to get header (metadata only, no bin data)
    record = await client.operate(
        wp,
        key,
        [Operation.get_header()]
    )

    # Verify the record was returned
    assert record is not None
    # GetHeader should return no bins (metadata only)
    assert record.bins is None or len(record.bins) == 0
    # Generation should be greater than zero
    assert record.generation is not None
    assert record.generation > 0


async def test_operate_delete(client_and_key):
    """Test operate with Delete operation."""
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()

    # Write initial record
    await client.put(wp, key, {
        "optintbin1": 1
    })

    # Read bin1 and then delete all
    record = await client.operate(
        wp,
        key,
        [
            Operation.get_bin("optintbin1"),
            Operation.delete()
        ]
    )

    # Verify we got the bin value before deletion
    assert record is not None
    assert record.bins is not None
    assert record.bins.get("optintbin1") == 1

    # Verify record is gone
    exists = await client.exists(rp, key)
    assert not exists

    # Rewrite record with two bins
    await client.put(wp, key, {
        "optintbin1": 1,
        "optintbin2": 2
    })

    # Read bin 1 and then delete all followed by a write of bin2
    record = await client.operate(
        wp,
        key,
        [
            Operation.get_bin("optintbin1"),
            Operation.delete(),
            Operation.put("optintbin2", 2),
            Operation.get_bin("optintbin2")
        ]
    )

    # Verify we got bin1 value before deletion
    assert record is not None
    assert record.bins is not None
    assert record.bins.get("optintbin1") == 1

    # Read record - should only have bin2
    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.bins.get("optintbin2") == 2
    # bin1 should be gone
    assert "optintbin1" not in rec.bins or rec.bins.get("optintbin1") is None
    # Should only have one bin
    assert len(rec.bins) == 1


async def test_operate_touch_and_get_header(client_and_key):
    """Test operate with Touch and GetHeader operations.

    If TTL is not available on the server, the test will be skipped.
    """
    client, key = client_and_key

    wp = WritePolicy()
    rp = ReadPolicy()

    # Check if TTL is supported by trying to set expiration
    try:
        wp.expiration = Expiration.seconds(60)
        await client.put(wp, key, {
            "touchbin": "touchvalue"
        })
    except Exception as e:
        # If setting expiration fails, TTL is not supported - skip the test
        pytest.skip(f"TTL not supported on this server: {e}")

    # Verify TTL was actually set on the record
    rp_check = ReadPolicy()
    check_rec = await client.get(rp_check, key, ["touchbin"])
    if check_rec.ttl == 0:
        pytest.skip(f"TTL not being applied by server (got ttl=0)")

    wp.expiration = Expiration.seconds(120)
    record = await client.operate(
        wp,
        key,
        [
            Operation.touch(),
            Operation.get_header()
        ]
    )

    assert record is not None
    # GetHeader should return no bins (metadata only)
    assert record.bins is None or len(record.bins) == 0
    # TTL should be set (expiration > 0)
    assert record.ttl is not None
    assert record.ttl > 0, f"Expected TTL > 0 after touch with expiration=120s, got {record.ttl}"

    rec = await client.get(rp, key, ["touchbin"])
    assert rec is not None
    assert rec.bins.get("touchbin") == "touchvalue"
    assert rec.ttl > 0, f"Record TTL should be > 0, got {rec.ttl}"


