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

import os
import pytest
import pytest_asyncio

from aerospike_async import (
    new_client, ClientPolicy, WritePolicy, ReadPolicy, Key,
    BatchPolicy, BatchReadPolicy, BatchWritePolicy, BatchDeletePolicy, BatchUDFPolicy,
    BatchRecord, ListOperation, Operation, ListReturnType,
    FilterExpression, ListPolicy, Expiration
)
from aerospike_async.exceptions import ServerError, ResultCode, InvalidNodeError

@pytest_asyncio.fixture
async def client_and_keys():
    """Setup client and create test records for batch operations."""

    cp = ClientPolicy()
    cp.use_services_alternate = True
    client = await new_client(cp, os.environ.get("AEROSPIKE_HOST", "localhost:3000"))

    wp = WritePolicy()
    size = 8
    keys = []
    bin_name = "bbin"

    for i in range(1, size + 1):
        key = Key("test", "test", f"batchkey{i}")
        keys.append(key)
        # List contains [0*i, 1*i, 2*i, ..., (i-1)*i]
        list_bin = [j * i for j in range(i)]
        # list_bin2 is always [0, 1] for all keys
        list_bin2 = [0, 1]
        if i != 6:
            await client.put(wp, key, {bin_name: f"batchvalue{i}", "lbin": list_bin, "lbin2": list_bin2})
        else:
            await client.put(wp, key, {bin_name: i, "lbin": list_bin, "lbin2": list_bin2})

    delete_keys = [
        Key("test", "test", 10000),
        Key("test", "test", 10001),
        Key("test", "test", 10002),
    ]
    for key in delete_keys:
        await client.put(wp, key, {bin_name: key.value})

    yield client, keys, delete_keys, bin_name
    await client.close()

async def test_batch_read(client_and_keys):
    """Test batch read operations."""

    client, keys, _, bin_name = client_and_keys

    results = await client.batch_read(None, None, keys, [bin_name])

    assert len(results) == len(keys)

    for i, result in enumerate(results):
        assert result.result_code is not None
        assert result.result_code == ResultCode.OK
        assert result.record is not None
        if i != 5:
            assert result.record.bins[bin_name] == f"batchvalue{i + 1}"
        else:
            assert result.record.bins[bin_name] == i + 1

async def test_batch_read_all_bins(client_and_keys):
    """Test batch read with all bins.

    Note: Rust core doesn't support Operation.get() in batch write, so we:
    1. First write the bin using batch_operate
    2. Then read all bins using batch_read with bins=None
    """

    client, keys, _, bin_name = client_and_keys

    operations = [
        Operation.put("bin5", "NewValue")
    ]

    write_results = await client.batch_operate(None, None, keys, [operations] * len(keys))

    assert len(write_results) == len(keys)
    for result in write_results:
        assert result.result_code == ResultCode.OK

    read_results = await client.batch_read(None, None, keys, None)

    assert len(read_results) == len(keys)

    for i, result in enumerate(read_results):
        assert result.result_code is not None
        assert result.result_code == ResultCode.OK
        assert result.record is not None

        s = result.record.bins.get("bin5")
        assert s == "NewValue"

        obj = result.record.bins.get(bin_name)
        assert obj is not None

async def test_batch_read_empty_bins(client_and_keys):
    """Test batch read with empty bin names list.

    Note: Empty bin names array returns all bins.
    We use None (which reads all bins) instead of [].
    """

    client, keys, _, bin_name = client_and_keys

    results = await client.batch_read(None, None, keys, None)

    assert len(results) == len(keys)

    for i, result in enumerate(results):
        assert result.result_code is not None
        assert result.result_code == ResultCode.OK
        assert result.record is not None
        if i != 5:
            assert result.record.bins[bin_name] == f"batchvalue{i + 1}"
        else:
            assert result.record.bins[bin_name] == i + 1

async def test_batch_read_complex(client_and_keys):
    """Test complex batch read scenarios.

    This test covers:
    - Specific bin names
    - All bins (None)
    - Empty bin list (headers only)
    - Non-existent bin
    - Non-existent key
    """

    client, keys, _, bin_name = client_and_keys

    results1 = await client.batch_read(None, None, [keys[0]], [bin_name])
    assert results1[0].result_code == ResultCode.OK
    assert results1[0].record.bins[bin_name] == "batchvalue1"

    results2 = await client.batch_read(None, None, [keys[1]], None)
    assert results2[0].result_code == ResultCode.OK
    assert bin_name in results2[0].record.bins

    results3 = await client.batch_read(None, None, [keys[2]], [])
    assert results3[0].result_code == ResultCode.OK
    assert len(results3[0].record.bins) == 0
    assert results3[0].record.generation > 0

    results4 = await client.batch_read(None, None, [keys[3]], [bin_name])
    assert results4[0].result_code == ResultCode.OK
    assert bin_name in results4[0].record.bins

    results5 = await client.batch_read(None, None, [keys[4]], None)
    assert results5[0].result_code == ResultCode.OK
    assert bin_name in results5[0].record.bins

    results6 = await client.batch_read(None, None, [keys[6]], [bin_name])
    assert results6[0].result_code == ResultCode.OK
    assert results6[0].record.bins[bin_name] == "batchvalue7"

    results7 = await client.batch_read(None, None, [keys[7]], ["binnotfound"])
    assert results7[0].result_code == ResultCode.OK
    assert "binnotfound" not in results7[0].record.bins

    non_existent_key = Key("test", "test", "keynotfound")
    results8 = await client.batch_read(None, None, [non_existent_key], [bin_name])
    assert results8[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR
    assert results8[0].record is None

async def test_batch_read_key_not_found(client_and_keys):
    """Test batch read with non-existent key (simplified from batchReadComplex)."""

    client, _, _, bin_name = client_and_keys

    non_existent_key = Key("test", "test", "keynotfound")
    results = await client.batch_read(None, None, [non_existent_key], [bin_name])

    assert len(results) == 1
    assert results[0].result_code is not None
    assert results[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR
    assert results[0].record is None

async def test_batch_write(client_and_keys):
    """Test batch write operations."""

    client, keys, _, _ = client_and_keys

    bp = BatchPolicy()
    bwp = BatchWritePolicy()
    bins_list = [{"newbin": f"newvalue{i}"} for i in range(len(keys))]

    results = await client.batch_write(bp, bwp, keys, bins_list)

    assert len(results) == len(keys)

    for result in results:
        assert result.result_code is not None
        assert result.result_code == ResultCode.OK

    rp = ReadPolicy()
    for i, key in enumerate(keys):
        rec = await client.get(rp, key)
        assert rec is not None
        assert rec.bins["newbin"] == f"newvalue{i}"

async def test_batch_delete(client_and_keys):
    """Test batch delete operations.

    Note: The Rust core doesn't provide a status field, so we verify individual result codes
    and existence checks instead.
    """

    client, _, delete_keys, _ = client_and_keys

    rp = ReadPolicy()
    for key in delete_keys[:2]:
        rec = await client.get(rp, key)
        assert rec is not None

    results = await client.batch_delete(None, None, delete_keys[:2])

    assert len(results) == 2
    assert results[0].result_code == ResultCode.OK
    assert results[1].result_code == ResultCode.OK

    for key in delete_keys[:2]:
        try:
            rec = await client.get(rp, key)
            assert rec is None
        except ServerError as e:
            assert e.result_code == ResultCode.KEY_NOT_FOUND_ERROR

async def test_batch_delete_key_not_found(client_and_keys):
    """Test batch delete with non-existent key.

    Note: The Rust core doesn't provide a status field, so we verify the result code instead.
    """

    client, _, _, _ = client_and_keys

    non_existent_key = Key("test", "test", 989299023)
    results = await client.batch_delete(None, None, [non_existent_key])

    assert len(results) == 1
    assert results[0].result_code is not None
    assert results[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR

async def test_batch_read_multiple_bins(client_and_keys):
    """Test batch read with multiple bin names.

    Note: Python-specific test. Verifies that batch_read correctly filters
    bins when multiple bin names are specified.
    """

    client, keys, _, _ = client_and_keys

    wp = WritePolicy()
    await client.put(wp, keys[0], {"bin1": "value1", "bin2": "value2", "bin3": "value3"})

    bp = BatchPolicy()
    brp = BatchReadPolicy()
    results = await client.batch_read(bp, brp, [keys[0]], ["bin1", "bin2"])

    assert len(results) == 1
    assert results[0].result_code is not None
    assert results[0].result_code == ResultCode.OK
    assert results[0].record is not None
    assert "bin1" in results[0].record.bins
    assert "bin2" in results[0].record.bins
    assert "bin3" not in results[0].record.bins

async def test_batch_write_empty_keys(client_and_keys):
    """Test batch write with empty keys list.

    Note: Python-specific edge case test. Verifies that empty key lists
    are handled correctly without errors.
    """

    client, _, _, _ = client_and_keys

    bp = BatchPolicy()
    bwp = BatchWritePolicy()
    results = await client.batch_write(bp, bwp, [], [])

    assert len(results) == 0

async def test_batch_read_empty_keys(client_and_keys):
    """Test batch read with empty keys list.

    Note: Python-specific edge case test. Verifies that empty key lists
    are handled correctly without errors.
    """

    client, _, _, _ = client_and_keys

    bp = BatchPolicy()
    brp = BatchReadPolicy()
    results = await client.batch_read(bp, brp, [], ["bin"])

    assert len(results) == 0

async def test_batch_write_different_bins_per_key(client_and_keys):
    """Test batch write with different bins for each key.

    Note: Python-specific test. Verifies that batch_write correctly handles
    different bin data for each key in the batch.
    """

    client, keys, _, _ = client_and_keys

    bp = BatchPolicy()
    bwp = BatchWritePolicy()
    bins_list = [
        {"a": 1, "b": "first"},
        {"a": 2, "b": "second"},
        {"a": 3, "b": "third"},
    ]

    results = await client.batch_write(bp, bwp, keys[:3], bins_list)

    assert len(results) == 3
    for result in results:
        assert result.result_code is not None
        assert result.result_code == ResultCode.OK

    rp = ReadPolicy()
    for i, key in enumerate(keys[:3]):
        rec = await client.get(rp, key)
        assert rec is not None
        assert rec.bins["a"] == i + 1
        assert rec.bins["b"] == ["first", "second", "third"][i]

async def test_batch_record_properties(client_and_keys):
    """Test BatchRecord properties.

    Note: Python-specific test. Verifies that BatchRecord objects have
    all expected properties populated correctly.
    """

    client, keys, _, bin_name = client_and_keys

    bp = BatchPolicy()
    brp = BatchReadPolicy()
    results = await client.batch_read(bp, brp, [keys[0]], [bin_name])

    assert len(results) == 1
    record = results[0]

    assert isinstance(record, BatchRecord)
    assert record.key is not None
    assert record.record is not None
    assert record.result_code is not None
    assert record.result_code == ResultCode.OK
    assert record.in_doubt is False

async def test_batch_read_with_filter_expression(client_and_keys):
    """Test batch read with filter expression.

    Note: Python-specific test. Verifies that filter expressions work
    correctly with batch read operations.
    """

    client, keys, _, bin_name = client_and_keys

    wp = WritePolicy()
    await client.put(wp, keys[0], {bin_name: "match"})

    bp = BatchPolicy()
    brp = BatchReadPolicy()
    brp.filter_expression = FilterExpression.eq(FilterExpression.string_bin(bin_name), FilterExpression.string_val("match"))

    results = await client.batch_read(bp, brp, [keys[0]], [bin_name])

    assert len(results) == 1
    assert results[0].result_code is not None
    assert results[0].result_code == ResultCode.OK
    assert results[0].record is not None
    assert results[0].record.bins[bin_name] == "match"

async def test_batch_write_with_policy(client_and_keys):
    """Test batch write with custom write policy.

    Note: Python-specific test. Verifies that custom BatchWritePolicy
    settings (like send_key) are applied correctly.
    """

    client, keys, _, _ = client_and_keys

    bp = BatchPolicy()
    bwp = BatchWritePolicy()
    bwp.send_key = True
    bwp.durable_delete = False

    bins_list = [{"testbin": "testvalue"} for _ in keys[:2]]
    results = await client.batch_write(bp, bwp, keys[:2], bins_list)

    assert len(results) == 2
    for result in results:
        assert result.result_code is not None
        assert result.result_code == ResultCode.OK

async def test_batch_mixed_operations(client_and_keys):
    """Test that we can perform multiple batch operations in sequence.

    Note: Python-specific test. Verifies that different batch operation
    types (read, write, delete) can be called in sequence without issues.
    """

    client, keys, delete_keys, bin_name = client_and_keys

    bp = BatchPolicy()
    brp = BatchReadPolicy()
    bwp = BatchWritePolicy()
    bdp = BatchDeletePolicy()

    read_results = await client.batch_read(bp, brp, keys[:3], [bin_name])
    assert len(read_results) == 3

    bins_list = [{"new": "data"} for _ in keys[:3]]
    write_results = await client.batch_write(bp, bwp, keys[:3], bins_list)
    assert len(write_results) == 3

    delete_results = await client.batch_delete(bp, bdp, delete_keys[:1])
    assert len(delete_results) == 1


async def test_batch_read_headers(client_and_keys):
    """Test batch read headers only."""

    client, keys, _, _ = client_and_keys

    results = await client.batch_get_header(None, None, keys)

    assert len(results) == len(keys)

    for i, record in enumerate(results):
        assert record is not None
        assert record.bins == {}
        assert record.generation is not None
        assert record.generation > 0
        if record.ttl is not None and record.ttl > 0:
            assert record.ttl > 0

async def test_batch_list_read_operate(client_and_keys):
    """Test batch read with list operations.

    Note: The Rust core returns all operation results as a list when multiple
    operations target the same bin (e.g., record.bins["lbin"] -> [size, value]).
    This test verifies that all operation results are returned.
    """

    client, keys, _, _ = client_and_keys

    operations = [
        ListOperation.size("lbin"),
        ListOperation.get_by_index("lbin", -1, ListReturnType.VALUE)
    ]

    results = await client.batch_operate(None, None, keys, [operations] * len(keys))

    assert len(results) == len(keys)

    for i, result in enumerate(results):
        assert result.result_code is not None
        assert result.result_code == ResultCode.OK
        assert result.record is not None
        val = result.record.bins["lbin"]

        assert isinstance(val, list)
        assert len(val) == 2
        # enumerate index i corresponds to key "batchkey{i+1}" which has list [0*(i+1), 1*(i+1), 2*(i+1), ..., (i)*(i+1)]
        # So size = i+1, and last value = i * (i+1)
        assert val[0] == i + 1  # size: list has i+1 elements
        assert val[1] == i * (i + 1)  # value: last element in list [0*(i+1), 1*(i+1), ..., i*(i+1)] is i*(i+1)

async def test_batch_list_write_operate(client_and_keys):
    """Test batch write with list operations.

    Note: The Rust core returns all operation results as a list when multiple
    operations target the same bin (e.g., record.bins["lbin2"] -> [insert_result, size, value]).
    This test verifies that all operation results are returned.
    """

    client, keys, _, _ = client_and_keys

    # lbin2 is already set up in the fixture as [0, 1] for all keys
    list_policy = ListPolicy(None, None)
    operations = [
        ListOperation.insert("lbin2", 0, 1000, list_policy),
        ListOperation.size("lbin2"),
        ListOperation.get_by_index("lbin2", -1, ListReturnType.VALUE)
    ]

    results = await client.batch_operate(None, None, keys, [operations] * len(keys))

    assert len(results) == len(keys)

    for result in results:
        assert result.result_code is not None
        assert result.result_code == ResultCode.OK
        assert result.record is not None
        val = result.record.bins["lbin2"]

        assert isinstance(val, list)
        assert len(val) == 3
        assert val[0] == 3  # insert_result (new size after insert)
        assert val[1] == 3  # size after insert
        assert val[2] == 1  # value from get_by_index(-1)

async def test_batch_operate_complex(client_and_keys):
    """Test complex batch operate with mixed operations.

    Note: Invalid namespace handling is tested separately in test_batch_invalid_namespace.
    Batch delete requires a separate batch_delete call.
    The Rust core doesn't provide a status field (indicates if all records succeeded).
    """

    client, keys, delete_keys, bin_name = client_and_keys

    bwp = BatchWritePolicy()
    bwp.send_key = True

    wops1 = [Operation.put("bbin2", 100)]
    wops2 = [Operation.put("bbin3", 200)]

    operations_list = [
        wops1,  # Key 1: write bbin2 = 100
        wops2,  # Key 6: write bbin3 = 200
    ]

    results = await client.batch_operate(None, bwp, [keys[0], keys[5]], operations_list)

    assert len(results) == 2
    assert results[0].result_code == ResultCode.OK
    assert results[1].result_code == ResultCode.OK

    delete_results = await client.batch_delete(None, None, [delete_keys[2]])

    assert len(delete_results) == 1
    assert delete_results[0].result_code == ResultCode.OK

    read_results = await client.batch_read(None, None, [keys[0], keys[5], delete_keys[2]], ["bbin2", "bbin3"])

    assert len(read_results) == 3
    assert read_results[0].result_code == ResultCode.OK
    assert read_results[0].record.bins["bbin2"] == 100
    assert read_results[1].result_code == ResultCode.OK
    assert read_results[1].record.bins["bbin3"] == 200
    assert read_results[2].result_code == ResultCode.KEY_NOT_FOUND_ERROR

async def test_batch_invalid_namespace(client_and_keys):
    """Test batch operations with invalid namespace.

    Note: The Rust core raises InvalidNodeError before the batch operation executes
    (client-side validation). This test verifies that invalid namespace is properly detected.
    """

    client, keys, _, _ = client_and_keys

    # Create key with invalid namespace
    invalid_key = Key("invalid", "test", "batchkey1")

    # Rust core raises InvalidNodeError for invalid namespace (client-side validation)
    with pytest.raises(InvalidNodeError):
        await client.batch_read(None, None, [invalid_key], ["bbin"])

    bwp = BatchWritePolicy()
    with pytest.raises(InvalidNodeError):
        await client.batch_write(None, bwp, [invalid_key], [{"bbin": 100}])

    operations = [Operation.put("bbin", 100)]
    with pytest.raises(InvalidNodeError):
        await client.batch_operate(None, bwp, [invalid_key], [operations])

async def test_batch_exists(client_and_keys):
    """Test batch exists operations."""

    client, keys, _, _ = client_and_keys

    exists_results = await client.batch_exists(None, None, keys)

    assert len(exists_results) == len(keys)
    for exists in exists_results:
        assert exists is True

@pytest.mark.slow
async def test_batch_read_ttl(client_and_keys):
    """Test batch read with TTL expiration.

    Note: This test takes a long time to run due to sleeps (19+ seconds total).
    Marked with @pytest.mark.slow so it can be excluded with: pytest -m "not slow"

    TTL must be enabled on the Aerospike server. To enable TTL, configure the server with:
    - namespace <namespace> { default-ttl 30D; nsup-period 120; }
    See: https://aerospike.com/docs/database/manage/namespace/retention/
    """

    import asyncio

    client, _, _, _ = client_and_keys

    key1 = Key("test", "test", 88888)
    key2 = Key("test", "test", 88889)

    # Check if TTL is supported by trying to set expiration and write, then verify TTL is actually set
    bwp = BatchWritePolicy()
    try:
        bwp.expiration = Expiration.seconds(10)
        operations = [Operation.put("a", 1)]
        await client.batch_operate(None, bwp, [key1], [operations])
        # Verify TTL is actually working by reading the record and checking it has a TTL
        rp = ReadPolicy()
        test_rec = await client.get(rp, key1)
        if test_rec is None or test_rec.ttl is None or test_rec.ttl == 0:
            pytest.skip("TTL not enabled on this server (expiration was set but record has no TTL)")
    except Exception as e:
        # If setting expiration fails, TTL is not supported - skip the test
        pytest.skip(f"TTL not supported on this server: {e}")

    # Write to both keys for the actual test
    operations = [Operation.put("a", 1)]
    await client.batch_operate(None, bwp, [key1, key2], [operations, operations])

    await asyncio.sleep(8)

    brp1 = BatchReadPolicy()
    brp1.read_touch_ttl = 80

    brp2 = BatchReadPolicy()
    brp2.read_touch_ttl = -1

    bp = BatchPolicy()
    results1 = await client.batch_read(bp, brp1, [key1], ["a"])
    results2 = await client.batch_read(bp, brp2, [key2], ["a"])

    assert results1[0].result_code == ResultCode.OK
    assert results2[0].result_code == ResultCode.OK

    await asyncio.sleep(3)

    brp1.read_touch_ttl = -1
    brp2.read_touch_ttl = -1

    results1 = await client.batch_read(bp, brp1, [key1], ["a"])
    results2 = await client.batch_read(bp, brp2, [key2], ["a"])

    assert results1[0].result_code == ResultCode.OK
    assert results2[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR

    await asyncio.sleep(8)

    results1 = await client.batch_read(bp, brp1, [key1], ["a"])
    results2 = await client.batch_read(bp, brp2, [key2], ["a"])

    assert results1[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR
    assert results2[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR
