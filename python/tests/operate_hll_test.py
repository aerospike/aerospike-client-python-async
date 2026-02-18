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

"""
HLL (HyperLogLog) operation tests.
"""

import pytest
import math
from fixtures import TestFixtureConnection
from aerospike_async import (
    Key, WritePolicy, ReadPolicy, HllOperation, HLLWriteFlags, Operation
)
from aerospike_async.exceptions import ServerError, ResultCode


async def safe_delete(client, key):
    """Delete a key, ignoring errors if key doesn't exist."""
    try:
        await client.delete(WritePolicy(), key)
    except ServerError:
        pass


class TestHllInit(TestFixtureConnection):
    """Test HLL init operations."""

    async def test_init_basic(self, client):
        """Test basic HLL init with various index_bit_count values."""
        key = Key("test", "test", "hll_init_basic")

        for index_bits in [4, 8, 12, 16]:
            result = await client.operate(WritePolicy(), key, [
                Operation.delete(),
                HllOperation.init("hll", index_bits),
                HllOperation.get_count("hll"),
                HllOperation.describe("hll")
            ])
            assert result.bins["hll"][0] == 0  # count should be 0
            desc = result.bins["hll"][1]
            assert desc[0] == index_bits  # index_bit_count
            assert desc[1] == 0  # min_hash_bit_count

    async def test_init_with_minhash(self, client):
        """Test HLL init with minhash bits."""
        key = Key("test", "test", "hll_init_minhash")

        result = await client.operate(WritePolicy(), key, [
            Operation.delete(),
            HllOperation.init("hll", 8, 16),  # index=8, minhash=16
            HllOperation.describe("hll")
        ])
        desc = result.bins["hll"]
        assert desc[0] == 8
        assert desc[1] == 16

    async def test_init_invalid_index_bits(self, client):
        """Test HLL init fails with invalid index bits."""
        key = Key("test", "test", "hll_init_invalid")
        await safe_delete(client, key)

        # Index bits too low (min is 4)
        with pytest.raises(ServerError) as exc_info:
            await client.operate(WritePolicy(), key, [
                HllOperation.init("hll", 2)
            ])
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR

        # Index bits too high (max is 16)
        with pytest.raises(ServerError) as exc_info:
            await client.operate(WritePolicy(), key, [
                HllOperation.init("hll", 20)
            ])
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR


class TestHllAdd(TestFixtureConnection):
    """Test HLL add operations."""

    async def test_add_basic(self, client):
        """Test basic HLL add."""
        key = Key("test", "test", "hll_add_basic")

        result = await client.operate(WritePolicy(), key, [
            Operation.delete(),
            HllOperation.init("hll", 10),
            HllOperation.add("hll", ["a", "b", "c", "d", "e"]),
            HllOperation.get_count("hll")
        ])
        # add returns number of updates, count returns estimated count
        add_count = result.bins["hll"][0]
        est_count = result.bins["hll"][1]
        assert add_count == 5  # 5 new values added
        assert est_count == 5  # estimated count should be ~5

    async def test_add_duplicates(self, client):
        """Test HLL add with duplicates returns 0 for already-seen values."""
        key = Key("test", "test", "hll_add_dups")

        await client.operate(WritePolicy(), key, [
            Operation.delete(),
            HllOperation.init("hll", 10),
            HllOperation.add("hll", ["a", "b", "c"])
        ])

        # Add same values again
        result = await client.operate(WritePolicy(), key, [
            HllOperation.add("hll", ["a", "b", "c"]),
            HllOperation.get_count("hll")
        ])
        add_count = result.bins["hll"][0]
        assert add_count == 0  # No new values

    async def test_add_creates_bin(self, client):
        """Test HLL add can create bin if index_bit_count provided."""
        key = Key("test", "test", "hll_add_create")

        result = await client.operate(WritePolicy(), key, [
            Operation.delete(),
            HllOperation.add("hll", ["x", "y", "z"], index_bit_count=8),
            HllOperation.describe("hll")
        ])
        desc = result.bins["hll"][1]
        assert desc[0] == 8


class TestHllCount(TestFixtureConnection):
    """Test HLL count operations."""

    async def test_count_empty(self, client):
        """Test count on empty HLL."""
        key = Key("test", "test", "hll_count_empty")

        result = await client.operate(WritePolicy(), key, [
            Operation.delete(),
            HllOperation.init("hll", 8),
            HllOperation.get_count("hll")
        ])
        assert result.bins["hll"] == 0

    async def test_count_accuracy(self, client):
        """Test HLL count accuracy is within expected error bounds."""
        key = Key("test", "test", "hll_count_accuracy")
        n_entries = 1000
        index_bits = 10

        values = [f"key_{i}" for i in range(n_entries)]

        result = await client.operate(WritePolicy(), key, [
            Operation.delete(),
            HllOperation.add("hll", values, index_bit_count=index_bits),
            HllOperation.get_count("hll")
        ])
        count = result.bins["hll"][1]

        # HLL relative error is approximately 1.04/sqrt(2^index_bits)
        relative_error = 1.04 / math.sqrt(2 ** index_bits)
        # Use 6-sigma for high confidence
        error_bound = relative_error * 6

        assert abs(count - n_entries) / n_entries <= error_bound, \
            f"Count {count} not within {error_bound*100}% of {n_entries}"


class TestHllDescribe(TestFixtureConnection):
    """Test HLL describe operations."""

    async def test_describe(self, client):
        """Test describe returns correct parameters."""
        key = Key("test", "test", "hll_describe")

        test_cases = [
            (4, 0),
            (8, 0),
            (8, 8),
            (12, 16),
            (16, 0),
        ]

        for index_bits, minhash_bits in test_cases:
            result = await client.operate(WritePolicy(), key, [
                Operation.delete(),
                HllOperation.init("hll", index_bits, minhash_bits),
                HllOperation.describe("hll")
            ])
            desc = result.bins["hll"]
            assert desc[0] == index_bits, f"Expected index_bits={index_bits}, got {desc[0]}"
            assert desc[1] == minhash_bits, f"Expected minhash_bits={minhash_bits}, got {desc[1]}"


class TestHllRefreshCount(TestFixtureConnection):
    """Test HLL refresh_count operations."""

    async def test_refresh_count(self, client):
        """Test refresh_count updates and returns count."""
        key = Key("test", "test", "hll_refresh")

        result = await client.operate(WritePolicy(), key, [
            Operation.delete(),
            HllOperation.init("hll", 8),
            HllOperation.add("hll", ["a", "b", "c"]),
            HllOperation.refresh_count("hll"),
            HllOperation.get_count("hll")
        ])
        refresh_count = result.bins["hll"][1]
        get_count = result.bins["hll"][2]
        assert refresh_count == get_count


class TestHllFold(TestFixtureConnection):
    """Test HLL fold operations."""

    async def test_fold_down(self, client):
        """Test folding HLL to smaller index_bit_count."""
        key = Key("test", "test", "hll_fold_down")

        # Create HLL with index_bits=10
        values = [f"key_{i}" for i in range(100)]
        result = await client.operate(WritePolicy(), key, [
            Operation.delete(),
            HllOperation.add("hll", values, index_bit_count=10),
            HllOperation.describe("hll")
        ])
        assert result.bins["hll"][1][0] == 10

        # Fold down to index_bits=6
        result = await client.operate(WritePolicy(), key, [
            HllOperation.fold("hll", 6),
            HllOperation.describe("hll"),
            HllOperation.get_count("hll")
        ])
        desc = result.bins["hll"][0]
        count = result.bins["hll"][1]
        assert desc[0] == 6
        # Count should still be approximately correct
        assert count > 80 and count < 120

    async def test_fold_up_fails(self, client):
        """Test folding HLL to larger index_bit_count fails."""
        key = Key("test", "test", "hll_fold_up")

        await client.operate(WritePolicy(), key, [
            Operation.delete(),
            HllOperation.init("hll", 6)
        ])

        # Try to fold up to index_bits=10 - should fail
        with pytest.raises(ServerError) as exc_info:
            await client.operate(WritePolicy(), key, [
                HllOperation.fold("hll", 10)
            ])
        assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE


class TestHllFlags(TestFixtureConnection):
    """Test HLL write flags."""

    async def test_create_only_flag(self, client):
        """Test CREATE_ONLY flag prevents overwrite."""
        key = Key("test", "test", "hll_flag_create")
        await safe_delete(client, key)

        # First init succeeds
        await client.operate(WritePolicy(), key, [
            HllOperation.init("hll", 8, flags=int(HLLWriteFlags.CREATE_ONLY))
        ])

        # Second init fails with CREATE_ONLY
        with pytest.raises(ServerError) as exc_info:
            await client.operate(WritePolicy(), key, [
                HllOperation.init("hll", 8, flags=int(HLLWriteFlags.CREATE_ONLY))
            ])
        assert exc_info.value.result_code == ResultCode.BIN_EXISTS_ERROR

    async def test_update_only_flag(self, client):
        """Test UPDATE_ONLY flag requires existing bin."""
        key = Key("test", "test", "hll_flag_update")
        await safe_delete(client, key)

        # Init fails on non-existent bin with UPDATE_ONLY
        with pytest.raises(ServerError) as exc_info:
            await client.operate(WritePolicy(), key, [
                HllOperation.init("hll", 8, flags=int(HLLWriteFlags.UPDATE_ONLY))
            ])
        assert exc_info.value.result_code == ResultCode.BIN_NOT_FOUND

        # Create bin first
        await client.operate(WritePolicy(), key, [
            HllOperation.init("hll", 8)
        ])

        # Now UPDATE_ONLY succeeds
        await client.operate(WritePolicy(), key, [
            HllOperation.init("hll", 8, flags=int(HLLWriteFlags.UPDATE_ONLY))
        ])

    async def test_no_fail_flag(self, client):
        """Test NO_FAIL flag suppresses errors."""
        key = Key("test", "test", "hll_flag_nofail")
        await safe_delete(client, key)

        # Create bin first
        await client.operate(WritePolicy(), key, [
            HllOperation.init("hll", 8)
        ])

        # CREATE_ONLY | NO_FAIL should not raise on existing bin
        flags = int(HLLWriteFlags.CREATE_ONLY) | int(HLLWriteFlags.NO_FAIL)
        result = await client.operate(WritePolicy(), key, [
            HllOperation.init("hll", 8, flags=flags),
            HllOperation.describe("hll")
        ])
        # Should succeed without raising
        assert result is not None


class TestHllUnion(TestFixtureConnection):
    """Test HLL union operations."""

    async def test_get_union_count(self, client):
        """Test get_union_count estimates union cardinality."""
        key1 = Key("test", "test", "hll_union_1")
        key2 = Key("test", "test", "hll_union_2")
        key_main = Key("test", "test", "hll_union_main")
        index_bits = 10

        # Create first HLL and get the HLL bin value
        values1 = [f"set1_key_{i}" for i in range(100)]
        await safe_delete(client, key1)
        result1 = await client.operate(WritePolicy(), key1, [
            HllOperation.add("hll", values1, index_bit_count=index_bits),
            Operation.get_bin("hll")
        ])
        hll1 = result1.bins["hll"][1]

        # Create second HLL with different values
        values2 = [f"set2_key_{i}" for i in range(100)]
        await safe_delete(client, key2)
        result2 = await client.operate(WritePolicy(), key2, [
            HllOperation.add("hll", values2, index_bit_count=index_bits),
            Operation.get_bin("hll")
        ])
        hll2 = result2.bins["hll"][1]

        # Create main HLL and get union count
        # When only one operation returns a value, result is that value directly
        await safe_delete(client, key_main)
        result = await client.operate(WritePolicy(), key_main, [
            HllOperation.init("hll", index_bits),
            HllOperation.get_union_count("hll", [hll1, hll2])
        ])
        union_count = result.bins["hll"]

        # Union of 100 + 100 disjoint sets should be ~200
        assert union_count > 160 and union_count < 240, \
            f"Union count {union_count} not in expected range"

    async def test_set_union(self, client):
        """Test set_union merges HLLs."""
        key1 = Key("test", "test", "hll_setunion_1")
        key2 = Key("test", "test", "hll_setunion_2")
        key_main = Key("test", "test", "hll_setunion_main")
        index_bits = 10

        # Create two HLLs
        values1 = [f"a_{i}" for i in range(50)]
        values2 = [f"b_{i}" for i in range(50)]

        await safe_delete(client, key1)
        result1 = await client.operate(WritePolicy(), key1, [
            HllOperation.add("hll", values1, index_bit_count=index_bits),
            Operation.get_bin("hll")
        ])
        hll1 = result1.bins["hll"][1]

        await safe_delete(client, key2)
        result2 = await client.operate(WritePolicy(), key2, [
            HllOperation.add("hll", values2, index_bit_count=index_bits),
            Operation.get_bin("hll")
        ])
        hll2 = result2.bins["hll"][1]

        # Set union and get count - result is directly the count int
        await safe_delete(client, key_main)
        result = await client.operate(WritePolicy(), key_main, [
            HllOperation.init("hll", index_bits),
            HllOperation.set_union("hll", [hll1, hll2]),
            HllOperation.get_count("hll")
        ])
        count = result.bins["hll"]
        assert count > 80 and count < 120

    async def test_get_union(self, client):
        """Test get_union returns merged HLL value."""
        key1 = Key("test", "test", "hll_getunion_1")
        key2 = Key("test", "test", "hll_getunion_2")
        key_main = Key("test", "test", "hll_getunion_main")
        index_bits = 8

        values1 = ["x", "y", "z"]
        values2 = ["a", "b", "c"]

        await safe_delete(client, key1)
        result1 = await client.operate(WritePolicy(), key1, [
            HllOperation.add("hll", values1, index_bit_count=index_bits),
            Operation.get_bin("hll")
        ])
        hll1 = result1.bins["hll"][1]

        await safe_delete(client, key2)
        result2 = await client.operate(WritePolicy(), key2, [
            HllOperation.add("hll", values2, index_bit_count=index_bits),
            Operation.get_bin("hll")
        ])
        hll2 = result2.bins["hll"][1]

        # Get union returns an HLL value object
        await safe_delete(client, key_main)
        result = await client.operate(WritePolicy(), key_main, [
            HllOperation.init("hll", index_bits),
            HllOperation.get_union("hll", [hll1, hll2])
        ])
        union_hll = result.bins["hll"]
        assert union_hll is not None
        # HLL values are returned as HLL objects, check it has the expected type name
        assert "HLL" in type(union_hll).__name__


class TestHllSimilarity(TestFixtureConnection):
    """Test HLL similarity and intersection operations."""

    async def test_get_intersect_count(self, client):
        """Test get_intersect_count estimates intersection cardinality."""
        key1 = Key("test", "test", "hll_intersect_1")
        key2 = Key("test", "test", "hll_intersect_2")
        key_main = Key("test", "test", "hll_intersect_main")
        index_bits = 12
        minhash_bits = 16

        # Create two sets with 50% overlap
        common = [f"common_{i}" for i in range(50)]
        unique1 = [f"unique1_{i}" for i in range(50)]
        unique2 = [f"unique2_{i}" for i in range(50)]

        await safe_delete(client, key1)
        result1 = await client.operate(WritePolicy(), key1, [
            HllOperation.add("hll", common + unique1, index_bit_count=index_bits, min_hash_bit_count=minhash_bits),
            Operation.get_bin("hll")
        ])
        hll1 = result1.bins["hll"][1]

        await safe_delete(client, key2)
        result2 = await client.operate(WritePolicy(), key2, [
            HllOperation.add("hll", common + unique2, index_bit_count=index_bits, min_hash_bit_count=minhash_bits),
            Operation.get_bin("hll")
        ])
        hll2 = result2.bins["hll"][1]

        # Get intersection count
        await safe_delete(client, key_main)
        result = await client.operate(WritePolicy(), key_main, [
            HllOperation.add("hll", common + unique1, index_bit_count=index_bits, min_hash_bit_count=minhash_bits),
            HllOperation.get_intersect_count("hll", [hll2])
        ])
        intersect_count = result.bins["hll"][1]

        # Intersection should be ~50 (the common elements)
        # HLL intersection is less accurate, use wider bounds
        assert intersect_count > 20 and intersect_count < 100, \
            f"Intersect count {intersect_count} not in expected range"

    async def test_get_similarity(self, client):
        """Test get_similarity estimates Jaccard similarity."""
        key1 = Key("test", "test", "hll_sim_1")
        key2 = Key("test", "test", "hll_sim_2")
        key_main = Key("test", "test", "hll_sim_main")
        index_bits = 12
        minhash_bits = 20

        # Create two identical sets - similarity should be ~1.0
        values = [f"value_{i}" for i in range(100)]

        await safe_delete(client, key1)
        result1 = await client.operate(WritePolicy(), key1, [
            HllOperation.add("hll", values, index_bit_count=index_bits, min_hash_bit_count=minhash_bits),
            Operation.get_bin("hll")
        ])
        hll1 = result1.bins["hll"][1]

        await safe_delete(client, key2)
        result2 = await client.operate(WritePolicy(), key2, [
            HllOperation.add("hll", values, index_bit_count=index_bits, min_hash_bit_count=minhash_bits),
            Operation.get_bin("hll")
        ])
        hll2 = result2.bins["hll"][1]

        # Get similarity
        await safe_delete(client, key_main)
        result = await client.operate(WritePolicy(), key_main, [
            HllOperation.add("hll", values, index_bit_count=index_bits, min_hash_bit_count=minhash_bits),
            HllOperation.get_similarity("hll", [hll1, hll2])
        ])
        similarity = result.bins["hll"][1]

        # Identical sets should have similarity close to 1.0
        assert similarity > 0.8, f"Similarity {similarity} not close to 1.0"

    async def test_similarity_disjoint_sets(self, client):
        """Test similarity of completely disjoint sets is close to 0."""
        key1 = Key("test", "test", "hll_sim_disjoint_1")
        key2 = Key("test", "test", "hll_sim_disjoint_2")
        key_main = Key("test", "test", "hll_sim_disjoint_main")
        index_bits = 12
        minhash_bits = 20

        values1 = [f"set1_value_{i}" for i in range(100)]
        values2 = [f"set2_value_{i}" for i in range(100)]

        await safe_delete(client, key1)
        result1 = await client.operate(WritePolicy(), key1, [
            HllOperation.add("hll", values1, index_bit_count=index_bits, min_hash_bit_count=minhash_bits),
            Operation.get_bin("hll")
        ])
        hll1 = result1.bins["hll"][1]

        await safe_delete(client, key2)
        result2 = await client.operate(WritePolicy(), key2, [
            HllOperation.add("hll", values2, index_bit_count=index_bits, min_hash_bit_count=minhash_bits),
            Operation.get_bin("hll")
        ])
        hll2 = result2.bins["hll"][1]

        # Get similarity of disjoint sets
        await safe_delete(client, key_main)
        result = await client.operate(WritePolicy(), key_main, [
            HllOperation.add("hll", values1, index_bit_count=index_bits, min_hash_bit_count=minhash_bits),
            HllOperation.get_similarity("hll", [hll2])
        ])
        similarity = result.bins["hll"][1]

        # Disjoint sets should have similarity close to 0
        assert similarity < 0.3, f"Similarity {similarity} not close to 0"


class TestHllMultipleOperations(TestFixtureConnection):
    """Test combining multiple HLL operations."""

    async def test_init_add_count_describe(self, client):
        """Test multiple operations in single operate call."""
        key = Key("test", "test", "hll_multi_ops")

        result = await client.operate(WritePolicy(), key, [
            Operation.delete(),
            HllOperation.init("hll", 10, 0),
            HllOperation.add("hll", ["a", "b", "c", "d", "e"]),
            HllOperation.get_count("hll"),
            HllOperation.refresh_count("hll"),
            HllOperation.describe("hll")
        ])

        results = result.bins["hll"]
        # results[0] = init (no return)
        add_count = results[0]
        get_count = results[1]
        refresh_count = results[2]
        desc = results[3]

        assert add_count == 5
        assert get_count == 5
        assert refresh_count == 5
        assert desc[0] == 10  # index_bit_count
        assert desc[1] == 0   # min_hash_bit_count
