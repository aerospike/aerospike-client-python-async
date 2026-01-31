"""Tests for generation-based optimistic concurrency control.

Tests that generation policies work correctly for concurrent update scenarios.
"""

import pytest
from aerospike_async import Key, WritePolicy, ReadPolicy, GenerationPolicy
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureConnection


class TestGeneration(TestFixtureConnection):
    """Test generation-based concurrency control."""

    async def test_generation_increments_on_write(self, client):
        """Test that generation increments with each write."""
        key = Key("test", "test", "gen_incr_1")

        # Clean up
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        # First write - generation should be 1
        await client.put(WritePolicy(), key, {"bin": "value1"})
        record = await client.get(ReadPolicy(), key)
        assert record.generation == 1

        # Second write - generation should be 2
        await client.put(WritePolicy(), key, {"bin": "value2"})
        record = await client.get(ReadPolicy(), key)
        assert record.generation == 2

        # Third write - generation should be 3
        await client.put(WritePolicy(), key, {"bin": "value3"})
        record = await client.get(ReadPolicy(), key)
        assert record.generation == 3

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_expect_gen_equal_succeeds(self, client):
        """Test EXPECT_GEN_EQUAL succeeds when generation matches."""
        key = Key("test", "test", "gen_equal_1")

        # Clean up and create initial record
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        await client.put(WritePolicy(), key, {"bin": "value1"})
        record = await client.get(ReadPolicy(), key)
        current_gen = record.generation

        # Update with matching generation should succeed
        wp = WritePolicy()
        wp.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
        wp.generation = current_gen

        await client.put(wp, key, {"bin": "value2"})

        # Verify update succeeded
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin"] == "value2"
        assert record.generation == current_gen + 1

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_expect_gen_equal_fails_on_mismatch(self, client):
        """Test EXPECT_GEN_EQUAL fails when generation doesn't match."""
        key = Key("test", "test", "gen_equal_2")

        # Clean up and create initial record
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        await client.put(WritePolicy(), key, {"bin": "value1"})

        # Try to update with wrong generation
        wp = WritePolicy()
        wp.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
        wp.generation = 9999  # Wrong generation

        with pytest.raises(ServerError) as exc_info:
            await client.put(wp, key, {"bin": "value2"})

        assert exc_info.value.result_code == ResultCode.GENERATION_ERROR

        # Verify record was not modified
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin"] == "value1"

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_expect_gen_greater_succeeds(self, client):
        """Test EXPECT_GEN_GREATER succeeds when provided generation is greater."""
        key = Key("test", "test", "gen_greater_1")

        # Clean up and create initial record
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        await client.put(WritePolicy(), key, {"bin": "value1"})
        record = await client.get(ReadPolicy(), key)
        current_gen = record.generation

        # Update with greater generation should succeed
        wp = WritePolicy()
        wp.generation_policy = GenerationPolicy.EXPECT_GEN_GREATER
        wp.generation = current_gen + 1

        await client.put(wp, key, {"bin": "value2"})

        # Verify update succeeded
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin"] == "value2"

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_expect_gen_greater_fails_on_equal(self, client):
        """Test EXPECT_GEN_GREATER fails when generation is equal."""
        key = Key("test", "test", "gen_greater_2")

        # Clean up and create initial record
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        await client.put(WritePolicy(), key, {"bin": "value1"})
        record = await client.get(ReadPolicy(), key)
        current_gen = record.generation

        # Try to update with equal generation (should fail)
        wp = WritePolicy()
        wp.generation_policy = GenerationPolicy.EXPECT_GEN_GREATER
        wp.generation = current_gen

        with pytest.raises(ServerError) as exc_info:
            await client.put(wp, key, {"bin": "value2"})

        assert exc_info.value.result_code == ResultCode.GENERATION_ERROR

        # Verify record was not modified
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin"] == "value1"

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_expect_gen_greater_fails_on_less(self, client):
        """Test EXPECT_GEN_GREATER fails when generation is less."""
        key = Key("test", "test", "gen_greater_3")

        # Clean up and create initial record with multiple writes
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        await client.put(WritePolicy(), key, {"bin": "value1"})
        await client.put(WritePolicy(), key, {"bin": "value2"})
        await client.put(WritePolicy(), key, {"bin": "value3"})

        record = await client.get(ReadPolicy(), key)
        assert record.generation == 3

        # Try to update with lesser generation (should fail)
        wp = WritePolicy()
        wp.generation_policy = GenerationPolicy.EXPECT_GEN_GREATER
        wp.generation = 1

        with pytest.raises(ServerError) as exc_info:
            await client.put(wp, key, {"bin": "value4"})

        assert exc_info.value.result_code == ResultCode.GENERATION_ERROR

        # Verify record was not modified
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin"] == "value3"

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_generation_none_ignores_generation(self, client):
        """Test GenerationPolicy.NONE ignores generation value."""
        key = Key("test", "test", "gen_none_1")

        # Clean up and create initial record
        try:
            await client.delete(WritePolicy(), key)
        except Exception:
            pass

        await client.put(WritePolicy(), key, {"bin": "value1"})

        # Update with NONE policy and wrong generation should still succeed
        wp = WritePolicy()
        wp.generation_policy = GenerationPolicy.NONE
        wp.generation = 9999  # Wrong, but should be ignored

        await client.put(wp, key, {"bin": "value2"})

        # Verify update succeeded
        record = await client.get(ReadPolicy(), key)
        assert record.bins["bin"] == "value2"

        # Cleanup
        await client.delete(WritePolicy(), key)
