"""Tests for record expiration/TTL behavior.

Tests actual expiration behavior, not just policy settings.
Exclude slow tests with: pytest -m "not slow"
"""

import asyncio
import pytest
from aerospike_async import Key, WritePolicy, ReadPolicy, Expiration
from fixtures import TestFixtureConnection


@pytest.mark.slow(reason="Tests sleep waiting for TTL expiration")
class TestExpiration(TestFixtureConnection):
    """Test record expiration behavior."""

    async def test_record_expires_after_ttl(self, client):
        """Test that a record expires after its TTL elapses."""
        key = Key("test", "test", "expire_ttl_1")
        wp = WritePolicy()
        wp.expiration = Expiration.seconds(2)

        await client.put(wp, key, {"bin": "expirevalue"})

        # Record should exist before expiration
        exists = await client.exists(ReadPolicy(), key)
        assert exists is True

        # Wait for expiration
        await asyncio.sleep(3)

        # Record should be gone after expiration
        exists = await client.exists(ReadPolicy(), key)
        assert exists is False

    async def test_never_expire_persists(self, client):
        """Test that a record with NEVER_EXPIRE persists beyond normal TTL."""
        key = Key("test", "test", "expire_never_1")
        wp = WritePolicy()
        wp.expiration = Expiration.NEVER_EXPIRE

        await client.put(wp, key, {"bin": "neverexpire"})

        # Record should exist immediately
        exists = await client.exists(ReadPolicy(), key)
        assert exists is True

        # Record should still exist after waiting
        await asyncio.sleep(3)

        exists = await client.exists(ReadPolicy(), key)
        assert exists is True

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_touch_resets_ttl(self, client):
        """Test that touch operation resets record TTL."""
        key = Key("test", "test", "expire_touch_1")
        wp = WritePolicy()
        wp.expiration = Expiration.seconds(2)

        await client.put(wp, key, {"bin": "touchvalue"})

        # Wait part of the TTL
        await asyncio.sleep(1)

        # Touch to reset TTL
        touch_wp = WritePolicy()
        touch_wp.expiration = Expiration.seconds(3)
        await client.touch(touch_wp, key)

        # Wait past original expiration time
        await asyncio.sleep(2)

        # Record should still exist due to touch
        exists = await client.exists(ReadPolicy(), key)
        assert exists is True

        # Wait for new TTL to expire
        await asyncio.sleep(2)

        exists = await client.exists(ReadPolicy(), key)
        assert exists is False

    async def test_update_changes_ttl(self, client):
        """Test that updating a record can change its TTL."""
        key = Key("test", "test", "expire_update_1")

        # Create with short TTL
        wp = WritePolicy()
        wp.expiration = Expiration.seconds(2)
        await client.put(wp, key, {"bin": "original"})

        # Update with longer TTL
        wp2 = WritePolicy()
        wp2.expiration = Expiration.seconds(5)
        await client.put(wp2, key, {"bin": "updated"})

        # Wait past original TTL
        await asyncio.sleep(3)

        # Record should still exist with new value
        exists = await client.exists(ReadPolicy(), key)
        assert exists is True

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_dont_update_preserves_ttl(self, client):
        """Test that DONT_UPDATE expiration preserves existing TTL."""
        key = Key("test", "test", "expire_dontupdate_1")

        # Create with TTL
        wp = WritePolicy()
        wp.expiration = Expiration.seconds(4)
        await client.put(wp, key, {"bin": "original"})

        # Update without changing TTL
        wp2 = WritePolicy()
        wp2.expiration = Expiration.DONT_UPDATE
        await client.put(wp2, key, {"bin": "updated"})

        # Wait and verify record still expires on original schedule
        await asyncio.sleep(2)
        exists = await client.exists(ReadPolicy(), key)
        assert exists is True

        await asyncio.sleep(3)
        exists = await client.exists(ReadPolicy(), key)
        assert exists is False


class TestExpirationMetadata(TestFixtureConnection):
    """Test TTL values in record metadata."""

    async def test_exists_legacy_shows_ttl(self, client):
        """Test that exists_legacy returns TTL information in metadata."""
        key = Key("test", "test", "expire_meta_1")
        wp = WritePolicy()
        wp.expiration = Expiration.seconds(100)
        await client.put(wp, key, {"bin": "value"})

        # exists_legacy returns (key, meta) where meta has ttl
        _, meta = await client.exists_legacy(ReadPolicy(), key)
        assert meta is not None
        assert "ttl" in meta
        assert meta["ttl"] > 0
        assert meta["ttl"] <= 100

        # Cleanup
        await client.delete(WritePolicy(), key)

    async def test_never_expire_ttl_value(self, client):
        """Test TTL value for never-expire records."""
        key = Key("test", "test", "expire_meta_2")
        wp = WritePolicy()
        wp.expiration = Expiration.NEVER_EXPIRE
        await client.put(wp, key, {"bin": "value"})

        _, meta = await client.exists_legacy(ReadPolicy(), key)
        assert meta is not None
        # Never-expire records may have None, -1, or very large TTL depending on server
        ttl = meta["ttl"]
        assert ttl is None or ttl == -1 or ttl > 1000000000

        # Cleanup
        await client.delete(WritePolicy(), key)
