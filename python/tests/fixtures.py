import pytest
import os
from aerospike_async import Key, new_client, WritePolicy, ClientPolicy, GeoJSON


class TestFixtureConnection:
    """Base fixture for tests that need a client connection."""

    @pytest.fixture
    async def client(self, aerospike_host):
        """Create a client connection for testing."""
        cp = ClientPolicy()
        cp.use_services_alternate = True
        client = await new_client(cp, aerospike_host)
        yield client
        await client.close()


class TestFixtureCleanDB(TestFixtureConnection):
    """Base fixture for tests that need a clean database."""

    @pytest.fixture
    async def client(self, aerospike_host):
        """Create a client connection and clean the test namespace."""
        cp = ClientPolicy()
        cp.use_services_alternate = True
        client = await new_client(cp, aerospike_host)
        
        # Clean the test namespace
        try:
            await client.truncate("test", "test")
        except Exception:
            # Truncate may fail due to permissions or server config, continue anyway
            pass
        
        yield client
        await client.close()

    @pytest.fixture
    def key(self):
        """Create a test key."""
        return Key("test", "test", 1)

    @pytest.fixture
    def key_invalid_primary_key(self):
        """Create a key with invalid primary key."""
        return Key("test", "test", 0)

    @pytest.fixture
    def key_invalid_namespace(self):
        """Create a key with invalid namespace."""
        return Key("test1", "test", 1)


class TestFixtureInsertRecord(TestFixtureCleanDB):
    """Base fixture for tests that need a record inserted in the database."""

    @pytest.fixture
    def original_bin_val(self):
        """Return the original bin values that were inserted."""
        return {
            "brand": "Ford",
            "model": "Mustang",
            "year": 1964,
            "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
            "mileage": 100000.1,
            "bytearray": bytearray(b'123'),
            "bytes": b'123',
            "geojson": GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')
        }

    @pytest.fixture
    async def client(self, key, original_bin_val, aerospike_host):
        """Create a client connection and insert a test record."""
        cp = ClientPolicy()
        cp.use_services_alternate = True
        client = await new_client(cp, aerospike_host)
        
        # Clean the test namespace - ignore errors if truncate fails
        try:
            await client.truncate("test", "test", before_nanos=0)
        except Exception:
            # Truncate may fail due to permissions or server config, continue anyway
            pass
        
        # Insert test record
        wp = WritePolicy()
        await client.put(wp, key, original_bin_val)
        
        yield client
        await client.close()
