import pytest
from aerospike_async import IndexType, CollectionIndexType
from aerospike_async.exceptions import ServerError, AerospikeError
from fixtures import TestFixtureConnection


class TestIndex(TestFixtureConnection):
    """Test index creation and management functionality."""

    async def test_create_string_index(self, client):
        """Test creating a string index."""
        # Clean up any existing index first
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass  # Index might not exist
            
        retval = await client.create_index(
            namespace="test",
            set_name="test",
            bin_name="brand",
            index_name="index_name",
            index_type=IndexType.String,
            cit=CollectionIndexType.Default)
        assert retval is None
        
        # Clean up
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass


    async def test_create_numeric_index(self, client):
        """Test creating a numeric index."""
        # Clean up any existing index first
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass  # Index might not exist
            
        retval = await client.create_index("test", "test", "year", "index_name", IndexType.Numeric, cit=CollectionIndexType.Default)
        assert retval is None
        
        # Clean up
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass


    async def test_create_geo2dsphere_index(self, client):
        """Test creating a geo2dsphere index."""
        # Clean up any existing index first
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass  # Index might not exist
            
        retval = await client.create_index("test", "test", "geojson", "index_name", IndexType.Geo2DSphere, cit=CollectionIndexType.Default)
        assert retval is None
        
        # Clean up
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass


    async def test_create_with_cit(self, client):
        """Test creating an index with collection index type."""
        # Clean up any existing index first
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass  # Index might not exist
            
        retval = await client.create_index("test", "test", "year", "index_name", IndexType.Numeric, cit=CollectionIndexType.Default)
        assert retval is None
        
        # Clean up
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass


    async def test_create_index_fail(self, client):
        """Test that creating duplicate index names fails."""
        # Clean up any existing index first
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass  # Index might not exist
            
        # Create first index
        await client.create_index("test", "test", "brand", "index_name", IndexType.String, cit=CollectionIndexType.Default)

        # Try to create another index with same name should fail
        with pytest.raises(AerospikeError):
            await client.create_index("test", "test", "year", "index_name", IndexType.Numeric, cit=CollectionIndexType.Default)
            
        # Clean up
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass


    async def test_drop_index(self, client):
        """Test dropping an index."""
        # Clean up any existing index first
        try:
            await client.drop_index("test", "test", "index_name")
        except:
            pass  # Index might not exist
            
        # Setup - create an index first
        await client.create_index("test", "test", "brand", "index_name", IndexType.String, cit=CollectionIndexType.Default)

        retval = await client.drop_index(namespace="test", set_name="test", index_name="index_name")
        assert retval is None


    async def test_drop_index_fail(self, client):
        """Test dropping a non-existent index."""
        retval = await client.drop_index(namespace="111", set_name="test1", index_name="index_name1")
        assert retval is None
