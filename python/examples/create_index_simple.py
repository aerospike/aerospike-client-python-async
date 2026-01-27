#!/usr/bin/env python3
"""
Simple example of using create_index function from aerospike_async.
"""

import asyncio
import os
from aerospike_async import new_client, ClientPolicy, IndexType, CollectionIndexType


async def simple_create_index_example():
    """Simple example of creating an index."""

    # Connect to Aerospike (set AEROSPIKE_HOST environment variable)
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client_policy = ClientPolicy()
    client_policy.use_services_alternate = True  # Required for connection
    client = None

    try:
        # Create client
        client = await new_client(client_policy, host)
        print("Connected to Aerospike")

        # Create a simple numeric index
        await client.create_index(
            namespace="test",           # Your namespace
            set_name="users",           # Your set name
            bin_name="age",             # Bin to index
            index_name="age_index",     # Unique index name
            index_type=IndexType.NUMERIC,  # Data type
            cit=CollectionIndexType.DEFAULT
        )
        print("✅ Index created successfully!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure AEROSPIKE_HOST is set (e.g., export AEROSPIKE_HOST=localhost:3000)")

    finally:
        if 'client' in locals() and client is not None:
            await client.close()


# Quick reference examples
def quick_examples():
    """Quick reference for different index types."""

    print("\n" + "="*50)
    print("QUICK REFERENCE EXAMPLES")
    print("="*50)

    print("\n1. Numeric Index (for age, score, etc.):")
    print("""
    await client.create_index(
        namespace="test",
        set_name="users",
        bin_name="age",
        index_name="age_idx",
        index_type=IndexType.NUMERIC,
        cit=CollectionIndexType.DEFAULT
    )
    """)

    print("\n2. String Index (for names, categories, etc.):")
    print("""
    await client.create_index(
        namespace="test",
        set_name="products",
        bin_name="category",
        index_name="category_idx",
        index_type=IndexType.STRING,
        cit=CollectionIndexType.DEFAULT
    )
    """)

    print("\n3. Geo Index (for location data):")
    print("""
    await client.create_index(
        namespace="test",
        set_name="locations",
        bin_name="coordinates",
        index_name="geo_idx",
        index_type=IndexType.GEO2D_SPHERE,
        cit=CollectionIndexType.DEFAULT
    )
    """)

    print("\n4. List Index (for tags, features, etc.):")
    print("""
    from aerospike_async import CollectionIndexType

    await client.create_index(
        namespace="test",
        set_name="products",
        bin_name="tags",
        index_name="tags_idx",
        index_type=IndexType.STRING,
        cit=CollectionIndexType.LIST
    )
    """)

    print("\n5. Map Keys Index:")
    print("""
    await client.create_index(
        namespace="test",
        set_name="configs",
        bin_name="settings",
        index_name="keys_idx",
        index_type=IndexType.STRING,
        cit=CollectionIndexType.MAP_KEYS
    )
    """)

    print("\n6. Map Values Index:")
    print("""
    await client.create_index(
        namespace="test",
        set_name="configs",
        bin_name="settings",
        index_name="values_idx",
        index_type=IndexType.STRING,
        cit=CollectionIndexType.MAP_VALUES
    )
    """)


if __name__ == "__main__":
    print("🚀 Simple Create Index Example")
    print("="*40)

    # Run the example
    asyncio.run(simple_create_index_example())

    # Show quick reference
    quick_examples()
