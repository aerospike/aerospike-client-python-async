#!/usr/bin/env python3
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
Example of how to use the create_index function from aerospike_async.

This example demonstrates:
1. Creating different types of indexes using direct parameters
2. Using various IndexType and CollectionIndexType options
3. Error handling
4. Async/await usage
"""

import asyncio
import os
from aerospike_async import (
    new_client, Client, ClientPolicy,
    IndexType, CollectionIndexType
)


async def create_index_examples():
    """Demonstrate various ways to create indexes."""

    # Set up client (you'll need to set AEROSPIKE_HOST environment variable)
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client_policy = ClientPolicy()
    client_policy.use_services_alternate = os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "").lower() in ("true", "1")  # Required for connection
    aerospike_client = None

    try:
        # Create client connection
        aerospike_client = await new_client(client_policy, host)
        print("✅ Connected to Aerospike server")

        # Example 1: Create a simple numeric index
        print("\n--- Example 1: Numeric Index ---")
        try:
            await aerospike_client.create_index(
                namespace="test",
                set_name="users",
                bin_name="age",
                index_name="age_idx",
                index_type=IndexType.NUMERIC,
                cit=CollectionIndexType.DEFAULT
            )
            print("✅ Created numeric index on 'age' bin")
        except Exception as e:
            print(f"❌ Failed to create numeric index: {e}")

        # Example 2: Create a string index
        print("\n--- Example 2: String Index ---")
        try:
            await aerospike_client.create_index(
                namespace="test",
                set_name="users",
                bin_name="name",
                index_name="name_idx",
                index_type=IndexType.STRING,
                cit=CollectionIndexType.DEFAULT
            )
            print("✅ Created string index on 'name' bin")
        except Exception as e:
            print(f"❌ Failed to create string index: {e}")

        # Example 3: Create a Geo2DSphere index for location data
        print("\n--- Example 3: Geo2DSphere Index ---")
        try:
            await aerospike_client.create_index(
                namespace="test",
                set_name="locations",
                bin_name="coordinates",
                index_name="geo_idx",
                index_type=IndexType.GEO2D_SPHERE,
                cit=CollectionIndexType.DEFAULT
            )
            print("✅ Created Geo2DSphere index on 'coordinates' bin")
        except Exception as e:
            print(f"❌ Failed to create geo index: {e}")

        # Example 4: Create an index on a list bin
        print("\n--- Example 4: List Index ---")
        try:
            await aerospike_client.create_index(
                namespace="test",
                set_name="products",
                bin_name="tags",
                index_name="tags_list_idx",
                index_type=IndexType.STRING,
                cit=CollectionIndexType.LIST  # Index on list elements
            )
            print("✅ Created list index on 'tags' bin")
        except Exception as e:
            print(f"❌ Failed to create list index: {e}")

        # Example 5: Create an index on map keys
        print("\n--- Example 5: Map Keys Index ---")
        try:
            await aerospike_client.create_index(
                namespace="test",
                set_name="configs",
                bin_name="settings",
                index_name="settings_keys_idx",
                index_type=IndexType.STRING,
                cit=CollectionIndexType.MAP_KEYS  # Index on map keys
            )
            print("✅ Created map keys index on 'settings' bin")
        except Exception as e:
            print(f"❌ Failed to create map keys index: {e}")

        # Example 6: Create an index on map values
        print("\n--- Example 6: Map Values Index ---")
        try:
            await aerospike_client.create_index(
                namespace="test",
                set_name="configs",
                bin_name="settings",
                index_name="settings_values_idx",
                index_type=IndexType.STRING,
                cit=CollectionIndexType.MAP_VALUES  # Index on map values
            )
            print("✅ Created map values index on 'settings' bin")
        except Exception as e:
            print(f"❌ Failed to create map values index: {e}")

        # Example 7: Create multiple indexes in parallel
        print("\n--- Example 7: Parallel Index Creation ---")
        try:
            # Create all indexes in parallel
            tasks = [
                aerospike_client.create_index(
                    namespace="test",
                    set_name="analytics",
                    bin_name="user_id",
                    index_name="user_id_idx",
                    index_type=IndexType.NUMERIC,
                    cit=CollectionIndexType.DEFAULT
                ),
                aerospike_client.create_index(
                    namespace="test",
                    set_name="analytics",
                    bin_name="event_type",
                    index_name="event_type_idx",
                    index_type=IndexType.STRING,
                    cit=CollectionIndexType.DEFAULT
                ),
                aerospike_client.create_index(
                    namespace="test",
                    set_name="analytics",
                    bin_name="timestamp",
                    index_name="timestamp_idx",
                    index_type=IndexType.NUMERIC,
                    cit=CollectionIndexType.DEFAULT
                )
            ]
            await asyncio.gather(*tasks)
            print("✅ Created all analytics indexes in parallel")
        except Exception as e:
            print(f"❌ Failed to create parallel indexes: {e}")

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("Make sure to set AEROSPIKE_HOST environment variable")
        print("Example: export AEROSPIKE_HOST=localhost:3000")

    finally:
        # Clean up
        if 'aerospike_client' in locals() and aerospike_client is not None:
            await aerospike_client.close()
            print("\n✅ Client connection closed")
        else:
            print("\n⚠️ No client to close")


def print_usage_examples():
    """Print additional usage examples and explanations."""
    print("\n" + "="*60)
    print("CREATE_INDEX USAGE EXAMPLES")
    print("="*60)

    print("\n📋 create_index Function Signature:")
    print("""
    await client.create_index(
        namespace: str,                    # Aerospike namespace
        set_name: str,                    # Set name (can be empty string for all sets)
        bin_name: str,                    # Bin name to index
        index_name: str,                  # Unique name for the index
        index_type: IndexType,            # Data type: Numeric, String, or Geo2DSphere
        cit: Optional[CollectionIndexType] # Collection type (optional)
    )
    """)

    print("\n🔢 IndexType Options:")
    print("  • IndexType.NUMERIC     - For integer/float values")
    print("  • IndexType.STRING      - For string values")
    print("  • IndexType.GEO2D_SPHERE - For GeoJSON coordinates")

    print("\n📦 CollectionIndexType Options:")
    print("  • CollectionIndexType.DEFAULT   - For scalar values (default)")
    print("  • CollectionIndexType.LIST      - For list elements")
    print("  • CollectionIndexType.MAP_KEYS   - For map keys")
    print("  • CollectionIndexType.MAP_VALUES - For map values")

    print("\n💡 Common Use Cases:")
    print("  • User age queries: Numeric index on 'age' bin")
    print("  • Name searches: String index on 'name' bin")
    print("  • Location queries: Geo2DSphere index on 'coordinates' bin")
    print("  • Tag filtering: List index on 'tags' bin")
    print("  • Map key lookups: MapKeys index on 'metadata' bin")

    print("\n⚠️  Important Notes:")
    print("  • Index names must be unique across the namespace")
    print("  • Index creation is asynchronous and may take time")
    print("  • Indexes are created on the server, not locally")
    print("  • Use appropriate IndexType for your data")
    print("  • CollectionIndexType is only needed for complex data types")

    print("\n🚀 Quick Examples:")
    print("""
    # Simple numeric index
    await client.create_index("test", "users", "age", "age_idx", IndexType.NUMERIC, None)

    # String index with default collection type
    await client.create_index("test", "users", "name", "name_idx", IndexType.STRING, CollectionIndexType.DEFAULT)

    # List index for tags
    await client.create_index("test", "products", "tags", "tags_idx", IndexType.STRING, CollectionIndexType.LIST)

    # Geo index for coordinates
    await client.create_index("test", "locations", "coords", "geo_idx", IndexType.GEO2D_SPHERE, None)
    """)


if __name__ == "__main__":
    print("🚀 Aerospike Async Create Index Examples")
    print("="*50)

    # Run the examples
    asyncio.run(create_index_examples())

    # Print usage information
    print_usage_examples()
