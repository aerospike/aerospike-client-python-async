#!/usr/bin/env python3
"""
Simple example of using Statement for queries with aerospike_async.
"""

import asyncio
import os
from aerospike_async import new_client, ClientPolicy, QueryPolicy, PartitionFilter, Statement, Filter, CollectionIndexType


async def simple_statement_example():
    """Simple example of using Statement for queries."""

    # Connect to Aerospike (set AEROSPIKE_HOST environment variable)
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client_policy = ClientPolicy()
    client_policy.use_services_alternate = True  # Required for connection
    query_policy = QueryPolicy()

    try:
        # Create client
        client = await new_client(client_policy, host)
        print("✅ Connected to Aerospike")

        print("\n--- Basic Statement (no filters) ---")
        # Create a basic statement for querying all records in a namespace/set
        statement = Statement("test", "users", ["name", "age"])  # namespace, set, bins

        # Execute query
        recordset = await client.query(
            policy=query_policy,
            partition_filter=PartitionFilter.all(),
            statement=statement
        )

        record_count = 0
        async for record in recordset:
            record_count += 1
            print(f"Record {record_count}: {record.bins}")

            if record_count >= 3:
                print("... (showing first 3 records)")
                break

        print(f"Total records queried: {record_count}")

        print("\n--- Statement with Range Filter ---")
        # Create a statement with a range filter (requires secondary index)
        try:
            # Filter for age between 18 and 65
            age_filter = Filter.range("age", 18, 65)

            statement_with_filter = Statement("test", "users", ["name", "age"])
            statement_with_filter.filters = [age_filter]  # Set the filter

            # Execute query with filter
            recordset = await client.query(
                policy=query_policy,
                partition_filter=PartitionFilter.all(),
                statement=statement_with_filter
            )

            record_count = 0
            async for record in recordset:
                record_count += 1
                print(f"Filtered Record {record_count}: {record.bins}")

                if record_count >= 3:
                    print("... (showing first 3 records)")
                    break

            print(f"Total filtered records: {record_count}")

        except Exception as e:
            print(f"⚠️  Filter query failed (likely no secondary index): {e}")
            print("   Note: Range filters require a secondary index on the 'age' bin")

        print("\n--- Statement with Contains Filter ---")
        # Create a statement with a contains filter (for list/map bins)
        try:
            # Filter for records containing "admin" in a list bin
            contains_filter = Filter.contains("roles", "admin", CollectionIndexType.LIST)

            statement_with_contains = Statement("test", "users", ["name", "roles"])
            statement_with_contains.filters = [contains_filter]

            # Execute query with contains filter
            recordset = await client.query(
                policy=query_policy,
                partition_filter=PartitionFilter.all(),
                statement=statement_with_contains
            )

            record_count = 0
            async for record in recordset:
                record_count += 1
                print(f"Contains Filter Record {record_count}: {record.bins}")

                if record_count >= 3:
                    print("... (showing first 3 records)")
                    break

            print(f"Total contains filtered records: {record_count}")

        except Exception as e:
            print(f"⚠️  Contains filter query failed: {e}")
            print("   Note: Contains filters require a secondary index on the 'roles' bin")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure AEROSPIKE_HOST is set (e.g., export AEROSPIKE_HOST=localhost:3000)")

    finally:
        if 'client' in locals() and client is not None:
            await client.close()


def show_statement_usage():
    """Show Statement usage examples and patterns."""

    print("\n" + "="*60)
    print("STATEMENT USAGE EXAMPLES")
    print("="*60)

    print("\n1. Basic Statement (no filters):")
    print("""
    statement = Statement("test", "users", ["name", "age"])
    recordset = await client.query(
        policy=QueryPolicy(),
        partition_filter=PartitionFilter.all(),
        statement=statement
    )
    """)

    print("\n2. Statement with Range Filter:")
    print("""
    age_filter = Filter.range("age", 18, 65)
    statement = Statement("test", "users", ["name", "age"])
    statement.filters = [age_filter]

    recordset = await client.query(
        policy=QueryPolicy(),
        partition_filter=PartitionFilter.all(),
        statement=statement
    )
    """)

    print("\n3. Statement with Contains Filter:")
    print("""
    contains_filter = Filter.contains("roles", "admin", CollectionIndexType.LIST)
    statement = Statement("test", "users", ["name", "roles"])
    statement.filters = [contains_filter]

    recordset = await client.query(
        policy=QueryPolicy(),
        partition_filter=PartitionFilter.all(),
        statement=statement
    )
    """)

    print("\n4. Statement with Contains Range Filter:")
    print("""
    range_filter = Filter.contains_range("scores", 80, 100, CollectionIndexType.LIST)
    statement = Statement("test", "students", ["name", "scores"])
    statement.filters = [range_filter]

    recordset = await client.query(
        policy=QueryPolicy(),
        partition_filter=PartitionFilter.all(),
        statement=statement
    )
    """)

    print("\n5. Statement with Geo Filter:")
    print("""
    geo_filter = Filter.within_radius("location", -89.0005, 43.0004, 1000.0, CollectionIndexType.DEFAULT)  # lng, lat order
    statement = Statement("test", "places", ["name", "location"])
    statement.filters = [geo_filter]

    recordset = await client.query(
        policy=QueryPolicy(),
        partition_filter=PartitionFilter.all(),
        statement=statement
    )
    """)

    print("\n6. Available Filter Types:")
    print("""
    - Filter.range(bin_name, begin, end)                    # Numeric range
    - Filter.contains(bin_name, value, cit)                 # Contains value in list/map
    - Filter.contains_range(bin_name, begin, end, cit)      # Contains range in list/map
    - Filter.within_region(bin_name, region, cit)           # Geo region
    - Filter.within_radius(bin_name, lng, lat, radius, cit) # Geo radius (lng, lat order)
    - Filter.regions_containing_point(bin_name, point, cit) # Geo point

    Where cit is CollectionIndexType (DEFAULT, LIST, MAP_KEYS, MAP_VALUES)
    """)

    print("\n7. Important Notes:")
    print("""
    - Filters require secondary indexes on the specified bins
    - Only one filter per Statement is currently supported
    - Geo filters require geo2dsphere indexes
    - Range filters require numeric or string indexes
    - Contains filters require list or map indexes
    """)


if __name__ == "__main__":
    print("📋 Simple Statement Example")
    print("="*40)

    # Run the example
    asyncio.run(simple_statement_example())

    # Show usage examples
    show_statement_usage()
