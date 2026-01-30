import asyncio
import os
from aerospike_async import (
    new_client,
    ClientPolicy,
    Key,
    GeoJSON,
    Statement,
    Filter,
    QueryPolicy,
    PartitionFilter,
    WritePolicy,
    ReadPolicy,
    CollectionIndexType,
    IndexType,
)

# Bin name for location data
LOCBIN = "location"


async def main():
    """
    Example of geospatial queries using GeoJSON regions.

    Note: Geo2dsphere index building is asynchronous and can take 5-10+ seconds.
    If you get 0 results, try:
    - Running the example multiple times (index persists between runs)
    - Increasing the wait times in the code
    - Creating the index in advance before running the example
    """

    # Connect to Aerospike cluster
    cp = ClientPolicy()
    cp.use_services_alternate = os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "").lower() in ("true", "1")  # Required for connection
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client = await new_client(cp, host)

    namespace = "test"
    set_name = "geo_example"

    try:
        # Create a GeoJSON Polygon region
        # This defines the area we want to search within
        region = GeoJSON({
            'type': 'Polygon',
            'coordinates': [[[-122.500000, 37.000000],
                             [-121.000000, 37.000000],
                             [-121.000000, 38.080000],
                             [-122.500000, 38.080000],
                             [-122.500000, 37.000000]]]  # Polygon must close (first point == last point)
        })
        print(f"Created search region: {region}")
        print()

        # Create a geo2dsphere index FIRST (required for geo queries)
        # This index allows efficient spatial queries
        index_name = "geo_idx_location_example"
        print(f"Setting up geo2dsphere index '{index_name}'...")

        # Create the index (idempotent - won't error if already exists)
        # Note: create_index is idempotent in Aerospike - it succeeds silently if index exists
        await client.create_index(
            namespace=namespace,
            set_name=set_name,
            bin_name=LOCBIN,
            index_name=index_name,
            index_type=IndexType.GEO2D_SPHERE,
            cit=CollectionIndexType.DEFAULT
        )
        print("  ✅ Index creation requested")
        print("  Note: create_index is idempotent (succeeds even if index already exists)")
        print("  Waiting for index to be ready...")
        # Always wait - we can't tell if index is new or existing
        await asyncio.sleep(5.0)  # Wait for index to build/update
        print()

        # Create some test records with locations
        # Important: Write records AFTER index exists so they get indexed
        wp = WritePolicy()

        print("Creating records with locations...")
        key1 = Key(namespace, set_name, "point1")
        await client.put(wp, key1, {
            LOCBIN: GeoJSON({"type": "Point", "coordinates": [-122.0, 37.5]}),
            "name": "Location 1 (inside region)"
        })
        print(f"  Created {key1}")

        key2 = Key(namespace, set_name, "point2")
        await client.put(wp, key2, {
            LOCBIN: GeoJSON({"type": "Point", "coordinates": [-121.5, 37.5]}),
            "name": "Location 2 (inside region)"
        })
        print(f"  Created {key2}")

        # Record outside the polygon (should NOT be found by query)
        key3 = Key(namespace, set_name, "point3")
        await client.put(wp, key3, {
            LOCBIN: GeoJSON({"type": "Point", "coordinates": [-120.0, 37.5]}),
            "name": "Location 3 (outside region)"
        })
        print(f"  Created {key3}")
        print()

        # Wait for newly written records to be indexed
        print("Waiting for records to be indexed...")
        await asyncio.sleep(3.0)  # Wait for records to be indexed
        print()

        # Note: create_index is idempotent - we can't easily detect if it's new or existing
        # So we always wait the same amount of time

        # Verify records were written correctly (for debugging)
        rp = ReadPolicy()
        print("Verifying records were written...")
        for key in [key1, key2, key3]:
            rec = await client.get(rp, key)
            if rec:
                loc = rec.bins.get(LOCBIN)
                print(f"  {key}: Found - location = {loc}")
                # Print the actual GeoJSON string value
                if hasattr(loc, 'value'):
                    print(f"    GeoJSON value: {loc.value}")
                elif isinstance(loc, str):
                    print(f"    GeoJSON string: {loc}")
            else:
                print(f"  {key}: NOT FOUND")
        print()

        # Note: Indexing can take a few seconds, especially for geo indexes
        # We already waited 3 seconds above, which should be sufficient

        # Construct the query predicate using Filter.within_region
        # Filter.within_region expects a GeoJSON JSON string, not a GeoJSON object
        # Use str(region) to get the JSON string representation
        # (This is equivalent to region.dumps() in the legacy Python client)
        region_str = str(region)
        print(f"Creating query filter...")
        print(f"  Region GeoJSON: {region_str}")
        print(f"  Bin name: {LOCBIN}")
        print()
        predicate = Filter.within_region(
            bin_name=LOCBIN,
            region=region_str,
            cit=CollectionIndexType.DEFAULT
        )

        # Construct the query statement
        statement = Statement(namespace, set_name, bins=None)
        statement.filters = [predicate]
        print(f"Query statement:")
        print(f"  Namespace: {namespace}")
        print(f"  Set: {set_name}")
        print(f"  Filters: {len(statement.filters)} filter(s)")
        print()

        # Execute the query
        print("Executing query...")
        try:
            records = await client.query(QueryPolicy(), PartitionFilter.all(), statement)
            records_list = []

            # Collect records from the recordset
            async for record in records:
                records_list.append(record)

            # Wait for query to complete
            max_wait = 10
            for _ in range(max_wait):
                if not records.active:
                    break
                await asyncio.sleep(0.1)

            records.close()

            # Display results
            print(f"Query returned {len(records_list)} record(s)")
            print()
            for i, record in enumerate(records_list, 1):
                print(f"Record {i}:")
                print(f"  Key: {record.key}")
                print(f"  Bins: {record.bins}")
                if LOCBIN in record.bins:
                    print(f"  Location: {record.bins[LOCBIN]}")
                print()

            if len(records_list) == 0:
                print("  ⚠️  No records found")
                print()
                print("  Possible reasons:")
                print("  1. Index is still building (geo2dsphere indexes can take 5-10+ seconds)")
                print("  2. Records haven't been indexed yet")
                print("  3. No points are actually within the polygon region")
                print()
                print("  💡 Tip: In production, create indexes ahead of time and wait for them")
                print("     to be fully built before writing data. You can also run this example")
                print("     multiple times - subsequent runs should find records if the index")
                print("     was created in a previous run.")
            elif len(records_list) >= 2:
                print(f"✅ Successfully found {len(records_list)} record(s) within the region!")

        except Exception as e:
            error_msg = str(e).lower()
            if "index" in error_msg or "not found" in error_msg:
                print(f"  Error: Geo2dsphere index not ready or not found: {e}")
                print("  Try waiting a few seconds and running again")
            else:
                print(f"  Query failed with error: {e}")
                raise

    finally:
        # Clean up: close the client connection
        await client.close()
        print("Closed client connection")


if __name__ == "__main__":
    asyncio.run(main())

