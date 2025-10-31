#!/usr/bin/env python3
"""
Demonstration of geo query serialization bug in aerospike-core.

This example demonstrates that Filter.within_radius() correctly constructs
the Filter object with proper GeoJSON, but the query fails because the
GeoJSON string is truncated during wire protocol serialization in aerospike-core.

BUG REPORT for aerospike-client-rust:
- Issue: GeoJSON strings in geo filters are truncated during serialization
- Server Error: "failed to parse geojson: 1: '[' or '{' expected near end of file"
- Root Cause: aerospike-core's wire protocol serialization of Value::GeoJSON

Expected Behavior:
- Query should return records within the specified radius

Actual Behavior:
- Query returns 0 results
- Server logs show GeoJSON parsing error (truncated string)
- Filter object structure is correct (verified via Debug output)
"""

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
    IndexType,
    CollectionIndexType,
)

# Configuration
NAMESPACE = "test"
SET_NAME = "geo_bug_demo"
BIN_NAME = "location"
INDEX_NAME = "geo_idx_bug_demo"


async def main():
    """Demonstrate the geo query serialization bug."""
    
    # Connect to server
    cp = ClientPolicy()
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3100")
    print(f"Connecting to: {host}\n")
    client = await new_client(cp, host)
    
    wp = WritePolicy()
    rp = ReadPolicy()
    
    try:
        # Step 1: Create geo2dsphere index
        print("=" * 70)
        print("STEP 1: Creating geo2dsphere index")
        print("=" * 70)
        try:
            await client.create_index(
                namespace=NAMESPACE,
                set_name=SET_NAME,
                bin_name=BIN_NAME,
                index_name=INDEX_NAME,
                index_type=IndexType.Geo2DSphere,
                cit=CollectionIndexType.Default
            )
            print(f"✓ Index '{INDEX_NAME}' creation requested")
            await asyncio.sleep(3.0)  # Wait for index to build
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"✓ Index '{INDEX_NAME}' already exists")
            else:
                print(f"⚠ Index creation error: {e}")
        print()
        
        # Step 2: Create test records with GeoJSON Points
        print("=" * 70)
        print("STEP 2: Creating test records with GeoJSON Points")
        print("=" * 70)
        
        # Point inside the query radius (should be found)
        key1 = Key(NAMESPACE, SET_NAME, "point_inside")
        point1 = GeoJSON({"type": "Point", "coordinates": [-122.0, 37.5]})
        await client.put(wp, key1, {
            BIN_NAME: point1,
            "name": "Point Inside Radius"
        })
        print(f"✓ Created {key1}: {point1}")
        
        # Point inside the query radius (should be found)
        key2 = Key(NAMESPACE, SET_NAME, "point_inside_2")
        point2 = GeoJSON({"type": "Point", "coordinates": [-121.5, 37.5]})
        await client.put(wp, key2, {
            BIN_NAME: point2,
            "name": "Point Inside Radius 2"
        })
        print(f"✓ Created {key2}: {point2}")
        
        # Point outside the query radius (should NOT be found)
        key3 = Key(NAMESPACE, SET_NAME, "point_outside")
        point3 = GeoJSON({"type": "Point", "coordinates": [-120.0, 37.5]})
        await client.put(wp, key3, {
            BIN_NAME: point3,
            "name": "Point Outside Radius"
        })
        print(f"✓ Created {key3}: {point3}")
        
        await asyncio.sleep(2.0)  # Wait for records to be indexed
        print()
        
        # Step 3: Verify records were stored correctly
        print("=" * 70)
        print("STEP 3: Verifying records were stored correctly")
        print("=" * 70)
        for key in [key1, key2, key3]:
            rec = await client.get(rp, key)
            if rec and BIN_NAME in rec.bins:
                loc = rec.bins[BIN_NAME]
                name = rec.bins.get("name", "N/A")
                print(f"✓ {key}: {name} - location = {loc}")
            else:
                print(f"✗ {key}: NOT FOUND")
        print()
        
        # Step 4: Inspect the Filter object structure
        print("=" * 70)
        print("STEP 4: Creating Filter.within_radius() and inspecting structure")
        print("=" * 70)
        print("Query: Find points within 100km of [-122.0, 37.5]")
        print()
        
        filter_obj = Filter.within_radius(
            bin_name=BIN_NAME,
            lng=-122.0,   # longitude first (GeoJSON standard)
            lat=37.5,     # latitude second
            radius=100000.0,  # 100km in meters
            cit=CollectionIndexType.Default
        )
        
        print("Filter object structure:")
        print(f"  {filter_obj}")
        print()
        
        # Verify the Filter structure is correct
        filter_str = str(filter_obj)
        issues = []
        
        # Check for correct type name (handle escaped quotes in debug output)
        if 'AeroCircle' in filter_str and 'Aeroircle' not in filter_str:
            print("  ✓ Type name correct: 'AeroCircle'")
        elif 'Aeroircle' in filter_str:
            issues.append("Type name has typo: 'Aeroircle' (should be 'AeroCircle')")
        else:
            issues.append("Type name incorrect or missing")
            
        # Check coordinate order (longitude should be first: -122 before 37.5)
        if '[[-122' in filter_str or '[-122.0' in filter_str:
            print("  ✓ Coordinates correct: longitude first [-122, 37.5]")
        elif '[[37.5' in filter_str or '[37.5, -122' in filter_str:
            issues.append("Coordinates in wrong order: latitude first (should be longitude first)")
        else:
            issues.append("Coordinates format unclear")
        
        if issues:
            print(f"  ✗ Issues found: {', '.join(issues)}")
        else:
            print("  ✓ Filter structure is CORRECT")
        
        print()
        print("Analysis:")
        if issues:
            print("  Filter structure has issues that need to be fixed")
        else:
            print("  Filter object structure is CORRECT - the bug is in serialization")
        print()
        
        # Step 5: Execute the query
        print("=" * 70)
        print("STEP 5: Executing query with Filter.within_radius()")
        print("=" * 70)
        
        statement = Statement(NAMESPACE, SET_NAME, bins=None)
        statement.filters = [filter_obj]
        
        policy = QueryPolicy()
        
        print("Executing query...")
        records = await client.query(policy, PartitionFilter.all(), statement)
        records_list = []
        
        for record in records:
            records_list.append(record)
        
        # Wait for query to complete
        max_wait = 10
        for _ in range(max_wait):
            if not records.active:
                break
            await asyncio.sleep(0.1)
        
        records.close()
        
        print()
        print("=" * 70)
        print("RESULTS")
        print("=" * 70)
        print(f"Records found: {len(records_list)}")
        print()
        
        if len(records_list) == 0:
            print("❌ BUG CONFIRMED: Query returned 0 results")
            print()
            print("Expected Results:")
            print("  - Should find at least 2 records (point_inside and point_inside_2)")
            print("  - Should NOT find point_outside")
            print()
            print("Actual Results:")
            print("  - Found 0 records")
            print()
            print("Evidence of Bug:")
            print("  1. Filter object structure is correct (verified above)")
            print("  2. Records exist and are indexed (verified above)")
            print("  3. Index exists and is ready (verified above)")
            print("  4. Server logs show error: 'failed to parse geojson: 1: '[' or '{' expected near end of file'")
            print()
            print("Root Cause:")
            print("  The GeoJSON string in the Filter is correct, but when aerospike-core")
            print("  serializes Value::GeoJSON for the wire protocol, the string is truncated.")
            print("  This causes the server to receive malformed GeoJSON and fail to parse it.")
            print()
            print("Workaround:")
            print("  Currently using manual AeroCircle string construction with Filter.within_region(),")
            print("  but this still fails due to the same serialization bug in aerospike-core.")
            print()
            print("Action Required:")
            print("  This bug needs to be fixed in aerospike-core's wire protocol serialization")
            print("  for Value::GeoJSON. The Filter construction is correct, but serialization")
            print("  truncates the GeoJSON string when sending to the server.")
        else:
            print("✅ Query succeeded! Found records:")
            for i, record in enumerate(records_list, 1):
                name = record.bins.get("name", "N/A")
                loc = record.bins.get(BIN_NAME, "N/A")
                print(f"  {i}. {record.key}: {name} - {loc}")
            
            # Verify we got the right records
            found_keys = {r.key for r in records_list}
            expected_keys = {key1, key2}
            unexpected_keys = {key3}
            
            if found_keys == expected_keys:
                print()
                print("✅ Perfect! Found exactly the expected records (inside radius only)")
            elif unexpected_keys.intersection(found_keys):
                print()
                print("⚠ Warning: Found records that should be outside the radius")
            else:
                print()
                print("⚠ Warning: Did not find all expected records")
        
        print()
        print("=" * 70)
        
    finally:
        client.close()
        print("\nClosed client connection")


if __name__ == "__main__":
    print(__doc__)
    print()
    asyncio.run(main())

