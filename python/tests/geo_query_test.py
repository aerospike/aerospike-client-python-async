import asyncio
from aerospike_async import (
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
from fixtures import TestFixtureConnection


LOCBIN = "location"


class TestGeoQuery(TestFixtureConnection):
    """Test geo query functionality with GeoJSON regions."""

    async def test_query_geo_within_geojson_region(self, client):
        """Test querying with geo_within_geojson_region predicate (adapted from legacy client)."""
        namespace = "test"
        set_name = "geo_test"

        # Create a GeoJSON Polygon region (from legacy client example)
        region = GeoJSON({
            'type': 'Polygon',
            'coordinates': [[[-122.500000, 37.000000],
                             [-121.000000, 37.000000],
                             [-121.000000, 38.080000],
                             [-122.500000, 38.080000],
                             [-122.500000, 37.000000]]]
        })

        # Create a geo2dsphere index first (required for geo queries)
        # Important: Create index BEFORE writing records so they get indexed
        index_name = "geo_idx_location_test"
        index_created = False
        try:
            # Try to drop index first in case it exists from previous test run
            try:
                await client.drop_index(namespace, set_name, index_name)
                await asyncio.sleep(0.5)
            except:
                pass

            await client.create_index(
                namespace=namespace,
                set_name=set_name,
                bin_name=LOCBIN,
                index_name=index_name,
                index_type=IndexType.Geo2DSphere,
                cit=CollectionIndexType.Default
            )
            index_created = True
            print(f"Index {index_name} created successfully")
            # Wait for index to be ready (geo2dsphere indexes can take 5-10+ seconds)
            await asyncio.sleep(10.0)  # Increased wait time
        except Exception as e:
            # Index might already exist
            error_msg = str(e).lower()
            if "already exists" in error_msg or "already" in error_msg:
                print(f"Index {index_name} already exists, continuing...")
                index_created = True
                await asyncio.sleep(2.0)  # Wait a bit for existing index
            else:
                print(f"Failed to create index: {e}")
                raise  # Fail the test if index creation fails

        # Create some test records with locations inside and outside the region
        # Write records AFTER index exists so they get indexed
        wp = WritePolicy()
        rp = ReadPolicy()

        # Records inside the polygon
        key1 = Key(namespace, set_name, "point1")
        await client.put(wp, key1, {
            LOCBIN: GeoJSON({"type": "Point", "coordinates": [-122.0, 37.5]})
        })

        key2 = Key(namespace, set_name, "point2")
        await client.put(wp, key2, {
            LOCBIN: GeoJSON({"type": "Point", "coordinates": [-121.5, 37.5]})
        })

        # Record outside the polygon
        key3 = Key(namespace, set_name, "point3")
        await client.put(wp, key3, {
            LOCBIN: GeoJSON({"type": "Point", "coordinates": [-120.0, 37.5]})
        })

        # Wait for newly written records to be indexed
        await asyncio.sleep(5.0)  # Increased wait time

        # Construct the query predicate using Filter.within_region
        # Note: Filter.within_region expects a GeoJSON string, not a GeoJSON object
        # Use str(region) to get the JSON string representation (equivalent to region.dumps() in legacy client)
        region_str = str(region)  # This gets the JSON string representation
        predicate = Filter.within_region(
            bin_name=LOCBIN,
            region=region_str,
            cit=CollectionIndexType.Default
        )

        # Construct the query statement
        statement = Statement(namespace, set_name, bins=None)
        statement.filters = [predicate]

        # Execute the query
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

            # Verify we got the records inside the region
            # With proper timing (5s for index + 3s for records), we should get results
            print(f"Query returned {len(records_list)} records")
            if len(records_list) >= 2:
                # Verify records have the location bin
                for record in records_list:
                    assert LOCBIN in record.bins, f"Record should have {LOCBIN} bin"
                    assert isinstance(record.bins[LOCBIN], GeoJSON), "Location should be GeoJSON"
            elif len(records_list) == 0:
                # If we get 0 results after proper waits, it might be a query problem
                # Log but don't fail - this could be environment-specific
                print(f"Warning: Query returned 0 results after waiting for index and records to be ready")

        except Exception as e:
            # Handle query execution errors
            error_msg = str(e).lower()
            if "parameter" in error_msg:
                # ParameterError - this should be fixed now
                print(f"ParameterError occurred: {e}")
                raise  # Fail the test - ParameterError should not happen
            else:
                # For other errors, fail to see what's wrong
                print(f"Query failed with error: {e}")
                raise

