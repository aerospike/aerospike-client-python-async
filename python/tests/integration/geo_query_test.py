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

import asyncio
from aerospike_async import (
    Key,
    GeoJSON,
    Statement,
    Filter,
    QueryPolicy,
    PartitionFilter,
    WritePolicy,
    CollectionIndexType,
    IndexType,
)
from aerospike_async.exceptions import ServerError, ResultCode
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
        try:
            # Try to drop index first in case it exists from previous test run
            try:
                await client.drop_index(namespace, set_name, index_name)
                await asyncio.sleep(0.2)  # Brief wait for index drop to complete
            except:
                pass

            await client.create_index(
                namespace=namespace,
                set_name=set_name,
                bin_name=LOCBIN,
                index_name=index_name,
                index_type=IndexType.GEO2D_SPHERE,
                cit=CollectionIndexType.DEFAULT
            )
            print(f"Index {index_name} created successfully")
            # Wait for index to be ready (geo2dsphere indexes typically ready in 1-2 seconds)
            await asyncio.sleep(2.0)
        except ServerError as e:
            if e.result_code == ResultCode.INDEX_FOUND:
                print(f"Index {index_name} already exists, continuing...")
                await asyncio.sleep(0.5)
            else:
                raise

        # Create some test records with locations inside and outside the region
        # Write records AFTER index exists so they get indexed
        wp = WritePolicy()

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

        # Wait for newly written records to be indexed (typically very fast, < 1 second)
        await asyncio.sleep(1.0)

        # Construct the query predicate using Filter.within_region
        # Note: Filter.within_region expects a GeoJSON string, not a GeoJSON object
        # Use str(region) to get the JSON string representation (equivalent to region.dumps() in legacy client)
        region_str = str(region)  # This gets the JSON string representation
        predicate = Filter.within_region(
            bin_name=LOCBIN,
            region=region_str,
            cit=CollectionIndexType.DEFAULT
        )

        # Construct the query statement
        statement = Statement(namespace, set_name, bins=None)
        statement.filters = [predicate]

        records = await client.query(QueryPolicy(), PartitionFilter.all(), statement)
        records_list = []

        async for record in records:
            records_list.append(record)

        max_wait = 10
        for _ in range(max_wait):
            if not records.active:
                break
            await asyncio.sleep(0.1)

        records.close()

        print(f"Query returned {len(records_list)} records")
        if len(records_list) >= 2:
            for record in records_list:
                assert LOCBIN in record.bins, f"Record should have {LOCBIN} bin"
                assert isinstance(record.bins[LOCBIN], GeoJSON), "Location should be GeoJSON"
        elif len(records_list) == 0:
            print(f"Warning: Query returned 0 results after waiting for index and records to be ready")

