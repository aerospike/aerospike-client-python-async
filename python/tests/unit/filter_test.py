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

from aerospike_async import Filter, CollectionIndexType


class TestFilter:
    """Test Filter class functionality."""

    def test_equal(self):
        """Test creating an equal filter."""
        a_filter = Filter.equal(bin_name="bin", value=42)
        assert isinstance(a_filter, Filter)

    def test_equal_string(self):
        """Test creating an equal filter with string value."""
        a_filter = Filter.equal(bin_name="name", value="hello")
        assert isinstance(a_filter, Filter)

    def test_range(self):
        """Test creating a range filter."""
        a_filter = Filter.range(bin_name="bin", begin=4, end=9)
        assert isinstance(a_filter, Filter)

    def test_contains(self):
        """Test creating a contains filter."""
        a_filter = Filter.contains(bin_name="bin", value=3.0, cit=CollectionIndexType.LIST)
        assert isinstance(a_filter, Filter)

    def test_contains_range(self):
        """Test creating a contains_range filter."""
        a_filter = Filter.contains_range(bin_name="bin", begin=1, end=4, cit=CollectionIndexType.LIST)
        assert isinstance(a_filter, Filter)

    def test_within_region(self):
        """Test creating a within_region filter."""
        a_filter = Filter.within_region(
            bin_name="bin",
            region='{"type":"AeroCircle","coordinates":[[-89.0000,23.0000], 1000]}',
            cit=CollectionIndexType.DEFAULT)
        assert isinstance(a_filter, Filter)

    def test_within_radius(self):
        """Test creating a within_radius filter."""
        a_filter = Filter.within_radius(
            bin_name="bin",
            lng=40.0,  # longitude first (GeoJSON standard: [lng, lat])
            lat=20.0,  # latitude second
            radius=5.0,
            cit=CollectionIndexType.DEFAULT)
        assert isinstance(a_filter, Filter)

    def test_regions_containing_point(self):
        """Test creating a regions_containing_point filter."""
        a_filter = Filter.regions_containing_point(
            bin_name="bin",
            point='{"type":"Point","coordinates":[-89.0000,23.0000]}',
            cit=CollectionIndexType.DEFAULT
        )
        assert isinstance(a_filter, Filter)
