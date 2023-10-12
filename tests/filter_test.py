import unittest

from aerospike_async import *


class TestFilter(unittest.TestCase):
    # Create all possible query filters
    def test_range(self):
        filter = Filter.range(bin_name="bin", begin=4, end=9)
        self.assertEquals(type(filter), Filter)

    def test_contains(self):
        filter = Filter.contains(bin_name="bin", value=3.0, cit=CollectionIndexType.List)
        self.assertEquals(type(filter), Filter)

    def test_contains_range(self):
        filter = Filter.contains_range(bin_name="bin", begin=1, end=4, cit=CollectionIndexType.List)
        self.assertEquals(type(filter), Filter)

    def test_within_region(self):
        filter = Filter.within_region(
            bin_name="bin",
            region='{"type":"AeroCircle","coordinates":[[-89.0000,23.0000], 1000]}',
            cit=CollectionIndexType.Default)
        self.assertEquals(type(filter), Filter)

    def test_within_radius(self):
        filter = Filter.within_radius(
            bin_name="bin",
            lat=20.0,
            lng=40.0,
            radius=5.0,
            cit=CollectionIndexType.Default)
        self.assertEquals(type(filter), Filter)

    def test_regions_containing_point(self):
        filter = Filter.regions_containing_point(
            bin_name="bin",
            point='{"type":"Point","coordinates":[-89.0000,23.0000]}',
            cit=CollectionIndexType.Default
        )
        self.assertEquals(type(filter), Filter)
