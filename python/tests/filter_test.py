from aerospike_async import Filter, CollectionIndexType


class TestFilter:
    """Test Filter class functionality."""

    def test_range(self):
        """Test creating a range filter."""
        a_filter = Filter.range(bin_name="bin", begin=4, end=9)
        assert isinstance(a_filter, Filter)

    def test_contains(self):
        """Test creating a contains filter."""
        a_filter = Filter.contains(bin_name="bin", value=3.0, cit=CollectionIndexType.List)
        assert isinstance(a_filter, Filter)

    def test_contains_range(self):
        """Test creating a contains_range filter."""
        a_filter = Filter.contains_range(bin_name="bin", begin=1, end=4, cit=CollectionIndexType.List)
        assert isinstance(a_filter, Filter)

    def test_within_region(self):
        """Test creating a within_region filter."""
        a_filter = Filter.within_region(
            bin_name="bin",
            region='{"type":"AeroCircle","coordinates":[[-89.0000,23.0000], 1000]}',
            cit=CollectionIndexType.Default)
        assert isinstance(a_filter, Filter)

    def test_within_radius(self):
        """Test creating a within_radius filter."""
        a_filter = Filter.within_radius(
            bin_name="bin",
            lng=40.0,  # longitude first (GeoJSON standard: [lng, lat])
            lat=20.0,  # latitude second
            radius=5.0,
            cit=CollectionIndexType.Default)
        assert isinstance(a_filter, Filter)

    def test_regions_containing_point(self):
        """Test creating a regions_containing_point filter."""
        a_filter = Filter.regions_containing_point(
            bin_name="bin",
            point='{"type":"Point","coordinates":[-89.0000,23.0000]}',
            cit=CollectionIndexType.Default
        )
        assert isinstance(a_filter, Filter)
