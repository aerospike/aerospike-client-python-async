from aerospike_async import Filter, CollectionIndexType


class TestFilter:
    """Test Filter class functionality."""

    def test_range(self):
        """Test creating a range filter."""
        filter = Filter.range(bin_name="bin", begin=4, end=9)
        assert isinstance(filter, Filter)

    def test_contains(self):
        """Test creating a contains filter."""
        filter = Filter.contains(bin_name="bin", value=3.0, cit=CollectionIndexType.List)
        assert isinstance(filter, Filter)

    def test_contains_range(self):
        """Test creating a contains_range filter."""
        filter = Filter.contains_range(bin_name="bin", begin=1, end=4, cit=CollectionIndexType.List)
        assert isinstance(filter, Filter)

    def test_within_region(self):
        """Test creating a within_region filter."""
        filter = Filter.within_region(
            bin_name="bin",
            region='{"type":"AeroCircle","coordinates":[[-89.0000,23.0000], 1000]}',
            cit=CollectionIndexType.Default)
        assert isinstance(filter, Filter)

    def test_within_radius(self):
        """Test creating a within_radius filter."""
        filter = Filter.within_radius(
            bin_name="bin",
            lat=20.0,
            lng=40.0,
            radius=5.0,
            cit=CollectionIndexType.Default)
        assert isinstance(filter, Filter)

    def test_regions_containing_point(self):
        """Test creating a regions_containing_point filter."""
        filter = Filter.regions_containing_point(
            bin_name="bin",
            point='{"type":"Point","coordinates":[-89.0000,23.0000]}',
            cit=CollectionIndexType.Default
        )
        assert isinstance(filter, Filter)
