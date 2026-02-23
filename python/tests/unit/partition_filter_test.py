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

import pytest
from aerospike_async import PartitionFilter, PartitionStatus, Key


class TestPartitionFilter:
    """Test PartitionFilter class functionality."""

    def test_all(self):
        """Test PartitionFilter.all() creates a filter for all partitions."""
        pf = PartitionFilter.all()
        assert isinstance(pf, PartitionFilter)
        assert pf.begin == 0
        assert pf.count == 4096
        assert pf.digest is None

    def test_by_id(self):
        """Test PartitionFilter.by_id() creates a filter for a specific partition."""
        pf = PartitionFilter.by_id(0)
        assert isinstance(pf, PartitionFilter)
        assert pf.begin == 0
        assert pf.count == 1
        assert pf.digest is None

        pf = PartitionFilter.by_id(100)
        assert pf.begin == 100
        assert pf.count == 1

        pf = PartitionFilter.by_id(4095)
        assert pf.begin == 4095
        assert pf.count == 1

    def test_by_id_invalid(self):
        """Test PartitionFilter.by_id() with invalid partition IDs."""
        pf = PartitionFilter.by_id(5000)
        assert pf.begin == 5000
        assert pf.count == 1

    def test_by_range(self):
        """Test PartitionFilter.by_range() creates a filter for a partition range."""
        pf = PartitionFilter.by_range(0, 10)
        assert isinstance(pf, PartitionFilter)
        assert pf.begin == 0
        assert pf.count == 10
        assert pf.digest is None

        pf = PartitionFilter.by_range(100, 50)
        assert pf.begin == 100
        assert pf.count == 50

        pf = PartitionFilter.by_range(4090, 6)
        assert pf.begin == 4090
        assert pf.count == 6

    def test_by_key(self):
        """Test PartitionFilter.by_key() creates a filter for a specific key."""
        key = Key("test", "test", "test_key")
        pf = PartitionFilter.by_key(key)
        assert isinstance(pf, PartitionFilter)
        assert pf.count == 1
        assert pf.digest is not None
        assert isinstance(pf.digest, str)
        assert len(pf.digest) == 40

    def test_by_key_with_digest_key(self):
        """Test PartitionFilter.by_key() with a key created from digest."""
        digest_hex = "a" * 40
        key = Key.key_with_digest("test", "test", digest_hex)
        pf = PartitionFilter.by_key(key)
        assert pf.count == 1
        assert pf.digest is not None

    def test_getters_setters(self):
        """Test getters and setters for PartitionFilter properties."""
        pf = PartitionFilter.all()

        assert pf.begin == 0
        pf.begin = 100
        assert pf.begin == 100

        assert pf.count == 4096
        pf.count = 50
        assert pf.count == 50

        assert pf.digest is None
        digest_hex = "a" * 40
        pf.digest = digest_hex
        assert pf.digest == digest_hex

        pf.digest = None
        assert pf.digest is None

    def test_digest_setter_invalid(self):
        """Test digest setter with invalid values."""
        pf = PartitionFilter.all()

        with pytest.raises(ValueError, match="Invalid hex digest"):
            pf.digest = "short"

        with pytest.raises(ValueError, match="Digest must be exactly 20 bytes"):
            pf.digest = "a" * 38

        with pytest.raises(ValueError, match="Digest must be exactly 20 bytes"):
            pf.digest = "a" * 42

        with pytest.raises(ValueError, match="Invalid hex digest"):
            pf.digest = "g" * 40

    def test_done(self):
        """Test done() method."""
        pf = PartitionFilter.all()
        assert pf.done() is False

    def test_default_constructor(self):
        """Test default constructor creates a filter for all partitions."""
        pf = PartitionFilter()
        assert pf.begin == 0
        assert pf.count == 4096
        assert pf.digest is None

    def test_partitions_setter_accepts_partition_status_objects(self):
        pf = PartitionFilter.by_range(0, 1)
        ps = PartitionStatus(0)
        ps.retry = False
        ps.bval = 0
        ps.digest = None
        pf.partitions = [ps]

    @pytest.mark.asyncio
    async def test_partitions_property_works_in_asyncio_context(self):
        pf = PartitionFilter.by_range(0, 1)
        ps = PartitionStatus(0)
        ps.retry = False
        ps.bval = 0
        ps.digest = None
        pf.partitions = [ps]

        got = pf.partitions
        assert got is not None
        assert len(got) == 1
        assert got[0].id == 0
