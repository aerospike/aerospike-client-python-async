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
from aerospike_async import PartitionStatus


class TestPartitionStatus:
    """Test PartitionStatus class functionality."""

    def test_new(self):
        """Test creating a new PartitionStatus from scratch."""
        ps = PartitionStatus(1000)
        assert isinstance(ps, PartitionStatus)
        assert ps.id == 1000
        assert ps.retry is True  # Default value
        assert ps.bval is None
        assert ps.digest is None

    def test_id_read_only(self):
        """Test that id is read-only and cannot be set."""
        ps = PartitionStatus(1000)
        assert ps.id == 1000

        # id should not have a setter
        assert not hasattr(ps, 'set_id')

        # Try to set via dict access should fail
        with pytest.raises(ValueError, match="'id' is read-only"):
            ps['id'] = 2000

    def test_bval_getter_setter(self):
        """Test bval getter and setter."""
        ps = PartitionStatus(1000)

        # Initially None
        assert ps.bval is None

        # Set via setter
        ps.bval = 12345
        assert ps.bval == 12345

        # Set to None
        ps.bval = None
        assert ps.bval is None

        # Set via dict access
        ps['bval'] = 67890
        assert ps.bval == 67890
        assert ps['bval'] == 67890

    def test_retry_getter_setter(self):
        """Test retry getter and setter."""
        ps = PartitionStatus(1000)

        # Default is True
        assert ps.retry is True

        # Set via setter
        ps.retry = False
        assert ps.retry is False

        # Set back to True
        ps.retry = True
        assert ps.retry is True

        # Set via dict access
        ps['retry'] = False
        assert ps.retry is False
        assert ps['retry'] is False

    def test_digest_getter_setter(self):
        """Test digest getter and setter."""
        ps = PartitionStatus(1000)

        # Initially None
        assert ps.digest is None

        # Set valid hex digest (20 bytes = 40 hex chars)
        valid_digest = "a" * 40
        ps.digest = valid_digest
        assert ps.digest == valid_digest

        # Set to None
        ps.digest = None
        assert ps.digest is None

        # Set via dict access
        ps['digest'] = valid_digest
        assert ps.digest == valid_digest
        assert ps['digest'] == valid_digest

    def test_digest_setter_invalid_hex(self):
        """Test digest setter with invalid hex string."""
        ps = PartitionStatus(1000)

        # Invalid hex (odd number of characters)
        with pytest.raises(ValueError, match="Invalid hex digest"):
            ps.digest = "short"

        # Invalid hex characters
        with pytest.raises(ValueError, match="Invalid hex digest"):
            ps.digest = "g" * 40

    def test_digest_setter_wrong_length(self):
        """Test digest setter with wrong length."""
        ps = PartitionStatus(1000)

        # Too short (19 bytes)
        with pytest.raises(ValueError, match="Digest must be exactly 20 bytes"):
            ps.digest = "a" * 38

        # Too long (21 bytes)
        with pytest.raises(ValueError, match="Digest must be exactly 20 bytes"):
            ps.digest = "a" * 42

    def test_digest_setter_via_dict_invalid(self):
        """Test digest setter via dict access with invalid values."""
        ps = PartitionStatus(1000)

        # Invalid hex via dict access
        with pytest.raises(ValueError, match="Invalid hex digest"):
            ps['digest'] = "short"

        # Wrong length via dict access
        with pytest.raises(ValueError, match="Digest must be exactly 20 bytes"):
            ps['digest'] = "a" * 38

    def test_dict_access_get(self):
        """Test dictionary-style get access."""
        ps = PartitionStatus(1000)
        ps.bval = 12345
        ps.retry = False
        ps.digest = "a" * 40

        # Test all valid keys
        assert ps['id'] == 1000
        assert ps['bval'] == 12345
        assert ps['retry'] is False
        assert ps['digest'] == "a" * 40

    def test_dict_access_set(self):
        """Test dictionary-style set access."""
        ps = PartitionStatus(1000)

        # Set via dict access
        ps['bval'] = 99999
        ps['retry'] = False
        ps['digest'] = "b" * 40

        # Verify via getters
        assert ps.bval == 99999
        assert ps.retry is False
        assert ps.digest == "b" * 40

    def test_dict_access_invalid_key(self):
        """Test dictionary-style access with invalid keys."""
        ps = PartitionStatus(1000)

        # Invalid key for get
        with pytest.raises(KeyError, match="Unknown key"):
            _ = ps['invalid_key']

        # Invalid key for set
        with pytest.raises(KeyError, match="Unknown key"):
            ps['invalid_key'] = 123

    def test_dict_access_id_read_only(self):
        """Test that id cannot be set via dict access."""
        ps = PartitionStatus(1000)

        # Try to set id via dict access
        with pytest.raises(ValueError, match="'id' is read-only"):
            ps['id'] = 2000

        # Verify id is unchanged
        assert ps.id == 1000

    def test_dict_access_none_values(self):
        """Test dictionary-style access with None values."""
        ps = PartitionStatus(1000)

        # Set values to None via dict
        ps['bval'] = None
        ps['digest'] = None

        # Verify None values
        assert ps['bval'] is None
        assert ps['digest'] is None
        assert ps.bval is None
        assert ps.digest is None

    def test_all_fields_roundtrip(self):
        """Test setting and getting all fields via both methods."""
        ps = PartitionStatus(500)

        # Set via setters
        ps.bval = 12345
        ps.retry = False
        ps.digest = "c" * 40

        # Verify via getters
        assert ps.bval == 12345
        assert ps.retry is False
        assert ps.digest == "c" * 40

        # Verify via dict access
        assert ps['bval'] == 12345
        assert ps['retry'] is False
        assert ps['digest'] == "c" * 40

        # Change via dict access
        ps['bval'] = 67890
        ps['retry'] = True
        ps['digest'] = "d" * 40

        # Verify via getters
        assert ps.bval == 67890
        assert ps.retry is True
        assert ps.digest == "d" * 40

    def test_default_values(self):
        """Test default values for new PartitionStatus."""
        ps = PartitionStatus(2000)

        assert ps.id == 2000
        assert ps.retry is True  # Default
        assert ps.bval is None
        assert ps.digest is None

    def test_multiple_instances(self):
        """Test creating multiple PartitionStatus instances."""
        ps1 = PartitionStatus(100)
        ps2 = PartitionStatus(200)

        ps1.bval = 111
        ps2.bval = 222

        assert ps1.id == 100
        assert ps2.id == 200
        assert ps1.bval == 111
        assert ps2.bval == 222

        # Instances should be independent
        assert ps1.bval != ps2.bval

