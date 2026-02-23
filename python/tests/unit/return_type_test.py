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

"""
Unit tests for ListReturnType and MapReturnType bitwise OR support.

These types support combining with INVERTED flag for CDT operations:
    - ListReturnType.VALUE | ListReturnType.INVERTED
    - MapReturnType.VALUE | MapReturnType.INVERTED
"""

import pytest
from aerospike_async import ListReturnType, MapReturnType


class TestListReturnType:
    """Tests for ListReturnType class."""

    def test_basic_values(self):
        """Test that basic return type values are defined."""
        assert ListReturnType.NONE is not None
        assert ListReturnType.INDEX is not None
        assert ListReturnType.REVERSE_INDEX is not None
        assert ListReturnType.RANK is not None
        assert ListReturnType.REVERSE_RANK is not None
        assert ListReturnType.COUNT is not None
        assert ListReturnType.VALUE is not None
        assert ListReturnType.EXISTS is not None
        assert ListReturnType.INVERTED is not None

    def test_bitwise_or(self):
        """Test that bitwise OR works for combining with INVERTED."""
        combined = ListReturnType.VALUE | ListReturnType.INVERTED
        assert combined is not None
        # VALUE is 6, INVERTED is 0x10000, combined should be 0x10006
        assert int(combined) == 0x10006

    def test_bitwise_or_count(self):
        """Test bitwise OR with COUNT."""
        combined = ListReturnType.COUNT | ListReturnType.INVERTED
        assert int(combined) == 0x10005  # COUNT is 5

    def test_bitwise_or_index(self):
        """Test bitwise OR with INDEX."""
        combined = ListReturnType.INDEX | ListReturnType.INVERTED
        assert int(combined) == 0x10001  # INDEX is 1

    def test_bitwise_or_rank(self):
        """Test bitwise OR with RANK."""
        combined = ListReturnType.RANK | ListReturnType.INVERTED
        assert int(combined) == 0x10003  # RANK is 3

    def test_bitwise_and(self):
        """Test that bitwise AND works."""
        combined = ListReturnType.VALUE | ListReturnType.INVERTED
        # AND with INVERTED should give INVERTED
        result = combined & ListReturnType.INVERTED
        assert int(result) == 0x10000

    def test_equality(self):
        """Test that equality comparison works."""
        assert ListReturnType.VALUE == ListReturnType.VALUE
        assert ListReturnType.VALUE != ListReturnType.COUNT
        assert ListReturnType.INVERTED == ListReturnType.INVERTED

    def test_equality_combined(self):
        """Test equality for combined values."""
        combined1 = ListReturnType.VALUE | ListReturnType.INVERTED
        combined2 = ListReturnType.VALUE | ListReturnType.INVERTED
        assert combined1 == combined2

    def test_inequality_combined(self):
        """Test inequality for different combined values."""
        combined1 = ListReturnType.VALUE | ListReturnType.INVERTED
        combined2 = ListReturnType.COUNT | ListReturnType.INVERTED
        assert combined1 != combined2

    def test_hash(self):
        """Test that values are hashable."""
        hash_val = hash(ListReturnType.VALUE)
        assert isinstance(hash_val, int)

    def test_hash_combined(self):
        """Test that combined values are hashable."""
        combined = ListReturnType.VALUE | ListReturnType.INVERTED
        hash_val = hash(combined)
        assert isinstance(hash_val, int)

    def test_repr(self):
        """Test string representation."""
        assert "VALUE" in repr(ListReturnType.VALUE)

    def test_repr_combined(self):
        """Test string representation of combined value."""
        combined = ListReturnType.VALUE | ListReturnType.INVERTED
        repr_str = repr(combined)
        assert "VALUE" in repr_str
        assert "INVERTED" in repr_str

    def test_int_conversion(self):
        """Test integer conversion."""
        assert int(ListReturnType.NONE) == 0
        assert int(ListReturnType.INDEX) == 1
        assert int(ListReturnType.REVERSE_INDEX) == 2
        assert int(ListReturnType.RANK) == 3
        assert int(ListReturnType.REVERSE_RANK) == 4
        assert int(ListReturnType.COUNT) == 5
        assert int(ListReturnType.VALUE) == 6
        assert int(ListReturnType.EXISTS) == 7
        assert int(ListReturnType.INVERTED) == 0x10000


class TestMapReturnType:
    """Tests for MapReturnType class."""

    def test_basic_values(self):
        """Test that basic return type values are defined."""
        assert MapReturnType.NONE is not None
        assert MapReturnType.INDEX is not None
        assert MapReturnType.REVERSE_INDEX is not None
        assert MapReturnType.RANK is not None
        assert MapReturnType.REVERSE_RANK is not None
        assert MapReturnType.COUNT is not None
        assert MapReturnType.KEY is not None
        assert MapReturnType.VALUE is not None
        assert MapReturnType.KEY_VALUE is not None
        assert MapReturnType.EXISTS is not None
        assert MapReturnType.UNORDERED_MAP is not None
        assert MapReturnType.ORDERED_MAP is not None
        assert MapReturnType.INVERTED is not None

    def test_bitwise_or(self):
        """Test that bitwise OR works for combining with INVERTED."""
        combined = MapReturnType.VALUE | MapReturnType.INVERTED
        assert combined is not None
        # VALUE is 7, INVERTED is 0x10000, combined should be 0x10007
        assert int(combined) == 0x10007

    def test_bitwise_or_count(self):
        """Test bitwise OR with COUNT."""
        combined = MapReturnType.COUNT | MapReturnType.INVERTED
        assert int(combined) == 0x10005  # COUNT is 5

    def test_bitwise_or_key(self):
        """Test bitwise OR with KEY."""
        combined = MapReturnType.KEY | MapReturnType.INVERTED
        assert int(combined) == 0x10006  # KEY is 6

    def test_bitwise_or_key_value(self):
        """Test bitwise OR with KEY_VALUE."""
        combined = MapReturnType.KEY_VALUE | MapReturnType.INVERTED
        assert int(combined) == 0x10008  # KEY_VALUE is 8

    def test_bitwise_and(self):
        """Test that bitwise AND works."""
        combined = MapReturnType.VALUE | MapReturnType.INVERTED
        result = combined & MapReturnType.INVERTED
        assert int(result) == 0x10000

    def test_equality(self):
        """Test that equality comparison works."""
        assert MapReturnType.VALUE == MapReturnType.VALUE
        assert MapReturnType.VALUE != MapReturnType.COUNT
        assert MapReturnType.INVERTED == MapReturnType.INVERTED

    def test_equality_combined(self):
        """Test equality for combined values."""
        combined1 = MapReturnType.VALUE | MapReturnType.INVERTED
        combined2 = MapReturnType.VALUE | MapReturnType.INVERTED
        assert combined1 == combined2

    def test_inequality_combined(self):
        """Test inequality for different combined values."""
        combined1 = MapReturnType.VALUE | MapReturnType.INVERTED
        combined2 = MapReturnType.COUNT | MapReturnType.INVERTED
        assert combined1 != combined2

    def test_hash(self):
        """Test that values are hashable."""
        hash_val = hash(MapReturnType.VALUE)
        assert isinstance(hash_val, int)

    def test_hash_combined(self):
        """Test that combined values are hashable."""
        combined = MapReturnType.VALUE | MapReturnType.INVERTED
        hash_val = hash(combined)
        assert isinstance(hash_val, int)

    def test_repr(self):
        """Test string representation."""
        assert "VALUE" in repr(MapReturnType.VALUE)

    def test_repr_combined(self):
        """Test string representation of combined value."""
        combined = MapReturnType.VALUE | MapReturnType.INVERTED
        repr_str = repr(combined)
        assert "VALUE" in repr_str
        assert "INVERTED" in repr_str

    def test_int_conversion(self):
        """Test integer conversion."""
        assert int(MapReturnType.NONE) == 0
        assert int(MapReturnType.INDEX) == 1
        assert int(MapReturnType.REVERSE_INDEX) == 2
        assert int(MapReturnType.RANK) == 3
        assert int(MapReturnType.REVERSE_RANK) == 4
        assert int(MapReturnType.COUNT) == 5
        assert int(MapReturnType.KEY) == 6
        assert int(MapReturnType.VALUE) == 7
        assert int(MapReturnType.KEY_VALUE) == 8
        assert int(MapReturnType.EXISTS) == 9
        assert int(MapReturnType.UNORDERED_MAP) == 10
        assert int(MapReturnType.ORDERED_MAP) == 11
        assert int(MapReturnType.INVERTED) == 0x10000


class TestReturnTypeUsageInExpressions:
    """Test that return types can be used in FilterExpression methods."""

    def test_list_return_type_in_expression(self):
        """Test ListReturnType can be passed to FilterExpression methods."""
        from aerospike_async import FilterExpression, ExpType

        # This should not raise - basic return type
        expr = FilterExpression.list_get_by_index(
            ListReturnType.VALUE,
            ExpType.INT,
            FilterExpression.int_val(0),
            FilterExpression.list_bin("test"),
            [],
        )
        assert expr is not None

    def test_list_return_type_inverted_in_expression(self):
        """Test combined ListReturnType can be passed to FilterExpression methods."""
        from aerospike_async import FilterExpression, ExpType

        # This should not raise - combined return type with INVERTED
        combined = ListReturnType.VALUE | ListReturnType.INVERTED
        expr = FilterExpression.list_get_by_index(
            combined,
            ExpType.INT,
            FilterExpression.int_val(0),
            FilterExpression.list_bin("test"),
            [],
        )
        assert expr is not None

    def test_map_return_type_in_expression(self):
        """Test MapReturnType can be passed to FilterExpression methods."""
        from aerospike_async import FilterExpression, ExpType

        # This should not raise - basic return type
        expr = FilterExpression.map_get_by_key(
            MapReturnType.VALUE,
            ExpType.INT,
            FilterExpression.string_val("key"),
            FilterExpression.map_bin("test"),
            [],
        )
        assert expr is not None

    def test_map_return_type_inverted_in_expression(self):
        """Test combined MapReturnType can be passed to FilterExpression methods."""
        from aerospike_async import FilterExpression, ExpType

        # This should not raise - combined return type with INVERTED
        combined = MapReturnType.VALUE | MapReturnType.INVERTED
        expr = FilterExpression.map_get_by_key(
            combined,
            ExpType.INT,
            FilterExpression.string_val("key"),
            FilterExpression.map_bin("test"),
            [],
        )
        assert expr is not None
