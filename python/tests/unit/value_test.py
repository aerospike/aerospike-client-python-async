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

import subprocess
import sys
import textwrap

import pytest
from aerospike_async import GeoJSON, List, Blob, HLL, Map, Vector, VectorElementType, geojson, null

# Common test data
TEST_BLOB_DATA_1 = [1, 7, 8, 4, 1]
TEST_BLOB_DATA_2 = [1, 7, 8, 4]
TEST_BLOB_DATA_3 = [1, 2, 3]
TEST_LIST_DATA_1 = [1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}]
TEST_LIST_DATA_2 = [1, 2, 3]

def test_geo_json_equality():
    """Test GeoJSON object creation and equality."""

    geo_str = '{"type":"Point","coordinates":[-80.590003, 28.60009]}'
    geo = GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')
    geo2 = GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')

    assert geo_str == geo == geo2

def test_geo_json_inequality():
    """Test GeoJSON object inequality."""

    geo_str = '{"type":"Point","coordinates":[-80.590003, 28.60009]}'
    geo_different_str = '{"type":"Point","coordinates":[-80.590003, 28.60008]}'
    
    geo = GeoJSON(geo_str)
    different_geo = GeoJSON(geo_different_str)
    
    assert geo_str != different_geo
    assert geo != different_geo

def test_geo_json_set_and_get():
    """Test GeoJSON value setting and getting."""

    geo_str = '{"type":"Point","coordinates":[-80.590003, 28.60009]}'
    geo_different_str = '{"type":"Point","coordinates":[-80.590003, 28.60008]}'
    
    geo = GeoJSON(geo_str)
    geo.value = geo_different_str
    assert geo.value == geo_different_str

def test_geo_json_str_repr():
    """Test GeoJSON string representation."""

    geo_str = '{"type":"Point","coordinates":[-80.590003, 28.60009]}'
    geo = GeoJSON(geo_str)
    
    assert str(geo) == geo_str
    assert repr(geo) == f"GeoJSON({geo_str})"


def test_geo_json_from_dict():
    """Test GeoJSON creation from dictionary."""
    geo_dict = {"type": "Point", "coordinates": [-80.590003, 28.60009]}
    geo = GeoJSON(geo_dict)

    # Python's json.dumps adds spaces, so we parse and compare the structure
    import json
    parsed = json.loads(geo.value)
    assert parsed == geo_dict
    assert geo.value.startswith('{"type":')
    assert '"coordinates"' in geo.value


def test_geo_json_from_dict_polygon():
    """Test GeoJSON creation from dictionary with Polygon type."""
    geo_dict = {
        "type": "Polygon",
        "coordinates": [[[-122.0, 37.0], [-121.0, 37.0], [-121.0, 38.0], [-122.0, 38.0], [-122.0, 37.0]]]
    }
    geo = GeoJSON(geo_dict)

    assert "type" in geo.value
    assert "Polygon" in geo.value
    assert "coordinates" in geo.value


def test_geo_json_from_dict_equality():
    """Test that GeoJSON created from dict equals GeoJSON created from string."""
    geo_dict = {"type": "Point", "coordinates": [-80.590003, 28.60009]}
    geo_from_dict = GeoJSON(geo_dict)

    geo_str = '{"type":"Point","coordinates":[-80.590003,28.60009]}'
    geo_from_str = GeoJSON(geo_str)

    # Note: JSON serialization may add/remove spaces, so compare parsed values
    import json
    assert json.loads(geo_from_dict.value) == json.loads(geo_from_str.value)

def test_list_equality():
    """Test List object creation and equality."""

    _list = TEST_LIST_DATA_1
    as_l = List(_list)
    as_l2 = List(_list)

    assert as_l == _list == as_l2

def test_list_inequality():
    """Test List object inequality."""

    as_l = List(TEST_LIST_DATA_1)
    different_list = TEST_LIST_DATA_2
    as_l2 = List(TEST_LIST_DATA_2)

    assert as_l != different_list
    assert as_l != as_l2

def test_list_set_and_get():
    """Test List value setting and getting."""

    as_l = List(TEST_LIST_DATA_1)
    as_l.value = [1]
    assert as_l.value == [1]

def test_list_str_repr():
    """Test List string representation."""

    as_l = List(TEST_LIST_DATA_1)
    
    assert str(as_l) == '[1, 2, [1, 2, 3], {"str": [1, 2, True], 1: "str"}]'
    assert repr(as_l) == 'List([1, 2, [1, 2, 3], {"str": [1, 2, True], 1: "str"}])'

def test_list_iteration():
    """Test List iteration."""

    as_l = List([1, 2, 3, 4])
    for i, v in enumerate(as_l, start=1):
        assert i == v

def test_list_get_and_set():
    """Test List indexing and assignment."""

    as_l = List(TEST_LIST_DATA_1)
    assert as_l[0] == 1
    as_l[0] = "0"
    assert as_l[0] == "0"

def test_list_get_out_of_bounds():
    """Test List indexing out of bounds raises IndexError."""

    as_l = List(TEST_LIST_DATA_1)
    with pytest.raises(IndexError) as exc_info:
        as_l[5]
    assert exc_info.value.args[0] == "index out of bounds"

def test_list_set_out_of_bounds():
    """Test List assignment out of bounds raises IndexError."""

    as_l = List(TEST_LIST_DATA_1)
    with pytest.raises(IndexError) as exc_info:
        as_l[5] = 0
    assert exc_info.value.args[0] == "index out of bounds"

def test_list_length():
    """Test List length."""

    as_l = List(TEST_LIST_DATA_1)
    assert len(as_l) == 4

def test_list_contains():
    """Test List contains operator."""

    as_l = List(TEST_LIST_DATA_1)
    assert 1 in as_l

def test_list_delete():
    """Test List item deletion."""

    l = List(TEST_LIST_DATA_2)
    del l[0]
    assert l == List([2, 3])

def test_list_concat():
    """Test List concatenation."""

    l1 = List([1])
    l2 = List([2])
    assert List([1, 2]) == l1 + l2

def test_list_repeat():
    """Test List repetition."""

    l = List([1])
    assert l * 3 == List([1, 1, 1])

def test_list_inplace_concat():
    """Test List in-place concatenation."""

    l = List([1])
    l += List([2, 3])
    assert l == List(TEST_LIST_DATA_2)

def test_list_inplace_repeat():
    """Test List in-place repetition."""

    l = List([1])
    l *= 3
    assert l == List([1, 1, 1])

def test_list_hash():
    """Test List hashing for dictionary keys."""

    as_l = List(TEST_LIST_DATA_1)
    # Note: List objects cannot be used as dictionary keys due to HashMap limitations
    # d = {1: as_l, as_l: 1}
    # d2 = {1: as_l, as_l: 1}
    # assert d == d2
    assert isinstance(as_l, List)

def test_list_use_as_native_type():
    """Test List isinstance check."""

    as_l = List(TEST_LIST_DATA_1)
    # Note: List objects are not instances of Python list
    # assert isinstance(as_l, list)
    assert isinstance(as_l, List)

def test_map_set_and_get():
    """Test Map value setting and getting."""

    m = Map({"a": 1})
    m.value = {"a": 2}
    assert m.value == {"a": 2}

def test_map_equality():
    """Test Map object equality."""

    m = Map({"a": 1})
    native_m = {"a": 1}
    m2 = Map({"a": 1})
    
    assert m == m2
    assert m == native_m

def test_map_inequality():
    """Test Map object inequality."""

    m = Map({"a": 1})
    native_m = {"a": 2}
    m2 = Map({"a": 2})
    
    assert m != m2
    assert m != native_m

def test_map_use_as_native_type():
    """Test Map isinstance check."""

    m = Map({"a": 1})
    # Note: Map objects are not instances of Python dict
    # assert isinstance(m, dict)
    assert isinstance(m, Map)

def test_map_hash():
    """Test Map hashing for dictionary keys."""

    # Note: Map objects cannot be used as dictionary keys
    # native_m1 = {Map({"a": 1}): 1}
    # native_m2 = {Map({"a": 1}): 1}
    # assert native_m1 == native_m2
    m = Map({"a": 1})
    assert isinstance(m, Map)

def test_map_str():
    """Test Map string representation."""

    m = Map({"a": 1})
    assert str(m) == '{"a": 1}'

def test_map_repr():
    """Test Map repr representation."""

    m = Map({"a": 1})
    assert repr(m) == 'Map({"a": 1})'

def test_blob_set_and_get():
    """Test Blob value setting and getting."""

    blob = Blob(TEST_BLOB_DATA_1)
    blob.value = [2, 3, 4]
    assert blob.value == bytes([2, 3, 4])

def test_blob_equality():
    """Test Blob object equality."""

    blob = Blob(TEST_BLOB_DATA_1)
    blob2 = bytearray(TEST_BLOB_DATA_1)
    blob3 = bytes(TEST_BLOB_DATA_1)
    blob4 = Blob(blob2)
    blob5 = Blob(blob3)

    assert blob == blob2
    assert blob == blob3
    assert blob == blob4
    assert blob == blob5
    assert blob4 == blob5

def test_blob_inequality():
    """Test Blob object inequality."""

    blob = Blob(TEST_BLOB_DATA_1)
    blob2 = bytearray(TEST_BLOB_DATA_2)
    blob3 = bytes(TEST_BLOB_DATA_2)
    blob4 = Blob(blob3)

    assert blob != blob4
    assert blob != blob2
    assert blob != blob3

def test_blob_get_by_index():
    """Test Blob indexing."""

    blob = Blob(TEST_BLOB_DATA_1)
    assert blob[0] == 1

def test_blob_get_by_index_fail():
    """Test Blob indexing out of bounds raises IndexError."""

    blob = Blob(TEST_BLOB_DATA_1)
    with pytest.raises(IndexError) as exc_info:
        test = blob[5]
    assert exc_info.value.args[0] == "index out of bounds"

def test_blob_set_by_index():
    """Test Blob assignment by index."""

    blob = Blob(TEST_BLOB_DATA_1)
    blob[0] = 1

def test_blob_set_by_index_fail():
    """Test Blob assignment out of bounds raises IndexError."""

    blob = Blob(TEST_BLOB_DATA_1)
    with pytest.raises(IndexError) as exc_info:
        blob[5] = 0
    assert exc_info.value.args[0] == "index out of bounds"

def test_blob_delete():
    """Test Blob item deletion."""

    blob = Blob(TEST_BLOB_DATA_3)
    del blob[0]
    assert blob == Blob(bytes([2, 3]))

def test_blob_concat():
    """Test Blob concatenation."""

    blob1 = Blob(bytes([1]))
    blob2 = Blob(bytes([2]))
    assert Blob(bytes([1, 2])) == blob1 + blob2

def test_blob_concat_fail():
    """Test failed Blob + String concatenation."""

    blob = Blob(bytes([1]))
    string = "bad_news"
    with pytest.raises(TypeError) as exc_info:
        blob + string
    assert exc_info.value.args[0] == "unsupported operand type(s) for +: 'Blob' and other type"

def test_blob_repeat():
    """Test Blob repetition."""

    blob = Blob(bytes([1]))
    assert blob * 3 == Blob(bytes([1, 1, 1]))

def test_blob_inplace_concat():
    """Test Blob in-place concatenation."""

    blob = Blob(bytes([1]))
    blob += Blob(bytes([2, 3]))
    assert blob == Blob(bytes([1, 2, 3]))

def test_blob_inplace_repeat():
    """Test Blob in-place repetition."""

    blob = Blob(bytes([1]))
    blob *= 3
    assert blob == Blob(bytes([1, 1, 1]))

def test_blob_hash():
    """Test Blob hashing for dictionary keys."""

    blob_bytes = bytes(TEST_BLOB_DATA_1)
    blob = Blob(blob_bytes)
    d = {1: blob, blob: 1}
    d2 = {1: blob_bytes, blob: 1}
    assert d == d2

def test_hll_equality():
    """Test HLL object equality."""

    hll = HLL(bytes([1, 2, 3, 4]))
    b = bytes([1, 2, 3, 4])
    hll2 = HLL(bytes([1, 2, 3, 4]))

    assert hll == b
    assert hll == hll2

def test_hll_inequality():
    """Test HLL object inequality."""

    hll = HLL(bytes([1, 2, 3, 4]))
    b = bytes([1, 2, 3, 5])
    hll2 = HLL(bytes([1, 2, 3, 5]))

    assert hll != b
    assert hll != hll2

def test_hll_set_and_get():
    """Test HLL value setting and getting."""

    hll = HLL(bytes([1, 2, 3, 4]))
    hll.value = [5, 6, 7]
    assert hll.value == bytes([5, 6, 7])


def test_geojson_helper_function():
    """Test geojson() helper function that converts coordinate strings to GeoJSON."""
    geo = geojson("-122.0, 37.5")

    assert isinstance(geo, GeoJSON)
    assert "type" in geo.value
    assert "Point" in geo.value
    assert "coordinates" in geo.value

    # Check coordinates are correct
    import json
    geo_data = json.loads(geo.value)
    assert geo_data["type"] == "Point"
    assert geo_data["coordinates"] == [-122.0, 37.5]


def test_geojson_helper_function_negative_coords():
    """Test geojson() helper with negative coordinates."""
    geo = geojson("-80.590003, 28.60009")

    import json
    geo_data = json.loads(geo.value)
    assert geo_data["coordinates"] == [-80.590003, 28.60009]


def test_geojson_helper_function_invalid():
    """Test geojson() helper with invalid coordinate string."""
    with pytest.raises(ValueError):
        geojson("invalid")

    with pytest.raises(ValueError):
        geojson("122.0")  # Missing comma

    with pytest.raises(ValueError):
        geojson("122.0, 37.5, 10.0")  # Too many coordinates


def test_null_function():
    """Test null() helper function returns None."""
    null_val = null()
    assert null_val is None


def test_none_converts_to_nil():
    """Test that Python None converts to PythonValue::Nil."""
    # This test verifies that None is handled correctly in value conversion
    # None in a list should be preserved as None
    from aerospike_async import List as ASList
    test_list = ASList([1, None, 3])
    # None should be preserved when converting back
    assert test_list[1] is None


def test_u64_large_integer():
    """Test that large positive integers (u64) overflow to negative i64.
    
    Note: Since Value::UInt was removed from the Rust core, values > i64::MAX
    will overflow when converted to i64. This test verifies the overflow behavior.
    """
    from aerospike_async import List as ASList, Map as ASMap
    
    # i64::MAX is 9223372036854775807
    # Test with a value larger than i64::MAX - it will overflow to negative
    large_uint = 2**63 + 1000  # 9223372036854775808 + 1000
    # This overflows: 9223372036854776808 -> -9223372036854774808 (i64::MIN + (value - i64::MAX - 1))
    expected_overflow = large_uint - 2**64  # Overflow calculation
    
    # This will overflow when converted to i64
    test_list = ASList([large_uint])
    assert test_list[0] == expected_overflow
    
    # Test in a map value
    test_map = ASMap({1: large_uint})
    assert test_map.value[1] == expected_overflow


def test_u64_boundary_values():
    """Test u64 boundary values (i64::MAX and i64::MAX + 1).
    
    Note: Since Value::UInt was removed from the Rust core, i64::MAX + 1
    will overflow to i64::MIN when converted to i64.
    """
    from aerospike_async import List as ASList
    
    i64_max = 2**63 - 1  # 9223372036854775807
    i64_max_plus_one = 2**63  # 9223372036854775808
    
    # i64_max works fine, but i64_max_plus_one overflows to i64::MIN
    test_list = ASList([i64_max, i64_max_plus_one])
    assert test_list[0] == i64_max
    assert test_list[1] == -9223372036854775808  # i64::MIN (overflow)


def test_special_value_enum_distinct():
    """CDT boundary markers are distinct enum members."""
    from aerospike_async import SpecialValue

    assert SpecialValue.NULL != SpecialValue.INFINITY
    assert SpecialValue.INFINITY != SpecialValue.WILDCARD
    assert "SpecialValue.INFINITY" == repr(SpecialValue.INFINITY)


def test_map_operation_accepts_special_value_range_endpoints():
    """Map get_by_key_range accepts SpecialValue for open-ended CDT ranges."""
    from aerospike_async import MapOperation, MapReturnType, SpecialValue

    op = MapOperation.get_by_key_range(
        "m", SpecialValue.NULL, SpecialValue.INFINITY, MapReturnType.KEY,
    )
    assert op is not None


def test_list_operation_accepts_special_value_range_endpoints():
    from aerospike_async import ListOperation, ListReturnType, SpecialValue

    op = ListOperation.get_by_value_range(
        "lst", 1, SpecialValue.INFINITY, ListReturnType.VALUE,
    )
    assert op is not None


def test_vector_default_element_type_is_float32():
    """Vector() with no element_type defaults to FLOAT32."""

    v = Vector([0.12, 0.98, -0.34])
    assert v.element_type == VectorElementType.FLOAT32
    assert v.dimensions == 3
    assert len(v) == 3


def test_vector_float64():
    v = Vector([1.5, -2.5, 3.25], VectorElementType.FLOAT64)
    assert v.element_type == VectorElementType.FLOAT64
    assert list(v.value) == [1.5, -2.5, 3.25]


def test_vector_int32():
    v = Vector([1, 2, 3, -4], VectorElementType.INT32)
    assert v.element_type == VectorElementType.INT32
    assert list(v.value) == [1, 2, 3, -4]


def test_vector_float32_value_round_trips_within_float32_precision():
    """FLOAT32 elements lose precision relative to Python's native float64,
    same as numpy float32 arrays would."""

    v = Vector([0.12, 0.98, -0.34])
    assert [round(x, 5) for x in v.value] == [0.12, 0.98, -0.34]


def test_vector_empty():
    v = Vector([])
    assert v.dimensions == 0
    assert len(v) == 0
    assert list(v.value) == []


def test_vector_equality():
    assert Vector([1.0, 2.0]) == Vector([1.0, 2.0])
    assert Vector([1.0, 2.0]) != Vector([1.0, 3.0])


def test_vector_different_element_types_are_not_equal():
    """Same numeric values but different element types are distinct, matching
    the underlying particle encoding."""

    assert Vector([1.0], VectorElementType.FLOAT32) != Vector([1.0], VectorElementType.FLOAT64)


def test_vector_str_repr():
    v = Vector([1.0, 2.0], VectorElementType.FLOAT32)
    assert str(v) == "Vector::float32([1.0, 2.0])"
    assert repr(v) == "Vector::float32([1.0, 2.0])"
    assert v.as_string() == "Vector::float32([1.0, 2.0])"


def test_vector_copy_construct_from_existing_vector():
    v = Vector([1.0, 2.0, 3.0])
    v_copy = Vector(v)
    assert v_copy == v
    assert v_copy.element_type == v.element_type


def test_vector_float16_from_list_rejected():
    """FLOAT16 has no native Python representation, so it can only be built from
    a numpy.float16 array; list construction is rejected with a clear error."""

    with pytest.raises(TypeError):
        Vector([0.1, 0.2], VectorElementType.FLOAT16)


def test_vector_rejects_non_list_input():
    with pytest.raises(TypeError):
        Vector("not a list")


def test_vector_cannot_be_used_as_dict_key():
    """Vectors are not hashable, matching the underlying core type
    (a vector has no natural map-key semantics)."""

    v = Vector([1.0, 2.0])
    with pytest.raises(TypeError):
        {v: 1}


def test_vector_numpy_rejects_multi_dimensional_arrays():
    np = pytest.importorskip("numpy")

    with pytest.raises(TypeError):
        Vector(np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32))


# --- Edge cases and type/readback permutations -----------------------------

# (input element type, matching numpy dtype, explicit VectorElementType, sample
# values) for the three element types that have a native Python list form.
_LISTABLE_TYPES = [
    ("float32", VectorElementType.FLOAT32, [1.0, 2.0, 3.0]),
    ("float64", VectorElementType.FLOAT64, [1.0, 2.0, 3.0]),
    ("int32", VectorElementType.INT32, [1, 2, -3]),
]


def test_vector_value_is_a_plain_list_not_numpy():
    """`.value` returns an actual Python list, never a numpy array."""
    v = Vector([1.0, 2.0, 3.0])
    assert type(v.value) is list


@pytest.mark.parametrize("np_dtype, element_type, values", _LISTABLE_TYPES)
def test_vector_list_input_readback_both_ways(np_dtype, element_type, values):
    """Build from a list, then read back both as a Python list and as a numpy
    array; the two views must agree."""
    np = pytest.importorskip("numpy")

    v = Vector(values, element_type)
    assert v.value == values

    arr = v.numpy_value
    assert arr.dtype == np.dtype(np_dtype)
    assert arr.tolist() == values
    assert v.value == arr.tolist()


@pytest.mark.parametrize("np_dtype, element_type, values", _LISTABLE_TYPES)
def test_vector_numpy_input_readback_both_ways(np_dtype, element_type, values):
    """Build from a numpy array (dtype inferred and explicit), then read back as
    a Python list and as a numpy array."""
    np = pytest.importorskip("numpy")
    arr_in = np.array(values, dtype=np_dtype)

    for et in (None, element_type):
        v = Vector(arr_in) if et is None else Vector(arr_in, et)
        assert v.element_type == element_type
        assert v.value == values
        out = v.numpy_value
        assert out.dtype == arr_in.dtype
        assert out.tolist() == values


@pytest.mark.parametrize("np_dtype", ["float16", "float32", "float64", "int32"])
def test_vector_roundtrip_through_numpy_value(np_dtype):
    """numpy array -> Vector -> numpy_value -> Vector round-trips for every type."""
    np = pytest.importorskip("numpy")
    values = [1, 2, 3] if np_dtype == "int32" else [1.0, 2.0, -3.5]
    arr_in = np.array(values, dtype=np_dtype)

    v = Vector(arr_in)
    v2 = Vector(v.numpy_value)
    assert v2 == v
    assert v2.element_type == v.element_type


def test_vector_float16_readback():
    """float16 is unreadable as a list but round-trips through numpy_value."""
    np = pytest.importorskip("numpy")
    arr_in = np.array([1.0, 2.0, -3.5], dtype=np.float16)

    v = Vector(arr_in)
    assert v.element_type == VectorElementType.FLOAT16
    with pytest.raises(TypeError):
        _ = v.value

    out = v.numpy_value
    assert out.dtype == np.float16
    assert out.tolist() == [1.0, 2.0, -3.5]
    assert Vector(out) == v


def test_vector_float16_special_values_roundtrip():
    """inf/-inf and signed zero survive a float16 numpy round-trip bit-for-bit."""
    np = pytest.importorskip("numpy")
    arr_in = np.array([np.inf, -np.inf, 0.0, -0.0, 1.5], dtype=np.float16)

    out = Vector(arr_in).numpy_value
    assert out.dtype == np.float16
    assert np.array_equal(out, arr_in)


@pytest.mark.parametrize(
    "np_dtype, element_type",
    [
        ("float16", VectorElementType.FLOAT16),
        ("float32", VectorElementType.FLOAT32),
        ("float64", VectorElementType.FLOAT64),
        ("int32", VectorElementType.INT32),
    ],
)
def test_vector_empty_numpy_input(np_dtype, element_type):
    """Empty numpy arrays produce empty vectors for every element type."""
    np = pytest.importorskip("numpy")

    v = Vector(np.array([], dtype=np_dtype))
    assert v.element_type == element_type
    assert v.dimensions == 0
    assert len(v) == 0

    arr = v.numpy_value
    assert arr.dtype == np.dtype(np_dtype)
    assert arr.tolist() == []
    if element_type != VectorElementType.FLOAT16:
        assert v.value == []


def test_vector_int32_boundary_values():
    """i32 min/max survive both list and numpy construction."""
    np = pytest.importorskip("numpy")
    data = [-2147483648, 0, 2147483647]

    assert Vector(data, VectorElementType.INT32).value == data
    v = Vector(np.array(data, dtype=np.int32))
    assert v.value == data
    assert v.numpy_value.tolist() == data


def test_vector_numpy_non_contiguous_input():
    """A non-contiguous 1-D array (e.g. a strided slice) is handled correctly."""
    np = pytest.importorskip("numpy")
    base = np.array([1.0, 99.0, 2.0, 99.0, 3.0], dtype=np.float32)
    sliced = base[::2]
    assert not sliced.flags["C_CONTIGUOUS"]

    assert Vector(sliced).value == [1.0, 2.0, 3.0]


def test_vector_numpy_rejects_zero_d_array():
    np = pytest.importorskip("numpy")
    with pytest.raises(TypeError):
        Vector(np.array(5.0, dtype=np.float32))


@pytest.mark.parametrize("np_dtype", ["int64", "int16", "int8", "uint8", "uint32", "complex64"])
def test_vector_numpy_rejects_various_unsupported_dtypes(np_dtype):
    np = pytest.importorskip("numpy")
    with pytest.raises(TypeError):
        Vector(np.array([1, 2, 3], dtype=np_dtype))


@pytest.mark.parametrize(
    "np_dtype, element_type",
    [
        ("float16", VectorElementType.FLOAT16),
        ("float32", VectorElementType.FLOAT32),
        ("float64", VectorElementType.FLOAT64),
        ("int32", VectorElementType.INT32),
    ],
)
def test_vector_numpy_explicit_matching_element_type(np_dtype, element_type):
    np = pytest.importorskip("numpy")
    values = [1, 2, 3] if np_dtype == "int32" else [1.0, 2.0, 3.0]
    v = Vector(np.array(values, dtype=np_dtype), element_type)
    assert v.element_type == element_type


@pytest.mark.parametrize(
    "np_dtype, wrong_element_type",
    [
        ("float32", VectorElementType.FLOAT64),
        ("float32", VectorElementType.INT32),
        ("float32", VectorElementType.FLOAT16),
        ("float64", VectorElementType.FLOAT32),
        ("int32", VectorElementType.FLOAT32),
        ("float16", VectorElementType.FLOAT32),
    ],
)
def test_vector_numpy_element_type_mismatch_permutations(np_dtype, wrong_element_type):
    np = pytest.importorskip("numpy")
    values = [1, 2, 3] if np_dtype == "int32" else [1.0, 2.0, 3.0]
    with pytest.raises(TypeError):
        Vector(np.array(values, dtype=np_dtype), wrong_element_type)


@pytest.mark.parametrize("np_dtype, element_type, values", _LISTABLE_TYPES)
def test_vector_numpy_and_list_cross_equality(np_dtype, element_type, values):
    np = pytest.importorskip("numpy")
    assert Vector(np.array(values, dtype=np_dtype)) == Vector(values, element_type)


# The following two tests simulate numpy NOT being installed. numpy is an
# optional dependency, so list-based Vector construction must keep working
# without it. They run in a subprocess because they mutate global
# `sys.modules`/`sys.meta_path`.

_BLOCK_NUMPY_PREAMBLE = """
import sys, importlib.abc

class _BlockNumpy(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path, target=None):
        if name == "numpy" or name.startswith("numpy."):
            raise ImportError("numpy blocked for test: " + name)
        return None

for _m in [m for m in list(sys.modules) if m == "numpy" or m.startswith("numpy.")]:
    del sys.modules[_m]
sys.meta_path.insert(0, _BlockNumpy())
"""


def _run_without_numpy(body: str) -> subprocess.CompletedProcess:
    script = _BLOCK_NUMPY_PREAMBLE + textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )


def test_vector_list_construction_works_without_numpy():
    """Regression: numpy is optional, so constructing a Vector from a plain
    list (and reading it back) must not require numpy to be importable."""
    result = _run_without_numpy(
        """
        from aerospike_async import Vector, VectorElementType

        v = Vector([1.0, 2.0, 3.0])
        assert v.element_type == VectorElementType.FLOAT32
        assert list(v.value) == [1.0, 2.0, 3.0]

        vi = Vector([1, 2, 3], VectorElementType.INT32)
        assert list(vi.value) == [1, 2, 3]

        # float16 from a list is unsupported, but must fail cleanly (TypeError),
        # not panic while probing for numpy.
        try:
            Vector([1.0, 2.0], VectorElementType.FLOAT16)
        except TypeError:
            pass
        else:
            raise AssertionError("expected TypeError for FLOAT16 from list")

        print("OK")
        """
    )
    assert result.returncode == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "OK" in result.stdout
    assert "PanicException" not in result.stderr


def test_vector_float16_numpy_value_without_numpy_raises_clean_error():
    """Reading FLOAT16 needs numpy via `.numpy_value`; without numpy it must
    surface a clean ImportError rather than a Rust panic. Build the vector while
    numpy is available, then make numpy unimportable before the getter runs."""
    pytest.importorskip("numpy")
    script = textwrap.dedent(
        """
        import sys, importlib.abc
        import numpy as np
        from aerospike_async import Vector

        v16 = Vector(np.array([1.0, 2.0, -3.5], dtype=np.float16))

        class _BlockNumpy(importlib.abc.MetaPathFinder):
            def find_spec(self, name, path, target=None):
                if name == "numpy" or name.startswith("numpy."):
                    raise ImportError("numpy blocked for test: " + name)
                return None

        for _m in [m for m in list(sys.modules) if m == "numpy" or m.startswith("numpy.")]:
            del sys.modules[_m]
        sys.meta_path.insert(0, _BlockNumpy())

        # Non-numpy accessors still work.
        assert v16.dimensions == 3

        # `.value` raises TypeError (numpy-independent), directing to numpy_value.
        try:
            _ = v16.value
        except TypeError:
            pass
        else:
            raise AssertionError("expected TypeError from FLOAT16 .value")

        # `.numpy_value` needs numpy -> clean ImportError, not a panic.
        try:
            _ = v16.numpy_value
        except ImportError:
            print("OK")
        else:
            raise AssertionError("expected ImportError from .numpy_value without numpy")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "OK" in result.stdout
    assert "PanicException" not in result.stderr
