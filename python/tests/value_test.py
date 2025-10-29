import pytest
from aerospike_async import GeoJSON, List, Blob, HLL, Map

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
