from aerospike_async import GeoJSON, List, Blob, HLL


def test_geo_json():
    """Test GeoJSON object creation and equality."""

    geo_str = '{"type":"Point","coordinates":[-80.590003, 28.60009]}'
    geo = GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')
    geo2 = GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')

    assert geo_str == geo == geo2


def test_list():
    """Test List object creation, equality, iteration, and assignment."""

    _list = [1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}]

    l = _list
    as_l = List(_list)
    as_l2 = List(_list)

    assert as_l == l == as_l2

    # iteration
    as_l = List([1, 2, 3, 4])
    for i, v in enumerate(as_l, start=1):
        assert i == v

    # assignment
    assert as_l[0] == 1
    as_l[0] = "0"
    assert as_l[0] == "0"

    # def test_list_hash(self):
    #     l = [1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}]
    #     as_l = List([1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}])
    #     # as_l = List([1, 2])
    #     d = {1: as_l, as_l: 1}
    #     d2 = {1: as_l, as_l: 1}

    #     self.assertEqual(d, d2)


def test_blob():
    """Test Blob object creation, equality, and indexing."""

    numbers = [1, 7, 8, 4, 1]

    b = Blob(bytes(numbers))
    b2 = bytearray(numbers)
    b3 = bytes(numbers)

    b4 = Blob(b2)
    b5 = Blob(b3)

    assert b == b2 == b3 == b4 == b5
    assert b[0] == 1
    # assignment
    b[0] = 1


def test_blob_hash():
    """Test Blob object hashing for dictionary keys."""

    bs = bytes([1, 7, 8, 4, 1])
    b = Blob(bs)
    d = {1: b, b: 1}
    d2 = {1: bs, b: 1}

    assert d == d2


def test_hll():
    """Test HLL (HyperLogLog) object creation and equality."""

    numbers = [1, 2, 3, 4]

    b = bytes(numbers)
    hll = HLL(bytes(numbers))
    hll2 = HLL(bytes(numbers))

    assert hll == b == hll2
