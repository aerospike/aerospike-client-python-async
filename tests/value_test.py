import unittest

from aerospike_async import GeoJSON, Blob, List, HLL, Map


class TestGeoJSON(unittest.TestCase):
    def setUp(self):
        # 2 different GeoJSON strings
        self.geo_str = '{"type":"Point","coordinates":[-80.590003, 28.60009]}'
        self.geo_different_str = '{"type":"Point","coordinates":[-80.590003, 28.60008]}'

        # Check once that keyword is correct
        self.geo = GeoJSON(
            value='{"type":"Point","coordinates":[-80.590003, 28.60009]}'
        )

    # GeoJSON strings and objects can be compared together
    # Equality and inequality are handled separately in the code, so we need to test both

    def test_equality(self):
        geo2 = GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')
        self.assertEqual(self.geo_str, self.geo)
        self.assertEqual(self.geo, geo2)

    def test_inequality(self):
        different_geo = GeoJSON(self.geo_different_str)
        self.assertNotEqual(self.geo_str, different_geo)
        self.assertNotEqual(self.geo, different_geo)

    def test_set_and_get(self):
        self.geo.value = self.geo_different_str
        self.assertEqual(self.geo.value, self.geo_different_str)

    def test_str_repr(self):
        self.assertEqual(str(self.geo), self.geo_str)
        # TODO: this test might be wrong
        self.assertEqual(repr(self.geo), f"GeoJSON({self.geo_str})")


class TestList(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.as_l = List(value=[1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}])

    def test_equality(self):
        l = [1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}]
        as_l2 = List([1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}])

        self.assertEqual(self.as_l, l)
        self.assertEqual(self.as_l, as_l2)

    def test_inequality(self):
        l = [1, 2, 3]
        as_l2 = List([1, 2, 3])

        self.assertNotEqual(self.as_l, l)
        self.assertNotEqual(self.as_l, as_l2)

    def test_set_and_get(self):
        self.as_l.value = [1]
        self.assertEqual(self.as_l.value, [1])

    def test_str_repr(self):
        self.assertEqual(
            str(self.as_l), '[1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}]'
        )
        self.assertEqual(
            repr(self.as_l), 'List([1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}])'
        )

    def test_iteration(self):
        as_l = List([1, 2, 3, 4])
        for i, v in enumerate(as_l, start=1):
            self.assertEqual(i, v)

    def test_get_and_set(self):
        self.assertEqual(self.as_l[0], 1)
        self.as_l[0] = "0"
        self.assertEqual(self.as_l[0], "0")

    def test_get_out_of_bounds(self):
        with self.assertRaises(IndexError) as cm:
            self.as_l[5]
        self.assertEqual(cm.exception.args[0], "index out of bound")

    def test_set_out_of_bounds(self):
        with self.assertRaises(IndexError) as cm:
            self.as_l[5] = 0
        self.assertEqual(cm.exception.args[0], "index out of bound")

    def test_length(self):
        self.assertEqual(len(self.as_l), 4)

    def test_contains(self):
        self.assertTrue(1 in self.as_l)

    def test_delete(self):
        l = List([1, 2, 3])
        del l[0]
        self.assertEqual(l, List([2, 3]))

    def test_concat(self):
        l1 = List([1])
        l2 = List([2])
        self.assertEqual(List([1, 2]), l1 + l2)

    def test_repeat(self):
        l = List([1])
        self.assertEqual(l * 3, List([1, 1, 1]))

    def test_inplace_concat(self):
        l = List([1])
        l += List([2, 3])
        self.assertEqual(l, List([1, 2, 3]))

    def test_inplace_repeat(self):
        l = List([1])
        l *= 3
        self.assertEqual(l, List([1, 1, 1]))

    def test_list_hash(self):
        as_l = List([1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}])
        d = {1: as_l, as_l: 1}
        d2 = {1: as_l, as_l: 1}

        self.assertEqual(d, d2)

    def test_use_as_native_type(self):
        self.assertEqual(isinstance(self.as_l), list)

class TestMap(unittest.TestCase):
    def setUp(self):
        self.m = Map(value={"a": 1})

    def test_set_and_get(self):
        self.m.value = {"a": 2}
        self.assertEqual(self.m.value, {"a": 2})

    def test_equality(self):
        native_m = {"a": 1}
        m = Map({"a": 1})
        self.assertEqual(self.m, m)
        self.assertEqual(self.m, native_m)

    def test_inequality(self):
        native_m = {"a": 2}
        m = Map({"a": 2})
        self.assertNotEqual(self.m, m)
        self.assertNotEqual(self.m, native_m)

    def test_use_as_native_type(self):
        m = Map({"a": 1})
        self.assertEqual(isinstance(m), dict)

    def test_hash(self):
        native_m1 = {Map({"a": 1}): 1}
        native_m2 = {Map({"a": 1}): 1}
        self.assertEqual(native_m1, native_m2)


class TestBlob(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.b = Blob(value=[1, 7, 8, 4, 1])

    def test_set_and_get(self):
        self.b.value = [2, 3, 4]
        self.assertEqual(self.b.value, [2, 3, 4])

    def test_equality(self):
        b2 = bytearray([1, 7, 8, 4, 1])
        b3 = bytes([1, 7, 8, 4, 1])

        # You can initialize a blob with either a bytes or bytearray type
        b4 = Blob(b2)
        b5 = Blob(b3)

        # Blobs can be compared to bytes or bytearrays
        self.assertEqual(self.b, b2)
        self.assertEqual(self.b, b3)
        # Blobs can be compared with each other
        self.assertEqual(self.b, b4)
        self.assertEqual(self.b, b5)
        self.assertEqual(b4, b5)

    def test_inequality(self):
        b2 = bytearray([1, 7, 8, 4])
        b3 = bytes([1, 7, 8, 4])
        b4 = Blob(b3)

        self.assertNotEqual(self.b, b4)
        self.assertNotEqual(self.b, b2)
        self.assertNotEqual(self.b, b3)

    def test_get_by_index(self):
        self.assertEqual(self.b[0], 1)

    def test_get_by_index_fail(self):
        with self.assertRaises(IndexError) as cm:
            self.b[5]
        self.assertEqual(cm.exception.args[0], "index out of bound")

    def test_set_by_index(self):
        self.b[0] = 1

    def test_set_by_index_fail(self):
        with self.assertRaises(IndexError) as cm:
            self.b[5] = 0
        self.assertEqual(cm.exception.args[0], "index out of bound")

    def test_delete(self):
        l = Blob([1, 2, 3])
        del l[0]
        self.assertEqual(l, Blob(bytes([2, 3])))

    def test_concat(self):
        l1 = Blob(bytes([1]))
        l2 = Blob(bytes([2]))
        self.assertEqual(Blob(bytes([1, 2])), l1 + l2)

    def test_repeat(self):
        l = Blob(bytes([1]))
        self.assertEqual(l * 3, Blob(bytes([1, 1, 1])))

    def test_inplace_concat(self):
        l = Blob(bytes([1]))
        l += Blob(bytes([2, 3]))
        self.assertEqual(l, Blob(bytes([1, 2, 3])))

    def test_inplace_repeat(self):
        l = Blob(bytes([1]))
        l *= 3
        self.assertEqual(l, Blob(bytes([1, 1, 1])))

    def test_blob_hash(self):
        bs = bytes([1, 7, 8, 4, 1])
        b = Blob(bs)
        # Can blob be used as a map key?
        d = {1: b, b: 1}
        d2 = {1: bs, b: 1}

        self.assertEqual(d, d2)

    # TODO: str repr test

class TestHLL(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.hll = HLL(value=bytes([1, 2, 3, 4]))

    def test_equality(self):
        b = bytes([1, 2, 3, 4])
        hll2 = HLL(bytes([1, 2, 3, 4]))

        self.assertEqual(self.hll, b)
        self.assertEqual(self.hll, hll2)

    def test_inequality(self):
        b = bytes([1, 2, 3, 5])
        hll2 = HLL(bytes([1, 2, 3, 5]))

        self.assertNotEqual(self.hll, b)
        self.assertNotEqual(self.hll, hll2)

    def test_set_and_get(self):
        self.hll.value = [5, 6, 7]
        self.assertEqual(self.hll.value, [5, 6, 7])

    # TODO: str repr test
