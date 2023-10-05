import asyncio
import unittest

from aerospike_async import *


class TestValue(unittest.IsolatedAsyncioTestCase):
    def test_geo_json(self):
        geo_str = '{"type":"Point","coordinates":[-80.590003, 28.60009]}'
        geo = GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')
        geo2 = GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')

        self.assertEqual(geo_str, geo)
        self.assertEqual(geo, geo2)

    def test_list(self):
        l = [1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}]
        as_l = List([1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}])
        as_l2 = List([1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}])

        self.assertEqual(as_l, l)
        self.assertEqual(as_l, as_l2)

        # iteration
        as_l = List([1, 2, 3, 4])
        for i, v in enumerate(as_l, start=1):
            self.assertEqual(i, v)

        # assignment
        self.assertEqual(as_l[0], 1)
        as_l[0] = "0"
        self.assertEqual(as_l[0], "0")

    # def test_list_hash(self):
    #     l = [1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}]
    #     as_l = List([1, 2, [1, 2, 3], {1: "str", "str": [1, 2, True]}])
    #     # as_l = List([1, 2])
    #     d = {1: as_l, as_l: 1}
    #     d2 = {1: as_l, as_l: 1}

    #     self.assertEqual(d, d2)

    def test_blob(self):
        b = Blob([1, 7, 8, 4, 1])
        b2 = bytearray([1, 7, 8, 4, 1])
        b3 = bytes([1, 7, 8, 4, 1])

        b4 = Blob(b2)
        b5 = Blob(b3)

        self.assertEqual(b, b2)
        self.assertEqual(b, b3)
        self.assertEqual(b, b4)
        self.assertEqual(b, b5)
        self.assertEqual(b4, b5)

        self.assertEqual(b[0], 1)
        # assignment
        b[0] = 1

    def test_blob_hash(self):
        bs = bytes([1, 7, 8, 4, 1])
        b = Blob(bs)
        d = {1: b, b: 1}
        d2 = {1: bs, b: 1}

        self.assertEqual(d, d2)

    def test_hll(self):
        b = bytes([1, 2, 3, 4])
        hll = HLL(bytes([1, 2, 3, 4]))
        hll2 = HLL(bytes([1, 2, 3, 4]))

        self.assertEqual(hll, b)
        self.assertEqual(hll, hll2)
