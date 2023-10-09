import asyncio
import unittest

from aerospike_async import *
from aerospike_async import FilterExpression as fe


class TestPut(unittest.IsolatedAsyncioTestCase):
    client: Client
    key: Key

    async def setup(self):
        self.client = await new_client("localhost:3000")

        # make a record
        self.key = Key("test", "test", 1)

        # delete the record first
        await self.client.delete(self.key)

    async def test_put_int(self):
        await self.setup()

        wp = WritePolicy()
        await self.client.put(
            self.key,
            {
                "bin": 1,
            },
        )

        rec = await self.client.get(self.key)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.bins, {"bin": 1})

    async def test_put_float(self):
        await self.setup()

        await self.client.put(
            self.key,
            {
                "bin": 1.76123,
            },
        )

        rec = await self.client.get(self.key)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.bins, {"bin": 1.76123})

    async def test_put_string(self):
        await self.setup()

        wp = WritePolicy()
        await self.client.put(
            self.key,
            {
                "bin": "str1",
            },
        )

        rec = await self.client.get(self.key)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.bins, {"bin": "str1"})

    async def test_put_bool(self):
        await self.setup()

        wp = WritePolicy()
        await self.client.put(
            self.key,
            {
                "bint": True,
                "binf": False,
            },
        )

        rec = await self.client.get(self.key)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.bins, {"bint": True, "binf": False})

    async def test_put_blob(self):
        await self.setup()

        ba = bytearray([1, 2, 3, 4, 5, 6])
        b = bytes([1, 2, 3, 4, 5, 6])

        wp = WritePolicy()
        await self.client.put(
            self.key,
            {
                "bin_b": b,
                "bin_ba": ba,
            },
        )

        rec = await self.client.get(self.key)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.bins, {"bin_b": b, "bin_ba": ba})

    async def test_put_list(self):
        await self.setup()

        l = [1, "str", bytearray([1, 2, 3, 4, 5, 6]), True, False, 1572, 3.1415]

        wp = WritePolicy()
        await self.client.put(
            self.key,
            {
                "bin": l,
            },
        )

        rec = await self.client.get(self.key)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.bins, {"bin": l})

    async def test_put_dict(self):
        await self.setup()

        b = Blob([1, 2, 3, 4, 5, 6])
        l = List([1572, 3.1415])
        d = {
            "str": 1,
            1: "str",
            b: 1,
            2: b,
            True: 1.761,
            9182.58723: False,
            3.141519: [123, 981, 4.12345, [1858673, "str"]],
            False: {"something": [123, 981, 4.12345, [1858673, "str"]]},
            l: b,
        }

        wp = WritePolicy()
        await self.client.put(
            self.key,
            {
                "bin": d,
            },
        )

        rec = await self.client.get(self.key)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.bins, {"bin": d})

    async def test_put_GeoJSON(self):
        await self.setup()

        geo = GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')

        wp = WritePolicy()
        await self.client.put(
            self.key,
            {
                "bin": geo,
            },
        )

        rec = await self.client.get(self.key)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.bins, {"bin": geo})
