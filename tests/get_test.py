import asyncio
import unittest

from aerospike_async import *
from aerospike_async import FilterExpression as fe

class TestClient(unittest.IsolatedAsyncioTestCase):
    client: Client
    rp: ReadPolicy
    key: Key

    async def setup(self):
        cp = ClientPolicy()
        self.client = await new_client(cp, "localhost:3000")

        self.rp = ReadPolicy()

        # make a record
        self.key = Key("test", "test", 1)
        wp = WritePolicy()

        await self.client.delete(wp, self.key)

        await self.client.put(wp, self.key, {
            "brand": "Ford",
            "model": "Mustang",
            "year": 1964,
            "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
        })

    async def test_all_bins(self):
        await self.setup()
        rec = await self.client.get(self.rp, self.key)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.generation, 1)
        # self.assertIsNotNone(rec.ttl)

    async def test_some_bins(self):
        await self.setup()
        rec = await self.client.get(self.rp, self.key, ["brand", "year"])
        self.assertIsNotNone(rec)
        self.assertEqual(rec.bins, {"brand": "Ford", "year": 1964})

    async def test_matching_filter_exp(self):
        await self.setup()

        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Ford"))
        rec = await self.client.get(rp, self.key, ["brand", "year"])
        self.assertIsNotNone(rec)
        self.assertEqual(rec.bins, {"brand": "Ford", "year": 1964})

    async def test_non_matching_filter_exp(self):
        await self.setup()

        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))

        with self.assertRaises(Exception):
            await self.client.get(rp, self.key, ["brand", "year"])


