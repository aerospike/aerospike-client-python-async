import unittest

from aerospike_async import Client, ReadPolicy, Key, new_client, Record
from aerospike_async import FilterExpression as fe

class TestClient(unittest.IsolatedAsyncioTestCase):
    client: Client
    key: Key

    async def asyncSetUp(self):
        self.client = await new_client("localhost:3000")

        # make a record
        self.key = Key("test", "test", 1)

        await self.client.delete(self.key)

        await self.client.put(self.key, {
            "brand": "Ford",
            "model": "Mustang",
            "year": 1964,
            "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
        })

    async def test_all_bins(self):
        rec = await self.client.get(self.key)

        # Check all Record attributes
        self.assertEqual(type(rec), Record)
        # Key is not returned in Record struct when reading a record
        # https://docs.rs/aerospike/latest/aerospike/struct.Record.html#structfield.key
        self.assertEqual(rec.key, None)
        self.assertEqual(rec.bins, {
            "brand": "Ford",
            "model": "Mustang",
            "year": 1964,
            "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
        })
        self.assertEqual(rec.generation, 1)
        self.assertEqual(type(rec.ttl), int)

    # TODO: should selecting some / no bins be separate API calls?

    async def test_some_bins(self):
        rec = await self.client.get(self.key, ["brand", "year"])
        self.assertEqual(type(rec), Record)
        self.assertEqual(rec.bins, {"brand": "Ford", "year": 1964})

    async def test_no_bins(self):
        rec = await self.client.get(self.key, [])
        self.assertEqual(type(rec), Record)
        self.assertEqual(rec.bins, {})

    # Test that policy works

    async def test_matching_filter_exp(self):

        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Ford"))
        rec = await self.client.get(self.key, ["brand", "year"], rp)
        self.assertEqual(type(rec), Record)
        self.assertEqual(rec.bins, {"brand": "Ford", "year": 1964})

    async def test_non_matching_filter_exp(self):

        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))

        with self.assertRaises(Exception):
            await self.client.get(self.key, ["brand", "year"], policy=rp)

    # Negative tests

    async def test_get_nonexistent_namespace(self):
        # A record does not exist with this namespace
        key = Key("test1", "test", 1)
        with self.assertRaises(Exception):
            await self.client.get(key)

    async def test_get_nonexistent_record(self):
        # A record does not exist with this key
        key = Key("test", "test", 0)
        with self.assertRaises(Exception):
            await self.client.get(key)
