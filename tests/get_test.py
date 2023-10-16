from fixtures import TestFixtureInsertRecord
from aerospike_async import *

class TestGet(TestFixtureInsertRecord):
    client: Client
    key: Key

    async def test_all_bins(self):
        rec = await self.client.get(key=self.key)

        # Check all Record attributes
        self.assertEqual(type(rec), Record)
        # Key is not returned in Record struct when reading a record
        # https://docs.rs/aerospike/latest/aerospike/struct.Record.html#structfield.key
        self.assertEqual(rec.key, None)
        self.assertEqual(
            rec.bins,
            self.original_bin_val
        )
        self.assertEqual(rec.generation, 1)
        self.assertEqual(type(rec.ttl), int)

    async def test_some_bins(self):
        rec = await self.client.get(self.key, bins=["brand", "year"])
        self.assertEqual(type(rec), Record)
        self.assertEqual(rec.bins, {"brand": "Ford", "year": 1964})

    async def test_no_bins(self):
        rec = await self.client.get(self.key, [])
        self.assertEqual(type(rec), Record)
        self.assertEqual(rec.bins, {})

    async def test_with_policy(self):
        rp = ReadPolicy()
        rec = await self.client.get(self.key, policy=rp)
        self.assertEqual(type(rec), Record)

    # Negative tests

    async def test_get_nonexistent_namespace(self):
        # A record does not exist with this namespace
        key = Key("test1", "test", 1)
        with self.assertRaises(ServerError):
            await self.client.get(key)

    async def test_get_nonexistent_record(self):
        # A record does not exist with this key
        key = Key("test", "test", 0)
        with self.assertRaises(ServerError):
            await self.client.get(key)
