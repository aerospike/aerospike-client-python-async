import unittest
from aerospike_async import Client, Key, new_client, Record, ReadPolicy
from aerospike_async import FilterExpression as fe


class TestFixtureConnection(unittest.IsolatedAsyncioTestCase):
    client: Client

    async def asyncSetUp(self):
        self.client = await new_client("localhost:3000")


class TestKVS(TestFixtureConnection):
    key: Key
    key_nonexistent_pk = Key("test", "test", 0)
    key_nonexistent_namespace = Key("test1", "test", 1)

    async def asyncSetUp(self):
        await super().asyncSetUp()

        # make a record
        self.key = Key("test", "test", 1)

        await self.client.delete(self.key)

        await self.client.put(self.key, {
            "brand": "Ford",
            "model": "Mustang",
            "year": 1964,
            "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
        })

    async def asyncTearDown(self):
        await self.client.truncate("test", "test")
        self.client.close()


class TestPut(TestKVS):
    async def test_bytes(self):
        self.client.put(self.key, {"blob": bytes(b'123')})

    async def test_double(self):
        self.client.put(self.key, {"double": 1.23})

    async def test_bool(self):
        self.client.put(self.key, {"bool": False})

    async def test_list(self):
        self.client.put(self.key, {"list": [1, 2, 3]})

    async def test_map(self):
        self.client.put(self.key, {"map": {"x": 1, "y": 2, "z": 3}})

class TestGet(TestKVS):
    client: Client
    key: Key

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


class TestAppend(TestKVS):
    client: Client
    key: Key

    async def test_append(self):
        retval = await self.client.append(self.key, {"brand": "d"})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand"], "Fordd")

    async def test_append_nonexistent_bin(self):
        with self.assertRaises(Exception):
            await self.client.append(self.key, {"brand1": "d"})

class TestPrepend(TestKVS):
    async def test_prepend(self):
        retval = await self.client.prepend(self.key, {"brand": "F"})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand"], "FFord")

    async def test_prepend_nonexistent_bin(self):
        with self.assertRaises(Exception):
            await self.client.append(self.key, {"brand1": "F"})
