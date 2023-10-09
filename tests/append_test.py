from fixtures import TestFixtureInsertRecord
from aerospike_async import WritePolicy


class TestAppend(TestFixtureInsertRecord):
    async def test_append(self):
        retval = await self.client.append(self.key, {"brand": "d"})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand"], "Fordd")

    # TODO: the positive tests below are currently not documented behavior

    async def test_append_with_policy(self):
        wp = WritePolicy()
        retval = await self.client.append(self.key, {"brand": "d"}, policy=wp)
        self.assertIsNone(retval)

    async def test_append_nonexistent_key(self):
        retval = await self.client.append(self.key_nonexistent_pk, {"brand": "d"})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key_nonexistent_pk)
        self.assertEqual(rec.bins["brand"], "d")

    async def test_append_bytearray(self):
        retval = await self.client.append(self.key, {"bytearray": bytearray(b'456')})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["bytearray"], bytearray(b'123456'))

    async def test_append_bytes(self):
        retval = await self.client.append(self.key, {"bytes": b'456'})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["bytes"], b'123456')

    async def test_append_nonexistent_bin(self):
        retval = await self.client.append(self.key, {"brand1": "d"})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand1"], "d")

    async def test_append_unsupported_bin_type(self):
        with self.assertRaises(Exception):
            await self.client.append(self.key, {"year": "d"})
