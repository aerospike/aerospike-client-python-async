from fixtures import TestFixtureInsertRecord
from aerospike_async import WritePolicy, ServerError


class TestPrepend(TestFixtureInsertRecord):
    async def test_prepend(self):
        retval = await self.client.prepend(key=self.key, bins={"brand": "F"})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand"], "FFord")

    async def test_prepend_with_policy(self):
        wp = WritePolicy()
        retval = await self.client.prepend(self.key, {"brand": "F"}, policy=wp)
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand"], "FFord")

    async def test_prepend_nonexistent_bin(self):
        await self.client.append(self.key, {"brand1": "F"})
        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand1"], "F")

    async def test_prepend_unsupported_type(self):
        with self.assertRaises(ServerError):
            await self.client.prepend(self.key, {"year": "d"})
