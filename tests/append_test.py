from fixtures import TestFixtureInsertRecord
from aerospike_async import WritePolicy, ServerError


class TestAppend(TestFixtureInsertRecord):
    async def test_append(self):
        retval = await self.client.append(key=self.key, bins={"brand": "d"})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand"], "Fordd")

    async def test_append_with_policy(self):
        wp = WritePolicy()
        retval = await self.client.append(self.key, {"brand": "d"}, policy=wp)
        self.assertIsNone(retval)

    async def test_append_unsupported_bin_type(self):
        with self.assertRaises(ServerError):
            await self.client.append(self.key, {"year": "d"})
