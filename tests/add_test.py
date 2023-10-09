from fixtures import TestFixtureInsertRecord
from aerospike_async import WritePolicy

class TestAdd(TestFixtureInsertRecord):
    async def test_add(self):
        retval = await self.client.add(self.key, {"year": 1})
        self.assertEqual(retval, None)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["year"], 1965)

    # TODO: document that you can increment float bins
    async def test_add_float_bin(self):
        retval = await self.client.add(self.key, {"mileage": 100000.0})
        self.assertEqual(retval, None)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["mileage"], 200000.1)

    async def test_add_with_policy(self):
        wp = WritePolicy()
        retval = await self.client.add(self.key, {"year": 1}, policy=wp)
        self.assertEqual(retval, None)

    async def test_add_unsupported_bin_type(self):
        with self.assertRaises(Exception):
            await self.client.add(self.key, {"brand": 1})
