from fixtures import TestFixtureInsertRecord
from aerospike_async import WritePolicy, ServerError


class TestTouch(TestFixtureInsertRecord):
    async def test_existing_record(self):
        retval = await self.client.touch(key=self.key)
        self.assertEqual(retval, None)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.generation, 2)

    async def test_touch_with_policy(self):
        wp = WritePolicy()
        retval = await self.client.touch(self.key, policy=wp)
        self.assertEqual(retval, None)

    async def test_nonexistent_record(self):
        with self.assertRaises(ServerError):
            await self.client.touch(self.key_invalid_primary_key)
