from fixtures import TestFixtureInsertRecord
from aerospike_async import ReadPolicy

class TestExists(TestFixtureInsertRecord):
    async def test_existing_record(self):
        retval = await self.client.exists(key=self.key)
        self.assertEqual(retval, True)

    async def test_nonexistent_record(self):
        retval = await self.client.exists(self.key_invalid_primary_key)
        self.assertEqual(retval, False)

    async def test_exists_with_policy(self):
        rp = ReadPolicy()
        retval = await self.client.exists(self.key, policy=rp)
        self.assertEqual(retval, True)

    async def test_exists_fail(self):
        with self.assertRaises(Exception):
            await self.client.exists(self.key_invalid_namespace)
