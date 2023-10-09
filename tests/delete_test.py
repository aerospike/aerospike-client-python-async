from fixtures import TestFixtureInsertRecord
from aerospike_async import WritePolicy

class TestDelete(TestFixtureInsertRecord):
    async def test_delete_existing_record(self):
        rec_existed = await self.client.delete(self.key)
        self.assertEqual(rec_existed, True)

    async def test_delete_nonexistent_record(self):
        rec_existed = await self.client.delete(self.key_invalid_primary_key)
        self.assertEqual(rec_existed, False)

    async def test_delete_with_policy(self):
        wp = WritePolicy()
        rec_existed = await self.client.delete(self.key, policy=wp)
        self.assertEqual(rec_existed, True)

    async def test_delete_with_nonexistent_namespace(self):
        with self.assertRaises(Exception):
            await self.client.delete(self.key_invalid_namespace)
