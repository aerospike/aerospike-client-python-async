from fixtures import TestFixtureInsertRecord


class TestDelete(TestFixtureInsertRecord):
    async def test_delete_existing_record(self):
        rec_existed = await self.client.delete(self.key)
        self.assertEqual(rec_existed, True)

    async def test_delete_nonexistent_record(self):
        rec_existed = await self.client.delete(self.key_nonexistent_pk)
        self.assertEqual(rec_existed, False)
