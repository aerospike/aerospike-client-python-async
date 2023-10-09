from fixtures import TestFixtureInsertRecord


class TestExists(TestFixtureInsertRecord):
    async def test_existing_record(self):
        retval = await self.client.exists(self.key)
        self.assertEqual(retval, True)

    async def test_nonexistent_record(self):
        retval = await self.client.exists(self.key_nonexistent_pk)
        self.assertEqual(retval, False)
