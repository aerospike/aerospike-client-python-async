from fixtures import TestFixtureInsertRecord


class TestTouch(TestFixtureInsertRecord):
    async def test_existing_record(self):
        retval = await self.client.touch(self.key)
        self.assertEqual(retval, None)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.generation, 2)

    async def test_nonexistent_record(self):
        with self.assertRaises(Exception):
            await self.client.touch(self.key_nonexistent_pk)
