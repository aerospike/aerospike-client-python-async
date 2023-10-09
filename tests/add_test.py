from fixtures import TestFixtureInsertRecord


class TestAdd(TestFixtureInsertRecord):
    async def test_add(self):
        retval = await self.client.add(self.key, {"year": 1})
        self.assertEqual(retval, None)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["year"], 1965)
