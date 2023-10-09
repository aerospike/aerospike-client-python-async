from fixtures import TestFixtureInsertRecord


class TestAppend(TestFixtureInsertRecord):
    async def test_append(self):
        retval = await self.client.append(self.key, {"brand": "d"})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand"], "Fordd")

    async def test_append_nonexistent_bin(self):
        await self.client.append(self.key, {"brand1": "d"})
        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand1"], "d")
