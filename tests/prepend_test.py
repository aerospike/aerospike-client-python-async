from fixtures import TestFixtureInsertRecord


class TestPrepend(TestFixtureInsertRecord):
    async def test_prepend(self):
        retval = await self.client.prepend(self.key, {"brand": "F"})
        self.assertIsNone(retval)

        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand"], "FFord")

    async def test_prepend_nonexistent_bin(self):
        await self.client.append(self.key, {"brand1": "F"})
        rec = await self.client.get(self.key)
        self.assertEqual(rec.bins["brand1"], "F")
