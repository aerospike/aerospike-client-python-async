from fixtures import TestFixtureInsertRecord


class TestTruncate(TestFixtureInsertRecord):
    async def test_truncate(self):
        retval = await self.client.truncate("test", "test")
        self.assertEqual(retval, None)

    async def test_truncate_before_nanos(self):
        retval = await self.client.truncate("test", "test", 0)
        self.assertEqual(retval, None)
