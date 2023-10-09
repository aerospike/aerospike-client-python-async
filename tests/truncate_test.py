from fixtures import TestFixtureInsertRecord
import time


class TestTruncate(TestFixtureInsertRecord):
    async def test_truncate(self):
        retval = await self.client.truncate("test", "test")
        self.assertEqual(retval, None)

    async def test_truncate_before_nanos(self):
        retval = await self.client.truncate("test", "test", 0)
        self.assertEqual(retval, None)

    async def test_truncate_fail(self):
        seconds_in_future = 1000
        future_threshold = time.time_ns() + seconds_in_future * 10**9
        with self.assertRaises(Exception):
            await self.client.truncate("test", "test", future_threshold)
