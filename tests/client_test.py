import unittest

from aerospike_async import new_client

class TestClient(unittest.IsolatedAsyncioTestCase):
    async def test_connect(self):
        c = await new_client("localhost:3000")
        self.assertIsNotNone(c)
        c.close()
