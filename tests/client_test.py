import unittest

from aerospike_async import ClientPolicy, new_client

class TestClient(unittest.IsolatedAsyncioTestCase):
    async def test_connect(self):
        cp = ClientPolicy()
        c = await new_client(cp, "localhost:3000")
        self.assertIsNotNone(c)
        c.close()
