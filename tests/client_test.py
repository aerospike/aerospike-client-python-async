import asyncio
import unittest

from aerospike_async import *

class TestClient(unittest.IsolatedAsyncioTestCase):
    async def test_connect(self):
        cp = ClientPolicy()
        c = await new_client(cp, "localhost:3000")
        self.assertIsNotNone(c)

    async def test_close(self):
        cp = ClientPolicy()
        c = await new_client(cp, "localhost:3000")
        self.assertIsNotNone(c)
        c.close()
