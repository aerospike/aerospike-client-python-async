import unittest

from aerospike_async import new_client, Client


class TestClient(unittest.IsolatedAsyncioTestCase):
    async def test_connect(self):
        c = await new_client("localhost:3000")
        self.assertEqual(type(c), Client)
        # TODO: shouldn't this be awaitable?
        retval = c.close()
        self.assertEqual(retval, None)
