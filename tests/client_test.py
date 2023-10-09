import unittest

from aerospike_async import new_client, Client, ClientPolicy, Key


class TestClient(unittest.IsolatedAsyncioTestCase):
    async def test_connect_and_close(self):
        c = await new_client("localhost:3000")
        self.assertEqual(type(c), Client)
        # TODO: shouldn't this be awaitable?
        retval = c.close()
        self.assertEqual(retval, None)

        # If close() actually works, this operation should fail because of no connection
        key = Key("test", "test", 0)
        with self.assertRaises(Exception):
            c.exists(key)

    async def test_connect_with_policy(self):
        cp = ClientPolicy()
        c = await new_client("localhost:3000", policy=cp)
        self.assertEqual(type(c), Client)

    async def test_fail_to_connect(self):
        with self.assertRaises(Exception):
            await new_client("invalidhost")

# TODO: need tests for client.close()
# TODO: need tests for client config
class TestClientConfig(unittest.IsolatedAsyncioTestCase):
    pass
