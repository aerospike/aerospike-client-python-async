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

    async def test_get_seeds(self):
        c = await new_client("localhost:3000")
        seeds = c.seeds()
        self.assertEqual(type(seeds), str)
        self.assertEqual(seeds, "localhost:3000")

# TODO: need tests for client.close()
# TODO: need tests for client config
class TestClientConfig(unittest.IsolatedAsyncioTestCase):
    async def test_default_config(self):
        cp = ClientPolicy()
        # These properties should exist by default in the client policy Python class
        # And reading client policy attributes should read from the wrapped Rust client's client policy
        # TODO: Rust client doesn't specify default values for client policy
        self.assertEqual(cp.user, None)
        self.assertEqual(cp.password, None)
        self.assertEqual(cp.timeout, 30000)
        self.assertEqual(cp.idle_timeout, 5000)
        self.assertEqual(cp.max_conns_per_node, 256)
        self.assertEqual(cp.conn_pools_per_node, 1)

    async def test_set_and_get_config(self):
        # Writing client policy attributes should write to the wrapped Rust client's client policy
        cp = ClientPolicy()
        cp.user = "user"
        cp.password = "pass"
        cp.timeout = 3000
        cp.idle_timeout = 1000
        cp.max_conns_per_node = 1
        cp.conn_pools_per_node = 1

        self.assertEqual(cp.user, "user")
        self.assertEqual(cp.password, "pass")
        self.assertEqual(cp.timeout, 3000)
        self.assertEqual(cp.idle_timeout, 1000)
        self.assertEqual(cp.max_conns_per_node, 1)
        self.assertEqual(cp.conn_pools_per_node, 1)
