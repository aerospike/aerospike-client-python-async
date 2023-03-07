import asyncio
import unittest
import docker
import time

import aerospike
from aerospike_async import AsyncClient, Key

class TestAsyncClient(unittest.TestCase):
    def setUp(self):
        self.docker_client = docker.from_env()
        self.server_container = self.docker_client.containers.run(
            image="aerospike/aerospike-server",
            ports={
                3000: 3000,
                3001: 3001,
                3002: 3002
            },
            detach=True
        )

        print("Waiting for server to start...")
        time.sleep(3)
        print("Running tests...")

        config = {
            "hosts": [("127.0.0.1", 3000)]
        }
        self.client = aerospike.client(config)
        self.key_tuple = ("test", "demo", "1")
        self.client.put(self.key_tuple, {"bin": "value"})

    def tearDown(self):
        self.client.remove(self.key_tuple)
        self.client.close()

        self.server_container.stop()
        self.server_container.remove()

        self.docker_client.close()

    def test_get_str(self):
        key = Key("test", "demo", "1")
        response = asyncio.run(AsyncClient.get(key))
        _, _, bins = self.client.get(self.key_tuple)
        self.assertEqual(response, bins)

if __name__ == '__main__':
    unittest.main()
