import asyncio
import unittest
import docker
import time
from parameterized import parameterized

import aerospike
from aerospike_async import AsyncClient, Key

class TestAsyncClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.docker_client = docker.from_env()
        cls.server_container = cls.docker_client.containers.run(
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
        cls.client = aerospike.client(config)

    @classmethod
    def tearDownClass(cls):
        print("Cleaning up...")
        cls.client.close()

        cls.server_container.stop()
        cls.server_container.remove()

        cls.docker_client.close()

    def tearDown(self):
        self.client.remove(self.key_tuple)

    @parameterized.expand(
        [
            (
                "string",
                Key("test", "demo", "1")
            ),
            (
                "integer",
                Key("test", "demo", 1)
            ),
            (
                "bytes",
                # Arbitrary string of bytes
                Key("test", "demo", bytes([12, 30, 20, 19]))
            ),
            (
                "bytearray",
                Key("test", "demo", bytearray([12, 30, 20, 19]))
            )
        ]
    )
    # Ensure user-key types are encoded properly when calculating the digest
    def test_get_uk_types(self, _, key: Key):
        self.key_tuple = (
            key.namespace,
            key.set,
            # Old Python client accepts user-key blobs as bytearrays
            # whereas AsyncClient accepts them as both bytes and bytearrays for ease of use
            key.user_key if type(key.user_key) != bytes else bytearray(key.user_key)
        )
        expected_results = {"bin": "value"}
        self.client.put(self.key_tuple, expected_results)

        actual_results = asyncio.run(AsyncClient.get(key))
        self.assertEqual(actual_results, expected_results)

if __name__ == '__main__':
    unittest.main()
