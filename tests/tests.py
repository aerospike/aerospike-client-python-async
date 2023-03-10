import asyncio
import unittest
import docker
import time
from parameterized import parameterized

import aerospike
from aerospike_async import AsyncClient, Key

class TestGet(unittest.TestCase):
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
        cls.old_client = aerospike.client(config)

        cls.async_client = AsyncClient()

    @classmethod
    def tearDownClass(cls):
        print("Cleaning up...")
        cls.old_client.close()

        cls.server_container.stop()
        cls.server_container.remove()

        cls.docker_client.close()

    def tearDown(self):
        self.old_client.remove(self.key_tuple)

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
    # User-key types should be encoded properly before calculating the digest
    def test_get_user_key_types(self, _, key: Key):
        self.key_tuple = (
            key.namespace,
            key.set,
            # Old Python client only accepts user-key blobs as bytearrays
            # whereas AsyncClient accepts them as both bytes and bytearrays for ease of use
            bytearray(key.user_key) if type(key.user_key) == bytes else key.user_key
        )
        expected_results = {"bin": "value"}
        self.old_client.put(self.key_tuple, expected_results)

        actual_results = asyncio.run(self.async_client.get(key))
        self.assertEqual(actual_results, expected_results)

    def test_get_multiple_bins(self):
        key = Key("test", "demo", "1")
        self.key_tuple = (
            key.namespace,
            key.set,
            key.user_key
        )
        expected_results = {"bin1": "value1", "bin2": "value2"}
        self.old_client.put(self.key_tuple, expected_results)

        actual_results = asyncio.run(self.async_client.get(key))
        self.assertEqual(actual_results, expected_results)

    @parameterized.expand(
        [
            (
                "string",
                "gluon gun"
            ),
            (
                "integer",
                42
            ),
            (
                "bytes",
                # Arbitrary string of bytes
                bytes([10, 20, 30])
            ),
            (
                "map",
                {3: {2: 1}}
            ),
            # get() should properly decode strings and blobs inside maps
            # since the msgpack library does not decode it for us
            (
                "maps with blobs",
                {
                    bytes([1, 2, 3]): {
                        bytes([4, 5, 6]): bytes([7, 8, 9])
                    }
                }
            ),
            (
                "maps with strings",
                {"a": {"b": "c"}}
            ),
            (
                "list",
                ["a", "b", "c"]
            ),
        ]
    )
    # After fetching bin values from the server,
    # they should be converted to Python native types properly
    def test_get_bin_value_types(self, _, bin_value):
        key = Key("test", "demo", "1")
        self.key_tuple = (
            key.namespace,
            key.set,
            key.user_key
        )
        expected_results = {"bin": bin_value}
        self.old_client.put(self.key_tuple, expected_results)

        actual_results = asyncio.run(self.async_client.get(key))
        self.assertEqual(actual_results, expected_results)


if __name__ == '__main__':
    unittest.main()
