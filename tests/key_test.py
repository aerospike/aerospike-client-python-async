import asyncio
import unittest

from aerospike_async import *


class TestKey(unittest.IsolatedAsyncioTestCase):
    def test_key_eq(self):
        key1 = Key("test", "test", 1)
        key2 = Key("test", "test", 1)
        key3 = Key("test", "test", 2)
        key4 = Key("test1", "test", 1)
        key5 = Key("test", "test1", 1)

        # self.assertEqual(key1, key2)
        self.assertNotEqual(key1, key3)
        # self.assertNotEqual(key1, key3)
        # self.assertNotEqual(key1, key4)
        # self.assertNotEqual(key1, key5)
