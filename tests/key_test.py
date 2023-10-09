import unittest

from aerospike_async import *


class TestKey(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.key1 = Key("test", "test", 1)
        self.key2 = Key("test", "test", 1)
        self.key3 = Key("test", "test", 2)

    # Keys are equal based on their computed digests

    def test_key_eq(self):
        self.assertEqual(self.key1, self.key2)

    def test_key_not_eq(self):
        self.assertNotEqual(self.key1, self.key3)
