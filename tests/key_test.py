import unittest

from aerospike_async import *


class TestKey(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.key = Key("test", "test", 1)
        self.same_key = Key("test", "test", 1)
        self.different_key = Key("test", "test", 2)

    # Keys are equal based on their computed digests

    def test_key_eq(self):
        self.assertEqual(self.key, self.same_key)

    def test_key_not_eq(self):
        self.assertNotEqual(self.key, self.different_key)

    def test_get_fields(self):
        self.assertEqual(self.key.namespace, "test")
        self.assertEqual(self.key.set_name, "test")
        self.assertEqual(self.key.value, 1)
        # Just test that we can access the digest from the wrapped Rust client
        self.assertEqual(type(self.key.digest), str)
