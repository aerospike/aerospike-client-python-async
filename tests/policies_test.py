import unittest
from aerospike_async import *
from aerospike_async import FilterExpression as fe

class TestReadPolicy(unittest.TestCase):
    def test_get_defaults(self):
        # TODO: defaults are not documented in Rust client docs
        rp = ReadPolicy()
        self.assertEqual(rp.priority, Priority.Default)
        self.assertEqual(rp.consistency_level, ConsistencyLevel.ConsistencyOne)
        self.assertEqual(rp.timeout, 30000)
        self.assertEqual(rp.max_retries, 2)
        self.assertEqual(rp.sleep_between_retries, 500)
        self.assertEqual(rp.filter_expression, None)

    def test_set_and_get_policy(self):
        rp = ReadPolicy()
        rp.priority = Priority.High
        rp.consistency_level = ConsistencyLevel.ConsistencyAll
        rp.timeout = 20000
        rp.max_retries = 4
        rp.sleep_between_retries = 1000
        # TODO: continue from here
        filter_exp = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))
        rp.filter_expression = filter_exp

        # Should read from the wrapped Rust client's read policy
        self.assertEqual(rp.priority, Priority.High)
        self.assertEqual(rp.consistency_level, ConsistencyLevel.ConsistencyAll)
        self.assertEqual(rp.timeout, 20000)
        self.assertEqual(rp.max_retries, 4)
        self.assertEqual(rp.sleep_between_retries, 1000)
        self.assertEqual(rp.filter_expression, filter_exp)
