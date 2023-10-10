import unittest

from aerospike_async import Statement
from aerospike_async import FilterExpression as fe


class TestStatement(unittest.TestCase):
    def test_new(self):
        stmt = Statement(namespace="test", set_name="test")
        # Test defaults
        self.assertEqual(stmt.filters, None)

    def test_set_filters(self):
        # TODO: check that filter expression copy works properly
        stmt = Statement("test", "test")
        expr = fe.eq(fe.string_bin("brand"), fe.string_val("Ford"))
        stmt.filters = expr.clone()
        self.assertEqual(stmt.filters, expr)
