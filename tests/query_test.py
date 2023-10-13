import unittest

from aerospike_async import Statement, Filter, Recordset, Record, QueryPolicy
from fixtures import TestFixtureInsertRecord

class TestStatement(unittest.TestCase):
    def test_new(self):
        stmt = Statement(namespace="test", set_name="test")
        # Test defaults
        self.assertEqual(stmt.filters, None)

    def test_set_filters(self):
        stmt = Statement("test", "test")
        filter = Filter.range("bin", 1, 3)
        stmt.filters = [
            filter
        ]
        self.assertEqual(type(stmt.filters), list)

        stmt.filters = None
        self.assertEqual(stmt.filters, None)


class TestQuery(TestFixtureInsertRecord):
    stmt = Statement("test", "test")

    # Basic usage test
    async def test_query_and_recordset(self):

        records = await self.client.query(statement=self.stmt)
        self.assertEqual(type(records), Recordset)

        for record in records:
            self.assertEqual(type(record), Record)

        # Query finished
        self.assertEqual(records.active, False)

        # Check that we can call close()
        records.close()


    async def test_with_policy(self):
        qp = QueryPolicy()
        records = await self.client.query(self.stmt, policy=qp)
        self.assertEqual(type(records), Recordset)

    async def test_fail(self):
        stmt_invalid_namespace = Statement("test1", "test")
        with self.assertRaises(Exception):
            await self.client.query(stmt_invalid_namespace)
