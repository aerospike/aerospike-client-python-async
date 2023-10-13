import unittest

from aerospike_async import Recordset, ScanPolicy
from fixtures import TestFixtureInsertRecord


class TestScan(TestFixtureInsertRecord):
    async def test_basic_usage(self):
        records = await self.client.scan(namespace="test", set_name="test")
        self.assertEqual(type(records), Recordset)

        # RecordSet tests are in the query basic usage test
        for record in records:
            print(record)

    async def test_with_bins(self):
        records = await self.client.scan("test", "test", bins=[])
        self.assertEqual(type(records), Recordset)
        # TODO:

    async def test_with_policy(self):
        sp = ScanPolicy()
        records = await self.client.scan("test", "test", policy=sp)
        self.assertEqual(type(records), Recordset)

    async def test_fail(self):
        with self.assertRaises(Exception):
            await self.client.scan("test1", "test")
