import unittest
from aerospike_async import Client, Key, new_client, Record, ReadPolicy, GeoJSON
from aerospike_async import FilterExpression as fe


class TestFixtureConnection(unittest.IsolatedAsyncioTestCase):
    client: Client

    async def asyncSetUp(self):
        self.client = await new_client("localhost:3000")

    async def asyncTearDown(self):
        self.client.close()


class TestFixtureCleanDB(TestFixtureConnection):
    key: Key
    key_invalid_primary_key = Key("test", "test", 0)
    key_invalid_namespace = Key("test1", "test", 1)

    async def asyncSetUp(self):
        await super().asyncSetUp()

        self.key = Key("test", "test", 1)

        # delete the record first
        await self.client.truncate("test", "test")


class TestFixtureInsertRecord(TestFixtureCleanDB):
    async def asyncSetUp(self):
        await super().asyncSetUp()

        self.original_bin_val = {
                "brand": "Ford",
                "model": "Mustang",
                "year": 1964,
                "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
                "mileage": 100000.1,
                "bytearray": bytearray(b'123'),
                "bytes": b'123',
                "geojson": GeoJSON('{"type":"Point","coordinates":[-80.590003, 28.60009]}')
        }

        # make a record
        await self.client.put(
            self.key,
            self.original_bin_val
        )
