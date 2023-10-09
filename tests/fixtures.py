import unittest
from aerospike_async import Client, Key, new_client, Record, ReadPolicy
from aerospike_async import FilterExpression as fe


class TestFixtureConnection(unittest.IsolatedAsyncioTestCase):
    client: Client

    async def asyncSetUp(self):
        self.client = await new_client("localhost:3000")

    async def asyncTearDown(self):
        self.client.close()


class TestFixtureCleanDB(TestFixtureConnection):
    key: Key
    key_nonexistent_pk = Key("test", "test", 0)
    key_nonexistent_namespace = Key("test1", "test", 1)

    async def asyncSetUp(self):
        await super().asyncSetUp()

        self.key = Key("test", "test", 1)

        # delete the record first
        await self.client.delete(self.key)


class TestFixtureInsertRecord(TestFixtureCleanDB):
    async def asyncSetUp(self):
        await super().asyncSetUp()

        # make a record
        await self.client.put(
            self.key,
            {
                "brand": "Ford",
                "model": "Mustang",
                "year": 1964,
                "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
            },
        )
