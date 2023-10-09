from aerospike_async import IndexType

from fixtures import TestFixtureConnection

class TestFixtureDeleteIndices(TestFixtureConnection):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.client.drop_index("test", "test", "index_name")

    async def asyncTearDown(self):
        self.client.drop_index("test", "test", "index_name")
        await super().asyncTearDown()


class TestIndex(TestFixtureConnection):
    async def test_create_string_index(self):
        retval = await self.client.create_index("test", "test", "brand", "index_name", IndexType.String)
        self.assertEqual(retval, None)

    async def test_create_numeric_index(self):
        retval = await self.client.create_index("test", "test", "year", "index_name", IndexType.Numeric)
        self.assertEqual(retval, None)

    async def test_create_geo2dsphere_index(self):
        retval = await self.client.create_index("test", "test", "geojson", "index_name", IndexType.Geo2DSphere)
        self.assertEqual(retval, None)

    async def test_create_index_fail(self):
        # Creating an index where an existing index has the same name should fail
        await self.client.create_index("test", "test", "brand", "index_name", IndexType.String)

        with self.assertRaises(Exception):
            await self.client.create_index("test", "test", "year", "index_name", IndexType.Numeric)

    async def test_drop_index(self):
        # Setup
        await self.client.create_index("test", "test", "brand", "index_name", IndexType.String)

        retval = self.client.drop_index("test", "test", "index_name")
        self.assertEqual(retval, None)
