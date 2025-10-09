import os

from aerospike_async import new_client, ClientPolicy


async def test_connect():
    cp = ClientPolicy()
    client = await new_client(cp, os.environ["AEROSPIKE_HOST"])
    assert client is not None


async def test_close():
    cp = ClientPolicy()
    client = await new_client(cp, os.environ["AEROSPIKE_HOST"])
    assert client is not None
    client.close()
