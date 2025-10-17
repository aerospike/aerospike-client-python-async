import os

from aerospike_async import new_client, ClientPolicy


async def test_connect():
    """Test basic client connection."""

    cp = ClientPolicy()
    client = await new_client(cp, os.environ["AEROSPIKE_HOST"])
    assert client is not None


async def test_close():
    """Test client connection and proper closing."""

    cp = ClientPolicy()
    client = await new_client(cp, os.environ["AEROSPIKE_HOST"])
    assert client is not None
    client.close()
