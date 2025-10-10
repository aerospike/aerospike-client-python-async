import os
import pytest
import pytest_asyncio

from aerospike_async import ClientPolicy, new_client, ReadPolicy, WritePolicy, Key, FilterExpression as fe


@pytest_asyncio.fixture
async def client_and_key():
    """Setup client and create test record."""
    cp = ClientPolicy()
    client = await new_client(cp, os.environ["AEROSPIKE_HOST"])

    rp = ReadPolicy()

    # make a record
    key = Key("test", "python_test", 1)
    wp = WritePolicy()

    await client.delete(wp, key)

    await client.put(
        wp,
        key,
        {
            "brand": "Ford",
            "model": "Mustang",
            "year": 1964,
            "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
        },
    )
    
    return client, rp, key


async def test_all_bins(client_and_key):
    client, rp, key = client_and_key
    rec = await client.get(rp, key)
    assert rec is not None
    assert rec.generation == 1
    # assert rec.ttl is not None


async def test_some_bins(client_and_key):
    client, rp, key = client_and_key
    rec = await client.get(rp, key, ["brand", "year"])
    assert rec is not None
    assert rec.bins == {"brand": "Ford", "year": 1964}


async def test_matching_filter_exp(client_and_key):
    client, rp, key = client_and_key

    rp = ReadPolicy()
    rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Ford"))
    rec = await client.get(rp, key, ["brand", "year"])
    assert rec is not None
    assert rec.bins == {"brand": "Ford", "year": 1964}


async def test_non_matching_filter_exp(client_and_key):
    client, rp, key = client_and_key

    rp = ReadPolicy()
    rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))
        
    # Debug: Check if filter expression is set
    print(f"\n\nFilter expression set: {rp.filter_expression}")
    print(f"Available methods: {[method for method in dir(rp) if 'filter' in method.lower()]}")
    print(f"Has set_filter_expression: {hasattr(rp, 'set_filter_expression')}")

    with pytest.raises(Exception):
        await client.get(rp, key, ["brand", "year"])
