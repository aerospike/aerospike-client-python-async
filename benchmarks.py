#!/usr/bin/env python3
import asyncio
import pyperf
import random

from aerospike_async import *

client = None

async def setup():
    cp = ClientPolicy()
    global client
    client = await new_client(cp, "localhost:3000")

    wp = WritePolicy()
    for i in range(100):
        key = Key("test", "test", i)
        await client.put(wp, key, {
            "brand": "Ford",
            "model": "Mustang",
            "year": 1964,
            "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
        })


async def put(i):
    global client

    wp = WritePolicy()
    key = Key("test", "test", i)
    await client.put(wp, key, {
        "id": i,
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964,
        "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
    })

async def get(i):
    global client

    rp = ReadPolicy()
    key = Key("test", "test", i)
    await client.get(rp, key)

async def touch(i):
    global client

    wp = WritePolicy()
    key = Key("test", "test", i)
    await client.touch(wp, key)

async def append(i):
    global client

    wp = WritePolicy()
    key = Key("test", "test", i)
    await client.append(wp, key, {"brand": "+"})

async def prepend(i):
    global client

    wp = WritePolicy()
    key = Key("test", "test", i)
    await client.prepend(wp, key, {"brand": "-"})

async def exists(i):
    global client

    wp = WritePolicy()
    key = Key("test", "test", i)
    await client.exists(wp, key)

asyncio.run(setup())

runner = pyperf.Runner()
runner.bench_async_func('put', put, random.randint(0, 99))
runner.bench_async_func('get', get, random.randint(0, 99))
runner.bench_async_func('touch', touch, random.randint(0, 99))
runner.bench_async_func('append', append, random.randint(0, 99))
runner.bench_async_func('prepend', prepend, random.randint(0, 99))
runner.bench_async_func('exists', exists, random.randint(0, 99))
