#!/usr/bin/env python3
import asyncio
import pyperf
import random

from aerospike_async import *

client = None
key = Key("test", "test", 0)
wp = WritePolicy()

async def setup():
    global client, key
    cp = ClientPolicy()
    client = await new_client(cp, "localhost:3000")
    await client.put(wp, key, {
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964,
        "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
    })

asyncio.run(setup())

runner = pyperf.Runner()
runner.bench_async_func('put', client.put, wp, key, {
        "id": 0,
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964,
        "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
})
runner.bench_async_func('get', client.get, ReadPolicy(), key)
runner.bench_async_func('touch', client.touch, wp, key)
runner.bench_async_func('append', client.append, wp, key, {"brand": "+"})
runner.bench_async_func('prepend', client.prepend, wp, key, {"brand": "-"})
runner.bench_async_func('exists', client.exists, wp, key)
