#!/usr/bin/env python
# Copyright 2023-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.


import asyncio
import os
import pyperf

from aerospike_async import new_client, ClientPolicy, Key, ReadPolicy, WritePolicy

client = None
key = Key("test", "test", 0)
rp = ReadPolicy()
wp = WritePolicy()

async def setup():
    global client, key
    cp = ClientPolicy()
    client = await new_client(cp, os.environ.get("AEROSPIKE_HOST", "localhost:3101"))
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
runner.bench_async_func('get', client.get, rp, key)
runner.bench_async_func('touch', client.touch, wp, key)
runner.bench_async_func('append', client.append, wp, key, {"brand": "+"})
runner.bench_async_func('prepend', client.prepend, wp, key, {"brand": "-"})
runner.bench_async_func('exists', client.exists, rp, key)
