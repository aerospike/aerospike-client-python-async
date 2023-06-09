import asyncio

from aerospike_async.cluster import Cluster
from aerospike_async.host import Host
from aerospike_async import AsyncClient

async def main():
    # conn = await Connection.new("localhost", 3000, 1)
    # print("Valid command")
    # res = await Info.request(conn, ["statistics"])
    # print(res)
    # print("Invalid command")
    # res = await Info.request(conn, ["statisticss"])
    # print(res)

    hosts = [
        Host("172.17.0.3", 3000),
        Host("172.17.0.4", 3000)
    ]
    client = await AsyncClient.new(hosts)
    test = client["test"]
    demo = test["demo"]
    await demo.put_record(1, {"a": 1})

import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S')
asyncio.run(main())
