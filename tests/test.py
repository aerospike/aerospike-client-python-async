import asyncio

from aerospike_async.cluster import Cluster
from aerospike_async.host import Host
from aerospike_async import Info
from aerospike_async import Connection

async def main():
    # conn = await Connection.new("localhost", 3000, 1)
    # print("Valid command")
    # res = await Info.request(conn, ["statistics"])
    # print(res)
    # print("Invalid command")
    # res = await Info.request(conn, ["statisticss"])
    # print(res)

    hosts = [Host("172.17.0.2", 3000)]
    c = await Cluster.new(hosts)

import logging
logging.basicConfig(level=logging.DEBUG)
asyncio.run(main())
