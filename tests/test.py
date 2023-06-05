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

    hosts = [Host("172.17.0.3", 3000)]
    c = await Cluster.new(hosts)

import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S')
asyncio.run(main())
