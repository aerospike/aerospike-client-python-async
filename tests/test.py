import asyncio

from aerospike_async.cluster import Cluster
from aerospike_async.host import Host
from aerospike_async.info import Info
from aerospike_async.connection import Connection

# c = Cluster([Host("127.0.0.1", 3000)])
# c.tend()

async def main():
    conn = await Connection.new(None, "localhost", 3000)
    conn.set_timeout(1)
    res = await Info.request(conn, ["statistics"])
    print(res)

asyncio.run(main())
