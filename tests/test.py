import asyncio

# from aerospike_async import Cluster
# from aerospike_async import Host
from aerospike_async import Info
from aerospike_async import Connection

# c = Cluster([Host("127.0.0.1", 3000)])
# c.tend()

async def main():
    conn = await Connection.new("localhost", 3000, 1)
    print("Valid command")
    res = await Info.request(conn, ["statistics"])
    print(res)
    print("Invalid command")
    res = await Info.request(conn, ["statisticss"])
    print(res)

asyncio.run(main())
