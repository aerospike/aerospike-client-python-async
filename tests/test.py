import asyncio

# from aerospike_async import Cluster
# from aerospike_async import Host
from aerospike_async import Info
from aerospike_async.policies.info import InfoPolicy
from aerospike_async import Connection

# c = Cluster([Host("127.0.0.1", 3000)])
# c.tend()

async def main():
    conn = await Connection.new(None, "localhost", 3000)
    policy = InfoPolicy(5)
    res = await Info.request(policy, conn, ["statistics"])
    print(res)

asyncio.run(main())
