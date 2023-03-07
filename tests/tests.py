import aerospike
from aerospike_async import AsyncClient, Key
import asyncio

config = {
    "hosts": [("127.0.0.1", 3000)]
}
client = aerospike.client(config)

key_tuple = ("test", "demo", "1")
client.put(key_tuple, {"bin": "value"})

key = Key("test", "demo", "1")
response = asyncio.run(AsyncClient.get(key))
print(response)

record = client.get(key_tuple)
print(record)

client.remove(key_tuple)
client.close()
