from aerospike_async import AsyncClient, Host

hosts = [
    Host("127.0.0.1", 3000)
]

with AsyncClient(hosts) as client:
    try:
        client[0]
    except TypeError:
        pass

    test_ns = client["test"]
    test_ns = client.test
    demo_set = test_ns["demo"]
    demo_set = test_ns.demo

    bins = {
        "a": 4
    }
    demo_set.put("userkey1", bins)
    demo_set.get("userkey1")
