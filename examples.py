import asyncio
from aerospike_async import *

async def main():
    cp = ClientPolicy()
    c = await new_client(cp, "localhost:3000")

    key = Key("test", "test", 1)

    wp = WritePolicy()
    existed = await c.delete(wp, key)
    print(existed)

    await c.put(wp, key, {
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964,
        "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
    })

    rp = ReadPolicy()
    rec = await c.get(rp, key)
    print(f".get result: {rec}")

    await c.add(wp, key, {"year": 1})
    rec = await c.get(rp, key, ["year"])
    print(f".add result: {rec}")

    await c.append(wp, key, {"brand": ")"})
    rec = await c.get(rp, key, ["brand"])
    print(f".append result: {rec}")

    await c.prepend(wp, key, {"brand": "("})
    rec = await c.get(rp, key, ["brand"])
    print(f".prepend result: {rec}")

    await c.touch(wp, key)
    rec = await c.get(rp, key)
    print(f".touch result: {rec}")

    exists = await c.exists(wp, key)
    print(f".exists result: {exists}")

    await c.truncate("test", "test")
    exists = await c.exists(wp, key)
    print(f".exists after .truncate result: {exists}")

    for i in range(10):
        key = Key("test", "test", i)
        await c.put(wp, key, {
            "id": i,
            "brand": "Ford",
            "model": "Mustang",
            "year": 1964 + i,
            "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
        })

    await c.create_index("test", "test", "year", "test.test.year", IndexType.Numeric)

    sp = ScanPolicy()
    rcs = await c.scan(sp, "test", "test")
    print(f"Scan results:")
    i = 0
    for rec in rcs:
        i += 1
        print(rec)

    print(f"Scan result count: {i}")

    qp = QueryPolicy()
    stmt = Statement("test", "test")
    stmt.filters = [Filter.range("year", 1964, 1968)]
    rcs = await c.query(qp, stmt)
    print(f"Query results:")
    i = 0
    for rec in rcs:
        i += 1
        print(rec)

    print(f"Query result count: {i}")


# Run the main() coroutine at the top-level instead
asyncio.run(main())
