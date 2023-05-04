import asyncio
from functools import partial

from aerospike_async import AsyncClient, Host, ListAppend, UDFCall, Operation, UserKey, Bins

async def main():
    hosts = [
        Host("127.0.0.1", 3000)
    ]
    async with AsyncClient(hosts) as client:
        # Can fetch namespaces from client directly
        # This is a lazy operation. Nothing is sent to the server yet
        test_ns = client["test"]
        test_ns = client.test

        # Can fetch sets from namespaces
        # Also a lazy operation
        demo_set = test_ns["demo"]
        demo_set = test_ns.demo

        # Single key operations

        # Can store and fetch records from set
        # Bins are still dictionaries, but type aliases make functions easy to read
        bins: Bins = {
            "a": 4
        }
        await demo_set.put_record("userkey1", bins)
        record = await demo_set.get_record("userkey1")
        print(record.bins["bin_name"]["map_key"])

        ops: list[Operation] = [
            ListAppend("bin_list", 4)
        ]
        await demo_set.operate_on_record("userkey1", ops)

        # Multi-key API calls

        # Store and fetch multiple records with the same bins
        user_keys: list[UserKey] = [
            "key1",
            "key2",
            "key3"
        ]
        bin_names = [
            "bin1",
            "bin2"
        ]
        await demo_set.put_records(
            user_keys,
            bins
        )
        batch_op_results = await demo_set.get_records(
            user_keys,
            bin_names
        )
        # We will get all the records at once
        for op_result in batch_op_results:
            if op_result.exception != None:
                # Batch operation can fail
                print(op_result.exception)
            else:
                # Otherwise a record should be returned for each operation
                print(op_result.record.bins)

        # Queries

        records = await demo_set.find_records("bin1", bin_value_equals = "test")
        records = await test_ns.find_records("bin1", bin_value_min = 2, bin_value_max = 5)
        # This will block on I/O while reading each record from a stream
        # Different from batch operations like above
        async for record in records:
            print(record.bins)

        # Batch operations
        # You can mix different batch operations in one call, with different bins for each operation

        batch_ops = [
            partial(demo_set.get_record, user_key="key1"),
            partial(demo_set.put_record, user_key="key2", bins=bins)
        ]
        op_results = await test_ns.batch_perform_on_records(
            batch_ops
        )
        for op_result in op_results:
            if op_result.exception != None:
                print(op_result.exception)
            else:
                print(op_result.record.bins)

        # UDF functions

        # You need to download a record UDF locally before calling it
        await client.download_udf("documentapi.lua")

        documentapi_udf_call=UDFCall(
            module_name="documentapi.lua",
            function_name="get",
            arguments = ["binname", "$.mapitem"]
        )
        results = await test_ns.apply_udf_to_record("key1", documentapi_udf_call)
        print(results)

        # You can apply a record udf to every matching record in a query
        await demo_set.find_records_and_apply_record_udf(documentapi_udf_call, bin_name="a", bin_value_equals="asdf")
        await demo_set.find_records_and_apply_record_udf(documentapi_udf_call, bin_name="b", bin_value_min=2, bin_value_max=4)

        # Find records and aggregate into a single value
        stream_udf_func=UDFCall(
            module_name="aggregate.lua",
            function_name="results",
        )
        value = await demo_set.find_and_aggregate_records(stream_udf_func)
        print(value)

asyncio.run(main())
