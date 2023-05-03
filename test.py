from aerospike_async import AsyncClient, Host, ListAppend, UDFCall

from functools import partial

hosts = [
    Host("127.0.0.1", 3000)
]
with AsyncClient(hosts) as client:
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
    bins = {
        "a": 4
    }
    demo_set.put_record("userkey1", bins)
    demo_set.get_record("userkey1").bins["bin_name"]["map_key"]

    ops = [
        ListAppend("bin_list", 4)
    ]
    demo_set.operate_on_record("userkey1", ops)

    # Multi-key API calls

    # Store and fetch multiple records with the same bins
    user_keys = [
        "key1",
        "key2",
        "key3"
    ]
    bin_names = [
        "bin1",
        "bin2"
    ]
    demo_set.put_records(
        user_keys,
        bins
    )
    records = demo_set.get_records(
        user_keys,
        bin_names
    )
    for record in records:
        print(record.bins)

    # Queries

    demo_set.find_records("bin1", bin_value_equals = "test")
    results = test_ns.find_records("bin1", bin_value_min = 2, bin_value_max = 5)
    for record in results:
        print(record.bins)

    # Batch operations

    batch_ops = [
        partial(demo_set.get_record, user_key="key1"),
        partial(demo_set.put_record, user_key="key2", bins=bins)
    ]
    op_results = test_ns.batch_perform_on_records(
        batch_ops
    )
    for op_result in op_results:
        if op_result.exception != None:
            print(op_result.exception)
        else:
            print(op_result.record.bins)

    # UDF functions

    client.download_udf("documentapi.lua")

    record_udf_func=UDFCall(
        module_name="test.lua",
        function_name="test1",
        arguments = ["$.key1"]
    )
    results = test_ns.apply_udf_to_record("key1", record_udf_func)
    print(results)

    stream_udf_func=UDFCall(
        module_name="aggregate.lua",
        function_name="results",
    )
    demo_set.find_records_and_apply_record_udf(record_udf_func, bin_name="a", bin_value_equals="asdf")
    demo_set.find_and_aggregate_records(stream_udf_func)
