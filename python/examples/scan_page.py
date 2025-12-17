#!/usr/bin/env python3
"""
Example of scanning records in pages using PartitionFilter and maxRecords.

This example demonstrates how to use PartitionFilter as a cursor for pagination.
The same PartitionFilter instance is reused across multiple scans, and the
done() method indicates when all records have been retrieved.
"""

import asyncio
import os
from aerospike_async import (
    new_client, ClientPolicy, ScanPolicy, PartitionFilter,
    WritePolicy, Key
)


async def write_records(client, namespace, set_name, bin_name, count):
    """Write test records to the database."""
    print(f"Writing {count} records to {namespace}.{set_name}...")
    write_policy = WritePolicy()
    
    for i in range(1, count + 1):
        key = Key(namespace, set_name, i)
        await client.put(write_policy, key, {bin_name: i})
    
    print(f"✓ Wrote {count} records\n")


async def scan_page_example():
    """Demonstrate scanning records in pages."""
    
    # Connect to Aerospike
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    print(f"Connecting to Aerospike at: {host}")
    client_policy = ClientPolicy()
    client_policy.use_services_alternate = True
    
    namespace = "test"
    set_name = "page"
    bin_name = "bin"
    total_records = 190
    
    try:
        # Create client
        client = await new_client(client_policy, host)
        print("Connected to Aerospike")
        print("=" * 50)
        
        # Write test records
        await write_records(client, namespace, set_name, bin_name, total_records)
        
        # Configure scan policy with maxRecords for pagination
        scan_policy = ScanPolicy()
        scan_policy.max_records = 100  # Limit records per page
        
        # Create partition filter - this will act as our cursor
        partition_filter = PartitionFilter.all()
        
        # Scan in pages
        print("Scanning records in pages...")
        print("=" * 50)
        
        page_num = 0
        total_scanned = 0
        
        # Scan up to 3 pages, or until done
        while page_num < 3 and not partition_filter.done():
            page_num += 1
            record_count = 0
            
            print(f"\n📄 Scan page {page_num}")
            print(f"   PartitionFilter.done() = {partition_filter.done()}")
            
            # Perform scan with the partition filter
            recordset = await client.scan(
                policy=scan_policy,
                partition_filter=partition_filter,
                namespace=namespace,
                set_name=set_name,
                bins=None
            )
            
            # Consume all records from this page
            async for record in recordset:
                record_count += 1
                total_scanned += 1
                
                # Optionally print first few records
                if record_count <= 3:
                    print(f"   Record {record_count}: key={record.key.value}, bin={record.bins.get(bin_name)}")
            
            print(f"   ✓ Records returned in this page: {record_count}")
            print(f"   ✓ Total records scanned so far: {total_scanned}")
            print(f"   ✓ PartitionFilter.done() after scan: {partition_filter.done()}")
        
        print("\n" + "=" * 50)
        print(f"📊 Summary:")
        print(f"   Total pages scanned: {page_num}")
        print(f"   Total records scanned: {total_scanned}")
        print(f"   All records retrieved: {partition_filter.done()}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if 'client' in locals() and client is not None:
            await client.close()
            print("\n✓ Client closed")


if __name__ == "__main__":
    print("📄 Scan Page Example")
    print("=" * 50)
    print("This example demonstrates pagination using PartitionFilter")
    print("and maxRecords in ScanPolicy.\n")
    
    # Run the example
    asyncio.run(scan_page_example())

