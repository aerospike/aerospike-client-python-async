#!/usr/bin/env python3
"""
Example of terminating and resuming a scan using PartitionFilter.

This example demonstrates how PartitionFilter acts as a cursor that can be
used to resume a scan after termination. The same PartitionFilter instance
is reused to continue scanning from where it left off.
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


async def scan_resume_example():
    """Demonstrate terminating and resuming a scan."""
    
    # Connect to Aerospike
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    print(f"Connecting to Aerospike at: {host}")
    client_policy = ClientPolicy()
    client_policy.use_services_alternate = True
    
    namespace = "test"
    set_name = "resume"
    bin_name = "bin"
    total_records = 200
    max_records_before_terminate = 50
    
    try:
        # Create client
        client = await new_client(client_policy, host)
        print("Connected to Aerospike")
        print("=" * 50)
        
        # Write test records
        await write_records(client, namespace, set_name, bin_name, total_records)
        
        # Configure scan policy
        # Note: concurrent_nodes=False ensures serialized node scans
        # (not directly exposed in Python async client, but scan is naturally serialized)
        scan_policy = ScanPolicy()
        
        # Create partition filter - this will act as our cursor
        partition_filter = PartitionFilter.all()
        
        # First scan: terminate after max_records_before_terminate records
        print("🔍 Start scan (will terminate after 50 records)")
        print("=" * 50)
        
        record_count = 0
        recordset = await client.scan(
            policy=scan_policy,
            partition_filter=partition_filter,
            namespace=namespace,
            set_name=set_name,
            bins=None
        )
        
        # Consume records until we hit our limit
        try:
            async for record in recordset:
                record_count += 1
                
                # Print first few records
                if record_count <= 3:
                    print(f"   Record {record_count}: key={record.key.value}, bin={record.bins.get(bin_name)}")
                
                # Simulate termination after max_records_before_terminate records
                if record_count >= max_records_before_terminate:
                    print(f"\n   ⚠️  Terminating scan after {record_count} records")
                    # Close the recordset to terminate the scan
                    recordset.close()
                    break
        except Exception as e:
            # Handle any errors during iteration
            print(f"   Scan terminated: {e}")
        
        print(f"✓ Records returned before termination: {record_count}")
        print(f"✓ PartitionFilter state preserved for resume")
        
        # At this point, the PartitionFilter could be serialized and saved
        # for later resumption. For this example, we'll resume immediately.
        
        print("\n" + "=" * 50)
        print("🔄 Resume scan (using same PartitionFilter)")
        print("=" * 50)
        
        # Reset counter for resume
        record_count = 0
        
        # Resume scan using the same partition filter
        # The filter maintains its state and will continue from where it left off
        recordset = await client.scan(
            policy=scan_policy,
            partition_filter=partition_filter,
            namespace=namespace,
            set_name=set_name,
            bins=None
        )
        
        # Consume remaining records
        async for record in recordset:
            record_count += 1
            
            # Print first few records from resume
            if record_count <= 3:
                print(f"   Record {record_count}: key={record.key.value}, bin={record.bins.get(bin_name)}")
        
        print(f"✓ Records returned after resume: {record_count}")
        
        print("\n" + "=" * 50)
        print(f"📊 Summary:")
        print(f"   Records before termination: {max_records_before_terminate}")
        print(f"   Records after resume: {record_count}")
        print(f"   Total records processed: {max_records_before_terminate + record_count}")
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
    print("🔄 Scan Resume Example")
    print("=" * 50)
    print("This example demonstrates terminating and resuming a scan")
    print("using the same PartitionFilter instance as a cursor.\n")
    
    # Run the example
    asyncio.run(scan_resume_example())

