#!/usr/bin/env python3
"""
Simple example of using scan functionality from aerospike_async.
"""

import asyncio
import os
from aerospike_async import new_client, ClientPolicy, ScanPolicy, PartitionFilter


async def simple_scan_example():
    """Simple example of scanning records."""
    
    # Connect to Aerospike (set AEROSPIKE_HOST environment variable)
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client_policy = ClientPolicy()
    scan_policy = ScanPolicy()
    
    try:
        # Create client
        client = await new_client(client_policy, host)
        print("Connected to Aerospike")
        
        # Scan all records in a namespace/set
        print("\n--- Scanning all records ---")
        partition_filter = PartitionFilter.all()
        
        # Scan all records in the "test" namespace, "users" set
        recordset = await client.scan(
            policy=scan_policy,
            partition_filter=partition_filter,
            namespace="test",
            set_name="users",
            bins=None  # Return all bins
        )
        
        print("Scanning records...")
        record_count = 0
        
        # Iterate through the recordset (synchronous iteration)
        while True:
            try:
                record = next(recordset)
                record_count += 1
                print(f"Record {record_count}:")
                print(f"  Key: {record.key}")
                print(f"  Bins: {record.bins}")
                print(f"  Generation: {record.generation}")
                print(f"  Expiration: {record.expiration}")
                print()
                
                # Limit output for demo
                if record_count >= 5:
                    print("... (showing first 5 records)")
                    break
            except StopIteration:
                break
        
        print(f"Total records scanned: {record_count}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure AEROSPIKE_HOST is set (e.g., export AEROSPIKE_HOST=localhost:3000)")
    
    finally:
        if 'client' in locals() and client is not None:
            await client.close()


if __name__ == "__main__":
    print("🔍 Simple Scan Example")
    print("="*40)
    
    # Run the example
    asyncio.run(simple_scan_example())
