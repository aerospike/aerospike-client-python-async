# Aerospike Async Python Benchmarks

This directory contains performance benchmarking scripts for the `aerospike_async` Python library.

## Available Benchmarks

- **`benchmarks.py`** - Performance benchmarking examples using pyperf

## Running Benchmarks

### Prerequisites
1. Install pyperf (if not already installed):
   ```bash
   pip install pyperf 
   ```

2. Make sure you have an Aerospike server running

3. Set the `AEROSPIKE_HOST` environment variable (if not already set):
   ```bash
   export AEROSPIKE_HOST=localhost:3000
   ```
   
### Running Benchmarks
```bash
# Run performance benchmarks
python benchmarks.py
```

## Benchmark Categories

- **Put Operations** - Performance testing for data insertion
- **Get Operations** - Performance testing for data retrieval
- **Query Operations** - Performance testing for data querying
- **Batch Operations** - Performance testing for batch operations

## Notes

- Benchmarks use pyperf for accurate performance measurement
- All benchmarks include proper setup and teardown
- Results are suitable for performance regression testing
- Benchmarks can be used to compare different configurations or versions