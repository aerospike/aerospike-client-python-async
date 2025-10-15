# Aerospike Async Python Examples

This directory contains example scripts demonstrating how to use the `aerospike_async` Python library.

## Available Examples

### Core Examples
- **[`basic_examples.py`](basic_examples.py)** - Basic examples showing fundamental operations like connecting, putting, getting, and querying data

### Create Index Examples  
- **[`create_index.py`](create_index.py)** - Comprehensive examples of creating different types of indexes
- **[`create_index_simple.py`](create_index_simple.py)** - Simple, focused examples of index creation with quick reference

### Scan Examples
- **[`scan_simple.py`](scan_simple.py)** - Basic scan operations across records
- **[`scan.py`](scan.py)** - Advanced scan patterns with different partition filters

### Statement Examples
- **[`statement_simple.py`](statement_simple.py)** - Basic query statements with filters

## Running the Examples

### Prerequisites
1. Set the `AEROSPIKE_HOST` environment variable:
   ```bash
   export AEROSPIKE_HOST=localhost:3000
   ```

2. Make sure you have an Aerospike server running

### Running Examples
```bash
# Run basic examples
python basic_examples.py

# Run create index examples
python create_index.py
python create_index_simple.py

# Run scan examples
python scan_simple.py
python scan.py

# Run statement examples
python statement_simple.py
```


## Example Categories

### Basic Operations ([`basic_examples.py`](basic_examples.py))
- Client connection and configuration
- Put operations (single and batch)
- Get operations (single and batch)
- Query operations with filters
- Error handling

### Index Creation ([`create_index.py`](create_index.py), [`create_index_simple.py`](create_index_simple.py))
- Numeric indexes for integer/float data
- String indexes for text data
- Geo2DSphere indexes for location data
- List indexes for array elements
- Map indexes for key-value data
- Parallel index creation
- Error handling and validation

### Scan Operations ([`scan.py`](scan.py), [`scan_simple.py`](scan_simple.py))
- Basic record scanning across namespaces/sets
- Partition-specific scanning
- Range-based partition scanning
- Bin-specific scanning for efficiency

### Statement Queries ([`statement_simple.py`](statement_simple.py))
- Basic query statements
- Range filters for numeric data
- Contains filters for list/map data
- Geo filters for location data
- Filter combinations and advanced patterns

## Quick Start

For a quick introduction, start with:
1. **[`create_index_simple.py`](create_index_simple.py)** - Shows basic index creation
2. **[`scan_simple.py`](scan_simple.py)** - Shows basic scanning
3. **[`statement_simple.py`](statement_simple.py)** - Shows basic querying
4. **[`basic_examples.py`](basic_examples.py)** - Shows basic data operations

For comprehensive usage, see:
1. **[`create_index.py`](create_index.py)** - Complete index creation guide
2. **[`scan.py`](scan.py)** - Advanced scan patterns

## Notes

- All examples include proper error handling
- Examples demonstrate both sync and async patterns
- Each example is self-contained and can be run independently
- Examples include detailed comments and explanations