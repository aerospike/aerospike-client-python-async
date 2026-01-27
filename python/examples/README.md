# Aerospike Async Python Examples

This directory contains example scripts demonstrating how to use the `aerospike_async` Python library.

## Available Examples

### Core Examples
- **[`basic_examples.py`](basic_examples.py)** - Basic examples showing fundamental operations like connecting, putting, getting, and querying data

### Create Index Examples
- **[`create_index.py`](create_index.py)** - Comprehensive examples of creating different types of indexes
- **[`create_index_simple.py`](create_index_simple.py)** - Simple, focused examples of index creation with quick reference

### Query Examples
- **[`statement_simple.py`](statement_simple.py)** - Basic query operations with Statement

### Statement Examples
- **[`statement_simple.py`](statement_simple.py)** - Basic query statements with filters

### User Management
- **[`user_management.py`](user_management.py)** - User creation, role assignment, and user administration
- **[`role_management.py`](role_management.py)** - Role creation, privilege assignment, and role configuration

### Privilege Examples
- **[`privilege.py`](privilege.py)** - Comprehensive privilege and user management examples
- **[`privilege_simple.py`](privilege_simple.py)** - Simple privilege object examples (no server required)

### TLS Examples
- **[`tls_example.py`](tls_example.py)** - TLS configuration, PKI authentication, and secure connection examples

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

# Run query examples
python statement_simple.py

# Run statement examples
python statement_simple.py

# Run user management examples
python user_management.py
python role_management.py

# Run privilege examples
python privilege.py
python privilege_simple.py
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

### Query Operations ([`statement_simple.py`](statement_simple.py))
- Basic record querying across namespaces/sets
- Query with filters (range, contains, geo)
- Partition-specific querying
- Bin-specific querying for efficiency

### Statement Queries ([`statement_simple.py`](statement_simple.py))
- Basic query statements
- Range filters for numeric data
- Contains filters for list/map data
- Geo filters for location data
- Filter combinations and advanced patterns

### User Management ([`user_management.py`](user_management.py))
- User creation with role assignment
- User query and administration
- Role management for users
- Password management
- User cleanup and deletion

### Role Management ([`role_management.py`](role_management.py))
- Custom role creation with privileges
- Privilege assignment and management
- IP allowlist configuration
- Quota management (read/write limits)
- Role query and administration

### Privilege Management ([`privilege.py`](privilege.py), [`privilege_simple.py`](privilege_simple.py))
- User privilege definitions and management
- Role-based access control patterns
- Privilege validation and testing
- Authentication and authorization examples

### TLS and Security ([`tls_example.py`](tls_example.py))
- Basic TLS connection configuration
- Client certificate authentication
- PKI (certificate-based) authentication
- TLS name override in host strings
- Authentication mode configuration

## Quick Start

For a quick introduction, start with:
1. **[`create_index_simple.py`](create_index_simple.py)** - Shows basic index creation
2. **[`statement_simple.py`](statement_simple.py)** - Shows basic querying
3. **[`statement_simple.py`](statement_simple.py)** - Shows basic querying
4. **[`privilege_simple.py`](privilege_simple.py)** - Shows privilege objects (no server needed)
5. **[`user_management.py`](user_management.py)** - Shows user management operations

For comprehensive usage, see:
1. **[`create_index.py`](create_index.py)** - Complete index creation guide
2. **[`geospatial.py`](geospatial.py)** - Advanced query patterns with geo filters

## Notes

- All examples include proper error handling
- Examples demonstrate both sync and async patterns
- Each example is self-contained and can be run independently
- Examples include detailed comments and explanations