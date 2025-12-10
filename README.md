# Aerospike Async Python Client

## Setup Instructions

### Customize your aerospike environment file (aerospike.env)
Edit the ```aerospike.env``` file to match your Aerospike database node configuration:
```commandline
export AEROSPIKE_HOST=localhost:3100
```
<br>

### Setup local virtual environment (pyenv is recommended, but any can work)

#### Install python packages:
```commandline
pip install -r requirements.txt
```
<br>

### All-in-one command:
1. Build Rust code into a wheel that is added as a Python module into local virtual environment
2. Generate Python stubs from Rust code
3. Run tests with Pytest
```commandline
make dev-test
```
<br>

### Individual commands:
#### Build Rust code into a wheel that is added as a Python module into local virtual environment :
```commandline
make dev
```

#### Run tests:
```commandline
make test
```
<br>

### macOS File Descriptor Limit

On macOS, you may encounter `ConnectionError: Failed to connect to host(s)` errors when running the full test suite. This occurs because:

- **Default macOS limit**: The default file descriptor limit on macOS is 256 (`ulimit -n` shows the current limit)
- **Why it's not enough**: The async client creates multiple connections, event loops, and file handles during testing. Running the full test suite can easily exceed 256 open file descriptors, especially with async operations that maintain multiple concurrent connections.

**Solution**: Increase the file descriptor limit before running tests:

```bash
# Check current limit
ulimit -n

# Increase limit (recommended: 4096)
ulimit -n 4096

# Verify the new limit
ulimit -n

# Now run tests
make test
```

**Note**: This change is temporary for the current shell session. To make it permanent, add `ulimit -n 4096` to your shell profile (e.g., `~/.zshrc` or `~/.bash_profile`).

<br>

### Optional - only needed if updating the Rust code
### Build the Python stubs for the Rust code:
```commandline
make stubs
```
<br>

### Known TODOs:
*  Pipeline benchmarks: track performance between runs.
*  Decide about introducing the Bin class, or keep using Dicts (for Khosrow and Ronen when the latter is back from vacation)?
*  Next APIs:
   - Security/Admin operations — exposed but tests skipped; may need updates to match current Rust core API
   - Batch operations
   - UDF operations
   - TLS configuration
   - Transactions
*  Object serialization:
    - Test __getstate__ and __setstate__ and make sure they work. Otherwise implement them.
*  Cross-Python Client compatibility testing - esp data types
    - Write from legacy, read from new
    - Write from new, read from legacy
*  Track known remaining "core" (Rust Client) items:
    - AP/SC
    - MRT
    - smaller items