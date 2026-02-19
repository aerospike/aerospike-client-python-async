# Aerospike Async Python Client

## Prerequisites

This project requires the Rust compiler (`rustc`) and package manager (`cargo`) to be installed, as it uses PyO3 to build a Rust extension for Python.

### Install Rust

If Rust is not already installed, install it using rustup:

```bash
# Install rustup (Rust toolchain installer)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# After installation, reload your shell or source the profile
source $HOME/.cargo/env
```

Verify the installation:
```bash
rustc --version
cargo --version
```

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

## Basic Usage

```python
import asyncio
from aerospike_async import new_client, ClientPolicy, WritePolicy, ReadPolicy, Key

async def main():
    policy = ClientPolicy()
    client = await new_client(policy, "localhost:3000")

    key = Key("test", "demo", "user1")

    # Write a record (bins are plain dicts)
    await client.put(WritePolicy(), key, {"name": "Alice", "age": 28})

    # Read it back
    record = await client.get(ReadPolicy(), key)
    print(record.bins)  # {'name': 'Alice', 'age': 28}

    # Read specific bins only
    record = await client.get(ReadPolicy(), key, ["name"])
    print(record.bins)  # {'name': 'Alice'}

    # Delete the record
    await client.delete(WritePolicy(), key)

    await client.close()

asyncio.run(main())
```

## TLS Configuration

The client supports TLS for secure connections and PKI (certificate-based) authentication.

### Basic TLS

```python
from aerospike_async import new_client, ClientPolicy, TlsConfig

policy = ClientPolicy()
policy.tls_config = TlsConfig("path/to/ca-certificate.pem")
client = await new_client(policy, "tls-host:4333")
```

### TLS with Client Authentication

```python
policy = ClientPolicy()
policy.tls_config = TlsConfig.with_client_auth(
    "ca.pem",      # CA certificate
    "client.pem",  # Client certificate
    "client.key"   # Client private key
)
client = await new_client(policy, "tls-host:4333")
```

### PKI Authentication

PKI mode uses client certificates for authentication (no username/password required):

```python
from aerospike_async import AuthMode

policy = ClientPolicy()
policy.tls_config = TlsConfig.with_client_auth("ca.pem", "client.pem", "client.key")
policy.set_pki_auth()  # or: policy.set_auth_mode(AuthMode.PKI)
client = await new_client(policy, "tls-host:4333")
```

### TLS Name in Host Strings

When the server certificate name differs from the connection hostname, specify the TLS name:

```python
# Format: hostname:tls_name:port
# Example: Connect to IP but validate certificate against "server.example.com"
client = await new_client(policy, "192.168.1.100:server.example.com:4333")
```

### Authentication Modes

The client supports multiple authentication modes via `AuthMode`:

- `AuthMode.NONE` - No authentication
- `AuthMode.INTERNAL` - Internal authentication (username/password)
- `AuthMode.EXTERNAL` - External authentication (LDAP, etc.)
- `AuthMode.PKI` - Certificate-based authentication (requires TLS + client cert)

```python
from aerospike_async import AuthMode

policy = ClientPolicy()
policy.set_auth_mode(AuthMode.INTERNAL, user="admin", password="secret")
# or
policy.set_auth_mode(AuthMode.PKI)  # No user/password needed
```

### Known TODOs:
*  Next APIs:
   - Transactions
*  Pipeline benchmarks: track performance between runs.
*  Object serialization:
    - Test __getstate__ and __setstate__ and make sure they work. Otherwise implement them.
*  Cross-Python Client compatibility testing - esp data types
    - Write from legacy, read from new
    - Write from new, read from legacy
*  Track known missing "Rust core" items:
    - AP/SC
    - Transactions
    - Background Tasks

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

