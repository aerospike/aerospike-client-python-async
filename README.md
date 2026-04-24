# Aerospike Async Python Client

## Installation

There are two ways to install the client:

1. **Install a pre-built wheel** — no Rust toolchain required.
2. **Build from source** — requires Rust and Cargo.

---

### Option 1: Install a Pre-built Wheel

Pre-built wheels for Linux (x86_64, aarch64), macOS (x86_64, arm64), and Windows (x86_64)
are available on the [GitHub Releases page](https://github.com/aerospike/aerospike-client-python-async/releases).

```bash
pip install aerospike_async-0.3.0a2-cp313-cp313-macosx_11_0_arm64.whl  # example
```

---

### Option 2: Build from Source

#### Prerequisites

This project uses [PyO3](https://pyo3.rs/) to build a Rust extension for Python.
You will need the Rust compiler (`rustc`) and package manager (`cargo`).

If Rust is not already installed:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

Verify the installation:

```bash
rustc --version
cargo --version
```

#### Install Python Dependencies

pyenv is recommended, but any virtual environment will work.

```commandline
pip install -r requirements.txt
```

#### Build & Test (all-in-one)

Builds the Rust code, generates Python stubs, and runs the full test suite:

```commandline
make dev-test
```

#### Build Commands

Build the Rust code into a development wheel and install it into the local virtual environment:

```commandline
make dev
```

Regenerate Python stubs (only needed after modifying Rust code):

```commandline
make stubs
```

---

## Environment Setup

Edit `aerospike.env` to match your Aerospike database node configuration:

```commandline
export AEROSPIKE_HOST=localhost:3100
```

For local-only overrides (e.g. TLS certificate paths), create an `aerospike.env.local` file
in the repo root. It is gitignored and automatically sourced by `aerospike.env`.

## Running Tests

Run all tests (unit + integration):

```commandline
make test
```

Run unit tests only (no server required):

```commandline
make test-unit
```

Run integration tests only (requires a running Aerospike server):

```commandline
make test-int
```

### macOS File Descriptor Limit

On macOS, you may encounter `ConnectionError: Failed to connect to host(s)` errors when
running the full test suite. The default file descriptor limit (256) can be exceeded by the
async client's concurrent connections.

```bash
ulimit -n 4096   # increase for the current shell session
make test
```

To make this permanent, add `ulimit -n 4096` to your shell profile (`~/.zshrc` or `~/.bash_profile`).

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

## Multi-Record Transactions (MRT)

Multi-record transactions require a strong-consistency namespace on the server
(Aerospike 8.0+). Group operations into a single atomic transaction by
attaching a `Txn` to each policy, then `commit` or `abort`:

```python
from aerospike_async import Txn, CommitStatus
from aerospike_async.exceptions import CommitFailedError

txn = Txn()

write = WritePolicy()
write.set_txn(txn)
read = ReadPolicy()
read.set_txn(txn)

try:
    await client.put(write, key_a, {"balance": 100})
    await client.put(write, key_b, {"balance": 200})
    status = await client.commit(txn)
    assert status == CommitStatus.OK_VERIFIED
except CommitFailedError:
    await client.abort(txn)
```

MRT-specific failure result codes are exposed on `ResultCode`:
`MRT_BLOCKED`, `MRT_VERSION_MISMATCH`, `MRT_EXPIRED`, `MRT_TOO_MANY_WRITES`,
`MRT_COMMITTED`, `MRT_ABORTED`, `MRT_ALREADY_LOCKED`, `MRT_MONITOR_EXISTS`.

## Strong Consistency Read Modes

Every read-capable policy exposes `read_mode_ap` and `read_mode_sc` for tuning
consistency on AP and SC namespaces respectively:

```python
from aerospike_async import ReadModeAP, ReadModeSC

policy = ReadPolicy()
policy.set_read_mode_ap(ReadModeAP.One)             # AP namespace
policy.set_read_mode_sc(ReadModeSC.Linearize)       # SC namespace
```

## Wire-Protocol Compression

Every policy exposes a `use_compression` flag (off by default) to enable
compression of request/response payloads on the wire:

```python
policy = WritePolicy()
policy.set_use_compression(True)
```

### Known TODOs:
*  Pipeline benchmarks: track performance between runs.
*  Object serialization:
   - Test __getstate__ and __setstate__ and make sure they work. Otherwise implement them.
*  Cross-Python Client compatibility testing - esp data types
   - Write from legacy, read from new
   - Write from new, read from legacy
*  Track known missing "Rust core" items:
   - Metrics
   - Dynamic Config

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

