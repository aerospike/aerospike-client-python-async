# Aerospike Async Python Client — `core-v2-maintenance`

> **You are looking at the `core-v2-maintenance` branch.** This branch tracks the
> released **Aerospike Rust client v2** line (`aerospike-core` `branch = "v2"`)
> and exists to give QA a stable PAC build for testing rust-core v2 in the
> field. Mainline development (`main` / `dev`) targets rust-core v3 and
> includes features (MRTs, path expressions, compression, strong-consistency
> read modes) that are **not** present on this branch.
>
> Forward-port policy: only PAC-internal fixes that are independent of v3
> rust-core APIs are eligible for backport here.
>
> Distributed on PyPI as `aerospike-async-core-v2` (separate from the v3
> `aerospike-async` distribution).

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
   - Metrics
   - Strong Consistency (SC)
   - Transactions
   - Dynamic Config

## Versioning

PAC follows [SemVer](https://semver.org/) for releases. Pre-releases use the
`MAJOR.MINOR.PATCH-{alpha,beta,rc}.N` form (e.g. `0.3.0-alpha.16`). PyPI
normalizes these on upload to the equivalent PEP 440 spelling (`0.3.0a16`).

### Single source of truth

`Cargo.toml` is the only place where the version lives:

```toml
[package]
name = "aerospike_async"
version = "0.3.0-alpha.16"
```

`pyproject.toml` does **not** duplicate the version. maturin reads it from
`Cargo.toml` when it builds the wheel, so the two are guaranteed to match.

### Bumping the version

Bumps are manual and happen in PRs against `dev`. Promotion workflows
(`dev → stage → main`) do not mutate the version.

```bash
# 1. Edit Cargo.toml [package] version field, then refresh Cargo.lock:
#    e.g. 0.3.0-alpha.16  →  0.3.0-alpha.17
cargo check    # or: cargo update -p aerospike_async --precise 0.3.0-alpha.17

# 2. Confirm:
bin/get-version    # prints 0.3.0-alpha.17

# 3. Open a PR against dev with just this change.
```

### Reading the version programmatically

Anywhere a build script, CI step, or release tool needs the version:

```bash
bin/get-version    # → 0.3.0-alpha.16
```

The script parses the first `version` field inside the `[package]` table of
`Cargo.toml`. It has no Python or cargo runtime dependencies — usable from
any shell, container, or CI environment.

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

