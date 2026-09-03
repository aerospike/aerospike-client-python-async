# Aerospike Python Async Client

Ultra-high performance Python bindings for the Aerospike Rust client core
— async and blocking surfaces in one client, with first-class free-threaded
Python support for parallel-thread throughput well past what GIL-bound
clients can sustain. Built with [PyO3](https://pyo3.rs/); ships pre-built
wheels for Linux (x86_64, aarch64), macOS (x86_64, arm64), and Windows
(x86_64) on Python 3.11–3.14, including the free-threaded build (`cp314t`).

> **Status:** Public preview (alpha). Not yet production-ready; feedback welcome
> via [GitHub Issues](https://github.com/aerospike/aerospike-client-python-async/issues).

> **Not officially supported as a standalone client; APIs at this layer are
> undocumented and may change between releases without notice.** This package
> is the low-level primitive layer underneath the
> [Aerospike Python SDK](https://pypi.org/project/aerospike-sdk/). The
> reference sections below exist for SDK users who need to drop down to
> low-level configuration (TLS, multi-record transactions, strong-consistency
> read modes, wire compression) and for client contributors.

## Resources

- **PyPI:** https://pypi.org/project/aerospike-async/
- **Source:** https://github.com/aerospike/aerospike-client-python-async
- **Issues:** https://github.com/aerospike/aerospike-client-python-async/issues
- **Releases:** https://github.com/aerospike/aerospike-client-python-async/releases

## Installation

```bash
pip install aerospike-async
```

Pin to a specific release if you need reproducible builds:

```bash
pip install aerospike-async==0.6.0a1  # latest on PyPI as of this writing
```

Pre-built wheels are published for every supported platform/Python combination
on regular CPython (3.11 – 3.14, ABI tags `cp311`–`cp314`) **and on the
free-threaded build** (`cp314t`), so no Rust toolchain is required
for ordinary use. If pip resolves to an sdist on your platform, see
[Building from source](#building-from-source) below.

## Quick start

```python
import asyncio

from aerospike_async import (
    ClientPolicy,
    Key,
    ReadPolicy,
    WritePolicy,
    new_client,
)


async def main():
    client = await new_client(ClientPolicy(), "localhost:3000")

    key = Key("test", "demo", "user1")

    # Write a record (bins are plain dicts)
    await client.put(key, {"name": "Alice", "age": 28})

    # Read it back
    record = await client.get(key)
    print(record.bins)  # {'name': 'Alice', 'age': 28}

    # Read specific bins only
    record = await client.get(key, ["name"])
    print(record.bins)  # {'name': 'Alice'}

    # Delete the record
    await client.delete(key)

    await client.close()


asyncio.run(main())
```

## Blocking (sync) surface

Every async method has a `*_blocking` counterpart that blocks the calling
thread instead of returning an awaitable. Use these when you need sync
semantics without an event loop — particularly useful on free-threaded
Python, where multiple OS threads can drive blocking calls in parallel
without the GIL serializing them.

```python
from aerospike_async import ClientPolicy, Key, new_client_blocking

client = new_client_blocking(ClientPolicy(), "localhost:3000")

key = Key("test", "demo", "user1")

# Write a record
client.put_blocking(key, {"name": "Alice", "age": 28})

# Read it back
record = client.get_blocking(key)
print(record.bins)  # {'name': 'Alice', 'age': 28}

# Delete the record
client.delete_blocking(key)

client.close_blocking()
```

Naming follows `<async_method>_blocking` — `get_blocking`, `put_blocking`,
`operate_blocking`, `batch_read_blocking`, `query_execute_blocking`, etc.

Clients created with `new_client_blocking()` are blocking-only; async
methods on them raise. If you want a single client that exposes both
sync and async ergonomics with a friendlier high-level API (sessions,
behaviors, AEL filters), use the
[Aerospike Python SDK](https://pypi.org/project/aerospike-sdk/) instead
— its `SyncClient` is built directly on top of this blocking surface.

## TLS configuration

The client supports TLS for secure connections and PKI (certificate-based)
authentication.

### Basic TLS

```python
from aerospike_async import ClientPolicy, TlsConfig, new_client

policy = ClientPolicy()
policy.tls_config = TlsConfig("path/to/ca-certificate.pem")
client = await new_client(policy, "tls-host:4333")
```

### TLS with client authentication

```python
policy = ClientPolicy()
policy.tls_config = TlsConfig.with_client_auth(
    "ca.pem",      # CA certificate
    "client.pem",  # Client certificate
    "client.key",  # Client private key
)
client = await new_client(policy, "tls-host:4333")
```

### PKI authentication

PKI mode uses client certificates for authentication (no username/password
required):

```python
from aerospike_async import AuthMode

policy = ClientPolicy()
policy.tls_config = TlsConfig.with_client_auth("ca.pem", "client.pem", "client.key")
policy.set_pki_auth()  # or: policy.set_auth_mode(AuthMode.PKI)
client = await new_client(policy, "tls-host:4333")
```

### TLS name in host strings

When the server certificate name differs from the connection hostname, specify
the TLS name:

```python
# Format: hostname:tls_name:port
# Example: connect to IP but validate certificate against "server.example.com"
client = await new_client(policy, "192.168.1.100:server.example.com:4333")
```

### Authentication modes

The client supports multiple authentication modes via `AuthMode`:

- `AuthMode.NONE` — no authentication
- `AuthMode.INTERNAL` — internal authentication (username/password)
- `AuthMode.EXTERNAL` — external authentication (LDAP, etc.)
- `AuthMode.PKI` — certificate-based authentication (requires TLS + client cert)

```python
from aerospike_async import AuthMode

policy = ClientPolicy()
policy.set_auth_mode(AuthMode.INTERNAL, user="admin", password="secret")
# or
policy.set_auth_mode(AuthMode.PKI)  # No user/password needed
```

## Multi-record transactions (MRT)

Multi-record transactions require a strong-consistency namespace on the server
(Aerospike 8.0+). Group operations into a single atomic transaction by
attaching a `Txn` to each policy, then `commit` or `abort`:

```python
from aerospike_async import CommitStatus, Txn
from aerospike_async.exceptions import CommitFailedError

txn = Txn()

write = WritePolicy()
write.set_txn(txn)
read = ReadPolicy()
read.set_txn(txn)

try:
    await client.put(key_a, {"balance": 100}, policy=write)
    await client.put(key_b, {"balance": 200}, policy=write)
    status = await client.commit(txn)
    assert status == CommitStatus.OK_VERIFIED
except CommitFailedError:
    await client.abort(txn)
```

MRT-specific failure result codes are exposed on `ResultCode`: `MRT_BLOCKED`,
`MRT_VERSION_MISMATCH`, `MRT_EXPIRED`, `MRT_TOO_MANY_WRITES`, `MRT_COMMITTED`,
`MRT_ABORTED`, `MRT_ALREADY_LOCKED`, `MRT_MONITOR_EXISTS`.

## Strong consistency read modes

Every read-capable policy exposes `read_mode_ap` and `read_mode_sc` for tuning
consistency on AP and SC namespaces respectively:

```python
from aerospike_async import ReadModeAP, ReadModeSC

policy = ReadPolicy()
policy.set_read_mode_ap(ReadModeAP.One)             # AP namespace
policy.set_read_mode_sc(ReadModeSC.Linearize)       # SC namespace
```

## Wire-protocol compression

Every policy exposes a `use_compression` flag (off by default) to enable
compression of request/response payloads on the wire:

```python
policy = WritePolicy()
policy.set_use_compression(True)
```

## Versioning

PAC follows [SemVer](https://semver.org/). Pre-releases use the
`MAJOR.MINOR.PATCH-{alpha,beta,rc}.N` form (e.g. `0.4.0-alpha.1`). PyPI
normalizes these on upload to the equivalent PEP 440 spelling (`0.4.0a1`).

`Cargo.toml` is the single source of truth; `pyproject.toml` does **not**
duplicate the version. maturin reads it from `Cargo.toml` when it builds the
wheel, so the two are guaranteed to match.

See the [Development](#development--contributing) section for the bump
procedure.

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## Development / Contributing

The sections below are for client *contributors*. Downstream users do **not**
need any of this — `pip install aerospike-async` is sufficient to use the
package.

### Prerequisites

This project uses [PyO3](https://pyo3.rs/) to build a Rust extension for
Python. You will need:

- **Python** 3.11 - 3.14, **or** 3.14t (free-threaded) for high-throughput / PSDK `AsyncPool` work.
  The client supports every CPython version under upstream security support; the
  floor rises in minor releases as versions reach end-of-life.
  Recommended installer: [`uv`](https://docs.astral.sh/uv/) (`uv python install 3.14.5+freethreaded`)
  or [`pyenv`](https://github.com/pyenv/pyenv) with a dedicated environment.
- **Rust toolchain** (`rustc` + `cargo`) — always required when building from source
- **Aerospike server** — required for integration tests

If Rust is not already installed:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Verify
rustc --version
cargo --version
```

### Building from source

Install Python build/test dependencies (any virtualenv works; pyenv is
recommended):

```bash
pip install -r requirements.txt
```

Build & test in one step (Rust build, stub regeneration, full test suite):

```bash
make dev-test
```

Or build only — produces a development wheel and installs it into the active
virtualenv:

```bash
make dev
```

Regenerate Python stubs (only needed after modifying Rust code):

```bash
make stubs
```

### Configuration

Edit `aerospike.env` to match your Aerospike database node configuration:

```bash
export AEROSPIKE_HOST=localhost:3000
```

For local-only overrides (e.g. TLS certificate paths), create an
`aerospike.env.local` file in the repo root. It is gitignored and automatically
sourced by `aerospike.env`.

### Running tests

```bash
make test          # unit + integration
make test-unit     # unit tests only (no server required)
make test-int      # integration tests only (requires running Aerospike server)
```

**macOS file descriptor limit.** On macOS, you may encounter
`ConnectionError: Failed to connect to host(s)` errors when running the full
test suite. The default file descriptor limit (256) can be exceeded by the
async client's concurrent connections.

```bash
ulimit -n 4096   # current shell session
make test
```

To make this permanent, add `ulimit -n 4096` to your shell profile (`~/.zshrc`
or `~/.bash_profile`).

### Bumping the version

Bumps are manual and happen in PRs against `dev`. Promotion workflows
(`dev → stage → main`) do not mutate the version.

```bash
# 1. Edit Cargo.toml [package] version field, then refresh Cargo.lock:
#    e.g. 0.6.0-alpha.1  →  0.6.0-alpha.2
cargo check    # or: cargo update -p aerospike_async --precise 0.6.0-alpha.2

# 2. Confirm:
bin/get-version    # prints 0.6.0-alpha.2

# 3. Open a PR against dev with just this change.
```

### Reading the version programmatically

Anywhere a build script, CI step, or release tool needs the version:

```bash
bin/get-version    # → 0.6.0-alpha.1
```

The script parses the first `version` field inside the `[package]` table of
`Cargo.toml`. It has no Python or cargo runtime dependencies — usable from any
shell, container, or CI environment.
