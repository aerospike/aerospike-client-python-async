# Copyright 2023-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Integration tests for multi-record transactions (MRT).

Requires a strong-consistency namespace. Point the suite at it via the
``AEROSPIKE_SC_*`` environment variables:

    AEROSPIKE_SC_HOST          seed host:port (default: AEROSPIKE_HOST)
    AEROSPIKE_SC_NAMESPACE     SC namespace name (default: ``test_sc``)
    AEROSPIKE_SC_USER          auth user (default: AEROSPIKE_AUTH_USER)
    AEROSPIKE_SC_PASSWORD      auth password (default: AEROSPIKE_AUTH_PASSWORD)
    AEROSPIKE_SC_AUTH_MODE     auth mode (default: AEROSPIKE_AUTH_MODE, else INTERNAL)

When the cluster isn't reachable, the namespace isn't configured, or it
isn't strong-consistency, every test skips cleanly with a clear reason.
"""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio

from aerospike_async import (
    AbortStatus,
    AuthMode,
    BatchPolicy,
    ClientPolicy,
    CommitStatus,
    Key,
    ReadPolicy,
    ResultCode,
    Txn,
    TxnState,
    WritePolicy,
    new_client,
)


_AUTH_MODES = {
    "NONE": AuthMode.NONE,
    "INTERNAL": AuthMode.INTERNAL,
    "EXTERNAL": AuthMode.EXTERNAL,
    "PKI": AuthMode.PKI,
}


def _sc_client_policy() -> ClientPolicy:
    cp = ClientPolicy()
    cp.use_services_alternate = (
        os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "").lower() == "true"
    )
    user = os.environ.get("AEROSPIKE_SC_USER") or os.environ.get("AEROSPIKE_AUTH_USER")
    password = (
        os.environ.get("AEROSPIKE_SC_PASSWORD")
        or os.environ.get("AEROSPIKE_AUTH_PASSWORD")
    )
    mode_name = (
        os.environ.get("AEROSPIKE_SC_AUTH_MODE")
        or os.environ.get("AEROSPIKE_AUTH_MODE")
        or ""
    ).strip().upper()
    if user and password:
        mode = _AUTH_MODES.get(mode_name, AuthMode.INTERNAL)
        cp.set_auth_mode(mode, user=user, password=password)
    return cp


def _sc_host() -> str:
    return os.environ.get("AEROSPIKE_SC_HOST") or os.environ.get("AEROSPIKE_HOST", "")


def _sc_namespace() -> str:
    return os.environ.get("AEROSPIKE_SC_NAMESPACE", "test_sc")


@pytest.fixture(scope="module")
def sc_namespace() -> str:
    return _sc_namespace()


@pytest_asyncio.fixture
async def sc_client(sc_namespace):
    """Async client pointed at the configured SC cluster.

    Skips whenever the cluster isn't reachable or the namespace isn't
    strong-consistency — mirrors the PSDK parity suite's skip shape.
    """
    host = _sc_host()
    if not host:
        pytest.skip(
            "AEROSPIKE_SC_HOST (or AEROSPIKE_HOST) is not set; "
            "cannot reach a strong-consistency cluster"
        )

    try:
        client = await new_client(_sc_client_policy(), host)
    except Exception as exc:
        pytest.skip(f"cannot connect to SC cluster at {host!r}: {exc}")

    try:
        ns_info = await client.info(f"namespace/{sc_namespace}")
    except Exception as exc:
        await client.close()
        pytest.skip(
            f"namespace {sc_namespace!r} unreachable on {host!r}: {exc}; "
            f"set AEROSPIKE_SC_NAMESPACE or stand up a strong-consistency cluster"
        )

    # ``info`` returns one entry per seed node; all should agree for an SC ns.
    is_sc = False
    for body in ns_info.values():
        parts = dict(kv.split("=", 1) for kv in body.split(";") if "=" in kv)
        if parts.get("strong-consistency") == "true":
            is_sc = True
            break
    if not is_sc:
        await client.close()
        pytest.skip(
            f"namespace {sc_namespace!r} on {host!r} is not strong-consistency; "
            "MRT tests require SC"
        )

    yield client
    await client.close()


@pytest.fixture
def sc_key(sc_namespace):
    """Fresh, unique key per test so parallel/retry runs don't collide."""
    return Key(sc_namespace, "mrt", f"pac-mrt-{uuid.uuid4().hex[:12]}")


async def _get_bin(client: "any", key: Key, bin_name: str):
    rec = await client.get(ReadPolicy(), key)
    if rec is None:
        return None
    return rec.bins.get(bin_name)


# ---------------------------------------------------------------------------
# Commit / abort persistence
# ---------------------------------------------------------------------------
async def test_txn_commit_persists_writes(sc_client, sc_key):
    # Pre-existing value outside the txn.
    await sc_client.put(WritePolicy(), sc_key, {"bin": 1})

    txn = Txn()
    wp = WritePolicy()
    wp.txn = txn
    await sc_client.put(wp, sc_key, {"bin": 2})

    status = await sc_client.commit(txn)
    assert status == CommitStatus.OK
    assert await _get_bin(sc_client, sc_key, "bin") == 2


async def test_txn_abort_rolls_back(sc_client, sc_key):
    await sc_client.put(WritePolicy(), sc_key, {"bin": 1})

    txn = Txn()
    wp = WritePolicy()
    wp.txn = txn
    await sc_client.put(wp, sc_key, {"bin": 2})

    status = await sc_client.abort(txn)
    assert status == AbortStatus.OK
    assert await _get_bin(sc_client, sc_key, "bin") == 1


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------
async def test_txn_state_open_then_committed(sc_client, sc_key):
    await sc_client.put(WritePolicy(), sc_key, {"bin": 1})
    txn = Txn()
    assert txn.state == TxnState.OPEN
    wp = WritePolicy()
    wp.txn = txn
    await sc_client.put(wp, sc_key, {"bin": 2})

    status = await sc_client.commit(txn)
    assert status == CommitStatus.OK
    assert txn.state == TxnState.COMMITTED


async def test_txn_state_open_then_aborted(sc_client, sc_key):
    await sc_client.put(WritePolicy(), sc_key, {"bin": 1})
    txn = Txn()
    assert txn.state == TxnState.OPEN
    wp = WritePolicy()
    wp.txn = txn
    await sc_client.put(wp, sc_key, {"bin": 2})

    status = await sc_client.abort(txn)
    assert status == AbortStatus.OK
    assert txn.state == TxnState.ABORTED


# ---------------------------------------------------------------------------
# Non-open txns must refuse further writes (client-side guard)
# ---------------------------------------------------------------------------
async def test_committed_txn_rejects_subsequent_writes(sc_client, sc_key):
    txn = Txn()
    wp = WritePolicy()
    wp.txn = txn
    await sc_client.put(wp, sc_key, {"bin": 1})
    assert await sc_client.commit(txn) == CommitStatus.OK

    with pytest.raises(Exception) as excinfo:
        await sc_client.put(wp, sc_key, {"bin": 2})
    msg = str(excinfo.value).lower()
    assert (
        "forbidden" in msg
        or "commit" in msg
        or "abort" in msg
    ), f"unexpected error text on committed txn: {excinfo.value!r}"


# ---------------------------------------------------------------------------
# Batch write under a txn
# ---------------------------------------------------------------------------
async def test_txn_batch_commit(sc_client, sc_namespace):
    keys = [
        Key(sc_namespace, "mrt", f"pac-mrt-batch-{uuid.uuid4().hex[:12]}-{i}")
        for i in range(5)
    ]
    for k in keys:
        await sc_client.put(WritePolicy(), k, {"bin": 1})

    txn = Txn()
    bp = BatchPolicy()
    bp.txn = txn

    results = await sc_client.batch_write(
        bp,
        None,
        keys,
        [{"bin": 2}] * len(keys),
    )
    for rec in results:
        assert rec.result_code == ResultCode.OK, (
            f"per-key batch failure: rc={rec.result_code}"
        )

    status = await sc_client.commit(txn)
    assert status == CommitStatus.OK

    for k in keys:
        assert await _get_bin(sc_client, k, "bin") == 2
