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

"""Regression tests for the single-key batch fast-path bug in core.

When a batch group contains exactly one key destined for a node, core routes
it through a single-key command instead of the batch wire protocol. Two
defects in that path cause behavior to diverge from the batch contract:

1. **Per-key error propagation**: The single-key path only absorbs
   KEY_NOT_FOUND_ERROR and FILTERED_OUT on the BatchRecord. All other
   per-key server errors (PARAMETER_ERROR, KEY_EXISTS_ERROR,
   GENERATION_ERROR, etc.) are both recorded on the BatchRecord *and*
   propagated as an exception, aborting the batch. The batch contract says
   every per-key result code lands on the BatchRecord; only cluster-level
   errors propagate.

2. **Delete-of-nonexistent returns OK**: The single-key delete command
   treats KEY_NOT_FOUND as success (Ok(())), and the fast-path caller
   unconditionally stamps result_code = OK. The batch wire protocol
   correctly returns KEY_NOT_FOUND_ERROR per record.

Each test below contrasts a 1-key batch (hits the fast path on a
single-node cluster) against the expected batch contract. When core fixes
the fast path, remove the xfail markers.
"""

import base64
import os

import pytest
import pytest_asyncio

from aerospike_async import (
    BatchDeletePolicy,
    BatchPolicy,
    BatchReadPolicy,
    BatchWriteOp,
    BatchWritePolicy,
    ClientPolicy,
    FilterExpression,
    Key,
    Operation,
    ReadPolicy,
    RecordExistsAction,
    WritePolicy,
    new_client,
)
from aerospike_async.exceptions import ResultCode


NS = "test"
SET = "batch_singlekey_bug"


@pytest_asyncio.fixture
async def client():
    cp = ClientPolicy()
    cp.use_services_alternate = True
    c = await new_client(cp, os.environ.get("AEROSPIKE_HOST", "localhost:3000"))
    yield c
    await c.close()


# ---------------------------------------------------------------------------
# Bug 2: delete of nonexistent key returns OK instead of KEY_NOT_FOUND_ERROR
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    reason="Core single-key fast path returns OK for delete of nonexistent key",
    strict=True,
)
async def test_single_key_batch_delete_nonexistent(client):
    """A 1-key batch_delete of a key that doesn't exist must report
    KEY_NOT_FOUND_ERROR on the BatchRecord, not OK.
    """
    key = Key(NS, SET, "does_not_exist_delete")
    try:
        await client.delete(key, policy=WritePolicy())
    except Exception:
        pass

    results = await client.batch_delete(
        [key], batch_policy=BatchPolicy(), delete_policy=BatchDeletePolicy()
    )
    assert len(results) == 1
    assert results[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR


# ---------------------------------------------------------------------------
# Bug 1: per-key errors propagate as exceptions instead of BatchRecord codes
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    reason="Core single-key fast path propagates KEY_EXISTS_ERROR as exception",
    strict=True,
    raises=Exception,
)
async def test_single_key_batch_write_key_exists_error(client):
    """A 1-key batch write with CREATE_ONLY on an existing key must record
    KEY_EXISTS_ERROR on the BatchRecord, not raise an exception.
    """
    key = Key(NS, SET, "exists_for_create_only")
    await client.put(key, {"v": 1}, policy=WritePolicy())

    bwp = BatchWritePolicy()
    bwp.record_exists_action = RecordExistsAction.CREATE_ONLY

    results = await client.batch(
        [BatchWriteOp(key, [Operation.put("v", 2)], policy=bwp)],
        batch_policy=BatchPolicy(),
    )
    assert len(results) == 1
    assert results[0].result_code == ResultCode.KEY_EXISTS_ERROR


@pytest.mark.xfail(
    reason="Core single-key fast path propagates PARAMETER_ERROR as exception",
    strict=True,
    raises=Exception,
)
async def test_single_key_batch_read_parameter_error(client):
    """A 1-key batch_read with an invalid filter expression must record
    PARAMETER_ERROR on the BatchRecord, not raise an exception.
    """
    key = Key(NS, SET, "param_error_key")
    await client.put(key, {"b": 1}, policy=WritePolicy())

    garbage_b64 = base64.b64encode(b"\xff\x00not-a-valid-expression").decode("ascii")
    brp = BatchReadPolicy()
    brp.filter_expression = FilterExpression.from_base64(garbage_b64)

    results = await client.batch_read(
        [key], ["b"], batch_policy=BatchPolicy(), read_policy=brp
    )
    assert len(results) == 1
    assert results[0].result_code == ResultCode.PARAMETER_ERROR


# ---------------------------------------------------------------------------
# Contrast: 2-key batches take the normal batch path and work correctly.
# These pass today and prove the bug is isolated to the single-key fast path.
# ---------------------------------------------------------------------------

async def test_two_key_batch_delete_nonexistent(client):
    """With 2 keys (normal batch path), delete of nonexistent keys correctly
    reports KEY_NOT_FOUND_ERROR on each BatchRecord.
    """
    keys = [
        Key(NS, SET, "two_key_nx_delete_0"),
        Key(NS, SET, "two_key_nx_delete_1"),
    ]
    for k in keys:
        try:
            await client.delete(k, policy=WritePolicy())
        except Exception:
            pass

    results = await client.batch_delete(
        keys, batch_policy=BatchPolicy(), delete_policy=BatchDeletePolicy()
    )
    assert len(results) == 2
    for r in results:
        assert r.result_code == ResultCode.KEY_NOT_FOUND_ERROR


async def test_two_key_batch_write_key_exists_error(client):
    """With 2 keys (normal batch path), CREATE_ONLY on existing keys correctly
    records KEY_EXISTS_ERROR on each BatchRecord without raising.
    """
    keys = [
        Key(NS, SET, "two_key_exists_0"),
        Key(NS, SET, "two_key_exists_1"),
    ]
    for k in keys:
        await client.put(k, {"v": 1}, policy=WritePolicy())

    bwp = BatchWritePolicy()
    bwp.record_exists_action = RecordExistsAction.CREATE_ONLY

    results = await client.batch(
        [
            BatchWriteOp(keys[0], [Operation.put("v", 2)], policy=bwp),
            BatchWriteOp(keys[1], [Operation.put("v", 2)], policy=bwp),
        ],
        batch_policy=BatchPolicy(),
    )
    assert len(results) == 2
    for r in results:
        assert r.result_code == ResultCode.KEY_EXISTS_ERROR
