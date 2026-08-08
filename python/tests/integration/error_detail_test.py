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

"""Integration tests for extended server error detail (server >= 8.1.3).

When ``error_detail_verbosity`` is raised on the operation's policy, a failing
response carries a numeric subcode (verbosity 1+) and a human-readable message
(verbosity 2+). These are surfaced on ``ServerError.sub_code`` and
``ServerError.server_message``.

The triggering ops here are CDT list index/rank out-of-bounds, which reliably
emit subcodes under ``OP_NOT_APPLICABLE``. Subcode *values* are asserted
directly (rather than via named ``SubCode`` constants) because a value is the
stable contract paired with its result code; the names track the core catalog.
"""

import pytest
import pytest_asyncio

from aerospike_async import (
    BatchPolicy,
    BatchReadPolicy,
    ClientPolicy,
    ErrorDetailVerbosity,
    ExpressionTrace,
    FilterExpression,
    Key,
    ListOperation,
    ListReturnType,
    Operation,
    ResultCode,
    ServerError,
    WritePolicy,
    new_client,
)


# All tests share one session-scoped event loop so the module-scoped client
# (one connect per module, not per test) is usable across them.
pytestmark = pytest.mark.asyncio(loop_scope="session")

# Subcode values under OP_NOT_APPLICABLE, from the server's per-status enum.
_SUB_CDT_INDEX_OUT_OF_BOUNDS = 1
_SUB_CDT_RANK_OUT_OF_BOUNDS = 2


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def edv_client(aerospike_host, use_services_alternate, supports_error_detail):
    """Module-scoped client for the extended-error-detail suite.

    Single-host model: connects to the default ``AEROSPIKE_HOST`` and skips the
    whole suite unless that cluster supplies extended error detail (server
    >= 8.1.3). Point ``AEROSPIKE_HOST`` at an 8.1.3+ build to run these; CI
    covers the version spread with a server matrix rather than a dedicated
    host var. ``supports_error_detail`` is also ``False`` when the seed is
    unreachable (``server_version`` probes to ``None``), so a down cluster
    skips cleanly rather than erroring.
    """
    if not supports_error_detail:
        pytest.skip(
            "default cluster does not supply extended error detail "
            "(server < 8.1.3, or unreachable)"
        )
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)
    yield client
    await client.close()


@pytest.fixture
def key():
    return Key("test", "test", "error-detail")


async def _operate_expecting_error(client, key, op, verbosity):
    """Seed a list bin, run ``op`` at ``verbosity``, and return the raised
    ``ServerError`` (fails the test if the op unexpectedly succeeds)."""
    await client.put(key, {"nums": [1, 2, 3]})
    wp = WritePolicy()
    if verbosity is not None:
        wp.error_detail_verbosity = verbosity
    try:
        await client.operate(key, [op], policy=wp)
    except ServerError as exc:
        return exc
    pytest.fail("expected the out-of-bounds op to raise ServerError")


class TestErrorDetail:
    """Verbosity controls how much failure detail the server returns."""

    async def test_default_verbosity_yields_no_detail(self, edv_client, key):
        op = ListOperation.get_by_index("nums", 99, ListReturnType.VALUE)
        exc = await _operate_expecting_error(edv_client, key, op, None)
        assert exc.result_code == ResultCode.OP_NOT_APPLICABLE
        assert exc.sub_code is None
        assert exc.server_message is None

    async def test_verbosity_none_yields_no_detail(self, edv_client, key):
        op = ListOperation.get_by_index("nums", 99, ListReturnType.VALUE)
        exc = await _operate_expecting_error(edv_client, key, op, ErrorDetailVerbosity.NONE)
        assert exc.sub_code is None
        assert exc.server_message is None

    async def test_verbosity_subcode_sets_subcode(self, edv_client, key):
        op = ListOperation.get_by_index("nums", 99, ListReturnType.VALUE)
        exc = await _operate_expecting_error(edv_client, key, op, ErrorDetailVerbosity.SUBCODE)
        assert exc.result_code == ResultCode.OP_NOT_APPLICABLE
        assert exc.sub_code == _SUB_CDT_INDEX_OUT_OF_BOUNDS

    async def test_verbosity_message_adds_server_message(self, edv_client, key):
        op = ListOperation.get_by_index("nums", 99, ListReturnType.VALUE)
        exc = await _operate_expecting_error(edv_client, key, op, ErrorDetailVerbosity.MESSAGE)
        assert exc.sub_code == _SUB_CDT_INDEX_OUT_OF_BOUNDS
        assert exc.server_message is not None
        assert "out of bounds" in exc.server_message

    async def test_subcode_is_scoped_to_result_code(self, edv_client, key):
        # Two distinct failures under the same result code carry distinct
        # subcodes — the (result_code, subcode) pair is what identifies the
        # condition.
        index_op = ListOperation.get_by_index("nums", 99, ListReturnType.VALUE)
        rank_op = ListOperation.get_by_rank("nums", 99, ListReturnType.VALUE)
        index_exc = await _operate_expecting_error(
            edv_client, key, index_op, ErrorDetailVerbosity.MESSAGE
        )
        rank_exc = await _operate_expecting_error(
            edv_client, key, rank_op, ErrorDetailVerbosity.MESSAGE
        )
        assert index_exc.result_code == ResultCode.OP_NOT_APPLICABLE
        assert rank_exc.result_code == ResultCode.OP_NOT_APPLICABLE
        assert index_exc.sub_code == _SUB_CDT_INDEX_OUT_OF_BOUNDS
        assert rank_exc.sub_code == _SUB_CDT_RANK_OUT_OF_BOUNDS


class TestBatchErrorDetail:
    """Per-record error detail on batch results.

    A batch reports failures per record rather than raising, so the subcode the
    single-key path puts on the exception has to travel on the record instead.
    """

    async def test_batch_per_record_carries_subcode(self, edv_client):
        """A failing record in a batch should carry the subcode the equivalent
        single-key failure raises."""
        bad = Key("test", "test", "error-detail-batch-bad")
        good = Key("test", "test", "error-detail-batch-good")
        for k in (bad, good):
            await edv_client.put(k, {"nums": [1, 2, 3]})

        bp = BatchPolicy()
        bp.error_detail_verbosity = ErrorDetailVerbosity.MESSAGE
        results = await edv_client.batch_operate(
            [bad, good],
            [
                [ListOperation.get_by_index("nums", 99, ListReturnType.VALUE)],
                [Operation.get_bin("nums")],
            ],
            batch_policy=bp,
            write_policy=None,
        )

        # Precondition: the failure is reported per record, not raised.
        assert results[0].result_code == ResultCode.OP_NOT_APPLICABLE
        assert results[1].result_code == ResultCode.OK

        assert results[0].sub_code == _SUB_CDT_INDEX_OUT_OF_BOUNDS

    async def test_batch_per_record_carries_server_message(self, edv_client):
        """At MESSAGE verbosity a failing record also carries the server's
        human-readable message; a succeeding record carries neither message
        nor subcode. Mirrors the single-key ``server_message`` surface."""
        bad = Key("test", "test", "error-detail-batch-msg-bad")
        good = Key("test", "test", "error-detail-batch-msg-good")
        for k in (bad, good):
            await edv_client.put(k, {"nums": [1, 2, 3]})

        bp = BatchPolicy()
        bp.error_detail_verbosity = ErrorDetailVerbosity.MESSAGE
        results = await edv_client.batch_operate(
            [bad, good],
            [
                [ListOperation.get_by_index("nums", 99, ListReturnType.VALUE)],
                [Operation.get_bin("nums")],
            ],
            batch_policy=bp,
            write_policy=None,
        )

        failed, succeeded = results[0], results[1]
        assert failed.result_code == ResultCode.OP_NOT_APPLICABLE
        assert failed.server_message is not None
        assert "out of bounds" in failed.server_message
        # No expression involved, so no build trace is attached.
        assert failed.exp_trace is None

        assert succeeded.result_code == ResultCode.OK
        assert succeeded.server_message is None
        assert succeeded.exp_trace is None

    async def test_batch_default_verbosity_yields_no_detail(self, edv_client):
        """Without a verbosity request, a failing record exposes neither a
        subcode nor a message (the request opts in to detail)."""
        bad = Key("test", "test", "error-detail-batch-no-detail")
        await edv_client.put(bad, {"nums": [1, 2, 3]})

        results = await edv_client.batch_operate(
            [bad],
            [[ListOperation.get_by_index("nums", 99, ListReturnType.VALUE)]],
            batch_policy=None,
            write_policy=None,
        )

        row = results[0]
        assert row.result_code == ResultCode.OP_NOT_APPLICABLE
        assert row.sub_code is None
        assert row.server_message is None
        assert row.exp_trace is None

    async def test_batch_invalid_expression_carries_exp_trace(self, edv_client):
        """A malformed filter expression on a batch read fails each row with
        PARAMETER_ERROR and, at EXPRESSION_TRACE verbosity, carries the
        server's message and a structured expression trace per row.

        An expression built from raw non-expression bytes compiles fine on
        the client but the server rejects it on build. ``from_base64`` keeps
        the packed bytes verbatim (no client-side structural validation), so
        base64 of arbitrary bytes reaches the server as a bad expression.

        Pins the full batch error-detail surface end to end — subcode (NONE
        for an expression rejection), message, and expression trace — parsed
        through the same path the single-key commands use. Requires a server
        that emits batch expression traces (8.1.3 build >= -75; earlier
        preliminary 8.1.3 images returned the message without a trace).
        """
        keys = [
            Key("test", "test", "error-detail-batch-badexp-1"),
            Key("test", "test", "error-detail-batch-badexp-2"),
        ]
        for k in keys:
            await edv_client.put(k, {"nums": [1, 2, 3]})

        # base64 of [0xFF, 0xFE, 0xFD] — valid base64, invalid expression wire form.
        bad_expr = FilterExpression.from_base64("//79")
        rp = BatchReadPolicy()
        rp.filter_expression = bad_expr
        bp = BatchPolicy()
        bp.error_detail_verbosity = ErrorDetailVerbosity.EXPRESSION_TRACE

        results = await edv_client.batch_read(
            keys, None, batch_policy=bp, read_policy=rp)

        assert len(results) == 2
        for row in results:
            assert row.result_code == ResultCode.PARAMETER_ERROR
            # An expression rejection carries no meaningful subcode
            # (SubCode.NONE), surfaced here as 0.
            assert not row.sub_code
            assert row.server_message is not None
            assert isinstance(row.exp_trace, ExpressionTrace)
