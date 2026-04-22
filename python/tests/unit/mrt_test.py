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

"""Unit tests for multi-record transaction types: Txn, TxnState, CommitStatus, AbortStatus."""

import pytest
from aerospike_async import Txn, TxnState, CommitStatus, AbortStatus


class TestTxn:

    def test_create_txn(self):
        txn = Txn()
        assert txn is not None

    def test_txn_id_is_int(self):
        txn = Txn()
        assert isinstance(txn.id, int)

    def test_txn_id_nonzero(self):
        txn = Txn()
        assert txn.id != 0

    def test_txn_ids_unique(self):
        a = Txn()
        b = Txn()
        assert a.id != b.id

    def test_txn_state_initial_open(self):
        txn = Txn()
        assert txn.state == TxnState.OPEN

    def test_txn_timeout_default(self):
        txn = Txn()
        assert isinstance(txn.timeout, int)
        assert txn.timeout >= 0

    def test_txn_namespace_default_none(self):
        txn = Txn()
        assert txn.namespace is None

    def test_txn_repr(self):
        txn = Txn()
        r = repr(txn)
        assert "Txn" in r
        assert str(txn.id) in r

    def test_txn_state_is_writable_and_round_trips(self):
        """``Txn.state`` is a writable property (used by SDK parity tests
        that need to force a non-``OPEN`` state without a live cluster)."""
        txn = Txn()
        assert txn.state == TxnState.OPEN
        for target in (
            TxnState.COMMITTED,
            TxnState.ABORTED,
            TxnState.VERIFIED,
            TxnState.OPEN,
        ):
            txn.state = target
            assert txn.state == target

    def test_txn_timeout_is_read_only(self):
        """``Txn.timeout`` stays read-only until core gains atomic storage
        for the timeout field. This guard pairs with the matching skip in
        the SDK's MRT parity suite — when the guard fails, the parity
        test can be filled in."""
        txn = Txn()
        with pytest.raises((AttributeError, TypeError)):
            txn.timeout = 2  # type: ignore[misc]


class TestTxnState:

    def test_all_states_exist(self):
        assert TxnState.OPEN is not None
        assert TxnState.VERIFIED is not None
        assert TxnState.COMMITTED is not None
        assert TxnState.ABORTED is not None

    def test_states_distinct(self):
        states = [TxnState.OPEN, TxnState.VERIFIED, TxnState.COMMITTED, TxnState.ABORTED]
        for i, a in enumerate(states):
            for b in states[i + 1:]:
                assert a != b

    def test_equality(self):
        assert TxnState.OPEN == TxnState.OPEN
        assert TxnState.COMMITTED == TxnState.COMMITTED

    def test_hash(self):
        s = {TxnState.OPEN, TxnState.COMMITTED}
        assert len(s) == 2

    def test_repr(self):
        for state in (TxnState.OPEN, TxnState.VERIFIED, TxnState.COMMITTED, TxnState.ABORTED):
            assert repr(state) != ""


class TestCommitStatus:

    def test_all_statuses_exist(self):
        assert CommitStatus.OK is not None
        assert CommitStatus.ALREADY_COMMITTED is not None
        assert CommitStatus.ROLL_FORWARD_ABANDONED is not None
        assert CommitStatus.CLOSE_ABANDONED is not None

    def test_statuses_distinct(self):
        statuses = [
            CommitStatus.OK,
            CommitStatus.ALREADY_COMMITTED,
            CommitStatus.ROLL_FORWARD_ABANDONED,
            CommitStatus.CLOSE_ABANDONED,
        ]
        for i, a in enumerate(statuses):
            for b in statuses[i + 1:]:
                assert a != b

    def test_equality(self):
        assert CommitStatus.OK == CommitStatus.OK

    def test_hash(self):
        s = {CommitStatus.OK, CommitStatus.ALREADY_COMMITTED}
        assert len(s) == 2

    def test_repr(self):
        for status in (
            CommitStatus.OK,
            CommitStatus.ALREADY_COMMITTED,
            CommitStatus.ROLL_FORWARD_ABANDONED,
            CommitStatus.CLOSE_ABANDONED,
        ):
            assert repr(status) != ""


class TestAbortStatus:

    def test_all_statuses_exist(self):
        assert AbortStatus.OK is not None
        assert AbortStatus.ALREADY_ABORTED is not None
        assert AbortStatus.ROLL_BACK_ABANDONED is not None
        assert AbortStatus.CLOSE_ABANDONED is not None

    def test_statuses_distinct(self):
        statuses = [
            AbortStatus.OK,
            AbortStatus.ALREADY_ABORTED,
            AbortStatus.ROLL_BACK_ABANDONED,
            AbortStatus.CLOSE_ABANDONED,
        ]
        for i, a in enumerate(statuses):
            for b in statuses[i + 1:]:
                assert a != b

    def test_equality(self):
        assert AbortStatus.OK == AbortStatus.OK

    def test_hash(self):
        s = {AbortStatus.OK, AbortStatus.ALREADY_ABORTED}
        assert len(s) == 2

    def test_repr(self):
        for status in (
            AbortStatus.OK,
            AbortStatus.ALREADY_ABORTED,
            AbortStatus.ROLL_BACK_ABANDONED,
            AbortStatus.CLOSE_ABANDONED,
        ):
            assert repr(status) != ""
