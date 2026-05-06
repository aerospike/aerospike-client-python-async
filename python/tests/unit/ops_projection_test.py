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

"""Unit tests for ``Statement.set_operations`` (ops projection)."""

import pytest

from aerospike_async import CTX, CdtOperation, Operation, Statement


class TestStatementSetOperations:
    """``Statement.set_operations`` accepts the same op shapes as ``operate``."""

    def test_basic_read_op(self):
        # Plain bin Read — the only op type accepted by servers older than 8.1.2
        # in foreground query ops projection.
        stmt = Statement("test", "users")
        stmt.set_operations([Operation.get_bin("name")])
        # Idempotent — a second call replaces the first projection.
        stmt.set_operations([Operation.get_bin("age")])

    def test_cdt_read_op_projection(self):
        # CDT path read projection (server >= 8.1.2). Constructing the
        # statement does not contact the server, so this only verifies the
        # PAC plumbing carries the op into core.
        stmt = Statement("test", "users")
        stmt.set_operations([CdtOperation.select_values("inventory", [CTX.map_key("books")])])

    def test_rejects_non_op(self):
        stmt = Statement("test", "users")
        with pytest.raises(TypeError):
            stmt.set_operations(["not an op"])

    def test_empty_projection_clears(self):
        # Empty list is allowed at the PAC layer; the core treats it the same
        # as no projection. (The server would reject zero-length operate
        # payloads via CLIENT-4685, but that's a separate code path.)
        stmt = Statement("test", "users")
        stmt.set_operations([])
