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

from aerospike_async import (
    ExpOperation, ExpWriteFlags, ExpReadFlags,
    FilterExpression as fe,
)


class TestExpOperationFlags:
    """Test ExpOperation flags."""

    def test_exp_write_flags_values(self):
        assert int(ExpWriteFlags.DEFAULT) == 0
        assert int(ExpWriteFlags.CREATE_ONLY) == 1
        assert int(ExpWriteFlags.UPDATE_ONLY) == 2
        assert int(ExpWriteFlags.ALLOW_DELETE) == 4
        assert int(ExpWriteFlags.POLICY_NO_FAIL) == 8
        assert int(ExpWriteFlags.EVAL_NO_FAIL) == 16

    def test_exp_read_flags_values(self):
        assert int(ExpReadFlags.DEFAULT) == 0
        assert int(ExpReadFlags.EVAL_NO_FAIL) == 16

    def test_combine_write_flags(self):
        combined = int(ExpWriteFlags.CREATE_ONLY) | int(ExpWriteFlags.POLICY_NO_FAIL)
        assert combined == 9


class TestExpOperationCreate:
    """Test creating ExpOperation objects without a server."""

    def test_read_creates_operation(self):
        expr = fe.int_bin("value")
        op = ExpOperation.read("result", expr)
        assert op is not None
        assert isinstance(op, ExpOperation)

    def test_read_with_flags(self):
        expr = fe.int_bin("value")
        op = ExpOperation.read("result", expr, int(ExpReadFlags.EVAL_NO_FAIL))
        assert op is not None

    def test_write_creates_operation(self):
        expr = fe.int_val(42)
        op = ExpOperation.write("target", expr)
        assert op is not None
        assert isinstance(op, ExpOperation)

    def test_write_with_flags(self):
        expr = fe.int_val(42)
        op = ExpOperation.write("target", expr, int(ExpWriteFlags.CREATE_ONLY))
        assert op is not None
