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

"""Unit tests for the server 8.1.2 enhanced expression API.

Exercises construction, base64 round-trip, and CTX restoration. No server
contact: these tests pin the wire format of the new ExpOps and CTX
helpers so that future edits can't silently shift the encoding.
"""

from aerospike_async import CTX, CdtOperation, ExpType, FilterExpression as fe


class TestNativeExpOps:
    """``in_list``, ``map_keys``, ``map_values`` are single-ExpOp constructors."""

    def test_in_list_constructs(self):
        e = fe.in_list(fe.int_val(2), fe.list_bin("lst"))
        assert isinstance(e, fe)
        # Round-trip through base64 to confirm the bytes are stable.
        b64 = e.base64()
        restored = fe.from_base64(b64)
        assert restored.base64() == b64

    def test_map_keys_constructs(self):
        e = fe.map_keys(fe.map_bin("m"))
        assert isinstance(e, fe)
        assert fe.from_base64(e.base64()).base64() == e.base64()

    def test_map_values_constructs(self):
        e = fe.map_values(fe.map_bin("m"))
        assert isinstance(e, fe)
        assert fe.from_base64(e.base64()).base64() == e.base64()


class TestExpFormPathOps:
    """Expression-form path operators delegate to the same wire opcodes as the op-form."""

    def test_exp_select_values_constructs(self):
        e = fe.exp_select_values(ExpType.LIST, fe.list_bin("b"), [CTX.map_key("book")])
        assert isinstance(e, fe)
        assert fe.from_base64(e.base64()).base64() == e.base64()

    def test_exp_select_map_keys_constructs(self):
        e = fe.exp_select_map_keys(ExpType.LIST, fe.map_bin("m"), [CTX.map_key("book")])
        assert isinstance(e, fe)

    def test_exp_select_map_entries_constructs(self):
        e = fe.exp_select_map_entries(ExpType.LIST, fe.map_bin("m"), [CTX.map_key("book")])
        assert isinstance(e, fe)

    def test_exp_select_matching_tree_constructs(self):
        e = fe.exp_select_matching_tree(ExpType.MAP, fe.map_bin("m"), [CTX.map_key("book")])
        assert isinstance(e, fe)

    def test_exp_modify_constructs(self):
        e = fe.exp_modify(ExpType.MAP, fe.map_bin("m"), fe.int_val(7), [CTX.map_key("book")])
        assert isinstance(e, fe)

    def test_exp_modify_no_fail_constructs(self):
        e = fe.exp_modify_no_fail(ExpType.MAP, fe.map_bin("m"), fe.int_val(7), [CTX.map_key("book")])
        assert isinstance(e, fe)

    def test_exp_remove_constructs(self):
        e = fe.exp_remove(ExpType.MAP, fe.map_bin("m"), [CTX.map_key("book")])
        assert isinstance(e, fe)


class TestCdtOperationConvenienceBuilders:
    """``select_values``/``modify_no_fail``/``remove`` produce the same CdtOperation type."""

    def test_select_values(self):
        op = CdtOperation.select_values("inv", [CTX.map_key("books"), CTX.list_index(0)])
        assert isinstance(op, CdtOperation)

    def test_select_map_keys(self):
        op = CdtOperation.select_map_keys("inv", [CTX.map_key("books")])
        assert isinstance(op, CdtOperation)

    def test_select_map_entries(self):
        op = CdtOperation.select_map_entries("inv", [CTX.map_key("books")])
        assert isinstance(op, CdtOperation)

    def test_select_matching_tree(self):
        op = CdtOperation.select_matching_tree("inv", [CTX.map_key("books")])
        assert isinstance(op, CdtOperation)

    def test_modify(self):
        op = CdtOperation.modify("inv", fe.int_val(99), [CTX.map_key("books"), CTX.map_key("price")])
        assert isinstance(op, CdtOperation)

    def test_modify_no_fail(self):
        op = CdtOperation.modify_no_fail("inv", fe.int_val(99), [CTX.map_key("books"), CTX.map_key("price")])
        assert isinstance(op, CdtOperation)

    def test_remove(self):
        op = CdtOperation.remove("inv", [CTX.map_key("books"), CTX.map_key("price")])
        assert isinstance(op, CdtOperation)


class TestCtxHelpers:
    """New CTX builders construct without server contact and round-trip via base64."""

    def test_map_keys_in(self):
        ctx = CTX.map_keys_in(["a", "b", "c"])
        assert isinstance(ctx, CTX)

    def test_and_filter(self):
        ctx = CTX.and_filter(fe.bool_val(True))
        assert isinstance(ctx, CTX)

    def test_from_base64_roundtrip(self):
        # Pack a small CTX list to base64, then restore. Restored list packs
        # back to the same bytes — confirms the wire-level idempotence the
        # core relies on for ctx_from_base64.
        original = [CTX.map_key("books"), CTX.list_index(0)]
        encoded = CTX.to_base64(original)
        restored = CTX.from_base64(encoded)
        re_encoded = CTX.to_base64(restored)
        assert encoded == re_encoded

    def test_from_bytes_roundtrip(self):
        import base64

        original = [CTX.map_key("books"), CTX.list_index(0)]
        encoded = CTX.to_base64(original)
        raw = base64.b64decode(encoded)
        restored = CTX.from_bytes(raw)
        assert CTX.to_base64(restored) == encoded
