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

from aerospike_async import CTX, ListOrderType, MapOrder


class TestCTXListMethods:

    def test_list_index(self):
        ctx = CTX.list_index(0)
        assert isinstance(ctx, CTX)

    def test_list_index_negative(self):
        ctx = CTX.list_index(-1)
        assert isinstance(ctx, CTX)

    def test_list_index_create(self):
        ctx = CTX.list_index_create(0, ListOrderType.ORDERED, True)
        assert isinstance(ctx, CTX)

    def test_list_rank(self):
        ctx = CTX.list_rank(0)
        assert isinstance(ctx, CTX)

    def test_list_rank_negative(self):
        ctx = CTX.list_rank(-1)
        assert isinstance(ctx, CTX)

    def test_list_value(self):
        ctx = CTX.list_value("hello")
        assert isinstance(ctx, CTX)

    def test_list_value_int(self):
        ctx = CTX.list_value(42)
        assert isinstance(ctx, CTX)


class TestCTXMapMethods:

    def test_map_index(self):
        ctx = CTX.map_index(0)
        assert isinstance(ctx, CTX)

    def test_map_rank(self):
        ctx = CTX.map_rank(0)
        assert isinstance(ctx, CTX)

    def test_map_key(self):
        ctx = CTX.map_key("mykey")
        assert isinstance(ctx, CTX)

    def test_map_key_int(self):
        ctx = CTX.map_key(42)
        assert isinstance(ctx, CTX)

    def test_map_key_create(self):
        ctx = CTX.map_key_create("newkey", MapOrder.KEY_ORDERED)
        assert isinstance(ctx, CTX)

    def test_map_value(self):
        ctx = CTX.map_value("myval")
        assert isinstance(ctx, CTX)


class TestCTXEquality:

    def test_same_list_index_equal(self):
        a = CTX.list_index(0)
        b = CTX.list_index(0)
        assert a == b

    def test_different_list_index_not_equal(self):
        a = CTX.list_index(0)
        b = CTX.list_index(1)
        assert a != b

    def test_same_map_key_equal(self):
        a = CTX.map_key("k")
        b = CTX.map_key("k")
        assert a == b

    def test_different_types_not_equal(self):
        a = CTX.list_index(0)
        b = CTX.map_index(0)
        assert a != b


class TestCTXBase64:
    """``to_base64`` / ``from_base64`` / ``from_bytes`` round-trip a context path."""

    PATH = [CTX.map_key("meta"), CTX.list_index(-1), CTX.list_value(937), CTX.map_rank(2)]

    def test_round_trip_restores_each_step(self):
        restored = CTX.from_base64(CTX.to_base64(self.PATH))
        assert restored == self.PATH

    def test_re_encoding_is_stable(self):
        b64 = CTX.to_base64(self.PATH)
        assert CTX.to_base64(CTX.from_base64(b64)) == b64

    def test_from_bytes_matches_from_base64(self):
        import base64

        b64 = CTX.to_base64(self.PATH)
        assert CTX.from_bytes(list(base64.b64decode(b64))) == self.PATH

    def test_to_bytes_is_the_decoded_base64(self):
        import base64

        packed = CTX.to_bytes(self.PATH)
        assert isinstance(packed, bytes)
        assert packed == base64.b64decode(CTX.to_base64(self.PATH))
        assert CTX.from_bytes(packed) == self.PATH

    def test_expression_contexts_round_trip_byte_for_byte(self):
        # A restored expression is held as its packed bytes rather than the
        # tree it was built from, so ``==`` is false even when nothing was
        # lost; the re-encoding is the proof.
        from aerospike_async import FilterExpression as fe, LoopVarPart

        value_filter = fe.gt(fe.map_loop_var(LoopVarPart.VALUE), fe.int_val(10))
        key_filter = fe.eq(fe.map_loop_var(LoopVarPart.MAP_KEY), fe.string_val("k1"))
        for path in (
            [CTX.all_children()],
            [CTX.all_children_with_filter(value_filter)],
            [CTX.map_key("parent"), CTX.all_children_with_filter(key_filter)],
            [CTX.map_key("parent"), CTX.and_filter(value_filter)],
        ):
            b64 = CTX.to_base64(path)
            restored = CTX.from_base64(b64)
            assert len(restored) == len(path)
            assert CTX.to_base64(restored) == b64

    def test_invalid_base64_raises(self):
        import pytest
        from aerospike_async.exceptions import AerospikeError

        with pytest.raises(AerospikeError):
            CTX.from_base64("[map_key(<string#4>), list_index(0)]")
