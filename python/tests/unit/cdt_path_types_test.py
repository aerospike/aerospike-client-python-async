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

"""Unit tests for CDT path expression types: LoopVarPart, SelectFlag, ModifyFlag, CdtOperation."""

import pytest
from aerospike_async import (
    CTX,
    CdtOperation,
    FilterExpression as fe,
    LoopVarPart,
    ModifyFlag,
    SelectFlag,
)


class TestLoopVarPart:

    def test_all_parts_exist(self):
        assert LoopVarPart.MAP_KEY is not None
        assert LoopVarPart.VALUE is not None
        assert LoopVarPart.INDEX is not None

    def test_parts_distinct(self):
        parts = [LoopVarPart.MAP_KEY, LoopVarPart.VALUE, LoopVarPart.INDEX]
        for i, a in enumerate(parts):
            for b in parts[i + 1:]:
                assert a != b

    def test_equality(self):
        assert LoopVarPart.MAP_KEY == LoopVarPart.MAP_KEY
        assert LoopVarPart.VALUE == LoopVarPart.VALUE
        assert LoopVarPart.INDEX == LoopVarPart.INDEX

    def test_hash(self):
        s = {LoopVarPart.MAP_KEY, LoopVarPart.VALUE, LoopVarPart.INDEX}
        assert len(s) == 3

    def test_repr(self):
        for part in (LoopVarPart.MAP_KEY, LoopVarPart.VALUE, LoopVarPart.INDEX):
            assert repr(part) != ""


class TestSelectFlag:

    def test_all_flags_exist(self):
        assert SelectFlag.VALUE is not None
        assert SelectFlag.MAP_KEY is not None
        assert SelectFlag.MATCHING_TREE is not None
        assert SelectFlag.NO_FAIL is not None

    def test_flags_distinct(self):
        flags = [SelectFlag.VALUE, SelectFlag.MAP_KEY, SelectFlag.MATCHING_TREE, SelectFlag.NO_FAIL]
        for i, a in enumerate(flags):
            for b in flags[i + 1:]:
                assert a != b

    def test_equality(self):
        assert SelectFlag.VALUE == SelectFlag.VALUE
        assert SelectFlag.NO_FAIL == SelectFlag.NO_FAIL

    def test_hash(self):
        s = {SelectFlag.VALUE, SelectFlag.MAP_KEY}
        assert len(s) == 2

    def test_repr(self):
        for flag in (SelectFlag.VALUE, SelectFlag.MAP_KEY, SelectFlag.MATCHING_TREE, SelectFlag.NO_FAIL):
            assert repr(flag) != ""

    def test_or_combines_flags(self):
        combined = SelectFlag.VALUE | SelectFlag.NO_FAIL
        assert combined is not None
        assert combined != SelectFlag.VALUE
        assert combined != SelectFlag.NO_FAIL


class TestModifyFlag:

    def test_all_flags_exist(self):
        assert ModifyFlag.DEFAULT is not None
        assert ModifyFlag.NO_FAIL is not None

    def test_flags_distinct(self):
        assert ModifyFlag.DEFAULT != ModifyFlag.NO_FAIL

    def test_equality(self):
        assert ModifyFlag.DEFAULT == ModifyFlag.DEFAULT
        assert ModifyFlag.NO_FAIL == ModifyFlag.NO_FAIL

    def test_hash(self):
        s = {ModifyFlag.DEFAULT, ModifyFlag.NO_FAIL}
        assert len(s) == 2

    def test_repr(self):
        for flag in (ModifyFlag.DEFAULT, ModifyFlag.NO_FAIL):
            assert repr(flag) != ""

    def test_or_combines_flags(self):
        combined = ModifyFlag.DEFAULT | ModifyFlag.NO_FAIL
        assert combined is not None


class TestCdtOperation:

    def test_select_by_path_returns_cdt_operation(self):
        ctx = [CTX.map_key("items")]
        op = CdtOperation.select_by_path("mybin", SelectFlag.VALUE, ctx)
        assert isinstance(op, CdtOperation)

    def test_select_by_path_empty_ctx(self):
        op = CdtOperation.select_by_path("mybin", SelectFlag.VALUE, [])
        assert isinstance(op, CdtOperation)

    def test_select_by_path_map_key_flag(self):
        ctx = [CTX.map_key("items"), CTX.all_children()]
        op = CdtOperation.select_by_path("mybin", SelectFlag.MAP_KEY, ctx)
        assert isinstance(op, CdtOperation)

    def test_select_by_path_no_fail_flag(self):
        ctx = [CTX.map_key("items"), CTX.all_children()]
        op = CdtOperation.select_by_path("mybin", SelectFlag.NO_FAIL, ctx)
        assert isinstance(op, CdtOperation)

    def test_modify_by_path_returns_cdt_operation(self):
        exp = fe.num_mul([fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(2)])
        ctx = [CTX.map_key("scores"), CTX.all_children()]
        op = CdtOperation.modify_by_path("mybin", ModifyFlag.DEFAULT, exp, ctx)
        assert isinstance(op, CdtOperation)

    def test_modify_by_path_empty_ctx(self):
        exp = fe.int_loop_var(LoopVarPart.VALUE)
        op = CdtOperation.modify_by_path("mybin", ModifyFlag.DEFAULT, exp, [])
        assert isinstance(op, CdtOperation)


class TestCTXAllChildren:

    def test_all_children_returns_ctx(self):
        ctx = CTX.all_children()
        assert isinstance(ctx, CTX)

    def test_all_children_with_filter_bool_true(self):
        ctx = CTX.all_children_with_filter(fe.bool_val(True))
        assert isinstance(ctx, CTX)

    def test_all_children_with_filter_comparison(self):
        ctx = CTX.all_children_with_filter(
            fe.gt(fe.int_loop_var(LoopVarPart.VALUE), fe.int_val(0))
        )
        assert isinstance(ctx, CTX)

    def test_all_children_with_filter_string_loop_var(self):
        ctx = CTX.all_children_with_filter(
            fe.eq(fe.string_loop_var(LoopVarPart.MAP_KEY), fe.string_val("title"))
        )
        assert isinstance(ctx, CTX)

    def test_all_children_distinct_from_map_key(self):
        a = CTX.all_children()
        b = CTX.map_key("key")
        assert a != b

    def test_all_children_filter_chain(self):
        ctxs = [
            CTX.map_key("store"),
            CTX.map_key("books"),
            CTX.all_children_with_filter(
                fe.lt(fe.float_loop_var(LoopVarPart.VALUE), fe.float_val(10.0))
            ),
        ]
        assert len(ctxs) == 3
        for c in ctxs:
            assert isinstance(c, CTX)


class TestLoopVarExpressions:

    def test_int_loop_var_value(self):
        exp = fe.int_loop_var(LoopVarPart.VALUE)
        assert exp is not None

    def test_int_loop_var_index(self):
        exp = fe.int_loop_var(LoopVarPart.INDEX)
        assert exp is not None

    def test_float_loop_var(self):
        exp = fe.float_loop_var(LoopVarPart.VALUE)
        assert exp is not None

    def test_string_loop_var_map_key(self):
        exp = fe.string_loop_var(LoopVarPart.MAP_KEY)
        assert exp is not None

    def test_bool_loop_var(self):
        exp = fe.bool_loop_var(LoopVarPart.VALUE)
        assert exp is not None

    def test_list_loop_var(self):
        exp = fe.list_loop_var(LoopVarPart.VALUE)
        assert exp is not None

    def test_map_loop_var(self):
        exp = fe.map_loop_var(LoopVarPart.VALUE)
        assert exp is not None

    def test_loop_var_in_comparison(self):
        exp = fe.lt(fe.int_loop_var(LoopVarPart.INDEX), fe.int_val(3))
        assert exp is not None

    def test_loop_var_in_ctx(self):
        ctx = CTX.all_children_with_filter(
            fe.eq(fe.string_loop_var(LoopVarPart.MAP_KEY), fe.string_val("name"))
        )
        assert isinstance(ctx, CTX)
