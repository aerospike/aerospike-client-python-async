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

"""Unit tests for enums not covered by other test files."""

from aerospike_async import (
    BitwiseOverflowActions,
    BitwiseResizeFlags,
    BitwiseWriteFlags,
    HLLWriteFlags,
    IndexType,
    ListOrderType,
    ListSortFlags,
    ListWriteFlags,
    MapOrder,
    MapWriteMode,
    TaskStatus,
    UDFLang,
)


class TestBitwiseOverflowActions:

    def test_variants_exist(self):
        assert BitwiseOverflowActions.FAIL is not None
        assert BitwiseOverflowActions.SATURATE is not None
        assert BitwiseOverflowActions.WRAP is not None

    def test_variants_distinct(self):
        assert BitwiseOverflowActions.FAIL != BitwiseOverflowActions.SATURATE
        assert BitwiseOverflowActions.SATURATE != BitwiseOverflowActions.WRAP
        assert BitwiseOverflowActions.FAIL != BitwiseOverflowActions.WRAP


class TestBitwiseResizeFlags:

    def test_variants_exist(self):
        assert BitwiseResizeFlags.DEFAULT is not None
        assert BitwiseResizeFlags.FROM_FRONT is not None
        assert BitwiseResizeFlags.GROW_ONLY is not None
        assert BitwiseResizeFlags.SHRINK_ONLY is not None

    def test_variants_distinct(self):
        flags = [
            BitwiseResizeFlags.DEFAULT,
            BitwiseResizeFlags.FROM_FRONT,
            BitwiseResizeFlags.GROW_ONLY,
            BitwiseResizeFlags.SHRINK_ONLY,
        ]
        for i, a in enumerate(flags):
            for b in flags[i + 1:]:
                assert a != b


class TestBitwiseWriteFlags:

    def test_variants_exist(self):
        assert BitwiseWriteFlags.DEFAULT is not None
        assert BitwiseWriteFlags.CREATE_ONLY is not None
        assert BitwiseWriteFlags.UPDATE_ONLY is not None
        assert BitwiseWriteFlags.NO_FAIL is not None
        assert BitwiseWriteFlags.PARTIAL is not None

    def test_variants_distinct(self):
        flags = [
            BitwiseWriteFlags.DEFAULT,
            BitwiseWriteFlags.CREATE_ONLY,
            BitwiseWriteFlags.UPDATE_ONLY,
            BitwiseWriteFlags.NO_FAIL,
            BitwiseWriteFlags.PARTIAL,
        ]
        for i, a in enumerate(flags):
            for b in flags[i + 1:]:
                assert a != b


class TestHLLWriteFlags:

    def test_variants_exist(self):
        assert HLLWriteFlags.DEFAULT is not None
        assert HLLWriteFlags.CREATE_ONLY is not None
        assert HLLWriteFlags.UPDATE_ONLY is not None
        assert HLLWriteFlags.NO_FAIL is not None
        assert HLLWriteFlags.ALLOW_FOLD is not None

    def test_variants_distinct(self):
        flags = [
            HLLWriteFlags.DEFAULT,
            HLLWriteFlags.CREATE_ONLY,
            HLLWriteFlags.UPDATE_ONLY,
            HLLWriteFlags.NO_FAIL,
            HLLWriteFlags.ALLOW_FOLD,
        ]
        for i, a in enumerate(flags):
            for b in flags[i + 1:]:
                assert a != b


class TestIndexType:

    def test_variants_exist(self):
        assert IndexType.NUMERIC is not None
        assert IndexType.STRING is not None
        assert IndexType.GEO2D_SPHERE is not None

    def test_variants_distinct(self):
        assert IndexType.NUMERIC != IndexType.STRING
        assert IndexType.STRING != IndexType.GEO2D_SPHERE
        assert IndexType.NUMERIC != IndexType.GEO2D_SPHERE


class TestListOrderType:

    def test_variants_exist(self):
        assert ListOrderType.UNORDERED is not None
        assert ListOrderType.ORDERED is not None

    def test_variants_distinct(self):
        assert ListOrderType.UNORDERED != ListOrderType.ORDERED


class TestListSortFlags:

    def test_variants_exist(self):
        assert ListSortFlags.DEFAULT is not None
        assert ListSortFlags.DROP_DUPLICATES is not None

    def test_variants_distinct(self):
        assert ListSortFlags.DEFAULT != ListSortFlags.DROP_DUPLICATES


class TestListWriteFlags:

    def test_variants_exist(self):
        assert ListWriteFlags.DEFAULT is not None
        assert ListWriteFlags.ADD_UNIQUE is not None
        assert ListWriteFlags.INSERT_BOUNDED is not None
        assert ListWriteFlags.NO_FAIL is not None
        assert ListWriteFlags.PARTIAL is not None

    def test_variants_distinct(self):
        flags = [
            ListWriteFlags.DEFAULT,
            ListWriteFlags.ADD_UNIQUE,
            ListWriteFlags.INSERT_BOUNDED,
            ListWriteFlags.NO_FAIL,
            ListWriteFlags.PARTIAL,
        ]
        for i, a in enumerate(flags):
            for b in flags[i + 1:]:
                assert a != b


class TestMapOrder:

    def test_variants_exist(self):
        assert MapOrder.UNORDERED is not None
        assert MapOrder.KEY_ORDERED is not None
        assert MapOrder.KEY_VALUE_ORDERED is not None

    def test_variants_distinct(self):
        assert MapOrder.UNORDERED != MapOrder.KEY_ORDERED
        assert MapOrder.KEY_ORDERED != MapOrder.KEY_VALUE_ORDERED
        assert MapOrder.UNORDERED != MapOrder.KEY_VALUE_ORDERED


class TestMapWriteMode:

    def test_variants_exist(self):
        assert MapWriteMode.UPDATE is not None
        assert MapWriteMode.UPDATE_ONLY is not None
        assert MapWriteMode.CREATE_ONLY is not None

    def test_variants_distinct(self):
        assert MapWriteMode.UPDATE != MapWriteMode.UPDATE_ONLY
        assert MapWriteMode.UPDATE_ONLY != MapWriteMode.CREATE_ONLY
        assert MapWriteMode.UPDATE != MapWriteMode.CREATE_ONLY


class TestTaskStatus:

    def test_variants_exist(self):
        assert TaskStatus.NOT_FOUND is not None
        assert TaskStatus.IN_PROGRESS is not None
        assert TaskStatus.COMPLETE is not None

    def test_variants_distinct(self):
        assert TaskStatus.NOT_FOUND != TaskStatus.IN_PROGRESS
        assert TaskStatus.IN_PROGRESS != TaskStatus.COMPLETE
        assert TaskStatus.NOT_FOUND != TaskStatus.COMPLETE


class TestUDFLang:

    def test_lua_exists(self):
        assert UDFLang.LUA is not None
