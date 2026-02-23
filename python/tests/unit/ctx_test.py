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
