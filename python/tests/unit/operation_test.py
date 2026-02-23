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
    Operation,
    ListOperation,
    MapOperation,
    BitOperation,
    HllOperation,
    ListPolicy,
    ListOrderType,
    ListReturnType,
    ListSortFlags,
    MapPolicy,
    MapOrder,
    MapWriteMode,
    MapReturnType,
    BitPolicy,
    BitwiseOverflowActions,
    BitwiseResizeFlags,
    CTX,
)


class TestOperation:

    def test_get(self):
        op = Operation.get()
        assert op is not None
        assert isinstance(op, Operation)

    def test_get_bin(self):
        op = Operation.get_bin("mybin")
        assert isinstance(op, Operation)

    def test_put(self):
        op = Operation.put("mybin", 42)
        assert isinstance(op, Operation)

    def test_put_string(self):
        op = Operation.put("mybin", "hello")
        assert isinstance(op, Operation)

    def test_get_header(self):
        op = Operation.get_header()
        assert isinstance(op, Operation)

    def test_delete(self):
        op = Operation.delete()
        assert isinstance(op, Operation)

    def test_touch(self):
        op = Operation.touch()
        assert isinstance(op, Operation)

    def test_add(self):
        op = Operation.add("counter", 1)
        assert isinstance(op, Operation)

    def test_append(self):
        op = Operation.append("strbin", "suffix")
        assert isinstance(op, Operation)

    def test_prepend(self):
        op = Operation.prepend("strbin", "prefix")
        assert isinstance(op, Operation)



class TestListOperation:

    def test_get(self):
        op = ListOperation.get("listbin", 0)
        assert isinstance(op, ListOperation)

    def test_size(self):
        op = ListOperation.size("listbin")
        assert isinstance(op, ListOperation)

    def test_pop(self):
        op = ListOperation.pop("listbin", 0)
        assert isinstance(op, ListOperation)

    def test_clear(self):
        op = ListOperation.clear("listbin")
        assert isinstance(op, ListOperation)

    def test_get_range(self):
        op = ListOperation.get_range("listbin", 0, 5)
        assert isinstance(op, ListOperation)

    def test_set(self):
        op = ListOperation.set("listbin", 0, "value")
        assert isinstance(op, ListOperation)

    def test_remove(self):
        op = ListOperation.remove("listbin", 0)
        assert isinstance(op, ListOperation)

    def test_remove_range(self):
        op = ListOperation.remove_range("listbin", 0, 3)
        assert isinstance(op, ListOperation)

    def test_append(self):
        lp = ListPolicy(None, None)
        op = ListOperation.append("listbin", "item", lp)
        assert isinstance(op, ListOperation)

    def test_append_items(self):
        lp = ListPolicy(None, None)
        op = ListOperation.append_items("listbin", [1, 2, 3], lp)
        assert isinstance(op, ListOperation)

    def test_insert(self):
        lp = ListPolicy(None, None)
        op = ListOperation.insert("listbin", 0, "item", lp)
        assert isinstance(op, ListOperation)

    def test_insert_items(self):
        lp = ListPolicy(None, None)
        op = ListOperation.insert_items("listbin", 0, [1, 2], lp)
        assert isinstance(op, ListOperation)

    def test_increment(self):
        lp = ListPolicy(None, None)
        op = ListOperation.increment("listbin", 0, 10, lp)
        assert isinstance(op, ListOperation)

    def test_sort(self):
        op = ListOperation.sort("listbin", ListSortFlags.DEFAULT)
        assert isinstance(op, ListOperation)

    def test_set_order(self):
        op = ListOperation.set_order("listbin", ListOrderType.ORDERED)
        assert isinstance(op, ListOperation)

    def test_get_by_index(self):
        op = ListOperation.get_by_index("listbin", 0, ListReturnType.VALUE)
        assert isinstance(op, ListOperation)

    def test_get_by_rank(self):
        op = ListOperation.get_by_rank("listbin", 0, ListReturnType.VALUE)
        assert isinstance(op, ListOperation)

    def test_get_by_value(self):
        op = ListOperation.get_by_value("listbin", 42, ListReturnType.COUNT)
        assert isinstance(op, ListOperation)

    def test_remove_by_index(self):
        op = ListOperation.remove_by_index("listbin", 0, ListReturnType.NONE)
        assert isinstance(op, ListOperation)

    def test_remove_by_value(self):
        op = ListOperation.remove_by_value("listbin", 42, ListReturnType.COUNT)
        assert isinstance(op, ListOperation)

    def test_create(self):
        op = ListOperation.create("listbin", ListOrderType.ORDERED, False, False)
        assert isinstance(op, ListOperation)

    def test_set_context(self):
        op = ListOperation.size("nested")
        result = op.set_context([CTX.list_index(0)])
        assert isinstance(result, ListOperation)


class TestMapOperation:

    def test_size(self):
        op = MapOperation.size("mapbin")
        assert isinstance(op, MapOperation)

    def test_clear(self):
        op = MapOperation.clear("mapbin")
        assert isinstance(op, MapOperation)

    def test_put(self):
        mp = MapPolicy(None, None)
        op = MapOperation.put("mapbin", "key", "value", mp)
        assert isinstance(op, MapOperation)

    def test_put_items(self):
        mp = MapPolicy(None, None)
        op = MapOperation.put_items("mapbin", [("k1", "v1"), ("k2", "v2")], mp)
        assert isinstance(op, MapOperation)

    def test_increment_value(self):
        mp = MapPolicy(None, None)
        op = MapOperation.increment_value("mapbin", "counter", 1, mp)
        assert isinstance(op, MapOperation)

    def test_get_by_key(self):
        op = MapOperation.get_by_key("mapbin", "key", MapReturnType.VALUE)
        assert isinstance(op, MapOperation)

    def test_remove_by_key(self):
        op = MapOperation.remove_by_key("mapbin", "key", MapReturnType.NONE)
        assert isinstance(op, MapOperation)

    def test_get_by_index(self):
        op = MapOperation.get_by_index("mapbin", 0, MapReturnType.KEY_VALUE)
        assert isinstance(op, MapOperation)

    def test_get_by_rank(self):
        op = MapOperation.get_by_rank("mapbin", 0, MapReturnType.VALUE)
        assert isinstance(op, MapOperation)

    def test_get_by_value(self):
        op = MapOperation.get_by_value("mapbin", "val", MapReturnType.KEY)
        assert isinstance(op, MapOperation)

    def test_set_map_policy(self):
        mp = MapPolicy(MapOrder.KEY_ORDERED, None)
        op = MapOperation.set_map_policy("mapbin", mp)
        assert isinstance(op, MapOperation)

    def test_create(self):
        op = MapOperation.create("mapbin", MapOrder.KEY_ORDERED)
        assert isinstance(op, MapOperation)

    def test_set_context(self):
        op = MapOperation.size("nested")
        result = op.set_context([CTX.map_key("inner")])
        assert isinstance(result, MapOperation)


class TestBitOperation:

    def test_resize(self):
        bp = BitPolicy(None)
        op = BitOperation.resize("bitbin", 10, BitwiseResizeFlags.DEFAULT, bp)
        assert isinstance(op, BitOperation)

    def test_insert(self):
        bp = BitPolicy(None)
        op = BitOperation.insert("bitbin", 0, b"\x00\x01", bp)
        assert isinstance(op, BitOperation)

    def test_remove(self):
        bp = BitPolicy(None)
        op = BitOperation.remove("bitbin", 0, 2, bp)
        assert isinstance(op, BitOperation)

    def test_set(self):
        bp = BitPolicy(None)
        op = BitOperation.set("bitbin", 0, 8, b"\xff", bp)
        assert isinstance(op, BitOperation)

    def test_keyword_methods(self):
        """Test methods named with Python reserved words via getattr."""
        bp = BitPolicy(None)
        for name in ("or", "xor", "and"):
            fn = getattr(BitOperation, name)
            op = fn("bitbin", 0, 8, b"\xff", bp)
            assert isinstance(op, BitOperation)

    def test_not(self):
        bp = BitPolicy(None)
        fn = getattr(BitOperation, "not")
        op = fn("bitbin", 0, 8, bp)
        assert isinstance(op, BitOperation)

    def test_lshift(self):
        bp = BitPolicy(None)
        op = BitOperation.lshift("bitbin", 0, 8, 2, bp)
        assert isinstance(op, BitOperation)

    def test_rshift(self):
        bp = BitPolicy(None)
        op = BitOperation.rshift("bitbin", 0, 8, 2, bp)
        assert isinstance(op, BitOperation)

    def test_add(self):
        bp = BitPolicy(None)
        op = BitOperation.add("bitbin", 0, 8, 1, False, BitwiseOverflowActions.FAIL, bp)
        assert isinstance(op, BitOperation)

    def test_subtract(self):
        bp = BitPolicy(None)
        op = BitOperation.subtract("bitbin", 0, 8, 1, False, BitwiseOverflowActions.WRAP, bp)
        assert isinstance(op, BitOperation)

    def test_get(self):
        op = BitOperation.get("bitbin", 0, 8)
        assert isinstance(op, BitOperation)

    def test_count(self):
        op = BitOperation.count("bitbin", 0, 16)
        assert isinstance(op, BitOperation)

    def test_lscan(self):
        op = BitOperation.lscan("bitbin", 0, 16, True)
        assert isinstance(op, BitOperation)

    def test_rscan(self):
        op = BitOperation.rscan("bitbin", 0, 16, False)
        assert isinstance(op, BitOperation)

    def test_get_int(self):
        op = BitOperation.get_int("bitbin", 0, 8, True)
        assert isinstance(op, BitOperation)

    def test_set_int(self):
        bp = BitPolicy(None)
        op = BitOperation.set_int("bitbin", 0, 8, 42, bp)
        assert isinstance(op, BitOperation)



class TestHllOperation:

    def test_init(self):
        op = HllOperation.init("hllbin", 8)
        assert isinstance(op, HllOperation)

    def test_init_with_min_hash(self):
        op = HllOperation.init("hllbin", 8, 0)
        assert isinstance(op, HllOperation)

    def test_add(self):
        op = HllOperation.add("hllbin", ["a", "b", "c"])
        assert isinstance(op, HllOperation)

    def test_add_with_index_bit_count(self):
        op = HllOperation.add("hllbin", ["a", "b"], 8)
        assert isinstance(op, HllOperation)

    def test_get_count(self):
        op = HllOperation.get_count("hllbin")
        assert isinstance(op, HllOperation)

    def test_describe(self):
        op = HllOperation.describe("hllbin")
        assert isinstance(op, HllOperation)

    def test_fold(self):
        op = HllOperation.fold("hllbin", 4)
        assert isinstance(op, HllOperation)

    def test_get_union(self):
        op = HllOperation.get_union("hllbin", [])
        assert isinstance(op, HllOperation)

    def test_get_union_count(self):
        op = HllOperation.get_union_count("hllbin", [])
        assert isinstance(op, HllOperation)

    def test_get_intersect_count(self):
        op = HllOperation.get_intersect_count("hllbin", [])
        assert isinstance(op, HllOperation)

    def test_get_similarity(self):
        op = HllOperation.get_similarity("hllbin", [])
        assert isinstance(op, HllOperation)
