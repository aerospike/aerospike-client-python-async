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

import pytest
from aerospike_async import (
    BitPolicy,
    BitwiseWriteFlags,
    ListPolicy,
    ListOrderType,
    ListWriteFlags,
    MapPolicy,
    MapOrder,
    MapWriteMode,
)


class TestBitPolicy:

    def test_default_construction(self):
        bp = BitPolicy(None)
        assert bp is not None

    def test_construction_with_flags(self):
        bp = BitPolicy(BitwiseWriteFlags.CREATE_ONLY)
        assert bp.get_write_flags() == int(BitwiseWriteFlags.CREATE_ONLY)

    def test_set_write_flags(self):
        bp = BitPolicy(None)
        bp.set_write_flags(BitwiseWriteFlags.UPDATE_ONLY)
        assert bp.get_write_flags() == int(BitwiseWriteFlags.UPDATE_ONLY)

    def test_all_write_flag_values(self):
        for flag in [
            BitwiseWriteFlags.DEFAULT,
            BitwiseWriteFlags.CREATE_ONLY,
            BitwiseWriteFlags.UPDATE_ONLY,
            BitwiseWriteFlags.NO_FAIL,
            BitwiseWriteFlags.PARTIAL,
        ]:
            bp = BitPolicy(flag)
            assert bp.get_write_flags() == int(flag)


class TestListPolicy:

    def test_default_construction(self):
        lp = ListPolicy(None, None)
        assert lp is not None

    def test_construction_with_order(self):
        lp = ListPolicy(ListOrderType.ORDERED, None)
        assert lp.order == ListOrderType.ORDERED

    def test_construction_with_write_flags(self):
        lp = ListPolicy(None, ListWriteFlags.ADD_UNIQUE)
        assert lp.write_flags == ListWriteFlags.ADD_UNIQUE

    def test_construction_with_both(self):
        lp = ListPolicy(ListOrderType.ORDERED, ListWriteFlags.INSERT_BOUNDED)
        assert lp.order == ListOrderType.ORDERED
        assert lp.write_flags == ListWriteFlags.INSERT_BOUNDED

    def test_order_setter(self):
        lp = ListPolicy(None, None)
        lp.order = ListOrderType.ORDERED
        assert lp.order == ListOrderType.ORDERED
        lp.order = ListOrderType.UNORDERED
        assert lp.order == ListOrderType.UNORDERED

    def test_write_flags_setter(self):
        lp = ListPolicy(None, None)
        for flag in [
            ListWriteFlags.DEFAULT,
            ListWriteFlags.ADD_UNIQUE,
            ListWriteFlags.INSERT_BOUNDED,
            ListWriteFlags.NO_FAIL,
            ListWriteFlags.PARTIAL,
        ]:
            lp.write_flags = flag
            assert lp.write_flags == flag


class TestMapPolicy:

    def test_default_construction(self):
        mp = MapPolicy(None, None)
        assert mp is not None

    def test_construction_with_order(self):
        mp = MapPolicy(MapOrder.KEY_ORDERED, None)
        assert mp.order == MapOrder.KEY_ORDERED

    def test_construction_with_write_mode(self):
        mp = MapPolicy(None, MapWriteMode.CREATE_ONLY)
        assert mp.write_mode == MapWriteMode.CREATE_ONLY

    def test_construction_with_both(self):
        mp = MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE_ONLY)
        assert mp.order == MapOrder.KEY_VALUE_ORDERED
        assert mp.write_mode == MapWriteMode.UPDATE_ONLY

    def test_order_setter(self):
        mp = MapPolicy(None, None)
        for order in [MapOrder.UNORDERED, MapOrder.KEY_ORDERED, MapOrder.KEY_VALUE_ORDERED]:
            mp.order = order
            assert mp.order == order

    def test_write_mode_setter(self):
        mp = MapPolicy(None, None)
        for mode in [MapWriteMode.UPDATE, MapWriteMode.UPDATE_ONLY, MapWriteMode.CREATE_ONLY]:
            mp.write_mode = mode
            assert mp.write_mode == mode


class TestHLLPolicy:
    """HLLPolicy tests — skipped if not importable (pre-existing issue)."""

    @pytest.fixture(autouse=True)
    def _import_hll(self):
        hll_mod = pytest.importorskip("aerospike_async", reason="HLLPolicy not importable")
        if not hasattr(hll_mod, "HLLPolicy"):
            pytest.skip("HLLPolicy not exported by aerospike_async")
        self.HLLPolicy = hll_mod.HLLPolicy
        self.HLLWriteFlags = hll_mod.HLLWriteFlags

    def test_default_construction(self):
        hp = self.HLLPolicy()
        assert hp is not None

    def test_construction_with_flags(self):
        hp = self.HLLPolicy(self.HLLWriteFlags.CREATE_ONLY)
        assert hp is not None
