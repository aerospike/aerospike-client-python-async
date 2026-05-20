# Copyright 2026 Aerospike, Inc.
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

"""Integration tests for the 8.1.2 enhanced expression API.

Covers a focused subset mirroring the reference CDT-expression suite:

- Native ExpOps: ``in_list`` / ``map_keys`` / ``map_values`` (server >= 8.1.2).
- Path-form expression operators: ``exp_select_by_path`` / ``exp_modify_by_path``
  (server >= 8.1.1).
- CTX helpers: ``CTX.all_children`` / ``CTX.all_children_with_filter`` /
  ``CTX.map_keys_in`` (the latter is a new server-8.1.2 helper).
- ``ExpReadFlags.DEFAULT`` and ``ExpWriteFlags.UPDATE_ONLY`` flag plumbing.

Server-8.1.2-only tests consume ``cdt_client_812``, which auto-routes to
``AEROSPIKE_HOST_8_1_2`` when the env var is set and skips cleanly
otherwise. 8.1.1+ path-form tests stay on ``cdt_client`` (the broad-surface
seed) so they continue to pass on a pre-8.1.2 cluster running 8.1.1+.
"""

import pytest
import pytest_asyncio

# Fixtures here are session-loop-scoped (clients live longer than one test);
# tests must run on the same session loop or the per-Client owning-loop guard
# in PAC's completion bridge fires.
pytestmark = pytest.mark.asyncio(loop_scope="session")

from aerospike_async import (
    CTX,
    ClientPolicy,
    ExpOperation,
    ExpReadFlags,
    ExpType,
    ExpWriteFlags,
    FilterExpression as Exp,
    Key,
    LoopVarPart,
    MapReturnType,
    ModifyFlags,
    SelectFlags,
    WritePolicy,
    new_client,
)


_NAMESPACE = "test"
_SET = "tcdtexp"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cdt_client(aerospike_host, use_services_alternate):
    """Module-scoped client; tests own their own keys for isolation.

    Connects to ``AEROSPIKE_HOST`` (the broad-surface seed). Tests that
    require server-8.1.2-only features should consume ``cdt_client_812``
    instead so they auto-route to the 8.1.2+ cluster when one is
    available and skip cleanly when one is not.
    """
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)
    yield client
    await client.close()


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def cdt_client_812(aerospike_host_812_required, use_services_alternate):
    """Function-scoped client connected to the 8.1.2+ seed.

    Used by tests that exercise server-8.1.2-only features. The dependent
    fixture ``aerospike_host_812_required`` skips the test cleanly when
    ``AEROSPIKE_HOST_8_1_2`` is unset, so individual tests can drop their
    inline ``pytest.skip(...)`` boilerplate.
    """
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host_812_required)
    yield client
    await client.close()


def _safe_delete(client, key):
    """Best-effort delete; swallow not-found / permission errors."""
    async def _go():
        try:
            await client.delete(key, policy=WritePolicy())
        except Exception:
            pass
    return _go()


# =====================================================================
# Native ExpOps (server >= 8.1.2)
# =====================================================================


class TestInListExpOp:

    async def test_in_list_positive(self, cdt_client_812):
        """``in_list("blue", ["red","blue","green"])`` returns true."""
        key = Key(_NAMESPACE, _SET, "in_list_pos")
        await _safe_delete(cdt_client_812, key)
        await cdt_client_812.put(key, {"color": "blue"}, policy=WritePolicy())

        exp = Exp.in_list(
            Exp.string_bin("color"),
            Exp.list_val(["red", "blue", "green"]),
        )
        rec = await cdt_client_812.operate(
            key,
            [ExpOperation.read("inList", exp, ExpReadFlags.DEFAULT)],
            policy=WritePolicy(),
        )
        assert rec.bins["inList"] is True

    async def test_in_list_negative(self, cdt_client_812):
        """``in_list("blue", ["red","yellow","green"])`` returns false."""
        key = Key(_NAMESPACE, _SET, "in_list_neg")
        await _safe_delete(cdt_client_812, key)
        await cdt_client_812.put(key, {"color": "blue"}, policy=WritePolicy())

        exp = Exp.in_list(
            Exp.string_bin("color"),
            Exp.list_val(["red", "yellow", "green"]),
        )
        rec = await cdt_client_812.operate(
            key,
            [ExpOperation.read("notInList", exp, ExpReadFlags.DEFAULT)],
            policy=WritePolicy(),
        )
        assert rec.bins["notInList"] is False


class TestMapKeysExpOp:

    async def test_map_keys_returns_all_keys(self, cdt_client_812):
        """``map_keys`` projects every key of a map bin into a list."""
        key = Key(_NAMESPACE, _SET, "map_keys")
        await _safe_delete(cdt_client_812, key)
        await cdt_client_812.put(key, {"myMap": {"x": 1, "y": 2, "z": 3}}, policy=WritePolicy())

        exp = Exp.map_keys(Exp.map_bin("myMap"))
        rec = await cdt_client_812.operate(
            key,
            [ExpOperation.read("keys", exp, ExpReadFlags.DEFAULT)],
            policy=WritePolicy(),
        )
        keys = rec.bins["keys"]
        assert isinstance(keys, list)
        assert sorted(keys) == ["x", "y", "z"]


class TestMapValuesExpOp:

    async def test_map_values_returns_all_values(self, cdt_client_812):
        """``map_values`` projects every value of a map bin into a list."""
        key = Key(_NAMESPACE, _SET, "map_values")
        await _safe_delete(cdt_client_812, key)
        await cdt_client_812.put(key, {"myMap": {"a": 100, "b": 200, "c": 300}}, policy=WritePolicy())

        exp = Exp.map_values(Exp.map_bin("myMap"))
        rec = await cdt_client_812.operate(
            key,
            [ExpOperation.read("values", exp, ExpReadFlags.DEFAULT)],
            policy=WritePolicy(),
        )
        vals = rec.bins["values"]
        assert isinstance(vals, list)
        assert sorted(vals) == [100, 200, 300]


# =====================================================================
# Path-form expression operators (server >= 8.1.1)
# =====================================================================


_BOOK_DATA = {
    "book": [
        {"title": "Sayings of the Century", "price": 10.45},
        {"title": "Sword of Honour", "price": 20.99},
        {"title": "Moby Dick", "price": 5.01},
        {"title": "The Lord of the Rings", "price": 30.98},
    ],
}


class TestPathFormExpressions:

    async def test_select_by_path_pulls_all_prices(
        self, cdt_client, supports_cdt_path_expressions
    ):
        """``exp_select_by_path`` flattens ``$.book[*].price`` into a list."""
        if not supports_cdt_path_expressions:
            pytest.skip("Path-form expression operators require server >= 8.1.1")

        key = Key(_NAMESPACE, _SET, "path_select")
        await _safe_delete(cdt_client, key)
        await cdt_client.put(key, {"res1": _BOOK_DATA}, policy=WritePolicy())

        path = [
            CTX.map_key("book"),
            CTX.all_children(),
            CTX.map_key("price"),
        ]
        exp = Exp.exp_select_by_path(
            ExpType.LIST,
            SelectFlags.VALUE,
            Exp.map_bin("res1"),
            path,
        )
        rec = await cdt_client.operate(
            key,
            [ExpOperation.read("prices", exp, ExpReadFlags.DEFAULT)],
            policy=WritePolicy(),
        )
        prices = rec.bins["prices"]
        assert isinstance(prices, list)
        assert len(prices) == 4
        assert all(isinstance(p, (int, float)) for p in prices)
        # Every input price is below 31.
        assert max(prices) < 31

    async def test_modify_by_path_multiplies_prices(self, cdt_client_812):
        """``exp_modify_by_path`` multiplies every ``price`` by 1.5.

        Uses a float loop variable, which stabilized on server >= 8.1.2.
        """
        key = Key(_NAMESPACE, _SET, "path_modify")
        await _safe_delete(cdt_client_812, key)
        await cdt_client_812.put(key, {"res1": _BOOK_DATA}, policy=WritePolicy())

        path = [
            CTX.map_key("book"),
            CTX.all_children(),
            CTX.map_key("price"),
        ]
        modify_exp = Exp.num_mul([
            Exp.float_loop_var(LoopVarPart.VALUE),
            Exp.float_val(1.5),
        ])
        apply_exp = Exp.exp_modify_by_path(
            ExpType.MAP,
            ModifyFlags.DEFAULT,
            Exp.map_bin("res1"),
            modify_exp,
            path,
        )

        wp = WritePolicy()
        await cdt_client_812.operate(
            key,
            [ExpOperation.write("res1", apply_exp, ExpWriteFlags.UPDATE_ONLY)],
            policy=wp,
        )

        from aerospike_async import ReadPolicy
        rec = await cdt_client_812.get(key, policy=ReadPolicy())
        root = rec.bins["res1"]
        prices = [b["price"] for b in root["book"]]
        # First book was 10.45, now ~15.675; the actual delta is what matters.
        assert prices[0] == pytest.approx(10.45 * 1.5, rel=1e-3)
        assert all(p > 0 for p in prices)


class TestAllChildrenWithFilter:

    async def test_all_children_with_filter_selects_cheap_titles(self, cdt_client_812):
        """``CTX.all_children_with_filter`` keeps only matching subtrees."""
        key = Key(_NAMESPACE, _SET, "filter_titles")
        await _safe_delete(cdt_client_812, key)
        await cdt_client_812.put(
            key,
            {
            "res1": {
                "book": [
                    {"title": "Cheap Book", "price": 5.99},
                    {"title": "Medium Book", "price": 15.50},
                    {"title": "Expensive Book", "price": 25.99},
                ],
            },
        },
            policy=WritePolicy(),
        )

        # Filter by price <= 10 over each book entry, then pick the title key
        # within the remaining subtree.
        price_lookup = Exp.map_get_by_key(
            MapReturnType.VALUE,
            ExpType.FLOAT,
            Exp.string_val("price"),
            Exp.map_loop_var(LoopVarPart.VALUE),
            [],
        )
        ctx = [
            CTX.map_key("book"),
            CTX.all_children_with_filter(Exp.le(price_lookup, Exp.float_val(10.0))),
            CTX.all_children_with_filter(
                Exp.eq(Exp.string_loop_var(LoopVarPart.MAP_KEY), Exp.string_val("title"))
            ),
        ]
        exp = Exp.exp_select_by_path(
            ExpType.LIST,
            SelectFlags.VALUE,
            Exp.map_bin("res1"),
            ctx,
        )
        rec = await cdt_client_812.operate(
            key,
            [ExpOperation.read("titles", exp, ExpReadFlags.DEFAULT)],
            policy=WritePolicy(),
        )
        titles = rec.bins["titles"]
        assert titles == ["Cheap Book"]


# =====================================================================
# CTX.map_keys_in — new 8.1.2 helper exposing the ``map_keys_in`` ctx op.
# =====================================================================


class TestCtxMapKeysIn:

    async def test_map_keys_in_select_subset(self, cdt_client_812):
        """``CTX.map_keys_in([key1, key2])`` selects only those keys' subtrees."""
        key = Key(_NAMESPACE, _SET, "map_keys_in")
        await _safe_delete(cdt_client_812, key)
        await cdt_client_812.put(
            key,
            {
            "res1": {"x": 100, "y": 200, "z": 300, "w": 400},
        },
            policy=WritePolicy(),
        )

        path = [CTX.map_keys_in(["x", "z"])]
        exp = Exp.exp_select_by_path(
            ExpType.LIST,
            SelectFlags.VALUE,
            Exp.map_bin("res1"),
            path,
        )
        rec = await cdt_client_812.operate(
            key,
            [ExpOperation.read("subset", exp, ExpReadFlags.DEFAULT)],
            policy=WritePolicy(),
        )
        subset = rec.bins["subset"]
        assert isinstance(subset, list)
        assert sorted(subset) == [100, 300]
