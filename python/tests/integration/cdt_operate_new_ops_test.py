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

"""Integration tests for the new ``CdtOperation`` path-form convenience builders.

Mirrors a focused subset of the reference CDT operate suite, exercising:
- ``CdtOperation.select_by_path`` (low-level, with explicit ``SelectFlags``)
- ``CdtOperation.modify_by_path`` (low-level, with explicit ``ModifyFlags``)
- ``CdtOperation.select_values`` / ``select_map_keys`` / ``select_map_entries``
  / ``select_matching_tree`` (convenience builders mapping to ``SelectFlags``)
- ``CdtOperation.modify`` / ``modify_no_fail`` (convenience builders mapping to
  ``ModifyFlags``)
- ``CdtOperation.remove`` (convenience for ``modify_by_path`` +
  ``remove_result()``)

All require server >= 8.1.1 (path expressions); a couple use loop-variable
filters that are stable from 8.1.2.
"""

import pytest
import pytest_asyncio

# Fixtures here are session-loop-scoped (clients live longer than one test);
# tests must run on the same session loop or the per-Client owning-loop guard
# in PAC's completion bridge fires.
pytestmark = pytest.mark.asyncio(loop_scope="session")

from aerospike_async import (
    CTX,
    CdtOperation,
    ClientPolicy,
    ExpType,
    FilterExpression as Exp,
    Key,
    LoopVarPart,
    MapReturnType,
    ModifyFlags,
    ReadPolicy,
    SelectFlags,
    WritePolicy,
    new_client,
)


_NAMESPACE = "test"
_SET = "tcdtop"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cdt_op_client(aerospike_host, use_services_alternate):
    """Module-scoped client connected to the broad-surface seed.

    Tests that exercise server-8.1.2-only features should consume
    ``cdt_op_client_812`` instead so they auto-route to the 8.1.2+
    cluster when one is available and skip cleanly when one is not.
    """
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)
    yield client
    await client.close()


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def cdt_op_client_812(aerospike_host_812_required, use_services_alternate):
    """Function-scoped client connected to the 8.1.2+ seed.

    The dependent ``aerospike_host_812_required`` fixture skips the test
    cleanly when ``AEROSPIKE_HOST_8_1_2`` is unset, so individual tests
    can drop their inline ``pytest.skip(...)`` boilerplate.
    """
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host_812_required)
    yield client
    await client.close()


_BOOK_DATA = {
    "book": [
        {"title": "Sayings of the Century", "price": 8.95},
        {"title": "Sword of Honour", "price": 12.99},
        {"title": "Moby Dick", "price": 8.99},
        {"title": "The Lord of the Rings", "price": 22.99},
    ],
}


async def _put_books(client, key):
    """Reset a key and load the standard 4-book fixture."""
    try:
        await client.delete(key, policy=WritePolicy())
    except Exception:
        pass
    await client.put(key, {"data": _BOOK_DATA}, policy=WritePolicy())


async def _get_data(client, key):
    rec = await client.get(key, policy=ReadPolicy())
    return rec.bins["data"]


# =====================================================================
# Low-level builders: select_by_path / modify_by_path
# =====================================================================


class TestSelectByPath:

    async def test_select_titles_under_filter(self, cdt_op_client_812):
        """``select_by_path`` with ``all_children_with_filter`` returns matches.

        Picks book titles whose price is <= 10.0; the dataset has two such
        books (8.95 and 8.99). Loop-variable filters stabilized on server
        >= 8.1.2.
        """
        key = Key(_NAMESPACE, _SET, "select_by_path")
        await _put_books(cdt_op_client_812, key)

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
        op = CdtOperation.select_by_path("data", SelectFlags.VALUE, ctx)
        result = await cdt_op_client_812.operate(key, [op], policy=WritePolicy())

        titles = result.bins.get("data")
        assert isinstance(titles, list)
        assert sorted(titles) == ["Moby Dick", "Sayings of the Century"]


class TestModifyByPath:

    async def test_modify_multiplies_prices(self, cdt_op_client_812):
        """``modify_by_path`` multiplies each ``price`` by 1.10.

        Loop-variable modify stabilized on server >= 8.1.2.
        """
        key = Key(_NAMESPACE, _SET, "modify_by_path")
        await _put_books(cdt_op_client_812, key)

        ctx = [
            CTX.map_key("book"),
            CTX.all_children(),
            CTX.map_key("price"),
        ]
        modify_exp = Exp.num_mul([
            Exp.float_loop_var(LoopVarPart.VALUE),
            Exp.float_val(1.10),
        ])
        op = CdtOperation.modify_by_path("data", ModifyFlags.DEFAULT, modify_exp, ctx)
        await cdt_op_client_812.operate(key, [op], policy=WritePolicy())

        data = await _get_data(cdt_op_client_812, key)
        prices = [b["price"] for b in data["book"]]
        assert prices[0] == pytest.approx(8.95 * 1.10, rel=1e-3)
        assert prices[3] == pytest.approx(22.99 * 1.10, rel=1e-3)


# =====================================================================
# Convenience select builders
# =====================================================================


class TestSelectConvenience:

    async def test_select_values_pulls_value_list(self, cdt_op_client_812):
        """``select_values`` is a shorthand for ``select_by_path`` w/ VALUE."""
        key = Key(_NAMESPACE, _SET, "select_values")
        await _put_books(cdt_op_client_812, key)

        ctx = [CTX.map_key("book"), CTX.all_children(), CTX.map_key("price")]
        result = await cdt_op_client_812.operate(
            key,
            [CdtOperation.select_values("data", ctx)],
            policy=WritePolicy(),
        )
        prices = result.bins.get("data")
        assert isinstance(prices, list)
        assert len(prices) == 4
        assert min(prices) == pytest.approx(8.95)
        assert max(prices) == pytest.approx(22.99)

    async def test_select_map_keys_pulls_keys(self, cdt_op_client_812):
        """``select_map_keys`` shorthand for ``select_by_path`` w/ MAP_KEY.

        Walk into a map of int values and filter for ``value > 75``, then
        emit the *keys* of the surviving entries.
        """
        key = Key(_NAMESPACE, _SET, "select_map_keys")
        try:
            await cdt_op_client_812.delete(key, policy=WritePolicy())
        except Exception:
            pass
        await cdt_op_client_812.put(
            key,
            {"data": {"items": {"item1": 100, "item2": 200, "item3": 50}}},
            policy=WritePolicy(),
        )

        # int_loop_var(VALUE) > 75 keeps item1 (100) and item2 (200), drops item3 (50).
        ctx = [
            CTX.map_key("items"),
            CTX.all_children_with_filter(
                Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.int_val(75))
            ),
        ]
        result = await cdt_op_client_812.operate(
            key,
            [CdtOperation.select_map_keys("data", ctx)],
            policy=WritePolicy(),
        )
        keys = result.bins.get("data")
        assert isinstance(keys, list)
        assert sorted(keys) == ["item1", "item2"]

    async def test_select_matching_tree_preserves_shape(self, cdt_op_client_812):
        """``select_matching_tree`` returns the original tree, matched-only."""
        key = Key(_NAMESPACE, _SET, "select_matching_tree")
        await _put_books(cdt_op_client_812, key)

        # Match books whose price is <= 10. The matching-tree result keeps
        # the surrounding map shape, so we get back ``{"book": [{...}, {...}]}``.
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
        ]
        result = await cdt_op_client_812.operate(
            key,
            [CdtOperation.select_matching_tree("data", ctx)],
            policy=WritePolicy(),
        )
        tree = result.bins.get("data")
        # Exact shape varies a bit by server, but the result must include the
        # two cheap books and exclude the expensive ones.
        assert isinstance(tree, dict)
        cheap = tree.get("book") or []
        prices = [b["price"] for b in cheap]
        assert sorted(prices) == [8.95, 8.99]


# =====================================================================
# Modify convenience builders
# =====================================================================


class TestModifyConvenience:

    async def test_modify_no_fail_skips_type_mismatch(self, cdt_op_client_812):
        """``modify_no_fail`` tolerates a type-mismatched leaf without aborting.

        Without ``ModifyFlags.NO_FAIL`` this would throw because the title
        leaves are strings, not numbers.
        """
        key = Key(_NAMESPACE, _SET, "modify_no_fail")
        await _put_books(cdt_op_client_812, key)

        # Try to multiply *every* leaf under ``book[*]`` (titles + prices) by 2.
        # Titles are strings — the per-leaf modify would fail without NO_FAIL.
        ctx = [CTX.map_key("book"), CTX.all_children(), CTX.all_children()]
        modify_exp = Exp.num_mul([
            Exp.float_loop_var(LoopVarPart.VALUE),
            Exp.float_val(2.0),
        ])
        op = CdtOperation.modify_no_fail("data", modify_exp, ctx)
        await cdt_op_client_812.operate(key, [op], policy=WritePolicy())

        data = await _get_data(cdt_op_client_812, key)
        # Prices were doubled.
        assert data["book"][0]["price"] == pytest.approx(8.95 * 2.0, rel=1e-3)
        # Titles were left untouched (the type mismatch was silently skipped).
        assert data["book"][0]["title"] == "Sayings of the Century"


# =====================================================================
# Remove convenience builder
# =====================================================================


class TestRemoveByPath:

    async def test_remove_strips_prices(self, cdt_op_client_812):
        """``CdtOperation.remove`` deletes the resolved leaves in place."""
        key = Key(_NAMESPACE, _SET, "remove")
        await _put_books(cdt_op_client_812, key)

        ctx = [CTX.map_key("book"), CTX.all_children(), CTX.map_key("price")]
        op = CdtOperation.remove("data", ctx)
        await cdt_op_client_812.operate(key, [op], policy=WritePolicy())

        data = await _get_data(cdt_op_client_812, key)
        # Every book should now have only ``title`` (no ``price``).
        for book in data["book"]:
            assert "title" in book
            assert "price" not in book
