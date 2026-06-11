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

"""Integration tests for string filter expressions (server 8.1.3+).

Scenarios mirror rust-core's ``tests/src/exp_string.rs`` plus the JSDK
reference at ``OperateStringTest.stringProjectionViaStringExpOnQuery``
(commit ``6bb348e``).

Tests opt in to an 8.1.3+ cluster via the ``aerospike_host_813_required``
fixture; they skip cleanly when ``AEROSPIKE_HOST_8_1_3`` is unset.

NOT exercised (per spec §4.2): the literal-source regex form
``string_regex_compare(Exp.val(pattern), Exp.val(literal))`` — the server
returns ``OP_NOT_APPLICABLE (26)`` because the expression engine doesn't
tag the literal as a STRING particle. Only bin-sourced regex tests are
included.
"""

import pytest
import pytest_asyncio

from aerospike_async import (
    ClientPolicy,
    ExpOperation,
    ExpReadFlags,
    ExpType,
    FilterExpression as Exp,
    Key,
    ListReturnType,
    MapReturnType,
    new_client,
    StringNumericType,
    StringRegexFlags,
    WritePolicy,
)


pytestmark = pytest.mark.asyncio(loop_scope="module")


_NAMESPACE = "test"
_SET = "tstrexp"


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def string_client_813(aerospike_host_813_required, use_services_alternate):
    """8.1.3+ client. See ``operate_string_test.py:string_client_813`` for
    the rationale on services-alternate handling + env-var-driven creds."""
    import asyncio as _asyncio
    import os as _os
    cp = ClientPolicy()
    sa_override = _os.environ.get("AEROSPIKE_HOST_8_1_3_USE_SERVICES_ALTERNATE")
    if sa_override is not None:
        cp.use_services_alternate = sa_override.lower() == "true"
    else:
        cp.use_services_alternate = use_services_alternate
    user = _os.environ.get("AEROSPIKE_HOST_8_1_3_USER")
    password = _os.environ.get("AEROSPIKE_HOST_8_1_3_PASSWORD")
    if user:
        cp.user = user
    if password:
        cp.password = password
    client = await new_client(cp, aerospike_host_813_required)
    await _asyncio.sleep(2)  # let first tend populate partition map
    yield client
    await client.close()


def _key(suffix: str) -> Key:
    return Key(_NAMESPACE, _SET, suffix)


async def _eval_exp(client, key, exp, *, bin_name="out"):
    """Build an ExpOperation.read of ``exp`` into ``bin_name`` and return the result."""
    rec = await client.operate(
        key,
        [ExpOperation.read(bin_name, exp, ExpReadFlags.DEFAULT)],
        policy=WritePolicy(),
    )
    assert rec is not None
    return rec.bins.get(bin_name)


# ---------------------------------------------------------------------------
# Read expressions
# ---------------------------------------------------------------------------


class TestStringReadExpressions:

    async def test_strlen_byte_length_blob(self, string_client_813):
        key = _key("read_basics")
        await string_client_813.put(key, {"s": "héllo"}, policy=WritePolicy())
        assert await _eval_exp(string_client_813, key, Exp.string_strlen(Exp.string_bin("s"))) == 5
        assert (
            await _eval_exp(string_client_813, key, Exp.string_byte_length(Exp.string_bin("s")))
        ) == 6
        assert (
            await _eval_exp(string_client_813, key, Exp.string_to_blob(Exp.string_bin("s")))
        ) == "héllo".encode("utf-8")

    async def test_substr_offset_form_and_range_form(self, string_client_813):
        key = _key("substr")
        await string_client_813.put(key, {"s": "hello world"}, policy=WritePolicy())
        # offset-to-end
        out = await _eval_exp(
            string_client_813, key, Exp.string_substr(Exp.int_val(6), Exp.string_bin("s"))
        )
        assert out == "world"
        # range form (end-exclusive — see operate_string_test:test_substr_range_form)
        out = await _eval_exp(
            string_client_813,
            key,
            Exp.string_substr_range(Exp.int_val(0), Exp.int_val(5), Exp.string_bin("s")),
        )
        assert out == "hello"

    async def test_char_at_and_find(self, string_client_813):
        key = _key("char_find")
        await string_client_813.put(key, {"s": "hello"}, policy=WritePolicy())
        assert (
            await _eval_exp(
                string_client_813, key, Exp.string_char_at(Exp.int_val(1), Exp.string_bin("s"))
            )
        ) == "e"
        # find first match
        assert (
            await _eval_exp(
                string_client_813, key, Exp.string_find(Exp.string_val("ll"), Exp.string_bin("s"))
            )
        ) == 2
        # find N-th match
        await string_client_813.put(key, {"s": "ab ab ab"}, policy=WritePolicy())
        assert (
            await _eval_exp(
                string_client_813,
                key,
                Exp.string_find_nth(Exp.string_val("ab"), Exp.int_val(2), Exp.string_bin("s")),
            )
        ) == 3

    async def test_contains_starts_ends_decode_as_bool(self, string_client_813):
        """Spec §2.7 reaffirms the bool decode for the seven predicate ops on the expression path."""
        key = _key("predicates")
        await string_client_813.put(key, {"s": "hello world"}, policy=WritePolicy())
        for exp_builder, expected in [
            (lambda: Exp.string_contains(Exp.string_val("world"), Exp.string_bin("s")), True),
            (lambda: Exp.string_starts_with(Exp.string_val("hello"), Exp.string_bin("s")), True),
            (lambda: Exp.string_ends_with(Exp.string_val("world"), Exp.string_bin("s")), True),
            (lambda: Exp.string_contains(Exp.string_val("ZZZ"), Exp.string_bin("s")), False),
        ]:
            out = await _eval_exp(string_client_813, key, exp_builder())
            assert out is expected
            assert isinstance(out, bool), f"expected bool, got {type(out).__name__}"

    async def test_is_numeric_default_and_int_only(self, string_client_813):
        key = _key("numeric")
        await string_client_813.put(key, {"d": "3.14", "i": "42"}, policy=WritePolicy())
        # ANY accepts float-formatted.
        assert (
            await _eval_exp(string_client_813, key, Exp.string_is_numeric(Exp.string_bin("d")))
        ) is True
        # INT-only rejects "3.14".
        assert (
            await _eval_exp(
                string_client_813,
                key,
                Exp.string_is_numeric_typed(StringNumericType.INT, Exp.string_bin("d")),
            )
        ) is False
        # INT-only accepts pure integer.
        assert (
            await _eval_exp(
                string_client_813,
                key,
                Exp.string_is_numeric_typed(StringNumericType.INT, Exp.string_bin("i")),
            )
        ) is True

    async def test_is_upper_is_lower(self, string_client_813):
        key = _key("case")
        await string_client_813.put(key, {"u": "HELLO", "l": "hello"}, policy=WritePolicy())
        assert (
            await _eval_exp(string_client_813, key, Exp.string_is_upper(Exp.string_bin("u")))
        ) is True
        assert (
            await _eval_exp(string_client_813, key, Exp.string_is_lower(Exp.string_bin("l")))
        ) is True

    async def test_split_with_and_without_separator(self, string_client_813):
        key = _key("split")
        await string_client_813.put(key, {"s": "a,b,c"}, policy=WritePolicy())
        assert (
            await _eval_exp(
                string_client_813,
                key,
                Exp.string_split_by_separator(Exp.string_val(","), Exp.string_bin("s")),
            )
        ) == ["a", "b", "c"]
        await string_client_813.put(key, {"s": "hi"}, policy=WritePolicy())
        assert (
            await _eval_exp(string_client_813, key, Exp.string_split(Exp.string_bin("s")))
        ) == ["h", "i"]

    async def test_regex_compare_bin_source_with_case_insensitive(self, string_client_813):
        """Spec §4.2: only bin-sourced regex is verified — literal-source
        ``regex_compare(val(pat), val(text))`` returns OP_NOT_APPLICABLE (26)
        per the server-side expression engine's STRING-particle tagging.
        """
        key = _key("regex")
        await string_client_813.put(key, {"s": "Hello World"}, policy=WritePolicy())
        # Default (case-sensitive): no match.
        assert (
            await _eval_exp(
                string_client_813,
                key,
                Exp.string_regex_compare(Exp.string_val("^hello.*$"), Exp.string_bin("s")),
            )
        ) is False
        # CASE_INSENSITIVE: matches.
        assert (
            await _eval_exp(
                string_client_813,
                key,
                Exp.string_regex_compare_with_flags(
                    Exp.string_val("^hello.*$"),
                    int(StringRegexFlags.CASE_INSENSITIVE),
                    Exp.string_bin("s"),
                ),
            )
        ) is True


# ---------------------------------------------------------------------------
# Modify expressions (return modified VALUE; do not persist)
# ---------------------------------------------------------------------------


class TestStringModifyExpressions:

    async def test_upper_returns_modified_value_does_not_persist(self, string_client_813):
        key = _key("upper_pure")
        await string_client_813.put(key, {"s": "hello"}, policy=WritePolicy())
        # Returns "HELLO" but the stored bin stays lowercase.
        out = await _eval_exp(
            string_client_813, key, Exp.string_upper(0, Exp.string_bin("s"))
        )
        assert out == "HELLO"
        rec = await string_client_813.get(key)
        assert rec.bins.get("s") == "hello"

    async def test_chained_modify_into_read_expression(self, string_client_813):
        """upper(trim(stringBin)) — verifies modify-expr output is a valid Exp input."""
        key = _key("chain")
        await string_client_813.put(key, {"s": "  hi  "}, policy=WritePolicy())
        out = await _eval_exp(
            string_client_813,
            key,
            Exp.string_upper(0, Exp.string_trim(0, Exp.string_bin("s"))),
        )
        assert out == "HI"

    async def test_replace_and_replace_all(self, string_client_813):
        key = _key("replace_exp")
        await string_client_813.put(key, {"s": "ab ab ab"}, policy=WritePolicy())
        # First only.
        out = await _eval_exp(
            string_client_813,
            key,
            Exp.string_replace(
                0, Exp.string_val("ab"), Exp.string_val("Z"), Exp.string_bin("s")
            ),
        )
        assert out == "Z ab ab"
        # All.
        out = await _eval_exp(
            string_client_813,
            key,
            Exp.string_replace_all(
                0, Exp.string_val("ab"), Exp.string_val("Z"), Exp.string_bin("s")
            ),
        )
        assert out == "Z Z Z"

    async def test_regex_replace_global_flag(self, string_client_813):
        key = _key("regex_replace_exp")
        await string_client_813.put(key, {"s": "ab ab ab"}, policy=WritePolicy())
        out = await _eval_exp(
            string_client_813,
            key,
            Exp.string_regex_replace(
                Exp.string_val("ab"),
                Exp.string_val("Z"),
                int(StringRegexFlags.GLOBAL),
                Exp.string_bin("s"),
            ),
        )
        assert out == "Z Z Z"

    async def test_pad_repeat(self, string_client_813):
        key = _key("pad_repeat_exp")
        await string_client_813.put(key, {"s": "42"}, policy=WritePolicy())
        out = await _eval_exp(
            string_client_813,
            key,
            Exp.string_pad_start(
                0, Exp.int_val(5), Exp.string_val("0"), Exp.string_bin("s")
            ),
        )
        assert out == "00042"
        out = await _eval_exp(
            string_client_813,
            key,
            Exp.string_repeat(0, Exp.int_val(3), Exp.string_bin("s")),
        )
        assert out == "424242"


# ---------------------------------------------------------------------------
# to_string expression (CALL_REPR, module 4)
# ---------------------------------------------------------------------------


class TestToStringExpression:

    async def test_int_to_string_via_expression(self, string_client_813):
        key = _key("ts_exp_int")
        await string_client_813.put(key, {"n": 42}, policy=WritePolicy())
        out = await _eval_exp(
            string_client_813, key, Exp.string_to_string(Exp.int_bin("n"))
        )
        assert out == "42"


# ---------------------------------------------------------------------------
# Nested-source via List/Map projection (spec §3.7 "no CTX on expression path")
# ---------------------------------------------------------------------------


class TestStringExpNestedSource:
    """Spec §3.7: expression path does NOT take CTX. To target a nested
    string, project via ``list_get_by_index`` / ``map_get_by_key`` first,
    then pass the projection as ``src`` to a string expression.
    """

    async def test_strlen_on_string_at_list_index(self, string_client_813):
        key = _key("nested_list")
        await string_client_813.put(
            key, {"lst": ["alpha", "beta", "gamma"]}, policy=WritePolicy()
        )
        # Pick lst[2] = "gamma" → strlen = 5.
        out = await _eval_exp(
            string_client_813,
            key,
            Exp.string_strlen(
                Exp.list_get_by_index(
                    ListReturnType.VALUE,
                    ExpType.STRING,
                    Exp.int_val(2),
                    Exp.list_bin("lst"),
                    [],
                )
            ),
        )
        assert out == 5

    async def test_upper_on_string_at_map_key(self, string_client_813):
        key = _key("nested_map")
        await string_client_813.put(
            key, {"m": {"a": "hello", "b": "world"}}, policy=WritePolicy()
        )
        out = await _eval_exp(
            string_client_813,
            key,
            Exp.string_upper(
                0,
                Exp.map_get_by_key(
                    MapReturnType.VALUE,
                    ExpType.STRING,
                    Exp.string_val("a"),
                    Exp.map_bin("m"),
                    [],
                ),
            ),
        )
        assert out == "HELLO"
