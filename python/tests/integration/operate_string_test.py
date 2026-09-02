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

"""Integration tests for server-side string operations (requires server 8.1.3+).

Scenarios mirror the rust-core suite at ``tests/src/string.rs``
(canonical wire-behavior coverage) and cover the operate-path
scenarios called out in the string-ops spec §4.1. Spec callouts are
surfaced as inline comments where the test pins a non-obvious behavior
(boolean accessor, missing-bin two-class behavior, CTX wrapper).

Tests target the default ``AEROSPIKE_HOST`` and skip cleanly via the
``supports_string_operations`` capability gate unless it is server >= 8.1.3.
Point ``AEROSPIKE_HOST`` at an 8.1.3+ build to run them; CI covers the
version spread via a server matrix.
"""

import pytest
import pytest_asyncio

from aerospike_async import (
    ClientPolicy,
    CTX,
    Key,
    MapPolicy,
    MapWriteFlags,
    new_client,
    StringNumericType,
    StringOperation,
    StringRegexFlags,
    StringWriteFlags,
    WritePolicy,
)
from aerospike_async.exceptions import InvalidRequest


# Module-level loop scope keeps the shared ``string_client_813`` fixture
# on one event loop across every test in this file. Without this, the
# fixture would be re-created per test (the test-loop scope default is
# ``function``), and each re-create pays a ~30s connect handshake against
# the remote 8.1.3 host.
pytestmark = pytest.mark.asyncio(loop_scope="module")


_NAMESPACE = "test"
_SET = "tstrop"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def string_client_813(aerospike_host, supports_string_operations, use_services_alternate):
    """Module-scoped client for server-side string ops (server >= 8.1.3).

    Single-host model: connects to the default ``AEROSPIKE_HOST`` and skips
    cleanly via ``supports_string_operations`` unless that cluster is 8.1.3+.
    Point ``AEROSPIKE_HOST`` at an 8.1.3+ build to run these; CI covers the
    version spread via a server matrix rather than a dedicated host var.
    """
    if not supports_string_operations:
        pytest.skip(
            "string operations require server >= 8.1.3; point AEROSPIKE_HOST "
            "at an 8.1.3+ build to run these"
        )
    import asyncio as _asyncio
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)
    # ``new_client`` returns once the seed handshake succeeds; the first
    # cluster-tend cycle that populates per-namespace partition maps may
    # not have completed yet. Without this delay, the first op against the
    # ``test`` namespace fails with ``Invalid namespace: Partition map
    # empty``. 2s covers the default 1s ``tend_interval`` with margin.
    await _asyncio.sleep(2)
    yield client
    await client.close()


def _key(suffix: str) -> Key:
    return Key(_NAMESPACE, _SET, suffix)


async def _put_str(client, key, bin_name, value, *, wp=None):
    await client.put(key, {bin_name: value}, policy=wp or WritePolicy())


async def _read_str(client, key, bin_name):
    rec = await client.get(key)
    assert rec is not None
    return rec.bins.get(bin_name)


async def _operate_first_value(client, key, ops, *, wp=None):
    """Run ``client.operate`` and return the first op result for the named bin.

    Helper for read tests that submit a single op against bin ``"s"`` (the
    convention used throughout this file).
    """
    rec = await client.operate(key, ops, policy=wp or WritePolicy())
    assert rec is not None
    return rec.bins.get("s")


# ---------------------------------------------------------------------------
# READ ops (STRING_READ, sub-ops 0..16)
# ---------------------------------------------------------------------------
#
# Names mirror rust-core's ``tests/src/string.rs`` scenarios verbatim
# where they pin a wire-observable behavior; additional scenarios cover
# multi-op pipelines and projections per spec §4.1.


class TestStringReads:
    """Read sub-ops 0..16 covered against representative inputs."""

    async def test_strlen_returns_codepoint_count(self, string_client_813):
        key = _key("strlen")
        await _put_str(string_client_813, key, "s", "héllo")
        out = await _operate_first_value(string_client_813, key, [StringOperation.strlen("s")])
        # 5 codepoints; byte_length would return 6 for é (2-byte UTF-8).
        assert out == 5

    async def test_strlen_empty_string_is_zero(self, string_client_813):
        key = _key("strlen_empty")
        await _put_str(string_client_813, key, "s", "")
        out = await _operate_first_value(string_client_813, key, [StringOperation.strlen("s")])
        assert out == 0

    async def test_byte_length_returns_utf8_bytes(self, string_client_813):
        key = _key("byte_len")
        await _put_str(string_client_813, key, "s", "héllo")
        out = await _operate_first_value(string_client_813, key, [StringOperation.byte_length("s")])
        # 'h'=1 'é'=2 'l'=1 'l'=1 'o'=1 → 6 UTF-8 bytes.
        assert out == 6

    async def test_substr_offset_to_end(self, string_client_813):
        key = _key("substr_from")
        await _put_str(string_client_813, key, "s", "hello world")
        out = await _operate_first_value(string_client_813, key, [StringOperation.substr("s", 6)])
        assert out == "world"

    async def test_substr_range_form(self, string_client_813):
        """Spec §3.1: substr(bin, start, end) is end-exclusive, NOT a length.

        ``substr("s", 1, 4)`` on ``"hello"`` returns ``"ell"`` —
        codepoints [1, 4) = ``'e','l','l'``.
        """
        key = _key("substr_range")
        await _put_str(string_client_813, key, "s", "hello")
        out = await _operate_first_value(string_client_813, key, [StringOperation.substr("s", 1, 4)])
        assert out == "ell"

    async def test_substr_negative_start(self, string_client_813):
        key = _key("substr_neg")
        await _put_str(string_client_813, key, "s", "hello world")
        out = await _operate_first_value(string_client_813, key, [StringOperation.substr("s", -5)])
        assert out == "world"

    async def test_char_at_returns_single_codepoint(self, string_client_813):
        key = _key("char_at")
        await _put_str(string_client_813, key, "s", "Hello123World")
        out = await _operate_first_value(string_client_813, key, [StringOperation.char_at("s", 5)])
        assert out == "1"

    async def test_find_first_match_and_miss(self, string_client_813):
        key = _key("find")
        await _put_str(string_client_813, key, "s", "hello world hello again")
        # First match index.
        out = await _operate_first_value(string_client_813, key, [StringOperation.find("s", "hello")])
        assert out == 0
        # Not found returns -1 (spec §2.4).
        out = await _operate_first_value(string_client_813, key, [StringOperation.find("s", "ZZZ")])
        assert out == -1

    async def test_find_nth_occurrence(self, string_client_813):
        key = _key("find_nth")
        await _put_str(string_client_813, key, "s", "ab ab ab")
        # Second occurrence of "ab" is at index 3.
        out = await _operate_first_value(
            string_client_813, key, [StringOperation.find("s", "ab", 2)]
        )
        assert out == 3

    async def test_contains_starts_with_ends_with_decode_as_bool(self, string_client_813):
        """Spec §2.4 boolean accessor: these sub-ops return native msgpack bool.

        ``getLong`` would fail to decode; the PAC stream surfaces a Python
        ``bool``, NOT an ``int``. Pinned here for parity-test reuse.
        """
        key = _key("predicates")
        await _put_str(string_client_813, key, "s", "hello world")
        rec = await string_client_813.operate(
            key,
            [
                StringOperation.contains("s", "world"),
                StringOperation.starts_with("s", "hello"),
                StringOperation.ends_with("s", "world"),
                StringOperation.contains("s", "ZZZ"),
            ],
            policy=WritePolicy(),
        )
        assert rec is not None
        # PAC returns each op result keyed by the bin name; multi-op-same-bin
        # is rolled into a list in declaration order.
        results = rec.bins.get("s")
        assert isinstance(results, list)
        assert results == [True, True, True, False]
        for r in results:
            assert isinstance(r, bool), f"expected bool, got {type(r).__name__}"

    async def test_to_integer_parses_digits(self, string_client_813):
        key = _key("to_int")
        await _put_str(string_client_813, key, "s", "12345")
        out = await _operate_first_value(string_client_813, key, [StringOperation.to_integer("s")])
        assert out == 12345

    async def test_to_double_parses_decimal(self, string_client_813):
        key = _key("to_double")
        await _put_str(string_client_813, key, "s", "3.14159")
        out = await _operate_first_value(string_client_813, key, [StringOperation.to_double("s")])
        assert out == pytest.approx(3.14159)

    async def test_is_numeric_default_any(self, string_client_813):
        key = _key("is_numeric_any")
        await _put_str(string_client_813, key, "s", "3.14")
        out = await _operate_first_value(string_client_813, key, [StringOperation.is_numeric("s")])
        assert out is True

    async def test_is_numeric_int_filter_rejects_float(self, string_client_813):
        key = _key("is_numeric_int")
        await _put_str(string_client_813, key, "s", "3.14")
        out = await _operate_first_value(
            string_client_813,
            key,
            [StringOperation.is_numeric("s", StringNumericType.INT)],
        )
        assert out is False

    async def test_is_upper_is_lower(self, string_client_813):
        key = _key("case_pred")
        await _put_str(string_client_813, key, "s", "HELLO")
        rec = await string_client_813.operate(
            key,
            [StringOperation.is_upper("s"), StringOperation.is_lower("s")],
            policy=WritePolicy(),
        )
        assert rec.bins.get("s") == [True, False]

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "is_upper/is_lower reject any string containing a non-cased "
            "character -- a space, digit or punctuation mark makes an "
            "otherwise-uppercase string report False. Tracked as SERVER-1603. "
            "Strict so this trips the moment the server is fixed, since a "
            "silently-passing xfail would leave the corrected behavior "
            "unasserted."
        ),
    )
    async def test_classifiers_ignore_non_cased_characters(self, string_client_813):
        """Classification should consider only the cased characters.

        Measured on 8.1.3.0-104: "HELLO" is True, but "HELLO WORLD", "ABC123"
        and "HELLO!" are all False. The empty string is True, which rules out
        the "no cased characters" reading -- it is the *presence* of a
        non-cased character that flips the answer.
        """
        key = _key("case_non_cased")
        for value in ("HELLO WORLD", "ABC123", "HELLO!"):
            await _put_str(string_client_813, key, "s", value)
            rec = await string_client_813.operate(
                key, [StringOperation.is_upper("s")], policy=WritePolicy(),
            )
            assert rec.bins.get("s") is True, f"is_upper({value!r}) should be True"

    async def test_split_with_separator(self, string_client_813):
        key = _key("split")
        await _put_str(string_client_813, key, "s", "a,b,c")
        out = await _operate_first_value(
            string_client_813, key, [StringOperation.split("s", ",")]
        )
        assert out == ["a", "b", "c"]

    async def test_split_no_separator_codepoints(self, string_client_813):
        """No-separator form returns one element per codepoint (spec §2.4)."""
        key = _key("split_cp")
        await _put_str(string_client_813, key, "s", "héy")
        out = await _operate_first_value(string_client_813, key, [StringOperation.split("s")])
        assert out == ["h", "é", "y"]

    async def test_to_blob_round_trip_via_b64_decode(self, string_client_813):
        """``to_blob`` returns UTF-8 bytes; ``b64_decode`` round-trips through base64."""
        key = _key("blob_rt")
        # "Hello!" base64-encoded is "SGVsbG8h".
        await _put_str(string_client_813, key, "s", "SGVsbG8h")
        out = await _operate_first_value(string_client_813, key, [StringOperation.b64_decode("s")])
        assert out == b"Hello!"

    async def test_regex_compare_case_insensitive_flag(self, string_client_813):
        """``StringRegexFlags.CASE_INSENSITIVE`` (1) honored by the wire decoder."""
        key = _key("regex_ci")
        await _put_str(string_client_813, key, "s", "Hello World")
        out_default = await _operate_first_value(
            string_client_813, key, [StringOperation.regex_compare("s", "^hello.*$")]
        )
        out_ci = await _operate_first_value(
            string_client_813,
            key,
            [StringOperation.regex_compare("s", "^hello.*$", int(StringRegexFlags.CASE_INSENSITIVE))],
        )
        assert out_default is False
        assert out_ci is True


# ---------------------------------------------------------------------------
# MODIFY ops (STRING_MODIFY, sub-ops 50..66)
# ---------------------------------------------------------------------------


class TestStringModifies:
    """Modify sub-ops verified by reading the bin back after the op."""

    async def test_upper_lower_case_fold_mutate_in_place(self, string_client_813):
        key = _key("case_mod")
        await _put_str(string_client_813, key, "s", "Hello")
        await string_client_813.operate(key, [StringOperation.upper("s")])
        assert await _read_str(string_client_813, key, "s") == "HELLO"
        await string_client_813.operate(key, [StringOperation.lower("s")])
        assert await _read_str(string_client_813, key, "s") == "hello"
        await string_client_813.operate(key, [StringOperation.case_fold("s")])
        assert await _read_str(string_client_813, key, "s") == "hello"

    async def test_normalize_nfc_identity_on_normalized_input(self, string_client_813):
        key = _key("nfc_identity")
        await _put_str(string_client_813, key, "s", "hello")
        await string_client_813.operate(key, [StringOperation.normalize_nfc("s")])
        assert await _read_str(string_client_813, key, "s") == "hello"

    async def test_insert_at_middle_start_end_and_negative(self, string_client_813):
        key = _key("insert")
        await _put_str(string_client_813, key, "s", "world")
        # At index 0 (prepend).
        await string_client_813.operate(key, [StringOperation.insert("s", 0, "hello ")])
        assert await _read_str(string_client_813, key, "s") == "hello world"
        # At end (append).
        await string_client_813.operate(key, [StringOperation.insert("s", 11, "!")])
        assert await _read_str(string_client_813, key, "s") == "hello world!"
        # At negative index (from end).
        await string_client_813.operate(key, [StringOperation.insert("s", -1, "?")])
        assert await _read_str(string_client_813, key, "s") == "hello world?!"

    async def test_overwrite_extends_beyond_original_length(self, string_client_813):
        key = _key("overwrite")
        await _put_str(string_client_813, key, "s", "abc")
        await string_client_813.operate(key, [StringOperation.overwrite("s", 1, "XYZWV")])
        assert await _read_str(string_client_813, key, "s") == "aXYZWV"

    async def test_snip_range_and_suffix(self, string_client_813):
        """Snip always takes explicit (start, end). Per the updated spec
        (and the underlying server constraint), there is no 1-arg form —
        a wire ``[53, start, flags]`` is misparsed as ``[53, start, end]``.
        Callers who want to drop a suffix supply the codepoint length as
        ``end`` explicitly (typically via a paired ``strlen`` read).
        """
        # Range form [start, end).
        key = _key("snip_range")
        await _put_str(string_client_813, key, "s", "abcdef")
        await string_client_813.operate(key, [StringOperation.snip("s", 2, 4)])
        assert await _read_str(string_client_813, key, "s") == "abef"
        # Drop suffix by passing the explicit codepoint length as ``end``.
        key = _key("snip_to_end")
        await _put_str(string_client_813, key, "s", "abcdef")
        await string_client_813.operate(key, [StringOperation.snip("s", 3, 6)])
        assert await _read_str(string_client_813, key, "s") == "abc"

    async def test_replace_first_match_only(self, string_client_813):
        key = _key("replace")
        await _put_str(string_client_813, key, "s", "ab ab ab")
        await string_client_813.operate(key, [StringOperation.replace("s", "ab", "Z")])
        assert await _read_str(string_client_813, key, "s") == "Z ab ab"

    async def test_replace_all_substitutes_every_match(self, string_client_813):
        key = _key("replace_all")
        await _put_str(string_client_813, key, "s", "ab ab ab")
        await string_client_813.operate(key, [StringOperation.replace_all("s", "ab", "Z")])
        assert await _read_str(string_client_813, key, "s") == "Z Z Z"

    async def test_trim_variants(self, string_client_813):
        # trim_start strips only leading whitespace.
        key = _key("trim_start")
        await _put_str(string_client_813, key, "s", "  hello  ")
        await string_client_813.operate(key, [StringOperation.trim_start("s")])
        assert await _read_str(string_client_813, key, "s") == "hello  "
        # trim_end strips only trailing whitespace.
        key = _key("trim_end")
        await _put_str(string_client_813, key, "s", "  hello  ")
        await string_client_813.operate(key, [StringOperation.trim_end("s")])
        assert await _read_str(string_client_813, key, "s") == "  hello"
        # trim strips both.
        key = _key("trim_both")
        await _put_str(string_client_813, key, "s", "  hello  ")
        await string_client_813.operate(key, [StringOperation.trim("s")])
        assert await _read_str(string_client_813, key, "s") == "hello"

    async def test_pad_start_and_pad_end(self, string_client_813):
        key = _key("pad_start")
        await _put_str(string_client_813, key, "s", "42")
        await string_client_813.operate(key, [StringOperation.pad_start("s", 5, "0")])
        assert await _read_str(string_client_813, key, "s") == "00042"
        key = _key("pad_end")
        await _put_str(string_client_813, key, "s", "42")
        await string_client_813.operate(key, [StringOperation.pad_end("s", 5, "x")])
        assert await _read_str(string_client_813, key, "s") == "42xxx"

    async def test_pad_noop_when_already_at_length(self, string_client_813):
        """Spec §2.5: pad is a no-op when the source already meets target_length."""
        key = _key("pad_noop")
        await _put_str(string_client_813, key, "s", "hello")
        await string_client_813.operate(key, [StringOperation.pad_start("s", 3, "x")])
        assert await _read_str(string_client_813, key, "s") == "hello"

    async def test_repeat_duplicates_contents(self, string_client_813):
        key = _key("repeat")
        await _put_str(string_client_813, key, "s", "ab")
        await string_client_813.operate(key, [StringOperation.repeat("s", 3)])
        assert await _read_str(string_client_813, key, "s") == "ababab"

    async def test_concat_single_string_form(self, string_client_813):
        """``concat`` accepts a single str; wire encoding wraps in a 1-elt list (spec §2.5)."""
        key = _key("concat_one")
        await _put_str(string_client_813, key, "s", "hello")
        await string_client_813.operate(key, [StringOperation.concat("s", " world")])
        assert await _read_str(string_client_813, key, "s") == "hello world"

    async def test_concat_list_form(self, string_client_813):
        """``concat`` accepts a list; each element appended in order."""
        key = _key("concat_list")
        await _put_str(string_client_813, key, "s", "a")
        await string_client_813.operate(key, [StringOperation.concat("s", ["b", "c", "d"])])
        assert await _read_str(string_client_813, key, "s") == "abcd"

    async def test_append_adds_to_end(self, string_client_813):
        """``append`` (sub-op 67) adds a single value to the end of the bin."""
        key = _key("append")
        await _put_str(string_client_813, key, "s", "hello")
        await string_client_813.operate(key, [StringOperation.append("s", " world")])
        assert await _read_str(string_client_813, key, "s") == "hello world"

    async def test_prepend_adds_to_start(self, string_client_813):
        """``prepend`` (sub-op 68) adds a single value to the start of the bin."""
        key = _key("prepend")
        await _put_str(string_client_813, key, "s", "world")
        await string_client_813.operate(key, [StringOperation.prepend("s", "hello ")])
        assert await _read_str(string_client_813, key, "s") == "hello world"

    async def test_regex_replace_first_match_default(self, string_client_813):
        key = _key("regex_replace")
        await _put_str(string_client_813, key, "s", "ab ab ab")
        # Default behavior: replace only the first match.
        await string_client_813.operate(
            key, [StringOperation.regex_replace("s", "ab", "Z")]
        )
        assert await _read_str(string_client_813, key, "s") == "Z ab ab"

    async def test_regex_replace_global_flag_replaces_every_match(self, string_client_813):
        """Spec §3.5: GLOBAL bit in regex_flags makes regex_replace replace all."""
        key = _key("regex_replace_global")
        await _put_str(string_client_813, key, "s", "ab ab ab")
        await string_client_813.operate(
            key,
            [
                StringOperation.regex_replace(
                    "s", "ab", "Z", int(StringRegexFlags.GLOBAL)
                )
            ],
        )
        assert await _read_str(string_client_813, key, "s") == "Z Z Z"


# ---------------------------------------------------------------------------
# Multi-op pipelines (spec §4.1)
# ---------------------------------------------------------------------------


class TestMultiOpPipelines:
    """Single ``client.operate`` carrying multiple ops; verifies that
    mixed return types decode together and that later read ops observe
    the state produced by earlier modify ops.
    """

    async def test_mixed_return_types_decode_correctly(self, string_client_813):
        """strlen (int) + isUpper (bool) + find (int) in one call."""
        key = _key("mixed_returns")
        await string_client_813.put(
            key, {"text": "hello", "upper_str": "HI"}, policy=WritePolicy()
        )
        rec = await string_client_813.operate(
            key,
            [
                StringOperation.strlen("text"),
                StringOperation.is_upper("upper_str"),
                StringOperation.find("text", "ll"),
            ],
            policy=WritePolicy(),
        )
        # Two ops on "text" (strlen=5, find=2) come back as a list.
        assert rec.bins.get("text") == [5, 2]
        # Single op on "upper_str" returns a scalar bool (not a 1-elt list).
        assert rec.bins.get("upper_str") is True

    async def test_modify_then_read_observes_post_modify_state(self, string_client_813):
        """trim → upper → strlen in one operate; the strlen must see the modified value.

        PAC unmarshaling note: the three ops on bin ``s`` produce
        (None, None, 2) on the wire (the two modify ops return the canonical
        null sentinel and PAC drops them from the response). The visible
        result is just the strlen int — but the persisted state proves the
        ordering: ``trim`` ran before ``upper`` ran before ``strlen`` saw
        the post-modify length of 2.
        """
        key = _key("mod_then_read")
        await _put_str(string_client_813, key, "s", "  hi  ")
        rec = await string_client_813.operate(
            key,
            [
                StringOperation.trim("s"),
                StringOperation.upper("s"),
                StringOperation.strlen("s"),
            ],
            policy=WritePolicy(),
        )
        # Final stored value confirms trim → upper applied in order.
        assert await _read_str(string_client_813, key, "s") == "HI"
        # Visible response is the strlen result observing the post-modify
        # state (2 codepoints).
        assert rec.bins.get("s") == 2


# ---------------------------------------------------------------------------
# TO_STRING (op-type 19) — type-conversion scenarios per spec §4.1
# ---------------------------------------------------------------------------


class TestToString:
    """Five conversions (spec §4.1). ``to_string`` has no payload + no CTX."""

    async def test_int_to_string(self, string_client_813):
        key = _key("ts_int")
        await string_client_813.put(key, {"n": 42}, policy=WritePolicy())
        out = await _operate_first_value(string_client_813, key, [StringOperation.to_string("n")])
        # Map _operate_first_value returns by "s" — re-key.
        rec = await string_client_813.operate(
            key, [StringOperation.to_string("n")], policy=WritePolicy()
        )
        assert rec.bins.get("n") == "42"

    async def test_double_to_string_parses_back(self, string_client_813):
        """Float-formatted string — assert by parse-back, not exact match (spec §4.1)."""
        key = _key("ts_double")
        await string_client_813.put(key, {"f": 3.14159}, policy=WritePolicy())
        rec = await string_client_813.operate(
            key, [StringOperation.to_string("f")], policy=WritePolicy()
        )
        s = rec.bins.get("f")
        assert isinstance(s, str)
        assert float(s) == pytest.approx(3.14159)

    async def test_string_to_string_is_identity(self, string_client_813):
        key = _key("ts_str")
        await string_client_813.put(key, {"s": "already-str"}, policy=WritePolicy())
        rec = await string_client_813.operate(
            key, [StringOperation.to_string("s")], policy=WritePolicy()
        )
        assert rec.bins.get("s") == "already-str"


# ---------------------------------------------------------------------------
# CTX navigation (spec §2.3.1 / §3.1)
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    raises=InvalidRequest,
    reason=(
        "String-op CTX envelope reshaped server-side. The server now expects "
        "the inner op nested — [0xFF, ctx_list, [inner_op, args...]] with the "
        "outer element count fixed at 3 — while core still emits the flat "
        "[0xFF, ctx_flat_list, inner_op, args...] and is rejected with "
        "PARAMETER_ERROR. Tracked as CLIENT-5329; promote these back to plain "
        "tests once core emits the nested envelope."
    ),
)
class TestStringWithCtx:
    """String ops on values nested inside list / map bins.

    The encoder emits the CTX-wrapper envelope when ctx is non-empty and omits
    it entirely when ctx is None, which the server dispatches on separately.
    The wrapper's own layout is mid-change: the flat form encoded here is the
    one the server is moving away from, so every test in this class is
    currently expected to fail — see the class marker.
    """

    async def test_strlen_on_string_at_list_index(self, string_client_813):
        key = _key("ctx_list")
        await string_client_813.put(
            key, {"lst": ["alpha", "beta", "gamma"]}, policy=WritePolicy()
        )
        rec = await string_client_813.operate(
            key,
            [StringOperation.strlen("lst", ctx=[CTX.list_index(1)])],
            policy=WritePolicy(),
        )
        # "beta" has 4 codepoints.
        assert rec.bins.get("lst") == 4

    async def test_starts_with_on_string_at_map_key_returns_bool(self, string_client_813):
        """Boolean return preserved through CTX wrapper (spec §2.4 callout)."""
        key = _key("ctx_map_bool")
        await string_client_813.put(
            key, {"m": {"name": "Alice", "city": "NYC"}}, policy=WritePolicy()
        )
        rec = await string_client_813.operate(
            key,
            [StringOperation.starts_with("m", "Ali", ctx=[CTX.map_key("name")])],
            policy=WritePolicy(),
        )
        assert rec.bins.get("m") is True
        assert isinstance(rec.bins.get("m"), bool)

    async def test_upper_on_string_at_list_index_leaves_siblings_untouched(
        self, string_client_813
    ):
        key = _key("ctx_list_mod")
        await string_client_813.put(
            key, {"lst": ["alpha", "beta", "gamma"]}, policy=WritePolicy()
        )
        await string_client_813.operate(
            key,
            [StringOperation.upper("lst", ctx=[CTX.list_index(1)])],
            policy=WritePolicy(),
        )
        rec = await string_client_813.get(key)
        assert rec.bins.get("lst") == ["alpha", "BETA", "gamma"]

    async def test_replace_on_string_at_map_key_leaves_other_entries_untouched(
        self, string_client_813
    ):
        key = _key("ctx_map_mod")
        await string_client_813.put(
            key, {"m": {"a": "hello", "b": "world"}}, policy=WritePolicy()
        )
        await string_client_813.operate(
            key,
            [StringOperation.replace("m", "ell", "EY", ctx=[CTX.map_key("a")])],
            policy=WritePolicy(),
        )
        rec = await string_client_813.get(key)
        assert rec.bins.get("m") == {"a": "hEYo", "b": "world"}


# ---------------------------------------------------------------------------
# Missing-bin behavior (two op classes)
# ---------------------------------------------------------------------------


class TestMissingBinBehavior:
    """The missing-bin path is determined by op class, not flag:

    - Transform / subtractive ops (upper, lower, trim*, snip, replace*,
      regex_replace, case_fold, normalize_nfc) succeed silently and
      do NOT create the bin.
    - Additive / create ops (insert, overwrite, append, prepend, concat,
      pad_start, pad_end, repeat) create the bin from empty.

    Behavior is independent of the NO_FAIL flag: BIN_NOT_FOUND never
    surfaces on a missing-bin string op. (NO_FAIL now governs only
    in-op execution failures.)
    """

    async def test_transform_op_silently_noops_on_missing_bin(self, string_client_813):
        """`upper()` on a missing bin: returns success, bin is not created."""
        key = _key("transform_noop_missing")
        # Ensure missing_bin really is missing — prior runs may have left state.
        await string_client_813.delete(key, policy=WritePolicy())
        await string_client_813.put(key, {"other": "x"}, policy=WritePolicy())
        await string_client_813.operate(
            key,
            [StringOperation.upper("missing_bin")],
            policy=WritePolicy(),
        )
        rec = await string_client_813.get(key)
        assert rec.bins.get("other") == "x"
        assert "missing_bin" not in (rec.bins or {})

    async def test_create_op_creates_bin_from_empty_on_missing_bin(self, string_client_813):
        """`insert(at=0, "hello")` on a missing bin creates it with the inserted value."""
        key = _key("create_from_missing")
        # Ensure missing_bin really is missing — prior runs may have left state.
        await string_client_813.delete(key, policy=WritePolicy())
        await string_client_813.put(key, {"other": "x"}, policy=WritePolicy())
        await string_client_813.operate(
            key,
            [StringOperation.insert("missing_bin", 0, "hello")],
            policy=WritePolicy(),
        )
        rec = await string_client_813.get(key)
        assert rec.bins.get("missing_bin") == "hello"
        assert rec.bins.get("other") == "x"


# ---------------------------------------------------------------------------
# Server version probe (sanity check the supports_string_operations accessor)
# ---------------------------------------------------------------------------


class TestServerVersionGate:
    """Pin the ``Node.version.supports_string_operations()`` accessor — the
    test cluster we connect to MUST self-report support, otherwise none of
    the above tests would have a clean wire path.
    """

    async def test_node_self_reports_string_operations_support(self, string_client_813):
        node_names = await string_client_813.node_names()
        node = await string_client_813.get_node(node_names[0])
        assert node.version.supports_string_operations() is True, (
            f"AEROSPIKE_HOST cluster reports server "
            f"{node.version} which does NOT advertise string-op support. "
            "Server-side string ops require >= 8.1.3."
        )
