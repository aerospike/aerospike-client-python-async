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

"""Unit tests for extended error detail: the verbosity enum, the subcode
catalog, and the ``error_detail_verbosity`` policy surface.

These exercise the client-side bindings only (no server). Behavior against a
live cluster is covered in ``tests/integration/error_detail_test.py``.
"""

import pytest

from aerospike_async import (
    BatchPolicy,
    ErrorDetailVerbosity,
    QueryPolicy,
    ReadPolicy,
    SubCode,
    WritePolicy,
)


class TestErrorDetailVerbosity:
    """The verbosity enum exposes the four documented levels."""

    def test_levels(self):
        assert ErrorDetailVerbosity.NONE == 0
        assert ErrorDetailVerbosity.SUBCODE == 1
        assert ErrorDetailVerbosity.MESSAGE == 2
        assert ErrorDetailVerbosity.EXPRESSION_TRACE == 3

    def test_ordered(self):
        assert (
            ErrorDetailVerbosity.NONE
            < ErrorDetailVerbosity.SUBCODE
            < ErrorDetailVerbosity.MESSAGE
            < ErrorDetailVerbosity.EXPRESSION_TRACE
        )


class TestExpressionTrace:
    """The structured expression-trace type exposes phase/lang constants."""

    def test_phase_constants(self):
        from aerospike_async import ExpressionTrace

        assert ExpressionTrace.PHASE_BUILD == 1
        assert ExpressionTrace.PHASE_EVAL == 2

    def test_lang_constants(self):
        from aerospike_async import ExpressionTrace

        assert ExpressionTrace.LANG_MSGPACK == 1
        assert ExpressionTrace.LANG_AEL == 2

    def test_fields_exposed(self):
        from aerospike_async import ExpressionTrace

        for field in (
            "phase", "byte_offset", "op", "depth", "path", "snippet",
            "lang", "ael_offset", "ael_span",
        ):
            assert hasattr(ExpressionTrace, field)


class TestSubCodeCatalog:
    """The subcode catalog mirrors the server's per-status enums."""

    def test_none_is_zero(self):
        assert SubCode.NONE == 0

    def test_representative_values(self):
        # Names mirror the core catalog. Values are scoped to a parent
        # result code, so they are only meaningful paired with it.
        assert SubCode.PARAM_TTL_INVALID == 1
        assert SubCode.PARAM_BITS_OFFSET_OUT_OF_RANGE == 2
        assert SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS == 1
        assert SubCode.OPNOT_CDT_RANK_OUT_OF_BOUNDS == 2
        assert SubCode.MRT_BLOCKED_RECORD_LOCKED == 1

    def test_values_are_scoped_not_globally_unique(self):
        # The same integer recurs under different parent result codes: a
        # subcode is only interpretable together with its result code.
        assert (
            SubCode.PARAM_TTL_INVALID
            == SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS
            == SubCode.MRT_BLOCKED_RECORD_LOCKED
            == 1
        )


class TestErrorDetailVerbosityPolicySurface:
    """``error_detail_verbosity`` is exposed on the derived policies."""

    @pytest.mark.parametrize("policy_cls", [ReadPolicy, WritePolicy, BatchPolicy, QueryPolicy])
    def test_default_is_none(self, policy_cls):
        # Default is verbosity 0 so the wire cost is nil unless opted in.
        assert policy_cls().error_detail_verbosity == ErrorDetailVerbosity.NONE

    @pytest.mark.parametrize("policy_cls", [ReadPolicy, WritePolicy, BatchPolicy, QueryPolicy])
    def test_round_trip(self, policy_cls):
        p = policy_cls()
        p.error_detail_verbosity = ErrorDetailVerbosity.MESSAGE
        assert p.error_detail_verbosity == ErrorDetailVerbosity.MESSAGE

    @pytest.mark.parametrize("policy_cls", [ReadPolicy, WritePolicy, BatchPolicy])
    def test_from_fields(self, policy_cls):
        p = policy_cls.from_fields(error_detail_verbosity=ErrorDetailVerbosity.SUBCODE)
        assert p.error_detail_verbosity == ErrorDetailVerbosity.SUBCODE

    def test_query_policy_is_attribute_only(self):
        # QueryPolicy has no from_fields constructor; the attribute is the
        # only way to set verbosity on it.
        assert not hasattr(QueryPolicy, "from_fields")
