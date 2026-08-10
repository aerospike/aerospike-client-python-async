# Copyright 2023-2026 Aerospike, Inc.
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

"""Tier D integration tests: ``REQUIRE_INDEX`` and ``HARD_HINT`` on field 44 explain.

Port of Java ``QuerySelectionHintFlagsTest`` / PSDK ``query_selection_hint_flags_test``.
"""

from __future__ import annotations

import pytest
from aerospike_async import (
    CollectionIndexType,
    Filter,
    IndexType,
    Key,
    QuerySelection,
    ResultCode,
    WritePolicy,
)
from aerospike_async.exceptions import IndexNotFound, InvalidRequest
from fixtures import TestFixtureConnection

from query_selection_helpers import (
    BIN_AGE,
    BIN_COUNTRY,
    BIN_SCORE,
    ExplainHint,
    HINT_BOGUS_INDEX_NAME,
    HINT_INDEX_NAME,
    HINT_SCORE_INDEX_NAME,
    HINT_SET_NAME,
    NS,
    explain_plan_async,
    hint_key_name,
)


@pytest.fixture
async def qselhint_fixture(client, supports_query_selection, wait_for_index):
    if not supports_query_selection:
        pytest.skip(
            "cluster lacks query selection "
            "(Node.version.supports_query_selection() is False on one or more nodes)"
        )

    wp = WritePolicy()
    for suffix in ("1", "2"):
        key = Key(NS, HINT_SET_NAME, hint_key_name(suffix))
        try:
            await client.delete(key, policy=wp)
        except Exception:
            pass

    for index_name, bin_name in (
        (HINT_INDEX_NAME, BIN_AGE),
        (HINT_SCORE_INDEX_NAME, BIN_SCORE),
    ):
        try:
            await client.create_index(
                NS,
                HINT_SET_NAME,
                bin_name,
                index_name,
                IndexType.NUMERIC,
                cit=CollectionIndexType.DEFAULT,
            )
        except Exception:
            pass

    await client.put(
        Key(NS, HINT_SET_NAME, hint_key_name("1")),
        {BIN_AGE: 25, BIN_SCORE: 25, BIN_COUNTRY: "US"},
        policy=wp,
    )
    await client.put(
        Key(NS, HINT_SET_NAME, hint_key_name("2")),
        {BIN_AGE: 30, BIN_SCORE: 30, BIN_COUNTRY: "CA"},
        policy=wp,
    )

    await wait_for_index(
        client,
        NS,
        HINT_SET_NAME,
        Filter.range(BIN_AGE, 25, 30),
    )
    await wait_for_index(
        client,
        NS,
        HINT_SET_NAME,
        Filter.range(BIN_SCORE, 25, 30),
    )

    yield client

    for suffix in ("1", "2"):
        try:
            await client.delete(Key(NS, HINT_SET_NAME, hint_key_name(suffix)))
        except Exception:
            pass


class TestQuerySelectionHintFlags(TestFixtureConnection):
    async def test_require_index_on_primary_index_plan_fails_explain(
        self, qselhint_fixture,
    ):
        with pytest.raises(IndexNotFound) as exc_info:
            await explain_plan_async(
                qselhint_fixture,
                "$.country == 'US'",
                hint=ExplainHint(require_index=True),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    async def test_require_index_with_soft_hint_selects_secondary_index(
        self, qselhint_fixture,
    ):
        plan = await explain_plan_async(
            qselhint_fixture,
            "$.age == 25",
            hint=ExplainHint(
                require_index=True,
                index_name=HINT_SCORE_INDEX_NAME,
            ),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == HINT_INDEX_NAME

    async def test_hard_hint_with_matching_index_selects_hinted_index(
        self, qselhint_fixture,
    ):
        plan = await explain_plan_async(
            qselhint_fixture,
            "$.age == 25",
            hint=ExplainHint(
                index_name=HINT_INDEX_NAME,
                hard_hint=True,
            ),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == HINT_INDEX_NAME

    async def test_require_index_and_hard_hint_selects_hinted_index(
        self, qselhint_fixture,
    ):
        plan = await explain_plan_async(
            qselhint_fixture,
            "$.age == 25",
            hint=ExplainHint(
                index_name=HINT_INDEX_NAME,
                require_index=True,
                hard_hint=True,
            ),
        )

        assert plan.index_name == HINT_INDEX_NAME

    async def test_hard_hint_with_wrong_index_fails_explain(self, qselhint_fixture):
        with pytest.raises(IndexNotFound) as exc_info:
            await explain_plan_async(
                qselhint_fixture,
                "$.age == 25",
                hint=ExplainHint(
                    index_name=HINT_BOGUS_INDEX_NAME,
                    hard_hint=True,
                ),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    async def test_bad_ael_fails_explain_with_parameter(self, qselhint_fixture):
        with pytest.raises(InvalidRequest) as exc_info:
            await explain_plan_async(
                qselhint_fixture, "$.age > 30 and",
            )
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
