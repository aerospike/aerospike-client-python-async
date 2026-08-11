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

"""End-to-end integration tests for server query selection (explain → execute).

Requires Aerospike Server >= 8.1.3; tests self-skip via
:func:`supports_query_selection`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest
from aerospike_async import (
    CollectionIndexType,
    Filter,
    IndexType,
    Key,
    PartitionFilter,
    QueryPolicy,
    QuerySelection,
    QueryWhereFlags,
    ResultCode,
    Statement,
    WritePolicy,
)
from aerospike_async.exceptions import (
    FilteredOut,
    IndexFoundError,
    IndexNotFound,
    InvalidRequest,
    ValueError,
)
from fixtures import TestFixtureConnection

NAMESPACE = "test"
SET_NAME = "qsel"
DATASET_SIZE = 50
AGE_BIN = "age"
SCORE_BIN = "score"
COUNTRY_BIN = "country"
AGE_INDEX_NAME = "qsel_age_idx"
SCORE_INDEX_NAME = "qsel_score_idx"
BOGUS_INDEX_NAME = "qsel_missing_idx"
HINT_KEY_PREFIX = "qselkey"


def hint_key_name(suffix: str) -> str:
    return f"{HINT_KEY_PREFIX}{suffix}"


@dataclass(frozen=True)
class ExplainHint:
    """Field ``44`` explain hint flags and optional index name."""

    index_name: Optional[str] = None
    require_index: bool = False
    hard_hint: bool = False


def explain_where_flags(hint: Optional[ExplainHint]) -> Optional[int]:
    """Map :class:`ExplainHint` to PAC ``explain_where_flags`` (field ``44``)."""
    if hint is None:
        return None
    flags = QueryWhereFlags.EXPLAIN
    if hint.require_index:
        flags |= QueryWhereFlags.REQUIRE_INDEX
    if hint.hard_hint:
        flags |= QueryWhereFlags.HARD_HINT
    if flags == QueryWhereFlags.EXPLAIN:
        return None
    return int(flags)


async def explain_plan_async(
    client,
    where: str,
    *,
    set_name: str = SET_NAME,
    hint: Optional[ExplainHint] = None,
):
    """Run phase-1 explain with optional field ``44`` hint flags."""
    index_name_hint = hint.index_name if hint is not None else None
    return await client.query_explain(
        NAMESPACE,
        where,
        set_name=set_name,
        index_name_hint=index_name_hint,
        explain_where_flags=explain_where_flags(hint),
    )


async def _collect_int_bin(recordset, bin_name: str) -> list[int]:
    values = []
    async for record in recordset:
        values.append(record.bins[bin_name])
    values.sort()
    return values


async def _drop_indexes(client) -> None:
    for index_name in (AGE_INDEX_NAME, SCORE_INDEX_NAME):
        try:
            task = await client.drop_index(NAMESPACE, SET_NAME, index_name)
            await task.wait_till_complete()
        except Exception:
            pass


@pytest.fixture
async def qsel_fixture(client, supports_query_selection, wait_for_index):
    if not supports_query_selection:
        pytest.skip(
            "cluster lacks query selection "
            "(Node.version.supports_query_selection() is False on one or more nodes)"
        )

    wp = WritePolicy()

    for i in range(1, DATASET_SIZE + 1):
        country = "US" if i % 2 == 0 else "CA"
        key = Key(NAMESPACE, SET_NAME, i)
        await client.put(
            key,
            {AGE_BIN: i, SCORE_BIN: i, COUNTRY_BIN: country},
            policy=wp,
        )

    for suffix in ("1", "2"):
        key = Key(NAMESPACE, SET_NAME, hint_key_name(suffix))
        try:
            await client.delete(key, policy=wp)
        except Exception:
            pass

    # Ages 51/52 avoid colliding with the 1..50 bulk seed (keys with the same age).
    await client.put(
        Key(NAMESPACE, SET_NAME, hint_key_name("1")),
        {AGE_BIN: 51, SCORE_BIN: 51, COUNTRY_BIN: "CA"},
        policy=wp,
    )
    await client.put(
        Key(NAMESPACE, SET_NAME, hint_key_name("2")),
        {AGE_BIN: 52, SCORE_BIN: 52, COUNTRY_BIN: "CA"},
        policy=wp,
    )

    for index_name, bin_name in (
        (AGE_INDEX_NAME, AGE_BIN),
        (SCORE_INDEX_NAME, SCORE_BIN),
    ):
        try:
            await client.create_index(
                NAMESPACE,
                SET_NAME,
                bin_name,
                index_name,
                IndexType.NUMERIC,
                cit=CollectionIndexType.DEFAULT,
            )
        except IndexFoundError:
            pass

    await wait_for_index(
        client,
        NAMESPACE,
        SET_NAME,
        Filter.range(AGE_BIN, 0, 100),
        bins=[AGE_BIN],
    )
    await wait_for_index(
        client,
        NAMESPACE,
        SET_NAME,
        Filter.range(SCORE_BIN, 51, 52),
    )

    yield {
        "set_name": SET_NAME,
        "age_index_name": AGE_INDEX_NAME,
        "score_index_name": SCORE_INDEX_NAME,
    }

    await _drop_indexes(client)


class TestQuerySelectionExplain(TestFixtureConnection):
    """Phase 1: server query explain."""

    async def test_explain_selects_secondary_index_for_age_range(
        self, client, qsel_fixture
    ):
        set_name = qsel_fixture["set_name"]
        age_index_name = qsel_fixture["age_index_name"]

        plan = await client.query_explain(
            NAMESPACE,
            "$.age >= 14 and $.age <= 18",
            set_name=set_name,
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.is_secondary_index
        assert plan.namespace == NAMESPACE
        assert plan.set_name == set_name
        assert plan.index_name == age_index_name
        assert plan.ael

    async def test_explain_selects_primary_index_for_non_indexed_predicate(
        self, client, qsel_fixture
    ):
        set_name = qsel_fixture["set_name"]

        plan = await client.query_explain(
            NAMESPACE,
            "$.country == 'US'",
            set_name=set_name,
        )

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.is_primary_index
        assert plan.namespace == NAMESPACE
        assert plan.set_name == set_name
        assert plan.index_name is None
        assert plan.ael

    async def test_explain_contradiction_predicate_filtered_out(
        self, client, qsel_fixture
    ):
        set_name = qsel_fixture["set_name"]

        plan = await client.query_explain(
            NAMESPACE,
            "$.age > 100 and $.age < 10",
            set_name=set_name,
        )

        assert plan.selection == QuerySelection.FILTERED_OUT
        assert plan.is_filtered_out
        assert plan.index_name is None
        assert plan.ael


class TestQueryPlanFilterForExecute(TestFixtureConnection):
    """``QueryPlan.filter_for_execute()`` for each selection type."""

    async def test_secondary_index_plan_returns_execute_filter(
        self, client, qsel_fixture
    ):
        set_name = qsel_fixture["set_name"]
        age_index_name = qsel_fixture["age_index_name"]

        plan = await client.query_explain(
            NAMESPACE,
            "$.age >= 14 and $.age <= 18",
            set_name=set_name,
        )

        assert plan.is_secondary_index
        execute_filter = plan.filter_for_execute()
        assert isinstance(execute_filter, Filter)
        assert age_index_name in str(execute_filter)

    async def test_primary_index_plan_returns_none(self, client, qsel_fixture):
        set_name = qsel_fixture["set_name"]

        plan = await client.query_explain(
            NAMESPACE,
            "$.country == 'US'",
            set_name=set_name,
        )

        assert plan.is_primary_index
        assert plan.filter_for_execute() is None

    async def test_filtered_out_plan_returns_none(self, client, qsel_fixture):
        set_name = qsel_fixture["set_name"]

        plan = await client.query_explain(
            NAMESPACE,
            "$.age > 100 and $.age < 10",
            set_name=set_name,
        )

        assert plan.is_filtered_out
        assert plan.filter_for_execute() is None


class TestQuerySelectionExecute(TestFixtureConnection):
    """Phase 2: explain then execute via ``query_with_plan``."""

    async def _execute_ael(self, client, set_name: str, ael: str, bins: list[str]):
        plan = await client.query_explain(
            NAMESPACE,
            ael,
            set_name=set_name,
        )
        stmt = Statement(NAMESPACE, set_name, bins)
        return await client.query_with_plan(
            stmt,
            PartitionFilter.all(),
            plan,
            policy=QueryPolicy(),
        )

    async def test_execute_returns_matching_records(self, client, qsel_fixture):
        set_name = qsel_fixture["set_name"]
        records = await self._execute_ael(
            client,
            set_name,
            "$.age >= 14 and $.age <= 18",
            [AGE_BIN],
        )
        ages = await _collect_int_bin(records, AGE_BIN)
        assert ages == [14, 15, 16, 17, 18]

    async def test_execute_equality_returns_single_record(self, client, qsel_fixture):
        set_name = qsel_fixture["set_name"]
        records = await self._execute_ael(
            client,
            set_name,
            "$.age == 25",
            [AGE_BIN],
        )
        ages = await _collect_int_bin(records, AGE_BIN)
        assert ages == [25]

    async def test_execute_primary_index_returns_matching_records(
        self, client, qsel_fixture
    ):
        set_name = qsel_fixture["set_name"]
        records = await self._execute_ael(
            client,
            set_name,
            "$.country == 'US'",
            [COUNTRY_BIN],
        )
        countries = []
        async for record in records:
            countries.append(record.bins[COUNTRY_BIN])
        assert len(countries) == 25
        assert all(c == "US" for c in countries)

    async def test_execute_filtered_out_plan_raises(self, client, qsel_fixture):
        set_name = qsel_fixture["set_name"]

        plan = await client.query_explain(
            NAMESPACE,
            "$.age > 100 and $.age < 10",
            set_name=set_name,
        )
        assert plan.is_filtered_out

        stmt = Statement(NAMESPACE, set_name, [AGE_BIN])
        with pytest.raises(FilteredOut) as exc_info:
            await client.query_with_plan(
                stmt,
                PartitionFilter.all(),
                plan,
                policy=QueryPolicy(),
            )
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

    async def test_execute_mismatched_plan_namespace_raises(self, client, qsel_fixture):
        set_name = qsel_fixture["set_name"]

        plan = await client.query_explain(
            NAMESPACE,
            "$.age >= 14 and $.age <= 18",
            set_name=set_name,
        )
        stmt = Statement("not_a_namespace", set_name, [AGE_BIN])
        with pytest.raises(ValueError, match="does not match statement namespace"):
            await client.query_with_plan(
                stmt,
                PartitionFilter.all(),
                plan,
                policy=QueryPolicy(),
            )

    async def test_execute_mismatched_plan_set_raises(self, client, qsel_fixture):
        set_name = qsel_fixture["set_name"]

        plan = await client.query_explain(
            NAMESPACE,
            "$.age >= 14 and $.age <= 18",
            set_name=set_name,
        )
        stmt = Statement(NAMESPACE, "other_set", [AGE_BIN])
        with pytest.raises(ValueError, match="does not match statement set"):
            await client.query_with_plan(
                stmt,
                PartitionFilter.all(),
                plan,
                policy=QueryPolicy(),
            )

    async def test_execute_statement_with_filters_raises(self, client, qsel_fixture):
        set_name = qsel_fixture["set_name"]

        plan = await client.query_explain(
            NAMESPACE,
            "$.age >= 14 and $.age <= 18",
            set_name=set_name,
        )
        stmt = Statement(NAMESPACE, set_name, [AGE_BIN])
        stmt.filters = [Filter.range(AGE_BIN, 14, 18)]
        with pytest.raises(ValueError, match="plan supplies the index filter"):
            await client.query_with_plan(
                stmt,
                PartitionFilter.all(),
                plan,
                policy=QueryPolicy(),
            )


class TestQuerySelectionHintFlags(TestFixtureConnection):
    """Tier D: ``REQUIRE_INDEX`` and ``HARD_HINT`` on field 44 explain."""

    async def test_require_index_on_primary_index_plan_fails_explain(
        self, client, qsel_fixture,
    ):
        with pytest.raises(IndexNotFound) as exc_info:
            await explain_plan_async(
                client,
                "$.country == 'US'",
                hint=ExplainHint(require_index=True),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    async def test_require_index_with_soft_hint_selects_secondary_index(
        self, client, qsel_fixture,
    ):
        plan = await explain_plan_async(
            client,
            "$.age == 51",
            hint=ExplainHint(
                require_index=True,
                index_name=SCORE_INDEX_NAME,
            ),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == AGE_INDEX_NAME

    async def test_hard_hint_with_matching_index_selects_hinted_index(
        self, client, qsel_fixture,
    ):
        plan = await explain_plan_async(
            client,
            "$.age == 51",
            hint=ExplainHint(
                index_name=AGE_INDEX_NAME,
                hard_hint=True,
            ),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == AGE_INDEX_NAME

    async def test_require_index_and_hard_hint_selects_hinted_index(
        self, client, qsel_fixture,
    ):
        plan = await explain_plan_async(
            client,
            "$.age == 51",
            hint=ExplainHint(
                index_name=AGE_INDEX_NAME,
                require_index=True,
                hard_hint=True,
            ),
        )

        assert plan.index_name == AGE_INDEX_NAME

    async def test_hard_hint_with_wrong_index_fails_explain(self, client, qsel_fixture):
        with pytest.raises(IndexNotFound) as exc_info:
            await explain_plan_async(
                client,
                "$.age == 51",
                hint=ExplainHint(
                    index_name=BOGUS_INDEX_NAME,
                    hard_hint=True,
                ),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    async def test_hard_hint_without_index_name_fails_explain(self, client, qsel_fixture):
        with pytest.raises(ValueError, match="index name hint"):
            await client.query_explain(
                NAMESPACE,
                "$.age == 51",
                set_name=SET_NAME,
                explain_where_flags=QueryWhereFlags.EXPLAIN | QueryWhereFlags.HARD_HINT,
            )

    async def test_explain_flags_without_explain_bit_fails(self, client, qsel_fixture):
        with pytest.raises(ValueError, match="EXPLAIN"):
            await client.query_explain(
                NAMESPACE,
                "$.age == 51",
                set_name=SET_NAME,
                explain_where_flags=QueryWhereFlags.REQUIRE_INDEX,
            )

    async def test_bad_ael_fails_explain_with_parameter(self, client, qsel_fixture):
        with pytest.raises(InvalidRequest) as exc_info:
            await explain_plan_async(client, "$.age > 30 and")
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
