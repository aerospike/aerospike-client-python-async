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

Ported in basic form from the Rust ``query_selection`` integration suite.
Requires Aerospike Server >= 8.1.3; tests self-skip via
:func:`supports_query_selection`.
"""

import uuid

import pytest
from aerospike_async import (
    CollectionIndexType,
    Filter,
    IndexType,
    Key,
    PartitionFilter,
    QueryPolicy,
    QuerySelection,
    Statement,
    WritePolicy,
)
from fixtures import TestFixtureConnection

NAMESPACE = "test"
DATASET_SIZE = 50
AGE_BIN = "age"
SCORE_BIN = "score"
COUNTRY_BIN = "country"


async def _collect_int_bin(recordset, bin_name: str) -> list[int]:
    values = []
    async for record in recordset:
        values.append(record.bins[bin_name])
    values.sort()
    return values


@pytest.fixture
async def qsel_fixture(client, supports_query_selection, wait_for_index):
    if not supports_query_selection:
        pytest.skip("server does not support query selection (Node.version.supports_query_selection() is False)")

    set_name = f"qsel_{uuid.uuid4().hex[:10]}"
    age_index_name = f"{NAMESPACE}_{set_name}_age_idx"
    score_index_name = f"{NAMESPACE}_{set_name}_score_idx"
    wp = WritePolicy()

    for i in range(1, DATASET_SIZE + 1):
        country = "US" if i % 2 == 0 else "CA"
        key = Key(NAMESPACE, set_name, i)
        await client.put(
            key,
            {AGE_BIN: i, SCORE_BIN: i, COUNTRY_BIN: country},
            policy=wp,
        )

    for bin_name, index_name in (
        (AGE_BIN, age_index_name),
        (SCORE_BIN, score_index_name),
    ):
        await client.create_index(
            NAMESPACE,
            set_name,
            bin_name,
            index_name,
            IndexType.NUMERIC,
            cit=CollectionIndexType.DEFAULT,
        )

    await wait_for_index(
        client,
        NAMESPACE,
        set_name,
        Filter.range(AGE_BIN, 0, 100),
        bins=[AGE_BIN],
    )

    yield {
        "set_name": set_name,
        "age_index_name": age_index_name,
        "score_index_name": score_index_name,
    }


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
