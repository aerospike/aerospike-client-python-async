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

"""Shared helpers for query-selection integration tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from aerospike_async import QueryWhereFlags

NS = "test"

# QuerySelectionHintFlagsTest fixture (Java qselhint set)
HINT_SET_NAME = "qselhint"
HINT_INDEX_NAME = "qselhint_age_idx"
HINT_SCORE_INDEX_NAME = "qselhint_score_idx"
HINT_BOGUS_INDEX_NAME = "qselhint_missing_idx"
BIN_AGE = "age"
BIN_SCORE = "score"
BIN_COUNTRY = "country"
HINT_KEY_PREFIX = "qselhintkey"


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
    set_name: str = HINT_SET_NAME,
    hint: Optional[ExplainHint] = None,
):
    """Run phase-1 explain (mirrors Java ``IndexProbePlanner.plan``)."""
    index_name_hint = hint.index_name if hint is not None else None
    return await client.query_explain(
        NS,
        where,
        set_name=set_name,
        index_name_hint=index_name_hint,
        explain_where_flags=explain_where_flags(hint),
    )
