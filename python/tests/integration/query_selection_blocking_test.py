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

"""Sync smoke tests for ``query_explain_blocking`` / ``query_with_plan_blocking``."""

import time
import uuid

import pytest

from aerospike_async import (
    ClientPolicy,
    CollectionIndexType,
    IndexType,
    Key,
    PartitionFilter,
    QueryPolicy,
    QuerySelection,
    Statement,
    WritePolicy,
    new_client_blocking,
)

NAMESPACE = "test"
DATASET_SIZE = 50
AGE_BIN = "age"
SCORE_BIN = "score"
COUNTRY_BIN = "country"


def _connect_blocking(aerospike_host, use_services_alternate):
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    return new_client_blocking(cp, aerospike_host)


def _supports_query_selection(aerospike_host, use_services_alternate) -> bool:
    client = _connect_blocking(aerospike_host, use_services_alternate)
    try:
        nodes = client.nodes_blocking()
        if not nodes:
            return False
        return all(n.version.supports_query_selection() for n in nodes)
    finally:
        client.close_blocking()


def _wait_for_index_blocking(client, ns, set_name, sindex_filter, *, bins=None,
                             timeout=5.0, interval=0.25):
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            stmt = Statement(ns, set_name, bins or [])
            stmt.filters = [sindex_filter]
            recordset = client.query_blocking(
                stmt,
                PartitionFilter.all(),
                policy=QueryPolicy(),
            )
            for _ in recordset:
                break
            return
        except Exception as exc:
            if "IndexNotReadable" not in str(exc):
                raise
            last_err = exc
            time.sleep(interval)
    raise last_err  # type: ignore[misc]


def _collect_int_bin_blocking(recordset, bin_name: str) -> list[int]:
    values = []
    for record in recordset:
        values.append(record.bins[bin_name])
    values.sort()
    return values


@pytest.fixture
def qsel_blocking_fixture(aerospike_host, use_services_alternate):
    if not _supports_query_selection(aerospike_host, use_services_alternate):
        pytest.skip(
            "cluster lacks query selection "
            "(Node.version.supports_query_selection() is False on one or more nodes)"
        )

    client = _connect_blocking(aerospike_host, use_services_alternate)
    set_name = f"qsel_blk_{uuid.uuid4().hex[:10]}"
    age_index_name = f"{NAMESPACE}_{set_name}_age_idx"
    score_index_name = f"{NAMESPACE}_{set_name}_score_idx"
    wp = WritePolicy()

    try:
        for i in range(1, DATASET_SIZE + 1):
            country = "US" if i % 2 == 0 else "CA"
            key = Key(NAMESPACE, set_name, i)
            client.put_blocking(
                key,
                {AGE_BIN: i, SCORE_BIN: i, COUNTRY_BIN: country},
                policy=wp,
            )

        from aerospike_async import Filter

        for bin_name, index_name in (
            (AGE_BIN, age_index_name),
            (SCORE_BIN, score_index_name),
        ):
            client.create_index_blocking(
                NAMESPACE,
                set_name,
                bin_name,
                index_name,
                IndexType.NUMERIC,
                cit=CollectionIndexType.DEFAULT,
            )

        _wait_for_index_blocking(
            client,
            NAMESPACE,
            set_name,
            Filter.range(AGE_BIN, 0, 100),
            bins=[AGE_BIN],
        )

        yield {
            "client": client,
            "set_name": set_name,
            "age_index_name": age_index_name,
        }
    finally:
        client.close_blocking()


def test_blocking_explain_selects_secondary_index(qsel_blocking_fixture):
    client = qsel_blocking_fixture["client"]
    set_name = qsel_blocking_fixture["set_name"]
    age_index_name = qsel_blocking_fixture["age_index_name"]

    plan = client.query_explain_blocking(
        NAMESPACE,
        "$.age >= 14 and $.age <= 18",
        set_name=set_name,
    )

    assert plan.selection == QuerySelection.SECONDARY_INDEX
    assert plan.is_secondary_index
    assert plan.index_name == age_index_name
    assert isinstance(plan.ael, str) and len(plan.ael) > 0


def test_blocking_execute_returns_matching_records(qsel_blocking_fixture):
    client = qsel_blocking_fixture["client"]
    set_name = qsel_blocking_fixture["set_name"]

    plan = client.query_explain_blocking(
        NAMESPACE,
        "$.age >= 14 and $.age <= 18",
        set_name=set_name,
    )
    stmt = Statement(NAMESPACE, set_name, [AGE_BIN])
    recordset = client.query_with_plan_blocking(
        stmt,
        PartitionFilter.all(),
        plan,
        policy=QueryPolicy(),
    )

    ages = _collect_int_bin_blocking(recordset, AGE_BIN)
    assert ages == [14, 15, 16, 17, 18]
