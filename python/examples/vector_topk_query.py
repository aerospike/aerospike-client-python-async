#!/usr/bin/env python3
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

"""
Vector bin + Top-K ("ORDER BY <bin> LIMIT k") hybrid search example, using
cosine similarity as the distance metric.

Top-K uses client-side reduction. Server pushdown is not yet encoded by this
client. Distance expressions require VECTOR support.
"""

import asyncio
import os

from aerospike_async import (
    ClientPolicy,
    ExpOperation,
    Key,
    Order,
    OrderByType,
    PartitionFilter,
    QueryPolicy,
    Statement,
    Vector,
    new_client,
)
from aerospike_async import FilterExpression as fe


def build_topk_statement(namespace, set_name, bin_name, query_vector, k, filter_expression=None):
    """Build a Statement that ranks records by cosine similarity to
    `query_vector` and keeps the top `k`. Larger cosine similarity means
    "more similar" — see `FilterExpression.cosine_similarity`'s docstring.

    The similarity expression is projected into a "similarity" output bin
    via `ExpOperation.read` — the order-by clause's bin name must match a
    bin present in the record the server returns (a physical bin, or, as
    here, one produced by a read-op projection).
    """
    similarity_expr = fe.cosine_similarity(query_vector, fe.vector_bin(bin_name))

    statement = Statement(namespace, set_name)
    statement.set_operations([ExpOperation.read("similarity", similarity_expr)])
    statement.set_order_by("similarity", OrderByType.DOUBLE, Order.DESC)
    statement.set_top_k(k)
    return statement


async def vector_topk_example(client):
    """Rank every record in the set by cosine similarity to a query vector,
    keeping the top 10."""
    query_vector = Vector([0.10, 0.95, 0.40, 0.02])
    statement = build_topk_statement("test", "products", "embedding", query_vector, k=10)

    recordset = await client.query(
        statement=statement,
        partition_filter=PartitionFilter.all(),
        policy=QueryPolicy(),
    )
    async for record in recordset:
        print(record.bins)  # includes the projected "similarity" bin


async def hybrid_search_example(client):
    """Filter to a category first (e.g. an equality/range secondary-index
    filter, or, as here, a `QueryPolicy.filter_expression` predicate), then
    Top-K-rank only the records that pass. Top-K composes with expression
    filters the same way non-vector queries do — nothing extra needed here."""
    query_vector = Vector([0.10, 0.95, 0.40, 0.02])
    statement = build_topk_statement("test", "products", "embedding", query_vector, k=10)

    policy = QueryPolicy()
    policy.filter_expression = fe.eq(fe.string_bin("category"), fe.string_val("electronics"))

    recordset = await client.query(
        statement=statement,
        partition_filter=PartitionFilter.all(),
        policy=policy,
    )
    async for record in recordset:
        print(record.bins)


async def main():
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client = await new_client(policy=ClientPolicy(), seeds=host)
    print("Connected to Aerospike")

    try:
        # A vector bin stores a fixed-dimension numeric embedding directly,
        # so it round-trips through put/get like any other bin type.
        await client.put(
            Key("test", "products", "sku-1"),
            {"name": "wireless mouse", "embedding": Vector([0.12, 0.98, 0.44, 0.05])},
        )

        print("--- Top-K by cosine_similarity ---")
        await vector_topk_example(client)

        print("\n--- Hybrid search (category filter + Top-K by cosine_similarity) ---")
        await hybrid_search_example(client)

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
