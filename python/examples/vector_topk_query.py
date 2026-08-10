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
Vector bin + Top-K ("ORDER BY <bin> LIMIT k") hybrid search example.

WORK IN PROGRESS: Top-K's wire encode is capability-gated in the underlying
Rust client and has no assigned minimum server version yet, so any query
below that sets ``order_by``/``top_k`` will currently fail fast client-side
with a ValueError, regardless of the server it targets. Kept here so the
Python-level API surface (which is fully implemented and unit-tested) is
documented and easy to try again once the server-side capability lands.
"""

import asyncio
import os

from aerospike_async import (
    ClientPolicy,
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


async def vector_topk_example():
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client = await new_client(policy=ClientPolicy(), seeds=host)
    print("Connected to Aerospike")

    namespace = "test"
    set_name = "products"

    try:
        # A vector bin stores a fixed-dimension numeric embedding directly,
        # so it round-trips through put/get like any other bin type.
        await client.put(
            Key(namespace, set_name, "sku-1"),
            {"name": "wireless mouse", "embedding": Vector([0.12, 0.98, 0.44, 0.05])},
        )

        query_vector = Vector([0.10, 0.95, 0.40, 0.02])

        # Pure Top-K: rank every record in the set by similarity to
        # `query_vector`, keep the closest 10. `similarity` here is a
        # *read-op projection* bin (see `set_operations`), not a physical
        # bin — the order-by clause's bin name must match a bin present in
        # the record the server returns, whether physical or projected.
        statement = Statement(namespace, set_name)
        statement.set_order_by("similarity", OrderByType.DOUBLE, Order.DESC)
        statement.set_top_k(10)

        recordset = await client.query(
            statement=statement,
            partition_filter=PartitionFilter.all(),
            policy=QueryPolicy(),
        )

        async for record in recordset:
            print(record.bins)

        # Hybrid search: filter first (e.g. an equality/range secondary-index
        # filter or a `QueryPolicy.filter_exp` predicate on category/price/
        # in-stock), *then* Top-K-rank only the records that pass. Top-K
        # composes with expression filters the same way non-vector queries
        # do — nothing extra needed here beyond what's already possible with
        # `QueryPolicy.filter_exp` / `Statement.filters`.
        hybrid_policy = QueryPolicy()
        hybrid_policy.filter_expression = fe.eq(fe.string_bin("category"), fe.string_val("electronics"))

        hybrid_statement = Statement(namespace, set_name)
        hybrid_statement.set_order_by("similarity", OrderByType.DOUBLE, Order.DESC)
        hybrid_statement.set_top_k(10)

        recordset = await client.query(
            statement=hybrid_statement,
            partition_filter=PartitionFilter.all(),
            policy=hybrid_policy,
        )

        async for record in recordset:
            print(record.bins)

    except Exception as e:
        print(f"Query failed (expected until the server-side Top-K capability gate is set): {e}")

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(vector_topk_example())
