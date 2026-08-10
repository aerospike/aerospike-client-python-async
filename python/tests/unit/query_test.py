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

import pytest

from aerospike_async import Order, OrderByFlags, OrderByType, Statement, Filter


class TestStatement:
    """Test Statement class functionality."""

    bin_name = "bin"

    def test_new(self):
        """Test creating a new Statement."""
        stmt = Statement(namespace="test", set_name="test", bins=["test_bin"])
        assert stmt.filters is None

    def test_set_filters(self):
        """Test setting filters on Statement."""
        stmt = Statement("test", "test", [self.bin_name])
        a_filter = Filter.range(self.bin_name, 1, 3)
        stmt.filters = [a_filter]
        assert isinstance(stmt.filters, list)

        stmt.filters = None
        assert stmt.filters is None


class TestStatementAggregateFunction:
    """Test Statement.set_aggregate_function method."""

    def test_set_aggregate_function_with_args(self):
        stmt = Statement("test", "test")
        stmt.set_aggregate_function("mypackage", "myfunction", ["arg1", 123])
        assert stmt is not None

    def test_set_aggregate_function_without_args(self):
        stmt = Statement("test", "test")
        stmt.set_aggregate_function("mypackage", "myfunction")
        assert stmt is not None

    def test_set_aggregate_function_various_arg_types(self):
        stmt = Statement("test", "test")
        stmt.set_aggregate_function("pkg", "func", ["string_arg"])
        stmt.set_aggregate_function("pkg", "func", [1, 2, 3])
        stmt.set_aggregate_function("pkg", "func", ["str", 123, 45.67])
        stmt.set_aggregate_function("pkg", "func", [[1, 2, 3]])
        stmt.set_aggregate_function("pkg", "func", [{"key": "value"}])
        assert stmt is not None


class TestStatementTopK:
    """Test Statement.set_order_by / set_top_k (Vector Phase 1 milestone 2, the
    Top-K "ORDER BY <bin> LIMIT k" developer API).

    `set_order_by`/`set_top_k` only build and store the clause on the Python
    side; the request-time validation described in the Top-K design doc (bad
    bin name, k out of range, orderBy missing from projection, incompatible
    with aggregate UDFs, etc.) lives in `aerospike_core::Statement::validate`
    and is only invoked when the statement is actually used in a query. These
    tests only cover what can be observed without a live server: that valid
    combinations build without raising, and that wrong Python-level argument
    types are rejected immediately.
    """

    def test_set_order_by_without_flags(self):
        stmt = Statement("test", "test", ["score"])
        stmt.set_order_by("score", OrderByType.INTEGER, Order.DESC)
        assert stmt is not None

    def test_set_order_by_with_flags(self):
        stmt = Statement("test", "test", ["name"])
        stmt.set_order_by("name", OrderByType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE)
        assert stmt is not None

    def test_set_order_by_all_types_and_directions(self):
        stmt = Statement("test", "test")
        for order_type in (
            OrderByType.INTEGER,
            OrderByType.DOUBLE,
            OrderByType.STRING,
            OrderByType.BYTES,
        ):
            for direction in (Order.ASC, Order.DESC):
                stmt.set_order_by("bin", order_type, direction)

    def test_set_top_k(self):
        stmt = Statement("test", "test", ["score"])
        stmt.set_order_by("score", OrderByType.INTEGER, Order.DESC)
        stmt.set_top_k(10)
        assert stmt is not None

    def test_set_order_by_rejects_bad_type_argument(self):
        stmt = Statement("test", "test")
        with pytest.raises(TypeError):
            stmt.set_order_by("score", "not-a-type", Order.DESC)

    def test_set_order_by_rejects_bad_direction_argument(self):
        stmt = Statement("test", "test")
        with pytest.raises(TypeError):
            stmt.set_order_by("score", OrderByType.INTEGER, "not-a-direction")

    def test_set_top_k_rejects_negative(self):
        stmt = Statement("test", "test")
        stmt.set_order_by("score", OrderByType.INTEGER, Order.DESC)
        with pytest.raises((TypeError, OverflowError)):
            stmt.set_top_k(-1)
