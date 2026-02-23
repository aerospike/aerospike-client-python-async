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

from aerospike_async import Statement, Filter


class TestStatement:
    """Test Statement class functionality."""

    bin_name = "bin"

    def test_new(self):
        """Test creating a new Statement."""
        stmt = Statement(namespace="test", set_name="test", bins=["test_bin"])
        assert stmt.filters is None
        assert stmt.index_name is None

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
