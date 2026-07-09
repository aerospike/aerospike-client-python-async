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

"""Unit tests for server query selection bindings."""

from aerospike_async import Client, QueryPlan, QuerySelection, QueryWhereFlags, Version


class TestQuerySelectionExports:
    def test_query_selection_enum_values(self):
        assert QuerySelection.PRIMARY_INDEX is not None
        assert QuerySelection.SECONDARY_INDEX is not None
        assert QuerySelection.FILTERED_OUT is not None
        assert QuerySelection.PRIMARY_INDEX != QuerySelection.SECONDARY_INDEX
        assert QuerySelection.SECONDARY_INDEX != QuerySelection.FILTERED_OUT
        assert QuerySelection.PRIMARY_INDEX != QuerySelection.FILTERED_OUT

    def test_query_plan_api_on_client(self):
        assert hasattr(Client, "query_explain")
        assert hasattr(Client, "query_with_plan")
        assert hasattr(Client, "query_explain_blocking")
        assert hasattr(Client, "query_with_plan_blocking")

    def test_version_supports_query_selection_method(self):
        assert hasattr(Version, "supports_query_selection")

    def test_query_where_flags_exported(self):
        assert QueryWhereFlags.EXPLAIN == 2
        assert QueryWhereFlags.REQUIRE_INDEX == 4
        assert QueryWhereFlags.HARD_HINT == 8
        combined = (
            QueryWhereFlags.EXPLAIN
            | QueryWhereFlags.REQUIRE_INDEX
            | QueryWhereFlags.HARD_HINT
        )
        assert combined == 14

    def test_query_plan_type_exported(self):
        assert QueryPlan.__name__ == "QueryPlan"
