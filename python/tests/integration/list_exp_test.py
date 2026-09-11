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

"""Tests for list write/remove FilterExpression methods."""

import pytest
from aerospike_async import (
    ExpOperation,
    FilterExpression as fe, WritePolicy, ReadPolicy, Key,
    ListPolicy, ListOrderType, ListWriteFlags, ListReturnType, CTX,
)
from aerospike_async.exceptions import ServerError, ResultCode, FilteredOut
from fixtures import TestFixtureConnection


class TestListExp(TestFixtureConnection):
    """Test list expression write operations used as filter expressions."""

    async def test_modify_with_context(self, client):
        """Append items to a nested list via CTX and verify size via filter."""
        key = Key("test", "test", "list_exp_ctx")
        wp = WritePolicy()
        rp = ReadPolicy()
        bin_a = "binA"
        bin_b = "binB"

        try:
            list_sub = ["e", "d", "c", "b", "a"]
            list_a = ["a", "b", "c", "d", list_sub]
            list_b = ["x", "y", "z"]
            await client.put(key, {bin_a: list_a, bin_b: list_b}, policy=wp)

            ctx = [CTX.list_index(4)]

            # Build expression: append binB and "M" to binA[4], check size == 9
            exp = fe.eq(
                fe.list_size(
                    fe.list_append(
                        ListPolicy(ListOrderType.UNORDERED, ListWriteFlags.DEFAULT),
                        fe.string_val("M"),
                        fe.list_append_items(
                            ListPolicy(ListOrderType.UNORDERED, ListWriteFlags.DEFAULT),
                            fe.list_bin(bin_b),
                            fe.list_bin(bin_a),
                            ctx,
                        ),
                        ctx,
                    ),
                    ctx,
                ),
                fe.int_val(9),
            )

            rp.filter_expression = exp
            rec = await client.get(key, [bin_a], policy=rp)
            assert rec is not None
            result = rec.bins[bin_a]
            assert len(result) == 5

            # Same test with local list values instead of bins
            exp2 = fe.eq(
                fe.list_size(
                    fe.list_append(
                        ListPolicy(ListOrderType.UNORDERED, ListWriteFlags.DEFAULT),
                        fe.string_val("M"),
                        fe.list_append_items(
                            ListPolicy(ListOrderType.UNORDERED, ListWriteFlags.DEFAULT),
                            fe.list_val(list_b),
                            fe.list_bin(bin_a),
                            ctx,
                        ),
                        ctx,
                    ),
                    ctx,
                ),
                fe.int_val(9),
            )

            rp.filter_expression = exp2
            rec = await client.get(key, [bin_a], policy=rp)
            assert rec is not None
            result = rec.bins[bin_a]
            assert len(result) == 5

        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass

    async def test_exp_returns_list(self, client):
        """Write a list via ExpOperation and read it back."""
        key = Key("test", "test", "list_exp_ret")
        wp = WritePolicy()
        bin_c = "binC"

        try:
            values = ["a", "b", "c", "d"]
            exp = fe.list_val(values)

            # Write the list via expression, then read it back
            await client.operate(
                key,
                [
                ExpOperation.write(bin_c, exp),
            ],
                policy=wp,
            )

            result = await client.operate(
                key,
                [
                ExpOperation.read("var", exp),
            ],
                policy=wp,
            )

            results = result.bins["var"]
            assert len(results) == 4

        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass

    async def test_list_remove_by_value(self, client):
        """Test remove_by_value with NONE and INVERTED return types."""
        key = Key("test", "test", "list_exp_remove")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            await client.put(key, {"nums": [1, 2, 3, 4]}, policy=wp)

            # NONE: remove value 3 -> [1, 2, 4], size == 3
            exp_none = fe.eq(
                fe.list_size(
                    fe.list_remove_by_value(
                        ListReturnType.NONE,
                        fe.int_val(3),
                        fe.list_bin("nums"),
                        [],
                    ),
                    [],
                ),
                fe.int_val(3),
            )
            rp.filter_expression = exp_none
            rec = await client.get(key, policy=rp)
            assert rec is not None

            # INVERTED: remove everything except value 3 -> [3], size == 1
            exp_inv = fe.eq(
                fe.list_size(
                    fe.list_remove_by_value(
                        ListReturnType.INVERTED,
                        fe.int_val(3),
                        fe.list_bin("nums"),
                        [],
                    ),
                    [],
                ),
                fe.int_val(1),
            )
            rp.filter_expression = exp_inv
            rec = await client.get(key, policy=rp)
            assert rec is not None

            # Negative: INVERTED with size == 3 should fail (actual size is 1)
            exp_neg = fe.eq(
                fe.list_size(
                    fe.list_remove_by_value(
                        ListReturnType.INVERTED,
                        fe.int_val(3),
                        fe.list_bin("nums"),
                        [],
                    ),
                    [],
                ),
                fe.int_val(3),
            )
            rp.filter_expression = exp_neg
            with pytest.raises(FilteredOut) as exc_info:
                await client.get(key, policy=rp)
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass

    async def test_list_join_read_with_and_without_separator(
        self, client, supports_string_operations
    ):
        """list_join / list_join_by_separator read expressions (server >= 8.2.0)."""
        if not supports_string_operations:
            pytest.skip("list join requires server >= 8.2.0")
        key = Key("test", "test", "list_exp_join")
        wp = WritePolicy()

        try:
            await client.put(key, {"strs": ["one", "two", "three"]}, policy=wp)

            rec = await client.operate(
                key,
                [ExpOperation.read("var", fe.list_join(fe.list_bin("strs"), []))],
                policy=wp,
            )
            assert rec.bins["var"] == "onetwothree"

            rec = await client.operate(
                key,
                [
                ExpOperation.read(
                    "var",
                    fe.list_join_by_separator(
                        fe.string_val("|"), fe.list_bin("strs"), [],
                    ),
                ),
            ],
                policy=wp,
            )
            assert rec.bins["var"] == "one|two|three"

        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass

    async def test_list_join_as_filter_expression(self, client, supports_string_operations):
        """Join used as a record filter: match passes, mismatch is filtered out."""
        if not supports_string_operations:
            pytest.skip("list join requires server >= 8.2.0")
        key = Key("test", "test", "list_exp_join_filter")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            await client.put(key, {"strs": ["a", "b"]}, policy=wp)

            rp.filter_expression = fe.eq(
                fe.list_join_by_separator(fe.string_val("-"), fe.list_bin("strs"), []),
                fe.string_val("a-b"),
            )
            rec = await client.get(key, policy=rp)
            assert rec is not None

            rp.filter_expression = fe.eq(
                fe.list_join(fe.list_bin("strs"), []),
                fe.string_val("mismatch"),
            )
            with pytest.raises(FilteredOut):
                await client.get(key, policy=rp)

        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass
