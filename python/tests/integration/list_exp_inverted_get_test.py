# Copyright 2026 Aerospike, Inc.
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

"""Inverted get on list expressions must return the complement selection.

These tests pin the contract that `ListReturnType.VALUE | ListReturnType.INVERTED`
reaches the wire intact (full 0x10000 bit) on every `list_get_by_*` expression
builder. The cross-client matrix (C, Java, server AEL parser) honors the bit on
identical inputs; lists in this binding currently drop it while maps preserve
it. Each test below has a maps-side mirror in the matching map test module to
demonstrate the asymmetry intentionally — if both sides ever fail or both ever
pass, the asymmetry has changed and the From-impl audit needs re-running.
"""

import pytest
from aerospike_async import (
    FilterExpression as fe, WritePolicy, ReadPolicy, Key,
    ListReturnType,
)
from aerospike_async.exceptions import ServerError, FilteredOut
from fixtures import TestFixtureConnection


class TestListGetInvertedReturn(TestFixtureConnection):
    """Inverted `list_get_by_*` expressions honor the INVERTED bit on the wire."""

    async def test_get_by_value_list_inverted_returns_complement(self, client):
        """`get_by_value_list([1,2,4,5,6,7,8])` with VALUE|INVERTED on
        l=[1..8] must equal [3] (the complement), not the listed values."""
        key = Key("test", "inverted_get", "value_list")
        wp = WritePolicy()
        rp = ReadPolicy()
        bin_name = "l"
        try:
            await client.put(key, {bin_name: [1, 2, 3, 4, 5, 6, 7, 8]}, policy=wp)

            # Inverted get over [1,2,4,5,6,7,8] on l=[1..8] selects [3].
            # If the INVERTED bit (0x10000) is dropped, the get returns
            # [1,2,4,5,6,7,8] instead and the filter no longer matches.
            rt = ListReturnType.VALUE | ListReturnType.INVERTED
            exp = fe.eq(
                fe.list_get_by_value_list(
                    rt,
                    fe.list_val([1, 2, 4, 5, 6, 7, 8]),
                    fe.list_bin(bin_name),
                    [],
                ),
                fe.list_val([3]),
            )
            rp.filter_expression = exp

            rec = await client.get(key, [bin_name], policy=rp)
            assert rec is not None
            assert rec.bins[bin_name] == [1, 2, 3, 4, 5, 6, 7, 8]
        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass

    async def test_get_by_value_range_inverted_returns_outside(self, client):
        """`get_by_value_range(1, 6)` with VALUE|INVERTED on l=[1..8] must
        equal [6,7,8] (the outside-range complement)."""
        key = Key("test", "inverted_get", "value_range")
        wp = WritePolicy()
        rp = ReadPolicy()
        bin_name = "l"
        try:
            await client.put(key, {bin_name: [1, 2, 3, 4, 5, 6, 7, 8]}, policy=wp)

            rt = ListReturnType.VALUE | ListReturnType.INVERTED
            exp = fe.eq(
                fe.list_get_by_value_range(
                    rt,
                    fe.int_val(1),
                    fe.int_val(6),
                    fe.list_bin(bin_name),
                    [],
                ),
                fe.list_val([6, 7, 8]),
            )
            rp.filter_expression = exp

            rec = await client.get(key, [bin_name], policy=rp)
            assert rec is not None
            assert rec.bins[bin_name] == [1, 2, 3, 4, 5, 6, 7, 8]
        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass

    async def test_get_by_index_range_count_inverted_returns_tail(self, client):
        """`get_by_index_range_count(0, 5)` with VALUE|INVERTED on l=[1..8]
        must equal [6,7,8] (the tail past the selected window)."""
        key = Key("test", "inverted_get", "index_range_count")
        wp = WritePolicy()
        rp = ReadPolicy()
        bin_name = "l"
        try:
            await client.put(key, {bin_name: [1, 2, 3, 4, 5, 6, 7, 8]}, policy=wp)

            rt = ListReturnType.VALUE | ListReturnType.INVERTED
            exp = fe.eq(
                fe.list_get_by_index_range_count(
                    rt,
                    fe.int_val(0),
                    fe.int_val(5),
                    fe.list_bin(bin_name),
                    [],
                ),
                fe.list_val([6, 7, 8]),
            )
            rp.filter_expression = exp

            rec = await client.get(key, [bin_name], policy=rp)
            assert rec is not None
        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass

    async def test_non_inverted_baseline_still_works(self, client):
        """Plain VALUE (no INVERTED) on `get_by_value_list` must still
        return the listed values — guards against a fix that flips the
        non-inverted path while repairing the inverted path."""
        key = Key("test", "inverted_get", "baseline")
        wp = WritePolicy()
        rp = ReadPolicy()
        bin_name = "l"
        try:
            await client.put(key, {bin_name: [1, 2, 3, 4, 5, 6, 7, 8]}, policy=wp)

            exp = fe.eq(
                fe.list_get_by_value_list(
                    ListReturnType.VALUE,
                    fe.list_val([1, 2, 4, 5, 6, 7, 8]),
                    fe.list_bin(bin_name),
                    [],
                ),
                fe.list_val([1, 2, 4, 5, 6, 7, 8]),
            )
            rp.filter_expression = exp

            rec = await client.get(key, [bin_name], policy=rp)
            assert rec is not None
        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass

    async def test_inverted_get_does_not_match_non_inverted_set(self, client):
        """Decisive negative: VALUE|INVERTED with `get_by_value_list([1,2,4..8])`
        on l=[1..8] MUST NOT equal [1,2,4,5,6,7,8]. If it does, the INVERTED
        bit was dropped and the inverted get devolved to a plain VALUE get."""
        key = Key("test", "inverted_get", "negative")
        wp = WritePolicy()
        rp = ReadPolicy()
        bin_name = "l"
        try:
            await client.put(key, {bin_name: [1, 2, 3, 4, 5, 6, 7, 8]}, policy=wp)

            rt = ListReturnType.VALUE | ListReturnType.INVERTED
            exp = fe.eq(
                fe.list_get_by_value_list(
                    rt,
                    fe.list_val([1, 2, 4, 5, 6, 7, 8]),
                    fe.list_bin(bin_name),
                    [],
                ),
                fe.list_val([1, 2, 4, 5, 6, 7, 8]),
            )
            rp.filter_expression = exp

            # The expectation here is a server-side FilteredOut: under a
            # correct binding the inverted get returns [3], which does NOT
            # equal [1,2,4,5,6,7,8], so the record is filtered out and the
            # client raises. Under the bug, the get returns [1,2,4,5,6,7,8]
            # and this assertion silently passes — making this the most
            # decisive cell in the matrix.
            with pytest.raises(FilteredOut):
                await client.get(key, [bin_name], policy=rp)
        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass
