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

"""Tests for map FilterExpression methods."""

import pytest
from aerospike_async import (
    ExpOperation,
    FilterExpression as fe, WritePolicy, ReadPolicy, Key,
    MapPolicy, MapOrder, MapWriteMode, MapOperation, MapReturnType,
)
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureConnection


class TestMapExp(TestFixtureConnection):
    """Test map expression operations used as filter expressions."""

    async def test_sorted_map_equality(self, client):
        """Direct map equality filter on a KEY_ORDERED map."""
        key = Key("test", "test", "map_exp_sme")
        wp = WritePolicy()
        rp = ReadPolicy()
        bin_name = "m"

        try:
            map_val = {"key1": "e", "key2": "d", "key3": "c", "key4": "b", "key5": "a"}
            mp = MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE)
            await client.operate(
                key,
                [
                MapOperation.put_items(bin_name, list(map_val.items()), mp),
            ],
                policy=wp,
            )

            rp.filter_expression = fe.eq(fe.map_bin(bin_name), fe.map_val(map_val))
            rec = await client.get(key, [bin_name], policy=rp)
            assert rec is not None
            assert isinstance(rec.bins[bin_name], dict)

        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass

    async def test_inverted_map_exp(self, client):
        """Remove entries where value != 2 using INVERTED, read result via ExpOperation."""
        key = Key("test", "test", "map_exp_ime")
        wp = WritePolicy()
        bin_name = "m"

        try:
            map_val = {"a": 1, "b": 2, "c": 2, "d": 3}
            await client.put(key, {bin_name: map_val}, policy=wp)

            exp = fe.map_remove_by_value(
                MapReturnType.INVERTED,
                fe.int_val(2),
                fe.map_bin(bin_name),
                [],
            )

            result = await client.operate(
                key,
                [
                ExpOperation.read(bin_name, exp),
            ],
                policy=wp,
            )
            assert result is not None
            m = result.bins[bin_name]
            assert isinstance(m, dict)
            assert len(m) == 2
            assert m["b"] == 2
            assert m["c"] == 2

        finally:
            try:
                await client.delete(key, policy=wp)
            except ServerError:
                pass
