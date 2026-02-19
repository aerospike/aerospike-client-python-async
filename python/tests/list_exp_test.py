"""Tests for list write/remove FilterExpression methods."""

import pytest
from aerospike_async import (
    ExpOperation,
    FilterExpression as fe, WritePolicy, ReadPolicy, Key,
    ListPolicy, ListOrderType, ListWriteFlags, ListReturnType, CTX,
)
from aerospike_async.exceptions import ServerError, ResultCode
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
            await client.put(wp, key, {bin_a: list_a, bin_b: list_b})

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
            rec = await client.get(rp, key, [bin_a])
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
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None
            result = rec.bins[bin_a]
            assert len(result) == 5

        finally:
            try:
                await client.delete(wp, key)
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
            await client.operate(wp, key, [
                ExpOperation.write(bin_c, exp),
            ])

            result = await client.operate(wp, key, [
                ExpOperation.read("var", exp),
            ])

            results = result.bins["var"]
            assert len(results) == 4

        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass

    async def test_list_remove_by_value(self, client):
        """Test remove_by_value with NONE and INVERTED return types."""
        key = Key("test", "test", "list_exp_remove")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            await client.put(wp, key, {"nums": [1, 2, 3, 4]})

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
            rec = await client.get(rp, key)
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
            rec = await client.get(rp, key)
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
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key)
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass
