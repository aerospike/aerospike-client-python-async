"""Tests for bitwise FilterExpression methods."""

import pytest
from aerospike_async import (
    FilterExpression as fe, WritePolicy, ReadPolicy, Key,
    BitPolicy, BitwiseResizeFlags, BitwiseOverflowActions, BitwiseWriteFlags,
)
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureConnection


class TestBitExpRead(TestFixtureConnection):
    """Test bitwise read expression methods used as filter expressions."""

    async def test_read_ops(self, client):
        """Test bit_get, bit_count, bit_lscan, bit_rscan, bit_get_int as filters."""
        key = Key("test", "test", "bit_exp_read")
        wp = WritePolicy()
        rp = ReadPolicy()
        bin_a = "A"
        blob = bytes([0x01, 0x42, 0x03, 0x04, 0x05])

        try:
            await client.put(wp, key, {bin_a: blob})
            bb = fe.blob_bin(bin_a)

            # --- bit_get ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(fe.int_val(16), fe.int_val(8), bb),
                fe.bit_get(fe.int_val(16), fe.int_val(8), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive: byte at offset 16 == 0x03
            rp.filter_expression = fe.eq(
                fe.bit_get(fe.int_val(16), fe.int_val(8), bb),
                fe.blob_val(bytes([0x03])),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- bit_count ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_count(fe.int_val(16), fe.int_val(8), bb),
                fe.bit_count(fe.int_val(32), fe.int_val(8), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_count(fe.int_val(16), fe.int_val(8), bb),
                fe.bit_count(fe.int_val(32), fe.int_val(8), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- bit_lscan ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_lscan(fe.int_val(32), fe.int_val(8), fe.bool_val(True), bb),
                fe.int_val(5),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Nested: lscan on bit_get result
            rp.filter_expression = fe.eq(
                fe.bit_lscan(
                    fe.int_val(0), fe.int_val(8), fe.bool_val(True),
                    fe.bit_get(fe.int_val(32), fe.int_val(8), bb),
                ),
                fe.int_val(5),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # Positive: direct lscan
            rp.filter_expression = fe.eq(
                fe.bit_lscan(fe.int_val(32), fe.int_val(8), fe.bool_val(True), bb),
                fe.int_val(5),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- bit_rscan ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_rscan(fe.int_val(32), fe.int_val(8), fe.bool_val(True), bb),
                fe.int_val(7),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_rscan(fe.int_val(32), fe.int_val(8), fe.bool_val(True), bb),
                fe.int_val(7),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- bit_get_int ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get_int(fe.int_val(32), fe.int_val(8), True, bb),
                fe.int_val(0x05),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get_int(fe.int_val(32), fe.int_val(8), True, bb),
                fe.int_val(0x05),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass


class TestBitExpModify(TestFixtureConnection):
    """Test bitwise modify expression methods used as filter expressions."""

    async def test_modify_ops(self, client):
        """Test bit modify expressions: resize, insert, remove, set, or, xor, and, not,
        lshift, rshift, add, subtract, set_int."""
        key = Key("test", "test", "bit_exp_mod")
        wp = WritePolicy()
        rp = ReadPolicy()
        bin_a = "A"
        blob = bytes([0x01, 0x42, 0x03, 0x04, 0x05])
        bp = BitPolicy(BitwiseWriteFlags.DEFAULT)

        try:
            await client.put(wp, key, {bin_a: blob})
            bb = fe.blob_bin(bin_a)

            # --- resize ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_resize(bp, fe.int_val(6), BitwiseResizeFlags.DEFAULT, bb),
                fe.bit_resize(bp, fe.int_val(6), BitwiseResizeFlags.DEFAULT, bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_resize(bp, fe.int_val(6), BitwiseResizeFlags.DEFAULT, bb),
                fe.bit_resize(bp, fe.int_val(6), BitwiseResizeFlags.DEFAULT, bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- insert ---
            insert_bytes = bytes([0xFF])
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get_int(
                    fe.int_val(8), fe.int_val(8), False,
                    fe.bit_insert(bp, fe.int_val(1), fe.blob_val(insert_bytes), bb),
                ),
                fe.int_val(0xFF),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get_int(
                    fe.int_val(8), fe.int_val(8), False,
                    fe.bit_insert(bp, fe.int_val(1), fe.blob_val(insert_bytes), bb),
                ),
                fe.int_val(0xFF),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- remove ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get_int(
                    fe.int_val(0), fe.int_val(8), False,
                    fe.bit_remove(bp, fe.int_val(0), fe.int_val(1), bb),
                ),
                fe.int_val(0x42),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get_int(
                    fe.int_val(0), fe.int_val(8), False,
                    fe.bit_remove(bp, fe.int_val(0), fe.int_val(1), bb),
                ),
                fe.int_val(0x42),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- set ---
            set_bytes = bytes([0x80])
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(
                    fe.int_val(24), fe.int_val(8),
                    fe.bit_set(bp, fe.int_val(31), fe.int_val(1), fe.blob_val(set_bytes), bb),
                ),
                fe.bit_get(fe.int_val(32), fe.int_val(8), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get(
                    fe.int_val(24), fe.int_val(8),
                    fe.bit_set(bp, fe.int_val(31), fe.int_val(1), fe.blob_val(set_bytes), bb),
                ),
                fe.bit_get(fe.int_val(32), fe.int_val(8), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- or ---
            or_bytes = bytes([0x01])
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(
                    fe.int_val(24), fe.int_val(8),
                    fe.bit_or(bp, fe.int_val(24), fe.int_val(8), fe.blob_val(or_bytes), bb),
                ),
                fe.bit_get(fe.int_val(32), fe.int_val(8), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get(
                    fe.int_val(24), fe.int_val(8),
                    fe.bit_or(bp, fe.int_val(24), fe.int_val(8), fe.blob_val(or_bytes), bb),
                ),
                fe.bit_get(fe.int_val(32), fe.int_val(8), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- xor ---
            xor_bytes = bytes([0x02])
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(
                    fe.int_val(0), fe.int_val(8),
                    fe.bit_xor(bp, fe.int_val(0), fe.int_val(8), fe.blob_val(xor_bytes), bb),
                ),
                fe.bit_get(fe.int_val(16), fe.int_val(8), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get(
                    fe.int_val(0), fe.int_val(8),
                    fe.bit_xor(bp, fe.int_val(0), fe.int_val(8), fe.blob_val(xor_bytes), bb),
                ),
                fe.bit_get(fe.int_val(16), fe.int_val(8), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- and ---
            and_bytes = bytes([0x01])
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(
                    fe.int_val(0), fe.int_val(8),
                    fe.bit_and(bp, fe.int_val(16), fe.int_val(8), fe.blob_val(and_bytes), bb),
                ),
                fe.bit_get(fe.int_val(0), fe.int_val(8), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get(
                    fe.int_val(0), fe.int_val(8),
                    fe.bit_and(bp, fe.int_val(16), fe.int_val(8), fe.blob_val(and_bytes), bb),
                ),
                fe.bit_get(fe.int_val(0), fe.int_val(8), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- not ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(
                    fe.int_val(0), fe.int_val(8),
                    fe.bit_not(bp, fe.int_val(6), fe.int_val(1), bb),
                ),
                fe.bit_get(fe.int_val(16), fe.int_val(8), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get(
                    fe.int_val(0), fe.int_val(8),
                    fe.bit_not(bp, fe.int_val(6), fe.int_val(1), bb),
                ),
                fe.bit_get(fe.int_val(16), fe.int_val(8), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- lshift ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(
                    fe.int_val(0), fe.int_val(6),
                    fe.bit_lshift(bp, fe.int_val(0), fe.int_val(8), fe.int_val(2), bb),
                ),
                fe.bit_get(fe.int_val(2), fe.int_val(6), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get(
                    fe.int_val(0), fe.int_val(6),
                    fe.bit_lshift(bp, fe.int_val(0), fe.int_val(8), fe.int_val(2), bb),
                ),
                fe.bit_get(fe.int_val(2), fe.int_val(6), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- rshift ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(
                    fe.int_val(26), fe.int_val(6),
                    fe.bit_rshift(bp, fe.int_val(24), fe.int_val(8), fe.int_val(2), bb),
                ),
                fe.bit_get(fe.int_val(24), fe.int_val(6), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get(
                    fe.int_val(26), fe.int_val(6),
                    fe.bit_rshift(bp, fe.int_val(24), fe.int_val(8), fe.int_val(2), bb),
                ),
                fe.bit_get(fe.int_val(24), fe.int_val(6), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- add ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(
                    fe.int_val(16), fe.int_val(8),
                    fe.bit_add(bp, fe.int_val(16), fe.int_val(8), fe.int_val(1), False, BitwiseOverflowActions.FAIL, bb),
                ),
                fe.bit_get(fe.int_val(24), fe.int_val(8), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get(
                    fe.int_val(16), fe.int_val(8),
                    fe.bit_add(bp, fe.int_val(16), fe.int_val(8), fe.int_val(1), False, BitwiseOverflowActions.FAIL, bb),
                ),
                fe.bit_get(fe.int_val(24), fe.int_val(8), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- subtract ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(
                    fe.int_val(24), fe.int_val(8),
                    fe.bit_subtract(bp, fe.int_val(24), fe.int_val(8), fe.int_val(1), False, BitwiseOverflowActions.FAIL, bb),
                ),
                fe.bit_get(fe.int_val(16), fe.int_val(8), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get(
                    fe.int_val(24), fe.int_val(8),
                    fe.bit_subtract(bp, fe.int_val(24), fe.int_val(8), fe.int_val(1), False, BitwiseOverflowActions.FAIL, bb),
                ),
                fe.bit_get(fe.int_val(16), fe.int_val(8), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

            # --- set_int ---
            # Negative
            rp.filter_expression = fe.ne(
                fe.bit_get(
                    fe.int_val(24), fe.int_val(8),
                    fe.bit_set_int(bp, fe.int_val(24), fe.int_val(8), fe.int_val(0x42), bb),
                ),
                fe.bit_get(fe.int_val(8), fe.int_val(8), bb),
            )
            with pytest.raises(ServerError) as exc_info:
                await client.get(rp, key, [bin_a])
            assert exc_info.value.result_code == ResultCode.FILTERED_OUT

            # Positive
            rp.filter_expression = fe.eq(
                fe.bit_get(
                    fe.int_val(24), fe.int_val(8),
                    fe.bit_set_int(bp, fe.int_val(24), fe.int_val(8), fe.int_val(0x42), bb),
                ),
                fe.bit_get(fe.int_val(8), fe.int_val(8), bb),
            )
            rec = await client.get(rp, key, [bin_a])
            assert rec is not None

        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass
