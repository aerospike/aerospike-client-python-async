"""Tests for HLL FilterExpression methods."""

import pytest
from aerospike_async import (
    ExpType, FilterExpression as fe, WritePolicy, ReadPolicy, Key,
    HllOperation, HLLPolicy, ListReturnType,
)
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureConnection


class TestHLLExp(TestFixtureConnection):
    """Test HLL expression methods used as filter expressions."""

    @pytest.fixture
    async def hll_key(self, client):
        """Set up HLL bins for testing."""
        key = Key("test", "test", "hll_exp_5200")
        wp = WritePolicy()

        list1 = ["Akey1", "Akey2", "Akey3"]
        list2 = ["Bkey1", "Bkey2", "Bkey3"]
        list3 = ["Akey1", "Akey2", "Bkey1", "Bkey2", "Ckey1", "Ckey2"]

        result = await client.operate(wp, key, [
            HllOperation.add("hll1", list1, index_bit_count=8),
            HllOperation.add("hll2", list2, index_bit_count=8),
            HllOperation.add("hll3", list3, index_bit_count=8),
        ])
        assert result is not None

        yield key

        try:
            await client.delete(wp, key)
        except ServerError:
            pass

    async def test_count(self, client, hll_key):
        """HLL getCount: count > 0 should match, count == 0 should not."""
        rp = ReadPolicy()

        # Negative: count == 0 should not match
        rp.filter_expression = fe.eq(fe.hll_get_count(fe.hll_bin("hll1")), fe.int_val(0))
        with pytest.raises(ServerError) as exc_info:
            await client.get(rp, hll_key, ["hll1"])
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        # Positive: count > 0 should match
        rp.filter_expression = fe.gt(fe.hll_get_count(fe.hll_bin("hll1")), fe.int_val(0))
        rec = await client.get(rp, hll_key, ["hll1"])
        assert rec is not None

    async def test_union(self, client, hll_key):
        """HLL getUnion + getUnionCount: count of union == union count."""
        rp = ReadPolicy()

        rec = await client.get(rp, hll_key, ["hll1", "hll2", "hll3"])
        hll1 = rec.bins["hll1"]
        hll2 = rec.bins["hll2"]
        hll3 = rec.bins["hll3"]
        hlls = [hll1, hll2, hll3]

        # Negative: count(union) != union_count should not match
        rp.filter_expression = fe.ne(
            fe.hll_get_count(
                fe.hll_get_union(fe.list_val(hlls), fe.hll_bin("hll1")),
            ),
            fe.hll_get_union_count(fe.list_val(hlls), fe.hll_bin("hll1")),
        )
        with pytest.raises(ServerError) as exc_info:
            await client.get(rp, hll_key, ["hll1"])
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        # Positive: count(union) == union_count should match
        rp.filter_expression = fe.eq(
            fe.hll_get_count(
                fe.hll_get_union(fe.list_val(hlls), fe.hll_bin("hll1")),
            ),
            fe.hll_get_union_count(fe.list_val(hlls), fe.hll_bin("hll1")),
        )
        rec = await client.get(rp, hll_key, ["hll1"])
        assert rec is not None

    async def test_intersect(self, client, hll_key):
        """HLL getIntersectCount: intersect(hll2, hll1) <= intersect(hll3, hll1)."""
        rp = ReadPolicy()

        rec = await client.get(rp, hll_key, ["hll2", "hll3"])
        hll2 = rec.bins["hll2"]
        hll3 = rec.bins["hll3"]

        # Negative: intersect(hll2, hll1) >= intersect(hll3, hll1) should not match
        rp.filter_expression = fe.ge(
            fe.hll_get_intersect_count(fe.list_val([hll2]), fe.hll_bin("hll1")),
            fe.hll_get_intersect_count(fe.list_val([hll3]), fe.hll_bin("hll1")),
        )
        with pytest.raises(ServerError) as exc_info:
            await client.get(rp, hll_key, ["hll1"])
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        # Positive: intersect(hll2, hll1) <= intersect(hll3, hll1) should match
        rp.filter_expression = fe.le(
            fe.hll_get_intersect_count(fe.list_val([hll2]), fe.hll_bin("hll1")),
            fe.hll_get_intersect_count(fe.list_val([hll3]), fe.hll_bin("hll1")),
        )
        rec = await client.get(rp, hll_key, ["hll1"])
        assert rec is not None

    async def test_similarity(self, client, hll_key):
        """HLL getSimilarity: similarity(hll2, hll1) <= similarity(hll3, hll1)."""
        rp = ReadPolicy()

        rec = await client.get(rp, hll_key, ["hll2", "hll3"])
        hll2 = rec.bins["hll2"]
        hll3 = rec.bins["hll3"]

        # Negative: similarity(hll2, hll1) >= similarity(hll3, hll1) should not match
        rp.filter_expression = fe.ge(
            fe.hll_get_similarity(fe.list_val([hll2]), fe.hll_bin("hll1")),
            fe.hll_get_similarity(fe.list_val([hll3]), fe.hll_bin("hll1")),
        )
        with pytest.raises(ServerError) as exc_info:
            await client.get(rp, hll_key, ["hll1"])
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        # Positive: similarity(hll2, hll1) <= similarity(hll3, hll1) should match
        rp.filter_expression = fe.le(
            fe.hll_get_similarity(fe.list_val([hll2]), fe.hll_bin("hll1")),
            fe.hll_get_similarity(fe.list_val([hll3]), fe.hll_bin("hll1")),
        )
        rec = await client.get(rp, hll_key, ["hll1"])
        assert rec is not None

    async def test_describe(self, client, hll_key):
        """HLL describe: index bit counts of hll1 and hll2 should be equal."""
        rp = ReadPolicy()

        # Negative: describe[0] of hll1 != describe[0] of hll2 should not match
        rp.filter_expression = fe.ne(
            fe.list_get_by_index(
                ListReturnType.VALUE, ExpType.INT, fe.int_val(0),
                fe.hll_describe(fe.hll_bin("hll1")), [],
            ),
            fe.list_get_by_index(
                ListReturnType.VALUE, ExpType.INT, fe.int_val(0),
                fe.hll_describe(fe.hll_bin("hll2")), [],
            ),
        )
        with pytest.raises(ServerError) as exc_info:
            await client.get(rp, hll_key, ["hll1"])
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        # Positive: describe[0] of hll1 == describe[0] of hll2 should match
        rp.filter_expression = fe.eq(
            fe.list_get_by_index(
                ListReturnType.VALUE, ExpType.INT, fe.int_val(0),
                fe.hll_describe(fe.hll_bin("hll1")), [],
            ),
            fe.list_get_by_index(
                ListReturnType.VALUE, ExpType.INT, fe.int_val(0),
                fe.hll_describe(fe.hll_bin("hll2")), [],
            ),
        )
        rec = await client.get(rp, hll_key, ["hll1"])
        assert rec is not None

    async def test_may_contain(self, client, hll_key):
        """HLL mayContain: non-existent value should not be contained."""
        rp = ReadPolicy()

        # Negative: "new_val" mayContain == 1 should not match
        rp.filter_expression = fe.eq(
            fe.hll_may_contain(fe.list_val(["new_val"]), fe.hll_bin("hll2")),
            fe.int_val(1),
        )
        with pytest.raises(ServerError) as exc_info:
            await client.get(rp, hll_key, ["hll2"])
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        # Positive: "new_val" mayContain != 1 should match
        rp.filter_expression = fe.ne(
            fe.hll_may_contain(fe.list_val(["new_val"]), fe.hll_bin("hll2")),
            fe.int_val(1),
        )
        rec = await client.get(rp, hll_key, ["hll2"])
        assert rec is not None

    async def test_add(self, client, hll_key):
        """HLL add: adding values should increase count."""
        rp = ReadPolicy()

        # Negative: count(hll1) == count(add(new_val, hll2)) should not match
        rp.filter_expression = fe.eq(
            fe.hll_get_count(fe.hll_bin("hll1")),
            fe.hll_get_count(
                fe.hll_add(HLLPolicy(), fe.list_val(["new_val"]), fe.hll_bin("hll2")),
            ),
        )
        with pytest.raises(ServerError) as exc_info:
            await client.get(rp, hll_key, ["hll1"])
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        # Positive: count(hll1) < count(add(new_val, hll2)) should match
        rp.filter_expression = fe.lt(
            fe.hll_get_count(fe.hll_bin("hll1")),
            fe.hll_get_count(
                fe.hll_add(HLLPolicy(), fe.list_val(["new_val"]), fe.hll_bin("hll2")),
            ),
        )
        rec = await client.get(rp, hll_key, ["hll1"])
        assert rec is not None
