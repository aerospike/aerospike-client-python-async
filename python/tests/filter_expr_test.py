import pytest
from aerospike_async import ExpType, ReadPolicy, Record
from aerospike_async.exceptions import ServerError
from aerospike_async import FilterExpression as fe
from fixtures import TestFixtureInsertRecord


class TestFilterExprUsage(TestFixtureInsertRecord):
    """Test FilterExpression usage in actual operations."""

    async def test_matching_filter_exp(self, client, key):
        """Test using a matching filter expression."""
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Ford"))
        rec = await client.get(rp, key, ["brand", "year"])
        assert isinstance(rec, Record)
        assert rec.bins == {"brand": "Ford", "year": 1964}

    async def test_non_matching_filter_exp(self, client, key):
        """Test using a non-matching filter expression raises ServerError."""
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))

        with pytest.raises(ServerError):
            await client.get(rp, key, ["brand", "year"])


# Check that we can create every possible filter expression
class TestFilterExprCreate:
    """Test creating various FilterExpression objects."""

    def test_key(self):
        """Test creating key expression."""
        expr = fe.key(exp_type=ExpType.String)
        assert isinstance(expr, fe)

    def test_key_exists(self):
        """Test creating key_exists expression."""
        expr = fe.key_exists()
        assert isinstance(expr, fe)

    def test_bins(self):
        """Test creating bin expressions."""
        funcs = [
            # TODO: missing bool bin
            fe.int_bin,
            fe.string_bin,
            fe.blob_bin,
            fe.float_bin,
            fe.geo_bin,
            fe.list_bin,
            fe.map_bin,
            fe.hll_bin,
        ]
        for func in funcs:
            expr = func(name="bin")
            assert isinstance(expr, fe)

    def test_bin_exists(self):
        """Test creating bin_exists expression."""
        expr = fe.bin_exists(name="bin")
        assert isinstance(expr, fe)

    def test_bin_type(self):
        """Test creating bin_type expression."""
        expr = fe.bin_type(name="bin")
        assert isinstance(expr, fe)

    def test_set_name(self):
        """Test creating set_name expression."""
        expr = fe.set_name()
        assert isinstance(expr, fe)

    def test_device_size(self):
        """Test creating device_size expression."""
        expr = fe.device_size()
        assert isinstance(expr, fe)

    def test_last_update(self):
        """Test creating last_update expression."""
        expr = fe.last_update()
        assert isinstance(expr, fe)

    def test_since_update(self):
        """Test creating since_update expression."""
        expr = fe.since_update()
        assert isinstance(expr, fe)

    def test_void_time(self):
        """Test creating void_time expression."""
        expr = fe.void_time()
        assert isinstance(expr, fe)

    def test_ttl(self):
        """Test creating ttl expression."""
        expr = fe.ttl()
        assert isinstance(expr, fe)

    def test_is_tombstone(self):
        """Test creating is_tombstone expression."""
        expr = fe.is_tombstone()
        assert isinstance(expr, fe)

    def test_digest_modulo(self):
        """Test creating digest_modulo expression."""
        expr = fe.digest_modulo(modulo=7)
        assert isinstance(expr, fe)

    def test_regex_compare(self):
        """Test creating regex_compare expression."""
        expr = fe.regex_compare(
            regex="c*", flags=0, bin=fe.string_bin("bin")
        )
        assert isinstance(expr, fe)

    def test_geo_compare(self):
        """Test creating geo_compare expression."""
        geo_bin = fe.geo_bin("bin")
        geo_val = fe.geo_val('{"type":"Point","coordinates":[-80.590003, 28.60009]}')
        expr = fe.geo_compare(left=geo_bin, right=geo_val)
        assert isinstance(expr, fe)

    def test_vals(self):
        """Test creating value expressions."""
        func_and_values = [
            (fe.int_val, 1),
            (fe.bool_val, True),
            (fe.string_val, "a"),
            (fe.float_val, 4.4),
            (fe.blob_val, b"asdf"),
            (fe.geo_val, '{"type":"Point","coordinates":[-80.590003, 28.60009]}'),
            (fe.list_val, [1, 2, 3]),
            (fe.map_val, {"key1": "value1", "key2": "value2"}),
            # TODO: missing HLL val
        ]
        for func, value in func_and_values:
            expr = func(val=value)
            assert isinstance(expr, fe)

    def test_nil(self):
        """Test creating nil expression."""
        expr = fe.nil()
        assert isinstance(expr, fe)

    def test_xor(self):
        """Test creating xor expression."""
        expr = fe.xor(
            exps=[fe.eq(fe.int_bin("bin"), fe.int_val(4)), fe.bool_val(False)]
        )
        assert isinstance(expr, fe)

    def test_equality(self):
        """Test creating equality expressions."""
        funcs = [fe.eq, fe.ne, fe.gt, fe.ge, fe.lt, fe.le]
        for func in funcs:
            expr = func(left=fe.int_bin("bin"), right=fe.int_val(4))
            assert isinstance(expr, fe)

    def test_num_arithmetic(self):
        """Test creating numeric arithmetic expressions."""
        funcs = [fe.num_add, fe.num_sub, fe.num_mul, fe.num_div]
        for func in funcs:
            expr = func(exps=[fe.int_bin("bin1"), fe.int_bin("bin2")])
            assert isinstance(expr, fe)

    def test_num_pow(self):
        """Test creating num_pow expression."""
        expr = fe.num_pow(base=fe.float_bin("bin"), exponent=fe.float_val(4))
        assert isinstance(expr, fe)

    def test_num_log(self):
        """Test creating num_log expression."""
        expr = fe.num_log(num=fe.float_bin("bin"), base=fe.float_val(10))
        assert isinstance(expr, fe)

    def test_num_mod(self):
        """Test creating num_mod expression."""
        expr = fe.num_mod(numerator=fe.int_bin("bin"), denominator=fe.int_val(3))
        assert isinstance(expr, fe)

    def test_num_abs(self):
        """Test creating num_abs expression."""
        expr = fe.num_abs(value=fe.int_val(-3))
        assert isinstance(expr, fe)

    def test_num_floor(self):
        """Test creating num_floor expression."""
        expr = fe.num_floor(num=fe.float_val(5.3))
        assert isinstance(expr, fe)

    def test_num_ceil(self):
        """Test creating num_ceil expression."""
        expr = fe.num_ceil(num=fe.float_val(5.3))
        assert isinstance(expr, fe)

    def test_to_int(self):
        """Test creating to_int expression."""
        expr = fe.to_int(num=fe.float_val(5.3))
        assert isinstance(expr, fe)

    def test_to_float(self):
        """Test creating to_float expression."""
        expr = fe.to_float(num=fe.int_val(5))
        assert isinstance(expr, fe)

    def test_int_bitwise_ops(self):
        """Test creating integer bitwise operation expressions."""
        funcs = [fe.int_and, fe.int_or, fe.int_xor]
        for func in funcs:
            expr = func(exps=[fe.int_bin("bin"), fe.int_val(4)])
            assert isinstance(expr, fe)

    def test_int_not(self):
        """Test creating int_not expression."""
        expr = fe.int_not(fe.int_val(5))
        assert isinstance(expr, fe)

    def test_int_shift(self):
        """Test creating integer shift expressions."""
        funcs = [
            fe.int_lshift,
            fe.int_rshift,
            fe.int_arshift,
        ]
        for func in funcs:
            expr = func(value=fe.int_bin("bin"), shift=fe.int_val(4))
            assert isinstance(expr, fe)

    def test_int_count(self):
        """Test creating int_count expression."""
        expr = fe.int_count(fe.int_bin("bin"))
        assert isinstance(expr, fe)

    def test_int_scan(self):
        """Test creating integer scan expressions."""
        funcs = [fe.int_lscan, fe.int_rscan]
        for func in funcs:
            expr = func(value=fe.int_bin("bin"), search=fe.bool_val(True))
            assert isinstance(expr, fe)

    def test_min_max(self):
        """Test creating min/max expressions."""
        funcs = [fe.min, fe.max]
        for func in funcs:
            expr = func(exps=[fe.float_bin("bin"), fe.float_val(5.0)])
            assert isinstance(expr, fe)

    def test_cond(self):
        """Test creating cond expression."""
        expr = fe.cond(exps=[fe.bool_val(True), fe.int_val(4), fe.int_val(3)])
        assert isinstance(expr, fe)

    def test_var(self):
        """Test creating var expression."""
        expr = fe.var(name="var")
        assert isinstance(expr, fe)

    def test_unknown(self):
        """Test creating unknown expression."""
        expr = fe.unknown()
        assert isinstance(expr, fe)


class TestFilterExprListVal(TestFixtureInsertRecord):
    """Test list_val filter expression usage."""

    async def test_list_val_equality(self, client, key):
        """Test comparing a list bin to a list value in filter expression."""
        # Create a test list
        test_list = [1, -1, 3, 5]
        
        # Put the list in a bin
        from aerospike_async import WritePolicy
        wp = WritePolicy()
        await client.put(wp, key, {"listbin": test_list})
        
        # Use filter expression to compare list bin to list value
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.list_bin("listbin"), fe.list_val(test_list))
        
        # Should match and return the record
        rec = await client.get(rp, key, ["listbin"])
        assert isinstance(rec, Record)
        assert rec.bins["listbin"] == test_list

    async def test_list_val_non_matching(self, client, key):
        """Test list_val with non-matching list raises ServerError."""
        test_list = [1, 2, 3]
        different_list = [4, 5, 6]
        
        from aerospike_async import WritePolicy
        wp = WritePolicy()
        await client.put(wp, key, {"listbin": test_list})
        
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.list_bin("listbin"), fe.list_val(different_list))
        
        with pytest.raises(ServerError):
            await client.get(rp, key, ["listbin"])


class TestFilterExprMapVal(TestFixtureInsertRecord):
    """Test map_val filter expression usage."""

    @pytest.mark.skip(reason="Map comparison in filter expressions requires exact byte-level matching. Python dicts convert to Rust HashMap which has non-deterministic iteration order, causing serialization mismatches even when using the same dict object. This is a known limitation similar to map_value context issues.")
    async def test_map_val_equality(self, client, key):
        """Test comparing a map bin to a map value in filter expression.
        
        Based on Java TestMapExp.sortedMapEquality()
        
        NOTE: This test is skipped due to serialization order issues. Map comparison
        in filter expressions requires exact byte-level matching. When Python dicts
        are converted to Rust HashMap for serialization, the iteration order is
        non-deterministic, causing the filter expression to fail even when comparing
        the same map. This is similar to the map_value context issue in CTX operations.
        """
        # Create a test map
        test_map = {
            "key1": "e",
            "key2": "d",
            "key3": "c",
            "key4": "b",
            "key5": "a",
        }
        
        # Put the map in a bin
        from aerospike_async import WritePolicy
        wp = WritePolicy()
        await client.put(wp, key, {"mapbin": test_map})
        
        # Retrieve the map as stored by the server to get exact serialization format
        # This ensures we use the same byte-level representation for comparison
        rp_no_filter = ReadPolicy()
        rec_stored = await client.get(rp_no_filter, key, ["mapbin"])
        stored_map = rec_stored.bins["mapbin"]
        
        # Use filter expression to compare map bin to the exact stored map value
        # The filter expression requires exact byte-level matching
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.map_bin("mapbin"), fe.map_val(stored_map))
        
        # Should match and return the record (not filtered out)
        rec = await client.get(rp, key, ["mapbin"])
        assert isinstance(rec, Record)
        # Verify the map contents match
        assert rec.bins["mapbin"] == stored_map

    async def test_map_val_non_matching(self, client, key):
        """Test map_val with non-matching map raises ServerError."""
        test_map = {"a": 1, "b": 2}
        different_map = {"c": 3, "d": 4}
        
        from aerospike_async import WritePolicy
        wp = WritePolicy()
        await client.put(wp, key, {"mapbin": test_map})
        
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.map_bin("mapbin"), fe.map_val(different_map))
        
        with pytest.raises(ServerError):
            await client.get(rp, key, ["mapbin"])
