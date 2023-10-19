import unittest
from aerospike_async import ExpType, ReadPolicy, Record, RegexFlag
from aerospike_async import FilterExpression as fe
from fixtures import TestFixtureInsertRecord

class TestFilterExprUsage(TestFixtureInsertRecord):
    async def test_matching_filter_exp(self):
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Ford"))
        rec = await self.client.get(self.key, ["brand", "year"], policy=rp)
        self.assertEqual(type(rec), Record)
        self.assertEqual(rec.bins, {"brand": "Ford", "year": 1964})

    async def test_non_matching_filter_exp(self):
        rp = ReadPolicy()
        rp.filter_expression = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))

        with self.assertRaises(Exception):
            await self.client.get(self.key, ["brand", "year"], policy=rp)


# Check that we can create every possible filter expression
class TestFilterExprCreate(unittest.IsolatedAsyncioTestCase):
    def test_key(self):
        expr = fe.key(exp_type=ExpType.STRING)
        self.assertEqual(type(expr), fe)

    def test_key_exists(self):
        expr = fe.key_exists()
        self.assertEqual(type(expr), fe)

    def test_bins(self):
        funcs = [
            # TODO: missing bool bin
            fe.int_bin,
            fe.string_bin,
            fe.blob_bin,
            fe.float_bin,
            fe.geo_bin,
            fe.list_bin,
            fe.map_bin,
            fe.hll_bin
        ]
        for func in funcs:
            with self.subTest(func=func):
                expr = func(name="bin")
                self.assertEqual(type(expr), fe)

    # TODO: implementation for this looks wrong...
    def test_bin_exists(self):
        expr = fe.bin_exists(name="bin")
        self.assertEqual(type(expr), fe)

    def test_bin_type(self):
        expr = fe.bin_type(name="bin")
        self.assertEqual(type(expr), fe)

    def test_bin_type(self):
        expr = fe.set_name()
        self.assertEqual(type(expr), fe)

    def test_device_size(self):
        expr = fe.device_size()
        self.assertEqual(type(expr), fe)

    def test_last_update(self):
        expr = fe.last_update()
        self.assertEqual(type(expr), fe)

    def test_since_update(self):
        expr = fe.since_update()
        self.assertEqual(type(expr), fe)

    def test_void_time(self):
        expr = fe.void_time()
        self.assertEqual(type(expr), fe)

    def test_ttl(self):
        expr = fe.ttl()
        self.assertEqual(type(expr), fe)

    def test_is_tombstone(self):
        expr = fe.is_tombstone()
        self.assertEqual(type(expr), fe)

    def test_digest_modulo(self):
        expr = fe.digest_modulo(modulo=7)
        self.assertEqual(type(expr), fe)

    def test_regex_compare(self):
        expr = fe.regex_compare(regex="c*", flags=RegexFlag.EXTENDED, bin=fe.string_bin("bin"))
        self.assertEqual(type(expr), fe)

    def test_geo_compare(self):
        geo_bin = fe.geo_bin("bin")
        geo_val = fe.geo_val('{"type":"Point","coordinates":[-80.590003, 28.60009]}')
        expr = fe.geo_compare(left=geo_bin, right=geo_val)
        self.assertEqual(type(expr), fe)

    def test_vals(self):
        func_and_values = [
            (fe.int_val, 1),
            (fe.bool_val, True),
            (fe.string_val, "a"),
            (fe.float_val, 4.4),
            (fe.blob_val, b'asdf'),
            (fe.geo_val, '{"type":"Point","coordinates":[-80.590003, 28.60009]}')
            # TODO: missing HLL val
        ]
        for (func, value) in func_and_values:
            with self.subTest(func=func):
                expr = func(val=value)
                self.assertEqual(type(expr), fe)

    def test_nil(self):
        expr = fe.nil()
        self.assertEqual(type(expr), fe)

    # TODO: change reserved keyword. Also for and, or
    # def test_not(self):
    #     expr = fe.not(exp=fe.bool_val(True)):
    #     self.assertEqual(type(expr), fe)

    def test_xor(self):
        expr = fe.xor(exps=[fe.eq(fe.int_bin("bin"), fe.int_val(4)), fe.bool_val(False)])
        self.assertEqual(type(expr), fe)

    def test_equality(self):
        funcs = [
            fe.eq,
            fe.ne,
            fe.gt,
            fe.ge,
            fe.lt,
            fe.le
        ]
        for func in funcs:
            with self.subTest(func=func):
                expr = func(left=fe.int_bin("bin"), right=fe.int_val(4))
                self.assertEqual(type(expr), fe)

    def test_num_arithmetic(self):
        funcs = [
            fe.num_add,
            fe.num_sub,
            fe.num_mul,
            fe.num_div
        ]
        for func in funcs:
            with self.subTest(func=func):
                expr = func(exps=[fe.int_bin("bin1"), fe.int_bin("bin2")])
                self.assertEqual(type(expr), fe)

    def test_num_pow(self):
        expr = fe.num_pow(base=fe.float_bin("bin"), exponent=fe.float_val(4))
        self.assertEqual(type(expr), fe)

    def test_num_log(self):
        expr = fe.num_log(num=fe.float_bin("bin"), base=fe.float_val(10))
        self.assertEqual(type(expr), fe)

    def test_num_mod(self):
        expr = fe.num_mod(numerator=fe.int_bin("bin"), denominator=fe.int_val(3))
        self.assertEqual(type(expr), fe)

    def test_num_abs(self):
        expr = fe.num_abs(value=fe.int_val(-3))
        self.assertEqual(type(expr), fe)

    def test_num_floor(self):
        expr = fe.num_floor(num=fe.float_val(5.3))
        self.assertEqual(type(expr), fe)

    def test_num_ceil(self):
        expr = fe.num_ceil(num=fe.float_val(5.3))
        self.assertEqual(type(expr), fe)

    def test_to_int(self):
        expr = fe.to_int(num=fe.float_val(5.3))
        self.assertEqual(type(expr), fe)

    def test_to_float(self):
        expr = fe.to_float(num=fe.int_val(5))
        self.assertEqual(type(expr), fe)

    def test_int_bitwise_ops(self):
        funcs = [
            fe.int_and,
            fe.int_or,
            fe.int_xor
        ]
        for func in funcs:
            with self.subTest(func=func):
                expr = func(exps=[fe.int_bin("bin"), fe.int_val(4)])
                self.assertEqual(type(expr), fe)

    def test_int_not(self):
        expr = fe.int_not(fe.int_val(5))
        self.assertEqual(type(expr), fe)

    def test_int_shift(self):
        funcs = [
            fe.int_lshift,
            fe.int_rshift,
            fe.int_arshift,
        ]
        for func in funcs:
            with self.subTest(func=func):
                expr = func(value=fe.int_bin("bin"), shift=fe.int_val(4))
                self.assertEqual(type(expr), fe)

    def test_int_count(self):
        expr = fe.int_count(fe.int_bin("bin"))
        self.assertEqual(type(expr), fe)

    def test_int_scan(self):
        funcs = [
            fe.int_lscan,
            fe.int_rscan
        ]
        for func in funcs:
            with self.subTest(func=func):
                expr = func(value=fe.int_bin("bin"), search=fe.bool_val(True))
                self.assertEqual(type(expr), fe)


    def test_min_max(self):
        funcs = [
            fe.min,
            fe.max
        ]
        for func in funcs:
            with self.subTest(func=func):
                expr = func(exps=[fe.float_bin("bin"), fe.float_val(5.0)])
                self.assertEqual(type(expr), fe)

    def test_cond(self):
        expr = fe.cond(exps=[fe.bool_val(True), fe.int_val(4), fe.int_val(3)])
        self.assertEqual(type(expr), fe)

    # TODO: change reserved keyword
    # def test_exp_let(self):
    #     expr = fe.exp_let(exps=[fe.def("var"), fe.int_val(4)])
    #     self.assertEqual(type(expr), fe)

    # def test_def(self):
    #     pass

    def test_var(self):
        expr = fe.var(name="var")
        self.assertEqual(type(expr), fe)

    def test_unknown(self):
        expr = fe.unknown()
        self.assertEqual(type(expr), fe)
