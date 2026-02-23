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

from aerospike_async import ExpType
from aerospike_async import FilterExpression as fe


class TestFilterExprCreate:
    """Test creating various FilterExpression objects."""

    def test_key(self):
        expr = fe.key(exp_type=ExpType.STRING)
        assert isinstance(expr, fe)

    def test_key_exists(self):
        expr = fe.key_exists()
        assert isinstance(expr, fe)

    def test_bins(self):
        funcs = [
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
        expr = fe.bin_exists(name="bin")
        assert isinstance(expr, fe)

    def test_bin_type(self):
        expr = fe.bin_type(name="bin")
        assert isinstance(expr, fe)

    def test_set_name(self):
        expr = fe.set_name()
        assert isinstance(expr, fe)

    def test_device_size(self):
        expr = fe.device_size()
        assert isinstance(expr, fe)

    def test_last_update(self):
        expr = fe.last_update()
        assert isinstance(expr, fe)

    def test_since_update(self):
        expr = fe.since_update()
        assert isinstance(expr, fe)

    def test_void_time(self):
        expr = fe.void_time()
        assert isinstance(expr, fe)

    def test_ttl(self):
        expr = fe.ttl()
        assert isinstance(expr, fe)

    def test_is_tombstone(self):
        expr = fe.is_tombstone()
        assert isinstance(expr, fe)

    def test_digest_modulo(self):
        expr = fe.digest_modulo(modulo=7)
        assert isinstance(expr, fe)

    def test_regex_compare(self):
        expr = fe.regex_compare(
            regex="c*", flags=0, bin=fe.string_bin("bin")
        )
        assert isinstance(expr, fe)

    def test_geo_compare(self):
        geo_bin = fe.geo_bin("bin")
        geo_val = fe.geo_val('{"type":"Point","coordinates":[-80.590003, 28.60009]}')
        expr = fe.geo_compare(left=geo_bin, right=geo_val)
        assert isinstance(expr, fe)

    def test_vals(self):
        func_and_values = [
            (fe.int_val, 1),
            (fe.bool_val, True),
            (fe.string_val, "a"),
            (fe.float_val, 4.4),
            (fe.blob_val, b"asdf"),
            (fe.geo_val, '{"type":"Point","coordinates":[-80.590003, 28.60009]}'),
            (fe.list_val, [1, 2, 3]),
            (fe.map_val, {"key1": "value1", "key2": "value2"}),
        ]
        for func, value in func_and_values:
            expr = func(val=value)
            assert isinstance(expr, fe)

    def test_nil(self):
        expr = fe.nil()
        assert isinstance(expr, fe)

    def test_xor(self):
        expr = fe.xor(
            exps=[fe.eq(fe.int_bin("bin"), fe.int_val(4)), fe.bool_val(False)]
        )
        assert isinstance(expr, fe)

    def test_equality(self):
        funcs = [fe.eq, fe.ne, fe.gt, fe.ge, fe.lt, fe.le]
        for func in funcs:
            expr = func(left=fe.int_bin("bin"), right=fe.int_val(4))
            assert isinstance(expr, fe)

    def test_num_arithmetic(self):
        funcs = [fe.num_add, fe.num_sub, fe.num_mul, fe.num_div]
        for func in funcs:
            expr = func(exps=[fe.int_bin("bin1"), fe.int_bin("bin2")])
            assert isinstance(expr, fe)

    def test_num_pow(self):
        expr = fe.num_pow(base=fe.float_bin("bin"), exponent=fe.float_val(4))
        assert isinstance(expr, fe)

    def test_num_log(self):
        expr = fe.num_log(num=fe.float_bin("bin"), base=fe.float_val(10))
        assert isinstance(expr, fe)

    def test_num_mod(self):
        expr = fe.num_mod(numerator=fe.int_bin("bin"), denominator=fe.int_val(3))
        assert isinstance(expr, fe)

    def test_num_abs(self):
        expr = fe.num_abs(value=fe.int_val(-3))
        assert isinstance(expr, fe)

    def test_num_floor(self):
        expr = fe.num_floor(num=fe.float_val(5.3))
        assert isinstance(expr, fe)

    def test_num_ceil(self):
        expr = fe.num_ceil(num=fe.float_val(5.3))
        assert isinstance(expr, fe)

    def test_to_int(self):
        expr = fe.to_int(num=fe.float_val(5.3))
        assert isinstance(expr, fe)

    def test_to_float(self):
        expr = fe.to_float(num=fe.int_val(5))
        assert isinstance(expr, fe)

    def test_int_bitwise_ops(self):
        funcs = [fe.int_and, fe.int_or, fe.int_xor]
        for func in funcs:
            expr = func(exps=[fe.int_bin("bin"), fe.int_val(4)])
            assert isinstance(expr, fe)

    def test_int_not(self):
        expr = fe.int_not(fe.int_val(5))
        assert isinstance(expr, fe)

    def test_int_shift(self):
        funcs = [
            fe.int_lshift,
            fe.int_rshift,
            fe.int_arshift,
        ]
        for func in funcs:
            expr = func(value=fe.int_bin("bin"), shift=fe.int_val(4))
            assert isinstance(expr, fe)

    def test_int_count(self):
        expr = fe.int_count(fe.int_bin("bin"))
        assert isinstance(expr, fe)

    def test_int_scan(self):
        funcs = [fe.int_lscan, fe.int_rscan]
        for func in funcs:
            expr = func(value=fe.int_bin("bin"), search=fe.bool_val(True))
            assert isinstance(expr, fe)

    def test_min_max(self):
        funcs = [fe.min, fe.max]
        for func in funcs:
            expr = func(exps=[fe.float_bin("bin"), fe.float_val(5.0)])
            assert isinstance(expr, fe)

    def test_cond(self):
        expr = fe.cond(exps=[fe.bool_val(True), fe.int_val(4), fe.int_val(3)])
        assert isinstance(expr, fe)

    def test_var(self):
        expr = fe.var(name="var")
        assert isinstance(expr, fe)

    def test_unknown(self):
        expr = fe.unknown()
        assert isinstance(expr, fe)
