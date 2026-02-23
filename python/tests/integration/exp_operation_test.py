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

"""Tests for expression operations (ExpOperation)."""

import pytest
from aerospike_async import (
    ExpOperation, ExpWriteFlags, ExpReadFlags,
    FilterExpression as fe, WritePolicy, ReadPolicy, Key
)
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureConnection


class TestExpOperationRead(TestFixtureConnection):
    """Test ExpOperation.read for evaluating expressions."""

    async def test_read_integer_expression(self, client):
        """Test reading an integer expression result."""
        key = Key("test", "test", "exp_read_int")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            # Create a record with an integer bin
            await client.put(wp, key, {"value": 100})

            # Read with expression that adds 50 to the value
            expr = fe.num_add([fe.int_bin("value"), fe.int_val(50)])
            result = await client.operate(wp, key, [
                ExpOperation.read("computed", expr)
            ])

            assert "computed" in result.bins
            assert result.bins["computed"] == 150
        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass

    async def test_read_string_expression(self, client):
        """Test reading a string bin expression."""
        key = Key("test", "test", "exp_read_str")
        wp = WritePolicy()

        try:
            await client.put(wp, key, {"name": "test"})

            # Read with expression that returns the string bin
            expr = fe.string_bin("name")
            result = await client.operate(wp, key, [
                ExpOperation.read("name_result", expr)
            ])

            assert "name_result" in result.bins
            assert result.bins["name_result"] == "test"
        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass

    async def test_read_comparison_expression(self, client):
        """Test reading a comparison expression result."""
        key = Key("test", "test", "exp_read_cmp")
        wp = WritePolicy()

        try:
            await client.put(wp, key, {"score": 85})

            # Read with expression that checks if score > 80
            expr = fe.gt(fe.int_bin("score"), fe.int_val(80))
            result = await client.operate(wp, key, [
                ExpOperation.read("passed", expr)
            ])

            assert "passed" in result.bins
            assert result.bins["passed"] == True
        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass

    async def test_read_with_flags(self, client):
        """Test read operation with EVAL_NO_FAIL flag."""
        key = Key("test", "test", "exp_read_flags")
        wp = WritePolicy()

        try:
            await client.put(wp, key, {"value": 10})

            # Read with EVAL_NO_FAIL flag
            expr = fe.int_bin("value")
            result = await client.operate(wp, key, [
                ExpOperation.read("result", expr, int(ExpReadFlags.EVAL_NO_FAIL))
            ])

            assert "result" in result.bins
            assert result.bins["result"] == 10
        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass


class TestExpOperationWrite(TestFixtureConnection):
    """Test ExpOperation.write for writing expression results to bins."""

    async def test_write_expression_to_bin(self, client):
        """Test writing an expression result to a bin."""
        key = Key("test", "test", "exp_write_basic")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            # Create a record with initial value
            await client.put(wp, key, {"value": 50})

            # Write doubled value to a new bin
            expr = fe.num_mul([fe.int_bin("value"), fe.int_val(2)])
            await client.operate(wp, key, [
                ExpOperation.write("doubled", expr)
            ])

            # Verify the result
            rec = await client.get(rp, key)
            assert rec.bins["value"] == 50
            assert rec.bins["doubled"] == 100
        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass

    async def test_write_overwrite_bin(self, client):
        """Test writing expression result overwrites existing bin."""
        key = Key("test", "test", "exp_write_overwrite")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            await client.put(wp, key, {"counter": 10})

            # Overwrite counter with incremented value
            expr = fe.num_add([fe.int_bin("counter"), fe.int_val(1)])
            await client.operate(wp, key, [
                ExpOperation.write("counter", expr)
            ])

            rec = await client.get(rp, key)
            assert rec.bins["counter"] == 11
        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass

    async def test_write_create_only_flag(self, client):
        """Test write with CREATE_ONLY flag fails if bin exists."""
        key = Key("test", "test", "exp_write_create_only")
        wp = WritePolicy()

        try:
            await client.put(wp, key, {"existing": 100})

            expr = fe.int_val(200)
            # This should fail because bin exists and CREATE_ONLY is set
            with pytest.raises(ServerError) as exc_info:
                await client.operate(wp, key, [
                    ExpOperation.write("existing", expr, int(ExpWriteFlags.CREATE_ONLY))
                ])
            assert exc_info.value.result_code == ResultCode.BIN_EXISTS_ERROR
        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass

    async def test_write_update_only_flag(self, client):
        """Test write with UPDATE_ONLY flag fails if bin doesn't exist."""
        key = Key("test", "test", "exp_write_update_only")
        wp = WritePolicy()

        try:
            await client.put(wp, key, {"other": 100})

            expr = fe.int_val(200)
            # This should fail because newbin doesn't exist and UPDATE_ONLY is set
            with pytest.raises(ServerError) as exc_info:
                await client.operate(wp, key, [
                    ExpOperation.write("newbin", expr, int(ExpWriteFlags.UPDATE_ONLY))
                ])
            assert exc_info.value.result_code == ResultCode.BIN_NOT_FOUND
        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass


class TestExpOperationCombined(TestFixtureConnection):
    """Test combining ExpOperation with other operations."""

    async def test_read_and_write_in_same_operate(self, client):
        """Test both read and write expression operations in same call."""
        key = Key("test", "test", "exp_combined")
        wp = WritePolicy()
        rp = ReadPolicy()

        try:
            await client.put(wp, key, {"x": 5, "y": 10})

            # Read sum and write product in same operation
            sum_expr = fe.num_add([fe.int_bin("x"), fe.int_bin("y")])
            prod_expr = fe.num_mul([fe.int_bin("x"), fe.int_bin("y")])

            result = await client.operate(wp, key, [
                ExpOperation.read("sum", sum_expr),
                ExpOperation.write("product", prod_expr)
            ])

            # Check read result
            assert result.bins["sum"] == 15

            # Check write result persisted
            rec = await client.get(rp, key)
            assert rec.bins["product"] == 50
        finally:
            try:
                await client.delete(wp, key)
            except ServerError:
                pass




