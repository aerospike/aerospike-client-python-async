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

from aerospike_async import (
    AdminPolicy,
    BatchDeleteOp,
    BatchDeletePolicy,
    BatchPolicy,
    BatchReadOp,
    BatchReadPolicy,
    BatchUDFPolicy,
    BatchWriteOp,
    BatchWritePolicy,
    Expiration,
    FilterExpression as fe,
    Key,
    Operation,
    RecordExistsAction,
)


class TestAdminPolicy:

    def test_default_construction(self):
        ap = AdminPolicy()
        assert ap is not None
        assert isinstance(ap.timeout, int)

    def test_timeout(self):
        ap = AdminPolicy()
        ap.timeout = 5000
        assert ap.timeout == 5000


class TestBatchDeletePolicy:

    def test_default_construction(self):
        p = BatchDeletePolicy()
        assert p is not None

    def test_filter_expression(self):
        p = BatchDeletePolicy()
        assert p.filter_expression is None
        expr = fe.eq(fe.int_bin("x"), fe.int_val(1))
        p.filter_expression = expr
        assert p.filter_expression == expr
        p.filter_expression = None
        assert p.filter_expression is None

    def test_send_key(self):
        p = BatchDeletePolicy()
        p.send_key = True
        assert p.send_key is True
        p.send_key = False
        assert p.send_key is False

    def test_durable_delete(self):
        p = BatchDeletePolicy()
        p.durable_delete = True
        assert p.durable_delete is True
        p.durable_delete = False
        assert p.durable_delete is False

    def test_generation(self):
        p = BatchDeletePolicy()
        p.generation = 0
        assert p.generation == 0
        p.generation = 42
        assert p.generation == 42


class TestBatchReadPolicy:

    def test_default_construction(self):
        p = BatchReadPolicy()
        assert p is not None

    def test_filter_expression(self):
        p = BatchReadPolicy()
        assert p.filter_expression is None
        expr = fe.eq(fe.int_bin("x"), fe.int_val(1))
        p.filter_expression = expr
        assert p.filter_expression == expr

    def test_read_touch_ttl_default(self):
        p = BatchReadPolicy()
        assert p.read_touch_ttl == 0

    def test_read_touch_ttl_valid_values(self):
        p = BatchReadPolicy()
        p.read_touch_ttl = -1
        assert p.read_touch_ttl == -1
        p.read_touch_ttl = 0
        assert p.read_touch_ttl == 0
        p.read_touch_ttl = 1
        assert p.read_touch_ttl == 1
        p.read_touch_ttl = 50
        assert p.read_touch_ttl == 50
        p.read_touch_ttl = 100
        assert p.read_touch_ttl == 100

    def test_read_touch_ttl_invalid_raises(self):
        import pytest
        p = BatchReadPolicy()
        with pytest.raises(ValueError):
            p.read_touch_ttl = -2
        with pytest.raises(ValueError):
            p.read_touch_ttl = 101
        with pytest.raises(ValueError):
            p.read_touch_ttl = 3600


class TestBatchUDFPolicy:

    def test_default_construction(self):
        p = BatchUDFPolicy()
        assert p is not None

    def test_filter_expression(self):
        p = BatchUDFPolicy()
        assert p.filter_expression is None
        expr = fe.eq(fe.int_bin("x"), fe.int_val(1))
        p.filter_expression = expr
        assert p.filter_expression == expr

    def test_send_key(self):
        p = BatchUDFPolicy()
        p.send_key = True
        assert p.send_key is True

    def test_durable_delete(self):
        p = BatchUDFPolicy()
        p.durable_delete = True
        assert p.durable_delete is True


class TestBatchWritePolicy:

    def test_default_construction(self):
        p = BatchWritePolicy()
        assert p is not None

    def test_filter_expression(self):
        p = BatchWritePolicy()
        assert p.filter_expression is None
        expr = fe.eq(fe.int_bin("x"), fe.int_val(1))
        p.filter_expression = expr
        assert p.filter_expression == expr

    def test_send_key(self):
        p = BatchWritePolicy()
        p.send_key = True
        assert p.send_key is True

    def test_durable_delete(self):
        p = BatchWritePolicy()
        p.durable_delete = True
        assert p.durable_delete is True

    def test_generation(self):
        p = BatchWritePolicy()
        p.generation = 99
        assert p.generation == 99

    def test_expiration(self):
        p = BatchWritePolicy()
        p.expiration = Expiration.NEVER_EXPIRE
        assert p.expiration == Expiration.NEVER_EXPIRE
        p.expiration = Expiration.NAMESPACE_DEFAULT
        assert p.expiration == Expiration.NAMESPACE_DEFAULT
        exp_seconds = Expiration.seconds(600)
        p.expiration = exp_seconds
        assert p.expiration == exp_seconds


def test_batch_policy_defaults():
    """Test that batch policies have correct defaults."""
    bp = BatchPolicy()
    assert bp.allow_inline is True
    assert bp.allow_inline_ssd is False
    assert bp.respond_all_keys is True

    brp = BatchReadPolicy()
    assert brp.filter_expression is None

    bwp = BatchWritePolicy()
    assert bwp.send_key is False
    assert bwp.durable_delete is False
    assert bwp.generation == 0

    bdp = BatchDeletePolicy()
    assert bdp.send_key is False
    assert bdp.durable_delete is False
    assert bdp.generation == 0

    budfp = BatchUDFPolicy()
    assert budfp.send_key is False
    assert budfp.durable_delete is False


class TestBatchWritePolicyRecordExistsAction:

    def test_default_is_update(self):
        p = BatchWritePolicy()
        assert p.record_exists_action == RecordExistsAction.UPDATE

    def test_set_create_only(self):
        p = BatchWritePolicy()
        p.record_exists_action = RecordExistsAction.CREATE_ONLY
        assert p.record_exists_action == RecordExistsAction.CREATE_ONLY

    def test_set_replace(self):
        p = BatchWritePolicy()
        p.record_exists_action = RecordExistsAction.REPLACE
        assert p.record_exists_action == RecordExistsAction.REPLACE


class TestBatchReadOp:

    def test_bins_only(self):
        k = Key("ns", "set", "k1")
        op = BatchReadOp(k, bins=["a", "b"])
        assert op is not None

    def test_no_bins_no_ops(self):
        k = Key("ns", "set", "k1")
        op = BatchReadOp(k)
        assert op is not None

    def test_with_policy(self):
        k = Key("ns", "set", "k1")
        p = BatchReadPolicy()
        p.read_touch_ttl = 50
        op = BatchReadOp(k, bins=["a"], policy=p)
        assert op is not None

    def test_with_operations(self):
        k = Key("ns", "set", "k1")
        op = BatchReadOp(k, operations=[Operation.get()])
        assert op is not None


class TestBatchWriteOp:

    def test_basic(self):
        k = Key("ns", "set", "k1")
        op = BatchWriteOp(k, operations=[Operation.put("b", 1)])
        assert op is not None

    def test_with_policy(self):
        k = Key("ns", "set", "k1")
        p = BatchWritePolicy()
        p.record_exists_action = RecordExistsAction.CREATE_ONLY
        op = BatchWriteOp(k, [Operation.put("b", 1)], policy=p)
        assert op is not None


class TestBatchDeleteOp:

    def test_basic(self):
        k = Key("ns", "set", "k1")
        op = BatchDeleteOp(k)
        assert op is not None

    def test_with_policy(self):
        k = Key("ns", "set", "k1")
        p = BatchDeletePolicy()
        p.durable_delete = True
        op = BatchDeleteOp(k, policy=p)
        assert op is not None
