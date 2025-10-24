import pytest
from aerospike_async import *
from aerospike_async import FilterExpression as fe


class TestReadPolicy:
    """Test ReadPolicy functionality."""

    def test_set_and_get_fields(self):
        """Test setting and getting ReadPolicy fields."""
        rp = ReadPolicy()
        rp.consistency_level = ConsistencyLevel.ConsistencyAll
        rp.timeout = 20000
        rp.max_retries = 4
        rp.sleep_between_retries = 1000
        filter_exp = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))
        rp.filter_expression = filter_exp

        assert rp.consistency_level == ConsistencyLevel.ConsistencyAll
        assert rp.timeout == 20000
        assert rp.max_retries == 4
        assert rp.sleep_between_retries == 1000
        assert rp.filter_expression == filter_exp


class TestWritePolicy:
    """Test WritePolicy functionality."""

    def test_set_and_get_fields(self):
        """Test setting and getting WritePolicy fields."""
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.UpdateOnly
        wp.generation_policy = GenerationPolicy.ExpectGenEqual
        wp.commit_level = CommitLevel.CommitMaster
        wp.generation = 4
        wp.expiration = Expiration.NEVER_EXPIRE
        wp.send_key = True
        wp.respond_per_each_op = True
        wp.durable_delete = True

        assert wp.record_exists_action == RecordExistsAction.UpdateOnly
        assert wp.generation_policy == GenerationPolicy.ExpectGenEqual
        assert wp.commit_level == CommitLevel.CommitMaster
        assert wp.generation == 4
        assert wp.expiration == Expiration.NEVER_EXPIRE
        assert wp.send_key is True
        assert wp.respond_per_each_op is True
        assert wp.durable_delete is True


class TestScanPolicy:
    """Test ScanPolicy functionality."""

    def test_set_and_get_fields(self):
        """Test setting and getting ScanPolicy fields."""
        sp = ScanPolicy()
        sp.max_concurrent_nodes = 1
        sp.record_queue_size = 1000
        sp.socket_timeout = 5000

        assert sp.max_concurrent_nodes == 1
        assert sp.record_queue_size == 1000
        assert sp.socket_timeout == 5000


class TestQueryPolicy:
    """Test QueryPolicy functionality."""

    def test_set_and_get_fields(self):
        """Test setting and getting QueryPolicy fields."""
        qp = QueryPolicy()
        qp.max_concurrent_nodes = 1
        qp.record_queue_size = 1023
        qp.fail_on_cluster_change = False

        assert qp.max_concurrent_nodes == 1
        assert qp.record_queue_size == 1023
        assert qp.fail_on_cluster_change is False