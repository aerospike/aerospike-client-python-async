import unittest
from aerospike_async import *
from aerospike_async import FilterExpression as fe

class TestReadPolicy(unittest.TestCase):
    def test_set_and_get_fields(self):
        # Set to values different from the default
        rp = ReadPolicy()
        rp.priority = Priority.High
        rp.consistency_level = ConsistencyLevel.ConsistencyAll
        rp.timeout = 20000
        rp.max_retries = 4
        rp.sleep_between_retries = 1000
        filter_exp = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))
        rp.filter_expression = filter_exp

        # Should read new values from the wrapped Rust client's read policy
        self.assertEqual(rp.priority, Priority.High)
        self.assertEqual(rp.consistency_level, ConsistencyLevel.ConsistencyAll)
        self.assertEqual(rp.timeout, 20000)
        self.assertEqual(rp.max_retries, 4)
        self.assertEqual(rp.sleep_between_retries, 1000)
        self.assertEqual(rp.filter_expression, filter_exp)

    def test_priority(self):
        rp = ReadPolicy()
        priorities = [
            Priority.Default,
            Priority.Low,
            Priority.Medium,
            Priority.High
        ]
        for priority in priorities:
            rp.priority = priority
            self.assertEqual(rp.priority, priority)

    # All consistency levels have been tested above

class TestWritePolicy(unittest.TestCase):
    def test_set_and_get_fields(self):
        
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.UpdateOnly
        wp.generation_policy = GenerationPolicy.ExpectGenEqual
        wp.commit_level = CommitLevel.CommitMaster
        wp.generation = 4
        wp.expiration = Expiration.Never
        wp.send_key = True
        wp.respond_per_each_op = True
        wp.durable_delete = True
        # TODO: filter exp already in read policy?

        self.assertEqual(wp.record_exists_action, RecordExistsAction.UpdateOnly)
        self.assertEqual(wp.generation_policy, GenerationPolicy.ExpectGenEqual)
        self.assertEqual(wp.commit_level, CommitLevel.CommitMaster)
        self.assertEqual(wp.generation, 4)
        self.assertEqual(wp.expiration, Expiration.Never)
        self.assertEqual(wp.send_key, True)
        self.assertEqual(wp.respond_per_each_op, True)
        self.assertEqual(wp.durable_delete, True)
        # TODO: filter exp already in read policy?


class ScanPolicy(unittest.TestCase):
    def test_set_and_get_fields(self):
        sp = ScanPolicy()
        sp.scan_percent = 30
        sp.max_concurrent_nodes = 1
        sp.record_queue_size = 1000
        sp.fail_on_cluster_change = False
        sp.socket_timeout = 5000
        # TODO: filter exp already in read policy?

        self.assertEqual(sp.scan_percent, 30)
        self.assertEqual(sp.max_concurrent_nodes, 1)
        self.assertEqual(sp.record_queue_size, 1000)
        self.assertEqual(sp.fail_on_cluster_change, False)
        self.assertEqual(sp.socket_timeout, 5000)
        # TODO: filter exp already in read policy?


class QueryPolicy(unittest.TestCase):
    def test_set_and_get_fields(self):
        qp = QueryPolicy()
        qp.max_concurrent_nodes = 1
        qp.record_queue_size = 1023
        qp.fail_on_cluster_change = False
        # TODO: filter exp already in read policy?

        self.assertEqual(qp.max_concurrent_nodes, 1)
        self.assertEqual(qp.record_queue_size, 1023)
        self.assertEqual(qp.fail_on_cluster_change, False)
        # TODO: filter exp already in read policy?
