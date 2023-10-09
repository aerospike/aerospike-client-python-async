import unittest
from aerospike_async import *
from aerospike_async import FilterExpression as fe

class TestReadPolicy(unittest.TestCase):
    def test_get_defaults(self):
        # TODO: defaults are not documented in Rust client docs
        rp = ReadPolicy()
        self.assertEqual(rp.priority, Priority.Default)
        self.assertEqual(rp.consistency_level, ConsistencyLevel.ConsistencyOne)
        self.assertEqual(rp.timeout, 30000)
        self.assertEqual(rp.max_retries, 2)
        self.assertEqual(rp.sleep_between_retries, 500)
        self.assertEqual(rp.filter_expression, None)

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
    def test_get_defaults(self):
        # TODO: WritePolicy should inherit ReadPolicy
        wp = WritePolicy()
        self.assertEqual(wp.record_exists_action, RecordExistsAction.Update)
        self.assertEqual(wp.generation_policy, GenerationPolicy.NONE)
        self.assertEqual(wp.commit_level, CommitLevel.CommitAll)
        self.assertEqual(wp.generation, 0)
        self.assertEqual(wp.expiration, Expiration.NamespaceDefault)
        self.assertEqual(wp.send_key, False)
        self.assertEqual(wp.respond_per_each_op, False)
        self.assertEqual(wp.durable_delete, False)
        # TODO: already in ReadPolicy?
        self.assertEqual(wp.filter_expression, None)

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
