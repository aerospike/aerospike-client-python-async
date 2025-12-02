from aerospike_async import (
    BasePolicy, QueryDuration, ReadPolicy, Replica, WritePolicy, ScanPolicy, QueryPolicy,
    ConsistencyLevel, RecordExistsAction, GenerationPolicy, 
    CommitLevel, Expiration, FilterExpression as fe
)


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

    def test_base_policy_inheritance(self):
        """Test that WritePolicy inherits BasePolicy fields."""
        wp = WritePolicy()
        wp.consistency_level = ConsistencyLevel.ConsistencyAll
        wp.timeout = 15000
        wp.max_retries = 3
        wp.sleep_between_retries = 500
        filter_exp = fe.eq(fe.string_bin("status"), fe.string_val("active"))
        wp.filter_expression = filter_exp

        assert wp.consistency_level == ConsistencyLevel.ConsistencyAll
        assert wp.timeout == 15000
        assert wp.max_retries == 3
        assert wp.sleep_between_retries == 500
        assert wp.filter_expression == filter_exp

    def test_combined_base_and_write_policy_fields(self):
        """Test that WritePolicy can use both BasePolicy and WritePolicy fields together."""
        wp = WritePolicy()
        # Set BasePolicy fields
        wp.consistency_level = ConsistencyLevel.ConsistencyOne
        wp.timeout = 10000
        wp.max_retries = 2
        # Set WritePolicy-specific fields
        wp.record_exists_action = RecordExistsAction.ReplaceOnly
        wp.generation_policy = GenerationPolicy.ExpectGenGreater
        wp.commit_level = CommitLevel.CommitAll
        wp.generation = 5
        wp.expiration = Expiration.NEVER_EXPIRE
        wp.send_key = False
        wp.durable_delete = True

        # Verify BasePolicy fields
        assert wp.consistency_level == ConsistencyLevel.ConsistencyOne
        assert wp.timeout == 10000
        assert wp.max_retries == 2
        # Verify WritePolicy fields
        assert wp.record_exists_action == RecordExistsAction.ReplaceOnly
        assert wp.generation_policy == GenerationPolicy.ExpectGenGreater
        assert wp.commit_level == CommitLevel.CommitAll
        assert wp.generation == 5
        assert wp.expiration == Expiration.NEVER_EXPIRE
        assert wp.send_key is False
        assert wp.durable_delete is True

    def test_filter_expression_clear(self):
        """Test clearing filter_expression on WritePolicy."""
        wp = WritePolicy()
        filter_exp = fe.eq(fe.string_bin("name"), fe.string_val("test"))
        wp.filter_expression = filter_exp
        assert wp.filter_expression == filter_exp

        # Clear the filter expression
        wp.filter_expression = None
        assert wp.filter_expression is None

    def test_all_record_exists_action_values(self):
        """Test all possible RecordExistsAction enum values."""
        wp = WritePolicy()

        actions = [
            RecordExistsAction.Update,
            RecordExistsAction.UpdateOnly,
            RecordExistsAction.Replace,
            RecordExistsAction.ReplaceOnly,
            RecordExistsAction.CreateOnly,
        ]

        for action in actions:
            wp.record_exists_action = action
            assert wp.record_exists_action == action

    def test_all_generation_policy_values(self):
        """Test all possible GenerationPolicy enum values."""
        wp = WritePolicy()

        policies = [
            GenerationPolicy.None_,
            GenerationPolicy.ExpectGenEqual,
            GenerationPolicy.ExpectGenGreater,
        ]

        for policy in policies:
            wp.generation_policy = policy
            assert wp.generation_policy == policy

    def test_all_commit_level_values(self):
        """Test all possible CommitLevel enum values."""
        wp = WritePolicy()

        commit_levels = [
            CommitLevel.CommitAll,
            CommitLevel.CommitMaster,
        ]

        for level in commit_levels:
            wp.commit_level = level
            assert wp.commit_level == level

    def test_expiration_values(self):
        """Test different Expiration values."""
        wp = WritePolicy()

        # Test NEVER_EXPIRE
        wp.expiration = Expiration.NEVER_EXPIRE
        assert wp.expiration == Expiration.NEVER_EXPIRE

        # Test NAMESPACE_DEFAULT
        wp.expiration = Expiration.NAMESPACE_DEFAULT
        assert wp.expiration == Expiration.NAMESPACE_DEFAULT

        # Test DONT_UPDATE
        wp.expiration = Expiration.DONT_UPDATE
        assert wp.expiration == Expiration.DONT_UPDATE

        # Test seconds
        exp_seconds = Expiration.seconds(3600)
        wp.expiration = exp_seconds
        assert wp.expiration == exp_seconds

    def test_max_retries_none(self):
        """Test setting max_retries to None."""
        wp = WritePolicy()
        wp.max_retries = 5
        assert wp.max_retries == 5

        wp.max_retries = None
        assert wp.max_retries is None

    def test_generation_edge_cases(self):
        """Test generation field with various values."""
        wp = WritePolicy()

        # Test zero
        wp.generation = 0
        assert wp.generation == 0

        # Test large value
        wp.generation = 4294967295  # max u32
        assert wp.generation == 4294967295

        # Test typical value
        wp.generation = 100
        assert wp.generation == 100

    def test_boolean_fields_all_combinations(self):
        """Test all combinations of boolean fields."""
        wp = WritePolicy()

        # Test send_key
        wp.send_key = True
        assert wp.send_key is True
        wp.send_key = False
        assert wp.send_key is False

        # Test respond_per_each_op
        wp.respond_per_each_op = True
        assert wp.respond_per_each_op is True
        wp.respond_per_each_op = False
        assert wp.respond_per_each_op is False

        # Test durable_delete
        wp.durable_delete = True
        assert wp.durable_delete is True
        wp.durable_delete = False
        assert wp.durable_delete is False


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
        # Note: fail_on_cluster_change field doesn't exist in TLS branch
        # qp.fail_on_cluster_change = False

        assert qp.max_concurrent_nodes == 1
        assert qp.record_queue_size == 1023
        # Note: fail_on_cluster_change field doesn't exist in TLS branch
        # assert qp.fail_on_cluster_change is False

    def test_records_per_second(self):
        """Test records_per_second field."""
        qp = QueryPolicy()

        # Test default value
        assert qp.records_per_second == 0

        # Test setting values
        qp.records_per_second = 1000
        assert qp.records_per_second == 1000

        qp.records_per_second = 5000
        assert qp.records_per_second == 5000

        # Test zero (no limit)
        qp.records_per_second = 0
        assert qp.records_per_second == 0

    def test_max_records(self):
        """Test max_records field."""
        qp = QueryPolicy()

        # Test default value
        assert qp.max_records == 0

        # Test setting values
        qp.max_records = 10000
        assert qp.max_records == 10000

        qp.max_records = 50000
        assert qp.max_records == 50000

        # Test zero (no limit)
        qp.max_records = 0
        assert qp.max_records == 0

        # Test large value
        qp.max_records = 18446744073709551615  # max u64
        assert qp.max_records == 18446744073709551615

    def test_expected_duration(self):
        """Test expected_duration field with QueryDuration enum."""
        qp = QueryPolicy()

        # Test default value
        assert qp.expected_duration == QueryDuration.Long

        # Test all enum values
        qp.expected_duration = QueryDuration.Long
        assert qp.expected_duration == QueryDuration.Long

        qp.expected_duration = QueryDuration.Short
        assert qp.expected_duration == QueryDuration.Short

        qp.expected_duration = QueryDuration.LongRelaxAP
        assert qp.expected_duration == QueryDuration.LongRelaxAP

        # Test inequality
        assert qp.expected_duration != QueryDuration.Long
        assert qp.expected_duration != QueryDuration.Short

    def test_replica(self):
        """Test replica field with Replica enum."""
        qp = QueryPolicy()

        # Test default value
        assert qp.replica == Replica.Sequence

        # Test all enum values
        qp.replica = Replica.Master
        assert qp.replica == Replica.Master

        qp.replica = Replica.Sequence
        assert qp.replica == Replica.Sequence

        qp.replica = Replica.PreferRack
        assert qp.replica == Replica.PreferRack

        # Test inequality
        assert qp.replica != Replica.Master
        assert qp.replica != Replica.Sequence

    def test_base_policy(self):
        """Test base_policy field."""
        qp = QueryPolicy()

        # Test default base_policy exists
        assert qp.base_policy is not None
        assert isinstance(qp.base_policy, BasePolicy)

        # Test setting a new base_policy
        new_base = BasePolicy()
        new_base.timeout = 5000
        new_base.max_retries = 3

        qp.base_policy = new_base
        assert qp.base_policy is not None
        assert qp.base_policy.timeout == 5000
        assert qp.base_policy.max_retries == 3

    def test_all_fields_together(self):
        """Test setting all QueryPolicy fields together."""
        qp = QueryPolicy()

        # Set all fields
        qp.max_concurrent_nodes = 4
        qp.record_queue_size = 2048
        qp.records_per_second = 2000
        qp.max_records = 50000
        qp.expected_duration = QueryDuration.Short
        qp.replica = Replica.PreferRack

        base = BasePolicy()
        base.timeout = 10000
        base.max_retries = 5
        qp.base_policy = base

        # Verify all fields
        assert qp.max_concurrent_nodes == 4
        assert qp.record_queue_size == 2048
        assert qp.records_per_second == 2000
        assert qp.max_records == 50000
        assert qp.expected_duration == QueryDuration.Short
        assert qp.replica == Replica.PreferRack
        assert qp.base_policy.timeout == 10000
        assert qp.base_policy.max_retries == 5
