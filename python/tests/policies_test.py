from aerospike_async import (
    BasePolicy, QueryDuration, ReadPolicy, Replica, WritePolicy, ScanPolicy, QueryPolicy,
    ConsistencyLevel, RecordExistsAction, GenerationPolicy, 
    CommitLevel, Expiration, FilterExpression as fe
)


class TestBasePolicy:
    """Test BasePolicy functionality."""

    def test_set_and_get_fields(self):
        """Test setting and getting BasePolicy fields."""
        bp = BasePolicy()
        bp.consistency_level = ConsistencyLevel.CONSISTENCY_ALL
        bp.timeout = 20000
        bp.max_retries = 4
        bp.sleep_between_retries = 1000
        bp.socket_timeout = 5000
        filter_exp = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))
        bp.filter_expression = filter_exp

        assert bp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert bp.timeout == 20000
        assert bp.max_retries == 4
        assert bp.sleep_between_retries == 1000
        assert bp.socket_timeout == 5000
        assert bp.filter_expression == filter_exp

    def test_socket_timeout(self):
        """Test socket_timeout on BasePolicy."""
        bp = BasePolicy()
        bp.socket_timeout = 3000
        assert bp.socket_timeout == 3000


class TestWritePolicy:
    """Test WritePolicy functionality."""

    def test_set_and_get_fields(self):
        """Test setting and getting WritePolicy fields."""
        wp = WritePolicy()
        wp.record_exists_action = RecordExistsAction.UPDATE_ONLY
        wp.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
        wp.commit_level = CommitLevel.COMMIT_MASTER
        wp.generation = 4
        wp.expiration = Expiration.NEVER_EXPIRE
        wp.send_key = True
        wp.respond_per_each_op = True
        wp.durable_delete = True

        assert wp.record_exists_action == RecordExistsAction.UPDATE_ONLY
        assert wp.generation_policy == GenerationPolicy.EXPECT_GEN_EQUAL
        assert wp.commit_level == CommitLevel.COMMIT_MASTER
        assert wp.generation == 4
        assert wp.expiration == Expiration.NEVER_EXPIRE
        assert wp.send_key is True
        assert wp.respond_per_each_op is True
        assert wp.durable_delete is True

    def test_base_policy_inheritance(self):
        """Test that WritePolicy inherits BasePolicy fields."""
        wp = WritePolicy()
        wp.consistency_level = ConsistencyLevel.CONSISTENCY_ALL
        wp.timeout = 15000
        wp.max_retries = 3
        wp.sleep_between_retries = 500
        wp.socket_timeout = 3000
        filter_exp = fe.eq(fe.string_bin("status"), fe.string_val("active"))
        wp.filter_expression = filter_exp

        assert wp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert wp.timeout == 15000
        assert wp.max_retries == 3
        assert wp.sleep_between_retries == 500
        assert wp.socket_timeout == 3000
        assert wp.filter_expression == filter_exp

    def test_socket_timeout(self):
        """Test socket_timeout on WritePolicy."""
        wp = WritePolicy()
        wp.socket_timeout = 4000
        assert wp.socket_timeout == 4000

    def test_combined_base_and_write_policy_fields(self):
        """Test that WritePolicy can use both BasePolicy and WritePolicy fields together."""
        wp = WritePolicy()
        # Set BasePolicy fields
        wp.consistency_level = ConsistencyLevel.CONSISTENCY_ONE
        wp.timeout = 10000
        wp.max_retries = 2
        # Set WritePolicy-specific fields
        wp.record_exists_action = RecordExistsAction.REPLACE_ONLY
        wp.generation_policy = GenerationPolicy.EXPECT_GEN_GREATER
        wp.commit_level = CommitLevel.COMMIT_ALL
        wp.generation = 5
        wp.expiration = Expiration.NEVER_EXPIRE
        wp.send_key = False
        wp.durable_delete = True

        # Verify BasePolicy fields
        assert wp.consistency_level == ConsistencyLevel.CONSISTENCY_ONE
        assert wp.timeout == 10000
        assert wp.max_retries == 2
        # Verify WritePolicy fields
        assert wp.record_exists_action == RecordExistsAction.REPLACE_ONLY
        assert wp.generation_policy == GenerationPolicy.EXPECT_GEN_GREATER
        assert wp.commit_level == CommitLevel.COMMIT_ALL
        assert wp.generation == 5
        assert wp.expiration == Expiration.NEVER_EXPIRE
        assert wp.send_key is False
        assert wp.durable_delete is True

    def test_isinstance_base_policy(self):
        """Test that WritePolicy is an instance of BasePolicy."""
        wp = WritePolicy()
        assert isinstance(wp, BasePolicy)

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
            RecordExistsAction.UPDATE,
            RecordExistsAction.UPDATE_ONLY,
            RecordExistsAction.REPLACE,
            RecordExistsAction.REPLACE_ONLY,
            RecordExistsAction.CREATE_ONLY,
        ]

        for action in actions:
            wp.record_exists_action = action
            assert wp.record_exists_action == action

    def test_all_generation_policy_values(self):
        """Test all possible GenerationPolicy enum values."""
        wp = WritePolicy()

        policies = [
            GenerationPolicy.NONE,
            GenerationPolicy.EXPECT_GEN_EQUAL,
            GenerationPolicy.EXPECT_GEN_GREATER,
        ]

        for policy in policies:
            wp.generation_policy = policy
            assert wp.generation_policy == policy

    def test_all_commit_level_values(self):
        """Test all possible CommitLevel enum values."""
        wp = WritePolicy()

        commit_levels = [
            CommitLevel.COMMIT_ALL,
            CommitLevel.COMMIT_MASTER,
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

    def test_max_retries_default(self):
        """Test max_retries default value (int, not nullable)."""
        wp = WritePolicy()
        # Default should be 2 (per Rust core default)
        assert wp.max_retries == 2
        
        wp.max_retries = 5
        assert wp.max_retries == 5

        # Setting to 0 is valid (no retries)
        wp.max_retries = 0
        assert wp.max_retries == 0

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
        rp.consistency_level = ConsistencyLevel.CONSISTENCY_ALL
        rp.timeout = 20000
        rp.max_retries = 4
        rp.sleep_between_retries = 1000
        filter_exp = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))
        rp.filter_expression = filter_exp

        assert rp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert rp.timeout == 20000
        assert rp.max_retries == 4
        assert rp.sleep_between_retries == 1000
        assert rp.filter_expression == filter_exp

    def test_base_policy_inheritance(self):
        """Test that ReadPolicy inherits BasePolicy fields."""
        rp = ReadPolicy()
        rp.consistency_level = ConsistencyLevel.CONSISTENCY_ALL
        rp.timeout = 15000
        rp.max_retries = 3
        rp.sleep_between_retries = 500
        rp.socket_timeout = 3000
        filter_exp = fe.eq(fe.string_bin("status"), fe.string_val("active"))
        rp.filter_expression = filter_exp

        assert rp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert rp.timeout == 15000
        assert rp.max_retries == 3
        assert rp.sleep_between_retries == 500
        assert rp.socket_timeout == 3000
        assert rp.filter_expression == filter_exp

    def test_isinstance_base_policy(self):
        """Test that ReadPolicy is an instance of BasePolicy."""
        rp = ReadPolicy()
        assert isinstance(rp, BasePolicy)

    def test_socket_timeout(self):
        """Test socket_timeout on ReadPolicy."""
        rp = ReadPolicy()
        rp.socket_timeout = 3000
        assert rp.socket_timeout == 3000


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

    def test_base_policy_inheritance(self):
        """Test that ScanPolicy inherits BasePolicy fields."""
        sp = ScanPolicy()
        sp.consistency_level = ConsistencyLevel.CONSISTENCY_ALL
        sp.timeout = 15000
        sp.max_retries = 3
        sp.sleep_between_retries = 500
        sp.socket_timeout = 3000
        filter_exp = fe.eq(fe.string_bin("status"), fe.string_val("active"))
        sp.filter_expression = filter_exp

        assert sp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert sp.timeout == 15000
        assert sp.max_retries == 3
        assert sp.sleep_between_retries == 500
        assert sp.socket_timeout == 3000
        assert sp.filter_expression == filter_exp

    def test_combined_base_and_scan_policy_fields(self):
        """Test that ScanPolicy can use both BasePolicy and ScanPolicy fields together."""
        sp = ScanPolicy()
        # Set BasePolicy fields
        sp.consistency_level = ConsistencyLevel.CONSISTENCY_ONE
        sp.timeout = 10000
        sp.max_retries = 2
        # Set ScanPolicy-specific fields
        sp.max_concurrent_nodes = 4
        sp.record_queue_size = 2048
        sp.socket_timeout = 5000

        # Verify BasePolicy fields
        assert sp.consistency_level == ConsistencyLevel.CONSISTENCY_ONE
        assert sp.timeout == 10000
        assert sp.max_retries == 2
        # Verify ScanPolicy fields
        assert sp.max_concurrent_nodes == 4
        assert sp.record_queue_size == 2048
        assert sp.socket_timeout == 5000

    def test_isinstance_base_policy(self):
        """Test that ScanPolicy is an instance of BasePolicy."""
        sp = ScanPolicy()
        assert isinstance(sp, BasePolicy)


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

    def test_socket_timeout(self):
        """Test socket_timeout on QueryPolicy."""
        qp = QueryPolicy()
        qp.socket_timeout = 6000
        assert qp.socket_timeout == 6000

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
        assert qp.expected_duration == QueryDuration.LONG

        # Test all enum values
        qp.expected_duration = QueryDuration.LONG
        assert qp.expected_duration == QueryDuration.LONG

        qp.expected_duration = QueryDuration.SHORT
        assert qp.expected_duration == QueryDuration.SHORT

        qp.expected_duration = QueryDuration.LONG_RELAX_AP
        assert qp.expected_duration == QueryDuration.LONG_RELAX_AP

        # Test inequality
        assert qp.expected_duration != QueryDuration.LONG
        assert qp.expected_duration != QueryDuration.SHORT

    def test_replica(self):
        """Test replica field with Replica enum."""
        qp = QueryPolicy()

        # Test default value
        assert qp.replica == Replica.SEQUENCE

        # Test all enum values
        qp.replica = Replica.MASTER
        assert qp.replica == Replica.MASTER

        qp.replica = Replica.SEQUENCE
        assert qp.replica == Replica.SEQUENCE

        qp.replica = Replica.PREFER_RACK
        assert qp.replica == Replica.PREFER_RACK

        # Test inequality
        assert qp.replica != Replica.MASTER
        assert qp.replica != Replica.SEQUENCE

    def test_base_policy_inheritance(self):
        """Test that QueryPolicy inherits BasePolicy fields."""
        qp = QueryPolicy()
        qp.consistency_level = ConsistencyLevel.CONSISTENCY_ALL
        qp.timeout = 15000
        qp.max_retries = 3
        qp.sleep_between_retries = 500
        qp.socket_timeout = 3000
        filter_exp = fe.eq(fe.string_bin("status"), fe.string_val("active"))
        qp.filter_expression = filter_exp

        assert qp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert qp.timeout == 15000
        assert qp.max_retries == 3
        assert qp.sleep_between_retries == 500
        assert qp.socket_timeout == 3000
        assert qp.filter_expression == filter_exp

    def test_combined_base_and_query_policy_fields(self):
        """Test that QueryPolicy can use both BasePolicy and QueryPolicy fields together."""
        qp = QueryPolicy()
        # Set BasePolicy fields
        qp.consistency_level = ConsistencyLevel.CONSISTENCY_ONE
        qp.timeout = 10000
        qp.max_retries = 2
        # Set QueryPolicy-specific fields
        qp.max_concurrent_nodes = 4
        qp.record_queue_size = 2048
        qp.records_per_second = 2000
        qp.max_records = 50000
        qp.expected_duration = QueryDuration.SHORT
        qp.replica = Replica.PREFER_RACK

        # Verify BasePolicy fields
        assert qp.consistency_level == ConsistencyLevel.CONSISTENCY_ONE
        assert qp.timeout == 10000
        assert qp.max_retries == 2
        # Verify QueryPolicy fields
        assert qp.max_concurrent_nodes == 4
        assert qp.record_queue_size == 2048
        assert qp.records_per_second == 2000
        assert qp.max_records == 50000
        assert qp.expected_duration == QueryDuration.SHORT
        assert qp.replica == Replica.PREFER_RACK

    def test_isinstance_base_policy(self):
        """Test that QueryPolicy is an instance of BasePolicy."""
        qp = QueryPolicy()
        assert isinstance(qp, BasePolicy)

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
        qp.expected_duration = QueryDuration.SHORT
        qp.replica = Replica.PREFER_RACK

        base = BasePolicy()
        base.timeout = 10000
        base.max_retries = 5
        qp.base_policy = base

        # Verify all fields
        assert qp.max_concurrent_nodes == 4
        assert qp.record_queue_size == 2048
        assert qp.records_per_second == 2000
        assert qp.max_records == 50000
        assert qp.expected_duration == QueryDuration.SHORT
        assert qp.replica == Replica.PREFER_RACK
        assert qp.base_policy.timeout == 10000
        assert qp.base_policy.max_retries == 5
