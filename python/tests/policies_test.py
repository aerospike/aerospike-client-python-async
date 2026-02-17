import pytest
from aerospike_async import (
    BasePolicy, QueryDuration, ReadPolicy, Replica, WritePolicy, QueryPolicy, BatchPolicy,
    ConsistencyLevel, RecordExistsAction, GenerationPolicy,
    CommitLevel, Expiration, FilterExpression as fe
)


class TestBasePolicy:
    """Test BasePolicy functionality."""

    def test_set_and_get_fields(self):
        """Test setting and getting BasePolicy fields."""
        bp = BasePolicy()
        bp.consistency_level = ConsistencyLevel.CONSISTENCY_ALL
        bp.total_timeout = 20000
        bp.max_retries = 4
        bp.sleep_between_retries = 1000
        bp.socket_timeout = 5000
        filter_exp = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))
        bp.filter_expression = filter_exp

        assert bp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert bp.total_timeout == 20000
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
        wp.total_timeout = 15000
        wp.max_retries = 3
        wp.sleep_between_retries = 500
        wp.socket_timeout = 3000
        filter_exp = fe.eq(fe.string_bin("status"), fe.string_val("active"))
        wp.filter_expression = filter_exp

        assert wp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert wp.total_timeout == 15000
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
        wp.total_timeout = 10000
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
        assert wp.total_timeout == 10000
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
        rp.total_timeout = 20000
        rp.max_retries = 4
        rp.sleep_between_retries = 1000
        filter_exp = fe.eq(fe.string_bin("brand"), fe.string_val("Peykan"))
        rp.filter_expression = filter_exp

        assert rp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert rp.total_timeout == 20000
        assert rp.max_retries == 4
        assert rp.sleep_between_retries == 1000
        assert rp.filter_expression == filter_exp

    def test_base_policy_inheritance(self):
        """Test that ReadPolicy inherits BasePolicy fields."""
        rp = ReadPolicy()
        rp.consistency_level = ConsistencyLevel.CONSISTENCY_ALL
        rp.total_timeout = 15000
        rp.max_retries = 3
        rp.sleep_between_retries = 500
        rp.socket_timeout = 3000
        filter_exp = fe.eq(fe.string_bin("status"), fe.string_val("active"))
        rp.filter_expression = filter_exp

        assert rp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert rp.total_timeout == 15000
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
        qp.total_timeout = 15000
        qp.max_retries = 3
        qp.sleep_between_retries = 500
        qp.socket_timeout = 3000
        filter_exp = fe.eq(fe.string_bin("status"), fe.string_val("active"))
        qp.filter_expression = filter_exp

        assert qp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert qp.total_timeout == 15000
        assert qp.max_retries == 3
        assert qp.sleep_between_retries == 500
        assert qp.socket_timeout == 3000
        assert qp.filter_expression == filter_exp

    def test_combined_base_and_query_policy_fields(self):
        """Test that QueryPolicy can use both BasePolicy and QueryPolicy fields together."""
        qp = QueryPolicy()
        # Set BasePolicy fields
        qp.consistency_level = ConsistencyLevel.CONSISTENCY_ONE
        qp.total_timeout = 10000
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
        assert qp.total_timeout == 10000
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
        new_base.total_timeout = 5000
        new_base.max_retries = 3

        qp.base_policy = new_base
        assert qp.base_policy is not None
        assert qp.base_policy.total_timeout == 5000
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
        base.total_timeout = 10000
        base.max_retries = 5
        qp.base_policy = base

        # Verify all fields
        assert qp.max_concurrent_nodes == 4
        assert qp.record_queue_size == 2048
        assert qp.records_per_second == 2000
        assert qp.max_records == 50000
        assert qp.expected_duration == QueryDuration.SHORT
        assert qp.replica == Replica.PREFER_RACK
        assert qp.base_policy.total_timeout == 10000
        assert qp.base_policy.max_retries == 5


class TestBasePolicySync:
    """Test that BasePolicy properties are synced between direct access and base_policy."""

    def test_read_policy_base_policy_sync(self):
        """Test that ReadPolicy direct property access syncs with base_policy."""
        rp = ReadPolicy()

        # Set properties directly on ReadPolicy
        rp.total_timeout = 999
        rp.max_retries = 5
        rp.sleep_between_retries = 100
        rp.consistency_level = ConsistencyLevel.CONSISTENCY_ALL
        rp.socket_timeout = 2000

        # Verify they're synced with base_policy
        assert rp.total_timeout == 999
        assert rp.base_policy.total_timeout == 999
        assert rp.max_retries == 5
        assert rp.base_policy.max_retries == 5
        assert rp.sleep_between_retries == 100
        assert rp.base_policy.sleep_between_retries == 100
        assert rp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert rp.base_policy.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert rp.socket_timeout == 2000
        assert rp.base_policy.socket_timeout == 2000

    def test_write_policy_base_policy_sync(self):
        """Test that WritePolicy direct property access syncs with base_policy."""
        wp = WritePolicy()

        # Set properties directly on WritePolicy
        wp.total_timeout = 888
        wp.max_retries = 3
        wp.sleep_between_retries = 200
        wp.consistency_level = ConsistencyLevel.CONSISTENCY_ONE
        wp.socket_timeout = 3000

        # Verify they're synced with base_policy
        assert wp.total_timeout == 888
        assert wp.base_policy.total_timeout == 888
        assert wp.max_retries == 3
        assert wp.base_policy.max_retries == 3
        assert wp.sleep_between_retries == 200
        assert wp.base_policy.sleep_between_retries == 200
        assert wp.consistency_level == ConsistencyLevel.CONSISTENCY_ONE
        assert wp.base_policy.consistency_level == ConsistencyLevel.CONSISTENCY_ONE
        assert wp.socket_timeout == 3000
        assert wp.base_policy.socket_timeout == 3000

    def test_query_policy_base_policy_sync(self):
        """Test that QueryPolicy direct property access syncs with base_policy."""
        qp = QueryPolicy()

        # Set properties directly on QueryPolicy
        qp.total_timeout = 777
        qp.max_retries = 4
        qp.sleep_between_retries = 300
        qp.consistency_level = ConsistencyLevel.CONSISTENCY_ALL
        qp.socket_timeout = 4000

        # Verify they're synced with base_policy
        assert qp.total_timeout == 777
        assert qp.base_policy.total_timeout == 777
        assert qp.max_retries == 4
        assert qp.base_policy.max_retries == 4
        assert qp.sleep_between_retries == 300
        assert qp.base_policy.sleep_between_retries == 300
        assert qp.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert qp.base_policy.consistency_level == ConsistencyLevel.CONSISTENCY_ALL
        assert qp.socket_timeout == 4000
        assert qp.base_policy.socket_timeout == 4000

    def test_batch_policy_base_policy_sync(self):
        """Test that BatchPolicy direct property access syncs with base_policy."""
        bp = BatchPolicy()

        # Set properties directly on BatchPolicy
        bp.total_timeout = 666
        bp.max_retries = 2
        bp.sleep_between_retries = 400
        bp.consistency_level = ConsistencyLevel.CONSISTENCY_ONE
        bp.socket_timeout = 5000

        # Verify they're synced with base_policy
        assert bp.total_timeout == 666
        assert bp.base_policy.total_timeout == 666
        assert bp.max_retries == 2
        assert bp.base_policy.max_retries == 2
        assert bp.sleep_between_retries == 400
        assert bp.base_policy.sleep_between_retries == 400
        assert bp.consistency_level == ConsistencyLevel.CONSISTENCY_ONE
        assert bp.base_policy.consistency_level == ConsistencyLevel.CONSISTENCY_ONE
        assert bp.socket_timeout == 5000
        assert bp.base_policy.socket_timeout == 5000

    def test_base_policy_clone_reflects_current_state(self):
        """Test that base_policy getter returns a clone that reflects current state."""
        bp = BatchPolicy()
        bp.total_timeout = 999

        # Get base_policy (returns a clone)
        base = bp.base_policy
        assert base.total_timeout == 999

        # Modify the policy directly
        bp.total_timeout = 123

        # The original clone should still have old value (it's a clone)
        assert base.total_timeout == 999

        # But getting base_policy again should have new value
        assert bp.base_policy.total_timeout == 123

    def test_base_policy_reassignment_syncs(self):
        """Test that reassigning base_policy syncs with direct property access."""
        bp = BatchPolicy()
        bp.total_timeout = 999

        # Get base_policy, modify it, and reassign
        base = bp.base_policy
        base.total_timeout = 123
        bp.base_policy = base

        # Both should now be synced
        assert bp.total_timeout == 123
        assert bp.base_policy.total_timeout == 123

    def test_filter_expression_sync(self):
        """Test that filter_expression syncs correctly across all policy types."""
        filter_exp = fe.eq(fe.string_bin("test"), fe.string_val("value"))

        # Test ReadPolicy
        rp = ReadPolicy()
        rp.filter_expression = filter_exp
        assert rp.filter_expression == filter_exp
        assert rp.base_policy.filter_expression == filter_exp

        # Test WritePolicy
        wp = WritePolicy()
        wp.filter_expression = filter_exp
        assert wp.filter_expression == filter_exp
        assert wp.base_policy.filter_expression == filter_exp

        # Test QueryPolicy
        qp = QueryPolicy()
        qp.filter_expression = filter_exp
        assert qp.filter_expression == filter_exp
        assert qp.base_policy.filter_expression == filter_exp

        # Test BatchPolicy
        bp = BatchPolicy()
        bp.filter_expression = filter_exp
        assert bp.filter_expression == filter_exp
        assert bp.base_policy.filter_expression == filter_exp


class TestSocketTimeout:
    """Test that socket_timeout actually enforces socket I/O timeouts."""

    @pytest.mark.asyncio
    async def test_socket_timeout_raises_timeout_error(self, aerospike_host):
        """Test that socket_timeout raises TimeoutError on slow socket operations.

        Note: This test may not always timeout on fast networks (e.g., localhost).
        socket_timeout applies to individual socket read/write operations, which
        can complete very quickly on local networks.
        """
        from aerospike_async import new_client, ClientPolicy, QueryPolicy, Statement, PartitionFilter
        from aerospike_async.exceptions import ClientError, TimeoutError

        # Setup client
        cp = ClientPolicy()
        cp.use_services_alternate = True
        client = await new_client(cp, aerospike_host)

        try:
            # Create a query policy with extremely short socket_timeout (1ms)
            # socket_timeout applies to individual socket read/write operations
            # With 1ms, socket I/O may timeout due to network latency
            qp = QueryPolicy()
            qp.socket_timeout = 1  # 1ms - extremely short, may timeout on socket I/O
            qp.total_timeout = 100  # 100ms - hard cap so the test can't hang
            qp.max_retries = 0  # No retries — fail immediately on first timeout

            stmt = Statement("test", "test", None)

            # Attempt query with very short socket_timeout
            # Query operations involve multiple socket reads, so more likely to timeout
            try:
                recordset = await client.query(qp, PartitionFilter.all(), stmt)
                # If we get here, the operation completed faster than 1ms (possible on localhost)
                # Consume the recordset to avoid warnings
                async for _ in recordset:
                    pass
                recordset.close()
                # On fast networks, socket operations may complete before timeout
                # This is acceptable - the test verifies socket_timeout can be set
                pytest.skip("Socket operations completed faster than 1ms timeout - network too fast to verify timeout")
            except (TimeoutError, ClientError) as e:
                # TimeoutError: socket timeout on first attempt
                # ClientError: max retries exceeded after repeated timeouts
                error_msg = str(e).lower()
                assert "timeout" in error_msg or "retries" in error_msg, \
                    f"Expected timeout or retry error, got: {e}"

        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_socket_timeout_not_triggered_on_fast_operation(self, aerospike_host):
        """Test that socket_timeout doesn't trigger on fast socket operations."""
        from aerospike_async import new_client, ClientPolicy, WritePolicy, ReadPolicy, Key

        # Setup client
        cp = ClientPolicy()
        cp.use_services_alternate = True
        client = await new_client(cp, aerospike_host)

        try:
            # Create policies with reasonable socket_timeout
            wp = WritePolicy()
            wp.socket_timeout = 1000  # 1 second - should be plenty for local operations
            rp = ReadPolicy()
            rp.socket_timeout = 1000

            key = Key("test", "test", "socket_timeout_fast_test")

            # Write a record
            await client.put(wp, key, {"test": "value"})

            # Read it back - should complete quickly without timeout
            record = await client.get(rp, key, None)
            assert record is not None
            assert record.bins["test"] == "value"

            # Clean up
            await client.delete(wp, key)

        finally:
            await client.close()


class TestTotalTimeout:
    """Test that total_timeout actually enforces operation timeouts (client-side TimeoutError)."""

    @pytest.mark.asyncio
    async def test_total_timeout_raises_timeout_error(self, aerospike_host):
        """Test that total_timeout raises TimeoutError (client-side timeout)."""
        from aerospike_async import new_client, ClientPolicy, QueryPolicy, Statement, PartitionFilter
        from aerospike_async.exceptions import TimeoutError

        # Setup client
        cp = ClientPolicy()
        cp.use_services_alternate = True
        client = await new_client(cp, aerospike_host)

        try:
            # Create a query policy with extremely short timeout (1ms)
            # This should timeout before the server can respond, triggering client-side TimeoutError
            qp = QueryPolicy()
            qp.total_timeout = 1  # 1ms - extremely short, should definitely timeout

            stmt = Statement("test", "test", None)

            # Attempt query with very short timeout
            # With 1ms timeout, this should almost certainly timeout due to network latency
            with pytest.raises(TimeoutError):
                recordset = await client.query(qp, PartitionFilter.all(), stmt)
                # If we somehow get here, consume the recordset
                async for _ in recordset:
                    pass
                recordset.close()

        finally:
            await client.close()
