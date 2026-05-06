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

"""Unit tests for top-level policy classes (compression_threshold, circuit breaker)."""

import pytest

from aerospike_async import (
    BasePolicy,
    BatchPolicy,
    ClientPolicy,
    QueryPolicy,
    ReadPolicy,
    WritePolicy,
)


class TestCompressionThresholdDefault:
    """Default ``compression_threshold`` is 128 bytes on every BasePolicy variant."""

    @pytest.mark.parametrize("policy_cls", [BasePolicy, ReadPolicy, WritePolicy, QueryPolicy, BatchPolicy])
    def test_default_is_128(self, policy_cls):
        p = policy_cls()
        assert p.compression_threshold == 128


class TestCompressionThresholdRoundTrip:
    """Setting ``compression_threshold`` round-trips through the getter."""

    @pytest.mark.parametrize("policy_cls", [BasePolicy, ReadPolicy, WritePolicy, QueryPolicy, BatchPolicy])
    def test_roundtrip(self, policy_cls):
        p = policy_cls()
        p.compression_threshold = 4096
        assert p.compression_threshold == 4096
        # Zero disables the threshold (every command goes through zlib).
        p.compression_threshold = 0
        assert p.compression_threshold == 0


class TestCompressionThresholdFromFields:
    """``from_fields`` builders accept ``compression_threshold`` keyword."""

    def test_read_policy_from_fields(self):
        rp = ReadPolicy.from_fields(use_compression=True, compression_threshold=1024)
        assert rp.use_compression is True
        assert rp.compression_threshold == 1024

    def test_write_policy_from_fields(self):
        wp = WritePolicy.from_fields(use_compression=True, compression_threshold=2048)
        assert wp.use_compression is True
        assert wp.compression_threshold == 2048

    def test_batch_policy_from_fields(self):
        bp = BatchPolicy.from_fields(use_compression=True, compression_threshold=512)
        assert bp.use_compression is True
        assert bp.compression_threshold == 512


class TestCircuitBreaker:
    """Circuit-breaker fields on ClientPolicy: defaults, round-trips, and exception class."""

    def test_default_construction(self):
        cp = ClientPolicy()
        # Defaults: max_error_rate=100 errors per error_rate_window=1 tick.
        assert cp.max_error_rate == 100
        assert cp.error_rate_window == 1

    def test_max_error_rate_roundtrip(self):
        cp = ClientPolicy()
        cp.max_error_rate = 250
        assert cp.max_error_rate == 250
        # Zero disables the breaker entirely.
        cp.max_error_rate = 0
        assert cp.max_error_rate == 0

    def test_error_rate_window_roundtrip(self):
        cp = ClientPolicy()
        cp.error_rate_window = 4
        assert cp.error_rate_window == 4

    def test_max_error_rate_exception_class(self):
        from aerospike_async.exceptions import AerospikeError, MaxErrorRate

        # MaxErrorRate is exposed and is a subclass of AerospikeError so users
        # can catch the broader category if they want.
        assert issubclass(MaxErrorRate, AerospikeError)
