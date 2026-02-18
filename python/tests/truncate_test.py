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

import time
import pytest
from aerospike_async.exceptions import ServerError, ResultCode
from fixtures import TestFixtureInsertRecord


class TestTruncate(TestFixtureInsertRecord):
    """Test client.truncate() method functionality."""

    async def test_truncate(self, client):
        """Test basic truncate operation."""
        retval = await client.truncate("test", "test", None)
        assert retval is None

    async def test_truncate_before_nanos(self, client):
        """Test truncate operation with before_nanos parameter."""
        retval = await client.truncate("test", "test", before_nanos=0)
        assert retval is None

    async def test_truncate_future_timestamp(self, client):
        """Test truncate operation with future timestamp.
        
        Server should reject future timestamps with PARAMETER_ERROR.
        """
        seconds_in_future = 1000
        future_threshold = time.time_ns() + seconds_in_future * 10**9
        isolated_set = "test_truncate_future_isolated"

        try:
            await client.truncate("test", isolated_set, future_threshold)
        except ServerError as e:
            assert e.result_code == ResultCode.PARAMETER_ERROR
