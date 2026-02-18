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

"""Tests for set_xdr_filter functionality."""

import pytest
from aerospike_async import FilterExpression
from fixtures import TestFixtureConnection


class TestSetXdrFilter(TestFixtureConnection):
    """Test set_xdr_filter functionality.

    Note: These tests require XDR to be configured on the server.
    Tests will be skipped if XDR is not available.
    """

    async def get_xdr_datacenter(self, client):
        """Helper to get XDR datacenter name from server config.

        Returns the datacenter name if XDR is configured, None otherwise.
        """
        try:
            node_names = await client.node_names()
            if not node_names:
                return None

            node = await client.get_node(node_names[0])
            response = await node.info("get-config:context=xdr")

            # Parse the response to extract datacenter name
            # Format is typically "dc=<name>;..."
            for key, value in response.items():
                if "dc=" in value:
                    # Extract dc name from the response
                    parts = value.split(";")
                    for part in parts:
                        if part.startswith("dc="):
                            return part.split("=")[1]
                    # Try alternate parsing
                    if "," in value:
                        for segment in value.split(","):
                            if "dc=" in segment:
                                dc_part = segment.split("dc=")[-1]
                                return dc_part.split(";")[0].split(",")[0]
            return None
        except Exception:
            return None

    async def test_set_xdr_filter_with_expression(self, client):
        """Test setting XDR filter with an expression."""
        dc = await self.get_xdr_datacenter(client)
        if dc is None:
            pytest.skip("XDR not configured on server")

        # Create a simple filter expression
        expr = FilterExpression.eq(
            FilterExpression.int_bin("bin1"),
            FilterExpression.int_val(6)
        )

        # Set the XDR filter
        await client.set_xdr_filter(
            datacenter=dc,
            namespace="test",
            filter_expression=expr,
        )

    async def test_set_xdr_filter_clear(self, client):
        """Test clearing XDR filter by passing None."""
        dc = await self.get_xdr_datacenter(client)
        if dc is None:
            pytest.skip("XDR not configured on server")

        # Clear the filter by passing None
        await client.set_xdr_filter(
            datacenter=dc,
            namespace="test",
            filter_expression=None,
        )

    async def test_set_xdr_filter_complex_expression(self, client):
        """Test setting XDR filter with a complex expression."""
        dc = await self.get_xdr_datacenter(client)
        if dc is None:
            pytest.skip("XDR not configured on server")

        # Create a more complex filter expression
        expr = FilterExpression.and_([
            FilterExpression.gt(
                FilterExpression.int_bin("age"),
                FilterExpression.int_val(21)
            ),
            FilterExpression.eq(
                FilterExpression.string_bin("status"),
                FilterExpression.string_val("active")
            )
        ])

        # Set the XDR filter
        await client.set_xdr_filter(
            datacenter=dc,
            namespace="test",
            filter_expression=expr,
        )

        # Clean up - clear the filter
        await client.set_xdr_filter(
            datacenter=dc,
            namespace="test",
            filter_expression=None,
        )
