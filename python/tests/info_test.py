"""Tests for client info command methods."""

import pytest
from fixtures import TestFixtureConnection


class TestInfoCommands(TestFixtureConnection):
    """Test info command functionality."""

    async def test_node_names(self, client):
        """Test getting list of node names."""
        node_names = await client.node_names()
        
        assert isinstance(node_names, list)
        assert len(node_names) > 0, "Should have at least one node"
        assert all(isinstance(name, str) for name in node_names), "All node names should be strings"
        print(f"Found {len(node_names)} node(s): {node_names}")

    async def test_info_build(self, client):
        """Test info command for build information."""
        response = await client.info("build")
        
        assert isinstance(response, dict)
        assert len(response) > 0, "Build info should contain data"
        # Build info typically has a key like "build" with version info
        print(f"Build info: {response}")

    async def test_info_namespaces(self, client):
        """Test info command for namespaces."""
        response = await client.info("namespaces")
        
        assert isinstance(response, dict)
        # Namespaces info typically has a "namespaces" key with comma-separated list
        assert len(response) > 0, "Namespaces info should contain data"
        print(f"Namespaces info: {response}")

    async def test_info_statistics(self, client):
        """Test info command for statistics."""
        response = await client.info("statistics")
        
        assert isinstance(response, dict)
        assert len(response) > 0, "Statistics should contain data"
        # Statistics typically has many key-value pairs
        print(f"Statistics keys: {list(response.keys())[:10]}...")  # Print first 10 keys

    async def test_info_on_all_nodes_build(self, client):
        """Test info command on all nodes for build information."""
        response = await client.info_on_all_nodes("build")
        
        assert isinstance(response, dict)
        assert len(response) > 0, "Should have responses from at least one node"
        
        # Each value should be a dict (the info response from that node)
        for node_name, node_response in response.items():
            assert isinstance(node_name, str), "Node names should be strings"
            assert isinstance(node_response, dict), "Node responses should be dictionaries"
            assert len(node_response) > 0, "Node response should contain data"
        
        print(f"Build info from {len(response)} node(s)")

    async def test_info_on_all_nodes_namespaces(self, client):
        """Test info command on all nodes for namespaces."""
        response = await client.info_on_all_nodes("namespaces")
        
        assert isinstance(response, dict)
        assert len(response) > 0, "Should have responses from at least one node"
        
        # Verify all nodes return namespace info
        for node_name, node_response in response.items():
            assert isinstance(node_response, dict), "Node responses should be dictionaries"
        
        print(f"Namespaces info from {len(response)} node(s)")

    async def test_info_on_all_nodes_statistics(self, client):
        """Test info command on all nodes for statistics."""
        response = await client.info_on_all_nodes("statistics")
        
        assert isinstance(response, dict)
        assert len(response) > 0, "Should have responses from at least one node"
        
        # Statistics should have many keys from each node
        for node_name, node_response in response.items():
            assert isinstance(node_response, dict), "Node responses should be dictionaries"
            assert len(node_response) > 0, "Statistics should contain data"
        
        print(f"Statistics from {len(response)} node(s)")

    async def test_info_namespace_details(self, client):
        """Test info command for namespace details."""
        # First get the list of namespaces
        namespaces_info = await client.info("namespaces")
        
        # Extract namespace names from the response
        # The response format is typically {"namespaces": "ns1,ns2,ns3"}
        namespace_names = []
        for value in namespaces_info.values():
            if isinstance(value, str) and value:
                # Split comma-separated namespace list
                namespace_names.extend([ns.strip() for ns in value.split(",") if ns.strip()])
        
        if not namespace_names:
            pytest.skip("No namespaces found to test")
        
        # Test info for the first namespace
        test_namespace = namespace_names[0]
        response = await client.info(f"namespace/{test_namespace}")
        
        assert isinstance(response, dict)
        assert len(response) > 0, "Namespace details should contain data"
        print(f"Namespace '{test_namespace}' details: {list(response.keys())[:5]}...")  # Print first 5 keys

