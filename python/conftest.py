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

"""
Pytest configuration to automatically load environment variables from aerospike.env
"""
import os
import pytest
from pathlib import Path


def load_env_file(env_file_path):
    """Load environment variables from a .env file"""
    if not os.path.exists(env_file_path):
        return
    
    with open(env_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Parse export VAR=value format
            if line.startswith('export '):
                line = line[7:]  # Remove 'export ' prefix
            
            if '=' in line:
                key, value = line.split('=', 1)
                # Remove quotes if present
                value = value.strip('"\'')
                os.environ[key] = value


def pytest_configure(config):
    """Called after command line options have been parsed and all plugins and initial conftest files been loaded."""
    # Load environment variables from aerospike.env (one directory up)
    env_dir = Path(__file__).parent.parent
    env_file = env_dir / "aerospike.env"
    load_env_file(env_file)

    # Local overrides (gitignored) for developer-specific settings like TLS cert paths
    env_local = env_dir / "aerospike.env.local"
    if env_local.exists():
        load_env_file(env_local)

    # Print loaded environment variables for debugging
    print(f"Loaded environment variables from {env_file}")
    print(f"CI environment variable: {os.environ.get('CI', 'NOT SET')}\n")
    
    # Ensure python path includes the python directory for imports
    import sys
    python_dir = Path(__file__).parent
    if str(python_dir) not in sys.path:
        sys.path.insert(0, str(python_dir))

@pytest.fixture(scope="session")
def aerospike_host():
    """Fixture providing the Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST')


@pytest.fixture(scope="session")
def use_services_alternate():
    """Fixture indicating whether to use services-alternate addresses (for containerized servers)"""
    return os.environ.get('AEROSPIKE_USE_SERVICES_ALTERNATE', '').lower() == 'true'


@pytest.fixture(scope="session") 
def aerospike_host_tls():
    """Fixture providing the TLS-enabled Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST_TLS')


@pytest.fixture(scope="session")
def aerospike_host_sec():
    """Fixture providing the security-enabled Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST_SEC')
