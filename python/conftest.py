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
import logging
import os
import pytest
from pathlib import Path


def load_env_file(env_file_path, *, override: bool = True):
    """Load KEY=value / export KEY=value lines from a file into os.environ."""
    if not os.path.exists(env_file_path):
        return
    with open(env_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line.startswith('export '):
                line = line[7:]
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"\'')
                if override or key not in os.environ:
                    os.environ[key] = value


def pytest_configure(config):
    """Called after command line options have been parsed and all plugins and initial conftest files been loaded."""
    root = Path(__file__).parent.parent
    env_local = root / "aerospike.env"
    env_example = root / "aerospike.env.example"
    if env_local.exists():
        load_env_file(env_local, override=True)
        print(f"Loaded environment variables from {env_local}\n")
    else:
        load_env_file(env_example, override=False)
        print(f"Loaded default environment variables from {env_example} (no {env_local.name})\n")

    
    # Configure logging from AEROSPIKE_LOG_LEVEL / AEROSPIKE_LOG_FILE
    log_level = os.environ.get("AEROSPIKE_LOG_LEVEL", "").upper()
    if log_level:
        numeric = getattr(logging, log_level, None)
        if numeric is None:
            print(f"Warning: invalid AEROSPIKE_LOG_LEVEL={log_level!r}, ignoring\n")
        else:
            log_file = os.environ.get("AEROSPIKE_LOG_FILE")
            handler: logging.Handler
            if log_file:
                handler = logging.FileHandler(log_file)
            else:
                handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
            ))
            for prefix in ("aerospike_core", "aerospike_async"):
                logger = logging.getLogger(prefix)
                logger.setLevel(numeric)
                logger.addHandler(handler)

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
