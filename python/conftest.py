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
import pytest_asyncio
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

    
    # Configure logging from AEROSPIKE_LOG_LEVEL / AEROSPIKE_LOG_FILE.
    #
    # When AEROSPIKE_LOG_FILE is set we attach a dedicated file handler with a
    # timestamped format. When it is unset we only set the level on the
    # aerospike_core / aerospike_async loggers and let records propagate to
    # the root — pytest's own `log_cli` handler (see pyproject.toml) prints
    # them to stderr in the standard pytest format. Attaching a stderr
    # handler here as well would duplicate every warning on the console.
    log_level = os.environ.get("AEROSPIKE_LOG_LEVEL", "").upper()
    if log_level:
        numeric = getattr(logging, log_level, None)
        if numeric is None:
            print(f"Warning: invalid AEROSPIKE_LOG_LEVEL={log_level!r}, ignoring\n")
        else:
            log_file = os.environ.get("AEROSPIKE_LOG_FILE")
            file_handler: logging.Handler | None = None
            if log_file:
                file_handler = logging.FileHandler(log_file)
                file_handler.setFormatter(logging.Formatter(
                    "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
                ))
            for prefix in ("aerospike_core", "aerospike_async"):
                logger = logging.getLogger(prefix)
                logger.setLevel(numeric)
                if file_handler is not None:
                    logger.addHandler(file_handler)

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


@pytest.fixture(scope="session")
def aerospike_host_8_1_2():
    """Seed for an 8.1.2+ Aerospike cluster, when one is available locally.

    Returns ``None`` when ``AEROSPIKE_HOST_8_1_2`` is unset; tests that depend
    on 8.1.2+ behavior should accept this fixture and ``pytest.skip`` when it
    is ``None`` rather than failing.
    """
    return os.environ.get('AEROSPIKE_HOST_8_1_2')


@pytest.fixture
def aerospike_host_812_required(aerospike_host_8_1_2):
    """Returns the 8.1.2+ host or skips the dependent test cleanly.

    Tests that exercise server-8.1.2-only features opt in by depending on
    this fixture (typically via a ``_812``-suffixed client fixture rather
    than directly). When ``AEROSPIKE_HOST_8_1_2`` is unset the dependent
    test is skipped with a clear message rather than running against the
    wrong cluster. When set, the test is auto-routed to the 8.1.2+ seed,
    so a single ``make test`` run can exercise the broad surface against
    ``AEROSPIKE_HOST`` and the 8.1.2-only subset against
    ``AEROSPIKE_HOST_8_1_2``.
    """
    if not aerospike_host_8_1_2:
        pytest.skip(
            "AEROSPIKE_HOST_8_1_2 is unset; this test requires an 8.1.2+ "
            "cluster. Set AEROSPIKE_HOST_8_1_2 in aerospike.env to enable."
        )
    return aerospike_host_8_1_2


def _parse_build_string(build: str):
    """Parse an Aerospike server build string (e.g. ``8.1.2.1``) into a tuple.

    Returns the leading ``(major, minor, patch, build)`` quadruple. Trailing
    suffixes are tolerated to match the core's regex parser. Returns
    ``None`` if the string does not start with four dot-separated integers.
    """
    parts = build.split('.')
    if len(parts) < 4:
        return None
    try:
        head = tuple(int(p) for p in parts[:4])
    except ValueError:
        # The fourth component may carry a non-numeric suffix (e.g.
        # ``1-asdf``); slice off everything after the first run of digits.
        try:
            fourth = parts[3]
            cut = 0
            while cut < len(fourth) and fourth[cut].isdigit():
                cut += 1
            head = (int(parts[0]), int(parts[1]), int(parts[2]), int(fourth[:cut]))
        except Exception:
            return None
    return head


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def server_version(aerospike_host, use_services_alternate):
    """Probe the seed for ``build`` info and return ``(M, m, p, b)``.

    Uses the first node in the response. Returns ``None`` if the probe
    fails for any reason. Tests that need a version comparison should
    short-circuit on ``None`` (e.g. ``pytest.skip`` or fall through to
    server-side enforcement).
    """
    from aerospike_async import ClientPolicy, new_client

    if not aerospike_host:
        return None
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    try:
        client = await new_client(cp, aerospike_host)
    except Exception:
        return None
    try:
        info = await client.info("build")
    finally:
        await client.close()
    for raw in info.values():
        if not raw:
            continue
        # ``info("build")`` returns either ``{"build": "8.1.2.1"}`` (single-
        # value) or a string ``build=8.1.2.1`` depending on transport. Tolerate
        # both shapes.
        if "=" in raw:
            _, _, value = raw.partition("=")
            parsed = _parse_build_string(value.strip())
        else:
            parsed = _parse_build_string(raw.strip())
        if parsed is not None:
            return parsed
    return None


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def supports_query_ops_projection_ext(server_version):
    """``True`` when the seed cluster accepts non-basic-read ops in queries.

    Mirrors the per-node feature exposed by the Rust core's
    ``Version::supports_query_ops_projection_ext`` (server >= 8.1.2). Tests
    that need extended reads in ``Statement.set_operations`` should
    ``pytest.skip`` when this is ``False``.
    """
    return server_version is not None and server_version >= (8, 1, 2, 0)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def supports_enhanced_expression_api(server_version):
    """``True`` when the cluster supports the 8.1.2 enhanced expression API.

    Covers native ``in_list`` / ``map_keys`` / ``map_values`` ExpOps and
    the ``CTX.map_keys_in`` / ``and_filter`` context helpers. Server
    >= 8.1.2. Path-form expression operators (``exp_select_*`` /
    ``exp_modify_*``) are 8.1.1 — gate those on
    ``supports_cdt_path_expressions``.
    """
    return server_version is not None and server_version >= (8, 1, 2, 0)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def supports_cdt_path_expressions(server_version):
    """``True`` when the cluster supports CDT path expression operations.

    Covers ``select_by_path`` / ``modify_by_path`` ops and their
    expression-form siblings (``exp_select_by_path`` /
    ``exp_modify_by_path``). Mirrors the per-node feature exposed by the
    Rust core's ``Version::supports_cdt_path_expressions`` (server
    >= 8.1.1). Tests that exercise path expressions should
    ``pytest.skip`` when this is ``False``.
    """
    return server_version is not None and server_version >= (8, 1, 1, 0)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def enterprise(aerospike_host, use_services_alternate):
    """True when the test cluster is Enterprise Edition (queried via info).

    Tests that exercise Enterprise-only features should accept this fixture
    and ``pytest.skip`` when it's ``False``, rather than relying on a
    ``ServerError(EnterpriseOnly)`` to bubble up.
    """
    from aerospike_async import ClientPolicy, new_client

    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)
    try:
        result = await client.info("edition")
        return any("Enterprise" in v for v in result.values())
    finally:
        await client.close()
