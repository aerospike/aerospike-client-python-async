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

"""Dynamic-configuration integration coverage.

The dynamic-config subsystem lives in the core and is reached via the
``AEROSPIKE_CLIENT_CONFIG_URL`` environment variable, read at connect time;
this build enables the core's ``dynamic-config`` feature. Coverage: load a
good YAML, tolerate a bad field, fall back on a missing file, and stay
usable on structurally broken input (the core fail-softs — logs and ignores
— rather than raising, so a client always connects with usable defaults).
"""

import contextlib
import os

import pytest

from aerospike_async import ClientPolicy, Key, WritePolicy, new_client

_GOOD = 'version: "1.0.0"\ndynamic:\n  read:\n    max_retries: 7\n'
_BAD_VALUE = 'version: "1.0.0"\ndynamic:\n  read:\n    max_retries: not_a_number\n'
_MALFORMED = 'version: "1.0.0"\ndynamic: [unbalanced : bracket\n'
_NO_VERSION = 'dynamic:\n  read:\n    max_retries: 7\n'
_BAD_VERSION_VALUE = 'version: "0.0.9"\ndynamic:\n  read:\n    max_retries: 7\n'


def _write(tmp_path, name: str, text: str) -> str:
    path = tmp_path / name
    path.write_text(text)
    return "file://" + str(path)


@contextlib.asynccontextmanager
async def _client(config_url, aerospike_host, use_services_alternate):
    """Connect with (or without) a dynamic-config URL in the environment.

    The env var is read by the core at connect time, so it must be set
    before ``new_client`` and restored afterward.
    """
    prev = os.environ.get("AEROSPIKE_CLIENT_CONFIG_URL")
    if config_url is None:
        os.environ.pop("AEROSPIKE_CLIENT_CONFIG_URL", None)
    else:
        os.environ["AEROSPIKE_CLIENT_CONFIG_URL"] = config_url
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)
    try:
        yield client
    finally:
        await client.close()
        if prev is None:
            os.environ.pop("AEROSPIKE_CLIENT_CONFIG_URL", None)
        else:
            os.environ["AEROSPIKE_CLIENT_CONFIG_URL"] = prev


async def _round_trip(client) -> dict:
    key = Key("test", "dynconf_it", "k1")
    await client.put(key, {"n": 1}, policy=WritePolicy())
    record = await client.get(key)
    return record.bins


# --- load / fallback / tolerance (client stays usable) ---------------------

async def test_valid_config_loads_and_client_operates(
    tmp_path, aerospike_host, use_services_alternate,
):
    """A well-formed dynamic config loads and the client reads/writes normally."""
    url = _write(tmp_path, "good.yaml", _GOOD)
    async with _client(url, aerospike_host, use_services_alternate) as client:
        assert await _round_trip(client) == {"n": 1}


async def test_missing_file_falls_back_to_defaults(
    tmp_path, aerospike_host, use_services_alternate,
):
    """A missing config file must not break the client."""
    url = "file://" + str(tmp_path / "nope.yaml")
    async with _client(url, aerospike_host, use_services_alternate) as client:
        assert await _round_trip(client) == {"n": 1}


async def test_bad_field_value_is_skipped_client_operates(
    tmp_path, aerospike_host, use_services_alternate,
):
    """An unparseable field value is skipped; the client still operates."""
    url = _write(tmp_path, "badval.yaml", _BAD_VALUE)
    async with _client(url, aerospike_host, use_services_alternate) as client:
        assert await _round_trip(client) == {"n": 1}


async def test_unrecognized_version_value_is_tolerated(
    tmp_path, aerospike_host, use_services_alternate,
):
    """A present-but-unrecognized ``version`` is tolerated; config still loads.

    The core only requires the ``version`` key to be present, not to hold a
    specific value.
    """
    url = _write(tmp_path, "badver.yaml", _BAD_VERSION_VALUE)
    async with _client(url, aerospike_host, use_services_alternate) as client:
        assert await _round_trip(client) == {"n": 1}


# --- structural errors: core fail-softs (logs + ignores), client stays usable ---

async def test_malformed_yaml_is_tolerated_client_operates(
    tmp_path, aerospike_host, use_services_alternate,
):
    """Malformed YAML is logged and ignored; the client still operates."""
    url = _write(tmp_path, "malformed.yaml", _MALFORMED)
    async with _client(url, aerospike_host, use_services_alternate) as client:
        assert await _round_trip(client) == {"n": 1}


async def test_missing_version_is_tolerated_client_operates(
    tmp_path, aerospike_host, use_services_alternate,
):
    """A config missing the top-level ``version`` is ignored; the client operates."""
    url = _write(tmp_path, "nover.yaml", _NO_VERSION)
    async with _client(url, aerospike_host, use_services_alternate) as client:
        assert await _round_trip(client) == {"n": 1}


# --- override / reload effect (blocked on a resolved-policy getter) ---------

@pytest.mark.skip(
    reason="Not testable yet — blocked on the core exposing the resolved read policy. "
    "core's resolve_read() is pub(crate) and nothing surfaces the effective policy, so "
    "an override's effect can't be observed from Python. Override was verified manually "
    "with a temporary core resolve_read debug log (max_retries 2 -> 7). Un-skip when "
    "the core exposes a resolved-settings getter.",
)
async def test_override_changes_resolved_read_policy(tmp_path):
    raise AssertionError("resolved read policy is not observable through this client")


@pytest.mark.skip(
    reason="Not testable yet — same missing resolved-policy getter as the override test. "
    "resolve_read() is pub(crate), so a reloaded value can't be read back. Un-skip when "
    "the core exposes a resolved-settings getter.",
)
async def test_reload_reflects_updated_resolved_policy(tmp_path):
    raise AssertionError("resolved read policy is not observable through this client")
