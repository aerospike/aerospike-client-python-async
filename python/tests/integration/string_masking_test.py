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

"""TestStringMasking — server-side masking suite (spec §4.3, 20 tests).

End-to-end coverage of ``StringOperation`` against bins protected by a
server-side masking rule. Mirrors the JSDK reference suite at commit
``6bb348e``'s `TestStringMasking`. Tests are gated on THREE conditions:

1. ``AEROSPIKE_HOST_8_1_3`` is set (string ops + masking are 8.1.3+ features)
2. Security is enabled on the target cluster (`query_users` succeeds)
3. Admin credentials are supplied via ``AEROSPIKE_HOST_8_1_3_USER`` /
   ``AEROSPIKE_HOST_8_1_3_PASSWORD`` (or the test cluster accepts the
   default `admin/admin`)

The suite creates two per-class test users (``stringops_reader`` with
``[ReadWrite, ReadMasked]`` and ``stringops_user`` with ``[ReadWrite]``)
and applies masking rules via the server's ``masking;...`` info command.
Cleanup drops the users + clears the rules at teardown.

Masking-command syntax (probed against ``34.28.91.57:3000``):

    masking;set=<set>;namespace=<ns>;bin=<bin>;type=string;function=redact
    masking;set=<set>;namespace=<ns>;bin=<bin>;type=string;function=constant;value=<v>
    masking;set=<set>;namespace=<ns>;bin=<bin>;type=string;function=remove

(The verb is the ``function=`` parameter, not a ``masking:VERB`` colon prefix.
``masking-show`` is the introspection command.)
"""

import asyncio
import os
import uuid

import pytest
import pytest_asyncio

from aerospike_async import (
    ClientPolicy,
    Key,
    Privilege,
    PrivilegeCode,
    StringOperation,
    StringWriteFlags,
    WritePolicy,
    new_client,
)
from aerospike_async.exceptions import ResultCode, ServerError, SecurityNotEnabled


pytestmark = pytest.mark.asyncio(loop_scope="module")


_NAMESPACE = "test"
_SET = "tmsk"
_BIN_MASKED = "pii"
_BIN_UNMASKED = "public"
_BIN_CONSTANT = "secret"  # second masked bin for the constant-mask variant
_RECORD_KEY = "mask_record"

_USER_READER = "stringops_reader"
_USER_BASIC = "stringops_user"
_USER_PASSWORD = "test_password_123"
_PROPAGATION_RETRIES = 10
_PROPAGATION_DELAY = 0.5


def _services_alternate() -> bool:
    sa_override = os.environ.get("AEROSPIKE_HOST_8_1_3_USE_SERVICES_ALTERNATE")
    if sa_override is not None:
        return sa_override.lower() == "true"
    return os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "").lower() == "true"


async def _wait_for_user(client, username, *, retries=_PROPAGATION_RETRIES):
    """Retry query_users until ``username`` is visible (SMD propagation)."""
    for attempt in range(retries):
        try:
            users = await client.query_users(None)
            if any(u.user == username for u in users):
                return
        except ServerError:
            pass
        await asyncio.sleep(_PROPAGATION_DELAY)
    pytest.fail(f"User {username!r} not visible after {retries} retries")


async def _apply_masking(admin_client, *, ns, set_name, bin_name, function, value=None):
    """Apply a masking rule via the server's ``masking;...`` info command."""
    parts = [
        f"set={set_name}",
        f"namespace={ns}",
        f"bin={bin_name}",
        "type=string",
        f"function={function}",
    ]
    if value is not None:
        parts.append(f"value={value}")
    cmd = "masking;" + ";".join(parts)
    response = await admin_client.info(cmd)
    for node_response in response.values():
        if node_response != "ok":
            raise RuntimeError(f"masking command failed: {cmd} → {node_response}")


async def _remove_masking(admin_client, *, ns, set_name, bin_name):
    """Best-effort removal of a masking rule. Ignores errors from non-existent rules."""
    cmd = (
        f"masking;set={set_name};namespace={ns};bin={bin_name};"
        f"type=string;function=remove"
    )
    try:
        await admin_client.info(cmd)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def admin_client(aerospike_host_813_required):
    """Admin-credentialed client; gates on the cluster having security enabled.

    Skips the entire module cleanly if (a) ``AEROSPIKE_HOST_8_1_3`` is unset,
    (b) security is not enabled, or (c) the credentials don't auth.
    """
    user = os.environ.get("AEROSPIKE_HOST_8_1_3_USER", "admin")
    password = os.environ.get("AEROSPIKE_HOST_8_1_3_PASSWORD", "admin")
    cp = ClientPolicy()
    cp.use_services_alternate = _services_alternate()
    cp.user = user
    cp.password = password
    try:
        client = await new_client(cp, aerospike_host_813_required)
    except Exception as exc:
        pytest.skip(f"Could not connect to {aerospike_host_813_required} as admin: {exc}")
    await asyncio.sleep(2)  # tend
    # Confirm security is enabled by issuing a privilege query
    try:
        await client.query_users(None)
    except ServerError as exc:
        await client.close()
        if exc.result_code == ResultCode.SECURITY_NOT_ENABLED or isinstance(exc, SecurityNotEnabled):
            pytest.skip("Security is not enabled on the 8.1.3+ cluster")
        raise
    yield client
    await client.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def masking_setup(admin_client):
    """Per-module fixture: creates users + applies masking rules, cleans up at teardown.

    Setup performs these steps in order:
    1. Drop any leftover test users from prior runs (idempotent)
    2. Create ``stringops_reader`` with ``[ReadWrite, ReadMasked]``
    3. Create ``stringops_user`` with ``[ReadWrite]``
    4. Apply ``redact`` masking rule to bin ``pii``
    5. Apply ``constant;value=HIDDEN`` masking rule to bin ``secret``

    Teardown reverses the order.
    """
    # 1. Idempotent cleanup of prior runs
    for username in (_USER_READER, _USER_BASIC):
        try:
            await admin_client.drop_user(username)
        except Exception:
            pass
    # 2. Create reader (read-masked privilege)
    await admin_client.create_user(
        _USER_READER,
        _USER_PASSWORD,
        [],  # roles handled via grant_privileges below
    )
    await _wait_for_user(admin_client, _USER_READER)
    # 3. Create basic user (no read-masked privilege)
    await admin_client.create_user(
        _USER_BASIC,
        _USER_PASSWORD,
        [],
    )
    await _wait_for_user(admin_client, _USER_BASIC)
    # Grant privileges via role names (the simpler builtin-role path).
    # ReadWrite + ReadMasked for the reader; ReadWrite-only for the user.
    await admin_client.grant_roles(_USER_READER, ["read-write", "read-masked"])
    await admin_client.grant_roles(_USER_BASIC, ["read-write"])
    # 4. + 5. Apply masking rules
    await _apply_masking(
        admin_client,
        ns=_NAMESPACE,
        set_name=_SET,
        bin_name=_BIN_MASKED,
        function="redact",
    )
    await _apply_masking(
        admin_client,
        ns=_NAMESPACE,
        set_name=_SET,
        bin_name=_BIN_CONSTANT,
        function="constant",
        value="HIDDEN",
    )

    yield  # tests run

    # Teardown
    for bin_name in (_BIN_MASKED, _BIN_CONSTANT):
        await _remove_masking(
            admin_client, ns=_NAMESPACE, set_name=_SET, bin_name=bin_name
        )
    for username in (_USER_READER, _USER_BASIC):
        try:
            await admin_client.drop_user(username)
        except Exception:
            pass


async def _make_user_client(aerospike_host, user, password):
    cp = ClientPolicy()
    cp.use_services_alternate = _services_alternate()
    cp.user = user
    cp.password = password
    client = await new_client(cp, aerospike_host)
    await asyncio.sleep(2)
    return client


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def reader_client(aerospike_host_813_required, masking_setup):
    """Client authenticated as ``stringops_reader`` ([ReadWrite, ReadMasked])."""
    client = await _make_user_client(
        aerospike_host_813_required, _USER_READER, _USER_PASSWORD
    )
    yield client
    await client.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def basic_client(aerospike_host_813_required, masking_setup):
    """Client authenticated as ``stringops_user`` ([ReadWrite])."""
    client = await _make_user_client(
        aerospike_host_813_required, _USER_BASIC, _USER_PASSWORD
    )
    yield client
    await client.close()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def reset_record(admin_client, masking_setup):
    """Reset the test record before every test (admin-credentialed put)."""
    key = Key(_NAMESPACE, _SET, _RECORD_KEY)
    await admin_client.put(
        key,
        {
            _BIN_MASKED: "hello world",
            _BIN_CONSTANT: "real-secret",
            _BIN_UNMASKED: "visible",
        },
        policy=WritePolicy(),
    )
    yield


def _key():
    return Key(_NAMESPACE, _SET, _RECORD_KEY)


def _is_role_violation(exc) -> bool:
    """Check if exception is a ROLE_VIOLATION (server result code 81).

    PAC's ``ResultCode`` enum doesn't expose ``ROLE_VIOLATION`` as a
    named attribute (only ``FAIL_FORBIDDEN``, ``ALWAYS_FORBIDDEN``, etc.
    are surfaced; the protocol's code 81 maps to one of those without a
    dedicated alias). The exception's ``result_code`` repr is the
    canonical signal. Match heuristically against:
      - ``result_code`` repr containing "role" / "forbidden" / "violation"
      - exception ``str()`` containing the same
      - exception type name containing the same
    """
    code_repr = str(getattr(exc, "result_code", "")).lower()
    msg = str(exc).lower()
    type_name = type(exc).__name__.lower()
    needles = ("roleviolation", "role violation", "forbidden", "fail_forbidden")
    return any(n in code_repr or n in msg or n in type_name for n in needles)


# ---------------------------------------------------------------------------
# Read-side scenarios (8 tests) — privilege gates which value the caller observes
# ---------------------------------------------------------------------------


class TestMaskingReads:

    async def test_privileged_strlen_returns_real_length(self, reader_client):
        """ReadMasked privilege → sees the real ``"hello world"`` length (11)."""
        rec = await reader_client.operate(
            _key(), [StringOperation.strlen(_BIN_MASKED)], policy=WritePolicy()
        )
        assert rec.bins.get(_BIN_MASKED) == 11

    async def test_privileged_substr_returns_real_prefix(self, reader_client):
        """ReadMasked privilege → substr returns the real prefix."""
        rec = await reader_client.operate(
            _key(),
            [StringOperation.substr(_BIN_MASKED, 0, 5)],
            policy=WritePolicy(),
        )
        assert rec.bins.get(_BIN_MASKED) == "hello"

    async def test_unprivileged_substr_returns_redacted_prefix(self, basic_client):
        """Without ReadMasked → substr returns a 5-codepoint redacted string.

        The ``redact`` function preserves LENGTH but replaces the contents.
        The suite asserts non-equality with the real prefix rather than a
        fixed redacted value (redact may choose any replacement).
        """
        rec = await basic_client.operate(
            _key(),
            [StringOperation.substr(_BIN_MASKED, 0, 5)],
            policy=WritePolicy(),
        )
        redacted_prefix = rec.bins.get(_BIN_MASKED)
        assert isinstance(redacted_prefix, str)
        assert len(redacted_prefix) == 5
        assert redacted_prefix != "hello"

    async def test_unprivileged_find_returns_minus_one(self, basic_client):
        """Without ReadMasked → ``find("world")`` on redacted value returns -1."""
        rec = await basic_client.operate(
            _key(),
            [StringOperation.find(_BIN_MASKED, "world")],
            policy=WritePolicy(),
        )
        assert rec.bins.get(_BIN_MASKED) == -1

    async def test_unprivileged_contains_returns_false(self, basic_client):
        """Without ReadMasked → ``contains("hello")`` returns False on redacted."""
        rec = await basic_client.operate(
            _key(),
            [StringOperation.contains(_BIN_MASKED, "hello")],
            policy=WritePolicy(),
        )
        result = rec.bins.get(_BIN_MASKED)
        assert result is False
        assert isinstance(result, bool)

    async def test_unprivileged_starts_ends_with_return_false(self, basic_client):
        """Without ReadMasked → starts_with("hello") AND ends_with("world") both False."""
        rec = await basic_client.operate(
            _key(),
            [
                StringOperation.starts_with(_BIN_MASKED, "hello"),
                StringOperation.ends_with(_BIN_MASKED, "world"),
            ],
            policy=WritePolicy(),
        )
        results = rec.bins.get(_BIN_MASKED)
        assert results == [False, False]

    async def test_unprivileged_regex_compare_returns_false(self, basic_client):
        """Without ReadMasked → ``regex_compare("hello.*")`` returns False."""
        rec = await basic_client.operate(
            _key(),
            [StringOperation.regex_compare(_BIN_MASKED, "hello.*")],
            policy=WritePolicy(),
        )
        result = rec.bins.get(_BIN_MASKED)
        assert result is False
        assert isinstance(result, bool)

    async def test_byte_length_is_length_preserved_by_redact(self, reader_client, basic_client):
        """``byte_length`` returns the same value for both users — redact
        preserves the length of the underlying bytes.
        """
        rec_priv = await reader_client.operate(
            _key(), [StringOperation.byte_length(_BIN_MASKED)], policy=WritePolicy()
        )
        rec_unpriv = await basic_client.operate(
            _key(), [StringOperation.byte_length(_BIN_MASKED)], policy=WritePolicy()
        )
        assert rec_priv.bins.get(_BIN_MASKED) == rec_unpriv.bins.get(_BIN_MASKED) == 11

    async def test_unmasked_bin_transparent_to_both_users(self, reader_client, basic_client):
        """The ``public`` bin (no masking rule applied) is identical for both users."""
        rec_priv = await reader_client.operate(
            _key(), [StringOperation.strlen(_BIN_UNMASKED)], policy=WritePolicy()
        )
        rec_unpriv = await basic_client.operate(
            _key(), [StringOperation.strlen(_BIN_UNMASKED)], policy=WritePolicy()
        )
        assert rec_priv.bins.get(_BIN_UNMASKED) == rec_unpriv.bins.get(_BIN_UNMASKED)
        # "visible" is 7 codepoints
        assert rec_priv.bins.get(_BIN_UNMASKED) == 7


# ---------------------------------------------------------------------------
# Modify ops blocked without write-masked privilege (7 tests)
# ---------------------------------------------------------------------------


class TestMaskingModifiesBlocked:
    """Each test asserts ROLE_VIOLATION (code 81) when an unprivileged user
    (``stringops_user`` with only [ReadWrite], NO ReadMasked / WriteMasked)
    attempts a modify op against the masked bin.

    The selection covers the modify-op families: case-change, splice,
    append, replace, whitespace, pad, regex.
    """

    async def test_upper_blocked(self, basic_client):
        with pytest.raises(Exception) as ei:
            await basic_client.operate(
                _key(), [StringOperation.upper(_BIN_MASKED)], policy=WritePolicy()
            )
        assert _is_role_violation(ei.value), f"expected ROLE_VIOLATION, got {ei.value!r}"

    async def test_insert_blocked(self, basic_client):
        with pytest.raises(Exception) as ei:
            await basic_client.operate(
                _key(),
                [StringOperation.insert(_BIN_MASKED, 0, "X")],
                policy=WritePolicy(),
            )
        assert _is_role_violation(ei.value)

    async def test_concat_blocked(self, basic_client):
        with pytest.raises(Exception) as ei:
            await basic_client.operate(
                _key(),
                [StringOperation.concat(_BIN_MASKED, "more")],
                policy=WritePolicy(),
            )
        assert _is_role_violation(ei.value)

    async def test_replace_blocked(self, basic_client):
        with pytest.raises(Exception) as ei:
            await basic_client.operate(
                _key(),
                [StringOperation.replace(_BIN_MASKED, "hello", "HI")],
                policy=WritePolicy(),
            )
        assert _is_role_violation(ei.value)

    async def test_trim_blocked(self, basic_client):
        with pytest.raises(Exception) as ei:
            await basic_client.operate(
                _key(),
                [StringOperation.trim(_BIN_MASKED)],
                policy=WritePolicy(),
            )
        assert _is_role_violation(ei.value)

    async def test_pad_start_blocked(self, basic_client):
        with pytest.raises(Exception) as ei:
            await basic_client.operate(
                _key(),
                [StringOperation.pad_start(_BIN_MASKED, 20, ".")],
                policy=WritePolicy(),
            )
        assert _is_role_violation(ei.value)

    async def test_regex_replace_blocked(self, basic_client):
        with pytest.raises(Exception) as ei:
            await basic_client.operate(
                _key(),
                [StringOperation.regex_replace(_BIN_MASKED, "hello", "HI")],
                policy=WritePolicy(),
            )
        assert _is_role_violation(ei.value)


# ---------------------------------------------------------------------------
# Privilege boundary (2 tests)
# ---------------------------------------------------------------------------


class TestMaskingPrivilegeBoundary:

    async def test_read_masked_only_cannot_modify(self, reader_client):
        """``ReadMasked`` is a READ privilege — it does NOT grant write-masked.
        Attempting ``upper`` on the masked bin from this client → ROLE_VIOLATION.
        """
        with pytest.raises(Exception) as ei:
            await reader_client.operate(
                _key(), [StringOperation.upper(_BIN_MASKED)], policy=WritePolicy()
            )
        assert _is_role_violation(ei.value), f"expected ROLE_VIOLATION, got {ei.value!r}"

    async def test_admin_can_modify_masked_bin(self, admin_client):
        """Admin bypasses masking restrictions — ``upper`` succeeds, value mutated."""
        await admin_client.operate(
            _key(), [StringOperation.upper(_BIN_MASKED)], policy=WritePolicy()
        )
        rec = await admin_client.get(_key())
        assert rec.bins.get(_BIN_MASKED) == "HELLO WORLD"


# ---------------------------------------------------------------------------
# Unprivileged user can modify the unmasked bin (1 test)
# ---------------------------------------------------------------------------


class TestMaskingUnmaskedBinWritable:

    async def test_unprivileged_can_upper_unmasked_bin(self, basic_client, admin_client):
        """Modify on the unmasked ``public`` bin succeeds for the unprivileged user.

        Same assertion also verifies that the masked ``pii`` bin is left
        untouched in the same operation context (no spillover).
        """
        await basic_client.operate(
            _key(), [StringOperation.upper(_BIN_UNMASKED)], policy=WritePolicy()
        )
        rec = await admin_client.get(_key())
        assert rec.bins.get(_BIN_UNMASKED) == "VISIBLE"
        # masked bin untouched
        assert rec.bins.get(_BIN_MASKED) == "hello world"


# ---------------------------------------------------------------------------
# Constant-mask variant (1 test)
# ---------------------------------------------------------------------------


class TestMaskingConstantFunction:
    """The ``constant`` mask function replaces with a fixed literal regardless
    of length. Verifies that the spec's behavior generalizes across mask types.
    """

    async def test_constant_mask_redirects_to_fixed_value(self, reader_client, basic_client):
        """Privileged reads see the real value; unprivileged see the constant ``HIDDEN``."""
        # Privileged strlen — real value "real-secret" = 11 codepoints
        rec_priv = await reader_client.operate(
            _key(), [StringOperation.strlen(_BIN_CONSTANT)], policy=WritePolicy()
        )
        assert rec_priv.bins.get(_BIN_CONSTANT) == 11
        # Unprivileged strlen — sees constant "HIDDEN" = 6 codepoints
        rec_unpriv = await basic_client.operate(
            _key(), [StringOperation.strlen(_BIN_CONSTANT)], policy=WritePolicy()
        )
        assert rec_unpriv.bins.get(_BIN_CONSTANT) == 6
        # Privileged substr(0,4) — first 4 of real value: "real"
        rec_priv = await reader_client.operate(
            _key(),
            [StringOperation.substr(_BIN_CONSTANT, 0, 4)],
            policy=WritePolicy(),
        )
        assert rec_priv.bins.get(_BIN_CONSTANT) == "real"
        # Unprivileged substr(0,4) — first 4 of constant: "HIDD"
        rec_unpriv = await basic_client.operate(
            _key(),
            [StringOperation.substr(_BIN_CONSTANT, 0, 4)],
            policy=WritePolicy(),
        )
        assert rec_unpriv.bins.get(_BIN_CONSTANT) == "HIDD"
