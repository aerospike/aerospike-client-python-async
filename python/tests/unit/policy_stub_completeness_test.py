# Copyright 2025-2026 Aerospike, Inc.
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

"""The policy classes' stubs have to be maintained by hand; check they match.

The four policy classes subclass ``BasePolicy``, so their ``new()`` returns a
``PyClassInitializer``, which stub generation has no type mapping for. The
generator therefore emits a bare class shell and the properties are declared in
``postprocess_stubs.py`` instead.

Nothing links that table to the Rust getters, so it drifts silently: a property
added in Rust is simply absent from the stub, and an incomplete stub is
indistinguishable from a correct one at a glance. These tests compare the two
directly, in both directions.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

import aerospike_async

POLICY_CLASSES = ["ReadPolicy", "WritePolicy", "QueryPolicy", "BatchPolicy"]

_STUB = Path(aerospike_async.__file__).with_name("_aerospike_async_native.pyi")


def _stubbed_properties(class_name: str) -> set[str]:
    block = re.search(
        rf"^class {class_name}\(.*?\):\n(.*?)(?=^class |\Z)",
        _STUB.read_text(),
        re.S | re.M,
    )
    assert block, f"{class_name} is missing from the stub file entirely"
    return set(re.findall(r"^    def (\w+)\(self\) -> ", block.group(1), re.M))


def _runtime_properties(class_name: str) -> set[str]:
    """Properties as the extension module actually defines them.

    These are ``getset_descriptor`` objects, not Python ``property`` objects --
    checking for the latter silently yields an empty set, which makes a
    comparison against it pass no matter what.
    """
    cls = getattr(aerospike_async, class_name)
    return {
        name
        for name, attr in vars(cls).items()
        if not name.startswith("_") and hasattr(type(attr), "__set__")
    }


@pytest.mark.parametrize("class_name", POLICY_CLASSES)
def test_every_property_is_stubbed(class_name):
    """A property the class has but the stub omits is invisible to type checkers."""
    missing = _runtime_properties(class_name) - _stubbed_properties(class_name)
    assert not missing, (
        f"{class_name} has properties absent from the stub: {sorted(missing)}. "
        f"Add them to POLICY_PROPERTIES in python/postprocess_stubs.py."
    )


@pytest.mark.parametrize("class_name", POLICY_CLASSES)
def test_no_property_is_stubbed_that_does_not_exist(class_name):
    """The opposite drift: a stub promising something the class does not have."""
    phantom = _stubbed_properties(class_name) - _runtime_properties(class_name)
    assert not phantom, (
        f"{class_name} stubs properties that do not exist: {sorted(phantom)}."
    )


@pytest.mark.parametrize("class_name", POLICY_CLASSES)
def test_class_is_not_a_bare_shell(class_name):
    """Guards the specific way this broke before.

    The fixups matched an unqualified base class; when generation started
    qualifying it, every one silently became a no-op and the classes reverted
    to shells. Nothing failed, so nothing was noticed.
    """
    assert _stubbed_properties(class_name), (
        f"{class_name} generated as a bare shell -- the postprocessing pattern "
        f"did not match."
    )


FROM_FIELDS_CLASSES = ["ReadPolicy", "WritePolicy", "BatchPolicy"]


def _stubbed_from_fields_kwargs(class_name: str) -> list[str]:
    block = re.search(
        rf"^class {class_name}\(.*?\):\n(.*?)(?=^class |\Z)",
        _STUB.read_text(),
        re.S | re.M,
    ).group(1)
    sig = re.search(r"def from_fields\(\*(.*?)\) ->", block, re.S)
    assert sig, f"{class_name}.from_fields is missing from the stub"
    return re.findall(r"(\w+): typing", sig.group(1))


def _real_from_fields_kwargs(class_name: str) -> list[str]:
    """The constructor's true keywords, from the signature the extension exposes."""
    sig = inspect.signature(getattr(aerospike_async, class_name).from_fields)
    return [n for n, p in sig.parameters.items()
            if p.kind is inspect.Parameter.KEYWORD_ONLY]


@pytest.mark.parametrize("class_name", FROM_FIELDS_CLASSES)
def test_from_fields_stub_matches_the_real_signature(class_name):
    """The stub has to list exactly the constructor's keywords -- no more, no less.

    ``from_fields`` is how the SDK layer builds every policy, so a stub that
    drifts from it misleads exactly the callers most likely to rely on it.
    Comparing against the extension's own signature catches an omission, which
    a check for validity alone cannot: dropping a keyword leaves the remainder
    perfectly valid.
    """
    stubbed = _stubbed_from_fields_kwargs(class_name)
    real = _real_from_fields_kwargs(class_name)
    assert stubbed == real, (
        f"{class_name}.from_fields stub does not match the real signature.\n"
        f"  missing from stub: {sorted(set(real) - set(stubbed))}\n"
        f"  not in the constructor: {sorted(set(stubbed) - set(real))}\n"
        f"Update POLICY_FROM_FIELDS in python/postprocess_stubs.py."
    )
