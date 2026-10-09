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

# Module-level names leak into the package namespace (this is the package
# root), so anything imported here is `_`-prefixed and `del`-ed after use.
import os as _os

# Install uvloop as the default event loop policy.
# uvloop is a drop-in replacement for asyncio's event loop that provides
# significantly better throughput and latency for async I/O workloads.
#
# uvloop is required everywhere except Windows (see pyproject.toml's
# `sys_platform != 'win32'` marker on the dependency). On Windows we
# fall back to asyncio's default selector loop.
#
# uvloop is installed by default. Set AEROSPIKE_NO_UVLOOP=1 to opt out
# and keep asyncio's default selector loop — a safety valve for the
# rare environment that hits a uvloop bug, with no need to uninstall
# the dependency.
#
# uvloop/warnings are imported lazily inside the guard on purpose: skip
# loading the uvloop C extension entirely when opted out, and tolerate
# its absence on Windows (the import is genuinely conditional).
if not _os.environ.get("AEROSPIKE_NO_UVLOOP"):
    try:
        import uvloop as _uvloop
        import warnings as _warnings
        # uvloop.install() uses set_event_loop_policy which is deprecated in
        # Python 3.14+.  Suppress the warning until uvloop provides a
        # non-deprecated alternative.
        with _warnings.catch_warnings():
            _warnings.filterwarnings(
                "ignore", category=DeprecationWarning, module=r"uvloop"
            )
            _uvloop.install()
        del _warnings, _uvloop
    except ImportError:
        pass

del _os

# Import all classes and functions from the compiled module
from ._native import *  # noqa: E402,F403

# Load the exceptions wrapper module with the package: it applies Python-side
# class defaults (e.g. AerospikeError.in_doubt) that must exist even for
# callers that never import aerospike_native.exceptions themselves. The star
# import above already bound `exceptions` to the raw PyO3 submodule, which
# would make `from . import exceptions` a no-op attribute lookup — import the
# wrapper explicitly and rebind the package attribute to it (it also carries
# the ServerError subclass hierarchy the native submodule lacks).
import importlib as _importlib  # noqa: E402
exceptions = _importlib.import_module(".exceptions", __name__)
del _importlib

try:
    from importlib.metadata import version as _pkg_version
    __version__ = _pkg_version("aerospike_native")
except Exception:
    __version__ = "0.0.0-dev"
