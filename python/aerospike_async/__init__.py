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

# Install uvloop as the default event loop policy when available.
# uvloop is a drop-in replacement for asyncio's event loop that provides
# significantly better throughput and latency for async I/O workloads.
# Set AEROSPIKE_NO_UVLOOP=1 to disable.
import os as _os

if not _os.environ.get("AEROSPIKE_NO_UVLOOP"):
    try:
        import uvloop as _uvloop
        import warnings as _warnings
        # uvloop.install() uses set_event_loop_policy which is deprecated
        # in Python 3.14+.  Suppress the warning until uvloop provides a
        # non-deprecated alternative.
        with _warnings.catch_warnings():
            _warnings.filterwarnings(
                "ignore", category=DeprecationWarning, module=r"uvloop"
            )
            _uvloop.install()
        del _warnings
    except ImportError:
        pass

del _os

# Import all classes and functions from the compiled module
from ._aerospike_async_native import *

try:
    from importlib.metadata import version as _pkg_version
    __version__ = _pkg_version("aerospike_client_python_async")
except Exception:
    __version__ = "0.0.0-dev"
