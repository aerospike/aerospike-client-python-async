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

import os
import sys
import time
from pathlib import Path

import pytest

# Ensure this directory is on sys.path so "from fixtures import ..." works
_this_dir = str(Path(__file__).parent)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from fixtures import wait_for_index_ready  # noqa: E402


@pytest.fixture
def wait_for_index():
    """Async helper: poll until a secondary index is queryable.

    Server-side SI build completes asynchronously after ``create_index`` even
    when the task reports done, so ``asyncio.sleep(N)`` is inherently flaky
    on loaded hosts. This fixture issues the same query the caller is about
    to run, retrying while ``ResultCode.INDEX_NOT_READABLE`` is reported.

    Usage::

        await wait_for_index(client, "test", "my_set", Filter.range("age", 0, 100))
    """
    return wait_for_index_ready


def pytest_runtest_logreport(report):
    """Record each failed test's time span for CI's server-log window.

    Active only when PNC_FAILURE_TIMES names a file (CI sets it); the job's
    failure step then prints the server log around each failure instead of
    only its tail.
    """
    path = os.environ.get("PNC_FAILURE_TIMES")
    if not path or not report.failed:
        return
    end = time.time()
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{end - report.duration:.0f} {end:.0f} {report.nodeid}\n")
