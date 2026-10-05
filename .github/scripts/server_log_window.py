#!/usr/bin/env python3
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

"""Print the server log around each failed test, not just its tail.

Usage: server_log_window.py <container> <failure-times-file>

The integration conftest appends one "<start> <end> <nodeid>" line per failed
test (epoch seconds) when PAC_FAILURE_TIMES names a file. Each failure gets the
server log from 30 seconds before the test started to a few seconds after it
ended; overlapping windows are merged so a cascade prints once.
"""

import os
import subprocess
import sys

LEAD_S = 30
TRAIL_S = 5
MAX_WINDOWS = 5
DOCKER = os.environ.get("DOCKER", "docker")


def windows(path):
    spans = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                parts = line.split(maxsplit=2)
                if len(parts) == 3:
                    start, end = float(parts[0]), float(parts[1])
                    spans.append((start - LEAD_S, end + TRAIL_S, parts[2].strip()))
    except OSError:
        return []
    spans.sort()
    merged = []
    for since, until, nodeid in spans:
        if merged and since <= merged[-1][1]:
            prev = merged[-1]
            merged[-1] = (prev[0], max(prev[1], until), prev[2] + [nodeid])
        else:
            merged.append((since, until, [nodeid]))
    return merged


def main():
    container, path = sys.argv[1], sys.argv[2]
    spans = windows(path)
    if not spans:
        print("No failed tests recorded: pytest exited for another reason "
              "(collection error or crash). See the pytest output above.")
        return
    for since, until, nodeids in spans[:MAX_WINDOWS]:
        print(f"=== Aerospike Server Log {LEAD_S}s before -> {TRAIL_S}s after: "
              f"{', '.join(nodeids[:3])}{' ...' if len(nodeids) > 3 else ''} ===", flush=True)
        subprocess.run([DOCKER, "logs", "--since", f"{since:.0f}", "--until", f"{until:.0f}", container],
                       check=False)
    if len(spans) > MAX_WINDOWS:
        print(f"=== {len(spans) - MAX_WINDOWS} more failure window(s) not shown ===", flush=True)
    print("=== End of Server Log ===", flush=True)


if __name__ == "__main__":
    main()
