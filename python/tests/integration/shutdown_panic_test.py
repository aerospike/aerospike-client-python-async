# Copyright 2025-2026 Aerospike, Inc.
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

"""Shutdown-race regression: Python::attach panic on dead interpreter.

When a user-code exception escapes `asyncio.run()` while Tokio tasks are
still in flight, those tasks complete after Python begins finalizing.  Each
one calls into the completion bridge or the `RustClientError → PyErr`
conversion path, which call `Python::attach`.  pyo3 asserts on
`Py_IsInitialized() != 0` and panics the worker — historically printing
the assertion to stderr even though the process exits correctly.

The fix combines (a) a fast-path `Py_IsInitialized` check in the bridge,
(b) `catch_unwind` around the spawn closure's synchronous tail, and (c) a
panic-hook filter installed at module init that suppresses this specific
assertion.  Together they keep stderr clean during abnormal shutdown.

This test launches a deterministic repro as a subprocess and asserts:
  - exit code is non-zero (the simulated abort exception was raised)
  - stderr does NOT contain the "Python interpreter is not initialized"
    assertion (i.e. the panic hook filtered it AND/OR `catch_unwind`
    swallowed it AND/OR our fast-path avoided the attach entirely).
"""

import os
import pytest
import subprocess
import sys
import textwrap


def _repro_script(host: str) -> str:
    """Return the source of a small script that reliably triggers the
    panic on a build without the fix."""
    return textwrap.dedent(f"""\
        import asyncio
        from aerospike_async import ClientPolicy, ReadPolicy, Key, new_client

        HOST = {host!r}
        NS = "test"
        SET = "_shutdown_repro"
        N_TASKS = 200

        async def main():
            cp = ClientPolicy()
            client = await new_client(cp, HOST)
            rp = ReadPolicy()

            async def do_op(i):
                try:
                    return await client.get(rp, Key(NS, SET, f"_repro_{{i}}"))
                except Exception:
                    return None

            tasks = [asyncio.create_task(do_op(i)) for i in range(N_TASKS)]
            await asyncio.sleep(0.001)
            raise RuntimeError("simulated workload abort")

        asyncio.run(main())
        """)


@pytest.mark.parametrize("attempt", range(3))
def test_no_panic_on_interpreter_finalization(
    aerospike_host, tmp_path, attempt
):
    """In 3 independent process invocations, stderr stays free of the
    `Python interpreter is not initialized` assertion that historically
    fired when in-flight Tokio tasks completed during shutdown.

    Run 3x because the race is timing-dependent — one clean run could
    be luck.  All 3 must pass.
    """
    if not aerospike_host:
        pytest.skip("AEROSPIKE_HOST not set")

    script_path = tmp_path / f"shutdown_repro_{attempt}.py"
    script_path.write_text(_repro_script(aerospike_host))

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )

    # The simulated abort raises out of asyncio.run() — non-zero exit is
    # expected and correct.
    assert result.returncode != 0, (
        f"expected non-zero exit from simulated abort, got 0"
    )

    # The thing this test exists to catch: the shutdown-race panic must
    # not appear in stderr.
    assert "Python interpreter is not initialized" not in result.stderr, (
        f"shutdown-race panic regressed (attempt {attempt}).\n"
        f"--- stderr ---\n{result.stderr}\n--- end stderr ---"
    )
    assert "panicked at" not in result.stderr, (
        f"Rust worker thread panic regressed (attempt {attempt}).\n"
        f"--- stderr ---\n{result.stderr}\n--- end stderr ---"
    )
