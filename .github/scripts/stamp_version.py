#!/usr/bin/env python3
"""Stamp a version into Cargo.toml and Cargo.lock in place.

Usage: stamp_version.py <cargo-version>

Used by CI to apply an ephemeral dev-channel version to the build
workspace without committing it. Both manifest and lockfile are updated
so builds that resolve with a locked dependency graph see a consistent
package version. Runs on the stock Python of every CI platform (no
third-party imports, no tomllib round-trip that would reorder the file).
"""

import pathlib
import re
import sys


def stamp(version: str) -> None:
    cargo_toml = pathlib.Path("Cargo.toml")
    text = cargo_toml.read_text()
    new_text, n = re.subn(
        r'^version = ".*"$', f'version = "{version}"', text, count=1, flags=re.M
    )
    if n != 1:
        raise SystemExit("could not find a version line in Cargo.toml")
    cargo_toml.write_text(new_text)

    cargo_lock = pathlib.Path("Cargo.lock")
    if cargo_lock.exists():
        text = cargo_lock.read_text()
        # Only this package's own entry; dependency versions stay locked.
        new_text, n = re.subn(
            r'(name = "aerospike_async"\nversion = )".*"',
            rf'\1"{version}"',
            text,
            count=1,
        )
        if n != 1:
            raise SystemExit(
                "could not find the aerospike_async entry in Cargo.lock"
            )
        cargo_lock.write_text(new_text)

    print(f"Stamped version {version}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    stamp(sys.argv[1])
