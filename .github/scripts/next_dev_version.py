#!/usr/bin/env python3
"""Compute the next dev-channel version from the committed Cargo version.

Usage: next_dev_version.py <cargo-version> <dev-number>

Prints GitHub-output-style lines:

    cargo-version=<semver string to stamp into Cargo.toml>
    pep440-version=<the version pip/maturin will see>

The committed Cargo version is the *last released* version, so dev builds
lead toward the next release: a prerelease base bumps its prerelease
number (``0.6.0-alpha.6`` -> ``0.6.0-alpha.7.dev.N``), a stable base bumps
the patch (``0.6.0`` -> ``0.6.1-dev.N``). PEP 440 orders dev releases
before the release they lead to, so the bump is required for dev wheels
to sort after the released version.

The semver -> PEP 440 mapping mirrors maturin's conversion and is
verified empirically via ``maturin sdist`` for the alpha and stable
shapes (``0.6.0-alpha.7.dev.123`` -> ``0.6.0a7.dev123``,
``0.6.1-dev.45`` -> ``0.6.1.dev45``).
"""

import re
import sys

_PRERELEASE_PEP440 = {"alpha": "a", "beta": "b", "rc": "rc"}


def next_dev_version(base: str, dev_number: int) -> tuple[str, str]:
    """Return ``(cargo_version, pep440_version)`` for the next dev build."""
    if ".dev." in base or base.endswith("-dev") or "-dev." in base:
        raise ValueError(
            f"committed Cargo version {base!r} already carries a dev segment"
        )

    prerelease = re.fullmatch(
        r"(\d+)\.(\d+)\.(\d+)-(alpha|beta|rc)\.(\d+)", base
    )
    if prerelease:
        major, minor, patch, kind, num = prerelease.groups()
        bumped = int(num) + 1
        cargo = f"{major}.{minor}.{patch}-{kind}.{bumped}.dev.{dev_number}"
        pep440 = (
            f"{major}.{minor}.{patch}"
            f"{_PRERELEASE_PEP440[kind]}{bumped}.dev{dev_number}"
        )
        return cargo, pep440

    stable = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", base)
    if stable:
        major, minor, patch = stable.groups()
        bumped = int(patch) + 1
        cargo = f"{major}.{minor}.{bumped}-dev.{dev_number}"
        pep440 = f"{major}.{minor}.{bumped}.dev{dev_number}"
        return cargo, pep440

    raise ValueError(f"unrecognized Cargo version shape: {base!r}")


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    cargo, pep440 = next_dev_version(sys.argv[1], int(sys.argv[2]))
    print(f"cargo-version={cargo}")
    print(f"pep440-version={pep440}")


if __name__ == "__main__":
    main()
