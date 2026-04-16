# Copyright 2025-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Environment helpers for PAC benchmark scripts."""

from __future__ import annotations

import os
from pathlib import Path

from aerospike_async import ClientPolicy


def _load_env_file(path: Path, *, override: bool = False) -> None:
    if not path.exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:]
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("\"'")
                if override or key not in os.environ:
                    os.environ[key] = value


def ensure_env() -> None:
    """Load aerospike.env (or .example) into ``os.environ``."""
    root = Path(__file__).resolve().parent.parent
    env_local = root / "aerospike.env"
    env_example = root / "aerospike.env.example"
    if env_local.exists():
        _load_env_file(env_local, override=False)
    elif env_example.exists():
        _load_env_file(env_example, override=False)


def default_host() -> str:
    """Return the seed host string from the environment."""
    return os.environ.get("AEROSPIKE_HOST", "127.0.0.1:3000")


def default_client_policy() -> ClientPolicy:
    """Return a default :class:`ClientPolicy`."""
    policy = ClientPolicy()
    v = os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "true").strip().lower()
    policy.use_services_alternate = v in ("true", "1", "yes")
    return policy


# Load on import.
ensure_env()
