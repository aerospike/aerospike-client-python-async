# Copyright 2025-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Standalone benchmark for the Python Async Client (PAC).

Drives the raw PAC ``get`` / ``operate`` API without any SDK overhead,
giving a floor measurement of achievable throughput and latency.

Usage::

    python -m benchmarks.benchmark -k 100000 -d 10 -z 32 -w RU,50
    python -m benchmarks.benchmark --help
"""

from __future__ import annotations

import argparse
import asyncio
import math
import random
import resource
import sys
import time
import tracemalloc
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import benchmarks._env  # noqa: E402, F401

from aerospike_async import (  # noqa: E402
    ClientPolicy,
    Key,
    Operation,
    ReadPolicy,
    WritePolicy,
)

from benchmarks._env import default_client_policy, default_host  # noqa: E402


# ---------------------------------------------------------------------------
# Lightweight stats (self-contained, no PSDK dependency)
# ---------------------------------------------------------------------------

class _Stats:
    """Minimal per-interval stats collector."""

    def __init__(self, warmup: int, cooldown: int) -> None:
        self._lock = __import__("threading").Lock()
        self._reads = 0
        self._writes = 0
        self._errors = 0
        self._prev_reads = 0
        self._prev_writes = 0
        self._warmup = warmup
        self._cooldown = cooldown
        self._planned = 0
        self._current = 0
        self._latencies: list[float] = []
        self._intervals: list[tuple[int, int]] = []

    def set_planned(self, n: int) -> None:
        self._planned = n

    def set_current(self, i: int) -> None:
        self._current = i

    def _include_lat(self) -> bool:
        if self._planned <= 0:
            return False
        hi = self._planned - self._cooldown
        return self._warmup <= self._current < hi

    def total_ops(self) -> int:
        return self._reads + self._writes

    def record(self, is_read: bool, latency_ms: float, is_error: bool) -> None:
        include = self._include_lat()
        with self._lock:
            if is_read:
                self._reads += 1
            else:
                self._writes += 1
            if is_error:
                self._errors += 1
            if include and not is_error:
                self._latencies.append(latency_ms)

    def end_interval(self) -> tuple[int, int, int]:
        with self._lock:
            dr = self._reads - self._prev_reads
            dw = self._writes - self._prev_writes
            self._prev_reads = self._reads
            self._prev_writes = self._writes
            self._intervals.append((dr, dw))
            return dr, dw, self._errors

    def summary(self) -> list[str]:
        ivs = self._intervals
        n = len(ivs)
        lo, hi = self._warmup, n - self._cooldown
        mid = ivs[lo:hi] if hi > lo else ivs

        def avg(xs: list[int]) -> float:
            return sum(xs) / len(xs) if xs else 0.0

        def median(xs: list[int]) -> float:
            if not xs:
                return 0.0
            ys = sorted(xs)
            m = len(ys) // 2
            return float(ys[m]) if len(ys) % 2 else (ys[m - 1] + ys[m]) / 2.0

        r = [x[0] for x in mid]
        w = [x[1] for x in mid]
        t = [x[0] + x[1] for x in mid]

        lines = [
            f"Summary (excluding {self._warmup} warmup + {self._cooldown} cooldown intervals):",
            f"  Read  TPS: avg={avg(r):.0f}  median={median(r):.0f}",
            f"  Write TPS: avg={avg(w):.0f}  median={median(w):.0f}",
            f"  Total TPS: avg={avg(t):.0f}  median={median(t):.0f}",
        ]

        lat = sorted(self._latencies)
        if lat:
            def pct(p: float) -> float:
                k = max(1, int(math.ceil(p / 100.0 * len(lat))))
                return lat[k - 1]

            lines.append(
                f"  Latency p50={pct(50):.1f}ms  p90={pct(90):.1f}ms  "
                f"p99={pct(99):.1f}ms  p99.9={pct(99.9):.1f}ms  "
                f"max={lat[-1]:.1f}ms"
            )

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            rss_mb = rss / (1024 * 1024)
        else:
            rss_mb = rss / 1024.0
        lines.append(f"  Peak RSS: {rss_mb:.1f} MB")

        if tracemalloc.is_tracing():
            _, peak = tracemalloc.get_traced_memory()
            lines.append(f"  Peak tracemalloc: {peak / (1024 * 1024):.1f} MB")

        return lines


# ---------------------------------------------------------------------------
# Bin value generation
# ---------------------------------------------------------------------------

_rng = random.Random()


def _random_bins(fields: list[tuple[str, str, int]]) -> list:
    """Return a list of Operation.put() for each field."""
    ops = []
    for name, kind, size in fields:
        if kind == "int":
            ops.append(Operation.put(name, _rng.randrange(1 << 30)))
        elif kind == "str":
            ops.append(Operation.put(name, _rng.randbytes(max(1, (size + 1) // 2)).hex()[:size]))
        else:
            ops.append(Operation.put(name, _rng.randbytes(size)))
    return ops


def _parse_bin_spec(spec: str) -> list[tuple[str, str, int]]:
    """Parse ``I1,S128,B1024`` into ``[(name, kind, size), ...]``."""
    import re
    tok_re = re.compile(r"^(I|S|B)(\d+)$", re.IGNORECASE)
    fields = []
    for i, tok in enumerate(spec.split(",")):
        m = tok_re.match(tok.strip())
        if not m:
            raise ValueError(f"invalid bin token {tok!r}")
        ch, n = m.group(1).upper(), int(m.group(2))
        kind = {"I": "int", "S": "str", "B": "bytes"}[ch]
        fields.append((f"b{i}", kind, n))
    return fields


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

async def _worker(
    client: Any,
    worker_id: int,
    cfg: argparse.Namespace,
    fields: list[tuple[str, str, int]],
    rp: ReadPolicy,
    wp: WritePolicy,
    stats: _Stats,
    stop: asyncio.Event,
) -> None:
    seed = (cfg.seed + worker_id + 1) % (2**32)
    rng = random.Random(seed)
    ns, sn = cfg.namespace, cfg.set_name
    key_count = cfg.keys
    read_pct = cfg.read_pct
    has_limit = cfg.max_ops is not None

    while not stop.is_set():
        if has_limit and stats.total_ops() >= cfg.max_ops:
            return

        kid = rng.randint(1, key_count)
        key = Key(ns, sn, kid)

        if cfg.workload == "I":
            is_read = False
        else:
            is_read = rng.randint(1, 100) <= read_pct

        t0 = time.perf_counter()
        try:
            if is_read:
                await client.get(rp, key, None)
            else:
                ops = _random_bins(fields)
                await client.operate(wp, key, ops)
        except Exception:
            dt = (time.perf_counter() - t0) * 1000.0
            stats.record(is_read, dt, True)
        else:
            dt = (time.perf_counter() - t0) * 1000.0
            stats.record(is_read, dt, False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def async_main() -> int:
    p = argparse.ArgumentParser(
        description="PAC benchmark — raw async client, no SDK layer.",
    )
    p.add_argument("-H", "--hosts", default=default_host(),
                   help="Cluster seed (default: %(default)s from aerospike.env).")
    p.add_argument("-n", "--namespace", default="test")
    p.add_argument("-s", "--set", dest="set_name", default="testset")
    p.add_argument("-k", "--keys", type=int, default=100_000, help="Key space size.")
    p.add_argument("-o", "--bins", default="I1", help="Bin spec (e.g. I1, I1,S128,B1024).")
    p.add_argument("-w", "--workload", default="RU,50",
                   help="Workload: I, RU,<read_pct> (default: %(default)s).")
    p.add_argument("-z", "--concurrency", type=int, default=32, help="Async tasks.")
    p.add_argument("-d", "--duration", type=float, default=10.0, help="Seconds.")
    p.add_argument("-c", "--max-ops", type=int, default=None, help="Stop after N ops.")
    p.add_argument("--warmup", type=int, default=4, help="Warmup intervals.")
    p.add_argument("--cooldown", type=int, default=4, help="Cooldown intervals.")
    p.add_argument("--seed", type=int, default=0, help="RNG seed; 0 = random.")

    args = p.parse_args()

    # Parse workload
    wl = args.workload.strip().upper()
    if wl == "I":
        args.workload = "I"
        args.read_pct = 0
    elif wl.startswith("RU"):
        parts = wl.split(",")
        args.workload = "RU"
        args.read_pct = int(parts[1]) if len(parts) > 1 else 50
    else:
        print(f"Unknown workload: {args.workload}", file=sys.stderr)
        return 2

    if args.seed == 0:
        args.seed = random.randint(1, 2**31 - 1)

    fields = _parse_bin_spec(args.bins)
    n_iv = max(1, math.ceil(args.duration))

    stats = _Stats(args.warmup, args.cooldown)
    stats.set_planned(n_iv)
    stop = asyncio.Event()

    tracemalloc.start()

    # Connect
    policy = default_client_policy()
    from aerospike_async import new_client
    client = await new_client(policy, args.hosts)

    print(f"Connected to {args.hosts}. Starting PAC benchmark ...")

    rp = ReadPolicy()
    wp = WritePolicy()

    tasks = [
        asyncio.create_task(
            _worker(client, i, args, fields, rp, wp, stats, stop)
        )
        for i in range(args.concurrency)
    ]

    for iv in range(n_iv):
        await asyncio.sleep(1.0)
        if stop.is_set():
            break
        stats.set_current(iv + 1)
        dr, dw, errs = stats.end_interval()
        total = dr + dw
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{stamp} write(tps={dw}) read(tps={dr}) total(tps={total} errors={errs})")

    stop.set()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await client.close()

    for line in stats.summary():
        print(line)
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()
