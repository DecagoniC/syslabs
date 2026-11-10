"""Микробенч горячих путей. Запуск: python -m tests.bench_hotpaths"""

from __future__ import annotations

import gzip
import json
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as collector  # noqa: E402


def make_item(i: int) -> dict:
    return {
        "id": i,
        "timestamp": "2026-11-01T12:00:00Z",
        "location": {
            "id": i,
            "country": "PL",
            "latitude": "1.5",
            "longitude": "2.5",
            "altitude": "10",
            "indoor": 0,
        },
        "sensor": {"id": i, "pin": "1", "sensor_type": {"name": "SDS011"}},
        "sensordatavalues": [
            {"value_type": "P1", "value": str(i)},
            {"value_type": "P2", "value": str(i / 2)},
        ],
    }


def timed(name: str, rounds: int, fn) -> float:
    samples = []
    for _ in range(rounds):
        started = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - started)
    mid = statistics.median(samples)
    print(f"{name}: median={mid:.6f}s min={min(samples):.6f}s max={max(samples):.6f}s n={rounds}")
    return mid


def main() -> None:
    received = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
    countries = frozenset({"PL", "CZ", "AT"})
    items = [make_item(i) for i in range(3000)]

    def parse_api() -> None:
        for item in items:
            collector.event_from_api(item, "sensors", received, countries)

    values = [{"P1": str(i), "P2": str(i / 2), "temperature": "20"} for i in range(3000)]

    def fingerprints() -> None:
        for row in values:
            collector.fingerprint(row)

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "stream" / "dt=2026-11-01" / "hour=12"
        root.mkdir(parents=True)
        path = root / "part-bench-PL.jsonl.gz"
        with gzip.open(path, "wt", encoding="utf-8", compresslevel=6) as handle:
            for item in items:
                event = collector.event_from_api(item, "sensors", received, countries)
                handle.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")

        def scan() -> None:
            collector.scan_recent_keys(Path(tmp) / "stream", received, 21600)

        print("=== bench ===")
        timed("event_from_api_3000", 7, parse_api)
        timed("fingerprint_3000", 7, fingerprints)
        timed("scan_recent_keys_3000", 5, scan)


if __name__ == "__main__":
    main()
