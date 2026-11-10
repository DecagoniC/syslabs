"""Проверка эквивалентности P1–P4: кеш slugs и одинаковый payload gzip."""

from __future__ import annotations

import gzip
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as c  # noqa: E402


def main() -> None:
    a = c.archive_slugs("SDS011")
    b = c.archive_slugs("SDS011")
    a.append("MUTATED")
    c_list = c.archive_slugs("SDS011")
    assert "MUTATED" not in c_list, c_list
    assert b == c_list

    dnms = c.archive_slugs("DNMS (Laerm)")
    assert dnms[0] == "laerm"
    dnms.clear()
    assert c.archive_slugs("DNMS (Laerm)")[0] == "laerm"

    received = datetime(2026, 10, 21, 15, 30, 45, 123000, tzinfo=timezone.utc)
    text = c.format_time(received)
    row = {
        "sensor_id": "10",
        "sensor_type": "SDS011",
        "location": "1",
        "lat": "1.0",
        "lon": "2.0",
        "timestamp": "2026-10-21T01:00:00.000Z",
        "P1": "1.5",
    }
    e1 = c.event_from_csv(row, "sensors", received, "PL", received_at_text=text)
    e2 = c.event_from_csv(row, "sensors", received, "PL")
    assert e1 == e2, (e1, e2)

    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        w1 = c.PartWriter(d, "a.jsonl.gz")
        w1.write(e1)
        p1 = w1.commit()
        w2 = c.PartWriter(d, "b.jsonl.gz")
        w2.write(e2)
        p2 = w2.commit()
        payload1 = gzip.decompress(p1.read_bytes())
        payload2 = gzip.decompress(p2.read_bytes())
        assert payload1 == payload2

    print("archive_slugs cache isolation: OK")
    print("event_from_csv received_at_text equivalence: OK")
    print("gzip payload identity: OK", "bytes", len(payload1))


if __name__ == "__main__":
    main()
