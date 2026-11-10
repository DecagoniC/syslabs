"""Characterization tests: фиксируют текущую семантику коллектора."""

from __future__ import annotations

import gzip
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1] / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as collector  # noqa: E402
from test_metrics import FakeResponse, reset_module_metrics, sample_cfg  # noqa: E402


def api_item(**overrides):
    item = {
        "id": 1,
        "timestamp": "2026-11-01T12:00:00Z",
        "location": {
            "id": 1,
            "country": "PL",
            "latitude": "1.5",
            "longitude": "2.5",
            "altitude": "10",
            "indoor": 0,
        },
        "sensor": {"id": 10, "pin": "1", "sensor_type": {"name": "SDS011"}},
        "sensordatavalues": [{"value_type": "P1", "value": "1.0"}, {"value_type": "P2", "value": "0.5"}],
    }
    item.update(overrides)
    return item


class BehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_module_metrics()
        self.received = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)

    def tearDown(self) -> None:
        reset_module_metrics()

    def test_event_from_api_ok_keeps_string_values(self) -> None:
        event = collector.event_from_api(api_item(), "sensors", self.received, frozenset({"PL"}))
        self.assertIsInstance(event, dict)
        assert isinstance(event, dict)
        self.assertEqual(event["event_key"], "10|2026-11-01T12:00:00.000Z")
        self.assertEqual(event["values"], {"P1": "1.0", "P2": "0.5"})
        self.assertEqual(event["country"], "PL")
        self.assertEqual(event["source"], "sensors")

    def test_event_from_api_wrong_country_returns_none(self) -> None:
        event = collector.event_from_api(api_item(), "sensors", self.received, frozenset({"AT"}))
        self.assertIsNone(event)

    def test_event_from_api_bad_returns_false(self) -> None:
        bad = api_item(timestamp="not-a-time")
        event = collector.event_from_api(bad, "sensors", self.received, frozenset({"PL"}))
        self.assertIs(event, False)

    def test_event_from_api_empty_countries_keeps_all(self) -> None:
        event = collector.event_from_api(api_item(), "sensors", self.received, frozenset())
        self.assertIsInstance(event, dict)

    def test_fingerprint_order_independent(self) -> None:
        a = collector.fingerprint({"b": "2", "a": "1"})
        b = collector.fingerprint({"a": "1", "b": "2"})
        self.assertEqual(a, b)

    def test_archive_slugs_dnms(self) -> None:
        self.assertEqual(collector.archive_slugs("DNMS (Laerm)")[0], "laerm")

    def test_history_days_exclusive_end(self) -> None:
        days = collector.history_days("2026-10-21", "2026-10-22")
        self.assertEqual([d.isoformat() for d in days], ["2026-10-21"])

    def test_poll_dedup_same_key(self) -> None:
        item = api_item()
        body = json.dumps([item, item]).encode()
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "stream"
            cfg = sample_cfg(raw_dir=raw, out_dir=out)
            now = self.received
            with mock.patch.object(collector, "open_once", return_value=FakeResponse(body)):
                counts = collector.poll_url(
                    cfg,
                    "http://example.test/pl",
                    now,
                    {},
                    out / "dt=2026-11-01" / "hour=12",
                    "part-test-PL.jsonl.gz",
                )
            self.assertEqual(counts["got"], 2)
            self.assertEqual(counts["new"], 1)
            self.assertEqual(counts["dup"], 1)

    def test_poll_empty_array_no_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "stream"
            cfg = sample_cfg(raw_dir=raw, out_dir=out)
            with mock.patch.object(collector, "open_once", return_value=FakeResponse(b"[]")):
                counts = collector.poll_url(
                    cfg,
                    "http://example.test/pl",
                    self.received,
                    {},
                    out / "dt=2026-11-01" / "hour=12",
                    "part-empty-PL.jsonl.gz",
                )
            self.assertEqual(counts["got"], 0)
            self.assertEqual(list(out.rglob("*.jsonl.gz")), [])

    def test_poll_stream_json_error_no_partial_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "stream"
            cfg = sample_cfg(raw_dir=raw, out_dir=out, retries=1)
            with mock.patch.object(collector, "open_once", return_value=FakeResponse(b"[{\"id\":")):
                counts = collector.poll_url(
                    cfg,
                    "http://example.test/pl",
                    self.received,
                    {},
                    out / "dt=2026-11-01" / "hour=12",
                    "part-bad-PL.jsonl.gz",
                )
            self.assertEqual(counts["got"], 0)
            self.assertEqual(list(out.rglob("*.jsonl.gz")), [])
            self.assertEqual(list(out.rglob("*.partial")), [])

    def test_scan_recent_keys_reads_written_file(self) -> None:
        item = api_item()
        body = json.dumps([item]).encode()
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "stream"
            cfg = sample_cfg(raw_dir=raw, out_dir=out)
            directory = out / "dt=2026-11-01" / "hour=12"
            with mock.patch.object(collector, "open_once", return_value=FakeResponse(body)):
                collector.poll_url(cfg, "http://example.test/pl", self.received, {}, directory, "part-scan-PL.jsonl.gz")
            found = collector.scan_recent_keys(out, self.received, 21600)
            self.assertIn("10|2026-11-01T12:00:00.000Z", found)

    def test_second_poll_marks_duplicate(self) -> None:
        item = api_item()
        body = json.dumps([item]).encode()
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "stream"
            cfg = sample_cfg(raw_dir=raw, out_dir=out)
            directory = out / "dt=2026-11-01" / "hour=12"
            seen: dict = {}
            with mock.patch.object(collector, "open_once", return_value=FakeResponse(body)):
                first = collector.poll_url(cfg, "http://example.test/pl", self.received, seen, directory, "part-a-PL.jsonl.gz")
            with mock.patch.object(collector, "open_once", return_value=FakeResponse(body)):
                second = collector.poll_url(cfg, "http://example.test/pl", self.received, seen, directory, "part-b-PL.jsonl.gz")
            self.assertEqual(first["new"], 1)
            self.assertEqual(second["new"], 0)
            self.assertEqual(second["dup"], 1)

    def test_csv_event_and_repeat_load_miss_after_ok_file_exists_path(self) -> None:
        csv_body = (
            "sensor_id;sensor_type;location;lat;lon;timestamp;P1\n"
            "10;SDS011;1;1.0;2.0;2026-10-21T01:00:00.000Z;1.5\n"
        ).encode()
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "history"
            cfg = sample_cfg(mode="backfill", raw_dir=raw, out_dir=out)
            with mock.patch.object(collector, "open_once", return_value=FakeResponse(csv_body)):
                status = collector.load_sensor_file(cfg, datetime(2026, 10, 21).date(), "10", "SDS011", "PL")
            self.assertEqual(status, "ok")
            path = out / "dt=2026-10-21" / "sensor-10.jsonl.gz"
            self.assertTrue(path.exists())
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["event_key"], "10|2026-10-21T01:00:00.000Z")
            self.assertEqual(row["country"], "PL")
            self.assertIsNone(row["measurement_id"])


if __name__ == "__main__":
    unittest.main()
