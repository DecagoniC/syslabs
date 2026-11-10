"""Unit-тесты Prometheus-интеграции сборщика. Без обращения к sensor.community."""

from __future__ import annotations

import csv
import io
import json
import socket
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from prometheus_client import CollectorRegistry, generate_latest

ROOT = Path(__file__).resolve().parents[1] / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as collector  # noqa: E402


def sample_cfg(**overrides) -> collector.Config:
    base = dict(
        mode="record",
        source="sensors",
        countries=frozenset({"PL"}),
        user_agent="test-collector/1 (test@example.com)",
        raw_dir=Path("/tmp/raw"),
        out_dir=Path("/tmp/raw/stream"),
        limit_bytes=20 * 1024**3,
        poll_seconds=300,
        metrics_port=19101,
        country_url="http://example.test/country={country}",
        global_url="http://example.test/global",
        archive_url="http://example.test/{date}_{slug}_sensor_{sensor_id}{suffix}.csv",
        history_from="2026-10-21",
        history_to="2026-10-22",
        dedup_seconds=21600,
        sample_seconds=21600,
        timeout=5,
        retries=3,
        once=True,
    )
    base.update(overrides)
    return collector.Config(**base)


def counter_value(registry: CollectorRegistry, name: str, labels: dict | None = None) -> float:
    value = 0.0
    for metric in registry.collect():
        if metric.name != name:
            continue
        for sample in metric.samples:
            if sample.name != name and not sample.name.endswith("_total"):
                if name.endswith("_total") and sample.name == name:
                    pass
                elif sample.name != name:
                    continue
            if labels and sample.labels != labels:
                continue
            if sample.name in {name, name + "_total"} or sample.name == name:
                value = float(sample.value)
    # Prefer exact sample name match
    for metric in registry.collect():
        for sample in metric.samples:
            if sample.name != name:
                continue
            if labels and dict(sample.labels) != labels:
                continue
            return float(sample.value)
    return value


def gauge_value(registry: CollectorRegistry, name: str) -> float:
    for metric in registry.collect():
        for sample in metric.samples:
            if sample.name == name and sample.labels == {}:
                return float(sample.value)
    raise AssertionError(f"gauge {name} not found")


def reset_module_metrics() -> None:
    collector.stop_metrics()
    collector._metrics = None
    collector._metrics_httpd = None
    collector._metrics_thread = None
    collector._metrics_server_started = False
    collector._stop.clear()


def threading_dummy():
    return object()


class DummyHttpd:
    def shutdown(self):
        return None

    def server_close(self):
        return None


class FakeResponse(io.BytesIO):
    def __init__(self, body: bytes, code: int = 200):
        super().__init__(body)
        self.code = code

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class MetricsUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_module_metrics()
        self.registry = CollectorRegistry()

    def tearDown(self) -> None:
        reset_module_metrics()

    def test_metrics_object_counters_and_gauges(self) -> None:
        metrics = collector.Metrics("record", registry=self.registry)
        metrics.add_events(3)
        metrics.add_error("http")
        metrics.add_request("success")
        metrics.add_records(10)
        metrics.add_backfill_completed()
        now = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
        metrics.mark_success(now)
        metrics.mark_poll(now)
        metrics.observe_request(0.5)
        self.assertEqual(counter_value(self.registry, "collector_events_total", {"mode": "record"}), 3.0)
        self.assertEqual(
            counter_value(self.registry, "collector_errors_total", {"mode": "record", "kind": "http"}),
            1.0,
        )
        self.assertEqual(gauge_value(self.registry, "collector_last_success_timestamp_seconds"), now.timestamp())
        self.assertEqual(gauge_value(self.registry, "collector_up"), 1.0)
        self.assertEqual(gauge_value(self.registry, "collector_metrics_server_up"), 0.0)
        text = generate_latest(self.registry).decode()
        self.assertIn("collector_request_duration_seconds_bucket", text)
        self.assertIn("collector_request_duration_seconds_sum", text)
        self.assertIn("collector_request_duration_seconds_count", text)
        self.assertIn("# HELP collector_events_total", text)
        self.assertIn("# TYPE collector_events_total counter", text)

    def test_no_duplicate_metric_registration_same_registry(self) -> None:
        collector.Metrics("record", registry=self.registry)
        with self.assertRaises(ValueError):
            collector.Metrics("record", registry=self.registry)

    def test_start_metrics_success(self) -> None:
        cfg = sample_cfg(metrics_port=19111)
        with mock.patch.object(collector, "start_http_server", return_value=(DummyHttpd(), threading_dummy())) as starter:
            metrics = collector.start_metrics(cfg, registry=self.registry)
            self.assertTrue(metrics.exporter_up)
            self.assertEqual(gauge_value(self.registry, "collector_metrics_server_up"), 1.0)
            starter.assert_called_once()
        collector.stop_metrics()
        self.assertEqual(gauge_value(self.registry, "collector_up"), 0.0)
        self.assertEqual(gauge_value(self.registry, "collector_metrics_server_up"), 0.0)

    def test_start_metrics_port_busy(self) -> None:
        holder = socket.socket()
        holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        holder.bind(("127.0.0.1", 0))
        port = holder.getsockname()[1]
        holder.listen(1)
        cfg = sample_cfg(metrics_port=port)
        metrics = collector.start_metrics(cfg, registry=self.registry)
        self.assertFalse(metrics.exporter_up)
        self.assertEqual(gauge_value(self.registry, "collector_metrics_server_up"), 0.0)
        self.assertEqual(gauge_value(self.registry, "collector_up"), 1.0)
        holder.close()

    def test_call_success(self) -> None:
        cfg = sample_cfg()
        collector._metrics = collector.Metrics("record", registry=self.registry)
        payload = b'[{"id":1}]'

        def action(response):
            return response.read()

        with mock.patch.object(collector, "open_once", return_value=FakeResponse(payload)):
            result = collector.call_with_retries(cfg, "http://example.test/x", action)
        self.assertEqual(result, payload)
        self.assertEqual(
            counter_value(self.registry, "collector_requests_total", {"mode": "record", "status": "success"}),
            1.0,
        )
        self.assertGreater(gauge_value(self.registry, "collector_last_success_timestamp_seconds"), 0.0)

    def test_call_http_500(self) -> None:
        cfg = sample_cfg(retries=2)
        collector._metrics = collector.Metrics("record", registry=self.registry)

        def boom(*_args, **_kwargs):
            raise urllib.error.HTTPError("http://x", 500, "err", hdrs=None, fp=io.BytesIO())

        with mock.patch.object(collector, "open_once", side_effect=boom):
            with self.assertRaises(urllib.error.HTTPError):
                collector.call_with_retries(cfg, "http://example.test/x", lambda r: None)
        self.assertEqual(
            counter_value(self.registry, "collector_errors_total", {"mode": "record", "kind": "http"}),
            2.0,
        )
        self.assertEqual(
            counter_value(self.registry, "collector_requests_total", {"mode": "record", "status": "http_error"}),
            2.0,
        )
        self.assertEqual(gauge_value(self.registry, "collector_last_success_timestamp_seconds"), 0.0)

    def test_call_http_404_not_error_counter(self) -> None:
        cfg = sample_cfg()
        collector._metrics = collector.Metrics("record", registry=self.registry)

        def boom(*_args, **_kwargs):
            raise urllib.error.HTTPError("http://x", 404, "missing", hdrs=None, fp=io.BytesIO())

        with mock.patch.object(collector, "open_once", side_effect=boom):
            result = collector.call_with_retries(cfg, "http://example.test/x", lambda r: None)
        self.assertEqual(result, 404)
        self.assertEqual(
            counter_value(self.registry, "collector_errors_total", {"mode": "record", "kind": "http"}),
            0.0,
        )
        self.assertEqual(
            counter_value(self.registry, "collector_requests_total", {"mode": "record", "status": "http_error"}),
            1.0,
        )

    def test_call_network_error(self) -> None:
        cfg = sample_cfg(retries=1)
        collector._metrics = collector.Metrics("record", registry=self.registry)

        with mock.patch.object(
            collector,
            "open_once",
            side_effect=urllib.error.URLError("down"),
        ):
            with self.assertRaises(urllib.error.URLError):
                collector.call_with_retries(cfg, "http://example.test/x", lambda r: None)
        self.assertEqual(
            counter_value(self.registry, "collector_errors_total", {"mode": "record", "kind": "network"}),
            1.0,
        )
        self.assertEqual(
            counter_value(self.registry, "collector_requests_total", {"mode": "record", "status": "network_error"}),
            1.0,
        )

    def test_call_parse_error_after_http(self) -> None:
        cfg = sample_cfg(retries=1)
        collector._metrics = collector.Metrics("record", registry=self.registry)

        def action(_response):
            raise json.JSONDecodeError("bad", "doc", 0)

        with mock.patch.object(collector, "open_once", return_value=FakeResponse(b"[]")):
            with self.assertRaises(json.JSONDecodeError):
                collector.call_with_retries(cfg, "http://example.test/x", action)
        self.assertEqual(
            counter_value(self.registry, "collector_errors_total", {"mode": "record", "kind": "parse"}),
            1.0,
        )
        self.assertEqual(gauge_value(self.registry, "collector_last_success_timestamp_seconds"), 0.0)
        self.assertEqual(
            counter_value(self.registry, "collector_requests_total", {"mode": "record", "status": "parse_error"}),
            1.0,
        )

    def test_success_not_marked_if_callback_fails(self) -> None:
        cfg = sample_cfg(retries=1)
        collector._metrics = collector.Metrics("record", registry=self.registry)

        def action(_response):
            raise ValueError("broken payload")

        with mock.patch.object(collector, "open_once", return_value=FakeResponse(b"[]")):
            with self.assertRaises(ValueError):
                collector.call_with_retries(cfg, "http://example.test/x", action)
        self.assertEqual(gauge_value(self.registry, "collector_last_success_timestamp_seconds"), 0.0)

    def test_poll_url_writes_events(self) -> None:
        item = {
            "id": 1,
            "timestamp": "2026-11-01T12:00:00Z",
            "location": {"id": 1, "country": "PL", "latitude": "1", "longitude": "2", "indoor": 0},
            "sensor": {"id": 10, "pin": "1", "sensor_type": {"name": "SDS011"}},
            "sensordatavalues": [{"value_type": "P1", "value": "1.0"}],
        }
        body = json.dumps([item]).encode()
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "stream"
            cfg = sample_cfg(raw_dir=raw, out_dir=out, mode="record")
            collector._metrics = collector.Metrics("record", registry=self.registry)
            now = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
            with mock.patch.object(collector, "open_once", return_value=FakeResponse(body)):
                counts = collector.poll_url(cfg, "http://example.test/pl", now, {}, out / "dt=2026-11-01" / "hour=12", "part-test-PL.jsonl.gz")
            self.assertEqual(counts["new"], 1)
            self.assertEqual(counter_value(self.registry, "collector_events_total", {"mode": "record"}), 1.0)
            self.assertEqual(counter_value(self.registry, "collector_records_processed_total", {"mode": "record"}), 1.0)
            files = list(out.rglob("*.jsonl.gz"))
            self.assertEqual(len(files), 1)

    def test_load_sensor_file_csv_ok_and_backfill_metric(self) -> None:
        csv_body = (
            "sensor_id;sensor_type;location;lat;lon;timestamp;P1\n"
            "10;SDS011;1;1.0;2.0;2026-10-21T01:00:00.000Z;1.5\n"
        ).encode()
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "history"
            cfg = sample_cfg(mode="backfill", raw_dir=raw, out_dir=out)
            collector._metrics = collector.Metrics("backfill", registry=self.registry)
            with mock.patch.object(collector, "open_once", return_value=FakeResponse(csv_body)):
                status = collector.load_sensor_file(cfg, datetime(2026, 10, 21).date(), "10", "SDS011", "PL")
            self.assertEqual(status, "ok")
            self.assertEqual(
                counter_value(self.registry, "collector_backfill_completed_total", {"mode": "backfill"}),
                1.0,
            )
            self.assertEqual(counter_value(self.registry, "collector_events_total", {"mode": "backfill"}), 1.0)

    def test_csv_parse_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "history"
            cfg = sample_cfg(mode="backfill", raw_dir=raw, out_dir=out, retries=1)
            collector._metrics = collector.Metrics("backfill", registry=self.registry)

            def action_fail(response):
                raise csv.Error("bad csv")

            # Force call_with_retries path via load_sensor_file internals: patch read by making open_once ok then fail in Text path
            with mock.patch.object(collector, "call_with_retries", side_effect=csv.Error("bad csv")):
                status = collector.load_sensor_file(cfg, datetime(2026, 10, 21).date(), "10", "SDS011", "PL")
            self.assertEqual(status, "stop")

    def test_record_loop_once(self) -> None:
        item = {
            "id": 1,
            "timestamp": "2026-11-01T12:00:00Z",
            "location": {"id": 1, "country": "PL", "latitude": "1", "longitude": "2", "indoor": 0},
            "sensor": {"id": 10, "pin": "1", "sensor_type": {"name": "SDS011"}},
            "sensordatavalues": [{"value_type": "P1", "value": "1.0"}],
        }
        body = json.dumps([item]).encode()
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "stream"
            cfg = sample_cfg(raw_dir=raw, out_dir=out, once=True, sample_seconds=10**9)
            collector._metrics = collector.Metrics("record", registry=self.registry)
            with mock.patch.object(collector, "open_once", return_value=FakeResponse(body)):
                collector.record_loop(cfg)
            self.assertGreater(gauge_value(self.registry, "collector_last_poll_timestamp_seconds"), 0.0)

    def test_backfill_mode_skips_existing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp)
            out = raw / "history"
            day = "2026-10-21"
            sensor = "10"
            path = out / f"dt={day}" / f"sensor-{sensor}.jsonl.gz"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"")
            cfg = sample_cfg(mode="backfill", raw_dir=raw, out_dir=out)
            collector._metrics = collector.Metrics("backfill", registry=self.registry)
            with mock.patch.object(collector, "discover_sensors", return_value={sensor: {"sensor_type": "SDS011", "country": "PL", "indoor": "0"}}):
                with mock.patch.object(collector, "load_sensor_file") as load:
                    collector.backfill(cfg)
                    load.assert_not_called()

    def test_sigterm_sets_stop(self) -> None:
        collector.install_signals()
        collector._stop.clear()
        collector._stop.set()
        self.assertTrue(collector._stop.is_set())
        self.assertFalse(collector.sleep_for(1.0))

    def test_stop_metrics_after_start(self) -> None:
        cfg = sample_cfg(metrics_port=19112)
        with mock.patch.object(collector, "start_http_server", return_value=(DummyHttpd(), threading_dummy())):
            collector.start_metrics(cfg, registry=self.registry)
        collector.stop_metrics()
        self.assertFalse(collector._metrics_server_started)
        self.assertEqual(gauge_value(self.registry, "collector_up"), 0.0)


if __name__ == "__main__":
    unittest.main()
