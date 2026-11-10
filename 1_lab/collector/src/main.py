"""Сборщик sensor.community: поток стран задания и архив за сутки.

Режим record пишет живой опрос в слой потока, backfill — архивные CSV
датчиков, которые поток отдаёт для этих стран. produce и replay — работа 6.
"""

import codecs
import csv
import gzip
import io
import json
import os
import re
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server
from prometheus_client.registry import REGISTRY


HISTORY_MODE = "backfill"
KNOWN_MODES = {"record", "backfill", "produce", "replay"}
REQUEST_STATUSES = frozenset({"success", "http_error", "network_error", "parse_error"})
ERROR_KINDS = frozenset({"http", "network", "parse"})
_JSON_SEPARATORS = (",", ":")
CSV_META = frozenset({"sensor_id", "sensor_type", "location", "lat", "lon", "timestamp"})
_archive_slugs_cache: dict[str, tuple[str, ...]] = {}

_stop = threading.Event()
_metrics: "Metrics | None" = None
_metrics_httpd = None
_metrics_thread = None
_metrics_server_started = False


@dataclass
class Config:
    mode: str
    source: str
    countries: frozenset[str]
    user_agent: str
    raw_dir: Path
    out_dir: Path
    limit_bytes: int
    poll_seconds: int
    metrics_port: int
    country_url: str
    global_url: str
    archive_url: str
    history_from: str | None
    history_to: str | None
    dedup_seconds: int
    sample_seconds: int
    timeout: int
    retries: int
    once: bool


class Metrics:
    """Метрики сборщика на prometheus_client. Labels только mode/status/kind."""

    def __init__(self, mode: str, registry: CollectorRegistry | None = None) -> None:
        reg = REGISTRY if registry is None else registry
        self.mode = mode
        self.registry = reg
        self.exporter_up = False

        self._events = Counter(
            "collector_events_total",
            "Events written to the raw layer",
            ["mode"],
            registry=reg,
        ).labels(mode=mode)
        self._errors = Counter(
            "collector_errors_total",
            "Failed source requests and data-processing errors",
            ["mode", "kind"],
            registry=reg,
        )
        self._last_success = Gauge(
            "collector_last_success_timestamp_seconds",
            "Unix time of last HTTP response successfully processed by the callback",
            registry=reg,
        )
        self._up = Gauge(
            "collector_up",
            "1 if the collector process is running, 0 when stopping or crashed",
            registry=reg,
        )
        self._metrics_server_up = Gauge(
            "collector_metrics_server_up",
            "1 if the Prometheus HTTP endpoint is serving /metrics",
            registry=reg,
        )
        self._requests = Counter(
            "collector_requests_total",
            "HTTP attempts to the source",
            ["mode", "status"],
            registry=reg,
        )
        self._request_duration = Histogram(
            "collector_request_duration_seconds",
            "Duration of HTTP requests to the source including response handling",
            ["mode"],
            buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
            registry=reg,
        ).labels(mode=mode)
        self._records = Counter(
            "collector_records_processed_total",
            "Source records inspected while building events",
            ["mode"],
            registry=reg,
        ).labels(mode=mode)
        self._last_poll = Gauge(
            "collector_last_poll_timestamp_seconds",
            "Unix time when the last record poll cycle finished",
            registry=reg,
        )
        self._backfill_completed = Counter(
            "collector_backfill_completed_total",
            "Archive sensor-day downloads finished with status ok",
            ["mode"],
            registry=reg,
        ).labels(mode=mode)

        self._up.set(1)
        self._metrics_server_up.set(0)

    def set_exporter_up(self, up: bool) -> None:
        self.exporter_up = up
        self._metrics_server_up.set(1 if up else 0)

    def set_up(self, up: bool) -> None:
        self._up.set(1 if up else 0)

    def add_events(self, count: int) -> None:
        if count:
            self._events.inc(count)

    def add_error(self, kind: str = "http") -> None:
        label = kind if kind in ERROR_KINDS else "http"
        self._errors.labels(mode=self.mode, kind=label).inc()

    def mark_success(self, when: datetime) -> None:
        self._last_success.set(int(when.timestamp()))

    def mark_poll(self, when: datetime) -> None:
        self._last_poll.set(int(when.timestamp()))

    def add_records(self, count: int) -> None:
        if count:
            self._records.inc(count)

    def add_backfill_completed(self) -> None:
        self._backfill_completed.inc()

    def observe_request(self, seconds: float) -> None:
        self._request_duration.observe(max(seconds, 0.0))

    def add_request(self, status: str) -> None:
        label = status if status in REQUEST_STATUSES else "network_error"
        self._requests.labels(mode=self.mode, status=label).inc()


def log(level: str, msg: str, **fields: object) -> None:
    record = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "level": level,
        "msg": msg,
    }
    record.update(fields)
    print(json.dumps(record, ensure_ascii=False, separators=_JSON_SEPARATORS), flush=True)


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return int(raw)


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        log("error", "config", name=name, reason="empty")
        raise SystemExit(2)
    return value


def load_config(mode: str) -> Config:
    source = require_env("SOURCE")
    user_agent = require_env("COLLECTOR_USER_AGENT")
    if "@" not in user_agent:
        log("error", "config", name="COLLECTOR_USER_AGENT", reason="need email")
        raise SystemExit(2)
    countries = frozenset(
        part.strip().upper()
        for part in os.environ.get("SOURCE_FILTER", "").split(",")
        if part.strip()
    )
    raw_dir = Path(os.environ.get("COLLECTOR_RAW_DIR", "/data/raw"))
    out_dir = os.environ.get("COLLECTOR_OUT_DIR", "").strip()
    if out_dir:
        output = Path(out_dir)
    elif mode == HISTORY_MODE:
        output = raw_dir / "history"
    else:
        output = raw_dir / "stream"
    poll = env_int("COLLECTOR_POLL_SECONDS", 300)
    once = os.environ.get("COLLECTOR_ONCE", "").strip() == "1"
    if poll < 300 and not once:
        log("warn", "config", name="COLLECTOR_POLL_SECONDS", using=300)
        poll = 300
    limit_gb = float(os.environ.get("COLLECTOR_RAW_LIMIT_GB", "20") or "20")
    return Config(
        mode=mode,
        source=source,
        countries=countries,
        user_agent=user_agent,
        raw_dir=raw_dir,
        out_dir=output,
        limit_bytes=int(limit_gb * 1024**3),
        poll_seconds=poll,
        metrics_port=env_int("COLLECTOR_METRICS_PORT", 9101),
        country_url=require_env("COLLECTOR_COUNTRY_URL"),
        global_url=require_env("COLLECTOR_GLOBAL_URL"),
        archive_url=require_env("COLLECTOR_ARCHIVE_URL"),
        history_from=os.environ.get("COLLECTOR_HISTORY_FROM", "").strip() or None,
        history_to=os.environ.get("COLLECTOR_HISTORY_TO", "").strip() or None,
        dedup_seconds=env_int("COLLECTOR_DEDUP_SECONDS", 6 * 3600),
        sample_seconds=env_int("COLLECTOR_SAMPLE_SECONDS", 6 * 3600),
        timeout=env_int("COLLECTOR_HTTP_TIMEOUT", 120),
        retries=env_int("COLLECTOR_RETRY_ATTEMPTS", 5),
        once=once,
    )


def install_signals() -> None:
    def handle(signum: int, _frame: object) -> None:
        _stop.set()
        log("info", "signal", signum=signum)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, handle)
        except (ValueError, OSError):
            continue


def sleep_for(seconds: float) -> bool:
    """Ждёт seconds. False — пришёл сигнал остановки."""
    end = time.monotonic() + seconds
    while not _stop.is_set():
        remain = end - time.monotonic()
        if remain <= 0:
            return True
        if _stop.wait(min(remain, 1)):
            return False
    return False


def format_time(moment: datetime) -> str:
    utc = moment.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def parse_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    millis = parsed.microsecond // 1000
    return parsed.replace(microsecond=millis * 1000)


def as_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fingerprint(values: dict[str, str]) -> str:
    return json.dumps(values, ensure_ascii=False, sort_keys=True, separators=_JSON_SEPARATORS)


def make_event(
    *,
    source: str,
    received_at: datetime,
    sensor_id: int,
    event_time: datetime,
    sensor_type: str | None,
    pin: str | None,
    location_id: int | None,
    latitude: str | None,
    longitude: str | None,
    altitude: str | None,
    country: str | None,
    indoor: int | None,
    measurement_id: int | None,
    values: dict[str, str],
    received_at_text: str | None = None,
) -> dict[str, object]:
    event_stamp = format_time(event_time)
    received_stamp = received_at_text if received_at_text is not None else format_time(received_at)
    return {
        "event_key": f"{sensor_id}|{event_stamp}",
        "event_time": event_stamp,
        "received_at": received_stamp,
        "source": source,
        "measurement_id": measurement_id,
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "pin": pin,
        "location_id": location_id,
        "latitude": latitude,
        "longitude": longitude,
        "altitude": altitude,
        "country": country,
        "indoor": indoor,
        "values": values,
    }


def event_from_api(
    item: dict[str, object],
    source: str,
    received_at: datetime,
    countries: frozenset[str],
) -> dict[str, object] | bool | None:
    # False — битая запись (нет sensor_id/timestamp); None — отброшена фильтром страны.
    location_raw = item.get("location")
    location = location_raw if isinstance(location_raw, dict) else {}
    country = str(location.get("country") or "").upper()
    if countries and country not in countries:
        return None
    sensor_raw = item.get("sensor")
    sensor = sensor_raw if isinstance(sensor_raw, dict) else {}
    sensor_id = as_int(sensor.get("id"))
    event_time = parse_time(item.get("timestamp"))
    if sensor_id is None or event_time is None:
        return False
    sensor_type = sensor.get("sensor_type")
    type_name = None
    if isinstance(sensor_type, dict) and sensor_type.get("name"):
        type_name = str(sensor_type["name"])
    values: dict[str, str] = {}
    rows = item.get("sensordatavalues")
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            value_type = row.get("value_type")
            if not value_type:
                continue
            value = row.get("value")
            if value is None:
                continue
            values[str(value_type)] = str(value)
    pin = sensor.get("pin")
    latitude = location.get("latitude")
    longitude = location.get("longitude")
    altitude = location.get("altitude")
    return make_event(
        source=source,
        received_at=received_at,
        sensor_id=sensor_id,
        event_time=event_time,
        sensor_type=type_name,
        pin=None if pin is None else str(pin),
        location_id=as_int(location.get("id")),
        latitude=None if latitude is None else str(latitude),
        longitude=None if longitude is None else str(longitude),
        altitude=None if altitude is None else str(altitude),
        country=country or None,
        indoor=as_int(location.get("indoor")),
        measurement_id=as_int(item.get("id")),
        values=values,
    )


def archive_slugs(sensor_type: str) -> list[str]:
    cached = _archive_slugs_cache.get(sensor_type)
    if cached is not None:
        return list(cached)

    def norm(text: str) -> str:
        text = re.sub(r"[^a-z0-9_-]", "", text.lower().replace(" ", "_"))
        return re.sub(r"_+", "_", text).strip("_")

    slugs: list[str] = []

    def add(text: str) -> None:
        slug = norm(text)
        if slug and slug not in slugs:
            slugs.append(slug)

    wrapped = re.search(r"\(([^)]+)\)", sensor_type)
    if wrapped:
        add(wrapped.group(1))
    add(re.sub(r"\([^)]*\)", " ", sensor_type))
    add(sensor_type)
    _archive_slugs_cache[sensor_type] = tuple(slugs)
    return list(slugs)


def history_days(start: str, end: str) -> list[date]:
    first = date.fromisoformat(start)
    last = date.fromisoformat(end)
    if last < first:
        raise ValueError("COLLECTOR_HISTORY_TO раньше COLLECTOR_HISTORY_FROM")
    if last == first:
        return [first]
    days: list[date] = []
    current = first
    while current < last:
        days.append(current)
        current += timedelta(days=1)
    return days


class PartWriter:
    def __init__(self, directory: Path, name: str) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.final = directory / name
        self.partial = directory / f"{name}.partial"
        self._gz = gzip.open(self.partial, "wb", compresslevel=6)
        self.lines = 0

    def write(self, event: dict[str, object]) -> None:
        line = json.dumps(event, ensure_ascii=False, separators=_JSON_SEPARATORS).encode("utf-8")
        self._gz.write(line + b"\n")
        self.lines += 1

    def commit(self) -> Path:
        self._gz.close()
        os.replace(self.partial, self.final)
        return self.final

    def abort(self) -> None:
        try:
            self._gz.close()
        except Exception:
            pass
        self.partial.unlink(missing_ok=True)


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def drop_partials(root: Path) -> None:
    if not root.exists():
        return
    for path in root.rglob("*.partial"):
        path.unlink(missing_ok=True)


def partition_time(path: Path) -> datetime | None:
    day = None
    hour = "00"
    for part in path.parts:
        if part.startswith("dt="):
            day = part[3:]
        elif part.startswith("hour="):
            hour = part[5:]
    if day is None:
        return None
    try:
        return datetime.fromisoformat(f"{day}T{hour}:00:00+00:00")
    except ValueError:
        return None


def scan_recent_keys(stream_dir: Path, now: datetime, window: int) -> dict[str, tuple[datetime, str]]:
    found: dict[str, tuple[datetime, str]] = {}
    if not stream_dir.exists():
        return found
    cutoff = now - timedelta(seconds=window)
    for path in stream_dir.rglob("*.jsonl.gz"):
        opened = partition_time(path)
        if opened is None or opened < cutoff:
            continue
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    key = event.get("event_key")
                    if not isinstance(key, str):
                        continue
                    raw_values = event.get("values")
                    values = raw_values if isinstance(raw_values, dict) else {}
                    found[key] = (opened, fingerprint(values))
        except (OSError, json.JSONDecodeError, EOFError, gzip.BadGzipFile) as exc:
            log("warn", "scan", file=path.name, error=str(exc))
    return found


def closed_files(raw_dir: Path) -> list[Path]:
    files: list[Path] = []
    for kind in ("history", "stream"):
        root = raw_dir / kind
        if not root.exists():
            continue
        files.extend(path for path in root.rglob("*.jsonl.gz") if path.is_file())
    files.sort(key=file_sort_key)
    return files


def file_sort_key(path: Path) -> tuple[str, str, str]:
    day = "9999-99-99"
    hour = ""
    for part in path.parts:
        if part.startswith("dt="):
            day = part[3:]
        elif part.startswith("hour="):
            hour = part[5:]
    return (day, hour, path.name)


def enforce_limit(raw_dir: Path, limit_bytes: int) -> None:
    files = closed_files(raw_dir)
    total = 0
    sizes: list[int] = []
    for path in files:
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        sizes.append(size)
        total += size
    removed = 0
    index = 0
    while total > limit_bytes and index < len(files) - 1:
        path = files[index]
        size = sizes[index]
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            log("warn", "rotated", file=path.name, error=str(exc))
            break
        total -= size
        removed += 1
        index += 1
    if removed:
        log("info", "rotated", files=removed)


def iter_json_array(chunks):
    """Читает JSON-массив объектами, не собирая ответ целиком."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    parser = json.JSONDecoder()
    buf = ""
    eof = False

    def pull() -> bool:
        nonlocal buf, eof
        if eof:
            return False
        chunk = next(chunks, None)
        if not chunk:
            buf += decoder.decode(b"", final=True)
            eof = True
            return False
        buf += decoder.decode(chunk)
        return True

    while True:
        buf = buf.lstrip("\ufeff \t\r\n")
        if buf:
            break
        if not pull():
            return
    if buf[0] != "[":
        raise ValueError("ожидался JSON-массив")
    buf = buf[1:]
    while True:
        buf = buf.lstrip()
        if not buf:
            if not pull():
                return
            continue
        if buf[0] == "]":
            return
        if buf[0] == ",":
            buf = buf[1:]
            continue
        while True:
            try:
                obj, index = parser.raw_decode(buf)
            except json.JSONDecodeError:
                if not pull():
                    raise
                continue
            if not isinstance(obj, dict):
                raise ValueError("элемент потока не объект")
            yield obj
            buf = buf[index:]
            break


def byte_chunks(response, size: int = 65536):
    while True:
        chunk = response.read(size)
        if not chunk:
            return
        yield chunk


def retryable(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code == 429 or 500 <= exc.code <= 599
    return isinstance(exc, (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError, ValueError))


def classify_failure(exc: BaseException) -> tuple[str, str]:
    """Возвращает (request_status, error_kind)."""
    if isinstance(exc, urllib.error.HTTPError):
        return "http_error", "http"
    if isinstance(exc, (json.JSONDecodeError, ValueError, csv.Error, UnicodeError, gzip.BadGzipFile)):
        return "parse_error", "parse"
    return "network_error", "network"


def open_once(cfg: Config, url: str):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": cfg.user_agent, "Accept": "application/json, text/csv, */*"},
    )
    return urllib.request.urlopen(request, timeout=cfg.timeout)


def call_with_retries(cfg: Config, url: str, action):
    pause = 1
    for attempt in range(1, cfg.retries + 1):
        if _stop.is_set():
            return None
        started = time.monotonic()
        try:
            with open_once(cfg, url) as response:
                result = action(response)
            elapsed = time.monotonic() - started
            if _metrics is not None:
                _metrics.observe_request(elapsed)
                _metrics.add_request("success")
                _metrics.mark_success(datetime.now(timezone.utc))
            return result
        except Exception as exc:
            elapsed = time.monotonic() - started
            status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
            req_status, err_kind = classify_failure(exc)
            if isinstance(exc, urllib.error.HTTPError):
                exc.close()
            if _metrics is not None:
                _metrics.observe_request(elapsed)
            if status == 404:
                if _metrics is not None:
                    _metrics.add_request("http_error")
                return 404
            if not retryable(exc) or attempt == cfg.retries:
                if _metrics is not None:
                    _metrics.add_request(req_status)
                    _metrics.add_error(err_kind)
                log(
                    "warn" if status else "error",
                    "http",
                    status=status or 0,
                    url=url,
                    error=str(exc),
                )
                raise
            if _metrics is not None:
                _metrics.add_request(req_status)
                _metrics.add_error(err_kind)
            log("warn", "http", status=status or 0, pause=pause, url=url)
            if not sleep_for(pause):
                return None
            log("info", "retry", attempt=attempt + 1, url=url)
            pause = min(pause * 2, 60)
    return None


def start_metrics(cfg: Config, registry: CollectorRegistry | None = None) -> Metrics:
    """Создаёт метрики и поднимает HTTP /metrics. Сбор данных не падает при ошибке порта."""
    global _metrics, _metrics_httpd, _metrics_thread, _metrics_server_started
    if _metrics is not None and _metrics_server_started:
        log("warn", "metrics", reason="already_started", port=cfg.metrics_port)
        return _metrics
    if _metrics is None:
        _metrics = Metrics(cfg.mode, registry=registry)
    if _metrics_server_started:
        return _metrics
    try:
        _metrics_httpd, _metrics_thread = start_http_server(
            cfg.metrics_port,
            registry=_metrics.registry,
        )
    except OSError as exc:
        _metrics.set_exporter_up(False)
        log("error", "metrics", port=cfg.metrics_port, error=str(exc), exporter="down")
        return _metrics
    _metrics_server_started = True
    _metrics.set_exporter_up(True)
    log("info", "metrics", port=cfg.metrics_port, exporter="up")
    return _metrics


def stop_metrics() -> None:
    """Гасит collector_up и останавливает HTTP-сервер метрик."""
    global _metrics_httpd, _metrics_thread, _metrics_server_started
    if _metrics is not None:
        _metrics.set_up(0)
        _metrics.set_exporter_up(False)
    httpd = _metrics_httpd
    _metrics_httpd = None
    _metrics_thread = None
    _metrics_server_started = False
    if httpd is None:
        return
    try:
        httpd.shutdown()
    except Exception as exc:
        log("warn", "metrics", action="shutdown", error=str(exc))
    try:
        httpd.server_close()
    except Exception:
        pass
    log("info", "metrics", exporter="down")


def sample_global(cfg: Config) -> None:
    def read(response) -> tuple[int, int]:
        got = 0
        kept = 0
        for item in iter_json_array(byte_chunks(response)):
            got += 1
            location_raw = item.get("location")
            location = location_raw if isinstance(location_raw, dict) else {}
            country = str(location.get("country") or "").upper()
            if not cfg.countries or country in cfg.countries:
                kept += 1
        return got, kept

    try:
        result = call_with_retries(cfg, cfg.global_url, read)
    except Exception as exc:
        log("error", "sample", error=str(exc))
        return
    if not isinstance(result, tuple):
        return
    got, kept = result
    log("info", "sample", got=got, kept=kept)


def remember(
    seen: dict[str, tuple[datetime, str]],
    pending: list[tuple[str, str]],
    received_at: datetime,
) -> None:
    for key, digest in pending:
        seen[key] = (received_at, digest)


def prune_seen(seen: dict[str, tuple[datetime, str]], now: datetime, window: int) -> None:
    cutoff = now - timedelta(seconds=window)
    stale = [key for key, (moment, _digest) in seen.items() if moment < cutoff]
    for key in stale:
        del seen[key]


def poll_url(cfg: Config, url: str, received_at: datetime, seen, directory: Path, name: str) -> dict[str, int]:
    empty = {"got": 0, "kept": 0, "new": 0, "dup": 0, "conflict": 0, "bad": 0}

    def read(response) -> tuple[dict[str, int], list[tuple[dict[str, object], str]]]:
        counts = dict(empty)
        fresh: list[tuple[dict[str, object], str]] = []
        local: dict[str, str] = {}
        for item in iter_json_array(byte_chunks(response)):
            counts["got"] += 1
            event = event_from_api(item, cfg.source, received_at, cfg.countries)
            if event is None:
                continue
            counts["kept"] += 1
            if event is False:
                counts["bad"] += 1
                continue
            key = str(event["event_key"])
            raw_values = event["values"]
            values = raw_values if isinstance(raw_values, dict) else {}
            digest = fingerprint(values)
            previous = seen.get(key)
            if previous is not None or key in local:
                counts["dup"] += 1
                old = previous[1] if previous is not None else local[key]
                if old != digest:
                    counts["conflict"] += 1
                continue
            local[key] = digest
            fresh.append((event, digest))
            counts["new"] += 1
        return counts, fresh

    try:
        result = call_with_retries(cfg, url, read)
    except Exception as exc:
        log("error", "poll", url=url, error=str(exc))
        return dict(empty)
    if result == 404:
        log("warn", "http", status=404, url=url)
        return dict(empty)
    if not isinstance(result, tuple):
        return dict(empty)
    counts, fresh = result
    if _metrics is not None and counts["got"]:
        _metrics.add_records(counts["got"])
    if not fresh:
        return counts
    writer = PartWriter(directory, name)
    try:
        for event, _digest in fresh:
            writer.write(event)
        path = writer.commit()
    except Exception as exc:
        writer.abort()
        if _metrics is not None:
            _metrics.add_error("parse")
        log("error", "write", file=name, error=str(exc))
        raise
    remember(seen, [(str(event["event_key"]), digest) for event, digest in fresh], received_at)
    if _metrics is not None:
        _metrics.add_events(writer.lines)
    log("info", "closed", lines=writer.lines, file=str(path.relative_to(cfg.raw_dir)))
    return counts


def record_loop(cfg: Config) -> None:
    state_path = cfg.raw_dir / "state" / "record.json"
    drop_partials(cfg.out_dir)
    now = datetime.now(timezone.utc)
    seen = scan_recent_keys(cfg.out_dir, now, cfg.dedup_seconds)
    state = load_json(state_path)
    log("info", "start", mode=cfg.mode, seen=len(seen))
    while not _stop.is_set():
        now = datetime.now(timezone.utc)
        last_text = state.get("last_poll_at")
        if isinstance(last_text, str):
            last = parse_time(last_text)
            if last is not None:
                wait = cfg.poll_seconds - (now - last).total_seconds()
                if wait > 0 and not sleep_for(wait):
                    break
                now = datetime.now(timezone.utc)
        sampled = parse_time(state["sampled_at"]) if isinstance(state.get("sampled_at"), str) else None
        if sampled is None or (now - sampled).total_seconds() >= cfg.sample_seconds:
            sample_global(cfg)
            state["sampled_at"] = format_time(datetime.now(timezone.utc))
            atomic_write(state_path, json.dumps(state))
        now = datetime.now(timezone.utc)
        state["last_poll_at"] = format_time(now)
        atomic_write(state_path, json.dumps(state))
        prune_seen(seen, now, cfg.dedup_seconds)
        day = now.strftime("%Y-%m-%d")
        hour = now.strftime("%H")
        directory = cfg.out_dir / f"dt={day}" / f"hour={hour}"
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        totals = {"got": 0, "kept": 0, "new": 0, "dup": 0, "conflict": 0, "bad": 0}
        targets = [(country, cfg.country_url.format(country=quote(country))) for country in sorted(cfg.countries)]
        if not targets:
            targets = [("all", cfg.global_url)]
        for country, url in targets:
            if _stop.is_set():
                break
            counts = poll_url(cfg, url, now, seen, directory, f"part-{stamp}-{country}.jsonl.gz")
            for key, value in counts.items():
                totals[key] += value
        log("info", "poll", **totals)
        if _metrics is not None:
            _metrics.mark_poll(datetime.now(timezone.utc))
        enforce_limit(cfg.raw_dir, cfg.limit_bytes)
        if cfg.once or _stop.is_set():
            break
        if not sleep_for(cfg.poll_seconds):
            break


def event_from_csv(
    row: dict[str, str | None],
    source: str,
    received_at: datetime,
    country: str,
    received_at_text: str | None = None,
) -> dict[str, object] | None:
    sensor_id = as_int(row.get("sensor_id"))
    event_time = parse_time(row.get("timestamp"))
    if sensor_id is None or event_time is None:
        return None
    values = {
        key: value
        for key, value in row.items()
        if key and key not in CSV_META and value not in (None, "")
    }
    sensor_type = row.get("sensor_type") or None
    return make_event(
        source=source,
        received_at=received_at,
        sensor_id=sensor_id,
        event_time=event_time,
        sensor_type=sensor_type,
        pin=None,
        location_id=as_int(row.get("location")),
        latitude=row.get("lat") or None,
        longitude=row.get("lon") or None,
        altitude=None,
        country=country,
        indoor=None,
        measurement_id=None,
        values=values,
        received_at_text=received_at_text,
    )


def discover_sensors(cfg: Config) -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}

    def consume(country: str):
        def read(response) -> None:
            for item in iter_json_array(byte_chunks(response)):
                event = event_from_api(item, cfg.source, datetime.now(timezone.utc), cfg.countries)
                if not isinstance(event, dict) or not event.get("sensor_type"):
                    continue
                sensor_id = str(event["sensor_id"])
                indoor = event.get("indoor")
                found.setdefault(
                    sensor_id,
                    {
                        "sensor_type": str(event["sensor_type"]),
                        "country": str(event.get("country") or ""),
                        "indoor": "1" if indoor == 1 else "0",
                    },
                )

        return read

    for country in sorted(cfg.countries):
        if _stop.is_set():
            break
        url = cfg.country_url.format(country=quote(country))
        try:
            call_with_retries(cfg, url, consume(country))
        except Exception as exc:
            log("error", "discover", country=country, error=str(exc))
    return found


def load_sensor_file(
    cfg: Config,
    day: date,
    sensor_id: str,
    sensor_type: str,
    country: str,
    indoor: str = "0",
) -> str:
    """Пишет файл датчика. Возвращает ok, miss или stop."""
    received_at = datetime.now(timezone.utc)
    received_at_text = format_time(received_at)
    writers: dict[str, PartWriter] = {}
    seen: set[str] = set()

    def finish(status: str) -> str:
        if status != "ok":
            for writer in writers.values():
                writer.abort()
            return status
        for writer in writers.values():
            if writer.lines == 0:
                writer.abort()
                continue
            path = writer.commit()
            if _metrics is not None:
                _metrics.add_events(writer.lines)
            log(
                "info",
                "closed",
                lines=writer.lines,
                file=str(path.relative_to(cfg.raw_dir)),
            )
        return "ok"

    suffixes = ["_indoor", ""] if indoor == "1" else ["", "_indoor"]
    for slug in archive_slugs(sensor_type):
        for suffix in suffixes:
            url = cfg.archive_url.format(
                date=day.isoformat(),
                slug=slug,
                sensor_id=sensor_id,
                suffix=suffix,
            )
            holder: dict[str, object] = {"status": None}

            def read(response, slug=slug) -> bool:
                nonlocal writers, seen
                for previous in writers.values():
                    previous.abort()
                writers = {}
                seen = set()
                processed = 0
                text = io.TextIOWrapper(response, encoding="utf-8", newline="")
                try:
                    header = text.readline()
                    if not header:
                        holder["status"] = "miss"
                        return True
                    header = header.lstrip("\ufeff")
                    delimiter = ";" if header.count(";") >= header.count(",") else ","
                    parsed = csv.DictReader(iter_header(header, text), delimiter=delimiter)
                    for row in parsed:
                        processed += 1
                        event = event_from_csv(
                            row,
                            cfg.source,
                            received_at,
                            country,
                            received_at_text=received_at_text,
                        )
                        if event is None:
                            continue
                        key = str(event["event_key"])
                        if key in seen:
                            continue
                        seen.add(key)
                        event_day = str(event["event_time"])[:10]
                        writer = writers.get(event_day)
                        if writer is None:
                            directory = cfg.out_dir / f"dt={event_day}"
                            writer = PartWriter(directory, f"sensor-{sensor_id}.jsonl.gz")
                            writers[event_day] = writer
                        writer.write(event)
                    if _metrics is not None and processed:
                        _metrics.add_records(processed)
                    holder["status"] = "ok"
                    holder["slug"] = slug
                    return True
                finally:
                    text.detach()

            try:
                result = call_with_retries(cfg, url, read)
            except Exception as exc:
                for writer in writers.values():
                    writer.abort()
                writers.clear()
                log("error", "history", sensor_id=sensor_id, date=day.isoformat(), error=str(exc))
                return "stop"
            if result == 404:
                continue
            if result is None:
                for writer in writers.values():
                    writer.abort()
                return "stop"
            outcome = finish(str(holder.get("status") or "miss"))
            if outcome == "ok" and _metrics is not None:
                _metrics.add_backfill_completed()
            return outcome
    log("info", "miss", sensor_id=sensor_id, date=day.isoformat(), sensor_type=sensor_type)
    return "miss"


def iter_header(header: str, rest):
    yield header
    for line in rest:
        yield line


def backfill(cfg: Config) -> None:
    if not cfg.history_from or not cfg.history_to:
        log("error", "config", name="COLLECTOR_HISTORY_FROM", reason="empty")
        raise SystemExit(2)
    days = history_days(cfg.history_from, cfg.history_to)
    state_path = cfg.raw_dir / "state" / "backfill.json"
    state = load_json(state_path)
    saved = state.get("sensors") if isinstance(state.get("sensors"), dict) else {}
    done = set(state.get("done") or [])
    log("info", "start", mode=cfg.mode, days=",".join(day.isoformat() for day in days))
    fresh = discover_sensors(cfg)
    sensors = {str(key): value for key, value in saved.items() if isinstance(value, dict)}
    sensors.update(fresh)
    state["sensors"] = sensors
    atomic_write(state_path, json.dumps(state))
    log("info", "discover", sensors=len(sensors))
    drop_partials(cfg.out_dir)
    finished = 0
    for day in days:
        for sensor_id in sorted(sensors, key=lambda item: int(item)):
            if _stop.is_set():
                break
            mark = f"{day.isoformat()}/{sensor_id}"
            final = cfg.out_dir / f"dt={day.isoformat()}" / f"sensor-{sensor_id}.jsonl.gz"
            if mark in done or final.exists():
                done.add(mark)
                continue
            info = sensors[sensor_id]
            status = load_sensor_file(
                cfg,
                day,
                sensor_id,
                str(info.get("sensor_type") or ""),
                str(info.get("country") or ""),
                str(info.get("indoor") or "0"),
            )
            if status == "stop":
                _stop.set()
                break
            done.add(mark)
            finished += 1
            state["done"] = sorted(done)
            atomic_write(state_path, json.dumps({"sensors": sensors, "done": state["done"]}))
            enforce_limit(cfg.raw_dir, cfg.limit_bytes)
            if finished % 100 == 0:
                log("info", "backfill", done=len(done), sensors=len(sensors) * len(days))
        if _stop.is_set():
            break
    log("info", "backfill", done=len(done), sensors=len(sensors) * len(days))


def resolve_mode() -> str:
    if len(sys.argv) > 1:
        return sys.argv[1]
    return os.environ.get("COLLECTOR_MODE", "record")


def main() -> None:
    install_signals()
    mode = resolve_mode()
    if mode not in KNOWN_MODES:
        log("error", "config", mode=mode, reason="unknown")
        raise SystemExit(2)
    if mode in {"produce", "replay"}:
        log("error", "unsupported", mode=mode)
        raise SystemExit(2)
    cfg = load_config(mode)
    start_metrics(cfg)
    try:
        if mode == HISTORY_MODE:
            backfill(cfg)
        else:
            record_loop(cfg)
    finally:
        stop_metrics()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        if _metrics is not None:
            _metrics.set_up(0)
            _metrics.set_exporter_up(False)
        log("error", "crash", error=str(exc))
        raise
