"""Проверка схемы Avro на событиях одного часа сырого слоя.

Читает JSON Lines из data/raw/stream/dt=.../hour=..., кодирует каждое
событие схемой Avro через fastavro и читает обратно. Печатает размер часа
в JSON и в двоичной кодировке, а также события, которые не прошли схему.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import fastavro
except ImportError:
    sys.exit("нужен пакет fastavro: python -m pip install fastavro")


def parse_millis(value: object) -> int | None:
    """Переводит ISO-8601 источника в миллисекунды UTC."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp() * 1000)


def load_schema(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def full_record(event: dict) -> dict:
    """Собирает запись со всеми полями схемы; лишние поля отбросит record_for_schema."""
    value_map = event.get("values")
    values = value_map if isinstance(value_map, dict) else {}
    return {
        "event_key": str(event.get("event_key") or ""),
        "event_time": parse_millis(event.get("event_time")),
        "received_at": parse_millis(event.get("received_at")),
        "source": str(event.get("source") or ""),
        "measurement_id": event.get("measurement_id"),
        "sensor_id": event.get("sensor_id"),
        "sensor_type": event.get("sensor_type"),
        "pin": event.get("pin"),
        "location_id": event.get("location_id"),
        "latitude": event.get("latitude"),
        "longitude": event.get("longitude"),
        "altitude": event.get("altitude"),
        "country": event.get("country"),
        "indoor": event.get("indoor"),
        "values": {str(key): str(value) for key, value in values.items()},
    }


def record_for_schema(record: dict, schema: dict) -> dict:
    names = {field["name"] for field in schema["fields"]}
    return {name: record[name] for name in names if name in record}


def latest_hour(stream_dir: Path, requested: str | None) -> Path:
    if requested:
        path = Path(requested)
        if not path.is_absolute():
            path = stream_dir / path
        if not path.is_dir():
            raise SystemExit(f"нет каталога часа: {path}")
        return path
    hours = sorted(path for path in stream_dir.glob("dt=*/hour=*") if path.is_dir())
    if not hours:
        raise SystemExit(f"в {stream_dir} нет каталогов dt=*/hour=*")
    return hours[-1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--hour", help="каталог часа: dt=ГГГГ-ММ-ДД/hour=ЧЧ или абсолютный путь")
    parser.add_argument("--write-schema", type=Path, default=Path("schemas/event.avsc"))
    parser.add_argument("--read-schema", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None, help="обработать не больше N событий")
    args = parser.parse_args()

    stream_dir = args.raw_dir / "stream"
    hour_dir = latest_hour(stream_dir, args.hour)
    write_schema = load_schema(args.write_schema)
    read_schema = load_schema(args.read_schema) if args.read_schema else write_schema

    events = 0
    json_bytes = 0
    avro_bytes = 0
    failures = 0
    files = sorted(hour_dir.glob("*.jsonl.gz"))

    for path in files:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                if args.limit is not None and events >= args.limit:
                    break
                events += 1
                json_bytes += len(line.encode("utf-8"))
                event: object = None
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        raise ValueError("событие не объект JSON")
                    record = record_for_schema(full_record(event), write_schema)
                    buffer = io.BytesIO()
                    fastavro.schemaless_writer(buffer, write_schema, record)
                    binary = buffer.getvalue()
                    fastavro.schemaless_reader(io.BytesIO(binary), write_schema, read_schema)
                    avro_bytes += len(binary)
                except Exception as exc:
                    failures += 1
                    key = event.get("event_key") if isinstance(event, dict) else None
                    print(
                        f"{path.name}:{line_number} {key}: {type(exc).__name__}: {exc}",
                        file=sys.stderr,
                    )
        if args.limit is not None and events >= args.limit:
            break

    result = {
        "hour": hour_dir.as_posix(),
        "files": len(files),
        "events": events,
        "json_bytes": json_bytes,
        "avro_bytes": avro_bytes,
        "failed": failures,
        "write_schema": write_schema.get("name"),
        "read_schema": read_schema.get("name"),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
