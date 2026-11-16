#!/usr/bin/env python3
"""ЛР3: удаление из озера партиций старше срока хранения.

Пример запуска из ~/stand:
  docker compose run --rm -T --entrypoint python \
      -v "$PWD/jobs:/jobs:ro" collector /jobs/retention.py \
      --layer clean --days 2 --dry-run
"""

import argparse
import os
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.config import Config

# слой: (префикс, метка партиции, срок по умолчанию)
LAYERS = {
    "raw": ("raw/", "/dt=", 56),
    "clean": ("clean/", "/event_date=", 90),
}


def env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit(f"не задана переменная {name}")
    return value


def partition_date(key: str, marker: str) -> date | None:
    """Достаёт дату из ключа .../dt=2026-11-01/... или .../event_date=..."""
    if marker not in key:
        return None
    text = key.split(marker, 1)[1][:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Удаление старых партиций озера")
    parser.add_argument("--layer", choices=LAYERS, required=True)
    parser.add_argument("--days", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    prefix, marker, default_days = LAYERS[args.layer]
    days = args.days if args.days is not None else default_days
    if days <= 0:
        parser.error("--days должен быть больше нуля")
    bucket = env("S3_BUCKET")

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days)
    s3 = boto3.client(
        "s3",
        endpoint_url=env("S3_ENDPOINT"),
        region_name=env("S3_REGION"),
        aws_access_key_id=env("S3_ACCESS_KEY"),
        aws_secret_access_key=env("S3_SECRET_KEY"),
        config=Config(
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )

    old: list[tuple[str, int]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        for obj in page.get("Contents", []):
            part = partition_date(obj["Key"], marker)
            if part is not None and part < cutoff:
                old.append((obj["Key"], obj["Size"]))

    print(f"layer={args.layer} days={days} cutoff={cutoff} "
          f"dry_run={args.dry_run}")
    action = "would delete" if args.dry_run else "delete"
    for key, size in sorted(old):
        print(f"{action} {key} ({size} bytes)")

    deleted = 0
    if not args.dry_run:
        for i in range(0, len(old), 1000):
            batch = [{"Key": key} for key, _ in old[i:i + 1000]]
            response = s3.delete_objects(Bucket=bucket,
                                         Delete={"Objects": batch})
            errors = response.get("Errors", [])
            for error in errors:
                print(f"ошибка: {error['Key']}: {error['Message']}")
            deleted += len(batch) - len(errors)

    print(f"objects={len(old)} bytes={sum(size for _, size in old)} "
          f"deleted={deleted}")


if __name__ == "__main__":
    main()
