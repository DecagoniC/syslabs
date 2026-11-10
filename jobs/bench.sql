-- ЛР2: замер одного и того же запроса на сырых файлах и на Parquet.
-- Сутки события — 2026-10-23. Сырьё партиционировано по received_at, поэтому
-- сырой запрос сканирует весь слой и фильтрует по event_time; Parquet читает
-- одну партицию event_date=2026-10-23.
-- Запуск: docker compose run --rm -T duckdb < jobs/bench.sql
-- Время печатает .timer on; в NOTES.md идёт медиана трёх запусков.

.timer on

SELECT date_trunc('hour', event_time) AS hour, country, count(*) AS events
FROM read_json_auto(
    ['/data/raw/stream/**/*.jsonl.gz', '/data/raw/history/**/*.jsonl.gz'],
    columns = { event_time: 'TIMESTAMP', country: 'VARCHAR' },
    hive_partitioning = false
)
WHERE CAST(event_time AS DATE) = DATE '2026-10-23'
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT date_trunc('hour', event_time) AS hour, country, count(*) AS events
FROM read_json_auto(
    ['/data/raw/stream/**/*.jsonl.gz', '/data/raw/history/**/*.jsonl.gz'],
    columns = { event_time: 'TIMESTAMP', country: 'VARCHAR' },
    hive_partitioning = false
)
WHERE CAST(event_time AS DATE) = DATE '2026-10-23'
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT date_trunc('hour', event_time) AS hour, country, count(*) AS events
FROM read_json_auto(
    ['/data/raw/stream/**/*.jsonl.gz', '/data/raw/history/**/*.jsonl.gz'],
    columns = { event_time: 'TIMESTAMP', country: 'VARCHAR' },
    hive_partitioning = false
)
WHERE CAST(event_time AS DATE) = DATE '2026-10-23'
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT date_trunc('hour', event_time) AS hour, country, count(*) AS events
FROM read_parquet('/data/duckdb/parquet/events/event_date=2026-10-23/*.parquet')
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT date_trunc('hour', event_time) AS hour, country, count(*) AS events
FROM read_parquet('/data/duckdb/parquet/events/event_date=2026-10-23/*.parquet')
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT date_trunc('hour', event_time) AS hour, country, count(*) AS events
FROM read_parquet('/data/duckdb/parquet/events/event_date=2026-10-23/*.parquet')
GROUP BY 1, 2
ORDER BY 1, 2;
