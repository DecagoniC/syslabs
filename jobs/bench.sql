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
