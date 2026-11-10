-- ЛР2, версия 1 схемы: весь сырой слой (поток и исторический срез) в Parquet.
-- Запуск на стенде:
--   mkdir -p data/duckdb/parquet
--   docker compose run --rm -T duckdb < jobs/to_parquet.sql
--
-- Пути внутри контейнера: сырьё смонтировано в /data/raw только на чтение,
-- рабочий каталог DuckDB — /data/duckdb.
-- Версия 1 не хранит measurement_id: поле появится в версии 2 схемы.

SET TimeZone = 'UTC';
SET memory_limit = '2GB';
SET threads = 2;
SET preserve_insertion_order = false;

COPY (
    SELECT
        event_key,
        event_time,
        received_at,
        source,
        sensor_id,
        sensor_type,
        pin,
        location_id,
        latitude,
        longitude,
        altitude,
        country,
        indoor,
        values,
        CAST(event_time AS DATE) AS event_date
    FROM read_json_auto(
        ['/data/raw/stream/**/*.jsonl.gz', '/data/raw/history/**/*.jsonl.gz'],
        columns = {
            event_key: 'VARCHAR',
            event_time: 'TIMESTAMP',
            received_at: 'TIMESTAMP',
            source: 'VARCHAR',
            sensor_id: 'BIGINT',
            sensor_type: 'VARCHAR',
            pin: 'VARCHAR',
            location_id: 'BIGINT',
            latitude: 'VARCHAR',
            longitude: 'VARCHAR',
            altitude: 'VARCHAR',
            country: 'VARCHAR',
            indoor: 'INTEGER',
            values: 'MAP(VARCHAR, VARCHAR)'
        },
        hive_partitioning = false
    )
) TO '/data/duckdb/parquet/events'
(FORMAT PARQUET, PARTITION_BY (event_date), OVERWRITE_OR_IGNORE true);
