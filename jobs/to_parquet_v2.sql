-- ЛР2, версия 2 схемы: запись только новых суток в Parquet, с measurement_id.
-- Старые партиции остаются без этого поля, а весь набор читается с union_by_name = true.
-- Запуск на стенде (день задан в первой строке, его можно переопределить снаружи):
--   docker compose run --rm -T duckdb < jobs/to_parquet_v2.sql
--
-- Версия 2 добавляет measurement_id как optional-поле с default null.
-- Шаблон пути raw/*/dt=.../** берёт и поток, и исторический срез за сутки.

SET TimeZone = 'UTC';
SET memory_limit = '2GB';
SET threads = 2;
SET preserve_insertion_order = false;
SET VARIABLE day = coalesce(getvariable('day'), DATE '2026-11-10');

COPY (
    SELECT
        event_key,
        event_time,
        received_at,
        source,
        measurement_id,
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
        ['/data/raw/*/dt=' || getvariable('day') || '/**/*.jsonl.gz'],
        columns = {
            event_key: 'VARCHAR',
            event_time: 'TIMESTAMP',
            received_at: 'TIMESTAMP',
            source: 'VARCHAR',
            measurement_id: 'BIGINT',
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
