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
        ['/data/raw/*/dt=' || getvariable('day') || '/**/*.jsonl.gz',
         '/data/raw/*/dt=' || (getvariable('day') + 1) || '/**/*.jsonl.gz'],
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
    WHERE CAST(event_time AS DATE) = getvariable('day')
) TO '/data/duckdb/parquet/events'
(FORMAT PARQUET, PARTITION_BY (event_date), OVERWRITE_OR_IGNORE true);
