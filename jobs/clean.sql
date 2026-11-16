SET VARIABLE day = coalesce(getvariable('day'), DATE '2026-11-14');

SET TimeZone = 'UTC';
SET memory_limit = '2GB';
SET threads = 2;
SET preserve_insertion_order = false;

CREATE OR REPLACE MACRO valid_value(k, v) AS
    CASE
        WHEN k IN ('P0', 'P1', 'P2', 'P4', 'P05', 'P10', 'P25',
                   'N05', 'N1', 'N4', 'N10', 'N25')
            THEN TRY_CAST(trim(v) AS DOUBLE) BETWEEN 0 AND 1000
        WHEN k = 'temperature'
            THEN TRY_CAST(trim(v) AS DOUBLE) BETWEEN -50 AND 60
        WHEN k = 'humidity'
            THEN TRY_CAST(trim(v) AS DOUBLE) BETWEEN 0 AND 100
        WHEN k IN ('pressure', 'pressure_at_sealevel', 'pressure_sealevel')
            THEN TRY_CAST(trim(v) AS DOUBLE) BETWEEN 30000 AND 110000
        ELSE isfinite(TRY_CAST(trim(v) AS DOUBLE))
    END;

COPY (
    WITH base AS (
        SELECT
            event_key,
            CAST(event_time AS TIMESTAMP) AS event_time,
            CAST(received_at AS TIMESTAMP) AS received_at,
            source,
            CAST(measurement_id AS BIGINT) AS measurement_id,
            CAST(sensor_id AS BIGINT) AS sensor_id,
            sensor_type,
            pin,
            CAST(location_id AS BIGINT) AS location_id,
            latitude,
            longitude,
            altitude,
            country,
            CAST(indoor AS INTEGER) AS indoor,
            values
        FROM read_json_auto(
            ['s3://lake/raw/*/dt=' || getvariable('day') || '/**/*.jsonl.gz',
             's3://lake/raw/*/dt=' || (getvariable('day') + 1) || '/**/*.jsonl.gz'],
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
          AND event_key IS NOT NULL AND event_key <> ''
          AND event_time IS NOT NULL AND received_at IS NOT NULL
          AND sensor_id IS NOT NULL AND sensor_id > 0
          AND source = 'sensors'
          AND country IN ('PL', 'CZ', 'AT')
          AND values IS NOT NULL AND cardinality(values) > 0
        QUALIFY row_number() OVER (
            PARTITION BY event_key ORDER BY received_at
        ) = 1
    ),
    cleaned AS (
        SELECT
            * EXCLUDE (values),
            map_from_entries(list_filter(
                map_entries(values),
                e -> valid_value(e.key, e.value)
            )) AS values
        FROM base
    )
    SELECT *, CAST(event_time AS DATE) AS event_date
    FROM cleaned
    WHERE cardinality(values) > 0
) TO 's3://lake/clean/events'
(FORMAT PARQUET,
 PARTITION_BY (event_date),
 OVERWRITE_OR_IGNORE true,
 COMPRESSION zstd,
 ROW_GROUP_SIZE 500000);
