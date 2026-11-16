import os
import shutil
import time

import duckdb

INPUT_DIR = os.environ.get("INPUT_DIR", "data/duckdb/parquet/events")
OUT_DIR = os.environ.get("OUT_DIR", "data/duckdb/bench")
FROM_DATE = os.environ.get("FROM_DATE", "2026-10-23")
TO_DATE = os.environ.get("TO_DATE", "2026-10-25")

CODECS = ["UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD"]
ROW_GROUPS = [100000, 500000, 1000000]

if not os.path.isdir(INPUT_DIR):
    print("Нет каталога", INPUT_DIR, "- сначала выполните jobs/to_parquet.sql")
    raise SystemExit(1)

os.makedirs(OUT_DIR, exist_ok=True)

con = duckdb.connect()
con.execute("SET memory_limit = '2GB'")
con.execute("SET threads = 2")
con.execute("SET TimeZone = 'UTC'")

input_glob = os.path.join(INPUT_DIR, "**", "*.parquet").replace("\\", "/")
select_sql = f"""
    SELECT event_key, event_time, received_at, source, sensor_id, sensor_type, pin,
           location_id, latitude, longitude, altitude, country, indoor, values,
           CAST(event_time AS DATE) AS event_date
    FROM read_parquet('{input_glob}', union_by_name = true, hive_partitioning = true)
    WHERE event_date BETWEEN DATE '{FROM_DATE}' AND DATE '{TO_DATE}'
"""

results = []

for codec in CODECS:
    for rg in ROW_GROUPS:
        name = codec.lower() + "_" + str(rg)
        folder = os.path.join(OUT_DIR, name)
        if os.path.isdir(folder):
            shutil.rmtree(folder)
        os.makedirs(folder)

        file_path = os.path.join(folder, "data.parquet").replace("\\", "/")

        copy_sql = f"""
            COPY ({select_sql}) TO '{file_path}'
            (FORMAT PARQUET, COMPRESSION {codec}, ROW_GROUP_SIZE {rg})
        """
        start = time.time()
        con.execute(copy_sql)
        write_s = time.time() - start

        size = os.path.getsize(file_path)
        meta = con.sql(
            f"SELECT num_rows, num_row_groups "
            f"FROM parquet_file_metadata('{file_path}')"
        ).fetchall()[0]
        rows = meta[0]
        groups = meta[1]

        query_sql = f"""
            SELECT date_trunc('hour', event_time) AS hour, country, count(*) AS events
            FROM read_parquet('{file_path}')
            GROUP BY 1, 2 ORDER BY 1, 2
        """
        con.sql(query_sql).fetchall()

        times = []
        for _ in range(3):
            start = time.time()
            con.sql(query_sql).fetchall()
            times.append(time.time() - start)
        times.sort()
        median_ms = times[1] * 1000

        size_mib = size / 1024 / 1024
        print(
            codec,
            "rg=" + str(rg),
            "rows=" + str(rows),
            "groups=" + str(groups),
            "size=" + str(round(size_mib, 1)) + " MiB",
            "write=" + str(round(write_s, 2)) + "s",
            "query=" + str(round(median_ms, 1)) + " ms",
        )

        results.append((codec, rg, rows, groups, size_mib, write_s, median_ms))

lines = []
lines.append("# ЛР2, дополнительное задание: кодеки и row groups")
lines.append("")
lines.append("Период: " + FROM_DATE + " - " + TO_DATE + ".")
lines.append("")
lines.append("| Кодек | ROW_GROUP_SIZE | Строк | Row groups | Размер, MiB | Запись, с | Запрос, медиана, мс |")
lines.append("|---|---:|---:|---:|---:|---:|---:|")
for item in results:
    lines.append(
        "| " + str(item[0]) + " | " + str(item[1]) + " | " + str(item[2]) + " | "
        + str(item[3]) + " | " + str(round(item[4], 1)) + " | "
        + str(round(item[5], 2)) + " | " + str(round(item[6], 1)) + " |"
    )

results_path = os.path.join(OUT_DIR, "results.md")
with open(results_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")

print()
print("\n".join(lines))
print()
print("Результаты:", results_path)

best = os.path.join(OUT_DIR, "zstd_500000", "data.parquet").replace("\\", "/")
if os.path.isfile(best):
    print()
    print("Row groups zstd_500000:")
    detail = con.sql(
        f"""
        SELECT row_group_id,
               max(row_group_num_rows) AS rows,
               row_group_compressed_bytes AS compressed_bytes,
               row_group_bytes AS uncompressed_bytes
        FROM parquet_metadata('{best}')
        GROUP BY row_group_id, row_group_compressed_bytes, row_group_bytes
        ORDER BY row_group_id
        """
    ).fetchall()
    for row in detail:
        print(
            "  rg=" + str(row[0]),
            "rows=" + str(row[1]),
            "compressed=" + str(row[2]),
            "uncompressed=" + str(row[3]),
        )
