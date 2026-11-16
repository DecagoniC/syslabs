## ЛР2

1. **Avro-схема v1/v2 в `schemas/` и в истории.**
   `cat schemas/event.avsc` — показывает поля и типы, `measurement_id` с `default: null`.
   `git -C ~/syslabs log --oneline | grep -i avro` — только коммиты со схемой Avro: `e49dda0` (v1) и `12038ef` (v2).

2. **Parquet с типами и партициями; повтор не создаёт копий.**
   `cat jobs/to_parquet.sql` — явные типы, `PARTITION_BY (event_date)`, `OVERWRITE_OR_IGNORE`.
   `find data/duckdb/parquet/events -name '*.parquet' | wc -l` — 26 файлов.
   `docker compose run --rm -T duckdb -c "SELECT count(*) FROM read_parquet('/data/duckdb/parquet/events/**/*.parquet', hive_partitioning=true);"` — 12 975 323 строки.
   `docker compose run --rm -T duckdb < jobs/to_parquet.sql` — повтор; после него снова 26 и 12 975 323.

3. **Замеры объёма и времени: сырьё против Parquet.**
   `du -sh data/raw/stream/dt=2026-10-23 data/duckdb/parquet/events/event_date=2026-10-23` — объём сырья и партиции.
   `docker compose run --rm -T duckdb < jobs/bench.sql` — время запроса: около 3 с по сырью против около 0,01 с по Parquet.

4. **v1 читается v2; вид совместимости назван.**
   `.venv/bin/python schemas/check.py --raw-dir data/raw --write-schema data/duckdb/event_v1.avsc --read-schema schemas/event.avsc` — `"failed": 0`; совместимость полная (backward + forward), обратное направление тоже 0.

5. **Каталог данных дополнен строкой `parquet.events`.**
   `grep -A 8 '^## parquet.events' CATALOG.md` — путь, формат, партиции, источник, потребители.

6. **Дополнительное задание: кодеки и row groups.**
   `cat data/duckdb/bench/results.md` — сравнение кодеков и размеров групп, выбран ZSTD + 500 000.

## ЛР3

1. **Garage работает, в bucket `lake` есть слои сырья и очистки.**
   `docker compose ps -a` — `collector`/`s3` Up, `s3-init` Exited (0).
   `docker compose exec s3 /garage bucket info lake` — bucket `lake`, число объектов и размер.

2. **Соглашение ключей и партиций в `CATALOG.md`; листинг ему соответствует.**
   `sed -n '1,90p' CATALOG.md` — слои, шаблон ключа, имена партиций, сроки и объяснение через свойства S3.
   `docker compose run --rm -T duckdb -init init.sql -c "SELECT regexp_extract(file,'^s3://lake/([^/]+/[^/]+)/',1) AS layer, count(*) AS files FROM glob('s3://lake/**') GROUP BY 1 ORDER BY 1;"` — `clean/events` 25, `raw/history` 1893, `raw/stream` около 9,6 тыс. — ровно по шаблону.

3. **Очистка отделена от сбора: `clean.sql` пишет только D, правила записаны, повтор без копий.**
   `cat jobs/clean.sql` — чтение D и D+1, дедупликация, отбрасывание мусора и выбросов, запись только партиции D.
   `docker compose run --rm -T duckdb -init init.sql -cmd "SET VARIABLE day = DATE '2026-10-23';" < jobs/clean.sql` — очищает 2026-10-23.
   `docker compose run --rm -T duckdb -init init.sql -c "SELECT count(*) FROM read_parquet('s3://lake/clean/events/event_date=2026-10-23/*.parquet');"` — 958 604 строки; повтор даёт тот же один файл.

4. **Запрос к Parquet из хранилища; предикат сокращает число файлов.**
   `docker compose run --rm -T duckdb -init init.sql -c "EXPLAIN ANALYZE SELECT date_trunc('hour',event_time) AS hour, country, count(*) FROM read_parquet('s3://lake/clean/events/**/*.parquet', hive_partitioning=true) WHERE event_date=DATE '2026-10-23' GROUP BY 1,2;"` — `Total Files Read: 1`; убрать `WHERE` — будет 25.

5. **Сроки хранения заданы и обоснованы; `retention.py` удаляет устаревшее; каталог дополнен.**
   `docker compose run --rm -T --entrypoint python -v "$PWD/jobs:/jobs:ro" collector /jobs/retention.py --layer clean --days 2 --dry-run` — показывает старые партиции, `deleted=0`.
   `grep -A 8 -E '^### Сроки хранения|^## clean.events' CATALOG.md` — сроки `raw`/`clean` и строка `clean.events`.

6. **Точка восстановления после ЛР1 и копия сырья вне виртуальной машины.**
   `ls -lh /mnt/d/bigdata` — два датированных архива: экспорт WSL2 и копия `raw`, дата 2026-11-16, свежее недели.

7. **Дополнительное задание: объёмы, прогноз на семестр, рост ×1000, уровни хранения.**
   `cat docs/lab3-storage.md` — текущие объёмы, прогноз, оценка роста и предложение по уровням/стоимости.
