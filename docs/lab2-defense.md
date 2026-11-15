# ЛР2: шпаргалка для защиты

Все команды — на стенде из `~/stand`.

## Перед защитой

```bash
cd ~/stand
docker compose ps
docker compose logs --tail=10 collector
git -C ~/syslabs log --oneline -5
curl -s localhost:9101/metrics | grep -E '^collector_(up|events_total|last_poll)'
```

Проверить, что файлы ЛР2 на месте:

```bash
ls schemas/event.avsc schemas/check.py \
   jobs/to_parquet.sql jobs/to_parquet_v2.sql jobs/bench.sql jobs/parquet_bench.py
```

## 1. Avro-схема, версии 1 и 2

Показать:

```bash
cat schemas/event.avsc
git -C ~/syslabs show e49dda0:schemas/event.avsc | head -40
git -C ~/syslabs show 12038ef:schemas/event.avsc | head -40
```

Сказать:

> «Событие описано Avro-схемой. Обязательные `event_key` и `event_time`
> (`long`, `timestamp-millis`), поля паспорта, `values` — `map<string>`.
> Версия 1 — коммит `e49dda0`, версия 2 — `12038ef`: добавлен
> `measurement_id` с `default: null`».

Вопросы:

- Почему Avro? — вариант требует Avro; он даёт бинарное кодирование, схему и эволюцию.
- Почему `event_time` — `long`? — логический тип `timestamp-millis`: компактно,
  однозначно, UTC, миллисекунды.
- Почему `values` — map? — у разных датчиков разные величины.
- Что такое `default`? — значение, которое читатель подставляет, если поля нет
  у писателя.

## 2. Parquet с типами и партициями; повтор без копий

Показать:

```bash
cat jobs/to_parquet.sql
find data/duckdb/parquet/events -name '*.parquet' | wc -l
du -sh data/duckdb/parquet/events
docker compose run --rm -T duckdb <<'SQL'
SELECT count(*) FROM read_parquet('/data/duckdb/parquet/events/**/*.parquet', hive_partitioning = true);
SQL
```

Ожидаемо: 26 файлов, 12 975 323 строки, около 411 МБ.

Повторный запуск:

```bash
docker compose run --rm -T duckdb < jobs/to_parquet.sql
```

Снова 26 файлов и 12 975 323 строки. Байты могут отличаться на сжатие — копий нет.

Сказать:

> «Запрос читает поток и историю, задаёт типы по схеме, вычисляет `event_date`
> из `event_time` и пишет Parquet с `PARTITION_BY (event_date)`.
> `OVERWRITE_OR_IGNORE true` перезаписывает файлы партиции, поэтому повторный
> запуск не добавляет копии».

Вопросы:

- Почему `event_date`, а не `dt`? — `dt` — дата получения, аналитика фильтрует
  по времени события; плюс `dt` подмешивается из пути и дал бы `dt_1`.
- Почему типы явные? — JSON не гарантирует типы; `columns` фиксирует
  `TIMESTAMP`, `MAP`, `INTEGER` по схеме.
- Почему нет копий? — `OVERWRITE_OR_IGNORE`.

## 3. Замеры: сырьё vs Parquet

```bash
docker compose run --rm -T duckdb < jobs/bench.sql
```

Ожидаемые медианы:

- сырьё: **3,321 с**;
- Parquet: **0,011 с**;
- результат одинаковый — 72 строки (24 часа × 3 страны).

Объёмы за 2026-10-23: сырьё 40 230 902 байта, Parquet-партиция 32 419 585 байт.

Сказать:

> «Сырьё построчное и партиционировано по `received_at`, поэтому фильтр по
> `event_time` сканирует весь слой. Parquet колоночный и партиционирован по
> `event_date`: читаются нужные колонки и одна партиция. Отсюда 3,3 с против
> 0,011 с».

Вопросы:

- Почему сырьё медленнее? — gzip + построчный JSON, нет pruning по колонкам/партициям.
- Почему Parquet быстрее? — column pruning, partition pruning, сжатие по колонкам.
- Почему объём уменьшился не сильно? — основной выигрыш в скорости; Parquet ещё и колоночный.

## 4. Совместимость v1 и v2

```bash
.venv/bin/python schemas/check.py \
  --raw-dir data/raw \
  --write-schema data/duckdb/event_v1.avsc \
  --read-schema schemas/event.avsc

.venv/bin/python schemas/check.py \
  --raw-dir data/raw \
  --write-schema schemas/event.avsc \
  --read-schema data/duckdb/event_v1.avsc
```

Ожидаемо: `"failed": 0` в обоих направлениях.

Партиции:

```bash
docker compose run --rm -T duckdb <<'SQL'
SELECT event_date, count(*) AS rows, count(measurement_id) AS with_id
FROM read_parquet('/data/duckdb/parquet/events/**/*.parquet',
                  union_by_name = true, hive_partitioning = true)
WHERE event_date IN (DATE '2026-10-21', DATE '2026-11-10')
GROUP BY event_date ORDER BY event_date;
SQL
```

Ожидаемо:

- `2026-10-21` — 1 026 436 строк, `with_id = 0`;
- `2026-11-10` — 363 788 строк, `with_id = 363 788`.

Сказать:

> «v2 читает v1 за счёт `default: null`; v1 читает v2, потому что Avro
> игнорирует незнакомое поле писателя. Совместимость полная. В Parquet старые
> партиции не содержат колонки, новая содержит; `union_by_name = true`
> объединяет файлы по именам и даёт `null` для старых строк. Запрос v2 читает
> сырьё D и D+1, чтобы поймать опоздавшие события, и перезаписывает только
> партицию D».

Вопросы:

- Какая совместимость? — полная: backward + forward.
- Что было бы без `default`? — новый читатель не смог бы прочитать старые данные.
- Почему старые партиции без поля? — они записаны по v1; Parquet хранит схему
  каждого файла.
- Почему D+1? — опоздавшие события суток D приходят в сырьё D+1; без этого
  терялись 2 994 события.

## 5. Каталог данных

```bash
grep -A 8 '^## parquet.events' CATALOG.md
```

Сказать:

> «В каталог добавлен набор `parquet.events`: слой и путь, формат Parquet,
> партиции по `event_date`, источник — `raw.stream` и `raw.history`,
> потребитель — ЛР3 (очищенный слой), свежесть и рост».

Вопросы:

- Зачем каталог? — чтобы потребители знали путь, формат, партиции и связи.
- Кто потребитель? — ЛР3, очищенный слой; дальше lakehouse.

## 6. Доп. задание: кодеки и row groups

```bash
sed -n '1,80p' jobs/parquet_bench.py
cat data/duckdb/bench/results.md
```

Сказать:

> «Сравнены 4 кодека и 3 размера row group на 2 827 716 строках. Выбран
> `ZSTD` + `ROW_GROUP_SIZE = 500000`: 57,5 MiB против 57,7 у gzip и 91,2 у
> snappy; запись 2,22 с против 6,14 у gzip; запрос 43,0 мс;
> `parquet_metadata()` показывает 6 row groups по ~10,6 МБ. Это баланс для
> clean-слоя ЛР3».

Вопросы:

- Что такое кодек? — алгоритм сжатия/распаковки; в Parquet применяется к
  column chunk.
- Что такое row group? — горизонтальная порция строк; внутри — column chunks.
- Почему ZSTD? — почти как gzip по размеру, но запись в 3 раза быстрее.
- Почему 500k? — минимальный размер, 6 групп; 100k — больше метаданных,
  1M — медленнее.
- Как проверяли? — `parquet_metadata()`, `parquet_file_metadata()`, медиана
  трёх запросов.

## Короткая речь на открытие

> «В ЛР2 я описал событие Avro-схемой, проверил кодирование и декодирование,
> переложил сырьё в Parquet с партициями по дате события, замерил выигрыш и
> показал эволюцию схемы v1→v2. Дополнительно сравнил кодеки и размеры row
> group и выбрал ZSTD + 500000 для clean-слоя ЛР3».

## Если что-то сломалось

- Нет `duckdb` в `.venv`:

```bash
.venv/bin/pip install duckdb==1.5.5
```

- Docker не тянет образ из-за credsStore:

```bash
DOCKER_CONFIG=/tmp/docker-nocreds docker compose run --rm -T duckdb < jobs/to_parquet.sql
```

- Нет `data/duckdb/parquet/events` — сначала:

```bash
mkdir -p data/duckdb/parquet
docker compose run --rm -T duckdb < jobs/to_parquet.sql
```
