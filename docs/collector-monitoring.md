# Мониторинг сборщика на стенде

Команды выполняются в Ubuntu WSL на машине стенда из каталога стенда:

```bash
cd ~/stand
```

Сборщик — контейнер `collector`, метрики — `http://localhost:9101/metrics`.
Все команды ниже только читают состояние, кроме отмеченных «меняет».

## Версии инструментов

```bash
docker --version
docker compose version
git --version
python3 --version
curl --version | head -1
```

Проверяют, что на стенде есть Docker Compose, git и curl. Ничего не меняют.

## Состояние контейнера

```bash
docker compose ps
docker ps --filter name=collector
docker inspect collector --format 'status={{.State.Status}} restarts={{.RestartCount}} started={{.State.StartedAt}}'
```

Показывают, запущен ли `collector`, сколько раз он перезапускался и когда стартовал.
`RestartCount` растёт, если контейнер падает и политика `unless-stopped` поднимает его снова.

## Журнал

```bash
docker compose logs -f --tail=100 collector
```

Следит за журналом в реальном времени; `Ctrl+C` отключает только просмотр, контейнер продолжает работать.

```bash
docker compose logs --since 30m collector
```

Показывает журнал за последние 30 минут без слежения.

```bash
docker compose logs --since 30m collector | grep -F '"level":"error"' | tail -20
```

Оставляет только строки ошибок за последние 30 минут.

## Метрики

```bash
curl -s http://localhost:9101/metrics | grep '^collector_'
```

Печатает все метрики сборщика. Основные:

- `collector_up` — 1, пока процесс жив;
- `collector_metrics_server_up` — 1, если порт `/metrics` отвечает;
- `collector_events_total{mode="record"}` — сколько событий записано;
- `collector_errors_total{kind="http|network|parse"}` — ошибки по видам;
- `collector_last_success_timestamp_seconds` — unix-время последнего успешного ответа;
- `collector_last_poll_timestamp_seconds` — unix-время последнего завершённого опроса;
- `collector_requests_total{status="..."}` — попытки HTTP по статусам;
- `collector_request_duration_seconds` — гистограмма длительности запросов;
- `collector_records_processed_total` — просмотренные записи источника;
- `collector_backfill_completed_total` — удачные пары «датчик/сутки» в истории.

Возраст последнего успеха:

```bash
now=$(date +%s)
last=$(curl -s http://localhost:9101/metrics | awk '/^collector_last_success_timestamp_seconds /{print int($2)}')
echo "last success age: $((now - ${last:-now})) s"
```

Считает, сколько секунд прошло с последнего успешного ответа. Если метрики ещё не было, возраст 0.

Проверка, что экспортер отвечает:

```bash
curl -fsS http://localhost:9101/metrics >/dev/null && echo "metrics OK"
```

## Данные и состояние

```bash
du -sh data/raw
df -h .
```

Показывают размер слоя и свободное место на диске стенда.

```bash
find data/raw/stream -name '*.jsonl.gz' | wc -l
find data/raw/history -name '*.jsonl.gz' | wc -l
```

Считают закрытые файлы потока и истории.

```bash
find data/raw/stream -name '*.jsonl.gz' -printf '%TY-%Tm-%Td %TH:%TM %p\n' | sort | tail -5
```

Показывает пять последних записанных файлов потока по времени.

```bash
cat data/raw/state/record.json
```

Показывает `last_poll_at` и `sampled_at` — позицию потока и время последнего замера.

## Быстрый цикл наблюдения

```bash
while true; do
  clear
  date -u
  docker compose ps
  curl -s http://localhost:9101/metrics | grep -E '^collector_(up|metrics_server_up|events_total|errors_total|last_success_timestamp_seconds|last_poll_timestamp_seconds)'
  sleep 10
done
```

Обновляет экран раз в 10 секунд. `Ctrl+C` останавливает только цикл, не сборщик.

## Управление (меняет состояние)

```bash
docker compose --profile lab1 up -d collector
```

Запускает сборщик, если он остановлен. Данные и состояние читаются с диска.

```bash
docker compose restart collector
```

Перезапускает контейнер. Позиция опроса сохраняется в `state/record.json`.

```bash
docker compose stop collector
```

Останавливает сборщик. Данные не удаляются.

## С этой машины по SSH

Если удобнее смотреть с рабочей машины:

```bash
ssh -i ~/.ssh/syslabs_transfer decag@192.168.31.161 \
  "wsl -d Ubuntu -- bash -lc 'cd /home/decag/stand && docker compose ps'"
```

То же для метрик:

```bash
ssh -i ~/.ssh/syslabs_transfer decag@192.168.31.161 \
  "wsl -d Ubuntu -- curl -s http://localhost:9101/metrics" | grep '^collector_'
```
