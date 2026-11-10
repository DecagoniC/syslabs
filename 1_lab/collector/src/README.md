# Программа сборщика
Точка входа — `main.py`. Режим приходит первым аргументом и в `COLLECTOR_MODE`.
Адреса источника в коде не записаны: их задаёт `.env` стенда. Значения варианта 27 — в `stand.env` рядом с заданием.
| Переменная | По умолчанию | Назначение |
|---|---|---|
| `COLLECTOR_USER_AGENT` | нет, обязательна | `User-Agent` с названием программы и адресом почты |
| `COLLECTOR_COUNTRY_URL` | нет, обязательна | шаблон потока одной страны, подстановка `{country}` |
| `COLLECTOR_GLOBAL_URL` | нет, обязательна | весь поток за последние 5 минут, для замера до фильтра |
| `COLLECTOR_ARCHIVE_URL` | нет, обязательна | шаблон файла архива: `{date}`, `{slug}`, `{sensor_id}`, `{suffix}` (`_indoor` или пусто) |
| `COLLECTOR_POLL_SECONDS` | `300` | пауза между опросами; меньше 300 служба не ставит |
| `COLLECTOR_HISTORY_FROM` | нет | начало истории, дата UTC, включительно |
| `COLLECTOR_HISTORY_TO` | нет | конец истории, дата UTC; если позже `FROM` — не включается |
| `COLLECTOR_RAW_LIMIT_GB` | `20` | предел суммы `stream` и `history`, гигабайты по 1024³ |
| `COLLECTOR_DEDUP_SECONDS` | `21600` | сколько помнить ключи потока, чтобы отбросить перекрытие окон |
| `COLLECTOR_SAMPLE_SECONDS` | `21600` | как часто повторять замер всего потока |
| `COLLECTOR_HTTP_TIMEOUT` | `120` | таймаут одного запроса, секунды |
| `COLLECTOR_RETRY_ATTEMPTS` | `5` | попытки при 429, 5xx и обрыве сети |
| `COLLECTOR_METRICS_PORT` | `9101` | порт экспортера `prometheus_client`, путь `/metrics` |
| `COLLECTOR_ONCE` | не задана | один опрос и выход; только для проверки, в службе не ставится |
Оболочка стенда сама задаёт `COLLECTOR_MODE`, `COLLECTOR_RAW_DIR`, `COLLECTOR_OUT_DIR` и `COLLECTOR_METRICS_PORT`.
