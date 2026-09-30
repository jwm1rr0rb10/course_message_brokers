# Запускаемые примеры к курсу по RabbitMQ

Всё здесь прогоняет CI ([`.github/workflows/examples.yml`](../../.github/workflows/examples.yml)) при каждом push и раз в неделю по расписанию: `go vet` и unit-тесты, затем на кластере из `cluster/` — смок-тест и интеграционные тесты на Go и Python. Кластер поднимается дважды: на образе, под который написан курс (`rabbitmq:4.3-management`), и на самом свежем (`RABBITMQ_IMAGE=rabbitmq:management`). Если новый релиз изменит поведение, описанное в курсе, сборка покраснеет.

```
examples/
├── cluster/            кластер из трёх узлов из модуля 2.3 (docker compose)
│   └── stream/         адрес stream-протокола для каждого узла (10.4)
├── scripts/
│   └── smoke-test.sh   прогоняет команды из курса и проверяет результат
├── go/                 amqp091-go
│   ├── cmd/publisher   publisher confirms + mandatory, повторная отправка неподтверждённых (5.6)
│   ├── cmd/consumer    manual ack, prefetch, reject -> DLX, переподключение (6.6, 8)
│   └── coursetest      интеграционные тесты утверждений курса
└── python/             pika: producer (5.5), consumer (6.5), check.py
```

## 1. Запусти кластер

```bash
cd examples/cluster
docker compose up -d
docker exec rabbit-1 rabbitmqctl cluster_status
```

AMQP на `localhost:5672/5673/5674`, management UI на `localhost:15672/15673/15674` (admin / admin), Prometheus на `localhost:15692`, stream-протокол на `localhost:5552/5553/5554` (узлы сообщают клиентам адрес `localhost` и свой порт, см. `cluster/stream/`).

Другой образ: `RABBITMQ_IMAGE=rabbitmq:management docker compose up -d`.

## 2. Смок-тест

Нужны Docker, `curl` и `python3`; работает и со старым bash 3.2 из macOS. При первой ошибке скрипт завершается с ненулевым кодом:

```bash
./examples/scripts/smoke-test.sh
```

Тест создаёт quorum-очередь из модуля 2.4 и проверяет три реплики, публикует через default exchange, показывает `routed:false` для сообщения без маршрута, строит topic-топологию из модуля 3.4, проверяет alternate exchange через политику, тип очереди по умолчанию для vhost, останавливает лидера quorum-очереди и убеждается, что сообщения доступны, проверяет `drain`/`revive`, метрики Prometheus и экспорт definitions.

## 3. Go

```bash
cd examples/go
go run ./cmd/consumer &                   # объявит топологию и начнёт читать payments
go run ./cmd/publisher -n 5               # 5 заказов с confirms
go run ./cmd/publisher -n 1 -poison       # битое сообщение: consumer отправит его в DLX
go run ./cmd/publisher -n 1 -key ordr.created   # опечатка: вернётся как NO_ROUTE

go vet ./... && go test ./...             # без брокера: unit-тесты
go test -tags integration -count=1 ./...  # интеграционные тесты: нужен запущенный брокер
```

Без брокера интеграционные тесты пропускаются (`SKIP` с причиной). В CI задано `COURSE_REQUIRE_BROKER=1`: там недоступный брокер — это ошибка, а не пропуск.

Consumer переживает перезапуск узла: при обрыве соединения или отмене подписки брокером он переподключается с экспоненциальной задержкой, заново объявляет топологию и подписывается. По Ctrl+C / SIGTERM отменяет подписку, дорабатывает и подтверждает уже полученные сообщения и закрывает канал. Publisher после обрыва заново отправляет всё, на что не пришёл `ack` (с тем же `message_id`), и завершается с ошибкой, если за `-attempts` попыток часть сообщений так и не подтверждена.

Тесты проверяют: topic-маршрутизацию, судьбу сообщений без маршрута (с `mandatory` и без), alternate exchange, повторную доставку после закрытия канала, prefetch, reject → DLX с `x-death`, `delivery-limit` (явный и значение 20 по умолчанию в 4.x; в 4.3 его увеличивает `reject(requeue=true)`, но не `nack`), `reject-publish` и `drop-head`, запрет non-durable неэксклюзивных очередей в 4.3, ретрай через TTL + DLX, RPC через direct reply-to, повторное чтение stream. Тесты, зависящие от версии, пропускаются на старых брокерах.

## 4. Python

```bash
cd examples/python
pip install -r requirements.txt
IDLE_SECONDS=5 python consumer.py &       # объявит топологию
python producer.py
python check.py
```

## Переменные окружения

| Переменная | По умолчанию | Где |
|---|---|---|
| `RABBITMQ_URL` | `amqp://admin:admin@localhost:5672/` | Go, Python |
| `IDLE_SECONDS` | `0` (работать бесконечно) | `consumer.py` |
| `COURSE_REQUIRE_BROKER` | не задана (пропускать тесты без брокера) | Go-тесты: `1` — падать, если брокер недоступен |
| `RABBITMQ_IMAGE` | `rabbitmq:4.3-management` | `docker compose` |
| `RABBITMQ_HOST` | `localhost` | `smoke-test.sh` |
