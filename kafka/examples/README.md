# Запускаемые примеры к курсу по Kafka

Всё здесь прогоняет CI ([`.github/workflows/examples.yml`](../../.github/workflows/examples.yml)) на каждый push и раз в неделю по расписанию: `go vet`, unit-тесты, смок-тест и интеграционные тесты на кластере из `cluster/docker-compose.yml` — на версии, под которую написан курс (`apache/kafka:4.3.1`), и на самом свежем образе (`KAFKA_IMAGE=apache/kafka:latest`). Если новый релиз изменит поведение, описанное в курсе, сборка покраснеет.

```
examples/
├── cluster/            кластер из трёх узлов KRaft из модуля 2.2 (docker compose)
├── scripts/
│   └── smoke-test.sh   прогоняет команды CLI из курса и проверяет результат
├── go/                 franz-go
│   ├── cmd/producer    идемпотентный producer с ключами (4.7)
│   ├── cmd/consumer    ручной коммит, BlockRebalanceOnPoll, DLQ до коммита (5.5, 13.4)
│   ├── cmd/kprobe      одна запись с заданным acks, для смок-теста (8.2)
│   ├── internal/dlq    сборка DLQ-записи с заголовками + unit-тесты
│   ├── internal/testutil  подключение тестов: кластер, kfake или пропуск
│   └── coursetest      интеграционные тесты утверждений курса
└── python/             confluent-kafka: producer (4.8), consumer с DLQ (5.6, 13.4), check.py
```

## 1. Запусти кластер

```bash
cd examples/cluster
docker compose up -d
# другая версия Kafka: KAFKA_IMAGE=apache/kafka:latest docker compose up -d
```

Узлы доступны с хоста на `localhost:9092`, `localhost:9192`, `localhost:9292`, изнутри Docker-сети — на `kafka-N:19092`.

## 2. Смок-тест

Нужны Docker и собранный `kprobe`:

```bash
cd examples/go && go build -o /tmp/kprobe ./cmd/kprobe && cd ../..
KPROBE=/tmp/kprobe ./examples/scripts/smoke-test.sh
```

Тест создаёт топик из модуля 2.3, проверяет, что сообщения одного ключа лежат в одной партиции и по порядку, смотрит lag и сброс offsets группы, показывает `min.insync.replicas` в действии (останавливает брокер: `acks=all` отклоняется, `acks=1` проходит), останавливает лидера партиции и убеждается, что данные доступны. Скрипт неинтерактивный, при первой ошибке выходит с ненулевым кодом, работает и с bash 3.2 из macOS, а остановленные брокеры при выходе запускает обратно.

## 3. Go

Нужен Go 1.26+ (его требует актуальный franz-go).

```bash
cd examples/go
# топики для демо (автосоздание в кластере выключено)
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 \
  --create --topic shop.orders.events --partitions 3 --replication-factor 3
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 \
  --create --topic shop.orders.events.dlq --partitions 3 --replication-factor 3

go run ./cmd/producer -n 5 -poison     # 5 заказов и одно битое сообщение
go run ./cmd/consumer                  # обрабатывает, битое уходит в DLQ, потом коммит

go test ./...                                  # unit-тесты, сервер не нужен
go test -tags integration -count=1 ./...       # на запущенном кластере; без него тесты пропускаются
KAFKA_FAKE=1 go test -tags integration -count=1 ./...   # без Docker: kfake в памяти процесса
```

С `KAFKA_FAKE=1` тесты идут против [kfake](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kfake) — поддельной Kafka из franz-go, которая поднимается прямо в процессе теста. Она проверяет клиентские и протокольные утверждения (ключ → партиция, коммиты группы, `read_committed`, DLQ-заголовки); тесты поведения брокера, которое kfake не моделирует (отказ compacted-топика принять запись без ключа), в этом режиме пропускаются. Полную проверку даёт только настоящий кластер.

Если кластер недоступен, интеграционные тесты пропускаются с объяснением; с `COURSE_REQUIRE_BROKER=1` (так в CI) они вместо этого падают.

Consumer пишет невалидные сообщения в DLQ и коммитит offset только после подтверждённой записи. Если DLQ недоступна, он повторяет запись с растущей паузой, а при остановке выходит без коммита: пачка будет перечитана, а не потеряна.

## 4. Python

```bash
cd examples/python
pip install -r requirements.txt
python producer.py
IDLE_SECONDS=5 python consumer.py   # битое сообщение от `go run ./cmd/producer -poison` уходит в shop.orders.events.dlq
python check.py
```

## Переменные окружения

| Переменная | По умолчанию | Где |
|---|---|---|
| `KAFKA_BROKERS` | три порта localhost | Go, Python |
| `KAFKA_RF` | `3` | тесты Go и Python (`1` для одиночного узла) |
| `KAFKA_FAKE` | пусто | `1`: интеграционные тесты Go идут против kfake в процессе, тесты поведения брокера пропускаются |
| `COURSE_REQUIRE_BROKER` | пусто | `1`: недоступный кластер — ошибка тестов, а не пропуск (CI) |
| `KAFKA_IMAGE` | `apache/kafka:4.3.1` | образ для `docker compose` |
| `KPROBE` | `kprobe` | смок-тест |
