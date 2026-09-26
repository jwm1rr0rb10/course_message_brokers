# Запускаемые примеры к курсу по Kafka

Всё здесь прогоняет CI ([`.github/workflows/examples.yml`](../.github/workflows/examples.yml)) на версии, под которую написан курс (`apache/kafka:4.3.1`), и на самом свежем образе, плюс раз в неделю по расписанию. Если новый релиз изменит поведение, описанное в курсе, сборка покраснеет.

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
│   └── coursetest      интеграционные тесты утверждений курса
└── python/             confluent-kafka: producer (4.8), consumer (5.6), check.py
```

## 1. Запусти кластер

```bash
cd examples/cluster
docker compose up -d
```

Узлы доступны с хоста на `localhost:9092`, `localhost:9192`, `localhost:9292`, изнутри Docker-сети — на `kafka-N:19092`.

## 2. Смок-тест

Нужны Docker, `python3` и собранный `kprobe`:

```bash
cd examples/go && go build -o /tmp/kprobe ./cmd/kprobe && cd ../..
KPROBE=/tmp/kprobe ./examples/scripts/smoke-test.sh
```

Тест создаёт топик из модуля 2.3, проверяет, что сообщения одного ключа лежат в одной партиции и по порядку, смотрит lag и сброс offsets группы, показывает `min.insync.replicas` в действии (останавливает брокер: `acks=all` отклоняется, `acks=1` проходит), останавливает лидера партиции и убеждается, что данные доступны.

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
go test -tags integration -count=1 ./...       # нужен запущенный кластер
```

## 4. Python

```bash
cd examples/python
pip install -r requirements.txt
python producer.py
IDLE_SECONDS=5 python consumer.py
python check.py
```

## Переменные окружения

| Переменная | По умолчанию | Где |
|---|---|---|
| `KAFKA_BROKERS` | три порта localhost | Go, Python |
| `KAFKA_RF` | `3` | тесты Go и Python (`1` для одиночного узла) |
| `KAFKA_FAKE` | пусто | пропускает тесты, требующие настоящего брокера |
| `KPROBE` | `kprobe` | смок-тест |
