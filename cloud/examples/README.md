# Запускаемые примеры к курсу по облачным брокерам

Тесты проверяют утверждения курса на **эмуляторах**, без облачного аккаунта. CI ([`.github/workflows/examples.yml`](../../.github/workflows/examples.yml)) прогоняет их при каждом push и раз в неделю по расписанию: `go vet` обоих Go-модулей, затем тесты на Go и Python против эмуляторов из `docker compose` — на закреплённых версиях образов и на свежих (`latest`). Так видно, если эмулятор или SDK изменили поведение.

```
examples/
├── emulators/
│   ├── docker-compose.yml       moto (AWS), Pub/Sub emulator, Service Bus emulator + SQL Server
│   └── servicebus-config.json   очереди, топик и подписки для эмулятора Service Bus
├── python/
│   ├── requirements.txt
│   ├── pytest.ini
│   └── tests/
│       ├── test_aws.py          SQS, SNS, EventBridge (модули 3–5, 11)
│       ├── test_azure.py        Service Bus (модуль 6)
│       └── test_gcp.py          Pub/Sub (модуль 8)
├── go/                          AWS + Azure (aws-sdk-go-v2, azservicebus)
│   ├── cmd/sqs-worker           консьюмер SQS: long polling, продление visibility, backoff, DLQ (3.7, 3.8, 3.10)
│   ├── cmd/servicebus-worker    консьюмер Service Bus: продление lock, complete / abandon / dead-letter (6.4)
│   └── cloudtest/               интеграционные тесты AWS и Service Bus
└── go-gcp/                      Google Cloud (cloud.google.com/go/pubsub/v2)
    ├── cmd/pubsub-worker        консьюмер Pub/Sub: streaming pull, flow control (8.4)
    └── gcptest/                 интеграционные тесты Pub/Sub
```

Go-примеры разделены на два модуля намеренно: клиент Google Cloud тянет большое дерево зависимостей, и тем, кому нужны только AWS или Azure, незачем его скачивать.

## Запуск

```bash
cd examples/emulators
docker compose up -d --wait         # все эмуляторы; --wait ждёт healthcheck'ов
# или по отдельности: docker compose up -d aws | pubsub | sql servicebus
curl -sf http://localhost:5300/health   # эмулятор Service Bus готов (стартует до минуты)

cd ../python
pip install -r requirements.txt
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=eu-central-1
python -m pytest -v -rs             # тесты облака без запущенного эмулятора будут пропущены
python -m pytest -v -m aws          # только AWS
```

AWS-тесты можно запустить и без Docker: `pip install "moto[server]"` и `moto_server -p 4566`.

Go (нужен Go 1.25+):

```bash
cd examples/go
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test go test -tags integration -count=1 -v ./...
go run ./cmd/sqs-worker -queue tasks

cd ../go-gcp
PUBSUB_EMULATOR_HOST=localhost:8085 go test -tags integration -count=1 -v ./...
```

Тесты облака без запущенного эмулятора пропускаются, как и в Python. С `COURSE_REQUIRE_BROKER=1` (так в CI) недоступный эмулятор — ошибка теста, а не пропуск: иначе упавший контейнер выглядел бы как зелёный прогон.

## Что проверяется

Python- и Go-тесты проверяют одни и те же утверждения курса.

| Облако | Тесты |
|---|---|
| AWS | Visibility timeout и повторная доставка с новым receipt handle, `ChangeMessageVisibility(0)`, redrive в DLQ после `maxReceiveCount`, дедупликация и порядок в FIFO, частичная ошибка батча при HTTP 200, SNS → SQS с raw delivery и filter policy, правило EventBridge с числовым сравнением |
| Azure | `complete`, `abandon` и рост `delivery_count`, явный dead-letter с причиной, автоматический dead-letter после `MaxDeliveryCount`, порядок в сессии, duplicate detection, отложенное сообщение, SQL-фильтр подписки |
| Google Cloud | Fan-out на несколько подписок, одна подписка как очередь, nack и повторная доставка, ordering keys, фильтр подписки, dead letter topic |

Если эмулятор не поддерживает какую-то функцию (например, фильтры или dead lettering в Pub/Sub emulator), тест **пропускается с объяснением** (`-rs` покажет причину), а не падает: это ограничение эмулятора, а не сервиса.

## Эмуляторы: что важно знать

- **AWS:** используется [moto](https://github.com/getmoto/moto) — open source, без регистрации. LocalStack с марта 2026 года требует auth token даже для бесплатного плана; если он у тебя есть, можно запускать тесты против LocalStack с тем же `AWS_ENDPOINT_URL=http://localhost:4566`.
- **Azure:** эмулятор Service Bus хранит метаданные в SQL Server. Azure SQL Edge, которую использовали раньше, выведена из эксплуатации, поэтому здесь образ `mcr.microsoft.com/mssql/server`. Сущности (очереди, топики, подписки, правила) эмулятор создаёт из `servicebus-config.json` при старте. Образ SQL Server есть только для amd64, поэтому в compose указано `platform: linux/amd64`: на Apple Silicon Docker Desktop запускает его через эмуляцию (Rosetta), это медленнее, но работает.
- **Google Cloud:** официальный эмулятор Pub/Sub из образа Google Cloud CLI. Он не реализует IAM, exactly-once и часть настроек подписок.

## Переменные окружения

| Переменная | По умолчанию | Для чего |
|---|---|---|
| `AWS_ENDPOINT_URL` | `http://localhost:4566` | moto, LocalStack; убери, чтобы тестировать настоящий AWS (осторожно со счётом) |
| `PUBSUB_EMULATOR_HOST` | `localhost:8085` | Эмулятор Pub/Sub |
| `PUBSUB_PROJECT_ID` | `course-project` | Проект в эмуляторе |
| `SERVICEBUS_CONNECTION_STRING` | строка эмулятора с `UseDevelopmentEmulator=true` | Эмулятор Service Bus |
| `COURSE_REQUIRE_BROKER` | не задана | `1` — недоступный эмулятор роняет тест вместо пропуска (CI) |
| `MOTO_IMAGE`, `PUBSUB_IMAGE`, `SERVICEBUS_IMAGE`, `MSSQL_IMAGE` | закреплённые версии из `docker-compose.yml` | Подменить образ эмулятора, например `MOTO_IMAGE=motoserver/moto:latest` |
