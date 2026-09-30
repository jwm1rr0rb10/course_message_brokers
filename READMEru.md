# Брокеры сообщений: бесплатная серия курсов на русском

[![examples](https://github.com/jwm1rr0rb10/course_message_brokers/actions/workflows/examples.yml/badge.svg)](https://github.com/jwm1rr0rb10/course_message_brokers/actions/workflows/examples.yml)
![Курс на русском](https://img.shields.io/badge/язык-русский-red)
![Бесплатный курс](https://img.shields.io/badge/цена-бесплатно-brightgreen)

Четыре курса от нуля до production: NATS, Apache Kafka, RabbitMQ и облачные брокеры AWS, Azure и Google Cloud. В каждом — теория, схемы, команды, которые можно запустить у себя, типичные ошибки, вопросы для самопроверки, финальный проект и вопросы к собеседованию.

🇬🇧 English version: [README.md](README.md)

## Курсы

| Курс | Версия | О чём |
| --- | --- | --- |
| [NATS и JetStream](nats/READMEru.md) | NATS Server 2.15 | Core NATS, subjects, request-reply, стримы и консьюмеры, KV и Object Store, кластеры, супер-кластеры и leaf nodes, безопасность |
| [Apache Kafka](kafka/READMEru.md) | Kafka 4.3, KRaft | Топики и партиции, consumer groups и новый протокол, транзакции, compaction, репликация и ISR, Connect, Streams, share groups |
| [RabbitMQ](rabbit/READMEru.md) | RabbitMQ 4.3 | Exchanges и маршрутизация, quorum queues, streams, confirms и ack, DLX и ретраи, кластер, AMQP 1.0, Federation и Shovel |
| [Облачные брокеры](cloud/READMEru.md) | 2026 | SQS, SNS, EventBridge, Kinesis, Service Bus, Event Grid, Event Hubs, Pub/Sub, Eventarc, Cloud Tasks — одни и те же задачи в трёх облаках рядом |

## С чего начать

Курсы независимы, начинать можно с любого. Если не знаешь, с какого:

1. **Нужна очередь задач или маршрутизация** — [RabbitMQ](rabbit/READMEru.md).
2. **Нужен лог событий, повторное чтение, большие потоки** — [Kafka](kafka/READMEru.md).
3. **Нужна лёгкая шина для микросервисов, request-reply, edge** — [NATS](nats/READMEru.md).
4. **Работаешь в облаке** — [облачные брокеры](cloud/READMEru.md), лучше после одного из курсов выше: облачный курс ссылается на них там, где сервис повторяет их идеи.

Если выбираешь брокер для проекта, начни с разделов сравнения: [NATS 0.7](nats/READMEru.md#07-nats-vs-kafka-vs-rabbitmq-vs-grpc), [Kafka 0.6](kafka/READMEru.md#06-kafka-против-rabbitmq-nats-и-pulsar), [RabbitMQ 0.6](rabbit/READMEru.md#06-rabbitmq-против-kafka-nats-и-redis) и, для облака, [15.3 «Облако или свой кластер»](cloud/READMEru.md#153-облако-или-свой-кластер-грубая-оценка).

## Примеры и проверка утверждений курса

У каждого курса есть папка `examples/`: кластер в Docker Compose, смок-тест команд из текста, код на Go и Python и интеграционные тесты, которые проверяют утверждения курса на живом брокере.

[CI](.github/workflows/examples.yml) на каждом push и раз в неделю:

- проверяет Go-код (`go vet`, юнит-тесты) и тесты на встроенных брокерах — встроенный nats-server и kfake для Kafka, без Docker;
- поднимает кластеры и эмуляторы и прогоняет смок-тесты и интеграционные тесты дважды: на версии, закреплённой в курсе, и на самом свежем образе.

Если новая версия брокера изменит поведение, описанное в курсе, сборка покраснеет.

Интеграционные тесты без запущенного брокера пропускаются; с `COURSE_REQUIRE_BROKER=1` они падают — так работает CI.

## Лицензия

[MIT](LICENSE)
