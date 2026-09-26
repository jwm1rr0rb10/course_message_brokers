# NATS курс 2026: бесплатный курс по NATS и JetStream с нуля до профи на русском

![NATS 2.12](https://img.shields.io/badge/NATS-2.12-27AAE1?logo=natsdotio&logoColor=white)
![JetStream](https://img.shields.io/badge/JetStream-persistence-blue)
![Курс на русском](https://img.shields.io/badge/язык-русский-red)
![Бесплатный курс](https://img.shields.io/badge/цена-бесплатно-brightgreen)
![От junior до senior](https://img.shields.io/badge/уровень-junior%20→%20senior-orange)

> **Полный бесплатный курс по NATS на русском языке.** Теория, практика, Docker, Go, Python и Java, Core NATS и JetStream, subjects и wildcards, queue groups, request-reply, стримы и консьюмеры, гарантии доставки и дедупликация, Key-Value и Object Store, микросервисы, кластеризация, супер-кластеры и leaf nodes, мониторинг, безопасность, тюнинг и production-архитектура. Всё в одном README, актуально для **NATS Server 2.12 (2026)**.

**NATS обучение без воды:** каждый модуль состоит из понятной теории, схем, команд, которые можно запустить у себя, типичных ошибок и вопросов для самопроверки. Курс подходит, чтобы выучить NATS с нуля, подготовиться к собеседованию на backend, platform или DevOps-позицию и спроектировать надёжную систему на NATS в продакшене.

⭐ Если курс полезен, поставь звезду репозиторию: так его найдут другие разработчики.

🇬🇧 English version: [README.md](README.md)

---

## Для кого этот курс по NATS

| Кто ты | Что получишь |
|---|---|
| **Новичок** в брокерах сообщений | Понимание, что такое NATS, чем он отличается от Kafka и RabbitMQ, и как запустить его за 1 минуту |
| **Backend-разработчик** (примеры на Go, Python и Java; паттерны переносятся на клиенты для Node.js, .NET и Rust) | Pub/Sub, request-reply, queue groups, надёжная доставка через JetStream, идемпотентность |
| **Разработчик микросервисов** | Сервисный слой на NATS вместо HTTP, service discovery из коробки, распределённый RPC |
| **Data / platform engineer** | Стримы, mirrors и sources, гео-репликация, KV и Object Store, честное сравнение с Kafka |
| **DevOps / SRE** | Кластер с RAFT, supercluster, leaf nodes, мониторинг, алерты, sizing, Kubernetes |
| **Архитектор / Tech Lead** | Проектирование subject-пространства, event-driven архитектура, multi-region, антипаттерны |
| **IoT / edge** | Leaf nodes на устройствах, работа при обрывах связи, встроенные MQTT- и WebSocket-листенеры |
| **Готовишься к собеседованию** | 33 вопроса по NATS с ответами уровня junior, middle и senior |

## Что ты будешь уметь после курса

- объяснить архитектуру NATS: subject, message, client, server, cluster, account, JetStream domain;
- поднять кластер NATS из трёх узлов в Docker и ломать его, наблюдая выборы RAFT-лидера;
- проектировать subject-пространство и грамотно использовать wildcards `*` и `>`;
- писать publisher и subscriber на Go, Python и Java без потери и дублирования сообщений;
- использовать request-reply, queue groups и паттерн scatter-gather;
- понимать разницу между Core NATS (at-most-once) и JetStream (at-least-once, exactly-once);
- создавать стримы с нужной политикой retention: `limits`, `interest`, `workqueue`;
- настраивать консьюмеров: pull vs push, `AckExplicit`, `AckWait`, `MaxDeliver`, `MaxAckPending`, backoff;
- реализовывать дедупликацию через заголовок `Nats-Msg-Id` и duplicate window;
- строить DLQ на advisory-сообщениях `MAX_DELIVERIES`;
- использовать Key-Value Store как конфиг, кэш, распределённую блокировку и leader election;
- хранить файлы в Object Store;
- писать микросервисы на Services API с автоматическим discovery и статистикой;
- собирать supercluster из нескольких дата-центров и подключать leaf nodes на edge;
- мониторить `/varz`, `/jsz`, consumer lag, slow consumers и настраивать алерты;
- включать TLS, accounts, NKeys, JWT-аутентификацию, permissions и лимиты;
- рассчитывать память, диск и число узлов для продакшена.

---

## Содержание

- [Для кого этот курс по NATS](#для-кого-этот-курс-по-nats)
- [Что ты будешь уметь после курса](#что-ты-будешь-уметь-после-курса)
- [Как проходить курс](#как-проходить-курс)
- [Модуль 0. Что такое NATS и зачем он нужен](#модуль-0-что-такое-nats-и-зачем-он-нужен)
- [Модуль 1. Архитектура NATS: subject, message, server, cluster](#модуль-1-архитектура-nats-subject-message-server-cluster)
- [Модуль 2. Установка NATS в Docker и первые команды](#модуль-2-установка-nats-в-docker-и-первые-команды)
- [Модуль 3. Subjects и wildcards: проектирование пространства имён](#модуль-3-subjects-и-wildcards-проектирование-пространства-имён)
- [Модуль 4. Core NATS: pub/sub, request-reply, queue groups](#модуль-4-core-nats-pubsub-request-reply-queue-groups)
- [Модуль 5. JetStream: стримы и хранение сообщений](#модуль-5-jetstream-стримы-и-хранение-сообщений)
- [Модуль 6. Консьюмеры JetStream: pull, push, ack](#модуль-6-консьюмеры-jetstream-pull-push-ack)
- [Модуль 7. Гарантии доставки, дедупликация и exactly-once](#модуль-7-гарантии-доставки-дедупликация-и-exactly-once)
- [Модуль 8. Retention, лимиты и работа с диском](#модуль-8-retention-лимиты-и-работа-с-диском)
- [Модуль 9. Key-Value Store](#модуль-9-key-value-store)
- [Модуль 10. Object Store](#модуль-10-object-store)
- [Модуль 11. Микросервисы на NATS: Services API](#модуль-11-микросервисы-на-nats-services-api)
- [Модуль 12. Кластеризация, superclusters и leaf nodes](#модуль-12-кластеризация-superclusters-и-leaf-nodes)
- [Модуль 13. Mirrors, sources и гео-репликация](#модуль-13-mirrors-sources-и-гео-репликация)
- [Модуль 14. Обработка ошибок: retry, DLQ и poison pill](#модуль-14-обработка-ошибок-retry-dlq-и-poison-pill)
- [Модуль 15. Производительность и тюнинг NATS](#модуль-15-производительность-и-тюнинг-nats)
- [Модуль 16. Мониторинг NATS: метрики, lag, алерты](#модуль-16-мониторинг-nats-метрики-lag-алерты)
- [Модуль 17. Безопасность NATS: accounts, NKeys, JWT, TLS](#модуль-17-безопасность-nats-accounts-nkeys-jwt-tls)
- [Модуль 18. NATS в продакшене: архитектура и эксплуатация](#модуль-18-nats-в-продакшене-архитектура-и-эксплуатация)
- [Модуль 19. Итоговый проект: event-driven интернет-магазин](#модуль-19-итоговый-проект-event-driven-интернет-магазин)
- [Что нового в NATS 2.11 и 2.12](#что-нового-в-nats-211-и-212)
- [Шпаргалка NATS CLI](#шпаргалка-nats-cli)
- [Шпаргалка важных настроек](#шпаргалка-важных-настроек)
- [Вопросы на собеседовании по NATS с ответами](#вопросы-на-собеседовании-по-nats-с-ответами)
- [FAQ: частые вопросы про NATS](#faq-частые-вопросы-про-nats)
- [Глоссарий NATS](#глоссарий-nats)
- [Официальные источники и что читать дальше](#официальные-источники-и-что-читать-дальше)

---

## Как проходить курс

1. **Иди по порядку.** Модули 0-6 это фундамент. Без понимания subject, queue group, стрима и ack всё остальное будет магией.
2. **Запускай каждую команду.** NATS учится руками, а `nats` CLI даёт обратную связь за секунды. Прочитать про redelivery и увидеть её в `nats stream view` это разные уровни понимания.
3. **Ломай кластер.** Останавливай узлы, убивай консьюмеров, заполняй лимиты стрима. Именно так появляется production-опыт.
4. **Отвечай на вопросы в конце модуля** вслух, как на собеседовании.
5. **Сделай итоговый проект.** Он собирает все темы в одну систему.

**Что нужно установить:** Docker и Docker Compose, `nats` CLI (natscli), Git, любую IDE. Для примеров на Go нужен Go 1.26+ (его требует актуальный `nats.go`), для Python нужен Python 3.10+, для Java нужна Java 17+.

**Версия:** все примеры написаны для NATS Server 2.12.x, образ `nats:2.12-alpine`. JetStream включается флагом `-js` и не работает по умолчанию.

**Запускаемые примеры:** кластер, смок-тест команд CLI, код на Go и Python и интеграционные тесты утверждений курса лежат в [`examples/`](examples/). CI каждую неделю прогоняет их на 2.12 и на свежем релизе 2.x, так что если новая версия изменит описанное здесь поведение, сборка покраснеет.

---

# Модуль 0. Что такое NATS и зачем он нужен

## 0.1 Определение NATS простыми словами

**NATS** это система обмена сообщениями для распределённых систем: один процесс (`nats-server`) размером около 20 МБ, который умеет три вещи:

1. **Мгновенно доставлять** сообщения между сервисами по subject-адресам (Core NATS).
2. **Надёжно хранить** сообщения на диске и переотправлять их, пока потребитель не подтвердит обработку (JetStream).
3. **Хранить состояние**: ключ-значение (KV Store) и файлы (Object Store) поверх того же движка.

Главная идея NATS: **subject-based messaging**. Отправитель публикует сообщение в тему вида `orders.created.eu`, а кто его получит, отправителя не волнует. Вся маршрутизация строится на иерархических именах и wildcards, без exchanges, routing keys и bindings.

NATS создал Derek Collison, до этого проектировавший системы обмена сообщениями в TIBCO. Первая версия была написана на Ruby как шина сообщений Cloud Foundry, позже сервер переписали на Go. NATS — проект CNCF, который развивает компания Synadia; его используют в продакшене множество компаний, платформ и IoT-решений. Актуальный и проверяемый список пользователей — на [nats.io](https://nats.io).

## 0.2 Проблема, которую решает NATS

Представим интернет-магазин. Пользователь оформил заказ, и об этом должны узнать сразу несколько систем:

```
                    +--> Payment Service
                    |
Order Service ------+--> Warehouse
                    |
                    +--> Analytics
                    |
                    +--> Notification Service
                    |
                    +--> Fraud Detection
```

Без брокера Order Service ходит в каждый сервис напрямую по HTTP:

```
Order Service
   |
   +--> POST http://payment/...
   +--> POST http://warehouse/...
   +--> POST http://analytics/...
   +--> POST http://notification/...
   +--> POST http://fraud/...
```

Дальше начинаются проблемы:

| Вопрос | Проблема синхронной интеграции |
|---|---|
| Notification Service упал | Заказ падает целиком или теряется уведомление |
| Analytics тормозит 3 секунды | Пользователь ждёт 3 секунды на оформлении заказа |
| Появилось ещё 10 потребителей | Приходится менять и деплоить Order Service |
| Сервис переехал на другой адрес | Нужно править конфиги, DNS, service mesh |
| Пик нагрузки в Чёрную пятницу | Нижестоящие сервисы ложатся каскадом |

Это **сильная связанность (tight coupling)**: каждый сервис знает адрес каждого.

## 0.3 Как выглядит та же система с NATS

```
Order Service
     |
     | publish "shop.orders.created"
     v
+---------------------+
|    nats-server      |
+---------------------+
     |      |      |       |
     v      v      v       v
Payment  Warehouse Analytics Fraud
 (subscribe "shop.orders.>")
```

Order Service публикует **одно сообщение** в subject `shop.orders.created` и больше ни о ком не знает. Каждый потребитель подписывается на тот subject, который ему интересен.

Что мы получили:

- **Слабая связанность.** Новый сервис подключается без изменения Order Service.
- **Location transparency.** Никто не знает IP и портов друг друга: адрес это subject, а не хост.
- **Готовый service discovery.** Подписался на subject значит уже «зарегистрирован».
- **Балансировка из коробки.** Десять экземпляров сервиса в одной queue group делят нагрузку.
- **Долговечность по требованию.** Нужен replay и гарантии? Включаешь JetStream для этого subject, код паблишера почти не меняется.

## 0.4 Core NATS и JetStream: два режима одной системы

Это **главное, что нужно понять про NATS**. В одном сервере живут два слоя.

```
+---------------------------- nats-server ----------------------------+
|                                                                     |
|  Core NATS                         JetStream                        |
|  --------------------------        -----------------------------    |
|  доставка в памяти                 запись на диск (или в RAM)       |
|  at-most-once                      at-least-once / exactly-once     |
|  нет подписчика = сообщение        сообщение лежит в стриме         |
|  просто исчезло                    и ждёт потребителя               |
|  задержка ~микросекунды            задержка ~миллисекунды           |
|  нет replay                        replay, история, курсоры         |
|  fire and forget                   ack, retry, DLQ                  |
+---------------------------------------------------------------------+
```

| Вопрос | Core NATS | JetStream |
|---|---|---|
| Сообщение сохраняется? | Нет | Да, в файл или память |
| Что если подписчиков нет? | Сообщение теряется | Сохраняется в стриме |
| Подтверждение обработки | Нет | Да (`ack`, `nak`, `term`, `inProgress`) |
| Повторная доставка | Нет | Да, до `MaxDeliver` раз |
| Перечитать историю | Нет | Да, с любой позиции или времени |
| Типичная задержка | Десятки-сотни микросекунд | Единицы миллисекунд |
| Когда использовать | Телеметрия, RPC, health, кэш-инвалидация | Заказы, платежи, очереди задач, события домена |

**Практическое правило:** Core NATS для того, что не жалко потерять и что важно доставить быстро. JetStream для всего, за что отвечает бизнес.

## 0.5 Что такое subject

**Subject** это строка-адрес, разделённая точками на токены:

```
shop.orders.created
shop.orders.cancelled
shop.payments.eu.succeeded
iot.sensors.msk.dc1.temperature
```

Правила:

- токены разделяются точкой, регистр имеет значение;
- `*` заменяет **ровно один** токен: `shop.*.created`;
- `>` заменяет **один или несколько** токенов и может стоять только в конце: `shop.orders.>`;
- subject, начинающийся с `$`, зарезервирован системой (`$JS.API.>`, `$SYS.>`, `$KV.>`).

Subject это не «очередь» и не «топик, который нужно создать». В Core NATS subject **не требует создания**: опубликовал и всё, он существует ровно в момент публикации.

## 0.6 Что такое сообщение (message)

```
Subject: shop.orders.created
Reply:   _INBOX.7fK2s3Lq.1      (куда отвечать, опционально)
Headers: Nats-Msg-Id: 5f1c2a7e-9b1d-4c1e-8a4e-0c7b2d9f1a11
         Event-Type: OrderCreated
         Trace-Id: 4bf92f3577b34da6
Data:    {"order_id":"order-123","user_id":"user-42","amount":4990}
```

- **Data** это просто байты. NATS не знает и не проверяет формат: JSON, Protobuf, Avro, MessagePack, что угодно.
- **Headers** появились в NATS 2.2 и работают и в Core, и в JetStream. Через них передают `Nats-Msg-Id`, trace id, тип события, версию схемы.
- **Reply subject** превращает обычную публикацию в RPC-запрос.
- По умолчанию максимальный размер сообщения `max_payload` = **1 МБ**. Его можно поднять (до 64 МБ), но это плохая идея: большие сообщения забивают буферы и рождают slow consumers. Файлы кладут в Object Store.

## 0.7 NATS vs Kafka vs RabbitMQ vs gRPC

| Критерий | NATS (Core + JetStream) | Apache Kafka | RabbitMQ | HTTP/gRPC |
|---|---|---|---|---|
| Модель | Subject-based pub/sub + лог (JetStream) | Распределённый лог | Брокер очередей (AMQP) | Запрос-ответ |
| Что деплоим | Один бинарник ~20 МБ, без зависимостей | JVM-кластер, KRaft | Erlang-кластер | Ничего |
| Request-reply | Встроен в протокол | Нет, делается вручную | Через reply-to очереди | Нативно |
| Гарантии | At-most-once (Core) или at-least/exactly-once (JetStream) | At-least/exactly-once | At-least-once | Зависит от реализации |
| Хранение и replay | Да, JetStream | Да, ядро продукта | Ограниченно (Streams) | Нет |
| Пропускная способность | Миллионы msg/s (Core), сотни тысяч (JetStream) | Миллионы msg/s | Десятки-сотни тысяч msg/s | Зависит от сервиса |
| Задержка | Десятки микросекунд | Миллисекунды | Субмиллисекунды-миллисекунды | Зависит от сети |
| Маршрутизация | Иерархические subjects и wildcards | Топики и партиции | Exchanges, routing keys, headers | URL-пути |
| Multi-tenancy | Accounts с полной изоляцией, из коробки | Через ACL и префиксы | Vhosts | Нет |
| Geo / edge | Superclusters, gateways, leaf nodes | MirrorMaker 2 | Federation, shovel | Нет |
| Порядок сообщений | Внутри стрима (и по subject) | Внутри partition | Внутри очереди | Нет |
| Когда выбирать | Микросервисы, edge/IoT, low latency, multi-region, «и шина, и очередь, и KV в одном» | Big data, аналитика, CDC, годы истории, экосистема Connect/Streams | Сложный роутинг, приоритеты, TTL на сообщение, legacy AMQP | Синхронный ответ пользователю |

**Честно про сравнение с Kafka:** если тебе нужны терабайты истории, Kafka Connect, Kafka Streams, Debezium CDC и интеграция с хранилищем данных, бери Kafka. Если нужна нервная система для микросервисов с RPC, низкой задержкой, простой эксплуатацией и работой через континенты и edge, NATS обычно оказывается более простым выбором. Это разные инструменты, и они часто живут в одной компании рядом.

### NATS и HTTP вместе

NATS не отменяет HTTP:

```
Client --HTTP--> Order API --(сохранил заказ, ответил 201)--> Client
                     |
                     +--publish shop.orders.created--> NATS --> остальные сервисы
```

HTTP нужен там, где пользователь ждёт синхронный ответ. NATS нужен для асинхронного распространения событий и для внутреннего RPC между сервисами.

## 0.8 Где используют NATS: реальные сценарии

| Сценарий | Как применяется NATS |
|---|---|
| **Микросервисы** | Внутренний RPC вместо HTTP, queue groups как балансировщик, Services API как discovery |
| **Event-driven архитектура** | Доменные события в JetStream, хореография саг, outbox |
| **IoT и edge** | Leaf node на устройстве или в цехе, буферизация при обрыве связи, синхронизация со «штабом» |
| **Телеметрия и метрики** | Core NATS: терять отдельные точки не страшно, важна скорость |
| **Distributed cache / конфиг** | KV Store с watch: сервисы мгновенно узнают об изменении конфига |
| **Очереди задач** | Стрим с `retention=workqueue` и pull-консьюмеры как пул воркеров |
| **Мультирегион** | Supercluster: публикация в Европе, потребление в США, без MirrorMaker |
| **Control plane** | Управление агентами, деплой-команды, сбор статусов с тысяч узлов |
| **Замена Redis Pub/Sub и части RabbitMQ** | Один сервис вместо трёх систем |

## 0.9 Когда NATS не нужен

- Нужна многолетняя история событий, big-data пайплайны, ClickHouse и Snowflake через готовые коннекторы: бери Kafka.
- Нужна сложная маршрутизация по заголовкам, приоритеты сообщений и TTL на каждое сообщение: RabbitMQ гибче.
- У тебя один монолит и три фоновые задачи: хватит очереди в PostgreSQL или Redis.
- Требуется синхронный ответ пользователю по HTTP: используй HTTP.
- Нужны транзакции между брокером и базой данных: этого не даст никто, проектируй outbox и идемпотентность.

## 0.10 Что NATS НЕ делает за тебя

NATS даёт инфраструктурные гарантии. Корректность системы проектируешь ты:

- идемпотентность обработки на стороне приложения;
- схему и версионирование событий (Schema Registry в NATS нет, контракты ведёшь сам);
- стратегию retry и обработку «ядовитых» сообщений;
- мониторинг и алерты;
- accounts, permissions и лимиты;
- продуманное subject-пространство: переименовать subject потом больно.

### Вопросы для самопроверки

1. Чем Core NATS отличается от JetStream и когда чего достаточно?
2. Почему в Core NATS сообщение исчезает, если нет подписчиков?
3. Зачем нужен reply subject?
4. В каких случаях ты выберешь Kafka вместо NATS?

---

# Модуль 1. Архитектура NATS: subject, message, server, cluster

## 1.1 Главная иерархия

```
Supercluster (несколько кластеров через gateways)
  └── Cluster (группа серверов, соединённых routes)
        └── Server (процесс nats-server)
              └── Account (изолированное пространство имён)
                    └── Subject (иерархический адрес)
                          └── Message (subject + headers + payload)

  и отдельно, если включён JetStream:
  Account
    └── Stream (набор subjects + правила хранения)
          └── Consumer (курсор + политика доставки и ack)
```

Запомни эту картинку. На ней держится весь курс.

## 1.2 Основные компоненты NATS

| Компонент | Что это | Аналогия |
|---|---|---|
| **Message** | subject, headers, reply, payload | Письмо с адресом |
| **Subject** | Иерархический адрес сообщения | Тема письма / путь URL |
| **Client** | Приложение, подключённое по TCP к серверу | Клиент БД |
| **Server** | Процесс `nats-server` | Сервер БД |
| **Cluster** | Группа серверов, связанных `routes`, с полной mesh-топологией | Кластер БД |
| **Gateway** | Соединение между кластерами (supercluster) | Межрегиональный линк |
| **Leaf node** | Сервер, подключённый к кластеру «сверху вниз», для edge и изоляции | Филиал |
| **Account** | Полностью изолированное пространство subjects | Отдельный виртуальный брокер |
| **Queue group** | Группа подписчиков, делящих сообщения между собой | Пул воркеров |
| **Stream** | Хранилище сообщений JetStream по набору subjects | Таблица / лог |
| **Consumer** | Позиция чтения стрима с политикой ack | Курсор + очередь |
| **KV bucket** | Key-Value поверх стрима | Redis-хэш / etcd |
| **Object bucket** | Хранилище файлов поверх стрима | S3-бакет |

## 1.3 Полносвязный кластер

NATS-кластер это **full mesh**: каждый сервер соединён с каждым напрямую.

```
        +-----------+
        | nats-1    |
        +-----------+
         /         \
        /           \
+-----------+   +-----------+
| nats-2    |---| nats-3    |
+-----------+   +-----------+
```

Важные следствия:

- **клиент подключается к любому серверу** и видит весь кластер: сообщения маршрутизируются между серверами автоматически;
- сервер сообщает клиенту список остальных адресов, и клиентская библиотека **сама переподключается** при падении узла;
- у сообщения максимум **один hop** между серверами внутри кластера, поэтому задержка предсказуема;
- сервер отправляет сообщение соседу, **только если у того есть заинтересованный подписчик**: распространение подписок (interest propagation) экономит трафик;
- кластер из 3 или 5 узлов это норма; в один кластер редко ставят больше 7-9 серверов, дальше строят supercluster.

## 1.4 Interest-based маршрутизация

В NATS сообщение никогда не хранится «на всякий случай» в Core-режиме. Сервер знает, кто и на что подписан, и рассылает точечно.

```
1. subscriber на nats-2 делает SUB shop.orders.>
2. nats-2 сообщает nats-1 и nats-3: "у меня есть интерес к shop.orders.>"
3. publisher на nats-1 делает PUB shop.orders.created
4. nats-1 доставляет локальным подписчикам и отправляет копию на nats-2
5. на nats-3 подписчиков нет -> туда не отправляется ничего
```

Отсюда важное свойство: **публикация в subject, на который никто не подписан, ничего не стоит и никуда не летит**. Это не ошибка, и клиент об этом не узнает. Именно поэтому в Core NATS так легко «потерять» сообщения, если подписчик ещё не успел стартовать.

## 1.5 Подключение клиента

```
1. client --> nats://nats-1:4222 : CONNECT {…}
2. server --> client: INFO {server_id, max_payload, connect_urls:[…], headers:true}
3. client --> server: SUB shop.orders.> 1
4. client --> server: PUB shop.orders.created 45 \r\n {payload}
5. server --> client: MSG shop.orders.created 1 45 \r\n {payload}
```

Протокол NATS **текстовый и человекочитаемый**. Его можно посмотреть телнетом:

```bash
telnet localhost 4222
```

Основные команды протокола: `CONNECT`, `PUB`, `HPUB`, `SUB`, `UNSUB`, `MSG`, `HMSG`, `PING`, `PONG`, `+OK`, `-ERR`. Это одна из причин, по которой клиентские библиотеки для NATS существуют почти для каждого языка и весят мало.

## 1.6 JetStream: слой хранения

JetStream включается флагом `-js` или блоком `jetstream {}` в конфиге. Он добавляет к серверу:

- **стримы** (streams) — хранилища сообщений, которые захватывают заданные subjects;
- **консьюмеров** (consumers) — позиции чтения с политиками ack и повторной доставки;
- **KV и Object Store** — надстройки над стримами;
- **RAFT-репликацию** — каждый стрим с `replicas=3` образует собственную RAFT-группу.

```
publish "shop.orders.created"
        |
        v
+------------------------------------------+
| Stream ORDERS                            |
|  subjects: shop.orders.>                 |
|  storage:  file                          |
|  replicas: 3   (RAFT: leader + 2)        |
|  retention: limits, max_age: 168h        |
|                                          |
|  seq 1 [shop.orders.created  order-1]    |
|  seq 2 [shop.orders.paid     order-1]    |
|  seq 3 [shop.orders.created  order-2]    |
+------------------------------------------+
        |                 |
        v                 v
 consumer BILLING   consumer ANALYTICS
 (pull, ack explicit) (pull, ack none)
```

Ключевое отличие от Core NATS: сообщение **сначала записывается в стрим и подтверждается сервером**, и только потом консьюмеры его читают в своём темпе.

## 1.7 Ключевые числа стрима

| Понятие | Смысл |
|---|---|
| **Stream sequence** | Глобальный порядковый номер сообщения внутри стрима, начинается с 1 |
| **Consumer sequence** | Счётчик доставок конкретному консьюмеру (растёт и при повторной доставке) |
| **Ack floor** | До какого stream sequence консьюмер подтвердил всё подряд |
| **Num pending** | Сколько сообщений консьюмеру ещё предстоит получить (аналог consumer lag в Kafka) |
| **Num ack pending** | Сколько сообщений доставлено, но ещё не подтверждено |
| **Num redelivered** | Сколько сообщений доставляются повторно прямо сейчас |
| **First / last seq** | Границы того, что сейчас физически лежит в стриме |

`num_pending` это и есть **lag**, за которым надо следить в мониторинге.

## 1.8 Accounts: изоляция в одном сервере

**Account** это отдельное пространство subjects. Два аккаунта не видят сообщений друг друга, даже если публикуют в одинаковые subjects.

```
account APP_A: shop.orders.created   <-- не видит account APP_B
account APP_B: shop.orders.created   <-- не видит account APP_A
```

Обмен между аккаунтами возможен только явно, через **exports** и **imports**. Это делает NATS по-настоящему multi-tenant: один кластер обслуживает десятки команд и клиентов без риска, что кто-то подпишется на `>` и увидит чужие данные. Системный аккаунт `$SYS` отдельно хранит служебные subjects мониторинга.

## 1.9 Первая ментальная модель

```
Core NATS:
  publish subject -> сервер ищет подписчиков ->
  доставляет всем (и по одному в каждой queue group) -> забывает сообщение

JetStream:
  publish subject -> стрим, чьи subjects совпали, записывает сообщение ->
  RAFT реплицирует на кворум -> сервер отвечает PubAck{stream, seq} ->
  консьюмер получает сообщение -> обрабатывает -> ack ->
  сообщение остаётся в стриме до истечения retention
```

### Вопросы для самопроверки

1. Почему публикация в subject без подписчиков ничего не стоит и чем это опасно?
2. Зачем NATS-кластеру полносвязная топология и сколько hops проходит сообщение?
3. Чем stream sequence отличается от consumer sequence?
4. Что даёт разделение на accounts?

---

# Модуль 2. Установка NATS в Docker и первые команды

## 2.1 Самый быстрый запуск NATS

```bash
docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:2.12-alpine -js -m 8222
```

- `4222` — клиентский порт;
- `6222` — порт для routes между серверами кластера;
- `8222` — HTTP-порт мониторинга;
- `-js` — включить JetStream (**без него не будет ни стримов, ни KV**);
- `-m 8222` — включить мониторинг.

Проверяем:

```bash
curl -s localhost:8222/varz | head -20
curl -s localhost:8222/jsz
docker logs nats | grep -i jetstream
```

## 2.2 Устанавливаем nats CLI

`nats` это официальный CLI, без него жить на порядок тяжелее.

```bash
# macOS / Linux через Homebrew
brew tap nats-io/nats-tools && brew install nats-io/nats-tools/nats

# Go
go install github.com/nats-io/natscli/nats@latest

# Или просто через Docker
docker run --rm -it --network host natsio/nats-box:latest
```

Контексты избавляют от бесконечных флагов:

```bash
nats context add local --server nats://localhost:4222 --description "Локальный NATS"
nats context select local
nats server check connection
```

## 2.3 Первое сообщение: pub/sub

Терминал 1:

```bash
nats sub "shop.orders.>"
```

Терминал 2:

```bash
nats pub shop.orders.created '{"order_id":"order-1","amount":4990}'
nats pub shop.orders.paid    '{"order_id":"order-1"}'
nats pub shop.payments.failed '{"order_id":"order-2"}'
```

Подписчик увидит первые два сообщения и не увидит третье: `shop.orders.>` не покрывает `shop.payments.*`.

**Первый важный эксперимент.** Останови подписчика, опубликуй сообщение, запусти подписчика снова. Сообщения не будет: это Core NATS, at-most-once. Именно этот момент отделяет тех, кто понял NATS, от тех, кто «попробовал».

## 2.4 Request-reply за 30 секунд

Терминал 1 — сервис:

```bash
nats reply "service.echo" --echo
```

Терминал 2 — клиент:

```bash
nats request "service.echo" "привет" --timeout 2s
```

Запусти `nats reply "service.echo" --echo` в **трёх** терминалах: это автоматически одна queue group, и запросы начнут распределяться между ними. Балансировка нагрузки без единой строчки конфигурации.

## 2.5 Кластер из трёх узлов в Docker Compose

Создай директорию и файлы:

```bash
mkdir nats-course && cd nats-course
```

`nats.conf` (общий для всех трёх узлов, отличия задаём флагами):

```hcl
# nats.conf
port: 4222
http: 8222

jetstream {
  store_dir: "/data/jetstream"
  max_memory_store: 1GB
  max_file_store: 10GB
}

cluster {
  name: "course"
  port: 6222
  routes: [
    "nats://nats-1:6222"
    "nats://nats-2:6222"
    "nats://nats-3:6222"
  ]
}

accounts {
  # аккаунт приложений с включённым JetStream
  APP: {
    jetstream: enabled
    users: [ { user: "app", password: "app" } ]
  }
  # системный аккаунт: мониторинг и команды `nats server ...`
  SYS: {
    users: [ { user: "admin", password: "admin" } ]
  }
}
system_account: SYS

# ТОЛЬКО ДЛЯ ОБУЧЕНИЯ: клиенты без учётных данных входят как "app".
# В продакшене так не делают (см. модуль 17).
no_auth_user: app
```

Почему этот блок важен: как только в конфиге появляется **хотя бы один** пользователь, сервер начинает требовать аутентификацию. Без `no_auth_user` все команды ниже, которые подключаются без учётных данных, упадут с `Authorization Violation`. А без `jetstream: enabled` у аккаунта ты получишь `jetstream not enabled for account`, хотя на самом сервере JetStream включён.

`docker-compose.yml`:

```yaml
x-nats-common: &nats-common
  image: nats:2.12-alpine
  restart: unless-stopped
  volumes:
    - ./nats.conf:/etc/nats/nats.conf:ro

services:
  nats-1:
    <<: *nats-common
    container_name: nats-1
    command: ["-c", "/etc/nats/nats.conf", "--server_name", "nats-1"]
    ports: ["4222:4222", "8222:8222"]
    volumes:
      - ./nats.conf:/etc/nats/nats.conf:ro
      - nats1-data:/data

  nats-2:
    <<: *nats-common
    container_name: nats-2
    command: ["-c", "/etc/nats/nats.conf", "--server_name", "nats-2"]
    ports: ["4223:4222", "8223:8222"]
    volumes:
      - ./nats.conf:/etc/nats/nats.conf:ro
      - nats2-data:/data

  nats-3:
    <<: *nats-common
    container_name: nats-3
    command: ["-c", "/etc/nats/nats.conf", "--server_name", "nats-3"]
    ports: ["4224:4222", "8224:8222"]
    volumes:
      - ./nats.conf:/etc/nats/nats.conf:ro
      - nats3-data:/data

  nats-box:
    image: natsio/nats-box:latest
    container_name: nats-box
    entrypoint: ["sleep", "infinity"]
    depends_on: [nats-1, nats-2, nats-3]

volumes:
  nats1-data:
  nats2-data:
  nats3-data:
```

Запуск:

```bash
docker compose up -d
docker compose ps
```

Проверяем кластер:

```bash
# контекст приложения: попадает в аккаунт APP через no_auth_user
nats context add cluster \
  --server "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224"
nats context select cluster

# системный контекст: командам `nats server ...` нужен системный аккаунт
nats context add cluster-sys \
  --server "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224" \
  --user admin --password admin

nats --context cluster-sys server list
nats --context cluster-sys server report jetstream
```

Ты увидишь три сервера, их роли и состояние JetStream. Всем командам `nats server ...` в этом курсе нужны учётные данные системного аккаунта; прикладные команды (`pub`, `sub`, `stream`, `consumer`, `kv`) работают через обычный контекст. Клиент указывает **все три адреса** в строке подключения: если первый недоступен, библиотека пойдёт ко второму.

## 2.6 Самая частая ошибка запуска

| Симптом | Причина | Решение |
|---|---|---|
| `nats: no responders available for request` | Никто не слушает subject запроса | Проверь, что сервис запущен и subject совпадает до символа |
| `jetstream not enabled for account` | Сервер запущен без `-js` или у аккаунта не включён JetStream | Добавь `-js` / блок `jetstream {}` серверу и `jetstream: enabled` (или лимиты) аккаунту |
| Кластер не собирается | Узлы не видят друг друга по `routes`, разные `cluster.name` | Проверь DNS-имена, порт 6222 и одинаковое имя кластера |
| Стрим есть на одном узле, но не на другом | JetStream не кластеризован, `replicas=1` | Создавай стримы с `--replicas 3` |
| Клиент отваливается с `slow consumer` | Подписчик не успевает читать | См. модуль 15 |
| `Authorization Violation` | В конфиге заданы пользователи, а клиент не передал учётные данные | Передай `--user`/`--password` или creds, либо (только для обучения) задай `no_auth_user` |
| `nats server list` ничего не возвращает или падает | Команде нужен системный аккаунт | Используй контекст с учётными данными системного аккаунта |

## 2.7 Где NATS хранит данные

```bash
docker exec nats-1 ls -R /data/jetstream
```

```
/data/jetstream/
└── APP/                     # имя аккаунта ($G на сервере без настроенных аккаунтов)
    └── streams/
        └── ORDERS/
            ├── msgs/
            │   ├── 1.blk    # блоки сообщений
            │   └── 2.blk
            ├── obs/         # состояние консьюмеров
            │   └── BILLING/
            └── meta.inf     # конфигурация стрима
```

Core NATS не хранит **ничего**: остановил сервер — состояние подписок исчезло. Всё, что должно пережить рестарт, живёт в `store_dir`. Монтируй его на persistent volume, иначе после перезапуска пода в Kubernetes стрим будет пустым.

## 2.8 Первый стрим и первый консьюмер

```bash
nats stream add ORDERS \
  --subjects "shop.orders.>" \
  --storage file \
  --replicas 3 \
  --retention limits \
  --max-age 168h \
  --max-msgs=-1 \
  --max-bytes 1GB \
  --discard old \
  --dupe-window 2m \
  --defaults
```

```bash
nats stream info ORDERS
nats stream ls
```

Публикуем и убеждаемся, что теперь сообщения сохраняются:

```bash
nats pub shop.orders.created '{"order_id":"order-1"}'
nats pub shop.orders.created '{"order_id":"order-2"}'
nats stream view ORDERS
```

Создаём консьюмера и читаем:

```bash
nats consumer add ORDERS BILLING \
  --pull \
  --ack explicit \
  --deliver all \
  --filter "shop.orders.created" \
  --max-deliver 5 \
  --wait 30s \
  --defaults

nats consumer next ORDERS BILLING --count 2
nats consumer info ORDERS BILLING
```

Теперь останови всё, запусти заново и прочитай снова: сообщения на месте. Это JetStream.

### Практика

1. Подними кластер из трёх узлов и проверь `nats server report jetstream`.
2. Опубликуй 10 сообщений в Core NATS без подписчика и убедись, что они нигде не появились.
3. Создай стрим `ORDERS` с `--replicas 3`, опубликуй 10 сообщений, останови лидера стрима (`nats stream info ORDERS` покажет его) и убедись, что данные доступны.
4. Найди в `curl "localhost:8222/jsz?accounts=true&streams=true"` информацию о своём стриме.

---

# Модуль 3. Subjects и wildcards: проектирование пространства имён

## 3.1 Как устроен subject

```
shop . orders . eu . created
 |       |       |     |
 t1      t2      t3    t4     -> токены, разделённые точкой
```

Ограничения:

- пустые токены запрещены (`shop..created` невалиден);
- пробелы и `.` внутри токена запрещены;
- регистр важен: `Shop.Orders` и `shop.orders` это разные subjects;
- практический предел длины около 255 байт, токенов до 16 в разумном коде;
- subject не нужно «создавать»: он существует в момент публикации.

## 3.2 Wildcards

| Символ | Что заменяет | Пример | Совпадает | Не совпадает |
|---|---|---|---|---|
| `*` | Ровно один токен | `shop.*.created` | `shop.orders.created` | `shop.orders.eu.created` |
| `>` | Один или более токенов, только в конце | `shop.orders.>` | `shop.orders.created`, `shop.orders.eu.paid` | `shop.payments.created` |

```
subject:   shop.orders.eu.created

shop.orders.eu.created   ✔ точное совпадение
shop.*.eu.created        ✔
shop.orders.*.created    ✔
shop.orders.>            ✔
shop.>                   ✔
>                        ✔ (всё подряд)
shop.*.created           ✘ токенов больше
shop.orders.eu           ✘ токенов меньше
```

Wildcards работают **только в подписке и в конфигурации стрима**. Публиковать в wildcard нельзя: `nats pub shop.orders.* ...` отправит сообщение в subject, буквально содержащий звёздочку, и никто его не получит.

## 3.3 Проектирование subject-пространства

Subject это API твоей системы. Менять его потом так же больно, как менять схему БД.

**Рабочий шаблон:**

```
<домен>.<сущность>.<событие>[.<регион|версия|id>]

shop.orders.created
shop.orders.cancelled
shop.payments.succeeded
crm.customers.updated
iot.sensors.msk.dc1.temperature
```

Правила, которые спасут через год:

| Правило | Почему |
|---|---|
| Токены от общего к частному | `shop.orders.created`, а не `created.orders.shop`: иначе wildcards бесполезны |
| Одна сущность = один префикс | Позволяет одной подпиской `shop.orders.>` собрать весь жизненный цикл |
| Версия отдельным токеном или в headers | `shop.orders.created.v2` или заголовок `Event-Version` |
| Идентификаторы в конце | `shop.orders.updated.order-123` даёт возможность подписаться на конкретный заказ |
| Не клади произвольный текст в subject | Пробелы и точки внутри id ломают маршрутизацию |
| Не используй `>` в продакшн-подписках сервисов | Сервис начнёт получать всё, что появится в будущем |
| Для команд и событий разные префиксы | `cmd.billing.charge` vs `evt.billing.charged` |

**Плохо:**

```
orders                      # нет иерархии, невозможно фильтровать
order_created_event_v1      # подчёркивания вместо токенов
shop.orders.created.2026-09-15T10:00:00Z   # время в subject плодит бесконечность subjects
```

**Хорошо:**

```
shop.orders.created
shop.orders.created.order-123
evt.shop.orders.v1.created
```

## 3.4 Cardinality: сколько subjects можно

Уникальных subjects может быть **очень много** (миллионы), это дешёвая операция для Core NATS. Но в JetStream каждый уникальный subject внутри стрима учитывается в индексе, и `max_msgs_per_subject` работает по этому индексу. Миллионы subjects в одном стриме означают большой расход памяти на метаданные.

Ориентир: `iot.sensors.<device_id>.temperature` с 100 000 устройств в одном стриме нормально. То же самое, но с меткой времени в subject — катастрофа.

## 3.5 Подписки, queue groups и wildcards вместе

```
service "billing", 3 экземпляра:
  sub "shop.orders.created", queue="billing"

service "analytics", 2 экземпляра:
  sub "shop.>", queue="analytics"
```

Результат для одного сообщения `shop.orders.created`:

- **один** из трёх экземпляров billing получит его;
- **один** из двух экземпляров analytics получит его;
- то есть каждая queue group получает копию, а внутри группы копия достаётся одному.

Это ровно то же поведение, что consumer groups в Kafka, но без партиций и без rebalance.

## 3.6 Зарезервированные subjects

| Префикс | Назначение |
|---|---|
| `$JS.API.>` | API JetStream (создание стримов, консьюмеров, pull-запросы) |
| `$JS.ACK.>` | Подтверждения сообщений |
| `$JS.EVENT.ADVISORY.>` | Advisory-события: max deliveries, terminated, потеря сообщений |
| `$SYS.>` | Системные события сервера: подключения, отключения, статистика |
| `$KV.<bucket>.>` | Key-Value Store |
| `$OBJ.<bucket>.>` | Object Store |
| `_INBOX.>` | Временные subjects для ответов в request-reply |

Не публикуй в них руками и не подписывайся на `>` в приложении: вместе с бизнес-сообщениями получишь весь служебный трафик.

### Практика

1. Подпишись на `shop.>`, `shop.*.created` и `shop.orders.>` в трёх терминалах и опубликуй `shop.orders.eu.created`. Объясни результат.
2. Спроектируй subject-пространство для сервиса доставки: заказы, курьеры, статусы, геопозиции.
3. Проверь, что произойдёт при `nats pub "shop.*.created" test`.

---

# Модуль 4. Core NATS: pub/sub, request-reply, queue groups

## 4.1 Три паттерна, на которых держится всё

```
1. PUB/SUB (fan-out)          2. QUEUE GROUP (балансировка)     3. REQUEST-REPLY (RPC)

   publisher                     publisher                         client
       |                             |                              | request
   +---+---+                     +---+---+                          v
   v   v   v                     v   v   v                      service (queue)
  s1  s2  s3                    w1  w2  w3                          |
 все получают                 получает один                        | reply
                                                                    v
                                                                 client
```

## 4.2 Pub/Sub на Go

```bash
go get github.com/nats-io/nats.go
```

```go
package main

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(
		"nats://localhost:4222,nats://localhost:4223,nats://localhost:4224",
		nats.Name("order-service"),
		nats.MaxReconnects(-1),                 // переподключаться бесконечно
		nats.ReconnectWait(500*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Println("disconnected:", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Println("reconnected to", c.ConnectedUrl())
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain() // корректное завершение: дочитать и отписаться

	// подписка
	sub, err := nc.Subscribe("shop.orders.>", func(m *nats.Msg) {
		log.Printf("subject=%s data=%s", m.Subject, string(m.Data))
	})
	if err != nil {
		log.Fatal(err)
	}
	sub.SetPendingLimits(65536, 64*1024*1024) // защита от slow consumer

	// публикация с заголовками
	msg := nats.NewMsg("shop.orders.created")
	msg.Header.Set("Event-Type", "OrderCreated")
	msg.Header.Set("Trace-Id", "4bf92f3577b34da6")
	msg.Data = []byte(`{"order_id":"order-1","amount":4990}`)

	if err := nc.PublishMsg(msg); err != nil {
		log.Fatal(err)
	}
	nc.Flush() // дождаться, что сервер принял

	time.Sleep(time.Second)
}
```

**Правила:**

- создавай **одно подключение на приложение**: оно потокобезопасно и мультиплексирует все подписки;
- `Publish()` **асинхронный** и не гарантирует доставку: он лишь пишет в буфер сокета. Хочешь подтверждение — это JetStream;
- `Flush()` убеждает, что сервер получил всё отправленное до этого момента;
- всегда используй `Drain()` вместо `Close()` при остановке: он отписывается, дообрабатывает полученное и только потом рвёт соединение;
- ошибку в `nats.ErrorHandler` обязательно логируй: там появляются `slow consumer` и `permissions violation`.

## 4.3 Pub/Sub на Python

```bash
pip install nats-py
```

```python
import asyncio, json
import nats

async def main():
    nc = await nats.connect(
        servers=["nats://localhost:4222", "nats://localhost:4223"],
        name="order-service",
        max_reconnect_attempts=-1,
        reconnect_time_wait=0.5,
    )

    async def handler(msg):
        print(msg.subject, msg.headers, msg.data.decode())

    await nc.subscribe("shop.orders.>", cb=handler)

    await nc.publish(
        "shop.orders.created",
        json.dumps({"order_id": "order-1", "amount": 4990}).encode(),
        headers={"Event-Type": "OrderCreated"},
    )
    await nc.flush()
    await asyncio.sleep(1)
    await nc.drain()

asyncio.run(main())
```

## 4.4 Pub/Sub на Java

```xml
<dependency>
  <groupId>io.nats</groupId>
  <artifactId>jnats</artifactId>
  <version>2.20.5</version>
</dependency>
```

```java
import io.nats.client.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class OrderService {
    public static void main(String[] args) throws Exception {
        Options options = new Options.Builder()
                .servers(new String[]{"nats://localhost:4222", "nats://localhost:4223"})
                .connectionName("order-service")
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(500))
                .build();

        try (Connection nc = Nats.connect(options)) {
            Dispatcher d = nc.createDispatcher(msg ->
                System.out.printf("%s -> %s%n", msg.getSubject(),
                        new String(msg.getData(), StandardCharsets.UTF_8)));
            d.subscribe("shop.orders.>");

            Headers h = new Headers();
            h.add("Event-Type", "OrderCreated");
            nc.publish(NatsMessage.builder()
                    .subject("shop.orders.created")
                    .headers(h)
                    .data("{\"order_id\":\"order-1\"}", StandardCharsets.UTF_8)
                    .build());

            nc.flush(Duration.ofSeconds(2));
            Thread.sleep(1000);
        }
    }
}
```

## 4.5 Queue groups: балансировка без конфигурации

```go
// запусти 3 экземпляра этого кода
_, _ = nc.QueueSubscribe("shop.orders.created", "billing-workers", func(m *nats.Msg) {
    process(m.Data)
})
```

```python
await nc.subscribe("shop.orders.created", queue="billing-workers", cb=handler)
```

Что важно знать:

- сообщение получает **ровно один** участник группы (выбирается сервером, обычно ближайший по топологии);
- участники могут быть на **разных серверах кластера**: NATS сначала отдаёт локальному подписчику, чтобы сэкономить hop;
- **rebalance не существует**: новый воркер просто начинает получать сообщения, упавший перестаёт;
- **порядок не гарантируется** между воркерами: два сообщения одного заказа могут обработаться параллельно;
- queue group в Core NATS **не даёт ack**: если воркер упал во время обработки, сообщение потеряно. Нужны гарантии — JetStream.

## 4.6 Request-Reply

Это то, ради чего многие приходят в NATS: RPC без service discovery, без load balancer, без DNS.

```go
// сервис
nc.QueueSubscribe("service.pricing.calculate", "pricing", func(m *nats.Msg) {
    result := calculate(m.Data)
    m.Respond(result)     // ответ уходит в m.Reply
})

// клиент
resp, err := nc.Request("service.pricing.calculate", []byte(`{"sku":"A1","qty":3}`), 2*time.Second)
if errors.Is(err, nats.ErrNoResponders) {
    // никто не слушает этот subject: сервис не поднят
}
```

Как это работает под капотом:

```
1. клиент создаёт временный inbox: _INBOX.aB9xK2.1
2. клиент подписывается на него
3. клиент публикует запрос, указав reply=_INBOX.aB9xK2.1
4. сервис обрабатывает и публикует ответ в _INBOX.aB9xK2.1
5. клиент получает ответ, отписывается
```

Преимущества перед HTTP:

- **нет адресов**: клиент не знает, где физически живёт сервис;
- **бесплатная балансировка**: несколько экземпляров в одной queue group;
- **мгновенный fail-fast**: если подписчиков нет, клиент получает `no responders` за микросекунды, а не по таймауту;
- **работает через NAT, кластеры и континенты**: соединение всегда исходящее от сервиса к NATS.

Минусы: нет схемы и типизации из коробки, нет HTTP-инфраструктуры (кэш, CDN), сложнее отлаживать без трассировки.

## 4.7 Scatter-Gather: собрать ответы от нескольких сервисов

```go
sub, _ := nc.SubscribeSync(nats.NewInbox())
defer sub.Unsubscribe()

msg := nats.NewMsg("service.quotes.request")
msg.Reply = sub.Subject
msg.Data = []byte(`{"route":"MSK-LED"}`)
nc.PublishMsg(msg)

deadline := time.Now().Add(500 * time.Millisecond)
var quotes [][]byte
for time.Now().Before(deadline) {
    m, err := sub.NextMsg(time.Until(deadline))
    if err != nil {
        break
    }
    quotes = append(quotes, m.Data)
}
```

Все подписчики без queue group отвечают, клиент собирает столько ответов, сколько успел за отведённое время. Так делают агрегаторы цен, опрос состояния всех узлов, распределённый поиск.

## 4.8 No responders и таймауты

| Ситуация | Что получит клиент |
|---|---|
| Никто не подписан на subject | Ошибка `no responders available` мгновенно (если сервер поддерживает headers) |
| Сервис подписан, но не успел ответить | Таймаут запроса |
| Сервис упал после получения запроса | Таймаут запроса |
| Несколько сервисов без queue group | Клиент возьмёт **первый** пришедший ответ, остальные проигнорирует |

Всегда задавай таймаут явно и делай его меньше, чем таймаут вышестоящего вызова.

## 4.9 Когда Core NATS достаточно, а когда нет

| Задача | Core NATS хватит? |
|---|---|
| Метрики и телеметрия раз в секунду | Да |
| Инвалидация кэша | Да |
| Health-check и heartbeat агентов | Да |
| RPC между сервисами | Да |
| Уведомление UI по WebSocket | Да |
| Событие «заказ оплачен» | **Нет**, нужен JetStream |
| Очередь задач с ретраями | **Нет**, нужен JetStream |
| Аудит и история операций | **Нет**, нужен JetStream |

### Практика

1. Запусти 3 воркера в одной queue group, отправь 100 сообщений и посчитай распределение.
2. Убей один воркер во время обработки: убедись, что сообщение потеряно.
3. Сделай сервис request-reply и измерь задержку: `nats bench --request --msgs 10000 service.echo`.
4. Воспроизведи ошибку `no responders`.

---

# Модуль 5. JetStream: стримы и хранение сообщений

## 5.1 Что такое стрим

**Stream** это хранилище сообщений, которое «захватывает» указанные subjects.

```
nats stream add ORDERS --subjects "shop.orders.>"
```

С этого момента **любое** сообщение, опубликованное в `shop.orders.*`, попадает в стрим и получает `stream sequence`. Публикатор при этом использует тот же subject: код не меняется, меняется только то, что он получает подтверждение.

```
publish shop.orders.created
   |
   v
[ Stream ORDERS ]
  seq 1: shop.orders.created  {order-1}
  seq 2: shop.orders.paid     {order-1}
  seq 3: shop.orders.created  {order-2}
   |
   +--> PubAck{stream:"ORDERS", seq:3, duplicate:false}
```

**Важное ограничение:** один subject может принадлежать **только одному стриму** в пределах аккаунта. Если создать второй стрим с пересекающимися subjects, сервер вернёт ошибку. Хочешь копию данных — используй `mirror` или `source` (модуль 13).

## 5.2 Storage: file или memory

| Тип | Плюсы | Минусы | Когда |
|---|---|---|---|
| `file` | Переживает рестарт, дёшево на объём | Задержка выше на десятки-сотни микросекунд | Значение по умолчанию для всего важного |
| `memory` | Минимальная задержка | Данные теряются при рестарте узла | Кратковременные буферы, дедупликация, тесты |

С `replicas=3` даже memory-стрим переживёт падение одного узла (данные останутся на других), но не переживёт одновременный рестарт всех.

## 5.3 Retention policy: три модели

Это ключевая настройка стрима, ошибка здесь стоит дороже всего.

| Policy | Когда сообщение удаляется | Для чего |
|---|---|---|
| `limits` (по умолчанию) | Только когда упёрлись в лимит (`max_age`, `max_msgs`, `max_bytes`) | Событийные логи, аудит, replay, несколько независимых потребителей |
| `interest` | Как только **все существующие консьюмеры** подтвердили | Pub/sub с гарантиями, когда история не нужна |
| `workqueue` | Как только **один** консьюмер подтвердил | Очередь задач: сообщение обрабатывается ровно одним потребителем |

```
LIMITS                       INTEREST                    WORKQUEUE
[1][2][3][4][5]              [1][2][3]                   [1][2][3]
 ^      ^                     ^                           ^
 A      B  читают             все ack -> удалить          один ack -> удалить
 независимо, история          (без консьюмеров            (несколько консьюмеров
 живёт до max_age             сообщения удаляются сразу)   должны иметь непересекающиеся фильтры)
```

**Ловушки:**

- `interest`: если консьюмеров нет вообще, сообщения удаляются **мгновенно** после публикации. Сначала создай консьюмера, потом публикуй.
- `workqueue`: в стриме не может быть двух консьюмеров с пересекающимися `filter_subject`. Сервер откажет при создании второго.
- `limits`: сообщения живут даже после обработки, и диск закончится, если не задан `max_age` или `max_bytes`.

## 5.4 Полная конфигурация стрима

```bash
nats stream add ORDERS \
  --subjects "shop.orders.>" \
  --storage file \
  --replicas 3 \
  --retention limits \
  --discard old \
  --max-age 720h \
  --max-msgs -1 \
  --max-msgs-per-subject -1 \
  --max-bytes 50GB \
  --max-msg-size 1MB \
  --dupe-window 2m \
  --allow-rollup \
  --deny-delete \
  --no-allow-purge=false
```

| Параметр | Смысл | Практика |
|---|---|---|
| `subjects` | Какие subjects захватывает стрим | Держи узко: `shop.orders.>`, не `shop.>` |
| `storage` | `file` или `memory` | `file` |
| `replicas` | 1, 3 или 5, RAFT-группа | 3 в продакшене |
| `retention` | `limits` / `interest` / `workqueue` | Подумай 5 минут перед выбором |
| `discard` | `old` (удалить старое) или `new` (отказать публикатору) | `old` для логов, `new` для очередей задач с жёстким лимитом |
| `max_age` | TTL сообщения | Всегда задавай |
| `max_bytes` | Лимит размера | Всегда задавай, иначе диск кончится |
| `max_msgs_per_subject` | Сколько версий каждого subject хранить | `1` превращает стрим в KV-подобный снапшот |
| `duplicate_window` | Окно дедупликации по `Nats-Msg-Id` | 2 минуты обычно достаточно |
| `allow_rollup` | Разрешить заголовок `Nats-Rollup` (схлопнуть историю subject) | Для снапшотов |
| `deny_delete` / `deny_purge` | Запретить удаление сообщений через API | Для аудита и compliance |

## 5.5 Публикация в JetStream на Go

Современный API — пакет `jetstream`:

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// создать или обновить стрим прямо из кода
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        "ORDERS",
		Subjects:    []string{"shop.orders.>"},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		Replicas:    3,
		MaxAge:      30 * 24 * time.Hour,
		MaxBytes:    50 << 30,
		Duplicates:  2 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}

	// синхронная публикация с подтверждением
	msg := &nats.Msg{
		Subject: "shop.orders.created",
		Header:  nats.Header{"Nats-Msg-Id": []string{"order-1-created"}},
		Data:    []byte(`{"order_id":"order-1","amount":4990}`),
	}
	ack, err := js.PublishMsg(ctx, msg)
	if err != nil {
		log.Fatal("publish failed:", err) // сообщение НЕ сохранено, нужен ретрай
	}
	log.Printf("stream=%s seq=%d duplicate=%v", ack.Stream, ack.Sequence, ack.Duplicate)
}
```

**Асинхронная публикация** для высокой пропускной способности:

```go
futures := make([]jetstream.PubAckFuture, 0, 1000)
for i := 0; i < 1000; i++ {
	f, err := js.PublishAsync("shop.orders.created", payload(i))
	if err != nil {
		log.Fatal(err)
	}
	futures = append(futures, f)
}

select {
case <-js.PublishAsyncComplete():
case <-time.After(10 * time.Second):
	log.Fatal("не дождались подтверждений")
}

for _, f := range futures {
	select {
	case <-f.Ok():
	case err := <-f.Err():
		log.Println("не сохранено:", err) // обязательно обработай
	default:
	}
}
```

Асинхронная публикация даёт кратный прирост throughput, но **каждую ошибку надо разбирать**: молча потерянный ack это молча потерянное событие.

## 5.6 Публикация в JetStream на Python и Java

**Python:**

```python
import asyncio, json, nats

async def main():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    await js.add_stream(name="ORDERS", subjects=["shop.orders.>"],
                        storage="file", num_replicas=3, duplicate_window=120)

    ack = await js.publish(
        "shop.orders.created",
        json.dumps({"order_id": "order-1"}).encode(),
        headers={"Nats-Msg-Id": "order-1-created"},
    )
    print(ack.stream, ack.seq, ack.duplicate)
    await nc.drain()

asyncio.run(main())
```

**Java:**

```java
Connection nc = Nats.connect("nats://localhost:4222");
JetStream js = nc.jetStream();

Headers h = new Headers();
h.add("Nats-Msg-Id", "order-1-created");

PublishAck ack = js.publish(NatsMessage.builder()
        .subject("shop.orders.created")
        .headers(h)
        .data("{\"order_id\":\"order-1\"}", StandardCharsets.UTF_8)
        .build());

System.out.println(ack.getStream() + " seq=" + ack.getSeqno() + " dup=" + ack.isDuplicate());
```

## 5.7 Что делать, если публикация упала

```
js.Publish() вернул ошибку
   |
   +-- timeout / no responders  -> стрим недоступен или нет лидера (в том числе сразу
   |                               после создания реплицированного стрима): ретрай с backoff
   +-- maximum messages exceeded -> упёрлись в лимит с discard=new: алерт, разбор
   +-- wrong last sequence      -> нарушено оптимистичное условие ExpectedLastSeq
   +-- no stream matches subject -> опечатка в subject или стрим не создан
```

Правило продакшена: **ретрай с экспоненциальным backoff плюс обязательный `Nats-Msg-Id`**. Тогда повтор не создаст дубликат (см. модуль 7). Если после нескольких попыток не вышло, событие складывают в локальный outbox в БД и досылают фоном.

## 5.8 Чтение и управление стримом из CLI

```bash
nats stream info ORDERS
nats stream view ORDERS                      # листать сообщения
nats stream get ORDERS 42                    # получить сообщение по seq
nats stream subjects ORDERS                  # какие subjects реально есть и сколько в них
nats stream report                           # сводка по всем стримам
nats stream edit ORDERS --max-age 48h
nats stream purge ORDERS --subject "shop.orders.created"
nats stream rm ORDERS
nats stream backup ORDERS ./orders-backup    # снапшот
nats stream restore ./orders-backup
```

### Вопросы для самопроверки

1. Почему один subject не может принадлежать двум стримам?
2. В чём разница между `interest` и `workqueue` retention?
3. Что произойдёт при `discard=new` и достижении `max_bytes`?
4. Почему асинхронная публикация опаснее синхронной?

---

# Модуль 6. Консьюмеры JetStream: pull, push, ack

## 6.1 Что такое консьюмер

**Consumer** это не «подписчик», а **объект на сервере**: курсор по стриму плюс правила доставки и подтверждения. Он живёт на сервере даже когда приложение выключено.

```
Stream ORDERS
  seq: 1  2  3  4  5  6  7  8
        ^        ^        ^
        |        |        |
   ack_floor  delivered  last_seq
        |________|
        num_ack_pending = 2       num_pending = 2  (это lag)
```

| Тип | Как создаётся | Живёт |
|---|---|---|
| **Durable** | С именем (`--durable BILLING`) | Пока не удалишь явно |
| **Ephemeral** | Без имени | Пока подключён клиент + `inactive_threshold` |

В продакшене почти всегда durable: перезапуск пода не должен обнулять прогресс.

## 6.2 Pull vs Push

| | **Pull (рекомендуется)** | **Push (legacy)** |
|---|---|---|
| Кто инициирует | Клиент запрашивает N сообщений | Сервер сам шлёт в subject |
| Flow control | Естественный: не просишь — не получаешь | Нужны `flow_control` и `idle_heartbeat` |
| Масштабирование | Просто добавь ещё экземпляр, они делят один консьюмер | Нужна queue group и `deliver_group` |
| Отказоустойчивость | Клиент упал — ничего не теряется | Сообщения летят в никуда |
| Когда использовать | Практически всегда | Legacy-код, специфичные сценарии |

**Pull-консьюмеры — рекомендуемый вариант по умолчанию**, и современные клиентские API (пакеты `jetstream`) построены вокруг них. Все примеры ниже на них.

## 6.3 Ack policy и что означает каждый ответ

| Ack policy | Смысл |
|---|---|
| `explicit` (по умолчанию и правильный выбор) | Каждое сообщение подтверждается отдельно |
| `all` | Ack на сообщение N подтверждает все до N включительно |
| `none` | Подтверждения не нужны, сервер считает доставленное обработанным |

Что клиент может ответить на сообщение:

| Ответ | Что делает | Когда |
|---|---|---|
| `Ack()` | Обработано, больше не доставлять | Успех |
| `Nak()` / `NakWithDelay(d)` | Вернуть в очередь и доставить снова (сразу или через d) | Временная ошибка: БД недоступна, внешний API отвечает 503 |
| `Term()` / `TermWithReason()` | Больше никогда не доставлять | Сообщение невалидно, обработать невозможно в принципе |
| `InProgress()` | «Я ещё работаю», сбросить таймер `AckWait` | Долгая обработка |
| ничего | Через `AckWait` сообщение доставят заново | Падение процесса |

**Это важнее, чем кажется.** Разница между `Nak` и `Term` — разница между «попробуем ещё» и «мусор, выкинь». Приложение, которое на любую ошибку делает `Nak`, будет вечно крутить ядовитое сообщение.

## 6.4 Основные параметры консьюмера

```bash
nats consumer add ORDERS BILLING \
  --pull \
  --ack explicit \
  --deliver all \
  --filter "shop.orders.created" \
  --max-deliver 6 \
  --wait 30s \
  --max-pending 1000 \
  --backoff linear \
  --backoff-steps 5 \
  --backoff-min 1s \
  --backoff-max 1m \
  --replicas 3 \
  --defaults
```

| Параметр | По умолчанию | Смысл |
|---|---|---|
| `filter_subject(s)` | все subjects стрима | Что читает этот консьюмер; с 2.10 можно несколько фильтров |
| `deliver_policy` | `all` | `all`, `last`, `new`, `by_start_sequence`, `by_start_time`, `last_per_subject` |
| `ack_wait` | 30s | Сколько сервер ждёт ack перед повторной доставкой |
| `max_deliver` | -1 (бесконечно) | Сколько раз пытаться доставить |
| `max_ack_pending` | 1000 | Сколько неподтверждённых сообщений можно выдать: это **и есть** ограничитель параллелизма |
| `backoff` | нет | Массив задержек для повторных доставок |
| `replicas` | как у стрима | Реплики состояния консьюмера |
| `inactive_threshold` | 5s (ephemeral) | Когда удалить неиспользуемого консьюмера |

**`max_ack_pending` — самый недооценённый параметр.** Он же защита от перегрузки: если воркеры не успевают, сервер перестаёт выдавать новые сообщения.

**Два правила backoff, на которых часто спотыкаются:**

- если задан `backoff`, он **заменяет `ack_wait` для повторных доставок**: первая доставка ждёт `ack_wait`, повтор N ждёт `backoff[N-1]` (если попыток больше, чем элементов, повторяется последнее значение);
- `max_deliver` не может быть **меньше** числа элементов backoff (сервер отклонит конфиг). Сделай его на один больше, чтобы последняя задержка реально использовалась, прежде чем консьюмер сдастся.

С флагами CLI выше (`linear`, 5 шагов, от 1s до 1m) natscli вычисляет равномерные задержки 1s, ~13s, ~25s, ~36s, ~48s: последний шаг остаётся ниже максимума. Проверить результат можно через `nats consumer info ORDERS BILLING`. Для своего расписания вроде 1s, 5s, 30s задай массив явно в коде (6.5) или в JSON-конфиге (`nats consumer add ORDERS --config billing.json`).

## 6.5 Pull-консьюмер на Go

```go
cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
	Durable:       "BILLING",
	FilterSubject: "shop.orders.created",
	AckPolicy:     jetstream.AckExplicitPolicy,
	DeliverPolicy: jetstream.DeliverAllPolicy,
	MaxDeliver:    5,
	AckWait:       30 * time.Second,
	MaxAckPending: 500,
	BackOff:       []time.Duration{time.Second, 5 * time.Second, 30 * time.Second},
})
if err != nil {
	log.Fatal(err)
}

// вариант 1: колбэк (обычно то, что нужно)
cc, err := cons.Consume(func(msg jetstream.Msg) {
	if err := process(msg.Data()); err != nil {
		if isTemporary(err) {
			msg.NakWithDelay(5 * time.Second)
		} else {
			msg.Term() // не повторять никогда
		}
		return
	}
	msg.Ack()
}, jetstream.PullMaxMessages(128))
if err != nil {
	log.Fatal(err)
}
defer cc.Stop()

// вариант 2: батчами вручную
batch, err := cons.Fetch(100, jetstream.FetchMaxWait(5*time.Second))
for msg := range batch.Messages() {
	process(msg.Data())
	msg.Ack()
}
if err := batch.Error(); err != nil {
	log.Println(err)
}
```

Метаданные сообщения:

```go
meta, _ := msg.Metadata()
log.Printf("stream_seq=%d consumer_seq=%d deliveries=%d pending=%d ts=%s",
	meta.Sequence.Stream, meta.Sequence.Consumer,
	meta.NumDelivered, meta.NumPending, meta.Timestamp)
```

`meta.NumDelivered > 1` означает, что это повторная доставка. Логируй это: рост повторов — первый признак проблем.

## 6.6 Pull-консьюмер на Python и Java

**Python:**

```python
js = nc.jetstream()

sub = await js.pull_subscribe(
    "shop.orders.created",
    durable="BILLING",
    stream="ORDERS",
)

while True:
    try:
        msgs = await sub.fetch(batch=50, timeout=5)
    except asyncio.TimeoutError:   # сообщений нет; ловит и подкласс nats.errors.TimeoutError
        continue

    for msg in msgs:
        try:
            await process(msg.data)
            await msg.ack()
        except TemporaryError:
            await msg.nak(delay=5)
        except Exception:
            await msg.term()
```

**Java:**

```java
JetStreamManagement jsm = nc.jetStreamManagement();
jsm.addOrUpdateConsumer("ORDERS", ConsumerConfiguration.builder()
        .durable("BILLING")
        .filterSubject("shop.orders.created")
        .ackPolicy(AckPolicy.Explicit)
        .maxDeliver(5)
        .ackWait(Duration.ofSeconds(30))
        .maxAckPending(500)
        .build());

ConsumerContext cc = nc.getStreamContext("ORDERS").getConsumerContext("BILLING");
try (MessageConsumer consumer = cc.consume(msg -> {
        try {
            process(msg.getData());
            msg.ack();
        } catch (TemporaryException e) {
            msg.nakWithDelay(Duration.ofSeconds(5));
        } catch (Exception e) {
            msg.term();
        }
    })) {
    Thread.sleep(Long.MAX_VALUE);
}
```

## 6.7 Масштабирование: несколько воркеров на один консьюмер

```
       Stream ORDERS
            |
      consumer BILLING (durable, pull)
       /      |      \
  worker-1 worker-2 worker-3
```

Все три процесса подключаются к **одному и тому же durable-консьюмеру** и тянут сообщения. Сервер сам следит, чтобы одно сообщение не ушло двоим одновременно (пока не истёк `AckWait`).

Отличия от Kafka, которые важно понять:

- **нет партиций и нет rebalance**: добавляй и убирай воркеров когда угодно;
- **параллелизм не ограничен числом партиций**: 100 воркеров на один консьюмер это нормально;
- **порядок между воркерами не гарантируется**: если нужен строгий порядок по сущности, сделай отдельного консьюмера с `filter_subject` на конкретный ключ или обрабатывай последовательно (`max_ack_pending=1`).

## 6.8 Порядок сообщений в NATS

| Что нужно | Как добиться |
|---|---|
| Глобальный порядок стрима | Один консьюмер, один воркер, `max_ack_pending=1` |
| Порядок по сущности (заказу, счёту) | Отдельный subject на сущность + консьюмер с фильтром, либо шардирование воркеров по subject-токену |
| Порядок не важен | Много воркеров, максимальный throughput |

Пример шардирования: subjects `shop.orders.shard-0.…` … `shop.orders.shard-7.…`, publisher считает `hash(order_id) % 8`, а каждый воркер создаёт консьюмера с фильтром на свой шард. Это ручной аналог партиций Kafka.

## 6.9 Ordered consumer

Особый режим для чтения стрима «как файла»: клиентская библиотека сама создаёт ephemeral-консьюмера, отслеживает пропуски по sequence и при разрыве автоматически пересоздаёт его с нужной позиции.

```go
cons, _ := js.OrderedConsumer(ctx, "ORDERS", jetstream.OrderedConsumerConfig{
	FilterSubjects: []string{"shop.orders.>"},
})
```

Ack не нужен, гарантирован строгий порядок. Идеально для построения проекций, материализованных представлений и кэшей в памяти.

## 6.10 Мониторинг консьюмера

```bash
nats consumer info ORDERS BILLING
nats consumer report ORDERS
nats consumer next ORDERS BILLING --count 1     # вручную вытянуть одно сообщение
nats consumer rm ORDERS BILLING
```

Что смотреть:

```
Unprocessed Messages: 4 023        <- num_pending, это lag
Outstanding Acks:       417        <- num_ack_pending, упирается в max_ack_pending?
Redelivered Messages:    38        <- растёт? значит обработка падает или AckWait мал
Acknowledgment Floor: seq 120 455  <- не двигается? консьюмер застрял
```

## 6.11 Изменение позиции консьюмера

Отдельной команды «сбросить offset» нет: у консьюмера позиция задаётся при создании. Перечитать стрим заново значит пересоздать консьюмера:

```bash
nats consumer rm ORDERS BILLING -f
nats consumer add ORDERS BILLING --pull --ack explicit \
  --start-time "2026-09-15T00:00:00Z" --filter "shop.orders.created" --defaults
```

Варианты старта: `--deliver all`, `--deliver last`, `--deliver new`, `--deliver last_per_subject`, `--start-sequence 1000`, `--start-time ...`.

### Практика

1. Создай консьюмера с `--wait 5s` и не подтверждай сообщение: увидишь повторную доставку.
2. Поставь `--max-deliver 3` и посмотри, что происходит после третьей попытки (`nats consumer info` и advisory-сообщения).
3. Запусти 3 воркера на одном durable-консьюмере и убей одного во время обработки: убедись, что сообщение переедет другому.
4. Сравни `max_ack_pending=1` и `max_ack_pending=1000` по throughput.

---

# Модуль 7. Гарантии доставки, дедупликация и exactly-once

## 7.1 Где появляются дубли и потери

| Сценарий | Результат | Защита |
|---|---|---|
| Core NATS, подписчик не запущен | Потеря | JetStream |
| Core NATS, подписчик упал в обработке | Потеря | JetStream + ack |
| Publisher не получил PubAck и повторил | Дубль в стриме | `Nats-Msg-Id` + `duplicate_window` |
| Приложение упало между записью в БД и публикацией | Потеря события | Transactional outbox |
| Приложение опубликовало, а транзакция БД откатилась | «Фантомное» событие | Transactional outbox |
| Консьюмер обработал, но упал до `Ack()` | Повторная обработка | Идемпотентность |
| Консьюмер обработал дольше `AckWait` | Параллельная повторная доставка | `InProgress()` или больший `AckWait` |
| `replicas=1` и упал узел | Потеря стрима | `replicas=3` |
| Отключение питания до fsync (`replicas=1`) | Теряются сообщения, на которые уже пришёл PubAck | `replicas=3` (кворум держит данные в памяти других узлов) или `sync_interval: always` (см. 15.4) |

## 7.2 Три семантики

```
AT-MOST-ONCE  (Core NATS)
  publish -> доставка подписчикам -> забыли
  никто не получил -> сообщение исчезло

AT-LEAST-ONCE (JetStream, база)
  publish -> PubAck -> доставка -> обработка -> Ack
  упали до Ack -> сообщение доставят снова

EXACTLY-ONCE (JetStream + Nats-Msg-Id + идемпотентный консьюмер)
  дедупликация на публикации + идемпотентность на обработке
```

**NATS честно называет свой exactly-once «exactly-once semantics»**: это комбинация дедупликации публикации и идемпотентности потребителя, а не магическая распределённая транзакция. Такой же честный ответ даёт и Kafka.

## 7.3 Дедупликация публикации: Nats-Msg-Id

Стрим помнит идентификаторы сообщений в течение `duplicate_window`. Повторная публикация с тем же `Nats-Msg-Id` не создаст второе сообщение, а вернёт `PubAck{duplicate: true}`.

```go
msg := nats.NewMsg("shop.orders.created")
msg.Header.Set("Nats-Msg-Id", eventID) // UUID события, НЕ случайный на каждую попытку
msg.Data = payload

ack, err := js.PublishMsg(ctx, msg)
if err == nil && ack.Duplicate {
	log.Println("это был повтор, сообщение уже в стриме")
}
```

Правила:

- `Nats-Msg-Id` должен быть **стабильным для одного бизнес-события**: генерируй его один раз и сохраняй, а не при каждой попытке отправки;
- `duplicate_window` должен быть **больше, чем окно ретраев** (обычно 2-5 минут);
- окно дедупликации стоит памяти: сервер держит идентификаторы в RAM;
- дедупликация работает **в пределах одного стрима**.

```bash
nats stream edit ORDERS --dupe-window 5m
nats stream info ORDERS   # секция "Duplicate Window"
```

## 7.4 Оптимистичный контроль версий при публикации

JetStream умеет проверять условия на момент записи. Это даёт CAS-семантику без внешних блокировок.

| Заголовок | Проверка |
|---|---|
| `Nats-Expected-Stream` | Публикуем именно в этот стрим |
| `Nats-Expected-Last-Sequence` | Последний seq стрима равен указанному |
| `Nats-Expected-Last-Subject-Sequence` | Последний seq **по этому subject** равен указанному |
| `Nats-Expected-Last-Msg-Id` | Последний `Nats-Msg-Id` равен указанному |

```go
opts := []jetstream.PublishOpt{
	jetstream.WithExpectLastSequencePerSubject(lastKnownSeq),
}
ack, err := js.Publish(ctx, "shop.orders.updated.order-123", payload, opts...)
// err == "wrong last sequence" -> кто-то успел записать раньше, перечитай состояние
```

Так строят event sourcing с проверкой версии агрегата: если между чтением и записью кто-то изменил состояние, публикация отклоняется.

## 7.5 Идемпотентный консьюмер

Самый надёжный способ получить «эффективно ровно один раз» при записи во внешнюю БД:

```sql
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```sql
BEGIN;
INSERT INTO processed_events (event_id) VALUES ($1)
  ON CONFLICT (event_id) DO NOTHING;
-- если вставлено 0 строк -> событие уже обработано: COMMIT и Ack()
UPDATE accounts SET balance = balance - $2 WHERE id = $3;
COMMIT;
-- и только после успешного COMMIT делаем msg.Ack()
```

Альтернативы: `UPSERT` по естественному ключу, условное обновление по версии (`WHERE version = $expected`), хранение `stream_sequence` в той же таблице, что и результат.

**Порядок операций критичен:** сначала обработка и коммит в БД, потом `Ack()`. Обратный порядок превращает at-least-once в at-most-once.

## 7.6 Double-ack: подтверждение подтверждения

Обычный `Ack()` это fire-and-forget: клиент не знает, дошло ли подтверждение до сервера. Если оно потерялось, сообщение доставят снова.

```go
msg.DoubleAck(ctx)   // ждёт ответа сервера о том, что ack принят
```

Стоит дороже (дополнительный round-trip), зато убирает целый класс лишних повторов. Используй там, где повторная обработка дорога, а идемпотентность неполная.

## 7.7 Transactional outbox

**Проблема двойной записи:**

```
1. INSERT INTO orders ...       OK
2. js.Publish(OrderCreated) -> сервис упал
   -> заказ есть, события нет, остальные системы о нём не узнали
```

**Решение: outbox-таблица в той же транзакции БД.**

```
+--------------- одна транзакция БД ----------------+
| INSERT INTO orders (...)                          |
| INSERT INTO outbox (id, subject, headers, payload)|
+---------------------------------------------------+
               |
               v
      outbox-relay (отдельная горутина или сервис)
      читает необработанные строки и публикует в NATS
      с Nats-Msg-Id = outbox.id
               |
               v
        Stream ORDERS
```

```sql
CREATE TABLE outbox (
    id           UUID PRIMARY KEY,
    subject      TEXT NOT NULL,
    headers      JSONB,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX ON outbox (created_at) WHERE published_at IS NULL;
```

Relay может опубликовать событие повторно (например, упал между `Publish` и `UPDATE outbox SET published_at`), поэтому `Nats-Msg-Id = outbox.id` обязателен: дедупликация стрима уберёт дубль.

## 7.8 Итог: какую гарантию выбрать

| Задача | Рекомендация |
|---|---|
| Метрики, телеметрия, heartbeat | Core NATS, потери допустимы |
| RPC между сервисами | Core NATS request-reply + таймауты и ретраи |
| Уведомления пользователю | JetStream, at-least-once, дубль не страшен |
| Бизнес-события домена | JetStream + `Nats-Msg-Id` + outbox + идемпотентный консьюмер |
| Деньги и списания | Всё вышеперечисленное + `processed_events` + double-ack |
| Очередь задач | `workqueue`-стрим + `max_deliver` + DLQ |

### Вопросы для самопроверки

1. Почему exactly-once в NATS это две разные вещи, а не одна настройка?
2. Что произойдёт, если `duplicate_window` меньше окна ретраев публикатора?
3. Зачем нужен `Nats-Expected-Last-Subject-Sequence`?
4. Почему `Ack()` нужно вызывать после коммита в БД, а не до?
5. Какую проблему решает transactional outbox и почему он не решает её полностью без дедупликации?

---

# Модуль 8. Retention, лимиты и работа с диском

## 8.1 Как JetStream хранит сообщения

```
/data/jetstream/APP/streams/ORDERS/
├── msgs/
│   ├── 1.blk       # блок сообщений (по умолчанию несколько МБ)
│   ├── 2.blk
│   └── index.db
├── obs/            # состояние консьюмеров
│   └── BILLING/
└── meta.inf        # конфигурация стрима
```

- сообщения пишутся **последовательно** в блоки, старые блоки удаляются целиком;
- индекс по subject держится в памяти: поэтому число уникальных subjects влияет на RAM;
- файловый стрим использует page cache ОС, как и большинство лог-систем;
- при `replicas=3` запись подтверждается после того, как её приняло **большинство** RAFT-группы (2 из 3);
- по умолчанию данные сбрасываются на диск (fsync) раз в 2 минуты, а не при каждой записи. PubAck означает «принято кворумом», а не «лежит на диске»: при `replicas=1` отключение питания может унести уже подтверждённые сообщения (подробнее в 15.4).

## 8.2 Лимиты стрима

| Лимит | Что ограничивает | Что делать при достижении |
|---|---|---|
| `max_msgs` | Число сообщений в стриме | `discard` |
| `max_bytes` | Размер стрима на диске (учитывая репликацию — умножь на replicas) | `discard` |
| `max_age` | Возраст сообщения | Удаляется автоматически |
| `max_msgs_per_subject` | Сколько сообщений на каждый уникальный subject | Удаляется самое старое по этому subject |
| `max_msg_size` | Размер одного сообщения | Публикация отклоняется |
| `max_consumers` | Число консьюмеров | Создание отклоняется |

**`discard` policy:**

```
discard = old  : приехало новое -> удаляем самое старое (кольцевой буфер)
discard = new  : приехало новое -> отклоняем публикацию с ошибкой
```

Для логов и телеметрии `old`. Для очереди задач, где терять задачи нельзя, `new` плюс алерт: лучше отказать публикатору, чем молча потерять работу.

`--discard-per-subject` (с 2.10) применяет `discard=new` в пределах subject: удобно для стримов, где каждый subject это отдельная сущность.

## 8.3 Лимиты аккаунта и сервера

Лимиты стрима не спасут, если один тенант съест весь диск. Настраивай лимиты уровнем выше:

```hcl
jetstream {
  store_dir: "/data/jetstream"
  max_memory_store: 4GB      # суммарно на сервер
  max_file_store: 500GB
}

accounts {
  SHOP: {
    jetstream: {
      max_memory: 1GB
      max_file: 100GB
      max_streams: 50
      max_consumers: 500
    }
    users: [ {user: shop, password: "…"} ]
  }
}
```

```bash
nats account info          # сколько занято и сколько осталось
nats server report jetstream
```

## 8.4 max_msgs_per_subject: снапшот вместо истории

```bash
nats stream add PROFILES \
  --subjects "profiles.*" \
  --storage file --replicas 3 \
  --max-msgs-per-subject 1 \
  --retention limits --defaults
```

Теперь стрим хранит **последнее состояние каждого профиля** и ничего больше:

```
publish profiles.user-1  {"name":"Ann"}
publish profiles.user-2  {"name":"Bob"}
publish profiles.user-1  {"name":"Anna"}   <- предыдущее сообщение по profiles.user-1 удаляется

в стриме: profiles.user-1 -> Anna, profiles.user-2 -> Bob
```

Это аналог log compaction в Kafka и ровно то, на чём построен KV Store. Читать такой стрим удобно консьюмером с `--deliver last_per_subject`: сервис на старте мгновенно получает полное состояние, а дальше только изменения.

## 8.5 Rollup: схлопнуть историю вручную

С `--allow-rollup` можно опубликовать сообщение с заголовком:

| Заголовок | Эффект |
|---|---|
| `Nats-Rollup: sub` | Удалить всю предыдущую историю **этого subject**, оставив только новое сообщение |
| `Nats-Rollup: all` | Очистить **весь стрим**, оставив только новое сообщение |

Применение: периодические снапшоты агрегата, чтобы не хранить миллион мелких событий.

## 8.6 Удаление сообщений

```bash
nats stream purge ORDERS                                   # всё
nats stream purge ORDERS --subject "shop.orders.created"   # по subject
nats stream purge ORDERS --keep 1000                       # оставить последние 1000
nats stream purge ORDERS --seq 5000                        # удалить всё до seq
nats stream rmm ORDERS 42                                  # удалить одно сообщение (GDPR)
```

Для стримов с требованиями compliance ставь `--deny-delete` и `--deny-purge`, чтобы никто не удалил аудит одной командой.

## 8.7 Backup и restore

```bash
nats stream backup ORDERS ./backups/orders-2026-09-16
nats stream restore ./backups/orders-2026-09-16
```

Бэкап снимается **с живого стрима** и включает конфигурацию, сообщения и состояние консьюмеров. Для больших стримов это тяжёлая операция: делай её с реплики и в окно низкой нагрузки. Отдельно храни `nats stream info --json` всех стримов: это ваш «инфраструктурный код», который стоит держать в Git.

## 8.8 Сколько диска и памяти нужно

```
disk = средний_размер_сообщения × сообщений_в_сутки × дней_хранения × replicas × 1.3

пример: 2 КБ × 20 000 000 × 7 × 3 × 1.3 ≈ 1.1 ТБ
```

Память:

- индекс subjects: примерно 100-200 байт на уникальный subject в стриме;
- окно дедупликации: размер `Nats-Msg-Id` × число сообщений за `duplicate_window`;
- состояние консьюмеров: пропорционально `max_ack_pending` и числу неподтверждённых;
- плюс page cache: оставь ОС хотя бы половину RAM.

Практический ориентир для узла: 8-16 ГБ RAM, NVMe-диск, отдельный от системного. JetStream очень чувствителен к задержкам fsync: сетевые диски и особенно EBS gp2 дают неприятные сюрпризы.

### Вопросы для самопроверки

1. Чем `discard=old` отличается от `discard=new` и когда какой брать?
2. Как превратить стрим в хранилище последнего состояния по ключу?
3. Почему число уникальных subjects влияет на потребление памяти?
4. Почему `max_bytes` нужно умножать на число реплик при расчёте диска?

---

# Модуль 9. Key-Value Store

## 9.1 Что это и зачем

KV Store это надстройка над стримом с `max_msgs_per_subject`. Никакой отдельной базы: бакет `CONFIG` это стрим `KV_CONFIG` с subjects `$KV.CONFIG.>`.

```
nats kv put CONFIG feature.dark_mode true
       |
       v
publish $KV.CONFIG.feature.dark_mode  "true"  -> stream KV_CONFIG
```

Что даёт:

- `get`, `put`, `delete`, атомарные `create` и `update` с версией;
- **историю значений** (`--history N`);
- **watch**: подписка на изменения ключа или префикса в реальном времени;
- **TTL** на бакет, а с 2.11 и на отдельные ключи;
- репликацию и отказоустойчивость от JetStream.

Типичные применения: динамическая конфигурация, feature flags, кэш, распределённые блокировки, leader election, реестр сервисов, сессии.

## 9.2 CLI

```bash
nats kv add CONFIG --history 5 --replicas 3 --storage file --ttl 0
nats kv put CONFIG feature.dark_mode true
nats kv get CONFIG feature.dark_mode
nats kv ls CONFIG
nats kv history CONFIG feature.dark_mode
nats kv watch CONFIG                     # следить за всеми изменениями
nats kv del CONFIG feature.dark_mode
nats kv rm CONFIG                        # удалить бакет
nats kv info CONFIG
```

## 9.3 KV на Go

```go
kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
	Bucket:   "CONFIG",
	History:  5,
	Replicas: 3,
	Storage:  jetstream.FileStorage,
	TTL:      0,
})

// запись
rev, err := kv.Put(ctx, "feature.dark_mode", []byte("true"))

// чтение
entry, err := kv.Get(ctx, "feature.dark_mode")
if errors.Is(err, jetstream.ErrKeyNotFound) { /* … */ }
log.Println(string(entry.Value()), entry.Revision(), entry.Created())

// атомарное создание: ошибка, если ключ уже есть
_, err = kv.Create(ctx, "lock.import-job", []byte("worker-1"))

// оптимистичное обновление: ошибка, если версия изменилась
_, err = kv.Update(ctx, "feature.dark_mode", []byte("false"), entry.Revision())

// watch: конфиг приезжает сам
w, _ := kv.Watch(ctx, "feature.>")
defer w.Stop()
for e := range w.Updates() {
	if e == nil { // означает "начальное состояние передано полностью"
		continue
	}
	log.Printf("%s = %s (rev %d, op %v)", e.Key(), e.Value(), e.Revision(), e.Operation())
}
```

## 9.4 KV на Python

```python
kv = await js.create_key_value(bucket="CONFIG", history=5, replicas=3)

await kv.put("feature.dark_mode", b"true")
entry = await kv.get("feature.dark_mode")
print(entry.value, entry.revision)

watcher = await kv.watch("feature.>")
async for e in watcher:
    if e is None:
        continue
    print(e.key, e.value, e.operation)
```

## 9.5 Распределённая блокировка на KV

```go
// захват
_, err := kv.Create(ctx, "lock.nightly-report", []byte(instanceID))
if errors.Is(err, jetstream.ErrKeyExists) {
	return ErrAlreadyRunning   // блокировка занята
}
defer kv.Delete(ctx, "lock.nightly-report")

// продление, чтобы блокировка не протухла, если бакет создан с TTL
ticker := time.NewTicker(10 * time.Second)
for range ticker.C {
	entry, err := kv.Get(ctx, "lock.nightly-report")
	if err != nil || string(entry.Value()) != instanceID {
		return ErrLockLost
	}
	kv.Update(ctx, "lock.nightly-report", []byte(instanceID), entry.Revision())
}
```

Бакет с TTL обязателен: иначе упавший процесс оставит блокировку навсегда. Это не Chubby и не etcd с fencing tokens, но для 95% задач вроде «пусть ночной отчёт считает только один под» этого достаточно.

## 9.6 Ограничения KV

- значение это байты до `max_payload` (по умолчанию 1 МБ): большие объекты в Object Store;
- ключи подчиняются правилам subjects: точка в ключе создаёт иерархию, пробелы запрещены;
- нет запросов «найди по значению», нет вторичных индексов, нет транзакций между ключами;
- `History` больше 64 не поддерживается;
- удаление оставляет маркер (`purge` убирает его полностью).

### Практика

1. Сделай сервис, который берёт конфиг из KV и применяет изменения через watch без рестарта.
2. Реализуй leader election: победитель тот, кто смог `Create` ключ; остальные ждут и следят через watch.
3. Посмотри, как KV выглядит изнутри: `nats stream info KV_CONFIG`.

---

# Модуль 10. Object Store

## 10.1 Зачем

Сообщения ограничены `max_payload`. Object Store разбивает файл на чанки, складывает их в стрим `OBJ_<bucket>` и собирает обратно при чтении. Это даёт «S3 на минималках» там, где поднимать MinIO избыточно.

Применения: артефакты сборки, конфиги и сертификаты для edge-узлов, прошивки для IoT, отчёты, вложения, модели ML для инференса на краю.

## 10.2 CLI

```bash
nats object add ASSETS --replicas 3 --storage file --max-bucket-size 10GB
nats object put ASSETS ./firmware-v2.bin
nats object ls ASSETS
nats object info ASSETS firmware-v2.bin
nats object get ASSETS firmware-v2.bin --output ./downloaded.bin
nats object watch ASSETS
nats object rm ASSETS firmware-v2.bin
```

## 10.3 Object Store на Go

```go
obs, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
	Bucket:   "ASSETS",
	Replicas: 3,
	Storage:  jetstream.FileStorage,
	MaxBytes: 10 << 30,
})

// загрузка файла
f, _ := os.Open("firmware-v2.bin")
defer f.Close()
info, err := obs.Put(ctx, jetstream.ObjectMeta{
	Name:        "firmware-v2.bin",
	Description: "прошивка для датчиков серии X",
	Headers:     nats.Header{"Sha256": []string{checksum}},
}, f)
log.Println(info.Size, info.Digest)

// скачивание
r, err := obs.Get(ctx, "firmware-v2.bin")
defer r.Close()
io.Copy(out, r)

// подписка на появление новых версий
w, _ := obs.Watch(ctx)
for i := range w.Updates() {
	if i == nil { continue }
	log.Println("новый объект:", i.Name, i.Size)
}
```

Object Store хранит метаданные и digest (SHA-256), поддерживает версии через перезапись и watch. Чего в нём нет: presigned URL, диапазонных запросов в привычном виде, политик доступа уровня объекта. Это не замена S3 для пользовательского контента, но отличная штука для распространения файлов по инфраструктуре, особенно на leaf nodes.

---

# Модуль 11. Микросервисы на NATS: Services API

## 11.1 Что это

Services API (пакет `micro` в Go, аналоги в других клиентах) это тонкая надстройка над request-reply, которая добавляет то, чего обычно не хватает для продакшена:

- единый формат ошибок в ответах;
- **автоматический discovery**: `PING`, `INFO`, `STATS` на служебных subjects;
- **статистику**: число запросов, ошибок, среднее время обработки;
- версионирование и группы эндпоинтов.

```
$SRV.PING                      -> откликнутся все сервисы
$SRV.PING.pricing              -> откликнутся все экземпляры сервиса pricing
$SRV.INFO.pricing.<id>         -> описание конкретного экземпляра
$SRV.STATS.pricing             -> статистика
```

## 11.2 Сервис на Go

```go
import "github.com/nats-io/nats.go/micro"

srv, err := micro.AddService(nc, micro.Config{
	Name:        "pricing",
	Version:     "1.2.0",
	Description: "Расчёт цен и скидок",
})
if err != nil {
	log.Fatal(err)
}
defer srv.Stop()

g := srv.AddGroup("service.pricing")

g.AddEndpoint("calculate", micro.HandlerFunc(func(r micro.Request) {
	var req CalcRequest
	if err := json.Unmarshal(r.Data(), &req); err != nil {
		r.Error("400", "invalid payload", nil)
		return
	}
	result, err := calculate(req)
	if err != nil {
		r.Error("500", err.Error(), nil)
		return
	}
	r.RespondJSON(result)
}), micro.WithEndpointMetadata(map[string]string{"format": "json"}))
```

Клиент вызывает как обычный request-reply:

```bash
nats request service.pricing.calculate '{"sku":"A1","qty":3}' --timeout 2s
```

## 11.3 Discovery и наблюдаемость из коробки

```bash
nats micro ls                      # все сервисы в системе
nats micro info pricing            # описание и эндпоинты
nats micro stats pricing           # запросы, ошибки, latency по экземплярам
nats micro ping pricing            # кто живой и с какой задержкой
```

Это то, за что в мире HTTP платят Consul, service mesh и половиной observability-стека. Здесь это встроено в протокол.

## 11.4 NATS вместо HTTP: честные плюсы и минусы

| | Плюс | Минус |
|---|---|---|
| Адресация | Не нужны DNS, ingress, service mesh | Нет привычных URL и HTTP-инструментов |
| Балансировка | Queue group, автоматически | Нет weighted routing и canary из коробки |
| Отказоустойчивость | Клиент переподключается сам, fail-fast `no responders` | Нужно самому продумать таймауты и circuit breaker |
| Multi-region | Работает через supercluster прозрачно | Нужно понимать топологию, иначе запрос уедет за океан |
| Контракты | Metadata эндпоинтов | Нет OpenAPI и типизации, схемы ведёшь сам |
| Отладка | `nats sub`, `nats req` из любого места | Нет curl, Postman, браузера |

Компромисс, который используют многие: HTTP-шлюз наружу, NATS внутри.

---

# Модуль 12. Кластеризация, superclusters и leaf nodes

## 12.1 Три уровня топологии

```
  LEAF NODES (edge, филиалы, ноутбуки, устройства)
        |  leafnode connection (исходящее, через NAT и TLS)
        v
  CLUSTER (3-5 серверов в одном ДЦ, routes, full mesh)
        |  gateway connection
        v
  SUPERCLUSTER (несколько кластеров в разных регионах)
```

| Уровень | Соединение | Порт по умолчанию | Для чего |
|---|---|---|---|
| Cluster | `routes` | 6222 | Отказоустойчивость и масштаб внутри ДЦ |
| Supercluster | `gateways` | 7222 | Соединение кластеров между регионами |
| Leaf node | `leafnodes` | 7422 | Edge, изоляция, слабая или нестабильная связь |

## 12.2 Кластер и RAFT

Кластер Core NATS не требует кворума: он просто маршрутизирует сообщения. Но **JetStream требует**: каждый стрим с `replicas > 1` это RAFT-группа, и ещё есть мета-группа, которая хранит список стримов и консьюмеров.

```
meta RAFT group (все серверы кластера):
  кто лидер кластера JetStream, где размещать стримы

stream RAFT group (по одной на стрим):
  ORDERS: leader=nats-2, followers=nats-1, nats-3
```

Следствия:

| Узлов в кластере | Переживает падение | Комментарий |
|---|---|---|
| 1 | 0 | Только разработка |
| 2 | 0 | **Худший вариант**: нет кворума, хуже одного узла |
| 3 | 1 | Стандарт продакшена |
| 5 | 2 | Крупные инсталляции, выше стоимость записи |

Чётное число узлов не добавляет отказоустойчивости. Не бывает `replicas=2` — точнее, бывает, но такой стрим не переживёт падения ни одного узла без потери доступности записи.

```bash
# командам `nats server ...` нужен контекст системного аккаунта
nats server report jetstream      # кто лидер, сколько стримов на узле
nats stream cluster step-down ORDERS   # принудительно переизбрать лидера стрима
nats server cluster step-down          # переизбрать мета-лидера (системный аккаунт)
```

## 12.3 Supercluster и gateways

```
       ┌──────────── cluster EU ────────────┐
       │  nats-eu-1  nats-eu-2  nats-eu-3   │
       └──────────────┬─────────────────────┘
                      │ gateway
       ┌──────────────┴─────────────────────┐
       │  nats-us-1  nats-us-2  nats-us-3   │
       └──────────── cluster US ────────────┘
```

```hcl
gateway {
  name: "EU"
  port: 7222
  gateways: [
    { name: "EU", urls: ["nats://nats-eu-1:7222", "nats://nats-eu-2:7222"] }
    { name: "US", urls: ["nats://nats-us-1:7222", "nats://nats-us-2:7222"] }
  ]
}
```

Как это работает:

- для каждого удалённого кластера достаточно указать **один или несколько seed-адресов**: остальные серверы обнаруживаются автоматически, и добавление узла не требует правки всех конфигов;
- сообщение пересекает океан **только если на той стороне есть заинтересованный подписчик**;
- запрос request-reply предпочтёт локального ответчика: если в EU есть экземпляр сервиса, запрос не поедет в US;
- **loop detection** встроен: сообщение не будет ходить по кругу между кластерами.

Это и есть главный аргумент за NATS в мультирегионе: гео-распределённая шина без MirrorMaker, без ручного зеркалирования топиков и без отдельного слоя маршрутизации.

## 12.4 Leaf nodes

Leaf node это отдельный `nats-server`, который подключается **исходящим** соединением к кластеру и связывает один из своих локальных аккаунтов с аккаунтом в центре. В какой аккаунт хаба ты попадёшь, определяют учётные данные; необязательное поле `account` задаёт локальный аккаунт на самом leaf-сервере. Используй один способ аутентификации на remote: либо creds, либо логин и пароль, но не оба сразу.

```hcl
# конфиг leaf-сервера на заводе / в магазине / на ноутбуке разработчика
leafnodes {
  remotes: [
    {
      url: "tls://nats.company.com:7422"
      # creds определяют, к КАКОМУ аккаунту хаба привяжется leaf
      credentials: "/etc/nats/edge.creds"
      # необязательно: ЛОКАЛЬНЫЙ аккаунт на этом leaf-сервере
      # account: "SHOP"
    }
  ]
}

jetstream { store_dir: "/data/js", domain: "edge-msk-42" }
```

Зачем это нужно:

- **работа при обрыве связи**: локальные сервисы продолжают общаться друг с другом через свой leaf-сервер;
- **локальный JetStream с собственным доменом**: данные копятся на месте и синхронизируются наверх, когда связь вернётся;
- **экономия трафика**: наверх уходит только то, на что есть интерес;
- **через NAT и firewall**: соединение всегда исходящее, входящие порты открывать не нужно;
- **изоляция**: leaf видит только тот аккаунт, к которому подключён.

```bash
curl -s localhost:8222/leafz | jq '.leafs[] | {name, account, ip, rtt}'   # на хабе
```

Типичный IoT-сценарий: на каждом объекте leaf node с JetStream-доменом `edge-<id>`, данные пишутся локально, а центральный кластер собирает их через `source` с указанием домена (модуль 13).

## 12.5 JetStream domains

Домен это имя изолированного пространства JetStream. Он нужен, чтобы отличать «мой локальный JetStream» от «JetStream в центре»:

```bash
nats --js-domain hub stream ls          # стримы в центральном домене
nats --js-domain edge-msk-42 stream ls  # стримы на этом leaf-узле
```

Без доменов leaf-узел с JetStream и центральный кластер начнут конфликтовать за одни и те же subjects API. Задавай домен всегда, когда включаешь JetStream на leaf.

## 12.6 MQTT и WebSocket: встроенные листенеры

`nats-server` нативно поддерживает MQTT 3.1.1 и WebSocket, и это закрывает IoT-устройства и браузеры без отдельного шлюза.

```hcl
server_name: "edge-msk-42"            # обязателен для MQTT
jetstream { store_dir: "/data/js" }   # сессии MQTT и QoS 1 хранятся в JetStream

mqtt {
  port: 1883
}

websocket {
  port: 8080
  no_tls: true                  # только для разработки; в продакшене нужен TLS
}
```

Как MQTT ложится на NATS:

| MQTT | NATS |
|---|---|
| разделитель топика `/` | разделитель токенов `.` |
| одноуровневый wildcard `+` | `*` |
| многоуровневый wildcard `#` | `>` |
| QoS 0 | доставка Core NATS |
| QoS 1, retained-сообщения, сессии | стримы JetStream, которые создаёт сервер |

То есть устройство, публикующее в `factory/line1/temp`, видно NATS-сервисам в `factory.line1.temp`, и наоборот. Не используй точки в именах MQTT-топиков: они станут разделителями токенов.

Через WebSocket браузерный код использует официальный JavaScript-клиент и получает те же subjects, права и аккаунты, что и любой другой клиент.

### Вопросы для самопроверки

1. Почему кластер из 2 узлов хуже, чем из 1, для JetStream?
2. Чем gateway отличается от route?
3. Почему запрос request-reply в supercluster обычно не уходит в другой регион?
4. Зачем leaf node собственный JetStream-домен?
5. Почему поддержке MQTT нужен JetStream?

---

# Модуль 13. Mirrors, sources и гео-репликация

## 13.1 Mirror и Source

Один subject может принадлежать только одному стриму. Копии данных делают через специальные стримы:

| | **Mirror** | **Source** |
|---|---|---|
| Источников | Ровно один | Один или несколько |
| Свои subjects | Нет, только копия | Может иметь, плюс данные из источников |
| Публикация напрямую | Запрещена | Разрешена |
| Порядок и seq | Сохраняет sequence оригинала | Перенумеровывает |
| Для чего | Реплика для чтения, бэкап, гео-копия | Агрегация нескольких стримов в один |

```
                      ORDERS (EU, основной)
                       |            |
              mirror   |            |  source
                       v            v
              ORDERS_MIRROR_US    ALL_EVENTS (собирает ORDERS + PAYMENTS + SHIPPING)
```

## 13.2 Mirror для гео-репликации

```bash
nats stream add ORDERS_US \
  --mirror ORDERS \
  --storage file --replicas 3 \
  --defaults
```

С фильтром и стартовой позицией нагляднее всего и не зависит от версии CLI — JSON-конфиг:

```json
{
  "name": "ORDERS_ARCHIVE",
  "storage": "file",
  "num_replicas": 3,
  "mirror": {
    "name": "ORDERS",
    "filter_subject": "shop.orders.created",
    "opt_start_seq": 1
  }
}
```

```bash
nats stream add ORDERS_ARCHIVE --config orders-archive.json
```

Читающие сервисы в США подключаются к `ORDERS_US` и не ходят через океан за каждым сообщением. Публикация при этом продолжает идти в EU.

## 13.3 Source: агрегация

```bash
nats stream add ALL_EVENTS \
  --source ORDERS \
  --source PAYMENTS \
  --source SHIPPING \
  --storage file --replicas 3
```

Полезно для аналитики и аудита: один стрим, из которого читает вся BI-команда, при этом доменные стримы остаются раздельными.

Источник может быть в другом JetStream-домене (например, на leaf-узле). Домен адресуется через свой API-префикс `$JS.<domain>.API`:

```json
{
  "name": "EDGE_TELEMETRY",
  "storage": "file",
  "num_replicas": 3,
  "sources": [
    {
      "name": "TELEMETRY",
      "external": { "api": "$JS.edge-msk-42.API" }
    }
  ]
}
```

```bash
nats stream add EDGE_TELEMETRY --config edge-telemetry.json
```

Так центральный кластер «подтягивает» данные с сотен edge-узлов, а те продолжают работать при обрывах связи.

## 13.4 Disaster recovery

| Сценарий | Решение |
|---|---|
| Упал один узел кластера | `replicas=3`, RAFT переизберёт лидера за секунды |
| Упал весь ДЦ | Mirror в другом регионе + переключение публикаторов |
| Повреждены данные приложением | `nats stream backup` по расписанию |
| Нужен перенос между кластерами | `backup` / `restore` или mirror с последующим переключением |

Проверяй восстановление регулярно. Бэкап, который никогда не восстанавливали, это не бэкап.

---

# Модуль 14. Обработка ошибок: retry, DLQ и poison pill

## 14.1 Классификация ошибок

```
ошибка при обработке сообщения
   |
   +-- временная (БД недоступна, 503, таймаут) -> Nak с задержкой, повторим
   +-- постоянная (невалидный JSON, нет такого заказа) -> Term, в DLQ
   +-- неизвестная -> Nak, но со счётчиком: после N попыток Term
```

Приложение, которое не различает эти случаи, обречено: либо будет вечно крутить мусор, либо выбросит валидное сообщение из-за временного сбоя сети.

## 14.2 Backoff вместо постоянного интервала

```bash
nats consumer add ORDERS BILLING --pull --ack explicit \
  --max-deliver 6 \
  --backoff linear --backoff-steps 5 --backoff-min 1s --backoff-max 5m
```

```
попытка 1 -> ошибка -> ждём 1s
попытка 2 -> ошибка -> ждём ~1m
попытка 3 -> ошибка -> ждём ~2m
попытка 4 -> ошибка -> ждём ~3m
попытка 5 -> ошибка -> ждём ~4m
попытка 6 -> ошибка -> MAX_DELIVERIES advisory -> сообщение больше не доставляется
```

`linear` равномерно распределяет задержки от min к max (в natscli последняя задержка остаётся на шаг ниже max). Если нужно растущее расписание вроде 1s, 15s, 1m, 3m, 5m, задай массив явно (`BackOff` в коде или `"backoff"` в JSON-конфиге консьюмера). `max_deliver` не может быть меньше числа элементов backoff; здесь 6 попыток на 5 задержек используют их все.

Без backoff упавшая база получит шквал повторов ровно в тот момент, когда ей хуже всего.

## 14.3 DLQ на advisory-сообщениях

В NATS нет встроенной DLQ, но есть advisory-события, из которых она собирается:

```
$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>
$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.<stream>.<consumer>
```

**Сначала ловушка.** Advisory — это обычные сообщения Core NATS. Если DLQ-сервис просто подписан на них и в момент события лежит (деплой, падение), advisory пропадёт, и упавшее сообщение никогда не попадёт в DLQ. Это ровно правило из модуля 0: «Core NATS — для того, что можно потерять». Поэтому сами advisory сначала складываются в стрим, а перекладчик читает их durable-консьюмером.

```go
// 1. надёжно сохраняем advisory
js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
	Name: "ADVISORIES",
	Subjects: []string{
		"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>",
		"$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>",
	},
	Storage:  jetstream.FileStorage,
	Replicas: 3,
	MaxAge:   7 * 24 * time.Hour,
})

// 2. стрим для «мёртвых» сообщений
js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
	Name:       "DLQ",
	Subjects:   []string{"dlq.>"},
	Storage:    jetstream.FileStorage,
	Replicas:   3,
	MaxAge:     30 * 24 * time.Hour,
	Duplicates: 10 * time.Minute,
})

// 3. durable-консьюмер, который перекладывает оригиналы в DLQ
mover, _ := js.CreateOrUpdateConsumer(ctx, "ADVISORIES", jetstream.ConsumerConfig{
	Durable:    "DLQ_MOVER",
	AckPolicy:  jetstream.AckExplicitPolicy,
	MaxDeliver: 20,
})

mover.Consume(func(m jetstream.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// эти поля есть в обоих типах advisory
	var adv struct {
		Stream     string `json:"stream"`
		Consumer   string `json:"consumer"`
		StreamSeq  uint64 `json:"stream_seq"`
		Deliveries uint64 `json:"deliveries"`
		Reason     string `json:"reason"` // только в MSG_TERMINATED
	}
	if err := json.Unmarshal(m.Data(), &adv); err != nil {
		m.Term()
		return
	}
	if adv.Stream == "DLQ" || adv.Stream == "ADVISORIES" {
		m.Ack() // не зацикливаемся на собственных стримах
		return
	}

	stream, err := js.Stream(ctx, adv.Stream)
	if err != nil {
		m.NakWithDelay(5 * time.Second)
		return
	}
	orig, err := stream.GetMsg(ctx, adv.StreamSeq)
	if err != nil {
		log.Println("оригинал уже удалён retention:", err)
		m.Ack()
		return
	}

	// копируем заголовки в НОВУЮ map: orig.Header может быть nil,
	// а исходный Nats-Msg-Id не должен вызвать дедупликацию в DLQ
	h := nats.Header{}
	for k, v := range orig.Header {
		if k != "Nats-Msg-Id" {
			h[k] = v
		}
	}
	h.Set("Dlq-Original-Subject", orig.Subject)
	h.Set("Dlq-Original-Stream", adv.Stream)
	h.Set("Dlq-Original-Seq", strconv.FormatUint(adv.StreamSeq, 10))
	h.Set("Dlq-Deliveries", strconv.FormatUint(adv.Deliveries, 10))
	if adv.Reason != "" {
		h.Set("Dlq-Reason", adv.Reason)
	}
	// стабильный id делает само перекладывание идемпотентным при повторах
	h.Set("Nats-Msg-Id", fmt.Sprintf("dlq-%s-%d", adv.Stream, adv.StreamSeq))

	dlqMsg := &nats.Msg{
		Subject: "dlq." + adv.Stream + "." + adv.Consumer,
		Header:  h,
		Data:    orig.Data,
	}
	if _, err := js.PublishMsg(ctx, dlqMsg); err != nil {
		m.NakWithDelay(5 * time.Second)
		return
	}

	alert("сообщение ушло в DLQ", adv)
	m.Ack()
})
```

Важно:

- оригинал достаётся по `stream_seq`, так что схема работает, пока retention его не удалил: держи `max_age` исходного стрима заметно больше суммарного времени повторов;
- при `workqueue` retention сообщение, исчерпавшее `max_deliver`, остаётся в стриме (его так и не подтвердили), поэтому здесь оно доступно, но продолжает занимать лимиты: при необходимости удаляй его после перекладывания;
- пользователю приложения нужно право подписываться на `$JS.EVENT.ADVISORY.>` в своём аккаунте (модуль 17).

Повторная обработка после починки бага:

```bash
nats stream view DLQ
nats consumer add DLQ REPROCESS --pull --ack explicit --defaults
# сервис читает DLQ и публикует обратно в исходный subject
```

## 14.4 Poison pill: как не положить всю очередь

```
сообщение с битым JSON
  -> handler паникует
  -> ack не отправлен
  -> через AckWait доставка повторяется
  -> handler снова паникует
  -> ... бесконечно, воркеры заняты только этим
```

Защита:

1. **`max_deliver` всегда конечный.** `-1` в продакшене это мина.
2. **Валидация на входе**: не распарсилось — сразу `Term()`, без ретраев.
3. **Recover от паники** в обработчике, иначе падает весь воркер.
4. **`max_ack_pending`** ограничивает, сколько ядовитых сообщений может занять воркеров одновременно.
5. **Алерт на рост `num_redelivered`** — самый ранний сигнал, что что-то пошло не так.

## 14.5 Полезные advisory-события

| Advisory | О чём |
|---|---|
| `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>` | Сообщение исчерпало попытки (складывай в стрим, см. 14.3) |
| `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>` | Клиент вызвал `Term()` |
| `$JS.EVENT.ADVISORY.STREAM.CREATED/DELETED/UPDATED.>` | Изменения конфигурации |
| `$JS.EVENT.ADVISORY.API` | Все вызовы JetStream API (аудит) |
| `$SYS.ACCOUNT.*.DISCONNECT` | Отключения клиентов, в том числе по ошибке |
| `$SYS.SERVER.*.CLIENT.AUTH.ERR` | Провалы аутентификации |

```bash
nats events                                  # интерактивный просмотр
nats sub "$JS.EVENT.ADVISORY.>" --headers-only
```

### Практика

1. Сделай обработчик, который всегда возвращает ошибку, и посмотри цепочку backoff.
2. Реализуй DLQ по схеме выше и убедись, что сообщение туда попадает. Затем останови перекладчик, вызови ошибку, запусти его снова и убедись, что сообщение всё равно дошло до DLQ.
3. Отправь битый JSON и добейся, чтобы он ушёл в DLQ с первой попытки через `Term()`.

---

# Модуль 15. Производительность и тюнинг NATS

## 15.1 Порядки величин

| Сценарий | Ориентир на одном приличном сервере |
|---|---|
| Core NATS, fan-out, мелкие сообщения | Миллионы msg/s, задержка десятки микросекунд |
| Core NATS request-reply | Сотни тысяч запросов/с |
| JetStream memory, replicas=1 | Сотни тысяч msg/s |
| JetStream file, replicas=1 | 100-300 тыс. msg/s (зависит от диска) |
| JetStream file, replicas=3 | Десятки тысяч msg/s синхронно, в разы больше асинхронно |

Главный вывод: **переход с Core на JetStream стоит на порядок дороже, а переход на replicas=3 ещё в разы.** Не включай персистентность там, где она не нужна.

## 15.2 Бенчмарк своими руками

```bash
# Core NATS pub/sub
nats bench pub bench.core --msgs 1000000 --size 128 --clients 5
nats bench sub bench.core --msgs 1000000 --clients 5

# request-reply
nats bench service serve bench.rpc --clients 5
nats bench service request bench.rpc --msgs 100000 --clients 10

# JetStream
nats bench js pub ORDERS --msgs 200000 --size 512 --clients 5
nats bench js pub ORDERS --msgs 200000 --size 512 --clients 5 --batch 500   # async
```

Всегда меряй **на своём железе, со своим размером сообщений и своей топологией**. Чужие цифры бесполезны.

## 15.3 Slow consumers: главная беда Core NATS

Если подписчик не успевает читать, сервер копит сообщения в его исходящем буфере. Когда буфер переполняется, сервер **отбрасывает сообщения и пишет в лог `slow consumer`**.

```bash
curl -s localhost:8222/varz | grep -i slow
nats server report connections --sort subs
```

Что делать:

| Причина | Решение |
|---|---|
| Тяжёлая обработка прямо в колбэке | Класть в канал/очередь, обрабатывать в пуле воркеров |
| Один подписчик на весь поток | Queue group и горизонтальное масштабирование |
| Маленький буфер клиента | `SetPendingLimits`, `nats.SyncQueueLen` |
| Медленная сеть до клиента | Ближе разместить или использовать leaf node |
| Нужны гарантии, а не скорость | JetStream: там нет потерь от медленного клиента, есть `max_ack_pending` |

## 15.4 Настройки сервера

```hcl
max_payload: 1MB          # не увеличивай без крайней нужды
max_pending: 64MB         # исходящий буфер на клиента
max_connections: 64000
write_deadline: "10s"
ping_interval: "2m"
ping_max: 2

jetstream {
  store_dir: "/data/jetstream"
  max_file_store: 500GB
  max_memory_store: 4GB
  sync_interval: "2m"     # как часто fsync (по умолчанию); "always" надёжнее, но кратно медленнее
}
```

**Что `sync_interval` значит для надёжности.** По умолчанию JetStream подтверждает публикацию, как только запись принята кворумом, а на диск сбрасывает раз в 2 минуты. При `replicas=3` на практике это безопасно: чтобы потерять подтверждённые данные, питание должно одновременно пропасть у большинства узлов. При `replicas=1` отключение питания или kernel panic может унести **подтверждённые** сообщения за последние пару минут. Если у тебя однорепликовые стримы с данными, которые нельзя терять, ставь `sync_interval: always` и закладывай падение пропускной способности, или просто используй `replicas=3`.

Системные настройки Linux:

```
# файловые дескрипторы
ulimit -n 1000000

# сеть
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
```

Диск для JetStream: локальный NVMe. Сетевые диски с плавающей задержкой fsync превращают RAFT в лотерею.

## 15.5 Тюнинг клиента

| Настройка | Зачем |
|---|---|
| Одно подключение на процесс | Соединение мультиплексирует все подписки |
| `PublishAsync` + `PublishAsyncMaxPending` | Кратный рост throughput в JetStream |
| `Fetch(batch)` побольше | Меньше round-trip на сообщение |
| `max_ack_pending` под число воркеров | Слишком мало — простой, слишком много — перегрузка |
| `AckWait` чуть больше p99 обработки | Иначе лишние повторные доставки |
| `Drain()` вместо `Close()` | Не терять сообщения при деплое |
| Не держать тяжёлую логику в колбэке | Источник slow consumers |

## 15.6 Что реально ускоряет систему

1. **Не включать JetStream там, где хватает Core.** Самая дешёвая оптимизация.
2. **Не публиковать лишнего.** Интерес-маршрутизация экономит сеть только если подписки узкие.
3. **Батчить.** И на публикации, и на fetch.
4. **Сжимать payload на стороне приложения**, если сообщения крупные и текстовые.
5. **Держать сообщения маленькими.** Файлы в Object Store, а в событии ссылка.
6. **Считать реплики.** `replicas=3` для стрима телеметрии, который никому не нужен через час, это выброшенные деньги.

---

# Модуль 16. Мониторинг NATS: метрики, lag, алерты

## 16.1 HTTP-эндпоинты мониторинга

Включаются `-m 8222` или `http: 8222`.

| Эндпоинт | Что показывает |
|---|---|
| `/varz` | Общая информация о сервере: версия, аптайм, память, соединения, slow consumers |
| `/connz` | Список соединений: кто, сколько сообщений, какие подписки |
| `/routez` | Соединения с другими серверами кластера |
| `/gatewayz` | Gateway-соединения |
| `/leafz` | Leaf-соединения |
| `/subsz` | Таблица подписок |
| `/jsz` | JetStream: аккаунты, стримы, консьюмеры, использование диска |
| `/healthz` | Health-check для Kubernetes |
| `/accountz` | Аккаунты |

```bash
curl -s localhost:8222/varz | jq '{connections, slow_consumers, mem, in_msgs, out_msgs}'
curl -s "localhost:8222/jsz?streams=1&consumers=1" | jq '.account_details'
curl -s "localhost:8222/healthz?js-enabled-only=true"
```

## 16.2 Prometheus

```yaml
# docker-compose фрагмент
  nats-exporter:
    image: natsio/prometheus-nats-exporter:latest
    command: ["-varz", "-connz", "-routez", "-subz", "-jsz=all", "http://nats-1:8222"]
    ports: ["7777:7777"]
```

Ключевые метрики:

| Метрика | О чём |
|---|---|
| `gnatsd_varz_slow_consumers` | Рост = клиенты не успевают читать |
| `gnatsd_varz_connections` | Число подключений |
| `gnatsd_varz_mem` / `cpu` | Ресурсы сервера |
| `gnatsd_varz_in_msgs` / `out_msgs` | Пропускная способность |
| `jetstream_consumer_num_pending` | **Lag консьюмера**, главная метрика |
| `jetstream_consumer_num_ack_pending` | Незакрытые ack |
| `jetstream_consumer_num_redelivered` | Повторные доставки |
| `jetstream_stream_total_bytes` / `total_messages` | Наполнение стрима |
| `jetstream_server_jetstream_disk_used` | Свободное место |

## 16.3 Что алертить

| Алерт | Условие | Почему важно |
|---|---|---|
| **Consumer lag растёт** | `num_pending` растёт 10 минут подряд | Потребитель не справляется или упал |
| **Повторные доставки** | `num_redelivered > 0` устойчиво | Ошибки обработки, ядовитые сообщения |
| **Ack pending на потолке** | `num_ack_pending >= max_ack_pending` | Обработка встала |
| **Slow consumers** | Любой рост счётчика | Потери сообщений в Core NATS |
| **Диск JetStream** | Занято > 75% | При 100% публикация встанет или начнёт терять |
| **Нет лидера стрима** | `leader` пустой в `nats stream info` | Потеряли кворум |
| **Сообщения в DLQ** | Любое | Бизнес-логика падает |
| **Падение числа серверов** | `< 3` в кластере | Кворум под угрозой |
| **Auth errors** | `$SYS.SERVER.*.CLIENT.AUTH.ERR` | Атака или сломанный деплой |

## 16.4 Повседневные команды

```bash
# `server`, `latency`, `events` и `traffic` требуют контекст системного аккаунта
nats server list
nats server info nats-1
nats server report jetstream
nats server report connections --sort in-msgs
nats server report accounts
nats stream report
nats consumer report ORDERS
nats latency --server-b nats://nats-2:4222 --duration 10s   # замер задержки между серверами
nats events                                                  # поток системных событий
nats traffic                                                 # живой трафик
```

## 16.5 Трассировка сообщений

С NATS 2.11 доступна трассировка доставки: сервер сообщает путь сообщения по кластеру.

```bash
nats trace shop.orders.created
```

Показывает, через какие серверы прошло сообщение, каким подписчикам ушло и попало ли в стрим. Незаменимо в вопросе «почему сервис не получает событие».

---

# Модуль 17. Безопасность NATS: accounts, NKeys, JWT, TLS

## 17.1 Три модели аутентификации

| Модель | Как | Когда |
|---|---|---|
| **Токены и пароли в конфиге** | `authorization { users: [...] }` | Разработка, маленькие инсталляции |
| **NKeys** | Ed25519-ключи, challenge-response, приватный ключ не покидает клиента | Средние инсталляции без централизованного управления |
| **JWT + операторная модель** | Оператор подписывает аккаунты, аккаунты подписывают пользователей | Продакшен, multi-tenancy, динамическое управление доступом |

## 17.2 Простой конфиг с пользователями и правами

```hcl
accounts {
  SHOP: {
    users: [
      {
        user: "order-service", password: "$2a$11$…"   # bcrypt через `nats server passwd`
        permissions: {
          publish:   { allow: ["shop.orders.>", "$JS.API.STREAM.MSG.GET.ORDERS"] }
          subscribe: { allow: ["_INBOX.>", "shop.payments.>"] }
        }
      },
      {
        user: "billing-service", password: "$2a$11$…"
        permissions: {
          publish: {
            allow: [
              "shop.payments.>",                             # свои события
              "$JS.API.CONSUMER.INFO.ORDERS.BILLING",        # клиент сначала запрашивает консьюмера
              "$JS.API.CONSUMER.MSG.NEXT.ORDERS.BILLING",    # pull-запрос — это PUBLISH
              "$JS.ACK.ORDERS.BILLING.>"                     # ack только для этого консьюмера
            ]
          }
          # сообщения и PubAck приходят в inbox
          subscribe: { allow: ["_INBOX.>"] }
        }
      }
    ]
    jetstream: { max_file: 100GB, max_streams: 20 }
  }

  ANALYTICS: {
    users: [ { user: "analytics", password: "$2a$11$…",
               permissions: { subscribe: { allow: ["shop.>"] }, publish: { deny: [">"] } } } ]
  }

  SYS: { users: [ { user: "admin", password: "$2a$11$…" } ] }
}
system_account: SYS
# никакого `no_auth_user`: в продакшене каждый клиент обязан аутентифицироваться
```

Частая ошибка — думать, что pull-запрос это подписка. Нет: клиент **публикует** в `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` с inbox в качестве reply subject, и сервер доставляет сообщения в этот inbox. То же самое верно для любого вызова JetStream API. Если сервис падает с `permissions violation`, в логе сервера будет точный subject, который запретили.

`_INBOX.>` — широкое право: любой пользователь, которому разрешено на него подписываться, теоретически может увидеть чужие ответы. Для строгой изоляции дай каждому сервису свой префикс inbox (`nats.CustomInboxPrefix("_INBOX_billing")` в Go) и разреши только `_INBOX_billing.>`.

**Принцип наименьших привилегий здесь особенно важен:** подписка на `>` в аккаунте означает доступ ко всем данным. Всегда перечисляй конкретные subjects.

## 17.3 Exports и imports между аккаунтами

```hcl
accounts {
  SHOP: {
    exports: [
      { stream: "shop.orders.created" }                       # публичный поток событий
      { service: "service.pricing.calculate" }                # публичный сервис
    ]
  }
  PARTNER: {
    imports: [
      { stream:  { account: "SHOP", subject: "shop.orders.created" } }
      { service: { account: "SHOP", subject: "service.pricing.calculate" } }
    ]
  }
}
```

Необязательный `prefix` у stream-импорта добавляет токен в начало: `prefix: "upstream"` превратит события в аккаунте `PARTNER` в `upstream.shop.orders.created`. Это удобно при импорте из нескольких аккаунтов и сбивает с толку, если префикс повторяет первый токен.

Это единственный способ пересечь границу аккаунта, и он явный. Хочешь дать партнёру ровно один subject — дашь ровно один.

## 17.4 Операторная модель: nsc

```bash
nsc add operator --generate-signing-key --sys --name SHOP_OPERATOR
nsc add account --name SHOP
nsc edit account SHOP --js-mem-storage 1G --js-disk-storage 100G --js-streams 50
nsc add user --account SHOP --name order-service \
  --allow-pub "shop.orders.>" --allow-sub "_INBOX.>"
nsc generate creds --account SHOP --name order-service > order-service.creds

# сервер должен знать об операторе: один раз генерируем конфиг резолвера
nsc generate config --nats-resolver --sys-account SYS > resolver.conf
# добавь `include resolver.conf` в nats.conf, перезапусти сервер и загрузи аккаунты
nsc push --all
```

```go
nc, _ := nats.Connect("nats://nats.company.com:4222",
	nats.UserCredentials("/etc/nats/order-service.creds"))
```

Что это даёт:

- приватные ключи **никогда не отправляются на сервер**;
- права зашиты в подписанный JWT: сервер не хранит список пользователей;
- пользователей можно выпускать и отзывать без перезапуска сервера (через NATS Account Server или встроенный resolver);
- срок действия, лимиты на соединения, данные, подписки — всё в JWT.

## 17.5 TLS

```hcl
tls {
  cert_file: "/etc/nats/certs/server-cert.pem"
  key_file:  "/etc/nats/certs/server-key.pem"
  ca_file:   "/etc/nats/certs/ca.pem"
  verify:    true          # требовать клиентский сертификат (mTLS)
  timeout:   5
}

cluster {
  name: "course"
  port: 6222
  tls { cert_file: "…", key_file: "…", ca_file: "…", verify: true }
  routes: [ … ]
}
```

TLS настраивается отдельно для клиентов, routes, gateways и leafnodes. Внутрикластерный трафик шифруй тоже: route-соединение видит все сообщения кластера.

При mTLS можно привязать пользователя к CN сертификата:

```hcl
authorization {
  users: [ { nkey: "…" } ]
}
# или
tls { verify_and_map: true }   # CN сертификата = имя пользователя
```

## 17.6 Лимиты как часть безопасности

```hcl
accounts {
  PARTNER: {
    limits: {
      max_connections: 100
      max_subscriptions: 1000
      max_payload: 256KB
      max_data: 10GB
    }
  }
}
```

Шумный сосед, который открывает 50 000 соединений или подписывается на `>`, опаснее внешнего злоумышленника. Лимиты на аккаунт это не паранойя, а гигиена.

## 17.7 Чек-лист безопасности продакшена

- [ ] TLS для клиентов, routes, gateways и leafnodes
- [ ] Никакого `no_auth_user` и анонимного доступа (docker compose из модуля 2 использует его только для обучения)
- [ ] Отдельный аккаунт на каждую команду или тенанта
- [ ] Права на конкретные subjects, никаких `>` в allow
- [ ] Системный аккаунт (`system_account`) с отдельными учётными данными, доступ только у SRE
- [ ] Лимиты JetStream на аккаунт
- [ ] Порт мониторинга 8222 не выставлен наружу
- [ ] Ротация creds и сертификатов автоматизирована
- [ ] Алерты на `AUTH.ERR` и всплески подключений
- [ ] `deny_delete` и `deny_purge` на аудит-стримах

---

# Модуль 18. NATS в продакшене: архитектура и эксплуатация

## 18.1 Референсная архитектура

```
        приложения                        приложения
          (EU)                              (US)
            |                                 |
   ┌────────┴─────────┐             ┌─────────┴────────┐
   │  cluster EU       │◄──gateway──►│  cluster US      │
   │  3 x nats-server  │             │  3 x nats-server │
   │  JetStream file   │             │  JetStream file  │
   └────────┬──────────┘             └──────────────────┘
            │ leafnodes
   ┌────────┴──────────┐
   │  edge: магазины,  │
   │  заводы, IoT      │
   │  leaf + JS domain │
   └───────────────────┘
```

## 18.2 Sizing

| Нагрузка | Конфигурация |
|---|---|
| До 10 тыс. msg/s, Core | 3 узла × 2 vCPU / 4 ГБ, JetStream можно не включать |
| До 50 тыс. msg/s с JetStream | 3 узла × 4-8 vCPU / 16 ГБ / NVMe 500 ГБ |
| 100+ тыс. msg/s с JetStream | 5 узлов × 8-16 vCPU / 32 ГБ / NVMe 1+ ТБ, разнести стримы по узлам |
| Мультирегион | По кластеру на регион + gateways, mirrors для чтения |

Считай **отдельно** трафик Core и JetStream: это принципиально разные по стоимости операции.

## 18.3 Kubernetes

```bash
helm repo add nats https://nats-io.github.io/k8s/helm/charts/
helm install nats nats/nats \
  --set config.jetstream.enabled=true \
  --set config.jetstream.fileStore.pvc.size=100Gi \
  --set config.cluster.enabled=true \
  --set config.cluster.replicas=3
```

Что важно:

- **StatefulSet и persistent volume** обязательны: без них JetStream теряет данные при переезде пода;
- `podAntiAffinity` по зонам: три реплики в одной зоне не переживут падения зоны;
- `/healthz` как readiness и liveness probe, с `js-enabled-only` на JetStream-узлах;
- **rolling update по одному поду** с ожиданием восстановления кворума между шагами;
- перед выводом узла делай `nats server cluster step-down`, если он мета-лидер.

## 18.4 Обновление версий

```bash
# 1. проверить состояние
nats server report jetstream
nats stream report

# 2. увести лидерство с обновляемого узла
nats server cluster step-down
nats stream cluster step-down ORDERS

# 3. обновить один узел, дождаться его возвращения в кворум
# 4. повторить для остальных
```

NATS хорошо совместим по протоколу между минорными версиями, но обновлять узлы **по одному с проверкой кворума** — правило без исключений.

## 18.5 Чек-лист продакшена

- [ ] 3 или 5 узлов в кластере, разнесены по зонам
- [ ] Все важные стримы с `replicas=3`
- [ ] У каждого стрима задан `max_age` **и** `max_bytes`
- [ ] Лимиты JetStream на каждый аккаунт
- [ ] `max_deliver` конечный у всех консьюмеров
- [ ] DLQ и алерт на попадание в неё
- [ ] Бэкапы стримов по расписанию и проверенный restore
- [ ] Конфигурация стримов и консьюмеров в Git, применяется автоматикой, а не руками
- [ ] Мониторинг: lag, redelivered, slow consumers, диск, кворум
- [ ] TLS и аккаунты по принципу минимальных привилегий
- [ ] `Drain()` при остановке приложений
- [ ] `Nats-Msg-Id` у всех бизнес-событий
- [ ] Идемпотентные обработчики
- [ ] Проведены учения: убить узел, убить лидера, заполнить диск

## 18.6 Антипаттерны

| Антипаттерн | Чем плохо | Как правильно |
|---|---|---|
| Core NATS для бизнес-событий | Тихая потеря данных | JetStream |
| Стрим на `shop.>` «на всякий случай» | Один стрим захватывает всё, ничего больше не создать | Узкие subjects по доменам |
| `max_deliver: -1` | Ядовитое сообщение крутится вечно | Конечный лимит + DLQ |
| Стрим без `max_bytes` и `max_age` | Диск кончится в самый неподходящий момент | Всегда задавать лимиты |
| `replicas: 2` | Нет кворума | 1, 3 или 5 |
| Подписка на `>` в приложении | Получаешь служебный трафик и чужие данные | Конкретные subjects |
| Ack до обработки | At-most-once вместо at-least-once | Ack после успешного коммита |
| Большие сообщения (десятки МБ) | Забитые буферы, slow consumers | Object Store + ссылка в событии |
| Время или UUID в subject стрима | Взрывной рост уникальных subjects и памяти | Идентификаторы в payload или в конце subject осознанно |
| Один аккаунт на всю компанию | Нет изоляции, любой видит всё | Аккаунт на команду |

---

# Модуль 19. Итоговый проект: event-driven интернет-магазин

## 19.1 Что строим

```
                    ┌─────────────┐
   HTTP  ──────────►│ Order API   │
                    └──────┬──────┘
                           │ JetStream: shop.orders.created
                    ┌──────▼───────────────────────────────┐
                    │        Stream ORDERS                  │
                    │  subjects: shop.orders.>              │
                    │  limits, 30d, replicas 3              │
                    └──┬────────┬────────────┬──────────────┘
                       │        │            │
              consumer │        │            │ consumer
              PAYMENT  │        │ consumer   │ ANALYTICS (ordered)
                       │        │ WAREHOUSE  │
                ┌──────▼─┐  ┌───▼────┐  ┌────▼─────┐
                │Payment │  │Warehouse│  │Analytics │
                └───┬────┘  └────────┘  └──────────┘
                    │ shop.payments.succeeded / failed
                    ▼
              Stream PAYMENTS ──► consumer NOTIFY ──► Core NATS ──► WebSocket клиентам

   Дополнительно:
   - KV бакет CONFIG: лимиты, фичефлаги, живое обновление через watch
   - KV бакет IDEMPOTENCY: обработанные event_id
   - Object bucket RECEIPTS: PDF-чеки
   - Stream DLQ: всё, что не смогли обработать
   - Services API: service.pricing.calculate, service.fraud.check
```

## 19.2 Требования

1. Заказ создаётся по HTTP, событие публикуется в JetStream **в одной транзакции с записью в БД** (outbox).
2. Каждое событие имеет `Nats-Msg-Id` и не дублируется при ретраях.
3. Payment Service обрабатывает заказ идемпотентно, при временной ошибке делает `Nak` с backoff, при невалидных данных `Term`.
4. Warehouse резервирует товар и умеет пережить повторную доставку.
5. Analytics читает стрим ordered-консьюмером и строит проекцию в памяти.
6. Notification отправляет уведомление через Core NATS в WebSocket-шлюз.
7. Всё, что исчерпало попытки, уезжает в DLQ с алертом.
8. Фичефлаги читаются из KV и применяются без рестарта.
9. Есть дашборд: lag каждого консьюмера, redelivered, размер DLQ.
10. Система переживает остановку любого одного узла NATS без потери данных.

## 19.3 Этапы

| Этап | Что сделать |
|---|---|
| 1 | Поднять кластер из 3 узлов, создать стримы `ORDERS`, `PAYMENTS`, `DLQ` |
| 2 | Order API: HTTP + PostgreSQL + outbox + relay в NATS |
| 3 | Payment Service: pull-консьюмер, идемпотентность через таблицу `processed_events` |
| 4 | Warehouse: свой консьюмер на тот же стрим, свой фильтр |
| 5 | Analytics: ordered consumer, проекция «выручка по часам» |
| 6 | Services API: `service.pricing.calculate`, вызываемый из Order API через request-reply |
| 7 | KV `CONFIG` + watch: включение/выключение fraud-проверки на лету |
| 8 | DLQ через advisory `MAX_DELIVERIES` + команда реобработки |
| 9 | Мониторинг: exporter + Prometheus + Grafana, алерты на lag и DLQ |
| 10 | Учения: убить лидера стрима, убить воркер в середине обработки, забить диск |

## 19.4 Как проверить, что получилось

```bash
# нагрузка
nats bench js pub ORDERS --msgs 100000 --size 512 --clients 5

# во время нагрузки
docker stop nats-2
nats stream info ORDERS      # лидер переизбран, данные целы
nats consumer report ORDERS  # lag вырос и рассосался

# проверка идемпотентности
# опубликуй одно и то же событие 3 раза с одинаковым Nats-Msg-Id
# в стриме должно быть 1 сообщение, в БД 1 запись
```

Если после всех учений `num_pending` вернулся к нулю, в DLQ пусто, а в БД нет дублей — курс пройден.

---

# Что нового в NATS 2.11 и 2.12

Модули выше опираются на функции, которые стабильны уже несколько лет. Последние релизы добавили возможности, меняющие подход к части задач курса. Имена полей и заголовков здесь не приводятся намеренно: перед использованием сверься с release notes и ADR для своих версий сервера и клиента.

**NATS 2.11**

- **TTL на отдельное сообщение.** Стрим, в котором это разрешено, может удалять отдельные сообщения по TTL-заголовку, а не только по общему `max_age`. На этом построен TTL отдельных ключей в KV Store (модуль 9).
- **Пауза консьюмера.** Консьюмер можно поставить на паузу до заданного времени и возобновить, а не удалять и пересоздавать. Полезно на время обслуживания базы-получателя.
- **Priority groups.** Pull-консьюмер может закрепить доставку за одним клиентом (с автоматическим переключением) или отдавать избыток резервным клиентам при превышении порога pending. Это серверная альтернатива части ручного шардирования из 6.8.
- **Трассировка сообщений.** `nats trace` показывает путь сообщения по кластеру (16.5).

**NATS 2.12**

- **Атомарная пакетная публикация.** Группа сообщений записывается в стрим целиком или не записывается вовсе, что помогает с событиями, которые должны появиться вместе.
- **Распределённые счётчики.** Стрим можно настроить так, чтобы сообщения были инкрементами счётчика по subject, корректно сливающимися через mirrors и sources.

Если используешь что-то из этого, зафиксируй минимальную версию сервера в документации деплоя: старые серверы в смешанном кластере могут игнорировать или отвергать новые поля.

---

# Шпаргалка NATS CLI

```bash
# контексты
nats context add prod --server nats://a:4222,nats://b:4222 --creds ./app.creds
nats context select prod
nats context ls

# базовое
nats pub shop.orders.created '{"id":1}' --count 10
nats pub shop.orders.created '{"id":1}' -H "Nats-Msg-Id:evt-1"
nats sub "shop.>" --headers-only
nats sub "shop.orders.created" --queue workers
nats request service.echo "ping" --timeout 2s
nats reply service.echo --echo

# стримы
nats stream ls
nats stream add ORDERS --subjects "shop.orders.>" --defaults
nats stream info ORDERS
nats stream view ORDERS
nats stream get ORDERS 42
nats stream subjects ORDERS
nats stream edit ORDERS --max-age 48h
nats stream purge ORDERS --subject "shop.orders.created"
nats stream rmm ORDERS 42
nats stream backup ORDERS ./bk && nats stream restore ./bk
nats stream report

# консьюмеры
nats consumer add ORDERS BILLING --pull --ack explicit --defaults
nats consumer ls ORDERS
nats consumer info ORDERS BILLING
nats consumer next ORDERS BILLING --count 5
nats consumer report ORDERS
nats consumer rm ORDERS BILLING

# KV
nats kv add CONFIG --history 5
nats kv put CONFIG key value
nats kv get CONFIG key
nats kv watch CONFIG
nats kv history CONFIG key

# Object store
nats object add ASSETS
nats object put ASSETS ./file.bin
nats object get ASSETS file.bin --output ./out.bin
nats object ls ASSETS

# микросервисы
nats micro ls
nats micro info pricing
nats micro stats pricing

# сервер и диагностика (контекст системного аккаунта)
nats server list
nats server report jetstream
nats server report connections
nats server check stream --stream ORDERS --peer-expect 3
nats server cluster step-down
nats stream cluster step-down ORDERS
nats latency --server-b nats://b:4222
nats events
nats traffic
nats bench pub bench.test --msgs 100000
```

---

# Шпаргалка важных настроек

**Стрим (надёжный, для бизнес-событий):**

```
storage: file
replicas: 3
retention: limits
discard: old
max_age: 720h
max_bytes: 50GB
duplicate_window: 2m
```

**Стрим (очередь задач):**

```
storage: file
replicas: 3
retention: workqueue
discard: new
max_msgs: 1000000
max_age: 24h
```

**Консьюмер (типовой сервис):**

```
pull
ack_policy: explicit
ack_wait: 30s            (чуть больше p99 обработки)
max_deliver: 5
max_ack_pending: 500     (≈ числу воркеров × 10)
backoff: 1s, 10s, 1m, 5m   (заменяет ack_wait для повторов; max_deliver >= 4, а 5 использует все задержки)
filter_subject: конкретный subject
```

**Клиент:**

```
все адреса кластера в строке подключения
max_reconnects: -1
reconnect_wait: 500ms
Drain() при остановке
Nats-Msg-Id на каждом бизнес-событии
Ack() после коммита в БД
```

**Сервер:**

```
max_payload: 1MB
jetstream.store_dir на локальном NVMe
http: 8222 (не наружу)
TLS везде: клиенты, routes, gateways, leafnodes
лимиты JetStream на каждый аккаунт
```

---

# Вопросы на собеседовании по NATS с ответами

**Junior**

1. **Что такое subject и чем он отличается от очереди?** Subject это иерархический адрес сообщения. Он не существует как объект, не требует создания и не хранит сообщения сам по себе.
2. **Чем `*` отличается от `>`?** `*` заменяет ровно один токен, `>` один или несколько и только в конце подписки.
3. **Что произойдёт, если опубликовать в subject, на который никто не подписан?** В Core NATS ничего: сообщение исчезнет. Если subject захвачен стримом JetStream, сообщение сохранится.
4. **Что такое queue group?** Группа подписчиков, между которыми сервер распределяет сообщения: копию получает один участник группы.
5. **Как работает request-reply?** Клиент создаёт временный inbox, указывает его в поле reply, сервис отвечает в этот subject.
6. **Чем Core NATS отличается от JetStream?** Core это доставка в памяти без гарантий (at-most-once), JetStream добавляет хранение, подтверждения и повторную доставку.
7. **Что такое стрим и консьюмер?** Стрим хранит сообщения по заданным subjects, консьюмер это серверный курсор с политикой доставки и ack.
8. **Что делает `Ack()`?** Подтверждает обработку: сервер перестаёт считать сообщение неподтверждённым и не доставит его снова.
9. **Зачем нужен `Drain()`?** Корректно завершить работу: отписаться, дообработать полученное и только потом закрыть соединение.
10. **Какой максимальный размер сообщения?** По умолчанию 1 МБ, настраивается, но увеличивать не рекомендуется.

**Middle**

11. **Чем `Nak` отличается от `Term`?** `Nak` возвращает сообщение в очередь для повторной доставки, `Term` говорит «никогда больше не доставлять».
12. **Зачем нужен `InProgress()`?** Сбросить таймер `AckWait` при долгой обработке, чтобы сервер не начал повторную доставку параллельно.
13. **Что такое `max_ack_pending` и почему это важно?** Лимит неподтверждённых сообщений у консьюмера: он же ограничитель параллелизма и защита от перегрузки.
14. **Три политики retention и когда какая?** `limits` для событийных логов с replay, `interest` для pub/sub с гарантиями без истории, `workqueue` для очередей задач.
15. **Почему один subject не может быть в двух стримах?** Иначе было бы неоднозначно, куда записывать сообщение; для копий есть mirror и source.
16. **Как работает дедупликация?** Стрим помнит `Nats-Msg-Id` в течение `duplicate_window` и отвечает `PubAck{duplicate:true}` на повтор.
17. **Как добиться exactly-once?** Дедупликация на публикации плюс идемпотентная обработка на стороне потребителя. Одной настройки не существует.
18. **Pull или push консьюмер и почему?** Pull: естественный flow control, проще масштабировать, ничего не теряется при падении клиента.
19. **Как масштабировать обработку?** Несколько экземпляров на один durable pull-консьюмер; партиций и rebalance нет.
20. **Как гарантировать порядок по сущности?** Отдельный subject на сущность и консьюмер с фильтром, либо шардирование по токену subject, либо `max_ack_pending=1`.
21. **Что такое slow consumer?** Подписчик, который не успевает читать; сервер отбрасывает его сообщения и увеличивает счётчик в `/varz`.
22. **Что такое `num_pending`?** Число сообщений, которые консьюмеру ещё предстоит получить, то есть lag.
23. **Как сделать DLQ?** Подписаться на advisory `MAX_DELIVERIES`, достать оригинал по `stream_seq` и переложить в отдельный стрим.
24. **Как устроен KV Store?** Это стрим с `max_msgs_per_subject` и subjects `$KV.<bucket>.>`, плюс клиентский API с версиями и watch.
25. **Зачем нужны accounts?** Полная изоляция пространства subjects: multi-tenancy без отдельных кластеров.

**Senior**

26. **Почему кластер из 2 узлов хуже, чем из 1?** JetStream использует RAFT: при двух узлах кворум теряется при падении любого, и стрим становится недоступен на запись.
27. **Как работает supercluster и почему трафик не идёт зря через океан?** Gateways передают сообщение в другой кластер только при наличии там интереса, а request-reply предпочитает локального ответчика.
28. **Когда нужен leaf node?** Edge и филиалы: локальная работа при обрыве связи, свой JetStream-домен, исходящее соединение через NAT, изоляция аккаунта.
29. **Mirror или source?** Mirror это точная копия одного стрима с сохранением sequence; source агрегирует несколько стримов и перенумеровывает сообщения.
30. **Как спроектировать subject-пространство на 5 лет?** От общего к частному, домен-сущность-событие, версия отдельно, идентификаторы осознанно, никаких меток времени, права выдаются по префиксам.
31. **Как не потерять событие при падении сервиса между БД и NATS?** Transactional outbox плюс `Nats-Msg-Id` для дедупликации при повторной публикации relay.
32. **Что делать, если `num_redelivered` растёт?** Найти ядовитые сообщения, проверить `AckWait` против реального p99, убедиться, что обработчик различает временные и постоянные ошибки, посмотреть DLQ.
33. **Как выбрать между NATS и Kafka?** По потребностям в истории и экосистеме данных против потребностей в низкой задержке, RPC, мультирегионе и простоте эксплуатации.

---

# FAQ: частые вопросы про NATS

**NATS теряет сообщения?**
Core NATS — да, это его модель (at-most-once). JetStream — нет, при правильной конфигурации (`replicas=3`, ack после обработки, конечный `max_deliver`).

**Нужно ли создавать subject перед публикацией?**
Нет. Subject существует в момент публикации. Создавать нужно только стримы, консьюмеров и бакеты.

**Можно ли заменить Kafka на NATS?**
Иногда да, иногда нет. Для событийного обмена между микросервисами, RPC, edge и мультирегиона NATS обычно удобнее. Для big-data пайплайнов, CDC и экосистемы Connect/Streams остаётся Kafka.

**Почему `no responders available`?**
Никто не подписан на subject запроса: сервис не запущен, опечатка в subject или прав нет. Это не таймаут, а мгновенный ответ сервера.

**Почему `jetstream not enabled for account`?**
Сервер запущен без `-js`, либо у аккаунта не включён JetStream (`jetstream: enabled` или лимиты в конфиге аккаунта).

**Как перечитать стрим с начала?**
Пересоздать консьюмера с `--deliver all` или со `--start-time`. Отдельной команды «сбросить offset» нет.

**Сколько сообщений может хранить стрим?**
Ограничено только диском и лимитами. Терабайты возможны, но NATS не проектировался как многолетнее хранилище: для этого лучше выгружать данные в S3 или Kafka.

**Как обновить конфигурацию стрима без потери данных?**
`nats stream edit`. Несколько базовых вещей (например, storage type) на месте не меняются. Для них создай новый стрим, который **берёт source** из старого, дождись, пока он догонит, переключи публикацию на subjects нового стрима и удали старый. Именно source, а не mirror: mirror не принимает прямые публикации.

**Чем `interest` retention отличается от `workqueue`?**
`interest` удаляет сообщение, когда его подтвердили **все** консьюмеры, `workqueue` — когда подтвердил **любой один**.

**Нужен ли Schema Registry?**
Встроенного нет. Контракты ведут в отдельном репозитории (Protobuf, Avro, JSON Schema) и версионируют через subject или заголовок.

**Работает ли NATS с MQTT и WebSocket?**
Да, сервер умеет оба протокола нативно: `mqtt { port: 1883 }` и `websocket { port: 8080 }`. Для MQTT нужны JetStream и `server_name`. Подробнее в 12.6.

**Как мигрировать с RabbitMQ?**
Очереди → стримы с `workqueue`, exchange fanout → wildcard-подписки, RPC → request-reply, DLX → DLQ на advisory. Сложная маршрутизация по headers прямого аналога не имеет и переносится в subject-иерархию.

---

# Глоссарий NATS

| Термин | Значение |
|---|---|
| **Subject** | Иерархический адрес сообщения из токенов, разделённых точкой |
| **Wildcard** | `*` (один токен) и `>` (хвост) в подписке |
| **Core NATS** | Слой доставки в памяти, at-most-once |
| **JetStream** | Слой персистентности: стримы, консьюмеры, KV, Object Store |
| **Queue group** | Группа подписчиков, делящих сообщения между собой |
| **Request-Reply** | Встроенный RPC через reply subject |
| **Inbox** | Временный subject вида `_INBOX.…` для ответов |
| **Stream** | Хранилище сообщений по набору subjects |
| **Consumer** | Серверный курсор со своей политикой доставки и ack |
| **Durable / Ephemeral** | Консьюмер, который живёт постоянно / только пока подключён клиент |
| **Stream sequence** | Порядковый номер сообщения в стриме |
| **Ack floor** | До какого seq консьюмер подтвердил всё подряд |
| **num_pending** | Сколько сообщений консьюмеру ещё предстоит получить (lag) |
| **AckWait** | Время ожидания подтверждения перед повторной доставкой |
| **MaxDeliver** | Максимальное число попыток доставки |
| **MaxAckPending** | Лимит неподтверждённых сообщений у консьюмера |
| **Nak / Term / InProgress** | Вернуть в очередь / никогда не повторять / продлить обработку |
| **Nats-Msg-Id** | Заголовок для дедупликации публикации |
| **Duplicate window** | Окно, в течение которого стрим помнит `Nats-Msg-Id` |
| **Retention** | `limits`, `interest` или `workqueue` |
| **Discard** | `old` или `new`: что делать при достижении лимита |
| **Mirror / Source** | Точная копия одного стрима / агрегация нескольких |
| **Advisory** | Служебное событие JetStream (`$JS.EVENT.ADVISORY.>`) |
| **KV bucket** | Key-Value поверх стрима с `max_msgs_per_subject` |
| **Object bucket** | Хранилище файлов, разбитых на чанки |
| **Account** | Изолированное пространство subjects |
| **Export / Import** | Явный обмен subjects между аккаунтами |
| **NKey** | Ed25519-ключ для аутентификации |
| **Creds / JWT** | Файл с подписанными правами пользователя |
| **Route** | Соединение между серверами одного кластера |
| **Gateway** | Соединение между кластерами (supercluster) |
| **Leaf node** | Сервер на краю, подключённый исходящим соединением |
| **JetStream domain** | Имя изолированного пространства JetStream (hub и edge) |
| **Slow consumer** | Подписчик, не успевающий читать; его сообщения отбрасываются |
| **RAFT group** | Кворум серверов, реплицирующих стрим или мета-состояние |

---

# Официальные источники и что читать дальше

- **Документация NATS** — https://docs.nats.io
- **Репозиторий сервера** — https://github.com/nats-io/nats-server
- **nats CLI** — https://github.com/nats-io/natscli
- **Примеры на всех языках (nats by example)** — https://natsbyexample.com
- **Go-клиент** — https://github.com/nats-io/nats.go (обрати внимание на пакет `jetstream`)
- **Python-клиент** — https://github.com/nats-io/nats.py
- **Java-клиент** — https://github.com/nats-io/nats.java
- **Helm-чарты и Kubernetes** — https://github.com/nats-io/k8s
- **nsc (управление аккаунтами и JWT)** — https://github.com/nats-io/nsc
- **ADR: архитектурные решения NATS** — https://github.com/nats-io/nats-architecture-and-design
- **Slack-сообщество** — https://slack.nats.io

---

## Как помочь проекту

Нашёл ошибку, неточность или устаревшую настройку? Открывай issue или присылай pull request. Особенно ценны:

- примеры на языках, которых здесь нет (Rust, C#, Node.js, Elixir);
- реальные production-истории и разборы инцидентов;
- уточнения по новым версиям NATS Server.

⭐ Если курс помог, поставь звезду: так его найдут другие разработчики.

**Лицензия:** текст курса распространяется по лицензии [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.ru), примеры кода — по [MIT License](../LICENSE). Можно свободно использовать, адаптировать и распространять материалы, в том числе для внутренних воркшопов, с указанием источника.
