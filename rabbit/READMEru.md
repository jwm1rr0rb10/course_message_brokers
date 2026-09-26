# RabbitMQ курс 2026: бесплатный курс по RabbitMQ с нуля до профи на русском

![RabbitMQ 4.3](https://img.shields.io/badge/RabbitMQ-4.3-FF6600?logo=rabbitmq&logoColor=white)
![Quorum queues](https://img.shields.io/badge/quorum%20queues-Raft-blue)
![Курс на русском](https://img.shields.io/badge/язык-русский-red)
![Бесплатный курс](https://img.shields.io/badge/цена-бесплатно-brightgreen)
![От junior до senior](https://img.shields.io/badge/уровень-junior%20→%20senior-orange)

> **Полный бесплатный курс по RabbitMQ на русском языке.** Теория, практика, Docker, Python, Go и Java, AMQP 0-9-1, exchanges и маршрутизация, classic и quorum queues, streams, publisher confirms, ack и prefetch, гарантии доставки и идемпотентность, dead letter exchanges, TTL и отложенные ретраи, кластер и Raft, RPC, AMQP 1.0 и MQTT, Federation и Shovel, мониторинг, безопасность и production-архитектура. Всё в одном README, актуально для **RabbitMQ 4.3 (2026)**.

**RabbitMQ без воды:** каждый модуль — это понятная теория, схемы, команды, которые можно запустить у себя, типичные ошибки и вопросы для самопроверки. Курс подходит, чтобы выучить RabbitMQ с нуля, подготовиться к собеседованию на backend- или DevOps-позицию и спроектировать надёжную систему на RabbitMQ в продакшене.

⭐ Если курс полезен, поставь звезду репозиторию: так его найдут другие разработчики.

🇬🇧 English version: [README.md](README.md)

---

## Для кого этот курс по RabbitMQ

| Кто ты | Что получишь |
|---|---|
| **Новичок** в брокерах сообщений | Что такое брокер, чем очередь отличается от лога, как поднять RabbitMQ за минуту |
| **Backend-разработчик** (примеры на Python, Go и Java) | Publisher confirms, ack и prefetch, ретраи и DLX, идемпотентная обработка |
| **Разработчик микросервисов** | Маршрутизация через exchanges, RPC, work queues, outbox, саги |
| **DevOps / SRE** | Кластер, quorum queues, Khepri и Raft, мониторинг, алармы памяти и диска, апгрейды, Kubernetes |
| **Архитектор / Tech Lead** | Проектирование топологии, выбор типа очереди, RabbitMQ против Kafka и NATS, антипаттерны |
| **IoT** | MQTT и WebSocket прямо в RabbitMQ |
| **Готовишься к собеседованию** | 35 вопросов по RabbitMQ с ответами уровня junior, middle и senior |

## Что ты будешь уметь после курса

- объяснить модель AMQP 0-9-1: connection, channel, exchange, queue, binding, routing key, virtual host;
- поднять кластер RabbitMQ из трёх узлов в Docker и ломать его, наблюдая выборы лидеров quorum-очередей;
- выбирать тип exchange (direct, fanout, topic, headers) и проектировать маршрутизацию;
- выбирать тип очереди: classic, quorum или stream — и понимать, почему;
- публиковать сообщения без потерь: persistent-сообщения, publisher confirms, `mandatory` и возвраты;
- потреблять сообщения правильно: ручной ack, `nack` и `reject`, prefetch, повторная доставка;
- строить ретраи с задержкой и dead letter exchanges, ограничивать число доставок;
- понимать, как работает кластер на Raft и что происходит при падении узлов;
- использовать streams для повторного чтения и больших потоков;
- делать RPC через RabbitMQ и direct reply-to;
- подключать клиентов по AMQP 1.0, MQTT и STOMP;
- связывать кластеры через Federation и Shovel;
- ограничивать ресурсы политиками и понимать алармы памяти и диска;
- мониторить очереди, консьюмеров и узлы через Prometheus;
- включать TLS, настраивать пользователей, права и OAuth 2;
- обновлять кластер без простоя.

---

## Оглавление

- [Для кого этот курс по RabbitMQ](#для-кого-этот-курс-по-rabbitmq)
- [Что ты будешь уметь после курса](#что-ты-будешь-уметь-после-курса)
- [Как проходить курс](#как-проходить-курс)
- [Модуль 0. Что такое RabbitMQ и зачем он нужен](#модуль-0-что-такое-rabbitmq-и-зачем-он-нужен)
- [Модуль 1. Архитектура: соединение, канал, exchange, очередь, binding](#модуль-1-архитектура-соединение-канал-exchange-очередь-binding)
- [Модуль 2. Установка RabbitMQ в Docker и первые команды](#модуль-2-установка-rabbitmq-в-docker-и-первые-команды)
- [Модуль 3. Exchanges и маршрутизация](#модуль-3-exchanges-и-маршрутизация)
- [Модуль 4. Очереди: classic, quorum и streams](#модуль-4-очереди-classic-quorum-и-streams)
- [Модуль 5. Publisher: persistent-сообщения, confirms и mandatory](#модуль-5-publisher-persistent-сообщения-confirms-и-mandatory)
- [Модуль 6. Consumer: ack, prefetch и повторная доставка](#модуль-6-consumer-ack-prefetch-и-повторная-доставка)
- [Модуль 7. Гарантии доставки, идемпотентность и outbox](#модуль-7-гарантии-доставки-идемпотентность-и-outbox)
- [Модуль 8. Dead letter exchanges, TTL и ретраи с задержкой](#модуль-8-dead-letter-exchanges-ttl-и-ретраи-с-задержкой)
- [Модуль 9. Кластер и quorum queues: Raft, Khepri, отказоустойчивость](#модуль-9-кластер-и-quorum-queues-raft-khepri-отказоустойчивость)
- [Модуль 10. Streams и super streams](#модуль-10-streams-и-super-streams)
- [Модуль 11. Паттерны: work queues, pub/sub, RPC, приоритеты](#модуль-11-паттерны-work-queues-pubsub-rpc-приоритеты)
- [Модуль 12. Протоколы: AMQP 1.0, MQTT, STOMP, WebSocket](#модуль-12-протоколы-amqp-10-mqtt-stomp-websocket)
- [Модуль 13. Несколько дата-центров: Federation и Shovel](#модуль-13-несколько-дата-центров-federation-и-shovel)
- [Модуль 14. Политики, лимиты и управление ресурсами](#модуль-14-политики-лимиты-и-управление-ресурсами)
- [Модуль 15. Производительность и тюнинг](#модуль-15-производительность-и-тюнинг)
- [Модуль 16. Мониторинг и алерты](#модуль-16-мониторинг-и-алерты)
- [Модуль 17. Безопасность: TLS, пользователи, права, OAuth 2](#модуль-17-безопасность-tls-пользователи-права-oauth-2)
- [Модуль 18. RabbitMQ в продакшене: Kubernetes, эксплуатация, апгрейды](#модуль-18-rabbitmq-в-продакшене-kubernetes-эксплуатация-апгрейды)
- [Модуль 19. Итоговый проект: event-driven интернет-магазин](#модуль-19-итоговый-проект-event-driven-интернет-магазин)
- [Шпаргалка RabbitMQ CLI и HTTP API](#шпаргалка-rabbitmq-cli-и-http-api)
- [Шпаргалка важных настроек](#шпаргалка-важных-настроек)
- [Вопросы на собеседовании по RabbitMQ с ответами](#вопросы-на-собеседовании-по-rabbitmq-с-ответами)
- [FAQ](#faq)
- [Глоссарий RabbitMQ](#глоссарий-rabbitmq)
- [Официальные источники и что читать дальше](#официальные-источники-и-что-читать-дальше)

---

## Как проходить курс

1. **Иди по порядку.** Модули 0–6 — фундамент. Без понимания exchanges, bindings и ack всё остальное будет магией.
2. **Запускай каждую команду.** RabbitMQ учится руками, а management UI показывает всё, что происходит внутри: очереди, скорость, неподтверждённые сообщения.
3. **Ломай кластер.** Останавливай узлы, убивай консьюмеров посреди обработки, заполняй очередь до лимита. Так появляется production-опыт.
4. **Отвечай на вопросы в конце модуля** вслух, как на собеседовании.
5. **Сделай итоговый проект.** Он собирает все темы в одну систему.

**Что нужно установить:** Docker и Docker Compose, Git, любую IDE. Для примеров на Python нужен Python 3.10+, для Go — актуальный Go, для Java — Java 17+.

**Версия:** все примеры написаны для RabbitMQ 4.3, образ `rabbitmq:4.3-management`. Важные изменения последних версий, которые встретятся в курсе:

- с 4.0 **удалены classic mirrored queues**: реплицированные очереди — это quorum queues и streams;
- с 4.3 метаданные кластера хранит только **Khepri** (на Raft), Mnesia и стратегии обработки partition удалены;
- с 4.3 по умолчанию **запрещены** неустаревающие (non-durable) неэксклюзивные очереди и `global` prefetch;
- минимальная версия Erlang для 4.3 — 27.

**Запускаемые примеры:** кластер, смок-тест команд, код на Go и Python и интеграционные тесты утверждений курса лежат в [`examples/`](examples/). CI каждую неделю прогоняет их на 4.3 и на свежем образе RabbitMQ, так что если новая версия изменит описанное здесь поведение, сборка покраснеет.

---

# Модуль 0. Что такое RabbitMQ и зачем он нужен

## 0.1 RabbitMQ простыми словами

**RabbitMQ** — это брокер сообщений: сервер, который принимает сообщения от одних программ, раскладывает их по очередям и отдаёт другим программам.

Три вещи, которые делает RabbitMQ:

1. **Маршрутизирует**: по правилам (exchanges и bindings) решает, в какие очереди попадёт сообщение.
2. **Хранит**: держит сообщения в очереди, пока их не заберёт и не подтвердит консьюмер.
3. **Доставляет**: раздаёт сообщения консьюмерам, следит за подтверждениями и доставляет повторно то, что не подтвердили.

Главная идея RabbitMQ — **умный брокер, простые клиенты**. Producer не знает, какие очереди существуют и кто их читает: он отправляет сообщение в exchange с ключом маршрутизации, а брокер сам решает, куда его положить. Консьюмер просто читает свою очередь.

RabbitMQ появился в 2007 году как реализация открытого протокола AMQP, написан на Erlang — языке, созданном для отказоустойчивых телеком-систем. Сейчас его развивает команда в Broadcom (ранее VMware), исходный код открыт под Mozilla Public License 2.0.

## 0.2 Проблема, которую решает RabbitMQ

Интернет-магазин: пользователь оформил заказ, и после этого нужно отправить письмо, списать деньги, зарезервировать товар, сгенерировать PDF-чек. Если делать всё синхронно в обработчике HTTP-запроса:

```
POST /orders
   |
   +--> записать заказ в базу         20 мс
   +--> списать деньги                300 мс
   +--> зарезервировать на складе     150 мс
   +--> сгенерировать PDF-чек         2 000 мс
   +--> отправить письмо              800 мс
   |
ответ пользователю через 3+ секунды, и любая ошибка ломает весь заказ
```

| Вопрос | Проблема синхронной обработки |
|---|---|
| Почтовый сервис лежит | Заказ падает, хотя письмо можно отправить позже |
| Генерация PDF медленная | Пользователь ждёт |
| Чёрная пятница, в 10 раз больше заказов | Все сервисы падают одновременно |
| Нужно добавить ещё одно действие | Меняем и перевыкатываем сервис заказов |
| Задача упала посередине | Непонятно, что уже сделано |

## 0.3 Та же система с RabbitMQ

```
POST /orders -> записать заказ -> publish "order.created" -> ответ за 30 мс
                                         |
                                         v
                              +---------------------+
                              |  exchange "orders"  |
                              +---------------------+
                              /      |       |       \
                             v       v       v        v
                        [payments] [stock] [receipts] [emails]   <- очереди
                            |        |       |          |
                         воркеры  воркеры  воркеры    воркеры
```

Что ты получаешь:

- **Быстрый ответ.** Долгая работа уходит в фон.
- **Буфер для пиков.** Очередь копит задачи, воркеры разбирают их в своём темпе.
- **Независимость.** Упал почтовый сервис — письма ждут в очереди, остальное работает.
- **Масштабирование.** Добавил воркеров на очередь — задачи разбираются быстрее.
- **Гибкую маршрутизацию.** Новая очередь подключается к exchange без изменений в producer'е.
- **Повторную доставку.** Воркер упал, не подтвердив сообщение, — брокер отдаст его другому.

## 0.4 Очередь против лога: главное, что нужно понять о RabbitMQ

Это **самая важная мысль курса**, особенно если ты знаешь Kafka.

```
ОЧЕРЕДЬ (RabbitMQ classic и quorum queues)   ЛОГ (Kafka, RabbitMQ streams)
------------------------------------------    ---------------------------------
сообщение удаляется после ack                  сообщение остаётся до retention
каждое сообщение получает один консьюмер       любое число независимых читателей
брокер помнит, что доставлено и подтверждено   читатель помнит свою позицию (offset)
перечитать нельзя                              можно перечитать с любого места
ack/nack каждого сообщения отдельно            коммитится позиция
параллелизм = число консьюмеров на очереди     параллелизм = число партиций
```

| Вопрос | Очередь RabbitMQ | Лог |
|---|---|---|
| Что после обработки | Сообщение удаляется | Остаётся, двигается offset |
| Нужно ли десяти системам читать одно событие | Каждой своя очередь, exchange копирует сообщение | Один лог, у каждой свой offset |
| Перечитать вчерашнее | Нельзя | Можно |
| Медленное сообщение | Не мешает остальным: другие консьюмеры берут следующие | Блокирует партицию |
| Отложить и повторить одно сообщение | Да: nack, DLX, TTL, delayed retry | Сложно |
| Маршрутизация | Богатая: exchanges, ключи, заголовки | По топику и ключу партиции |

**Правило:** RabbitMQ-очереди — когда нужно раздать задачи воркерам, гибко маршрутизировать и надёжно довести каждое сообщение до обработки. Лог — когда важна история и повторное чтение. В RabbitMQ есть и то и другое: для лога — **streams** (модуль 10).

## 0.5 Что такое сообщение

```
Exchange:    orders
Routing key: order.created.eu
Properties:
  delivery_mode:  2                 (persistent: пережить перезапуск)
  content_type:   application/json
  message_id:     5f1c2a7e-9b1d-4c1e-8a4e-0c7b2d9f1a11
  correlation_id: (для RPC)
  reply_to:       (для RPC)
  expiration:     "60000"           (TTL сообщения, мс)
  priority:       5
  timestamp:      1789466400
  headers:        {event-type: OrderCreated, trace-id: 4bf92f3577b34da6}
Body:        {"order_id":"order-123","user_id":"user-42","amount":4990}
```

- **Body** — просто байты. Формат (JSON, Protobuf, Avro) выбираешь ты.
- **Routing key** — строка, по которой exchange решает, куда положить сообщение.
- **Properties** — стандартные поля AMQP. Самые важные: `delivery_mode` (persistent или нет), `message_id` (для дедупликации), `correlation_id` и `reply_to` (для RPC), `headers` (свои метаданные).

Максимальный размер сообщения по умолчанию — **16 МБ** (`max_message_size`, верхний предел 512 МБ). Но большие сообщения — плохая идея: они давят на память и сеть. Файлы кладут в объектное хранилище, а в сообщение — ссылку.

## 0.6 RabbitMQ против Kafka, NATS и Redis

| Критерий | RabbitMQ | Apache Kafka | NATS JetStream | Redis Streams / Pub/Sub |
|---|---|---|---|---|
| Модель | Брокер очередей + streams | Распределённый лог | Subjects + стримы | Структуры данных в памяти |
| Маршрутизация | Богатая: direct, topic, fanout, headers | Топик и ключ | Иерархические subjects | Каналы и ключи |
| Подтверждение сообщения | Каждого отдельно | Коммит позиции | Каждого отдельно | XACK для Streams |
| Повторное чтение | Streams | Да, ядро продукта | Да | Streams |
| Задержанные ретраи | DLX + TTL, delayed retry в quorum queues | Вручную | NakWithDelay, backoff | Вручную |
| Приоритеты | Да | Нет | Нет | Нет |
| Протоколы | AMQP 0-9-1, AMQP 1.0, MQTT, STOMP, Stream | Kafka protocol | NATS, MQTT, WebSocket | RESP |
| Сильная сторона | Гибкая маршрутизация, очереди задач, зрелость | Пропускная способность, история, интеграция данных | Простота, низкая задержка, edge | Скорость, если Redis уже есть |

**Честно про выбор:** RabbitMQ — отличный выбор для очередей задач, интеграции сервисов с непростой маршрутизацией и корпоративных систем, где нужны гарантии доставки каждого сообщения. Если нужен многолетний лог событий, CDC и аналитические конвейеры, — Kafka. Если нужен лёгкий RPC и edge — посмотри на NATS. Часто в одной компании живут RabbitMQ для задач и команд и Kafka для потоков данных.

### RabbitMQ и HTTP вместе

```
Клиент --HTTP--> Order API --(сохранил заказ, вернул 201)--> Клиент
                     |
                     +--publish order.created--> RabbitMQ --> воркеры
```

HTTP — когда пользователь ждёт ответа. RabbitMQ — для фоновой работы, асинхронных событий и развязки сервисов.

## 0.7 Где используют RabbitMQ

| Сценарий | Как применяется |
|---|---|
| **Очереди задач** | Генерация отчётов, обработка изображений, отправка писем |
| **Интеграция микросервисов** | События и команды через topic exchanges |
| **RPC** | Запрос через очередь, ответ в reply-to |
| **Буфер перед медленной системой** | Очередь выравнивает нагрузку на базу или внешнее API |
| **Отложенные задачи и ретраи** | TTL + DLX, delayed retry в quorum queues |
| **IoT** | MQTT-устройства подключаются прямо к брокеру |
| **Фан-аут уведомлений** | Fanout exchange раздаёт событие всем подписчикам |
| **Лог событий с повторным чтением** | Streams |

## 0.8 Когда RabbitMQ не нужен

- Нужна многолетняя история событий и аналитика: Kafka.
- Одна-две фоновые задачи в монолите: хватит очереди в PostgreSQL или Redis.
- Нужен синхронный ответ пользователю: HTTP или gRPC.
- Нужно миллионы сообщений в секунду с повторным чтением: Kafka или RabbitMQ streams, но не классические очереди.
- Нужны транзакции между базой и брокером: их не даёт никто, проектируй outbox (модуль 7).

## 0.9 Чего RabbitMQ НЕ сделает за тебя

- идемпотентную обработку в приложении;
- схемы сообщений и их версионирование (Schema Registry в RabbitMQ нет);
- стратегию ретраев и обработку «ядовитых» сообщений;
- мониторинг длины очередей и консьюмеров;
- права доступа и лимиты;
- продуманную топологию: переименование exchanges и очередей потом болезненно.

### Вопросы для самопроверки

1. Чем очередь отличается от лога, и что это значит для консьюмеров?
2. Почему producer в RabbitMQ не знает, в какие очереди попадёт сообщение?
3. Какие свойства сообщения нужны для RPC?
4. Когда ты выберешь Kafka вместо RabbitMQ?

---

# Модуль 1. Архитектура: соединение, канал, exchange, очередь, binding

## 1.1 Главная иерархия

```
Кластер RabbitMQ (узлы rabbit@rabbit-1, rabbit@rabbit-2, rabbit@rabbit-3)
  └── Virtual host (изолированное пространство, например "/" или "shop")
        ├── Exchanges (точки входа: сюда публикуют)
        │     └── Bindings (правила: exchange -> очередь по ключу)
        └── Queues (здесь лежат сообщения)
              └── Consumers (получают сообщения из очереди)

и отдельно, на стороне клиента:
  Connection (одно TCP-соединение)
    └── Channels (лёгкие логические каналы внутри соединения)
```

Запомни эту картинку. На ней держится весь курс.

## 1.2 Основные компоненты

| Компонент | Что это | Аналогия |
|---|---|---|
| **Message** | Тело + свойства + routing key | Письмо с адресом |
| **Producer** | Клиент, публикующий сообщения | Отправитель |
| **Exchange** | Принимает сообщения и маршрутизирует их в очереди | Сортировочный центр |
| **Queue** | Упорядоченный буфер сообщений | Почтовый ящик |
| **Binding** | Правило, связывающее exchange с очередью | Правило сортировки |
| **Routing key** | Строка, по которой работает маршрутизация | Адрес на конверте |
| **Consumer** | Клиент, получающий сообщения из очереди | Получатель |
| **Connection** | TCP-соединение клиента с брокером | Телефонная линия |
| **Channel** | Логический канал внутри соединения | Разговор по линии |
| **Virtual host** | Изолированное пространство имён | Отдельный брокер |

## 1.3 Путь сообщения

```
producer                                                        consumer
   |                                                               ^
   | basic.publish(exchange="orders", routing_key="order.created") |
   v                                                               |
+----------+   binding "order.*"    +-----------------+    deliver |
| exchange | ---------------------> | queue: payments | -----------+
|  orders  |                        +-----------------+
|  (topic) |   binding "order.#"    +-----------------+
|          | ---------------------> | queue: audit    | ---> другой consumer
+----------+                        +-----------------+
      |
      | ни один binding не подошёл -> сообщение отброшено
      |   (или возвращено producer'у, если mandatory=true)
```

Ключевые следствия:

- **publish всегда идёт в exchange**, никогда напрямую в очередь. Даже «публикация в очередь» — это публикация в default exchange (модуль 3);
- одно сообщение может попасть **в несколько очередей** — брокер скопирует его в каждую подходящую;
- если ни одна очередь не подошла, сообщение **молча пропадает**. Это одна из самых частых причин «потерянных» сообщений (защита — `mandatory` и alternate exchange, модули 3 и 5);
- каждая очередь доставляет каждое сообщение **одному** из своих консьюмеров.

## 1.4 Connection и channel

```
приложение
  |
  +-- Connection (TCP, TLS, аутентификация, heartbeats)
        +-- Channel 1: публикация заказов
        +-- Channel 2: консьюмер очереди payments
        +-- Channel 3: консьюмер очереди stock
```

- **Connection** — дорогое: TCP-рукопожатие, TLS, аутентификация, память на брокере. Открывай одно-два на процесс и держи долго.
- **Channel** — дешёвое логическое соединение внутри connection. Все операции AMQP (declare, publish, consume, ack) идут через канал.
- **Каналы не потокобезопасны** в большинстве клиентов: один канал — один поток (или одна горутина).
- Ошибка протокола (например, публикация в несуществующий exchange) **закрывает канал**, а не соединение. Приложение должно уметь открыть канал заново.
- Не открывай соединение на каждое сообщение: это классический антипаттерн, который кладёт брокер при нагрузке.

Практическое правило: одно соединение для публикации и одно для потребления (чтобы flow control на публикации не тормозил доставку консьюмерам), каналы — по потоку.

## 1.5 Virtual hosts: изоляция внутри брокера

**Virtual host** (vhost) — отдельное пространство имён: свои exchanges, очереди, bindings, права и лимиты. Сообщения между vhost'ами не ходят.

```
vhost "/"        — по умолчанию
vhost "shop"     — exchanges orders, payments; очереди payments, stock
vhost "billing"  — свои очереди с теми же именами, но полностью отдельные
```

Vhost — удобная граница для команды или окружения: права пользователей выдаются на vhost, лимиты (число очередей и соединений) задаются на vhost.

## 1.6 Свойства очереди

| Свойство | Что значит |
|---|---|
| **durable** | Очередь переживает перезапуск брокера (её определение сохраняется) |
| **exclusive** | Очередь принадлежит одному соединению и удаляется при его закрытии |
| **auto-delete** | Очередь удаляется, когда отписывается последний консьюмер |
| **arguments** | Дополнительные параметры: тип (`x-queue-type`), TTL, лимиты, DLX |

Важное различие: **durable-очередь** и **persistent-сообщение** — разные вещи. Durable-очередь сохраняет своё определение. Чтобы пережили перезапуск и сами сообщения, у них должен быть `delivery_mode=2` (в quorum queues и streams сообщения хранятся на диске всегда).

С 4.3 очереди `durable=false` и `exclusive=false` одновременно **запрещены по умолчанию**. Для временных очередей используй `exclusive=true` или durable-очередь с TTL очереди (`x-expires`).

## 1.7 Три типа очередей

| Тип | Репликация | Хранение | Когда |
|---|---|---|---|
| **Classic** | Нет (живёт на одном узле) | Память + диск | Временные и эксклюзивные очереди, данные, которые не жалко |
| **Quorum** | Да, Raft на нескольких узлах | Диск | Всё, что важно: стандарт для продакшена |
| **Stream** | Да, репликация лога | Диск, лог | Повторное чтение, много читателей, большие объёмы |

Подробно — в модуле 4. Главное сейчас: **для важных данных используй quorum queues**. Classic mirrored queues, которые раньше давали репликацию, удалены в 4.0.

## 1.8 Цифры, которые описывают очередь

| Показатель | Что значит |
|---|---|
| **Ready** | Сообщения в очереди, готовые к доставке |
| **Unacked** | Доставлены консьюмерам, но ещё не подтверждены |
| **Total** | Ready + Unacked |
| **Consumers** | Сколько консьюмеров подписано |
| **Publish rate / Deliver rate / Ack rate** | Скорость входа, доставки и подтверждений |

**Растущий Ready** — консьюмеры не справляются или их нет. **Растущий Unacked** — консьюмеры забрали сообщения, но не подтверждают (зависли, медленные или забыли ack).

## 1.9 Первая ментальная модель

```
Публикация:
  producer -> канал -> exchange -> bindings решают, в какие очереди ->
  копия сообщения в каждую подходящую очередь ->
  (с publisher confirms) брокер подтверждает producer'у

Потребление:
  consumer подписан на очередь -> брокер доставляет до prefetch сообщений ->
  consumer обрабатывает -> ack -> сообщение удаляется из очереди
  nack/reject или падение consumer'а -> сообщение возвращается в очередь
  (или уходит в dead letter exchange)
```

### Вопросы для самопроверки

1. Почему producer публикует в exchange, а не в очередь?
2. Что произойдёт с сообщением, если ни один binding не подошёл?
3. Чем connection отличается от channel, и почему нельзя открывать соединение на каждое сообщение?
4. Чем durable-очередь отличается от persistent-сообщения?
5. Что означает растущий Unacked?

---

# Модуль 2. Установка RabbitMQ в Docker и первые команды

## 2.1 Самый быстрый способ запустить RabbitMQ

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin \
  rabbitmq:4.3-management
```

- `5672` — AMQP 0-9-1 и AMQP 1.0;
- `15672` — management UI и HTTP API: открой http://localhost:15672 (admin / admin);
- тег `-management` включает плагин управления.

Пользователь `guest` по умолчанию может подключаться **только с localhost** внутри контейнера, поэтому задаём своего пользователя сразу.

Проверь:

```bash
docker exec rabbitmq rabbitmq-diagnostics status
docker exec rabbitmq rabbitmq-diagnostics check_port_connectivity
docker exec rabbitmq rabbitmqctl list_queues name type messages consumers
```

## 2.2 Основные утилиты

| Утилита | Для чего |
|---|---|
| `rabbitmqctl` | Управление узлом: пользователи, права, vhosts, политики, кластер, списки объектов |
| `rabbitmq-diagnostics` | Здоровье и диагностика: статус, проверки, алармы, память |
| `rabbitmq-queues` | Операции с quorum queues и streams: реплики, лидеры |
| `rabbitmq-plugins` | Включение и выключение плагинов |
| `rabbitmq-upgrade` | Подготовка узла к обновлению (drain) |
| `rabbitmqadmin` | CLI поверх HTTP API: объявить exchange, очередь, опубликовать сообщение |
| Management UI | Всё то же в браузере + графики |

`rabbitmqadmin` второго поколения (v2) — отдельный бинарник, который работает с HTTP API и не требует доступа к узлу. В курсе для работы с объектами используется HTTP API через `curl` или `rabbitmqadmin`, а для операций с узлом — `rabbitmqctl` внутри контейнера.

## 2.3 Кластер из трёх узлов в Docker Compose

Для обучения нужен настоящий кластер: так можно останавливать узлы и смотреть, как quorum-очереди выбирают новых лидеров.

Узлы кластера RabbitMQ должны:

- иметь **одинаковый Erlang cookie** — общий секрет, по которому узлы и CLI доверяют друг другу;
- видеть друг друга по **стабильным именам хостов**: имя узла — `rabbit@<hostname>`;
- знать, с кем собираться в кластер: в Compose удобен **peer discovery** через `classic_config`.

Создай каталог:

```bash
mkdir rabbitmq-course && cd rabbitmq-course
```

`rabbitmq.conf`:

```ini
# узлы находят друг друга по списку и сами собираются в кластер
cluster_formation.peer_discovery_backend = classic_config
cluster_formation.classic_config.nodes.1 = rabbit@rabbit-1
cluster_formation.classic_config.nodes.2 = rabbit@rabbit-2
cluster_formation.classic_config.nodes.3 = rabbit@rabbit-3

# метрики для Prometheus на порту 15692
prometheus.return_per_object_metrics = false
```

`enabled_plugins`:

```erlang
[rabbitmq_management,rabbitmq_prometheus,rabbitmq_stream,rabbitmq_stream_management].
```

`docker-compose.yml`:

```yaml
x-rabbit-common: &rabbit-common
  image: rabbitmq:4.3-management
  restart: unless-stopped
  environment:
    RABBITMQ_DEFAULT_USER: admin
    RABBITMQ_DEFAULT_PASS: admin
    # общий секрет узлов кластера; в продакшене — длинная случайная строка из секрета
    RABBITMQ_COOKIE: "course-secret-cookie"
  # записать cookie в файл до старта: образ сам выставит владельца и права
  entrypoint:
    - /bin/sh
    - -c
    - |
      echo "$$RABBITMQ_COOKIE" > /var/lib/rabbitmq/.erlang.cookie
      chmod 400 /var/lib/rabbitmq/.erlang.cookie
      exec docker-entrypoint.sh rabbitmq-server

services:
  rabbit-1:
    <<: *rabbit-common
    hostname: rabbit-1
    container_name: rabbit-1
    ports: ["5672:5672", "15672:15672", "15692:15692", "5552:5552"]
    volumes:
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
      - ./enabled_plugins:/etc/rabbitmq/enabled_plugins:ro
      - rabbit1-data:/var/lib/rabbitmq

  rabbit-2:
    <<: *rabbit-common
    hostname: rabbit-2
    container_name: rabbit-2
    ports: ["5673:5672", "15673:15672"]
    volumes:
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
      - ./enabled_plugins:/etc/rabbitmq/enabled_plugins:ro
      - rabbit2-data:/var/lib/rabbitmq

  rabbit-3:
    <<: *rabbit-common
    hostname: rabbit-3
    container_name: rabbit-3
    ports: ["5674:5672", "15674:15672"]
    volumes:
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
      - ./enabled_plugins:/etc/rabbitmq/enabled_plugins:ro
      - rabbit3-data:/var/lib/rabbitmq

volumes:
  rabbit1-data:
  rabbit2-data:
  rabbit3-data:
```

**Почему `hostname` обязателен.** Имя узла RabbitMQ — `rabbit@<hostname>`, и данные узла привязаны к этому имени. Если контейнер получит случайный hostname, после пересоздания он поднимется «новым узлом» с пустыми данными и не узнает кластер.

Запусти кластер:

```bash
docker compose up -d
docker compose ps
```

Проверь кластер:

```bash
docker exec rabbit-1 rabbitmqctl cluster_status
docker exec rabbit-1 rabbitmq-diagnostics check_running
docker exec rabbit-1 rabbitmqctl list_feature_flags name state
```

В `cluster_status` ты увидишь три узла в разделе *Disk Nodes* и все три в *Running Nodes*. Если узел не вошёл в кластер — смотри логи: `docker logs rabbit-2`.

Management UI доступен на любом узле: http://localhost:15672, http://localhost:15673, http://localhost:15674.

## 2.4 Первая очередь и первое сообщение

Объявим очередь через HTTP API (так же делает management UI):

```bash
# quorum-очередь tasks
curl -s -u admin:admin -X PUT http://localhost:15672/api/queues/%2F/tasks \
  -H "content-type: application/json" \
  -d '{"durable": true, "arguments": {"x-queue-type": "quorum"}}'

# опубликовать через default exchange (routing key = имя очереди)
curl -s -u admin:admin -X POST http://localhost:15672/api/exchanges/%2F/amq.default/publish \
  -H "content-type: application/json" \
  -d '{"routing_key": "tasks", "payload": "{\"task\":\"resize\",\"image\":\"1.png\"}",
       "payload_encoding": "string", "properties": {"delivery_mode": 2}}'

# забрать сообщение (для отладки; в приложениях — только consume)
curl -s -u admin:admin -X POST http://localhost:15672/api/queues/%2F/tasks/get \
  -H "content-type: application/json" \
  -d '{"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"}'
```

`%2F` — это закодированное имя vhost `/`.

Посмотри на очередь:

```bash
docker exec rabbit-1 rabbitmqctl list_queues name type messages_ready messages_unacknowledged consumers
docker exec rabbit-1 rabbitmq-queues quorum_status tasks
```

`quorum_status` покажет три реплики очереди на трёх узлах и кто из них лидер.

**Эксперимент, который отличает понимание от «я пробовал».** Опубликуй сообщение в exchange `amq.direct` с ключом `nobody`, для которого нет ни одного binding:

```bash
curl -s -u admin:admin -X POST http://localhost:15672/api/exchanges/%2F/amq.direct/publish \
  -H "content-type: application/json" \
  -d '{"routing_key": "nobody", "payload": "lost", "payload_encoding": "string", "properties": {}}'
```

Ответ `{"routed": false}`: брокер принял сообщение и выбросил его. Producer без `mandatory` об этом не узнает никогда.

## 2.5 Первый producer и consumer на Python

```bash
pip install pika
```

`consumer.py`:

```python
import pika

params = pika.URLParameters("amqp://admin:admin@localhost:5672/%2F")
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare("tasks", durable=True, arguments={"x-queue-type": "quorum"})
ch.basic_qos(prefetch_count=10)

def on_message(ch, method, properties, body):
    print("получено:", body.decode(), "redelivered:", method.redelivered)
    ch.basic_ack(delivery_tag=method.delivery_tag)   # подтверждаем после обработки

ch.basic_consume("tasks", on_message)
print("жду сообщения, Ctrl+C для выхода")
ch.start_consuming()
```

`producer.py`:

```python
import pika

conn = pika.BlockingConnection(pika.URLParameters("amqp://admin:admin@localhost:5672/%2F"))
ch = conn.channel()
ch.queue_declare("tasks", durable=True, arguments={"x-queue-type": "quorum"})
ch.confirm_delivery()   # publisher confirms: basic_publish бросит исключение, если брокер не принял

for i in range(5):
    ch.basic_publish(
        exchange="",                 # default exchange
        routing_key="tasks",         # = имя очереди
        body=f'{{"task":"resize","n":{i}}}',
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
        mandatory=True,              # вернуть, если очереди нет
    )
print("отправлено 5 сообщений")
conn.close()
```

Запусти consumer в одном терминале, producer — в другом. Потом запусти **два** consumer'а и отправь 10 сообщений: они распределятся между ними. Это competing consumers — основа очередей задач.

## 2.6 Где RabbitMQ хранит данные

```bash
docker exec rabbit-1 ls /var/lib/rabbitmq/mnesia
```

```
/var/lib/rabbitmq/
├── .erlang.cookie
└── mnesia/                         # исторически так называется каталог данных
    └── rabbit@rabbit-1/
        ├── coordination/           # Khepri (метаданные) и логи Raft
        ├── quorum/                 # данные quorum-очередей
        ├── stream/                 # данные streams
        └── msg_stores/             # данные classic-очередей
```

Каталог данных называется `mnesia` по историческим причинам, хотя с 4.3 метаданные хранит Khepri. Каталог обязан лежать на постоянном томе и быть привязан к **постоянному имени узла**.

## 2.7 Самые частые ошибки запуска

| Симптом | Причина | Решение |
|---|---|---|
| `ACCESS_REFUSED` для `guest` | `guest` разрешён только с localhost | Создай своего пользователя (`RABBITMQ_DEFAULT_USER`) |
| Узлы не собираются в кластер | Разные Erlang cookie или имена хостов не резолвятся | Одинаковый cookie, стабильные `hostname` |
| `Cookie file ... must be accessible by owner only` | Слишком открытые права на cookie | `chmod 400`, владелец — пользователь `rabbitmq` |
| После пересоздания контейнера узел пустой | Сменился hostname, а значит имя узла | Задай `hostname` явно |
| `PRECONDITION_FAILED - inequivalent arg` | Очередь уже объявлена с другими аргументами | Объявляй одинаково везде или удали очередь |
| `PRECONDITION_FAILED` при объявлении non-durable очереди | С 4.3 non-durable неэксклюзивные очереди запрещены | `durable=True` или `exclusive=True` |
| Публикации «пропадают» | Нет binding'а под routing key | `mandatory=True`, alternate exchange, проверь bindings |
| Кластер недоступен после падения двух узлов из трёх | Khepri нужен кворум (большинство узлов) | Вернуть хотя бы один узел |

### Практика

1. Подними кластер из трёх узлов и проверь `rabbitmqctl cluster_status`.
2. Создай quorum-очередь `tasks`, посмотри `rabbitmq-queues quorum_status tasks`, найди лидера.
3. Останови узел-лидер (`docker stop rabbit-N`) и снова посмотри статус: кто стал лидером? Продолжай публиковать и читать.
4. Запусти два консьюмера на одной очереди и отправь 10 сообщений. Как они распределились?

---

# Модуль 3. Exchanges и маршрутизация

## 3.1 Четыре типа exchange

| Тип | Как маршрутизирует | Пример |
|---|---|---|
| **direct** | Routing key сообщения **равен** binding key | `payment.succeeded` → очередь с binding `payment.succeeded` |
| **fanout** | Во **все** привязанные очереди, ключ игнорируется | Событие для всех подписчиков |
| **topic** | По шаблону: `*` — одно слово, `#` — ноль или больше слов | `order.*.eu`, `order.#` |
| **headers** | По заголовкам сообщения, ключ игнорируется | `x-match=all`, `format=pdf`, `region=eu` |

```
DIRECT                      FANOUT                       TOPIC
routing key "pay"           routing key любой            routing key "order.created.eu"
   |                           |                            |
   +--> [q1] binding "pay"     +--> [q1]                    +--> [q1] "order.*.eu"     ✔
   +-x- [q2] binding "ship"    +--> [q2]                    +--> [q2] "order.#"        ✔
                               +--> [q3]                    +-x- [q3] "order.*"        ✘ (три слова)
```

## 3.2 Default exchange

У каждого vhost есть **default exchange** — direct exchange с пустым именем `""`. Каждая очередь автоматически привязана к нему по своему имени.

```
publish(exchange="", routing_key="tasks")  ==  «положить прямо в очередь tasks»
```

Это удобно для простых очередей задач, но плохо для архитектуры: producer начинает знать имена очередей, а значит, и потребителей. Для событий и интеграции сервисов публикуй в **именованный exchange**.

## 3.3 Topic exchange: главный инструмент

Routing key в topic exchange — слова через точку. Шаблоны в binding:

| Шаблон | Совпадает | Не совпадает |
|---|---|---|
| `order.created` | `order.created` | `order.created.eu` |
| `order.*` | `order.created`, `order.paid` | `order.created.eu` |
| `order.#` | `order`, `order.created`, `order.created.eu` | `payment.created` |
| `*.created.*` | `order.created.eu` | `order.created` |
| `#.eu` | `order.created.eu`, `eu` | `order.created` |
| `#` | всё | — |

Шаблон ключей, который работает:

```
<сущность>.<событие>[.<регион|версия>]

order.created
order.paid
order.cancelled
payment.succeeded.eu
stock.reserved
```

Правила:

- слова от общего к частному: `order.created`, а не `created.order`;
- одна сущность — один префикс: `order.#` ловит весь жизненный цикл;
- не клади в ключ идентификаторы: миллионы уникальных ключей не нужны, маршрутизация идёт по шаблонам;
- `#` в binding используют в конце шаблона, в 4.3 число `#` в одном binding key ограничено двумя.

## 3.4 Пример топологии интернет-магазина

```bash
API=http://localhost:15672/api
AUTH="-u admin:admin -H content-type:application/json"

# exchange для событий заказов
curl -s $AUTH -X PUT $API/exchanges/%2F/shop.events -d '{"type":"topic","durable":true}'

# очереди сервисов
for q in payments stock notifications audit; do
  curl -s $AUTH -X PUT $API/queues/%2F/$q -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}'
done

# bindings: кому что нужно
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/q/payments      -d '{"routing_key":"order.created"}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/q/stock         -d '{"routing_key":"order.created"}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/q/stock         -d '{"routing_key":"order.cancelled"}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/q/notifications -d '{"routing_key":"*.succeeded"}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/q/audit         -d '{"routing_key":"#"}'
```

```
                                  +--> [payments]       order.created
publish order.created  ---+       +--> [stock]          order.created, order.cancelled
                          |       |
                    [shop.events] +--> [notifications]  *.succeeded
                       (topic)    |
                                  +--> [audit]          #   (всё)
```

Одно `order.created` попадёт в три очереди: `payments`, `stock`, `audit`. Новый сервис подключается добавлением очереди и binding'а, **без изменений в producer'е**.

## 3.5 Fanout: всем подписчикам

```bash
curl -s $AUTH -X PUT $API/exchanges/%2F/cache.invalidate -d '{"type":"fanout","durable":true}'
```

Типичный приём: каждый экземпляр сервиса создаёт **свою эксклюзивную очередь** с именем от сервера и привязывает её к fanout exchange. Так каждое событие получает каждый экземпляр (сброс кеша, обновление конфигурации):

```python
q = ch.queue_declare(queue="", exclusive=True)          # имя придумает сервер: amq.gen-...
ch.queue_bind(exchange="cache.invalidate", queue=q.method.queue)
```

Эксклюзивная очередь исчезает вместе с соединением: упавший экземпляр не оставит за собой мусор.

## 3.6 Headers exchange

Маршрутизация по заголовкам вместо ключа:

```bash
curl -s $AUTH -X PUT $API/exchanges/%2F/reports -d '{"type":"headers","durable":true}'
curl -s $AUTH -X POST $API/bindings/%2F/e/reports/q/pdf-eu \
  -d '{"routing_key":"","arguments":{"x-match":"all","format":"pdf","region":"eu"}}'
```

`x-match=all` — должны совпасть все заголовки, `any` — хотя бы один. Headers exchange медленнее и используется редко: почти всегда задачу решает topic exchange с продуманным ключом.

## 3.7 Alternate exchange: не терять неразмеченные сообщения

Если сообщение не подошло ни к одному binding'у, его можно не выбрасывать, а отправить в **alternate exchange**:

```bash
curl -s $AUTH -X PUT $API/exchanges/%2F/shop.unrouted -d '{"type":"fanout","durable":true}'
curl -s $AUTH -X PUT $API/queues/%2F/unrouted -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.unrouted/q/unrouted -d '{"routing_key":""}'

# политика: всем exchanges с префиксом shop. назначить alternate exchange
docker exec rabbit-1 rabbitmqctl set_policy AE '^shop\.' \
  '{"alternate-exchange":"shop.unrouted"}' --apply-to exchanges
```

Теперь сообщение с опечаткой в ключе окажется в очереди `unrouted`, где его увидит мониторинг, а не пропадёт.

## 3.8 Exchange-to-exchange bindings

Exchange можно привязать к другому exchange. Это позволяет строить иерархию: общий входной exchange и отдельные exchanges команд, которые подписываются на нужные ключи.

```bash
curl -s $AUTH -X PUT $API/exchanges/%2F/billing.in -d '{"type":"topic","durable":true}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/e/billing.in -d '{"routing_key":"payment.#"}'
```

Команда billing управляет своим exchange `billing.in` и его очередями, не трогая общий `shop.events`.

## 3.9 Встроенные exchanges

| Exchange | Тип |
|---|---|
| `""` (default) | direct, привязан ко всем очередям по имени |
| `amq.direct` | direct |
| `amq.fanout` | fanout |
| `amq.topic` | topic (его же использует MQTT-плагин) |
| `amq.headers`, `amq.match` | headers |

Для своих систем создавай свои exchanges с понятными именами: встроенные нельзя удалить и неудобно разграничивать по правам.

### Практика

1. Построй топологию из 3.4 и опубликуй `order.created`, `order.cancelled`, `payment.succeeded`. Проверь, в какие очереди попало каждое сообщение.
2. Опубликуй сообщение с ключом `ordr.created` (опечатка) без alternate exchange и с ним. Где оно окажется?
3. Спроектируй ключи маршрутизации для сервиса доставки: заказы, курьеры, статусы, геопозиции. Какие bindings понадобятся уведомлениям и аналитике?

---

# Модуль 4. Очереди: classic, quorum и streams

## 4.1 Какой тип очереди выбрать

| | **Classic** | **Quorum** | **Stream** |
|---|---|---|---|
| Репликация | Нет, очередь живёт на одном узле | Да, Raft (обычно 3 реплики) | Да, репликация лога |
| Переживает падение узла | Нет: очередь недоступна, пока узел не вернётся | Да, пока жив кворум | Да, пока жив кворум |
| Сообщение после ack | Удаляется | Удаляется | **Остаётся** до retention |
| Повторное чтение | Нет | Нет | Да, с любого offset |
| Лимит повторных доставок | Нет | Да, `delivery-limit` (по умолчанию 20) | Не применимо |
| Exclusive / non-durable | Да | Нет | Нет |
| Приоритеты | Да (`x-max-priority`) | Да | Нет |
| Когда | Временные, эксклюзивные очереди, RPC-ответы | Всё, что важно | История, много читателей, большие объёмы |

**Правило по умолчанию:** важные данные — **quorum**, временные ответы и эксклюзивные очереди — **classic**, повторное чтение и фан-аут на много читателей — **stream**.

## 4.2 Classic queues

Classic queue хранит сообщения на одном узле: в памяти, с выгрузкой на диск (с 4.2 используется только вторая версия хранилища, CQv2; в 4.3 первая удалена вместе с аргументом `x-queue-mode`).

Что важно знать:

- если узел очереди упал, очередь **недоступна** до его возвращения; persistent-сообщения в durable-очереди переживут перезапуск узла, но не потерю его диска;
- classic mirrored queues (зеркалирование) **удалены в 4.0**, поэтому classic-очереди больше не реплицируются вообще;
- хороши для временных очередей: эксклюзивных, с TTL, для ответов RPC.

## 4.3 Quorum queues

Quorum queue — реплицированная очередь на протоколе **Raft**:

```
            quorum queue "payments" (3 реплики)
   rabbit-1               rabbit-2               rabbit-3
   [LEADER]  ---------->  [follower]  ------->  [follower]
      ^
      | publish/consume идут через лидера,
      | запись подтверждается, когда её сохранило большинство (2 из 3)
```

- Все операции идут через **лидера**; клиенты могут быть подключены к любому узлу — брокер сам перенаправит.
- Сообщение подтверждается publisher'у, когда его записало **большинство** реплик на диск.
- Если лидер упал, реплики выбирают нового за секунды. Очередь доступна, пока живо большинство: 2 из 3, 3 из 5.
- Размер группы задаётся при объявлении (`x-quorum-initial-group-size`, по умолчанию 3 или меньше, если узлов меньше).

Полезные встроенные возможности:

| Возможность | Что даёт |
|---|---|
| `delivery-limit` | После N неудачных доставок сообщение уходит в DLX или удаляется. С 4.0 по умолчанию **20** — защита от бесконечного цикла «ядовитых» сообщений |
| Заголовок `x-delivery-count` | Сколько раз сообщение уже доставлялось |
| At-least-once dead lettering | Надёжная пересылка в DLX без потерь (модуль 8) |
| Приоритеты | С 4.3 — строгие приоритеты с корректным порядком повторной доставки |
| Delayed retry (4.3) | Возвращённое сообщение ждёт перед повторной доставкой, задержка растёт с числом попыток (модуль 8) |
| Consumer timeout (4.3) | Сообщения, которые консьюмер держит без ack слишком долго, возвращаются в очередь |

Чего quorum queues **не умеют**: быть эксклюзивными или non-durable, работать с `global` prefetch. Для временных очередей используй classic.

```python
ch.queue_declare("payments", durable=True, arguments={
    "x-queue-type": "quorum",
    "x-delivery-limit": 5,                  # не больше 5 доставок одного сообщения
    "x-dead-letter-exchange": "shop.dlx",   # куда уходят исчерпавшие попытки
})
```

## 4.4 Streams

Stream — **лог** внутри RabbitMQ: сообщения дописываются в конец и не удаляются после чтения. Каждый консьюмер читает со своей позиции.

```
stream "shop.events.log"
  offset: 0   1   2   3   4   5   6   7 ...
                  ^           ^           ^
          аналитика      аудит         новые события
         (перечитывает) (догоняет)
```

```python
ch.queue_declare("shop.events.log", durable=True, arguments={
    "x-queue-type": "stream",
    "x-max-age": "7D",                          # хранить 7 дней
    "x-stream-max-segment-size-bytes": 100_000_000,
})

ch.basic_qos(prefetch_count=100)                # для streams prefetch обязателен
ch.basic_consume("shop.events.log", on_message,
                 arguments={"x-stream-offset": "first"})   # first | last | next | число | timestamp
```

Streams подробно — в модуле 10. Коротко: они дают повторное чтение, фан-аут на сотни читателей без копирования сообщений в сотни очередей и огромную пропускную способность через отдельный stream-протокол (порт 5552).

## 4.5 Аргументы очереди

| Аргумент | Типы | Смысл |
|---|---|---|
| `x-queue-type` | все | `classic`, `quorum`, `stream` |
| `x-message-ttl` | classic, quorum | Время жизни сообщения в очереди, мс |
| `x-expires` | classic, quorum | Удалить очередь, если ею не пользуются N мс |
| `x-max-length` | classic, quorum | Максимум сообщений |
| `x-max-length-bytes` | все | Максимум байт (для stream — retention по размеру) |
| `x-overflow` | classic, quorum | Что делать при переполнении: `drop-head` (по умолчанию), `reject-publish`, `reject-publish-dlx` (только classic) |
| `x-dead-letter-exchange`, `x-dead-letter-routing-key` | classic, quorum | Куда отправлять «мёртвые» сообщения |
| `x-delivery-limit` | quorum | Лимит доставок |
| `x-single-active-consumer` | classic, quorum | Только один активный консьюмер (модуль 6.8) |
| `x-max-priority` | classic | Число уровней приоритета |
| `x-quorum-initial-group-size` | quorum | Число реплик |
| `x-max-age` | stream | Retention по времени (`7D`, `12h`) |

**Ловушка `drop-head`:** при переполнении очередь по умолчанию **молча удаляет самые старые сообщения**. Для задач, которые нельзя терять, ставь `x-overflow=reject-publish`: тогда publisher с confirms получит `nack` и узнает о проблеме.

Classic-очереди соблюдают лимит точно. Для quorum-очередей лимит **мягкий**: очередь может принять одно-два сообщения сверх лимита, прежде чем начнёт отвечать `nack` (тесты в `examples/` проверяют именно это).

## 4.6 Политики вместо аргументов

Аргументы задаются при объявлении и **не меняются**: повторное объявление с другими аргументами вернёт `PRECONDITION_FAILED`. Поэтому TTL, лимиты и DLX лучше задавать **политиками**: их можно менять на лету без пересоздания очередей.

```bash
docker exec rabbit-1 rabbitmqctl set_policy shop-limits '^shop\.' \
  '{"max-length": 100000, "overflow": "reject-publish", "dead-letter-exchange": "shop.dlx", "delivery-limit": 10}' \
  --apply-to queues --priority 10

docker exec rabbit-1 rabbitmqctl list_policies
```

- Политика применяется ко всем очередям, чьё имя подходит под регулярное выражение.
- На очередь действует **одна** политика с наибольшим приоритетом (плюс operator policy от администратора).
- Если значение задано и аргументом, и политикой, для большинства ключей **побеждает аргумент**.
- `x-queue-type` задаётся только аргументом: тип очереди нельзя поменять политикой.

Правило: в коде объявляй только тип очереди и то, без чего приложение не работает; лимиты, TTL и DLX держи в политиках.

## 4.7 Тип очереди по умолчанию для vhost

Чтобы разработчики не создавали classic-очереди случайно, задай тип по умолчанию на vhost:

```bash
docker exec rabbit-1 rabbitmqctl add_vhost shop --default-queue-type quorum
```

Очереди, объявленные в `shop` без `x-queue-type`, станут quorum (кроме эксклюзивных — они остаются classic).

### Вопросы для самопроверки

1. Почему после удаления classic mirrored queues для важных данных нужны именно quorum queues?
2. Что происходит с quorum-очередью, когда падает один узел из трёх? А два?
3. Зачем quorum queues нужен `delivery-limit`, и какое значение у него по умолчанию?
4. Чем опасен `x-overflow=drop-head`?
5. Почему TTL и лимиты лучше задавать политиками, а не аргументами?

---

# Модуль 5. Publisher: persistent-сообщения, confirms и mandatory

## 5.1 Три вопроса, которые должен решить publisher

```
publish(message)
   |
   +-- 1. Переживёт ли сообщение перезапуск брокера?   -> durable-очередь + persistent-сообщение
   +-- 2. Принял ли брокер сообщение?                   -> publisher confirms
   +-- 3. Попало ли сообщение хоть в одну очередь?      -> mandatory + basic.return
```

`basic.publish` в AMQP 0-9-1 **асинхронный и без ответа**: клиент записал байты в сокет, и всё. Без confirms producer не знает, дошло ли сообщение, сохранил ли его брокер и не выбросил ли его, не найдя очереди.

## 5.2 Persistent-сообщения

| Очередь | Сообщение | После перезапуска брокера |
|---|---|---|
| durable classic | `delivery_mode=2` | Сообщение на месте |
| durable classic | `delivery_mode=1` | Очередь есть, сообщение **потеряно** |
| quorum / stream | любое | Сообщение на месте (всегда на диске) |
| exclusive / auto-delete | любое | Очереди нет |

Для quorum queues и streams `delivery_mode` не влияет на хранение, но ставить `delivery_mode=2` всё равно стоит: сообщение может пройти через DLX или shovel в classic-очередь.

## 5.3 Publisher confirms

Канал переводится в режим подтверждений (`confirm.select`), и брокер отвечает на каждую публикацию:

```
producer                          broker
   |-- publish (seq 1) -------------->|
   |-- publish (seq 2) -------------->|
   |-- publish (seq 3) -------------->|
   |<-------------- basic.ack (seq 2, multiple=true)   # 1 и 2 приняты
   |<-------------- basic.nack (seq 3)                 # 3 не принят: повторить или алерт
```

Когда брокер отправляет `ack`:

| Очередь | Confirm приходит, когда… |
|---|---|
| Quorum | Сообщение записано большинством реплик |
| Stream | Сообщение записано большинством реплик |
| Classic durable + persistent | Сообщение записано на диск (или доставлено и подтверждено консьюмером) |
| Нет подходящей очереди | Сразу (и `basic.return` перед ним, если `mandatory=true`) |

`nack` приходит, если брокер не смог принять сообщение: например, очередь с `x-overflow=reject-publish` переполнена или реплики quorum-очереди недоступны.

**Главное правило:** считай сообщение отправленным только после `ack`. Если пришёл `nack` или истёк таймаут ожидания — повтори отправку (с тем же `message_id`) или сохрани сообщение в outbox (модуль 7).

## 5.4 Mandatory и возвраты

```
publish(mandatory=true, routing_key="ordr.created")   # опечатка
   -> ни одна очередь не подошла
   -> брокер отправляет basic.return (NO_ROUTE) обратно producer'у
   -> потом basic.ack (сообщение «обработано» брокером)
```

Без `mandatory` сообщение молча исчезает. С `mandatory` producer узнаёт о проблеме, но **только если слушает возвраты**. Альтернатива на стороне брокера — alternate exchange (модуль 3.7).

## 5.5 Publisher на Python (pika)

```python
import json
import uuid

import pika
from pika.exceptions import NackError, UnroutableError

conn = pika.BlockingConnection(pika.URLParameters("amqp://admin:admin@localhost:5672/%2F"))
ch = conn.channel()
ch.exchange_declare("shop.events", exchange_type="topic", durable=True)
ch.confirm_delivery()          # каждый basic_publish ждёт ack от брокера

event = {"order_id": "order-1", "amount": 4990}
try:
    ch.basic_publish(
        exchange="shop.events",
        routing_key="order.created",
        body=json.dumps(event),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            message_id=str(uuid.uuid4()),        # стабильный id события для дедупликации
            headers={"event-type": "OrderCreated"},
        ),
        mandatory=True,
    )
    print("принято брокером")
except UnroutableError:
    print("нет ни одной очереди для этого ключа")     # алерт: сломана топология
except NackError:
    print("брокер не принял сообщение")               # повторить или outbox
finally:
    conn.close()
```

`BlockingConnection` с `confirm_delivery()` ждёт подтверждения **каждого** сообщения: это просто и надёжно, но медленно. Для высокой пропускной способности нужны асинхронные confirms (как в Go и Java ниже) или асинхронный клиент (`aio-pika`).

## 5.6 Publisher на Go (amqp091-go)

```bash
go get github.com/rabbitmq/amqp091-go
```

```go
package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("shop.events", "topic", true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	if err := ch.Confirm(false); err != nil { // включить publisher confirms
		log.Fatal(err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, 16)) // возвраты для mandatory

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx,
		"shop.events", "order.created",
		true,  // mandatory
		false, // immediate (не поддерживается, всегда false)
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			MessageId:    "order-1-created",
			Body:         []byte(`{"order_id":"order-1","amount":4990}`),
		})
	if err != nil {
		log.Fatal(err)
	}

	acked, err := dc.WaitContext(ctx) // дождаться ack/nack от брокера
	if err != nil || !acked {
		log.Fatalf("не подтверждено: acked=%v err=%v", acked, err) // повторить или outbox
	}
	select {
	case r := <-returns:
		log.Fatalf("возвращено: %s (%d), ключ %s", r.ReplyText, r.ReplyCode, r.RoutingKey)
	default:
		log.Println("принято брокером и доставлено в очередь")
	}
}
```

Для высокой пропускной способности публикуй пачку сообщений, собирая `DeferredConfirmation`, а потом жди их все. Возвраты приходят асинхронно и раньше ack, поэтому в продакшене их читают в отдельной горутине.

## 5.7 Publisher на Java

```xml
<dependency>
  <groupId>com.rabbitmq</groupId>
  <artifactId>amqp-client</artifactId>
  <version>5.25.0</version> <!-- подставь актуальную версию -->
</dependency>
```

```java
import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class OrderPublisher {
    public static void main(String[] args) throws Exception {
        ConnectionFactory f = new ConnectionFactory();
        f.setUri("amqp://admin:admin@localhost:5672/%2F");
        f.setAutomaticRecoveryEnabled(true);          // Java-клиент умеет переподключаться сам

        try (Connection conn = f.newConnection("order-service");
             Channel ch = conn.createChannel()) {
            ch.exchangeDeclare("shop.events", BuiltinExchangeType.TOPIC, true);
            ch.confirmSelect();

            // асинхронные confirms: храним неподтверждённые сообщения по номеру
            ConcurrentNavigableMap<Long, String> outstanding = new ConcurrentSkipListMap<>();
            ch.addConfirmListener(
                (seq, multiple) -> clear(outstanding, seq, multiple),               // ack
                (seq, multiple) -> {                                                // nack
                    System.err.println("nack до " + seq + ": повторить или outbox");
                    clear(outstanding, seq, multiple);
                });
            ch.addReturnListener(r ->
                System.err.println("возвращено: " + r.getReplyText() + " ключ " + r.getRoutingKey()));

            for (int i = 1; i <= 100; i++) {
                String body = "{\"order_id\":\"order-" + i + "\"}";
                outstanding.put(ch.getNextPublishSeqNo(), body);
                ch.basicPublish("shop.events", "order.created", true,
                    new AMQP.BasicProperties.Builder()
                        .deliveryMode(2)
                        .contentType("application/json")
                        .messageId("order-" + i + "-created")
                        .headers(Map.of("event-type", "OrderCreated"))
                        .build(),
                    body.getBytes(StandardCharsets.UTF_8));
            }
            ch.waitForConfirmsOrDie(10_000);   // дождаться всех confirms перед выходом
        }
    }

    static void clear(ConcurrentNavigableMap<Long, String> m, long seq, boolean multiple) {
        if (multiple) m.headMap(seq, true).clear(); else m.remove(seq);
    }
}
```

## 5.8 Flow control и заблокированные соединения

Когда брокеру не хватает памяти или диска, срабатывает **аларм** (модуль 14): брокер **блокирует все публикующие соединения** в кластере. Publisher'ы зависают на `publish`, консьюмеры продолжают работать и разгребают очереди.

```
memory_high_watermark превышен (или свободного диска меньше disk_free_limit)
   -> connection.blocked всем publisher'ам
   -> публикация стоит, консьюмеры работают
   -> память освободилась -> connection.unblocked
```

Что из этого следует:

- **раздельные соединения** для публикации и потребления: иначе заблокированное соединение затормозит и ack'и консьюмера;
- подпишись на `connection.blocked`/`unblocked` (`NotifyBlocked` в Go, `BlockedListener` в Java) и выставляй метрику: заблокированный publisher — первый признак беды;
- таймауты на публикацию, иначе HTTP-запросы твоего сервиса повиснут вместе с брокером.

## 5.9 Что делать, если публикация не удалась

```
публикация не подтверждена
   |
   +-- nack                       -> очередь переполнена (reject-publish) или недоступна: повторить с backoff, алерт
   +-- таймаут ожидания confirm   -> соединение или брокер проблемный: переподключиться и повторить
   +-- basic.return (NO_ROUTE)    -> сломана топология: не ретраить бесконечно, алерт
   +-- канал закрыт с ошибкой     -> ошибка протокола (нет exchange, нет прав): исправлять код или права
```

Повторная отправка может создать **дубль**: брокер мог принять сообщение, а подтверждение потерялось. Поэтому у каждого события должен быть стабильный `message_id`, а консьюмеры — идемпотентны (модуль 7). Встроенной дедупликации публикаций в очередях RabbitMQ нет (у streams она есть через stream-протокол, модуль 10).

### Вопросы для самопроверки

1. Почему durable-очередь не спасает сообщение с `delivery_mode=1`?
2. Когда брокер отправляет confirm для quorum-очереди?
3. Что происходит с сообщением без `mandatory`, если нет подходящей очереди?
4. Зачем держать отдельные соединения для публикации и потребления?
5. Почему повторная отправка после таймаута confirm может создать дубль?

---

# Модуль 6. Consumer: ack, prefetch и повторная доставка

## 6.1 Как брокер доставляет сообщения

```
очередь [m1][m2][m3][m4][m5][m6] ...
            |
            | basic.consume: брокер сам отправляет (push),
            | но не больше prefetch неподтверждённых на консьюмера
            v
   consumer A: m1, m3, m5  (unacked)      consumer B: m2, m4, m6  (unacked)
            |                                   |
         ack m1  -> m1 удалено               упал -> m2, m4, m6 вернутся в очередь
```

- **basic.consume** (push) — правильный способ: брокер доставляет сообщения по мере появления.
- **basic.get** (pull одного сообщения) — только для отладки: каждое сообщение — отдельный запрос по сети, это медленно и нагружает брокер.

## 6.2 Режимы подтверждения

| Режим | Как | Гарантия |
|---|---|---|
| **auto ack** (`auto_ack=True`) | Брокер считает сообщение подтверждённым в момент отправки | At-most-once: консьюмер упал — сообщение потеряно |
| **manual ack** | Консьюмер явно подтверждает после обработки | At-least-once: упал до ack — сообщение будет доставлено снова |

Почти всегда нужен **manual ack**. Auto ack допустим для данных, которые не жалко потерять (метрики, логи), и опасен ещё тем, что брокер отправляет сообщения без ограничения prefetch: быстрый брокер может завалить медленного консьюмера и исчерпать его память.

## 6.3 ack, nack, reject

| Ответ | Эффект | Когда |
|---|---|---|
| `basic.ack` | Обработано, удалить | Успех |
| `basic.nack(requeue=true)` | Вернуть в очередь | Временная ошибка — но осторожно, см. ниже |
| `basic.nack(requeue=false)` / `basic.reject(requeue=false)` | Отбросить или отправить в DLX | Сообщение невалидно |
| ничего, консьюмер упал | Все его unacked-сообщения вернутся в очередь | |

Флаг `multiple=true` подтверждает сразу все сообщения до указанного `delivery_tag` — удобно для пачек.

**Ловушка `requeue=true`:** сообщение возвращается **в начало очереди** и тут же доставляется снова. «Ядовитое» сообщение, которое всегда падает, превращается в бесконечный цикл, съедающий CPU. В quorum queues от этого защищает `delivery-limit` (по умолчанию 20): после лимита сообщение уходит в DLX или удаляется. Правильные ретраи с задержкой — в модуле 8.

**Важно:** `delivery_tag` уникален **внутри канала**. Подтверждать сообщение нужно в том же канале, в котором оно получено. Если канал закрылся, ack по старому тегу уже невозможен, а сообщение будет доставлено снова.

## 6.4 Prefetch (QoS)

`basic.qos(prefetch_count=N)` — сколько **неподтверждённых** сообщений брокер может держать у одного консьюмера.

```
prefetch=1     брокер ждёт ack перед отправкой следующего: равномерно, но медленно (сеть на каждое сообщение)
prefetch=10-50 хороший старт для большинства задач
prefetch=500+  быстрые консьюмеры и маленькие сообщения
без лимита     опасно: все сообщения очереди уезжают в память одного консьюмера
```

Как выбрать:

- время обработки сообщения большое (сотни мс и больше) — маленький prefetch (1–10), иначе один консьюмер «захапает» пачку, а остальные будут простаивать;
- обработка быстрая, сеть — узкое место — prefetch побольше (100–300);
- для streams prefetch обязателен.

Prefetch задаётся **на консьюмера** (`global=false`). Режим `global=true` (общий лимит на канал) с 4.3 запрещён по умолчанию.

## 6.5 Consumer на Python (pika)

```python
import json
import pika

conn = pika.BlockingConnection(pika.URLParameters("amqp://admin:admin@localhost:5672/%2F"))
ch = conn.channel()
ch.queue_declare("payments", durable=True, arguments={"x-queue-type": "quorum"})
ch.basic_qos(prefetch_count=20)

class TemporaryError(Exception):
    pass

def on_message(ch, method, props, body):
    deliveries = (props.headers or {}).get("x-delivery-count", 0)   # quorum queues
    try:
        event = json.loads(body)
        charge(event)                                   # должно быть идемпотентным
        ch.basic_ack(method.delivery_tag)
    except json.JSONDecodeError:
        ch.basic_reject(method.delivery_tag, requeue=False)   # мусор: в DLX
    except TemporaryError:
        ch.basic_nack(method.delivery_tag, requeue=True)      # попробовать снова (delivery-limit защитит)

ch.basic_consume("payments", on_message)
try:
    ch.start_consuming()
except KeyboardInterrupt:
    ch.stop_consuming()
conn.close()
```

`BlockingConnection` однопоточный: пока выполняется `on_message`, pika не обслуживает heartbeats. Если обработка сообщения дольше heartbeat-таймаута (60 с по умолчанию), брокер закроет соединение. Долгую работу выноси в отдельный поток и подтверждай через `conn.add_callback_threadsafe`, либо используй асинхронный клиент.

## 6.6 Consumer на Go (amqp091-go)

```go
conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
if err != nil {
	log.Fatal(err)
}
defer conn.Close()
ch, err := conn.Channel()
if err != nil {
	log.Fatal(err)
}
defer ch.Close()

if err := ch.Qos(20, 0, false); err != nil { // prefetch 20 на консьюмера
	log.Fatal(err)
}
msgs, err := ch.Consume("payments", "billing-1",
	false, // autoAck: выключен
	false, false, false, nil)
if err != nil {
	log.Fatal(err)
}

for d := range msgs { // канал закроется, если закроется соединение или AMQP-канал
	count, _ := d.Headers["x-delivery-count"].(int64)
	if err := process(d.Body); err != nil {
		if isTemporary(err) {
			d.Nack(false, true) // вернуть в очередь
		} else {
			d.Reject(false) // в DLX
		}
		log.Printf("error (delivery %d): %v", count, err)
		continue
	}
	d.Ack(false)
}
log.Println("канал доставки закрыт: нужно переподключиться")
```

`amqp091-go` **не переподключается сам**. Когда соединение рвётся, канал `msgs` закрывается; приложение должно подписаться на `conn.NotifyClose` и заново открыть соединение, канал и подписку.

## 6.7 Consumer на Java

```java
ConnectionFactory f = new ConnectionFactory();
f.setUri("amqp://admin:admin@localhost:5672/%2F");
f.setAutomaticRecoveryEnabled(true);           // восстановит соединение, каналы и подписки
Connection conn = f.newConnection("billing");
Channel ch = conn.createChannel();
ch.basicQos(20);

ch.basicConsume("payments", false, "billing-1", new DefaultConsumer(ch) {
    @Override
    public void handleDelivery(String tag, Envelope env, AMQP.BasicProperties props, byte[] body)
            throws IOException {
        long deliveryTag = env.getDeliveryTag();
        try {
            charge(body);                                     // идемпотентно
            getChannel().basicAck(deliveryTag, false);
        } catch (InvalidMessageException e) {
            getChannel().basicReject(deliveryTag, false);     // в DLX
        } catch (Exception e) {
            getChannel().basicNack(deliveryTag, false, true); // вернуть в очередь
        }
    }
});
```

## 6.8 Порядок, single active consumer и приоритеты консьюмеров

**Порядок.** Очередь выдаёт сообщения по порядку, но при нескольких консьюмерах они обрабатываются **параллельно**, а после `nack` или падения консьюмера сообщение возвращается и может быть обработано после более поздних. Если важен порядок:

- **single active consumer** (`x-single-active-consumer=true`): консьюмеров много, но получает сообщения только один; если он упал, брокер переключается на следующего. Порядок сохраняется, есть горячий резерв;
- **несколько очередей по ключу**: события одного заказа всегда в одной очереди (плагин consistent hash exchange или super streams, модуль 10).

**Приоритеты консьюмеров** (`x-priority` в аргументах `basic.consume`): брокер отдаёт сообщения консьюмерам с высшим приоритетом, пока у них есть место в prefetch, и только потом — остальным. Полезно, когда есть быстрые «основные» воркеры и медленные резервные.

## 6.9 Consumer timeout

Если консьюмер держит сообщение без ack дольше `consumer_timeout` (по умолчанию 30 минут), брокер возвращает сообщения в очередь. С 4.3 таймаут вычисляет **сама quorum-очередь**: для classic-очередей и streams он больше не применяется. Для quorum queues его можно задать аргументом `x-consumer-timeout`, политикой `consumer-timeout` или глобально в `rabbitmq.conf`.

Если обработка законно занимает часы — это сигнал пересмотреть дизайн (разбить задачу или хранить прогресс вне брокера), а не просто поднимать таймаут.

## 6.10 Масштабирование потребления

```
очередь payments
  1 консьюмер  -> все сообщения ему
  5 консьюмеров -> сообщения распределяются по кругу с учётом prefetch
  50 консьюмеров на одной очереди -> очередь (один процесс Erlang, один лидер) может стать узким местом
```

В отличие от Kafka, число консьюмеров на очереди **не ограничено числом партиций**: добавляй воркеров, пока очередь справляется. Одна очередь — это один процесс на лидере, её предел — десятки тысяч сообщений в секунду. Если нужно больше, шардируй: несколько очередей и маршрутизация по ключу (consistent hash exchange) или super streams.

### Вопросы для самопроверки

1. Почему auto ack — это at-most-once?
2. Чем опасен `nack(requeue=true)` для «ядовитого» сообщения, и что от этого защищает в quorum queues?
3. Как выбрать prefetch для медленной и для быстрой обработки?
4. Как сохранить порядок обработки, если консьюмеров несколько?
5. Почему `delivery_tag` нельзя подтвердить в другом канале?

---

# Модуль 7. Гарантии доставки, идемпотентность и outbox

## 7.1 Откуда берутся потери и дубли

| Сценарий | Результат | Защита |
|---|---|---|
| Нет подходящей очереди, `mandatory=false` | Потеря | `mandatory`, alternate exchange |
| Publisher не дождался confirm, брокер упал | Потеря | Publisher confirms + повтор |
| Confirm потерялся, publisher повторил | Дубль в очереди | `message_id` + идемпотентный консьюмер |
| Classic-очередь, `delivery_mode=1`, перезапуск | Потеря | Persistent-сообщения или quorum queue |
| Classic-очередь, потерян диск узла | Потеря | Quorum queue |
| Очередь переполнена, `overflow=drop-head` | Молча удалены старые сообщения | `reject-publish` + confirms |
| Auto ack, консьюмер упал | Потеря | Manual ack |
| Консьюмер обработал, упал до ack | Повторная обработка | Идемпотентность |
| Консьюмер закрыл канал до ack | Повторная доставка | Идемпотентность |
| Упали между записью в БД и публикацией | Потеря события | Transactional outbox |
| Опубликовали, а транзакция БД откатилась | «Фантомное» событие | Transactional outbox |

## 7.2 Три семантики

```
AT-MOST-ONCE
  auto ack, без confirms
  что-то упало -> сообщение потеряно

AT-LEAST-ONCE (стандарт)
  confirms + manual ack после обработки
  что-то упало -> повторная доставка, возможен дубль

EXACTLY-ONCE
  в RabbitMQ как свойства брокера нет;
  достигается как at-least-once + идемпотентная обработка
```

**Честно:** RabbitMQ не обещает exactly-once. «Ровно один эффект» получается, когда брокер гарантирует доставку хотя бы один раз, а приложение делает повторную обработку безвредной.

## 7.3 Идемпотентный консьюмер

```sql
CREATE TABLE processed_messages (
    message_id   TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```python
def on_message(ch, method, props, body):
    with db.transaction() as tx:
        inserted = tx.execute(
            "INSERT INTO processed_messages (message_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (props.message_id,),
        ).rowcount
        if inserted:                    # 0 строк -> уже обработано, просто подтверждаем
            charge(tx, json.loads(body))
    ch.basic_ack(method.delivery_tag)   # ack только ПОСЛЕ успешного COMMIT
```

Правила:

- `message_id` генерируется **один раз для бизнес-события** и не меняется при повторной отправке;
- ack — **после** коммита в базе. Наоборот — это at-most-once;
- таблицу обработанных id периодически чистят (например, старше срока, за который возможен повтор);
- альтернативы: `UPSERT` по естественному ключу, условное обновление по версии (`WHERE version = $expected`).

Встроенной дедупликации в классических и quorum-очередях нет. Существует сторонний плагин дедупликации по заголовку, но идемпотентность консьюмера надёжнее и не зависит от брокера.

## 7.4 Транзакции AMQP: почему не они

В AMQP 0-9-1 есть транзакции канала (`tx.select`, `tx.commit`). Они атомарно фиксируют публикации и ack'и **внутри брокера**, но:

- очень медленные (синхронный обмен на каждый commit);
- не распространяются на базу данных приложения;
- не дают exactly-once.

Используй publisher confirms вместо транзакций. `tx.*` встречаются только в старом коде.

## 7.5 Transactional outbox

**Проблема двойной записи:**

```
1. INSERT INTO orders ...           OK
2. basic_publish(order.created)     -> сервис упал
   -> заказ есть, события нет
```

**Решение — outbox-таблица в той же транзакции:**

```
+--------------- одна транзакция базы ----------------+
| INSERT INTO orders (...)                             |
| INSERT INTO outbox (id, exchange, routing_key, body) |
+------------------------------------------------------+
                |
                v
   ретранслятор: читает неотправленные строки,
   публикует с confirms и message_id = outbox.id,
   после ack помечает строку отправленной
                |
                v
         exchange shop.events
```

```sql
CREATE TABLE outbox (
    id           UUID PRIMARY KEY,
    exchange     TEXT NOT NULL,
    routing_key  TEXT NOT NULL,
    body         JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX ON outbox (created_at) WHERE published_at IS NULL;
```

Ретранслятор может отправить событие дважды (упал между ack брокера и пометкой строки), поэтому `message_id = outbox.id` обязателен: консьюмеры дедуплицируют по нему. Вместо опроса таблицы можно читать WAL через CDC (Debezium умеет публиковать в RabbitMQ через Debezium Server).

## 7.6 Выбор гарантий

| Задача | Рекомендация |
|---|---|
| Метрики, логи | Auto ack допустим, classic-очередь |
| Уведомления | Quorum queue, confirms, manual ack; дубль безвреден или отсекается по id |
| Бизнес-события | Quorum queue, confirms, `mandatory`, outbox, идемпотентный консьюмер |
| Деньги | Всё вышеперечисленное + `processed_messages` + сверки |
| Очередь задач | Quorum queue + `delivery-limit` + DLX |

### Вопросы для самопроверки

1. Почему RabbitMQ не даёт exactly-once, и как получить «ровно один эффект»?
2. Почему ack должен идти после коммита в базе?
3. Чем publisher confirms лучше транзакций AMQP?
4. Зачем в outbox `message_id = outbox.id`?

---

# Модуль 8. Dead letter exchanges, TTL и ретраи с задержкой

## 8.1 Когда сообщение становится «мёртвым»

Сообщение отправляется в **dead letter exchange** (DLX), если:

| Причина (`x-death` reason) | Что произошло |
|---|---|
| `rejected` | Консьюмер сделал `reject` или `nack` с `requeue=false` |
| `expired` | Истёк TTL сообщения или очереди |
| `maxlen` | Очередь переполнена (`drop-head` или `reject-publish-dlx`) |
| `delivery_limit` | Quorum queue: исчерпан лимит доставок |

Если DLX не настроен, такое сообщение просто удаляется.

## 8.2 Настройка DLX

```bash
API=http://localhost:15672/api
AUTH="-u admin:admin -H content-type:application/json"

curl -s $AUTH -X PUT $API/exchanges/%2F/shop.dlx -d '{"type":"topic","durable":true}'
curl -s $AUTH -X PUT $API/queues/%2F/shop.dead -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.dlx/q/shop.dead -d '{"routing_key":"#"}'

# политика: все очереди shop.* отправляют мёртвые сообщения в shop.dlx
docker exec rabbit-1 rabbitmqctl set_policy shop-dlx '^(payments|stock|notifications)$' \
  '{"dead-letter-exchange":"shop.dlx","delivery-limit":5}' --apply-to queues
```

По умолчанию сообщение уходит в DLX с **исходным routing key**. Задать другой можно через `dead-letter-routing-key`.

## 8.3 Заголовок x-death

Брокер дописывает историю в заголовки мёртвого сообщения:

```
x-first-death-reason:   rejected
x-first-death-queue:    payments
x-first-death-exchange: shop.events
x-death: [
  {reason: rejected, queue: payments, exchange: shop.events,
   routing-keys: [order.created], count: 1, time: ...}
]
```

По `x-death` видно, откуда пришло сообщение и сколько раз оно умирало в каждой очереди. Для quorum queues число доставок есть и в `x-delivery-count`.

## 8.4 Dead lettering в quorum queues: at-most-once и at-least-once

По умолчанию пересылка в DLX идёт в режиме **at-most-once**: если целевая очередь недоступна, мёртвое сообщение теряется. Для надёжной пересылки:

```bash
docker exec rabbit-1 rabbitmqctl set_policy payments-dlx '^payments$' \
  '{"dead-letter-exchange":"shop.dlx","dead-letter-strategy":"at-least-once","overflow":"reject-publish"}' \
  --apply-to queues
```

В режиме `at-least-once` quorum-очередь держит сообщение у себя, пока DLX-очередь не подтвердит приём. Условие — `overflow=reject-publish` (иначе при переполнении пришлось бы удалять сообщения).

## 8.5 TTL

| Где | Как | Особенность |
|---|---|---|
| TTL сообщений очереди | `x-message-ttl` или политика `message-ttl` | Все сообщения очереди живут не дольше N мс |
| TTL отдельного сообщения | свойство `expiration` (строка, мс) | **В classic-очередях истекает только в голове очереди** |
| TTL очереди | `x-expires` или политика `expires` | Удалить неиспользуемую очередь |

**Ловушка TTL отдельного сообщения.** Classic-очередь проверяет истечение только у сообщения в голове. Если впереди стоит сообщение с TTL час, а за ним — с TTL секунда, второе будет ждать час. Поэтому для очередей задержки используй **TTL на очередь**, а не на сообщение.

## 8.6 Ретраи с задержкой через TTL + DLX

Классический паттерн: неудачное сообщение уходит в очередь ожидания без консьюмеров, где лежит N секунд, а потом через DLX возвращается в рабочую очередь.

```
                      nack(requeue=false)
[payments] ─────────────────────────────────> exchange shop.retry
   ^                                               |
   |                                  по ключу -> [payments.retry.10s]  x-message-ttl=10000
   |                                               |   (нет консьюмеров)
   |           истёк TTL -> DLX = shop.events       |
   +───────────────────────────────────────────────+
```

```bash
# очередь ожидания: TTL 10 секунд, после истечения вернуть в рабочую очередь
curl -s $AUTH -X PUT $API/exchanges/%2F/shop.retry -d '{"type":"direct","durable":true}'
curl -s $AUTH -X PUT $API/queues/%2F/payments.retry.10s -d '{"durable":true,"arguments":{
  "x-queue-type":"quorum",
  "x-message-ttl":10000,
  "x-dead-letter-exchange":"",
  "x-dead-letter-routing-key":"payments"}}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.retry/q/payments.retry.10s -d '{"routing_key":"payments"}'
```

Консьюмер решает сам, куда отправить неудачное сообщение: в retry-очередь (опубликовав копию и подтвердив оригинал) или в финальную DLQ, если попыток слишком много. Для нескольких ступеней (10 с, 1 мин, 10 мин) заводят несколько retry-очередей.

```python
MAX_ATTEMPTS = 5

def on_message(ch, method, props, body):
    headers = props.headers or {}
    attempt = headers.get("x-attempt", 0)
    try:
        process(body)
        ch.basic_ack(method.delivery_tag)
    except TemporaryError:
        target = "shop.retry" if attempt < MAX_ATTEMPTS else "shop.dlx"
        # сначала надёжно опубликовать копию (канал в режиме confirms), потом ack оригинала
        ch.basic_publish(target, "payments", body, pika.BasicProperties(
            delivery_mode=2, message_id=props.message_id,
            headers={**headers, "x-attempt": attempt + 1}))
        ch.basic_ack(method.delivery_tag)
    except ValueError:
        ch.basic_reject(method.delivery_tag, requeue=False)   # сразу в DLX
```

Порядок важен: **сначала публикация копии с подтверждением, потом ack оригинала**. Иначе при падении между шагами сообщение пропадёт.

## 8.7 Delayed retry в quorum queues (4.3)

В RabbitMQ 4.3 quorum-очереди умеют откладывать повторную доставку сами, без дополнительных очередей. Возвращённое сообщение (`reject`, `nack`) ждёт перед повторной доставкой, задержка растёт с числом попыток:

```
задержка = min(delayed-retry-min × delivery_count, delayed-retry-max)
```

```bash
docker exec rabbit-1 rabbitmqctl set_policy payments-retry '^payments$' \
  '{"delayed-retry-type":"failed","delayed-retry-min":1000,"delayed-retry-max":60000,
    "delivery-limit":10,"dead-letter-exchange":"shop.dlx"}' --apply-to queues
```

- `delayed-retry-type`: `disabled`, `all`, `failed` или `returned` — какие возвраты задерживать;
- в сочетании с `delivery-limit` и DLX получается полноценная схема «ретраи с растущей задержкой, потом DLQ» средствами одной очереди;
- работает, только когда все узлы кластера на 4.3; точные единицы и допустимые значения сверь с документацией своей версии.

## 8.8 Плагин delayed message exchange

Сторонний плагин `rabbitmq_delayed_message_exchange` добавляет тип exchange `x-delayed-message`: сообщение с заголовком `x-delay` доставляется через заданное время. Удобен для «отправить через час», но:

- отложенные сообщения хранятся **на одном узле и не реплицируются**;
- плохо масштабируется на миллионы отложенных сообщений;
- это отдельный плагин, который нужно ставить и обновлять самому.

Для ретраев предпочитай delayed retry в quorum queues или TTL + DLX. Для длинных отложенных задач (дни) часто проще хранить расписание в базе.

## 8.9 Poison messages и parking lot

«Ядовитое» сообщение падает при каждой обработке. Защита:

1. **`delivery-limit`** в quorum queues (по умолчанию 20; для задач лучше 3–10).
2. **Валидация на входе**: не парсится — сразу `reject(requeue=false)`, без ретраев.
3. **DLQ как parking lot**: мёртвые сообщения копятся в отдельной очереди, **алерт** на любое сообщение в ней.
4. **Инструмент переотправки**: после исправления бага перенеси сообщения обратно (Shovel из модуля 13 или скрипт), сохранив `message_id`.

### Практика

1. Настрой DLX для очереди `payments`, отправь сообщение и сделай `reject(requeue=false)`. Посмотри заголовки `x-death` в `shop.dead`.
2. Собери ретрай через TTL + DLX с задержкой 10 секунд и убедись, что сообщение возвращается не раньше.
3. Отправь сообщение, которое всегда падает, в quorum-очередь с `delivery-limit=3` и `nack(requeue=true)`. Сколько раз его доставят, и где оно окажется?

---

# Модуль 9. Кластер и quorum queues: Raft, Khepri, отказоустойчивость

## 9.1 Что реплицируется в кластере

```
кластер из 3 узлов
  ├── метаданные (vhosts, пользователи, exchanges, bindings, очереди, политики)
  │     -> Khepri: Raft-группа на всех узлах
  ├── quorum queues   -> у каждой своя Raft-группа (обычно 3 реплики)
  ├── streams         -> у каждого своя группа реплик
  └── classic queues  -> НЕ реплицируются, живут на одном узле
```

Клиент может подключиться к **любому** узлу: он увидит все exchanges и очереди кластера, а брокер сам перенаправит операции к узлу-лидеру нужной очереди.

## 9.2 Khepri: метаданные на Raft

С RabbitMQ 4.3 метаданные хранит только **Khepri** — хранилище на Raft. Mnesia и её стратегии обработки сетевых разделов (`pause_minority`, `autoheal`) удалены.

Что это значит на практике:

- кластеру нужно **большинство узлов онлайн**: из 3 узлов — 2, из 5 — 3. Без кворума нельзя объявлять очереди, менять права, создавать bindings;
- восстановление после сбоев и разделов стало единообразным: метаданные, quorum queues и streams восстанавливаются по правилам Raft;
- **нечётное число узлов**: 3 или 5. Второй узел к одному не добавляет отказоустойчивости, четвёртый к трём — тоже.

## 9.3 Размер кластера

| Узлов | Переживает падение | Комментарий |
|---|---|---|
| 1 | 0 | Разработка |
| 2 | 0 | **Хуже одного**: потеря любого узла лишает кворума |
| 3 | 1 | Стандарт продакшена |
| 5 | 2 | Крупные инсталляции, большая стоимость записи |
| 7+ | 3+ | Редко оправдано |

## 9.4 Лидеры quorum-очередей

Каждая quorum-очередь — отдельная Raft-группа со своим лидером. Лидеры распределяются по узлам, и нагрузка делится между ними.

```bash
docker exec rabbit-1 rabbitmq-queues quorum_status payments   # реплики, лидер, отставание
docker exec rabbit-1 rabbitmqctl list_queues name type leader members online
```

Где создаётся лидер новой очереди, задаёт `queue_leader_locator` (`client-local` — на узле, к которому подключён клиент, или `balanced` — на наименее загруженном). После перезапуска узла лидерство перекашивается: верни баланс командой

```bash
docker exec rabbit-1 rabbitmq-queues rebalance quorum
```

## 9.5 Управление репликами

```bash
# добавить реплики всех quorum-очередей на новый узел
docker exec rabbit-1 rabbitmq-queues grow rabbit@rabbit-4 all

# убрать реплики с узла перед его выводом
docker exec rabbit-1 rabbitmq-queues shrink rabbit@rabbit-3

# точечно для одной очереди
docker exec rabbit-1 rabbitmq-queues add_member payments rabbit@rabbit-4
docker exec rabbit-1 rabbitmq-queues delete_member payments rabbit@rabbit-3

# безопасно ли сейчас останавливать узел?
docker exec rabbit-1 rabbitmq-queues check_if_node_is_quorum_critical
```

Новая реплика сначала догоняет лидера как **non-voter** и только потом получает право голоса: так добавление реплики не ослабляет кворум.

`check_if_node_is_quorum_critical` — обязательная проверка перед остановкой узла: если какая-то очередь потеряет кворум при его остановке, команда вернёт ошибку.

## 9.6 Что происходит при сбоях

| Сбой | Метаданные (Khepri) | Quorum queues | Classic queues |
|---|---|---|---|
| 1 узел из 3 | Работают | Лидеры переезжают, очереди доступны | Очереди этого узла недоступны |
| 2 узла из 3 | **Недоступны** (нельзя объявлять и менять топологию) | Очереди без кворума недоступны | Очереди упавших узлов недоступны |
| Сетевой раздел 2 + 1 | Работает сторона большинства | Работает сторона, где большинство реплик | Как повезёт |
| Потерян диск узла | Узел нужно заново ввести в кластер | Реплики восстановятся с лидеров | Данные потеряны |

## 9.7 Как клиенты переживают падение узла

- Указывай клиенту **несколько адресов** узлов или адрес балансировщика (TCP load balancer перед узлами).
- Клиент должен **переподключаться** и заново объявлять каналы и подписки. Java-клиент делает это сам (`automatic recovery`), Go и Python — нет, это нужно реализовать.
- После переподключения неподтверждённые сообщения будут доставлены снова: ещё одна причина для идемпотентности.
- Publisher confirms, не полученные до разрыва, считаются неизвестными: сообщения нужно отправить повторно.

## 9.8 Обслуживание узла

```bash
# перевести узел в режим обслуживания: перенести лидеров, закрыть клиентские соединения
docker exec rabbit-2 rabbitmq-upgrade drain

# ... обновление, перезагрузка ...

# вернуть узел в работу
docker exec rabbit-2 rabbitmq-upgrade revive

# окончательно вывести узел из кластера (выполняется на другом узле, выводимый остановлен)
docker exec rabbit-1 rabbitmqctl forget_cluster_node rabbit@rabbit-3
```

### Практика

1. Создай 10 quorum-очередей и посмотри, как распределились лидеры. Останови узел, верни его и выполни `rebalance quorum`.
2. Проверь `check_if_node_is_quorum_critical`, затем останови два узла из трёх. Что происходит с публикацией в quorum-очередь и с объявлением новой очереди?
3. Выполни `drain` на узле и посмотри, куда переехали лидеры и соединения.

---

# Модуль 10. Streams и super streams

## 10.1 Зачем streams, если есть очереди

| Задача | Очередь | Stream |
|---|---|---|
| Одно событие нужно 50 сервисам | 50 очередей, 50 копий каждого сообщения | Один stream, 50 читателей со своими offset |
| Перечитать события за вчера | Невозможно | Сдвинуть offset |
| Миллионы сообщений в секунду | Упрёшься в одну очередь | Stream-протокол, пакетная запись |
| Хранить неделю истории | Очередь растёт и тормозит | Retention по времени или размеру |

Stream — это лог внутри RabbitMQ, с той же моделью, что у партиции Kafka: запись в конец, чтение с offset, удаление по retention.

## 10.2 Создание и retention

```bash
curl -s -u admin:admin -X PUT http://localhost:15672/api/queues/%2F/shop.events.log \
  -H "content-type: application/json" \
  -d '{"durable":true,"arguments":{
        "x-queue-type":"stream",
        "x-max-age":"7D",
        "x-max-length-bytes":20000000000,
        "x-stream-max-segment-size-bytes":500000000}}'
```

| Параметр | Смысл |
|---|---|
| `x-max-age` | Удалять сегменты старше (`30s`, `12h`, `7D`, `1M`) |
| `x-max-length-bytes` | Удалять старые сегменты при превышении размера |
| `x-stream-max-segment-size-bytes` | Размер сегмента (удаление идёт целыми сегментами) |
| `x-initial-cluster-size` | Число реплик |

Как и в Kafka, данные удаляются **целыми сегментами**, поэтому сообщения могут жить немного дольше заданного срока.

## 10.3 Чтение через AMQP 0-9-1

Streams можно читать обычными AMQP-клиентами:

```python
ch.basic_qos(prefetch_count=500)                   # обязателен для streams
ch.basic_consume(
    "shop.events.log", on_message,
    arguments={"x-stream-offset": "first"},        # first | last | next | <число> | <timestamp>
)

def on_message(ch, method, props, body):
    offset = props.headers.get("x-stream-offset")   # позиция сообщения в stream
    handle(body)
    ch.basic_ack(method.delivery_tag)               # ack нужен для flow control, сообщение не удаляется
```

Через AMQP 0-9-1 брокер **не хранит позицию читателя**: при перезапуске консьюмер сам решает, откуда читать. Сохраняй последний обработанный offset (в базе рядом с результатом) и подписывайся с `x-stream-offset: <сохранённый + 1>`.

## 10.4 Stream-протокол

Для высокой пропускной способности у streams есть отдельный бинарный протокол (порт **5552**, плагин `rabbitmq_stream`) и отдельные клиенты: Java, Go, .NET, Python, Rust.

Что он даёт сверх AMQP:

| Возможность | Что это |
|---|---|
| Пакетная запись и чтение | На порядок выше пропускная способность |
| **Серверное хранение offset** | Консьюмер с именем сохраняет позицию на брокере |
| **Дедупликация публикаций** | Producer с именем и возрастающим `publishing id`: брокер отбрасывает повторы |
| Single active consumer | Один активный читатель с автоматическим переключением |
| Фильтрация | Брокер отдаёт только куски, где есть сообщения с нужным значением фильтра |

Дедупликация — редкий случай, когда RabbitMQ отсекает дубли на стороне брокера: producer должен иметь **стабильное имя** и **монотонно растущий** номер публикации (например, id из outbox).

## 10.5 Фильтрация

Producer помечает сообщения значением фильтра (заголовок `x-stream-filter-value` в AMQP 0-9-1), консьюмер просит только нужные значения:

```python
# producer
ch.basic_publish("", "shop.events.log", body,
                 pika.BasicProperties(headers={"x-stream-filter-value": "eu"}))

# consumer
ch.basic_consume("shop.events.log", on_message, arguments={
    "x-stream-offset": "first",
    "x-stream-filter": "eu",
    "x-stream-match-unfiltered": False,
})
```

Фильтр работает на уровне кусков (bloom-фильтр): брокер пропускает куски, где точно нет нужных значений, но в пришедших кусках могут быть и чужие сообщения. **Консьюмер обязан дофильтровать сам.** С 4.2 для AMQP 1.0-клиентов есть серверные SQL-выражения фильтра.

## 10.6 Super streams: партиционированный stream

Один stream — одна последовательность на одном лидере. Для масштабирования есть **super stream**: набор обычных streams-партиций и exchange, который раскладывает сообщения по ключу.

```bash
docker exec rabbit-1 rabbitmq-streams add_super_stream invoices --partitions 3
```

```
producer --(ключ: customer_id)--> exchange invoices --hash--> invoices-0
                                                         +--> invoices-1
                                                         +--> invoices-2
```

- Сообщения одного ключа попадают в одну партицию: порядок по ключу сохраняется.
- С single active consumer каждую партицию читает один экземпляр приложения, партиции распределяются между экземплярами — модель consumer group из Kafka.
- С 4.2.9/4.3.3 число партиций super stream ограничено по умолчанию 1000 (`stream.max_super_stream_partitions`).

## 10.7 Когда streams, а когда Kafka

| Нужно | Выбор |
|---|---|
| Уже есть RabbitMQ, нужен лог с повторным чтением | Streams |
| Фан-аут одного события на сотни читателей | Streams |
| Экосистема коннекторов, CDC, Kafka Streams, Schema Registry | Kafka |
| Годы истории, петабайты | Kafka с tiered storage |
| Смешанная система: очереди задач + лог | RabbitMQ с quorum queues и streams |

### Вопросы для самопроверки

1. Почему для фан-аута на 50 читателей stream лучше 50 очередей?
2. Кто хранит позицию читателя при чтении stream через AMQP 0-9-1, а кто — через stream-протокол?
3. Что нужно producer'у для дедупликации в stream?
4. Почему консьюмер должен дофильтровывать сообщения даже с фильтром?

---

# Модуль 11. Паттерны: work queues, pub/sub, RPC, приоритеты

## 11.1 Work queue (очередь задач)

```
producer --> [tasks] --> worker 1
                    \--> worker 2
                    \--> worker 3
```

Классика RabbitMQ: задачи распределяются между воркерами, каждая обрабатывается один раз. Рецепт:

- quorum-очередь, `delivery-limit`, DLX;
- manual ack после выполнения;
- prefetch по времени обработки (для долгих задач — 1–5);
- идемпотентные задачи.

## 11.2 Publish/subscribe

```
producer --> [exchange events (fanout или topic)] --> [queue svc-a] --> сервис A
                                                   \-> [queue svc-b] --> сервис B
```

Каждый сервис владеет **своей durable-очередью**. Внутри сервиса экземпляры читают одну очередь (competing consumers), между сервисами каждый получает копию. Это главный паттерн интеграции микросервисов на RabbitMQ.

Правило именования: очередь называется по **потребителю**, а не по событию: `billing.order-events`, а не `order.created`. Тогда видно, чья это очередь, и её отставание — это отставание конкретного сервиса.

## 11.3 RPC: запрос и ответ

```
client                                              server
  |-- publish rpc.pricing                              |
  |     reply_to = amq.rabbitmq.reply-to               |
  |     correlation_id = 42  ------------------------> | обрабатывает
  |<------------ publish в reply_to, correlation_id=42 |
```

**Direct reply-to** — псевдо-очередь `amq.rabbitmq.reply-to`: не нужно создавать очередь ответов, ответ идёт прямо в канал клиента. С 4.2 работает и для AMQP 1.0, в том числе между протоколами.

Клиент:

```python
import uuid
import pika

conn = pika.BlockingConnection(pika.URLParameters("amqp://admin:admin@localhost:5672/%2F"))
ch = conn.channel()
response = {}

def on_reply(ch, method, props, body):
    response[props.correlation_id] = body

# подписаться на псевдо-очередь ДО публикации запроса, обязательно в режиме auto ack
ch.basic_consume("amq.rabbitmq.reply-to", on_reply, auto_ack=True)

corr_id = str(uuid.uuid4())
ch.basic_publish("", "rpc.pricing", b'{"sku":"A1","qty":3}',
                 pika.BasicProperties(reply_to="amq.rabbitmq.reply-to", correlation_id=corr_id,
                                      expiration="5000"))   # запрос не нужен через 5 секунд
conn.process_data_events(time_limit=5)                        # ждём ответ не дольше 5 секунд
print(response.get(corr_id, "таймаут"))
```

Сервер:

```python
ch.queue_declare("rpc.pricing", durable=True, arguments={"x-queue-type": "quorum"})
ch.basic_qos(prefetch_count=10)

def on_request(ch, method, props, body):
    result = calculate(body)
    ch.basic_publish("", props.reply_to, result,
                     pika.BasicProperties(correlation_id=props.correlation_id))
    ch.basic_ack(method.delivery_tag)

ch.basic_consume("rpc.pricing", on_request)
ch.start_consuming()
```

Правила RPC через брокер:

- **всегда таймаут** на стороне клиента и `expiration` на запросе: ответ, который никто не ждёт, не нужен;
- `correlation_id` связывает ответ с запросом;
- сервер должен быть идемпотентным: при таймауте клиент может повторить запрос;
- если нужен быстрый синхронный RPC между сервисами, подумай о gRPC или HTTP: брокер добавляет задержку и точку отказа.

## 11.4 Приоритеты

```bash
# classic: число уровней задаётся при объявлении
curl -s -u admin:admin -X PUT http://localhost:15672/api/queues/%2F/reports \
  -H "content-type: application/json" \
  -d '{"durable":true,"arguments":{"x-queue-type":"classic","x-max-priority":5}}'
```

```python
ch.basic_publish("", "reports", body, pika.BasicProperties(priority=5, delivery_mode=2))
```

- Сообщения с более высоким `priority` доставляются раньше.
- Приоритет работает, только когда в очереди **есть очередь**: если консьюмеры разбирают всё мгновенно, сортировать нечего. Маленький prefetch усиливает эффект.
- Quorum queues поддерживают приоритеты, а с 4.3 — строгие приоритеты с корректным порядком повторной доставки.
- Не делай десятки уровней: 2–5 хватает почти всегда.

## 11.5 Competing consumers и порядок

Несколько консьюмеров на одной очереди — это масштабирование ценой порядка (модуль 6.8). Варианты, если порядок нужен:

| Вариант | Как | Цена |
|---|---|---|
| Single active consumer | Один читает, остальные в резерве | Нет параллелизма |
| Шардирование по ключу | Consistent hash exchange → N очередей, у каждой single active consumer | Сложнее топология |
| Super streams + SAC | Партиции по ключу, по одному активному читателю на партицию | Stream-клиенты |

## 11.6 Саги и хореография

В event-driven системе шаги бизнес-процесса выполняются разными сервисами, связанными событиями:

```
order.created -> payments: списать -> payment.succeeded -> stock: зарезервировать -> stock.reserved -> order confirmed
                                  \-> payment.failed   -> orders: отменить заказ
                                                          stock.failed -> payments: вернуть деньги
```

- каждый шаг идемпотентен и имеет **компенсирующее действие**;
- события публикуются через outbox;
- для сложных процессов с таймаутами и ветвлениями удобнее оркестратор (отдельный сервис, который хранит состояние процесса), чем чистая хореография.

### Вопросы для самопроверки

1. Почему очередь в pub/sub называют по потребителю, а не по событию?
2. Зачем в RPC `correlation_id`, `expiration` и таймаут на клиенте?
3. Почему приоритеты не работают, когда очередь пустая?
4. Как сохранить порядок по ключу и при этом масштабироваться?

---

# Модуль 12. Протоколы: AMQP 1.0, MQTT, STOMP, WebSocket

## 12.1 Какие протоколы понимает RabbitMQ

| Протокол | Порт | Плагин | Для кого |
|---|---|---|---|
| AMQP 0-9-1 | 5672 | встроен | Классические клиенты: pika, amqp091-go, Java amqp-client |
| AMQP 1.0 | 5672 | встроен с 4.0 | Новые клиенты RabbitMQ, Azure Service Bus-совместимые, ActiveMQ-клиенты |
| Stream | 5552 | `rabbitmq_stream` | Высокопроизводительная работа со streams |
| MQTT 3.1.1 и 5.0 | 1883 | `rabbitmq_mqtt` | IoT-устройства |
| STOMP | 61613 | `rabbitmq_stomp` | Простые текстовые клиенты |
| Web MQTT / Web STOMP | 15675 / 15674 | `rabbitmq_web_mqtt` / `rabbitmq_web_stomp` | Браузеры через WebSocket |

Все протоколы работают с **одними и теми же** exchanges и очередями: устройство публикует по MQTT, а сервис читает эти сообщения по AMQP.

## 12.2 AMQP 1.0

С RabbitMQ 4.0 AMQP 1.0 — **базовый протокол брокера** на том же порту 5672, без отдельного плагина. Для него есть новые официальные клиенты (Java, .NET, Go, Python), которые умеют ещё и управлять топологией.

Адресация в AMQP 1.0 (версия 2 адресов):

| Адрес | Что значит |
|---|---|
| `/queues/payments` | Публиковать в очередь или читать из неё |
| `/exchanges/shop.events/order.created` | Публиковать в exchange с ключом |
| `/exchanges/shop.events` | Публиковать в exchange, ключ задаётся в сообщении |

Отличия от AMQP 0-9-1, которые стоит знать:

- исходы доставки богаче: `accepted`, `released` (вернуть без увеличения счётчика доставок), `rejected` (в DLX), `modified` (вернуть с изменением аннотаций);
- с 4.3 publisher получает в исходе `rejected` имя очереди и причину отказа (`maxlen` или `unavailable`);
- серверная фильтрация streams (Property Filter Expressions с 4.1, SQL-выражения с 4.2);
- direct reply-to с 4.2 работает между протоколами.

Если начинаешь новый проект и клиент для твоего языка готов — AMQP 1.0 с официальным клиентом RabbitMQ стоит рассмотреть. Старые адреса формата v1 с 4.3 по умолчанию запрещены.

## 12.3 MQTT

```bash
docker exec rabbit-1 rabbitmq-plugins enable rabbitmq_mqtt
```

(Добавь `rabbitmq_mqtt` в `enabled_plugins` и порт `1883` в compose, чтобы плагин включался на всех узлах.)

Как MQTT ложится на модель RabbitMQ:

| MQTT | RabbitMQ |
|---|---|
| Публикация в топик `sensors/eu/temp` | Публикация в `amq.topic` с ключом `sensors.eu.temp` |
| `/` в топике | `.` в routing key |
| `+` (один уровень) | `*` |
| `#` (много уровней) | `#` |
| Подписка QoS 0 | Специальный тип очереди MQTT QoS 0 (без хранения, максимально быстро) |
| Подписка QoS 1 | Обычная очередь (classic или quorum по настройке) |

```
датчик --MQTT publish sensors/eu/temp--> amq.topic --binding sensors.#--> [telemetry] --AMQP--> сервис аналитики
```

RabbitMQ поддерживает MQTT 5.0 и держит сотни тысяч и миллионы MQTT-соединений на кластер при правильной настройке. Для IoT с очень большим числом устройств на краю сети иногда ставят отдельный MQTT-брокер и мост в RabbitMQ.

## 12.4 STOMP и WebSocket

STOMP — простой текстовый протокол: удобен для скриптов и старых систем. Web STOMP и Web MQTT дают доступ из браузера через WebSocket:

```javascript
// браузер: Web MQTT через mqtt.js
const client = mqtt.connect("ws://localhost:15675/ws", { username: "web", password: "..." });
client.subscribe("orders/+/status");
client.on("message", (topic, payload) => console.log(topic, payload.toString()));
```

Выдавай браузерным клиентам **отдельного пользователя** с правами только на нужные топики (topic permissions, модуль 17): браузер — недоверенная среда.

### Вопросы для самопроверки

1. Как MQTT-топик `sensors/eu/temp` превращается в routing key RabbitMQ?
2. Чем исход `released` в AMQP 1.0 отличается от `rejected`?
3. Почему браузерным клиентам нужен отдельный пользователь с ограниченными правами?

---

# Модуль 13. Несколько дата-центров: Federation и Shovel

## 13.1 Почему не растягивать кластер между регионами

Кластер RabbitMQ построен на Raft: каждая запись в quorum-очередь и в метаданные ждёт подтверждения большинства узлов. Между регионами с задержкой в десятки миллисекунд это:

- медленная публикация и медленные изменения топологии;
- риск потери кворума при проблемах с сетью между регионами.

Узлы одного кластера должны стоять в **одном регионе** (можно в разных зонах доступности). Между регионами сообщения передают **Federation** или **Shovel**.

## 13.2 Federation

Federation связывает брокеры **асинхронно** на уровне exchanges или очередей.

**Federated exchange:** сообщения, опубликованные в exchange вышестоящего брокера (upstream), копируются в exchange нижестоящего, если там есть заинтересованные bindings.

```
регион EU (upstream)                         регион US (downstream)
exchange shop.events  ---federation link--->  exchange shop.events -> [us.analytics]
```

**Federated queue:** очередь-downstream забирает сообщения из одноимённой очереди upstream, когда у неё есть свободные консьюмеры. Так консьюмеры в двух регионах разбирают одну логическую очередь.

```bash
docker exec rabbit-us rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management

# на брокере US: описать upstream
docker exec rabbit-us rabbitmqctl set_parameter federation-upstream eu \
  '{"uri":"amqps://federation:secret@rabbit-eu.example.com:5671","ack-mode":"on-confirm","expires":3600000}'

# политика: федерировать exchanges shop.*
docker exec rabbit-us rabbitmqctl set_policy federate-shop '^shop\.' \
  '{"federation-upstream-set":"all"}' --apply-to exchanges

docker exec rabbit-us rabbitmqctl federation_status   # или страница Admin → Federation Status в UI
```

`ack-mode=on-confirm` — сообщение подтверждается upstream'у только после confirm на downstream: без потерь при сбоях связи.

## 13.3 Shovel

Shovel — более простой и прямолинейный инструмент: **забрать сообщения из источника и опубликовать в приёмник**. Источник и приёмник — очереди или exchanges на том же или другом брокере, по AMQP 0-9-1 или AMQP 1.0.

```bash
docker exec rabbit-1 rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management

docker exec rabbit-1 rabbitmqctl set_parameter shovel orders-to-dr \
  '{"src-protocol":"amqp091","src-uri":"amqp://","src-queue":"orders.outbound",
    "dest-protocol":"amqp091","dest-uri":"amqps://shovel:secret@rabbit-dr.example.com:5671",
    "dest-exchange":"shop.events","ack-mode":"on-confirm"}'

docker exec rabbit-1 rabbitmqctl shovel_status
```

Типичные применения Shovel:

- перенос сообщений между брокерами при миграции (например, со старого кластера на новый — blue-green);
- переотправка сообщений из DLQ обратно в рабочую очередь после исправления бага;
- односторонняя доставка данных в другой регион или в изолированный контур.

С 4.3.5 у динамических shovel появился параметр `src-delete-after-duration`: shovel сам удаляется через заданное время, удобно для разовых переносов.

## 13.4 Federation или Shovel

| | Federation | Shovel |
|---|---|---|
| Уровень | Exchanges и очереди, по политикам | Пара источник → приёмник |
| Настройка | Upstream + политика, применяется ко многим объектам | Каждый shovel отдельно |
| Направление | Downstream тянет с upstream | Как настроишь |
| Когда | Постоянная связь регионов, общая топология | Миграции, переотправка, точечные мосты |

## 13.5 Disaster recovery

| Сценарий | Решение |
|---|---|
| Упал один узел | Quorum queues, 3 узла |
| Упала зона доступности | Узлы в разных зонах одного региона |
| Упал регион | Второй кластер + Federation/Shovel для данных, определения (definitions) синхронизированы |
| Приложение испортило данные | Экспорт definitions в Git; сами сообщения в очередях не бэкапят — важные события должны храниться в источнике (outbox, база) |

Определения (exchanges, очереди, bindings, политики, пользователи) экспортируются и импортируются одной командой:

```bash
docker exec rabbit-1 rabbitmqctl export_definitions /tmp/definitions.json
docker exec rabbit-1 rabbitmqctl import_definitions /tmp/definitions.json
```

Держи definitions в Git и применяй автоматически: так резервный кластер всегда имеет ту же топологию.

### Практика

1. Подними два отдельных брокера и настрой federated exchange: опубликуй в upstream и получи сообщение в очереди downstream.
2. Настрой shovel, который переносит сообщения из `shop.dead` обратно в `payments`, и выключи его после переноса.
3. Экспортируй definitions кластера и импортируй их в чистый брокер.

---

# Модуль 14. Политики, лимиты и управление ресурсами

## 14.1 Политики и operator policies

**Политики** (модуль 4.6) задают поведение очередей и exchanges по шаблону имени: TTL, лимиты длины, DLX, delivery-limit, federation. Их меняют владельцы приложений.

**Operator policies** задаёт администратор кластера. Они накладываются поверх обычных политик и нужны, чтобы **ограничить** приложения: для числовых ключей (например, `max-length`, `message-ttl`, `delivery-limit`) побеждает **меньшее** значение из политики и operator policy.

```bash
# обычная политика команды shop
docker exec rabbit-1 rabbitmqctl set_policy -p shop shop-queues '.*' \
  '{"max-length": 500000, "dead-letter-exchange": "shop.dlx"}' --apply-to queues

# ограничение от администратора: никакая очередь в shop не держит больше 1 млн сообщений
docker exec rabbit-1 rabbitmqctl set_operator_policy -p shop cap '.*' \
  '{"max-length": 1000000, "overflow": "reject-publish"}' --apply-to queues

docker exec rabbit-1 rabbitmqctl list_operator_policies -p shop
```

## 14.2 Лимиты на vhost и пользователя

```bash
# vhost: не больше 256 соединений и 1024 очередей
docker exec rabbit-1 rabbitmqctl set_vhost_limits -p shop '{"max-connections": 256, "max-queues": 1024}'

# пользователь: не больше 20 соединений и 200 каналов
docker exec rabbit-1 rabbitmqctl set_user_limits billing '{"max-connections": 20, "max-channels": 200}'
```

Лимиты защищают кластер от одного приложения с утечкой соединений или бесконечным созданием очередей — типичной причины аварий.

В `rabbitmq.conf` есть и общие лимиты узла: `max_connections` и `max_channels` (так они называются с 4.2.7/4.3.1; старые имена `connection_max` и `channel_max` работают как синонимы).

## 14.3 Аларм памяти

```ini
# rabbitmq.conf
vm_memory_high_watermark.relative = 0.6      # 60% доступной памяти (значение по умолчанию)
# или абсолютно:
# vm_memory_high_watermark.absolute = 6GB
```

Когда процесс RabbitMQ занимает больше порога, срабатывает **аларм памяти**:

```
память > vm_memory_high_watermark
   -> аларм на узле
   -> ВСЕ публикующие соединения кластера блокируются (connection.blocked)
   -> консьюмеры продолжают работать и разгребают очереди
   -> память освободилась -> публикация возобновляется
```

В контейнере RabbitMQ видит лимит памяти cgroup, поэтому порог считается от лимита контейнера. Не ставь порог выше 0.7–0.8: Erlang нужна память на сборку мусора, и при нехватке ОС убьёт процесс.

## 14.4 Аларм диска

```ini
disk_free_limit.absolute = 4GB
# или относительно объёма памяти:
# disk_free_limit.relative = 1.5
```

Если свободного места на диске меньше лимита, публикации блокируются так же, как при аларме памяти. **Значение по умолчанию (50 МБ) слишком мало для продакшена**: quorum-очереди и streams пишут много, и до срабатывания аларма диск может успеть заполниться. Ставь несколько гигабайт или `relative = 1.0–2.0`.

```bash
docker exec rabbit-1 rabbitmq-diagnostics check_local_alarms
docker exec rabbit-1 rabbitmq-diagnostics alarms
docker exec rabbit-1 rabbitmq-diagnostics memory_breakdown
```

## 14.5 Flow control на соединениях

Кроме глобальных алармов есть **кредитный flow control**: если очередь или узел не успевают обрабатывать публикации, брокер притормаживает конкретные publishing-соединения. В management UI такое соединение показывается в состоянии `flow`.

Кратковременный `flow` нормален. Постоянный `flow` означает, что publisher'ы пишут быстрее, чем очередь успевает принять: нужно больше очередей (шардирование), быстрее диски или меньше нагрузки.

## 14.6 Длинные очереди — это проблема

RabbitMQ быстрее всего работает, когда очереди **короткие**: сообщения приходят и почти сразу уходят к консьюмерам. Очередь с миллионами сообщений:

- занимает память и диск;
- медленнее восстанавливается после перезапуска;
- для quorum-очереди раздувает Raft-лог и снапшоты.

Правила:

- мониторь длину и возраст сообщений, а не только скорость;
- лимит длины + `reject-publish` на каждой важной очереди, чтобы переполнение было видно, а не бесконечно;
- если нужно хранить много и долго — это задача для streams.

## 14.7 Feature flags и устаревшие функции

**Feature flags** включают новые возможности, когда все узлы кластера обновлены:

```bash
docker exec rabbit-1 rabbitmqctl list_feature_flags name state
docker exec rabbit-1 rabbitmqctl enable_feature_flag all     # после обновления всех узлов
```

**Deprecated features** — устаревшие функции, которые проходят этапы «разрешено по умолчанию» → «запрещено по умолчанию» → удалено. С 4.3 запрещены по умолчанию, например, `transient_nonexcl_queues` и `global_qos`. Временно разрешить можно в `rabbitmq.conf` на **всех** узлах:

```ini
deprecated_features.permit.transient_nonexcl_queues = true
```

Лучше не включать, а переписать код: эти функции будут удалены.

### Вопросы для самопроверки

1. Чем operator policy отличается от обычной политики?
2. Что происходит с publisher'ами и консьюмерами при аларме памяти?
3. Почему значение `disk_free_limit` по умолчанию опасно?
4. Почему длинные очереди — проблема для RabbitMQ?

---

# Модуль 15. Производительность и тюнинг

## 15.1 Порядки величин

| Сценарий | Ориентир на хорошем железе |
|---|---|
| Одна classic-очередь, маленькие сообщения | Десятки тысяч msg/s |
| Одна quorum-очередь, confirms | Десятки тысяч msg/s (упирается в одного лидера и диск) |
| Кластер из 3 узлов, много очередей | Сотни тысяч msg/s |
| Stream через stream-протокол | Сотни тысяч – миллионы msg/s |
| Задержка при коротких очередях | Единицы миллисекунд |

Главное: **одна очередь — один процесс на лидере**. Масштабирование RabbitMQ идёт через **число очередей**, а не через «ускорение» одной.

## 15.2 Бенчмарк: PerfTest

```bash
docker run -it --rm --network host pivotalrabbitmq/perf-test:latest \
  --uri amqp://admin:admin@localhost:5672 \
  --queue perf-q --quorum-queue \
  --producers 2 --consumers 2 \
  --confirm 100 --qos 100 \
  --size 1000 --time 60
```

- `--confirm 100` — не больше 100 неподтверждённых публикаций на producer'а;
- `--qos 100` — prefetch консьюмера;
- PerfTest выводит скорость отправки, получения и задержку (перцентили).

Меряй на своём железе, со своими размерами сообщений и своими настройками. Чужие цифры бесполезны.

## 15.3 Что тюнить у publisher'а

| Настройка | Эффект |
|---|---|
| Асинхронные confirms с окном 100–1000 | Главный рычаг пропускной способности |
| Синхронное ожидание confirm на каждое сообщение | Надёжно, но медленно: для низких нагрузок |
| Долгоживущие соединения и каналы | Открытие соединения на сообщение убивает брокер |
| Размер сообщения | Маленькие сообщения — больше накладных расходов на единицу данных; огромные — давление на память |
| Отдельное соединение для публикации | Flow control не тормозит консьюмеров |

## 15.4 Что тюнить у консьюмера

| Настройка | Эффект |
|---|---|
| Prefetch | 1 — медленно; 10–300 — обычно оптимально; без лимита — опасно |
| Ack пачками (`multiple=true`) | Меньше сетевых операций |
| Число консьюмеров | Больше консьюмеров — выше параллелизм, пока очередь не станет узким местом |
| Скорость обработки | Чаще всего узкое место — база и внешние API, а не брокер |

## 15.5 Сервер

- **Диски:** быстрые SSD или NVMe для quorum-очередей и streams: они пишут с fsync.
- **CPU:** RabbitMQ хорошо использует несколько ядер (планировщики Erlang). 4–8 ядер на узел — типичный старт.
- **Память:** держи запас до `vm_memory_high_watermark`; аларм памяти — это остановка всех publisher'ов.
- **Файловые дескрипторы:** лимит 100 000+; каждое соединение — дескриптор.
- **Сеть:** узлы кластера — в одном регионе, с низкой задержкой между собой.
- **Меньше очередей-однодневок:** массовое создание и удаление очередей (очередь на запрос, на пользователя) нагружает метаданные кластера.

## 15.6 Что реально ускоряет систему

1. **Короткие очереди**: консьюмеры успевают за publisher'ами.
2. **Асинхронные confirms с окном** вместо ожидания каждого сообщения.
3. **Правильный prefetch**.
4. **Больше очередей** (шардирование) вместо одной огромной.
5. **Streams** для больших потоков с повторным чтением.
6. **Долгоживущие соединения и каналы**.
7. **Отключение лишних метрик**: если используешь Prometheus, сбор статистики management-плагином можно выключить (`management_agent.disable_metrics_collector = true`).

---

# Модуль 16. Мониторинг и алерты

## 16.1 Откуда брать метрики

Плагин `rabbitmq_prometheus` отдаёт метрики на порту **15692**:

| Endpoint | Что отдаёт |
|---|---|
| `/metrics` | Агрегированные метрики узла (дёшево, для большинства дашбордов) |
| `/metrics/per-object` | Метрики по каждой очереди и соединению (дорого при тысячах объектов) |
| `/metrics/detailed?family=...&vhost=...` | Выборочные подробные метрики, например по очередям одного vhost |

```bash
curl -s localhost:15692/metrics | grep -E '^rabbitmq_(queue_messages_ready|connections|alarms)' | head
curl -s "localhost:15692/metrics/detailed?family=queue_coarse_metrics&vhost=%2F" | head
```

Команда RabbitMQ публикует готовые дашборды Grafana (Overview, Quorum Queues Raft, Streams). С 4.2 метрики Raft переименованы: обнови дашборды и алерты при обновлении.

Management UI удобен для разбора ситуации, но не заменяет Prometheus: он хранит историю недолго, а сбор его статистики сам стоит ресурсов.

## 16.2 Ключевые метрики

| Метрика | Что показывает |
|---|---|
| `rabbitmq_queue_messages_ready` | Сообщения, ждущие консьюмера |
| `rabbitmq_queue_messages_unacked` | Доставлены, но не подтверждены |
| `rabbitmq_queue_consumers` | Консьюмеры на очереди (0 при растущей очереди — авария) |
| `rabbitmq_connections`, `rabbitmq_channels` | Число соединений и каналов (рост — утечка) |
| `rabbitmq_alarms_memory_used_watermark` | 1 — аларм памяти, публикации заблокированы |
| `rabbitmq_alarms_free_disk_space_watermark` | 1 — аларм диска |
| `rabbitmq_process_resident_memory_bytes` | Память процесса |
| `rabbitmq_disk_space_available_bytes` | Свободное место |
| `rabbitmq_global_messages_unroutable_dropped_total` | Сообщения, выброшенные без маршрута |
| `rabbitmq_global_messages_redelivered_total` | Повторные доставки |
| `rabbitmq_global_messages_confirmed_total` | Подтверждённые публикации |

## 16.3 Проверки здоровья

```bash
docker exec rabbit-1 rabbitmq-diagnostics ping                      # узел отвечает
docker exec rabbit-1 rabbitmq-diagnostics check_running             # приложение RabbitMQ запущено
docker exec rabbit-1 rabbitmq-diagnostics check_local_alarms        # нет алармов
docker exec rabbit-1 rabbitmq-diagnostics check_port_connectivity   # слушает клиентские порты
docker exec rabbit-1 rabbitmq-diagnostics check_virtual_hosts       # vhosts работают
docker exec rabbit-1 rabbitmq-queues check_if_node_is_quorum_critical
```

Для Kubernetes: liveness — `rabbitmq-diagnostics ping` (узел жив), readiness — `rabbitmq-diagnostics check_port_connectivity` (принимает клиентов). Не делай liveness-пробу строгой: перезапуск узла из-за аларма только ухудшит ситуацию.

## 16.4 Что алертить

| Алерт | Условие | Почему важно |
|---|---|---|
| **Аларм памяти или диска** | Любой | Публикации во всём кластере заблокированы |
| **Очередь без консьюмеров** | `consumers == 0` и `messages_ready > 0` | Сервис не работает |
| **Растущая очередь** | `messages_ready` растёт 10+ минут | Консьюмеры не справляются |
| **Растущий unacked** | Долго высокий | Консьюмеры зависли или не делают ack |
| **Сообщения в DLQ** | Любые | Бизнес-логика падает |
| **Unroutable dropped** | Растёт | Сломана маршрутизация, сообщения теряются |
| **Узел недоступен** | Меньше узлов, чем ожидается | Кворум под угрозой |
| **Quorum-очередь без лидера или с отстающими репликами** | Любая | Очередь недоступна или теряет избыточность |
| **Рост соединений или каналов** | Резкий | Утечка соединений в приложении |
| **Свободное место** | < 20% | Скоро аларм диска |
| **Ошибки аутентификации** | Рост | Атака или сломанный деплой |

## 16.5 Повседневные команды

```bash
docker exec rabbit-1 rabbitmqctl cluster_status
docker exec rabbit-1 rabbitmqctl list_queues name type messages_ready messages_unacknowledged consumers
docker exec rabbit-1 rabbitmqctl list_connections name user state channels
docker exec rabbit-1 rabbitmqctl list_consumers queue_name channel_pid prefetch_count
docker exec rabbit-1 rabbitmq-queues quorum_status payments
docker exec rabbit-1 rabbitmq-diagnostics memory_breakdown
docker exec rabbit-1 rabbitmq-diagnostics log_tail -N 100
```

---

# Модуль 17. Безопасность: TLS, пользователи, права, OAuth 2

## 17.1 Первые шаги

- **Удали или не используй `guest`.** Он может подключаться только с localhost, но в продакшене ему нечего делать.
- **Отдельный пользователь на каждый сервис** с минимальными правами.
- **Отдельный vhost** на команду или окружение.
- **TLS** на клиентских портах, management UI — по HTTPS.
- **Erlang cookie** — секрет: с ним можно получить полный контроль над узлом. Храни в секретах, а не в образе или репозитории.

## 17.2 Пользователи, теги и права

```bash
docker exec rabbit-1 rabbitmqctl add_vhost shop
docker exec rabbit-1 rabbitmqctl add_user billing 'длинный-случайный-пароль'
docker exec rabbit-1 rabbitmqctl set_user_tags billing        # без тегов: нет доступа к UI

# права: configure (объявлять), write (публиковать, биндить очередь к exchange), read (читать, биндить exchange как источник)
docker exec rabbit-1 rabbitmqctl set_permissions -p shop billing \
  '^billing\..*' \
  '^(shop\.events|billing\..*)$' \
  '^(shop\.events|billing\..*)$'

docker exec rabbit-1 rabbitmqctl list_user_permissions billing
```

Три регулярных выражения — это права **configure**, **write** и **read** на имена ресурсов (exchanges и очередей):

| Операция | Нужно право |
|---|---|
| Объявить или удалить очередь или exchange | configure на этот объект |
| Опубликовать в exchange | write на exchange |
| Читать из очереди (consume, get) | read на очередь |
| Привязать очередь к exchange | write на очередь, read на exchange |

**Теги пользователей** управляют доступом к management UI и HTTP API:

| Тег | Доступ |
|---|---|
| `management` | Свои vhosts и объекты |
| `policymaker` | + политики и параметры |
| `monitoring` | + чтение состояния всего кластера |
| `administrator` | Всё |

## 17.3 Topic permissions

Для topic exchanges можно ограничить, **с какими routing key** пользователь может публиковать и подписываться:

```bash
# order-service может публиковать в shop.events только события order.*
docker exec rabbit-1 rabbitmqctl set_topic_permissions -p shop order-service shop.events '^order\..*' '^$'
```

Особенно важно для MQTT и браузерных клиентов (модуль 12): устройство не должно публиковать в чужие топики.

## 17.4 TLS

```ini
# rabbitmq.conf
listeners.tcp = none                       # отключить незашифрованный порт
listeners.ssl.default = 5671

ssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem
ssl_options.certfile   = /etc/rabbitmq/tls/server.pem
ssl_options.keyfile    = /etc/rabbitmq/tls/server-key.pem
ssl_options.verify     = verify_peer
ssl_options.fail_if_no_peer_cert = false   # true = требовать клиентский сертификат (mTLS)

management.ssl.port       = 15671
management.ssl.cacertfile = /etc/rabbitmq/tls/ca.pem
management.ssl.certfile   = /etc/rabbitmq/tls/server.pem
management.ssl.keyfile    = /etc/rabbitmq/tls/server-key.pem
```

Клиент подключается по `amqps://host:5671`. С mTLS и плагином `rabbitmq_auth_mechanism_ssl` клиент может аутентифицироваться сертификатом (механизм `EXTERNAL`) без пароля.

Не забудь межузловой трафик: для кластеров в недоверенной сети настраивают TLS и для Erlang distribution между узлами.

## 17.5 OAuth 2 и LDAP

**OAuth 2** (плагин `rabbitmq_auth_backend_oauth2`): клиенты подключаются с JWT-токеном вместо пароля, права берутся из scope токена:

```
rabbitmq.read:shop/billing.*      читать из ресурсов billing.* в vhost shop
rabbitmq.write:shop/shop.events   публиковать в shop.events
rabbitmq.tag:monitoring           тег пользователя
```

Работает с Keycloak, Entra ID, Okta и другими OIDC-провайдерами, включая вход в management UI. Токены короткоживущие, а с 4.x клиент может обновить токен в открытом соединении.

**LDAP** (`rabbitmq_auth_backend_ldap`): пользователи и группы из корпоративного каталога.

Бэкенды можно комбинировать: например, внутренние пользователи для служебных аккаунтов и OAuth 2 для людей.

## 17.6 Секреты в конфигурации

- Значения в `rabbitmq.conf` можно хранить зашифрованными (`encrypted:...`), с 4.2–4.3 это поддерживают многие ключи.
- `protected_users` не даёт удалить критичных пользователей через HTTP API.
- В Kubernetes cookie, пароли и сертификаты — только через Secrets.

## 17.7 Чеклист безопасности

- [ ] `guest` удалён или не используется
- [ ] Отдельный пользователь на сервис, права по регулярным выражениям на свои ресурсы
- [ ] Topic permissions для публикации в общие topic exchanges
- [ ] Отдельные vhosts на команды и окружения, лимиты на vhost и пользователей
- [ ] TLS на клиентских портах, незашифрованный порт выключен
- [ ] Management UI по HTTPS и не открыт в интернет
- [ ] Erlang cookie в секретах, длинный и случайный
- [ ] OAuth 2 или LDAP для людей
- [ ] Секреты в конфигурации зашифрованы
- [ ] Алерты на ошибки аутентификации
- [ ] Порты Erlang distribution (4369, 25672) недоступны снаружи кластера

---

# Модуль 18. RabbitMQ в продакшене: Kubernetes, эксплуатация, апгрейды

## 18.1 Референсная архитектура

```
              приложения (publishers / consumers)
                          |
                 TCP load balancer (или список адресов в клиенте)
                          |
       +------------------+------------------+
       |         кластер RabbitMQ 4.3        |
       |  3 узла в разных зонах одного региона|
       |  quorum queues (3 реплики), streams |
       |  Khepri (метаданные на Raft)        |
       +------------------+------------------+
                          |
          Federation / Shovel --> кластер в резервном регионе
                          |
      Prometheus + Grafana + Alertmanager, definitions в Git
```

## 18.2 Размеры

| Нагрузка | Конфигурация |
|---|---|
| До нескольких тысяч msg/s | 3 узла × 2–4 vCPU, 8 ГБ RAM, SSD |
| Десятки тысяч msg/s | 3 узла × 4–8 vCPU, 16–32 ГБ RAM, NVMe |
| Сотни тысяч msg/s | 3–5 узлов × 8–16 vCPU, 32–64 ГБ RAM, NVMe, шардирование очередей, streams |
| Несколько регионов | Кластер на регион + Federation/Shovel |

Считай отдельно: число очередей и соединений (каждое соединение стоит памяти), объём сообщений в очередях в худшем случае (сколько накопится, если консьюмеры лягут на час) и диск под Raft-логи quorum-очередей и streams.

## 18.3 Kubernetes: RabbitMQ Cluster Operator

Официальный способ — **RabbitMQ Cluster Operator** (кластеры) и **Messaging Topology Operator** (очереди, exchanges, bindings, пользователи, политики как ресурсы Kubernetes).

```yaml
apiVersion: rabbitmq.com/v1beta1
kind: RabbitmqCluster
metadata:
  name: shop
spec:
  replicas: 3
  image: rabbitmq:4.3-management
  resources:
    requests: { cpu: "2", memory: 8Gi }
    limits:   { cpu: "2", memory: 8Gi }
  persistence:
    storageClassName: fast-ssd
    storage: 100Gi
  rabbitmq:
    additionalPlugins: [rabbitmq_stream, rabbitmq_prometheus]
    additionalConfig: |
      disk_free_limit.absolute = 8GB
      vm_memory_high_watermark.relative = 0.6
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchLabels: { app.kubernetes.io/name: shop }
          topologyKey: topology.kubernetes.io/zone
---
apiVersion: rabbitmq.com/v1beta1
kind: Queue
metadata:
  name: payments
spec:
  name: payments
  type: quorum
  durable: true
  rabbitmqClusterReference: { name: shop }
---
apiVersion: rabbitmq.com/v1beta1
kind: Policy
metadata:
  name: shop-limits
spec:
  name: shop-limits
  pattern: "^(payments|stock)$"
  applyTo: queues
  definition:
    max-length: 500000
    overflow: reject-publish
    dead-letter-exchange: shop.dlx
    delivery-limit: 10
  rabbitmqClusterReference: { name: shop }
```

Что оператор делает за тебя: StatefulSet со стабильными именами узлов, Erlang cookie и учётные данные в Secrets, peer discovery через Kubernetes API, безопасный rolling restart с учётом кворума.

Что важно помнить:

- **постоянные тома** обязательны;
- **anti-affinity по зонам**: три пода в одной зоне не переживут её отказ;
- лимит памяти контейнера = основа для аларма памяти: не давай его слишком маленьким;
- топология как ресурсы в Git — это инфраструктура как код и защита от «кто-то удалил очередь руками».

## 18.4 Апгрейды

Правила из release notes, которые важно знать:

- **обновляться можно только на следующую серию**: на 4.3 — только с последнего патча 4.2.x, на 4.2 — с 4.1, 4.0 или 3.13;
- **перед обновлением включи все feature flags**: `rabbitmqctl enable_feature_flag all`, иначе новые узлы не стартуют в кластере;
- **Erlang**: для 4.3 (и последних патчей 4.2) нужен Erlang 27+. Официальные образы Docker уже содержат подходящий Erlang;
- **смешанные версии** в кластере допустимы только на время rolling upgrade — несколько часов, не дни.

Rolling upgrade:

```bash
# 1. проверить здоровье и что узел можно остановить
docker exec rabbit-1 rabbitmq-diagnostics check_local_alarms
docker exec rabbit-1 rabbitmq-queues check_if_node_is_quorum_critical

# 2. перевести узел в обслуживание
docker exec rabbit-1 rabbitmq-upgrade drain

# 3. обновить образ или пакет и перезапустить узел

# 4. вернуть в работу и дождаться синхронизации реплик
docker exec rabbit-1 rabbitmq-upgrade revive
docker exec rabbit-1 rabbitmq-queues quorum_status payments

# 5. повторить для остальных узлов по одному
# 6. когда все узлы обновлены:
docker exec rabbit-1 rabbitmqctl enable_feature_flag all
docker exec rabbit-1 rabbitmq-queues rebalance quorum
```

**Blue-green** для больших скачков версий или миграции со старых кластеров (например, с classic mirrored queues на 3.x): поднимается новый кластер, топология переносится через definitions, сообщения — через Shovel или Federation, клиенты переключаются, старый кластер выводится.

## 18.5 Чеклист продакшена

- [ ] 3 (или 5) узлов в разных зонах одного региона
- [ ] Quorum queues для всего важного, classic — только для временных и эксклюзивных
- [ ] Publisher confirms, `mandatory` или alternate exchange
- [ ] Manual ack после обработки, осмысленный prefetch
- [ ] `delivery-limit`, DLX и алерт на DLQ
- [ ] Лимиты длины очередей с `reject-publish`
- [ ] `disk_free_limit` в гигабайтах, память с запасом до аларма
- [ ] Лимиты на vhost и пользователей
- [ ] Отдельные соединения для публикации и потребления, долгоживущие
- [ ] Переподключение клиентов реализовано и проверено
- [ ] Идемпотентные консьюмеры, outbox для бизнес-событий
- [ ] Prometheus, дашборды, алерты из модуля 16
- [ ] TLS, отдельные пользователи, topic permissions
- [ ] Definitions и политики в Git, применяются автоматически
- [ ] Отрепетированы: падение узла, падение зоны, аларм диска, rolling upgrade

## 18.6 Антипаттерны

| Антипаттерн | Почему плохо | Как правильно |
|---|---|---|
| Соединение на каждое сообщение | Нагрузка на брокер, исчерпание портов и памяти | Долгоживущие соединения и каналы |
| Auto ack для важных данных | Потери при падении консьюмера | Manual ack после обработки |
| Публикация без confirms | Не знаешь, дошло ли сообщение | Publisher confirms |
| Classic-очереди для важных данных | Нет репликации | Quorum queues |
| `nack(requeue=true)` на любую ошибку | Бесконечный цикл «ядовитых» сообщений | Delivery-limit, DLX, ретраи с задержкой |
| Prefetch без лимита | Память консьюмера и неравномерное распределение | Осмысленный prefetch |
| Очереди с миллионами сообщений | Память, медленное восстановление | Короткие очереди, лимиты, streams |
| Очередь на каждый запрос или пользователя | Нагрузка на метаданные кластера | Общие очереди, direct reply-to |
| Кластер растянут между регионами | Медленно, потеря кворума | Кластер на регион + Federation/Shovel |
| Два узла в кластере | Нет кворума при потере любого | 3 или 5 узлов |
| `guest` в продакшене | Дыра в безопасности | Отдельные пользователи |
| Аргументы очередей для TTL и лимитов | Нельзя поменять без пересоздания | Политики |
| RabbitMQ как база с историей | Очереди не для хранения | Streams или база |

---

# Модуль 19. Итоговый проект: event-driven интернет-магазин

## 19.1 Что строим

```
                     ┌──────────────┐
  HTTP  ────────────►│  Order API   │── INSERT orders + outbox (одна транзакция)
                     └──────┬───────┘
                            │ ретранслятор outbox (confirms, message_id)
                            ▼
                  exchange shop.events (topic)
       ┌───────────────┬──────────┴──────────┬───────────────────┐
       ▼               ▼                     ▼                   ▼
  [billing.orders] [stock.orders]   [notifications.events]  stream shop.events.log
   quorum,          quorum,          quorum,                  аналитика и аудит
   delivery-limit   single active     priority                (повторное чтение)
       │            consumer
       ▼               │
  Payment Service   Warehouse          Notification workers (email / push)
  идемпотентно      порядок по складу
       │
  payment.succeeded / payment.failed -> shop.events

  Плюс:
  - rpc.pricing: синхронный расчёт цены через direct reply-to
  - ретраи: delayed retry в quorum-очередях, DLX shop.dlx -> [shop.dead] + алерт
  - MQTT: курьерские устройства публикуют статусы в couriers/+/status
  - мониторинг: длина очередей, unacked, consumers, алармы, DLQ
```

## 19.2 Требования

1. Заказ создаётся по HTTP, событие публикуется через outbox с confirms: **никакой двойной записи**.
2. Все важные очереди — quorum, с `delivery-limit`, DLX и лимитом длины (`reject-publish`), настроенными политиками.
3. Payment Service обрабатывает идемпотентно (`processed_messages`), делает ack после COMMIT.
4. Warehouse сохраняет порядок событий через single active consumer.
5. Временные ошибки уходят на повтор с растущей задержкой, исчерпавшие попытки — в DLQ с алертом.
6. Уведомления используют приоритеты: письма о платежах раньше маркетинговых.
7. Цена рассчитывается через RPC с direct reply-to и таймаутом.
8. Аналитика читает stream `shop.events.log` и умеет перечитать историю за неделю.
9. Статусы курьеров приходят по MQTT и маршрутизируются в очередь сервиса доставки.
10. Система переживает остановку любого узла без потери сообщений и без остановки публикации.

## 19.3 Этапы

| Этап | Что сделать |
|---|---|
| 1 | Кластер из 3 узлов, плагины, vhost `shop` с типом очередей по умолчанию quorum |
| 2 | Топология через definitions или Messaging Topology Operator: exchanges, очереди, bindings, политики |
| 3 | Order API + outbox + ретранслятор с confirms |
| 4 | Payment Service: идемпотентный консьюмер, prefetch, ack после COMMIT |
| 5 | Ретраи с задержкой и DLX, parking lot, алерт |
| 6 | Warehouse с single active consumer |
| 7 | Notifications с приоритетами |
| 8 | RPC `rpc.pricing` |
| 9 | Stream для аналитики, чтение с сохранённого offset |
| 10 | MQTT для курьеров, topic permissions |
| 11 | Мониторинг и алерты |
| 12 | Учения: остановить узел, убить консьюмер посреди обработки, заполнить очередь до лимита, отправить битое сообщение |

## 19.4 Как проверить, что всё работает

```bash
# нагрузка
docker run -it --rm --network host pivotalrabbitmq/perf-test:latest \
  --uri amqp://admin:admin@localhost:5672/shop --exchange shop.events --type topic \
  --routing-key order.created --producers 2 --consumers 0 --confirm 100 --time 120 --predeclared

# во время нагрузки
docker stop rabbit-2
docker exec rabbit-1 rabbitmqctl list_queues -p shop name messages_ready consumers   # очереди растут и уходят
docker exec rabbit-1 rabbitmq-queues quorum_status -p shop billing.orders            # новый лидер
docker start rabbit-2

# идемпотентность: остановить Payment Service посреди обработки и запустить снова
# в базе не должно появиться двойных списаний
```

Если после всех учений очереди вернулись к нулю, в DLQ только намеренно битые сообщения, в базе нет дублей, а число заказов в аналитике совпадает с базой — курс пройден.

---

# Шпаргалка RabbitMQ CLI и HTTP API

```bash
# узел и кластер
rabbitmq-diagnostics status
rabbitmq-diagnostics ping
rabbitmq-diagnostics check_running
rabbitmq-diagnostics check_local_alarms
rabbitmq-diagnostics check_port_connectivity
rabbitmq-diagnostics memory_breakdown
rabbitmq-diagnostics log_tail -N 100
rabbitmqctl cluster_status
rabbitmqctl list_feature_flags name state
rabbitmqctl enable_feature_flag all

# vhosts, пользователи, права
rabbitmqctl add_vhost shop --default-queue-type quorum
rabbitmqctl add_user app 'password'
rabbitmqctl set_user_tags app monitoring
rabbitmqctl set_permissions -p shop app '^app\..*' '^(shop\.events|app\..*)$' '^(shop\.events|app\..*)$'
rabbitmqctl set_topic_permissions -p shop app shop.events '^order\..*' '^$'
rabbitmqctl list_users
rabbitmqctl list_permissions -p shop

# объекты
rabbitmqctl list_exchanges -p shop name type
rabbitmqctl list_queues -p shop name type messages_ready messages_unacknowledged consumers
rabbitmqctl list_bindings -p shop
rabbitmqctl list_connections name user state channels
rabbitmqctl list_consumers -p shop
rabbitmqctl purge_queue -p shop payments
rabbitmqctl delete_queue -p shop payments

# политики и лимиты
rabbitmqctl set_policy -p shop limits '^(payments|stock)$' '{"max-length":500000,"overflow":"reject-publish"}' --apply-to queues
rabbitmqctl set_operator_policy -p shop cap '.*' '{"max-length":1000000}' --apply-to queues
rabbitmqctl list_policies -p shop
rabbitmqctl set_vhost_limits -p shop '{"max-connections":256,"max-queues":1024}'
rabbitmqctl set_user_limits app '{"max-connections":20,"max-channels":200}'

# quorum queues и streams
rabbitmq-queues quorum_status -p shop payments
rabbitmq-queues check_if_node_is_quorum_critical
rabbitmq-queues rebalance quorum
rabbitmq-queues grow rabbit@rabbit-4 all
rabbitmq-queues shrink rabbit@rabbit-3
rabbitmq-streams add_super_stream invoices --partitions 3

# обслуживание
rabbitmq-upgrade drain
rabbitmq-upgrade revive
rabbitmqctl forget_cluster_node rabbit@rabbit-3

# definitions
rabbitmqctl export_definitions /tmp/definitions.json
rabbitmqctl import_definitions /tmp/definitions.json

# HTTP API
curl -u admin:admin http://localhost:15672/api/overview
curl -u admin:admin http://localhost:15672/api/queues/shop
curl -u admin:admin -X PUT http://localhost:15672/api/queues/shop/payments \
  -H content-type:application/json -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}'
curl -u admin:admin -X POST http://localhost:15672/api/exchanges/shop/shop.events/publish \
  -H content-type:application/json -d '{"routing_key":"order.created","payload":"{}","payload_encoding":"string","properties":{"delivery_mode":2}}'
curl -u admin:admin http://localhost:15672/api/definitions > definitions.json
```

---

# Шпаргалка важных настроек

**Важная очередь:**

```
x-queue-type: quorum                      (аргумент)
delivery-limit: 5–10                      (политика)
dead-letter-exchange: <dlx>               (политика)
dead-letter-strategy: at-least-once       (политика, вместе с overflow=reject-publish)
max-length: по худшему сценарию накопления (политика)
overflow: reject-publish                  (политика)
```

**Очередь для ответов и временных данных:**

```
x-queue-type: classic
exclusive: true   (или durable + x-expires)
```

**Лог с повторным чтением:**

```
x-queue-type: stream
x-max-age: 7D
x-max-length-bytes: по бюджету диска
```

**Publisher:**

```
долгоживущее соединение только для публикации
publisher confirms, асинхронно с окном 100–1000
delivery_mode: 2
message_id: стабильный id события
mandatory: true (и обработка возвратов) или alternate exchange
таймаут на ожидание confirm
```

**Consumer:**

```
manual ack после обработки (после COMMIT в базе)
prefetch: 1–10 для долгих задач, 50–300 для быстрых
reject(requeue=false) для невалидных сообщений
переподключение и повторная подписка
идемпотентная обработка по message_id
```

**Узел (rabbitmq.conf):**

```
vm_memory_high_watermark.relative = 0.6
disk_free_limit.absolute = 4GB (или больше)
listeners.tcp = none + listeners.ssl.default = 5671
max_connections / max_channels по оценке нагрузки
cluster_formation.* (или оператор в Kubernetes)
```

---

# Вопросы на собеседовании по RabbitMQ с ответами

**Junior**

1. **Что такое RabbitMQ?** Брокер сообщений: принимает сообщения от producer'ов, маршрутизирует их через exchanges в очереди и доставляет консьюмерам с подтверждениями.
2. **Что такое exchange, queue и binding?** Exchange принимает публикации и маршрутизирует их, очередь хранит сообщения, binding — правило, связывающее exchange с очередью по ключу.
3. **Какие типы exchange бывают?** Direct (точное совпадение ключа), fanout (во все очереди), topic (по шаблону с `*` и `#`), headers (по заголовкам).
4. **Что такое default exchange?** Direct exchange с пустым именем, к которому каждая очередь привязана по своему имени.
5. **Чем connection отличается от channel?** Connection — TCP-соединение, дорогое; channel — лёгкий логический канал внутри соединения, через который идут операции.
6. **Что произойдёт с сообщением, если нет подходящей очереди?** Оно будет отброшено; с `mandatory=true` вернётся producer'у, с alternate exchange — уйдёт туда.
7. **Что такое ack?** Подтверждение консьюмера, что сообщение обработано; после него брокер удаляет сообщение из очереди.
8. **Чем durable-очередь отличается от persistent-сообщения?** Durable сохраняет определение очереди при перезапуске, persistent (`delivery_mode=2`) — само сообщение.
9. **Что такое vhost?** Изолированное пространство имён внутри брокера со своими объектами, правами и лимитами.
10. **Что такое competing consumers?** Несколько консьюмеров на одной очереди, между которыми распределяются сообщения.

**Middle**

11. **Чем auto ack отличается от manual ack?** Auto ack подтверждает сообщение при отправке (at-most-once), manual — после обработки (at-least-once).
12. **Что делают nack и reject с requeue=true и false?** `requeue=true` возвращает сообщение в очередь, `false` — отбрасывает или отправляет в DLX.
13. **Что такое prefetch и как его выбрать?** Лимит неподтверждённых сообщений на консьюмера: маленький для долгих задач, больше для быстрых; без лимита опасно.
14. **Что такое publisher confirms?** Режим канала, в котором брокер подтверждает (ack) или отклоняет (nack) каждую публикацию; сообщение считается отправленным только после ack.
15. **Когда приходит confirm для quorum-очереди?** Когда сообщение записано большинством реплик.
16. **Чем classic отличается от quorum и stream?** Classic не реплицируется, quorum реплицируется по Raft и удаляет сообщения после ack, stream — реплицируемый лог с повторным чтением.
17. **Что такое DLX и когда сообщение туда попадает?** Dead letter exchange: сюда уходят отклонённые без requeue, истёкшие по TTL, вытесненные при переполнении и исчерпавшие delivery-limit сообщения.
18. **Как сделать ретрай с задержкой?** Через retry-очередь с TTL на очередь и DLX обратно в рабочую очередь, или через delayed retry в quorum-очередях 4.3.
19. **Почему TTL отдельного сообщения опасен в classic-очередях?** Истечение проверяется только в голове очереди: сообщение с коротким TTL будет ждать сообщение с длинным TTL перед ним.
20. **Зачем политики, если есть аргументы очереди?** Аргументы нельзя поменять без пересоздания очереди, политики меняются на лету и применяются к группам очередей.
21. **Что такое alternate exchange?** Exchange, куда уходят сообщения, не подошедшие ни к одному binding'у.
22. **Как сохранить порядок при нескольких консьюмерах?** Single active consumer или шардирование по ключу на несколько очередей (consistent hash, super streams).
23. **Как сделать RPC через RabbitMQ?** Запрос с `reply_to` и `correlation_id`, ответ в reply-to; удобнее всего direct reply-to (`amq.rabbitmq.reply-to`) и обязательный таймаут.
24. **Что такое аларм памяти?** При превышении `vm_memory_high_watermark` брокер блокирует всех publisher'ов кластера, консьюмеры продолжают работать.
25. **Зачем отдельные соединения для публикации и потребления?** Flow control и алармы блокируют публикующие соединения; ack консьюмера в том же соединении тоже бы застрял.

**Senior**

26. **Как работает quorum-очередь?** Raft-группа из нескольких реплик с лидером: запись подтверждается большинством, при падении лидера выбирается новый из реплик, очередь доступна, пока жив кворум.
27. **Что изменил Khepri в 4.3?** Метаданные хранятся только в Khepri на Raft; кластеру нужно большинство узлов онлайн, стратегии обработки partition Mnesia удалены, восстановление единообразно по Raft.
28. **Почему кластер из двух узлов хуже одного?** Потеря любого узла лишает кворума и метаданные, и quorum-очереди.
29. **Как защититься от «ядовитых» сообщений?** Delivery-limit в quorum-очередях, валидация с reject без requeue, DLQ с алертом и инструментом переотправки.
30. **Как получить эффект exactly-once?** At-least-once (confirms, manual ack) плюс идемпотентная обработка по стабильному `message_id`; для публикации из транзакции — outbox.
31. **Когда выбрать streams вместо очередей?** Когда нужны повторное чтение, фан-аут на много читателей без копий сообщений или очень высокая пропускная способность.
32. **Как связать кластеры в разных регионах?** Не растягивать кластер, а использовать Federation (exchanges и очереди по политикам) или Shovel (перенос из источника в приёмник), с `ack-mode=on-confirm`.
33. **Как обновить кластер без простоя?** Включить все feature flags, по одному узлу: проверить quorum critical, drain, обновить, revive, дождаться реплик; после всех — включить новые feature flags и rebalance. Для больших скачков — blue-green.
34. **Почему длинные очереди — проблема?** Они занимают память и диск, медленно восстанавливаются, раздувают Raft-логи; RabbitMQ оптимален при коротких очередях.
35. **Какие метрики алертить в первую очередь?** Алармы памяти и диска, очереди без консьюмеров, растущие очереди и unacked, сообщения в DLQ, unroutable-сообщения, доступность узлов и кворум quorum-очередей.

---

# FAQ

**Теряет ли RabbitMQ сообщения?**
При quorum-очередях, publisher confirms, `mandatory` или alternate exchange, manual ack после обработки и лимитах с `reject-publish` — нет. Большинство «потерь» — это публикация без confirms, auto ack, отсутствие binding'а или `drop-head` при переполнении.

**Classic или quorum?**
Для всего важного — quorum. Classic — для временных, эксклюзивных очередей и ответов RPC.

**Можно ли поменять тип существующей очереди?**
Нет. Создаётся новая очередь, трафик переключается, старая дочитывается и удаляется (или переносится Shovel'ом).

**Почему моя очередь не создаётся с ошибкой PRECONDITION_FAILED?**
Либо очередь уже существует с другими аргументами, либо ты объявляешь non-durable неэксклюзивную очередь, а с 4.3 это запрещено по умолчанию.

**Как перечитать сообщения?**
Из очереди — никак: после ack они удалены. Если нужна история — streams.

**Сколько очередей выдержит кластер?**
Десятки тысяч очередей — нормально при достаточной памяти, но каждая quorum-очередь — отдельная Raft-группа. Не создавай очередь на каждый запрос или пользователя.

**RabbitMQ или Kafka?**
RabbitMQ — очереди задач, гибкая маршрутизация, подтверждение каждого сообщения, ретраи и приоритеты. Kafka — многолетний лог событий, CDC, аналитика и экосистема коннекторов. Streams в RabbitMQ закрывают часть задач Kafka.

**Нужен ли плагин delayed message exchange для отложенных сообщений?**
Для ретраев — нет: есть TTL + DLX и delayed retry в quorum-очередях 4.3. Плагин удобен для «отправить через N минут», но не реплицирует отложенные сообщения.

**Как отправить большой файл?**
Положить его в объектное хранилище и отправить в сообщении ссылку. Лимит сообщения по умолчанию 16 МБ, но даже мегабайты в каждом сообщении — плохая идея.

**Какие клиенты использовать?**
Python — pika (синхронный) или aio-pika (асинхронный); Go — `github.com/rabbitmq/amqp091-go`; Java — `com.rabbitmq:amqp-client` или Spring AMQP. Для AMQP 1.0 — новые официальные клиенты RabbitMQ; для streams — stream-клиенты.

**Что делать с Erlang cookie?**
Одинаковый длинный случайный секрет на всех узлах, хранится в секретах. Кто знает cookie и имеет сетевой доступ к порту распределения Erlang, тот управляет узлом.

---

# Глоссарий RabbitMQ

| Термин | Что значит |
|---|---|
| **AMQP 0-9-1** | Классический протокол RabbitMQ |
| **AMQP 1.0** | Стандартный протокол, базовый в RabbitMQ с 4.0 |
| **Connection** | TCP-соединение клиента с брокером |
| **Channel** | Логический канал внутри соединения |
| **Virtual host** | Изолированное пространство имён |
| **Exchange** | Точка публикации, маршрутизирующая сообщения |
| **Direct / Fanout / Topic / Headers** | Типы exchange |
| **Default exchange** | Direct exchange `""`, привязанный ко всем очередям по имени |
| **Binding** | Правило связи exchange с очередью или другим exchange |
| **Routing key** | Ключ маршрутизации сообщения |
| **Queue** | Буфер сообщений |
| **Classic queue** | Нереплицируемая очередь |
| **Quorum queue** | Реплицируемая очередь на Raft |
| **Stream** | Реплицируемый лог с повторным чтением |
| **Super stream** | Партиционированный stream |
| **Durable** | Определение очереди или exchange переживает перезапуск |
| **Persistent message** | Сообщение с `delivery_mode=2` |
| **Exclusive queue** | Очередь одного соединения |
| **Publisher confirms** | Подтверждения публикаций от брокера |
| **Mandatory** | Флаг: вернуть сообщение, если оно никуда не попало |
| **Alternate exchange** | Exchange для неразмеченных сообщений |
| **Ack / Nack / Reject** | Подтверждение / отказ с возможностью requeue |
| **Prefetch (QoS)** | Лимит неподтверждённых сообщений на консьюмера |
| **Delivery tag** | Номер доставки внутри канала |
| **Redelivered** | Флаг повторной доставки |
| **Delivery limit** | Лимит доставок в quorum-очереди |
| **DLX** | Dead letter exchange |
| **TTL** | Время жизни сообщения или очереди |
| **Single active consumer** | Только один активный консьюмер на очереди |
| **Consumer priority** | Приоритет консьюмера при раздаче сообщений |
| **Direct reply-to** | Псевдо-очередь для ответов RPC |
| **Policy / Operator policy** | Настройки по шаблону имени / ограничения администратора |
| **Memory / disk alarm** | Блокировка публикаций при нехватке памяти или диска |
| **Flow control** | Притормаживание публикующих соединений |
| **Khepri** | Хранилище метаданных на Raft |
| **Raft** | Протокол консенсуса quorum-очередей, streams и Khepri |
| **Feature flag** | Механизм включения новых возможностей в кластере |
| **Federation** | Асинхронная связь брокеров по exchanges и очередям |
| **Shovel** | Перенос сообщений из источника в приёмник |
| **Definitions** | Экспорт топологии, пользователей и политик в JSON |
| **Erlang cookie** | Общий секрет узлов кластера |

---

# Официальные источники и что читать дальше

- **Документация RabbitMQ** — https://www.rabbitmq.com/docs
- **Release notes** — https://github.com/rabbitmq/rabbitmq-server/releases
- **Исходный код** — https://github.com/rabbitmq/rabbitmq-server
- **Официальный Docker-образ** — https://hub.docker.com/_/rabbitmq
- **Туториалы для всех языков** — https://www.rabbitmq.com/tutorials
- **Cluster Operator для Kubernetes** — https://www.rabbitmq.com/kubernetes/operator/operator-overview
- **PerfTest** — https://perftest.rabbitmq.com
- **pika** — https://github.com/pika/pika
- **amqp091-go** — https://github.com/rabbitmq/amqp091-go
- **Java-клиент** — https://github.com/rabbitmq/rabbitmq-java-client
- **Блог RabbitMQ** — https://www.rabbitmq.com/blog
- **Книги:** «RabbitMQ in Depth» (Gavin M. Roy), «Enterprise Integration Patterns» (Gregor Hohpe, Bobby Woolf) — классика паттернов обмена сообщениями

---

## Как помочь курсу

Нашёл ошибку, неточность или устаревшую настройку? Открой issue или пришли pull request. Особенно полезны:

- примеры на языках, которых здесь нет (C#, Node.js, Rust, Kotlin);
- реальные production-истории и разборы инцидентов;
- уточнения под новые версии RabbitMQ.

⭐ Если курс помог, поставь звезду: так его найдут другие разработчики.

**Лицензия:** текст курса распространяется по лицензии [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.ru), примеры кода — по [MIT License](LICENSE). Можно свободно использовать, адаптировать и распространять материалы, в том числе для внутренних воркшопов, с указанием источника.
