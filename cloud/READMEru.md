# Облачные брокеры сообщений 2026: бесплатный курс по AWS, Azure и Google Cloud с нуля до профи на русском

![AWS](https://img.shields.io/badge/AWS-SQS%20%7C%20SNS%20%7C%20EventBridge-FF9900?logo=amazonwebservices&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Service%20Bus%20%7C%20Event%20Grid%20%7C%20Event%20Hubs-0078D4?logo=microsoftazure&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-Pub%2FSub%20%7C%20Eventarc-4285F4?logo=googlecloud&logoColor=white)
![Курс на русском](https://img.shields.io/badge/язык-русский-red)
![Бесплатный курс](https://img.shields.io/badge/цена-бесплатно-brightgreen)

> **Полный бесплатный курс по облачным брокерам сообщений на русском языке.** AWS SQS, SNS, EventBridge и Kinesis, Azure Service Bus, Event Grid и Event Hubs, Google Cloud Pub/Sub, Eventarc и Cloud Tasks. Одни и те же задачи — очередь задач, fan-out, порядок, дедупликация, DLQ, отложенные сообщения, exactly-once — разобраны в трёх облаках рядом, с таблицами соответствия. Локальная разработка на эмуляторах, serverless-интеграции, безопасность, мониторинг, стоимость, Terraform и выбор между облачным сервисом и своим Kafka, RabbitMQ или NATS. Актуально на **2026 год**.

**Облака без воды:** каждый модуль — это понятная теория, схемы, команды, которые можно запустить у себя (в том числе без облачного аккаунта, на эмуляторах), типичные ошибки и вопросы для самопроверки.

Этот курс — часть серии. Если нужно глубже разобраться в самих моделях обмена сообщениями, смотри парные курсы: **NATS**, **Apache Kafka** и **RabbitMQ**. Здесь на них будут ссылки там, где облачный сервис повторяет их идеи.

⭐ Если курс полезен, поставь звезду репозиторию: так его найдут другие разработчики.

🇬🇧 English version: [README.md](README.md)

---

## Для кого этот курс

| Кто ты | Что получишь |
|---|---|
| **Backend-разработчик** в облаке | Как правильно отправлять и получать сообщения в SQS, Service Bus и Pub/Sub, не теряя их и не обрабатывая дважды |
| **Serverless-разработчик** | Lambda, Azure Functions и Cloud Run как консьюмеры: батчи, частичные ошибки, ретраи |
| **Архитектор** | Карта сервисов трёх облаков, выбор между очередью, pub/sub, шиной событий и логом, мультиоблако |
| **DevOps / SRE / Platform** | IAM, шифрование, мониторинг, алерты, квоты, Terraform, стоимость |
| **Переходишь с RabbitMQ или Kafka** | Что чему соответствует в облаке и где облачный сервис ведёт себя иначе |
| **Готовишься к собеседованию** | 35 вопросов с ответами уровня junior, middle и senior |

## Что ты будешь уметь после курса

- разбираться в сервисах обмена сообщениями AWS, Azure и Google Cloud и выбирать нужный под задачу;
- работать с очередями SQS: standard и FIFO, visibility timeout, long polling, DLQ и redrive;
- строить fan-out через SNS + SQS и фильтровать сообщения;
- маршрутизировать события через EventBridge, Event Grid и Eventarc;
- использовать Azure Service Bus: peek-lock, сессии, дедупликацию, отложенные сообщения, топики и подписки;
- использовать Google Pub/Sub: pull и push, ack deadline, ordering keys, exactly-once, фильтры и seek;
- понимать, когда нужен лог (Kinesis, Event Hubs, Managed Kafka) вместо очереди;
- проектировать идемпотентных консьюмеров и outbox в облаке;
- подключать serverless-функции к очередям без потерь при частичных ошибках;
- настраивать IAM, шифрование ключами KMS и приватный доступ;
- мониторить возраст самого старого сообщения, DLQ и отставание подписчиков;
- считать стоимость и не попадать на неожиданные счета;
- описывать всю топологию в Terraform;
- разрабатывать и тестировать локально на эмуляторах, без облачного аккаунта.

---

## Оглавление

- [Для кого этот курс](#для-кого-этот-курс)
- [Что ты будешь уметь после курса](#что-ты-будешь-уметь-после-курса)
- [Как проходить курс](#как-проходить-курс)
- [Модуль 0. Зачем облачные брокеры и чем они отличаются от своих](#модуль-0-зачем-облачные-брокеры-и-чем-они-отличаются-от-своих)
- [Модуль 1. Карта сервисов: очереди, pub/sub, шины событий и логи](#модуль-1-карта-сервисов-очереди-pubsub-шины-событий-и-логи)
- [Модуль 2. Локальная среда: эмуляторы и первые команды](#модуль-2-локальная-среда-эмуляторы-и-первые-команды)
- [Модуль 3. AWS SQS: очереди standard и FIFO](#модуль-3-aws-sqs-очереди-standard-и-fifo)
- [Модуль 4. AWS SNS: fan-out, фильтрация и SNS + SQS](#модуль-4-aws-sns-fan-out-фильтрация-и-sns--sqs)
- [Модуль 5. AWS EventBridge и Kinesis](#модуль-5-aws-eventbridge-и-kinesis)
- [Модуль 6. Azure Service Bus: очереди, топики, сессии](#модуль-6-azure-service-bus-очереди-топики-сессии)
- [Модуль 7. Azure Event Grid и Event Hubs](#модуль-7-azure-event-grid-и-event-hubs)
- [Модуль 8. Google Cloud Pub/Sub](#модуль-8-google-cloud-pubsub)
- [Модуль 9. Google Eventarc, Cloud Tasks и Managed Kafka](#модуль-9-google-eventarc-cloud-tasks-и-managed-kafka)
- [Модуль 10. Паттерны в трёх облаках: таблицы соответствия](#модуль-10-паттерны-в-трёх-облаках-таблицы-соответствия)
- [Модуль 11. Гарантии доставки, идемпотентность и outbox](#модуль-11-гарантии-доставки-идемпотентность-и-outbox)
- [Модуль 12. Serverless-консьюмеры: Lambda, Azure Functions, Cloud Run](#модуль-12-serverless-консьюмеры-lambda-azure-functions-cloud-run)
- [Модуль 13. Безопасность: IAM, шифрование, приватный доступ](#модуль-13-безопасность-iam-шифрование-приватный-доступ)
- [Модуль 14. Мониторинг и алерты](#модуль-14-мониторинг-и-алерты)
- [Модуль 15. Стоимость, квоты и лимиты](#модуль-15-стоимость-квоты-и-лимиты)
- [Модуль 16. Инфраструктура как код: Terraform](#модуль-16-инфраструктура-как-код-terraform)
- [Модуль 17. Переносимость, CloudEvents и миграции](#модуль-17-переносимость-cloudevents-и-миграции)
- [Модуль 18. Облачный сервис или свой Kafka, RabbitMQ, NATS](#модуль-18-облачный-сервис-или-свой-kafka-rabbitmq-nats)
- [Модуль 19. Итоговый проект: интернет-магазин в трёх облаках](#модуль-19-итоговый-проект-интернет-магазин-в-трёх-облаках)
- [Шпаргалка CLI: aws, az, gcloud](#шпаргалка-cli-aws-az-gcloud)
- [Шпаргалка важных настроек](#шпаргалка-важных-настроек)
- [Вопросы на собеседовании с ответами](#вопросы-на-собеседовании-с-ответами)
- [FAQ](#faq)
- [Глоссарий](#глоссарий)
- [Официальные источники и что читать дальше](#официальные-источники-и-что-читать-дальше)

---

## Как проходить курс

1. **Сначала модули 0–2**: общая карта и локальная среда. Дальше можно идти по облаку, которое нужно тебе (AWS — модули 3–5, Azure — 6–7, Google Cloud — 8–9), а потом вернуться к общим модулям 10–18.
2. **Запускай команды на эмуляторах.** Для большинства упражнений облачный аккаунт не нужен.
3. **Сравнивай.** Модуль 10 собирает одни и те же паттерны в трёх облаках: это лучший способ понять, где сервисы похожи, а где — нет.
4. **Отвечай на вопросы в конце модуля** вслух, как на собеседовании.
5. **Сделай итоговый проект** хотя бы в одном облаке.

**Что нужно установить:** Docker, Python 3.10+, Git. CLI облаков: `aws` (AWS CLI v2), `az` (Azure CLI), `gcloud` (Google Cloud CLI) — ставь те, что нужны. Примеры кода — на Python (boto3, azure-servicebus, google-cloud-pubsub) и Go (aws-sdk-go-v2, azservicebus, cloud.google.com/go/pubsub/v2).

**Запускаемые примеры:** docker-compose с эмуляторами трёх облаков, консьюмеры и тесты утверждений курса на Python и Go лежат в [`examples/`](examples/). CI каждую неделю прогоняет их, так что изменения в эмуляторах и SDK видны сразу.

**Актуальность.** Облачные сервисы меняются постоянно и без номеров версий: лимиты растут, появляются новые возможности. В курсе указаны значения на 2026 год; перед проектированием сверяй лимиты с разделом *Quotas* документации сервиса. Например, SQS в августе 2025 года и EventBridge в январе 2026 года подняли максимальный размер сообщения с 256 КиБ до 1 МиБ, SNS в сентябре 2026 года разрешил до 1 МиБ через отдельный атрибут топика, а Google Pub/Sub Lite был отключён 18 марта 2026 года — старые статьи об этом не знают.

---

# Модуль 0. Зачем облачные брокеры и чем они отличаются от своих

## 0.1 Облачный брокер простыми словами

**Облачный брокер сообщений** — сервис, который делает то же, что RabbitMQ или Kafka: принимает сообщения, хранит их и отдаёт получателям. Разница в том, что серверов **не видно**: ты создаёшь очередь или топик через API, консоль или Terraform, а масштабирование, репликацию, обновления и восстановление после сбоев берёт на себя облако.

```
СВОЙ БРОКЕР (Kafka, RabbitMQ, NATS)        ОБЛАЧНЫЙ БРОКЕР (SQS, Service Bus, Pub/Sub)
-----------------------------------        -------------------------------------------
ты выбираешь серверы и диски                  серверов нет, есть API и квоты
ты настраиваешь кластер и репликацию          репликация между зонами включена
ты обновляешь версии                          обновления прозрачны
ты мониторишь узлы                            мониторишь очереди, узлов не видно
платишь за серверы 24/7                       платишь за запросы и объём
переносится между облаками                    привязан к облаку
любые возможности open-source                 только то, что даёт сервис
```

## 0.2 Модель ответственности

| Задача | Свой брокер | Облачный брокер |
|---|---|---|
| Серверы, диски, сеть | Ты | Облако |
| Репликация и отказоустойчивость | Ты | Облако (в пределах региона) |
| Обновления и патчи безопасности | Ты | Облако |
| Масштабирование | Ты | Облако (в пределах квот) |
| Топология: очереди, топики, подписки | Ты | Ты |
| Права доступа | Ты | Ты (через IAM) |
| Идемпотентность, ретраи, DLQ | Ты | Ты |
| Мониторинг очередей и алерты | Ты | Ты (метрики дает облако) |
| Стоимость | Серверы | Запросы, объём, трафик |

Главное: **облако снимает с тебя эксплуатацию, но не проектирование**. Потерять сообщение или обработать его дважды в SQS так же легко, как в RabbitMQ, если не понимать модель доставки.

## 0.3 Когда облачный брокер — хороший выбор

- Система уже живёт в одном облаке, и нет цели быть переносимой.
- Нет команды, которая хочет эксплуатировать кластеры брокеров.
- Нагрузка неравномерная: платить за запросы дешевле, чем держать кластер под пик.
- Нужна тесная интеграция с другими сервисами облака: функции, хранилище, аналитика, события инфраструктуры.
- Нужны сертифицированные средства безопасности и аудита «из коробки».

## 0.4 Когда облачный брокер — плохой выбор

- Нужна переносимость между облаками или работа on-premises.
- Нужны возможности, которых у сервиса нет: сложная маршрутизация, приоритеты, многолетний лог, request-reply с низкой задержкой.
- Очень большой постоянный поток сообщений: при оплате за запросы свой кластер может оказаться в разы дешевле (модуль 15).
- Нужна минимальная задержка (микросекунды и единицы миллисекунд): облачные API работают по HTTPS и добавляют задержку.

## 0.5 Две группы облачных сервисов

| Группа | Примеры | Особенность |
|---|---|---|
| **Собственные сервисы облака** | SQS, SNS, EventBridge, Kinesis; Service Bus, Event Grid, Event Hubs; Pub/Sub, Eventarc, Cloud Tasks | Свой API, есть только в этом облаке |
| **Управляемые open-source брокеры** | Amazon MSK (Kafka), Amazon MQ (RabbitMQ, ActiveMQ); Google Managed Service for Apache Kafka; Event Hubs с протоколом Kafka | Обычные клиенты Kafka и RabbitMQ, переносимость сохраняется |

Управляемые Kafka и RabbitMQ подробно разобраны в парных курсах серии: всё, что там сказано о клиентах, гарантиях и проектировании, к ним применимо. Этот курс — про **собственные** сервисы облаков.

## 0.6 Общая модель: HTTP API, pull и push

Почти все собственные облачные брокеры работают через **HTTPS API**, а не через постоянное TCP-соединение со своим бинарным протоколом (исключение — Azure Service Bus и Event Hubs, которые говорят по AMQP 1.0).

```
PULL (SQS, Pub/Sub pull, Service Bus)       PUSH (SNS, EventBridge, Event Grid, Pub/Sub push)
-----------------------------------         ------------------------------------------------
консьюмер сам спрашивает: «есть что?»        сервис сам вызывает твой обработчик
long polling: ждать ответа до N секунд       HTTP-эндпоинт, функция, другая очередь
консьюмер контролирует темп                  сервис контролирует темп и ретраи
удобно для воркеров                          удобно для serverless и вебхуков
```

## 0.7 Чего облачный брокер НЕ сделает за тебя

- идемпотентную обработку: почти все сервисы — at-least-once;
- правильный visibility timeout / ack deadline под время обработки;
- DLQ и алерт на неё — их нужно создать и подключить;
- схемы сообщений и их версионирование;
- защиту от неожиданного счёта: бесконечные ретраи тоже стоят денег;
- переносимость: код под конкретный API привязан к облаку.

### Вопросы для самопроверки

1. Что облако берёт на себя, а что остаётся на тебе при использовании облачного брокера?
2. Чем собственные облачные сервисы отличаются от управляемых Kafka и RabbitMQ?
3. Чем pull-модель отличается от push-модели?
4. Когда облачный брокер оказывается дороже своего кластера?

---

# Модуль 1. Карта сервисов: очереди, pub/sub, шины событий и логи

## 1.1 Четыре класса сервисов

| Класс | Что делает | Модель доставки | Аналог из open-source |
|---|---|---|---|
| **Очередь** | Раздаёт задачи воркерам, каждое сообщение обрабатывается один раз | Pull, подтверждение каждого сообщения | Очередь RabbitMQ |
| **Pub/sub** | Копирует сообщение всем подписчикам | Push или pull | Fanout/topic exchange RabbitMQ, subjects NATS |
| **Шина событий** | Маршрутизирует события по правилам на содержимое, интегрирует сервисы и SaaS | Push на цели | Topic exchange с фильтрами |
| **Лог (стриминг)** | Хранит поток, даёт повторное чтение, партиции | Pull с offset | Kafka, RabbitMQ streams, NATS JetStream |

## 1.2 Таблица соответствия трёх облаков

| Задача | AWS | Azure | Google Cloud |
|---|---|---|---|
| Очередь задач | **SQS** (standard, FIFO) | **Service Bus** queues; Storage Queues (простая) | **Pub/Sub** pull-подписка; **Cloud Tasks** |
| Pub/sub, fan-out | **SNS** (+ SQS) | **Service Bus** topics и subscriptions | **Pub/Sub** топик + несколько подписок |
| Шина событий, маршрутизация по содержимому | **EventBridge** | **Event Grid** | **Eventarc** |
| Лог, стриминг | **Kinesis Data Streams**, **MSK** | **Event Hubs** (в т.ч. протокол Kafka) | **Managed Service for Apache Kafka**; Pub/Sub с seek |
| Управляемый RabbitMQ | **Amazon MQ** | — (маркетплейс) | — (маркетплейс) |
| Отложенные задачи и расписание | SQS delay (до 15 мин), **EventBridge Scheduler** | Service Bus scheduled messages | **Cloud Tasks**, Cloud Scheduler |
| Порядок сообщений | SQS FIFO (message group), SNS FIFO | Service Bus sessions | Pub/Sub ordering keys |
| Дедупликация на стороне сервиса | SQS FIFO (5 минут) | Service Bus duplicate detection | Pub/Sub exactly-once (на подписке) |

**Главный вывод из таблицы:** в Google Cloud один сервис Pub/Sub закрывает и очередь, и pub/sub; в AWS для этого два сервиса (SQS и SNS); в Azure Service Bus умеет и очереди, и топики.

## 1.3 Основные лимиты (ориентиры на 2026 год)

| | SQS | SNS | EventBridge | Service Bus | Pub/Sub |
|---|---|---|---|---|---|
| Макс. размер сообщения | 1 МиБ (с августа 2025) | 256 КиБ; до 1 МиБ через атрибут `MaximumMessageSize` (с сентября 2026, для подписок SQS, Lambda, Firehose) | 1 МБ (с января 2026) | 256 КиБ (Standard), до 100 МБ (Premium) | 10 МБ |
| Хранение | 1 мин – 14 дней (по умолчанию 4 дня) | Не хранит, доставляет | Не хранит (кроме архива и replay) | По TTL сообщения; на тарифе Basic — не больше 14 дней | До 7 дней на подписке (retention топика — до 31 дня) |
| Время на обработку | Visibility timeout до 12 часов | — | — | Lock duration до 5 минут (продлевается) | Ack deadline 10 с – 10 мин (продлевается) |
| Порядок | FIFO-очереди | FIFO-топики | Нет | Sessions | Ordering keys |

Эти значения меняются: перед проектированием открой страницу *Quotas* нужного сервиса.

## 1.4 Как выбирать

```
нужно раздать задачи воркерам?
   ├── да -> ОЧЕРЕДЬ: SQS / Service Bus queue / Pub/Sub pull
   └── нет
        нужно доставить одно событие многим независимым получателям?
           ├── да -> маршрутизация по содержимому и интеграции с SaaS важнее?
           │         ├── да -> ШИНА СОБЫТИЙ: EventBridge / Event Grid / Eventarc
           │         └── нет -> PUB/SUB: SNS+SQS / Service Bus topic / Pub/Sub
           └── нет
                нужна история, повторное чтение, большие потоки, партиции?
                   └── да -> ЛОГ: Kinesis или MSK / Event Hubs / Managed Kafka
```

### Вопросы для самопроверки

1. Какие четыре класса сервисов есть у облачных брокеров, и чем шина событий отличается от pub/sub?
2. Какими сервисами решается fan-out в каждом из трёх облаков?
3. Почему в AWS для очереди с fan-out нужны два сервиса, а в Google Cloud — один?
4. Какой сервис выбрать для многочасовой истории событий с повторным чтением?

---

# Модуль 2. Локальная среда: эмуляторы и первые команды

## 2.1 Зачем эмуляторы

Облачный аккаунт для обучения — это риск случайного счёта и необходимость настраивать доступ. Для большинства упражнений курса хватит **эмуляторов**, которые запускаются в Docker и говорят по тем же API:

| Облако | Эмулятор | Что умеет |
|---|---|---|
| AWS | **moto** в режиме сервера (open source) или **LocalStack** (с марта 2026 года нужен auth token, есть бесплатный план) | SQS, SNS, EventBridge, Kinesis и многое другое |
| Azure | **Azure Service Bus emulator** (официальный) | Очереди, топики, подписки, сессии, DLQ |
| Google Cloud | **Pub/Sub emulator** (официальный, из Cloud SDK) | Топики, подписки, pull и push |

Ограничения эмуляторов: нет IAM и квот, часть функций не реализована (например, exactly-once в эмуляторе Pub/Sub), а поведение под нагрузкой не соответствует облаку. Для обучения и интеграционных тестов этого достаточно; поведение, от которого зависит продакшен, проверяй в облаке.

## 2.2 Docker Compose со всеми эмуляторами

`docker-compose.yml`:

```yaml
services:
  # AWS: SQS, SNS, EventBridge через moto (open source, без токена)
  aws:
    image: motoserver/moto:latest
    ports: ["4566:5000"]

  # Google Cloud Pub/Sub
  pubsub:
    image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
    command: gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=course-project
    ports: ["8085:8085"]

  # Azure Service Bus emulator; сущности создаются из servicebus-config.json
  servicebus:
    image: mcr.microsoft.com/azure-messaging/servicebus-emulator:latest
    depends_on: [sql]
    ports: ["5672:5672", "5300:5300"]
    environment:
      ACCEPT_EULA: "Y"
      SQL_SERVER: sql
      MSSQL_SA_PASSWORD: "Course_Passw0rd!"
    volumes:
      - ./servicebus-config.json:/ServiceBus_Emulator/ConfigFiles/Config.json:ro

  # база метаданных для эмулятора Service Bus (Azure SQL Edge выведена из эксплуатации)
  sql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "Course_Passw0rd!"
```

Эмулятор Service Bus создаёт очереди и топики из конфигурационного файла при старте; готовый `servicebus-config.json` лежит в `examples/emulators/`. Метаданные эмулятор хранит в SQL Server: раньше в примерах использовали Azure SQL Edge, но её вывели из эксплуатации, поэтому здесь образ `mssql/server`. Образы и переменные окружения эмуляторов меняются — сверяйся с их документацией, если что-то не стартует.

LocalStack долго был стандартным эмулятором AWS, но с марта 2026 года его образ требует auth token (бесплатный план сохранился). Поэтому в курсе используется open-source **moto** в режиме сервера: те же API SQS, SNS и EventBridge на порту 4566, без регистрации. Команды ниже работают и с LocalStack.

```bash
docker compose up -d
```

## 2.3 AWS CLI с эмулятором

```bash
# фиктивные учётные данные: эмулятор их не проверяет
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=eu-central-1
alias awsl='aws --endpoint-url=http://localhost:4566'

awsl sqs create-queue --queue-name tasks
awsl sqs list-queues
awsl sqs send-message --queue-url http://localhost:4566/123456789012/tasks --message-body '{"task":"resize"}'
awsl sqs receive-message --queue-url http://localhost:4566/123456789012/tasks --wait-time-seconds 5
```

`123456789012` — идентификатор аккаунта, который использует moto (у LocalStack — `000000000000`). В облаке URL очереди выглядит как `https://sqs.eu-central-1.amazonaws.com/<account-id>/tasks`.

## 2.4 Pub/Sub emulator

Клиентские библиотеки Google сами переключаются на эмулятор, если задана переменная окружения:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PUBSUB_PROJECT_ID=course-project
```

```python
from google.cloud import pubsub_v1

project = "course-project"
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic = publisher.topic_path(project, "orders")
sub = subscriber.subscription_path(project, "billing")

publisher.create_topic(name=topic)
subscriber.create_subscription(name=sub, topic=topic)
publisher.publish(topic, b'{"order_id":"order-1"}').result()

resp = subscriber.pull(subscription=sub, max_messages=10, timeout=5)
for m in resp.received_messages:
    print(m.message.data)
subscriber.acknowledge(subscription=sub, ack_ids=[m.ack_id for m in resp.received_messages])
```

`gcloud pubsub` с эмулятором не работает: управляй топиками и подписками через клиентские библиотеки или REST API эмулятора.

## 2.5 Service Bus emulator

Эмулятор Service Bus принимает строку подключения со специальным флагом:

```
Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
```

```python
from azure.servicebus import ServiceBusClient, ServiceBusMessage

CONN = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"

with ServiceBusClient.from_connection_string(CONN) as client:
    with client.get_queue_sender("tasks") as sender:
        sender.send_messages(ServiceBusMessage('{"task":"resize"}'))
    with client.get_queue_receiver("tasks", max_wait_time=5) as receiver:
        for msg in receiver:
            print(str(msg))
            receiver.complete_message(msg)
```

В облаке вместо строки подключения лучше использовать учётные данные Entra ID (`DefaultAzureCredential`, модуль 13).

## 2.6 Когда нужен настоящий облачный аккаунт

- IAM-политики и приватные эндпоинты;
- serverless-интеграции (Lambda, Azure Functions, Cloud Run) в реальном окружении;
- поведение под нагрузкой, квоты и стоимость;
- функции, которых нет в эмуляторах.

Для таких упражнений заведи отдельный учебный аккаунт или проект с **бюджетом и алертом на расходы** (модуль 15) и удаляй ресурсы после занятия (`terraform destroy`, модуль 16).

### Практика

1. Подними эмуляторы через Docker Compose.
2. Создай очередь SQS в эмуляторе, отправь и получи сообщение через AWS CLI.
3. Создай топик и подписку в эмуляторе Pub/Sub, опубликуй и получи сообщение из Python.
4. Отправь и получи сообщение через эмулятор Service Bus.

---

# Модуль 3. AWS SQS: очереди standard и FIFO

## 3.1 Как работает SQS

**Amazon SQS** — полностью управляемая очередь: создаёшь очередь, отправляешь сообщения, воркеры забирают их, обрабатывают и удаляют.

```
producer --SendMessage--> [ очередь SQS ] <--ReceiveMessage-- воркер
                                |                                |
                                |   сообщение становится          | обработал
                                |   НЕВИДИМЫМ на visibility       v
                                |   timeout (30 с по умолчанию)  DeleteMessage(receipt handle)
                                |
                                +-- не удалили вовремя -> сообщение снова видно -> повторная доставка
```

Ключевая идея SQS — **visibility timeout** вместо ack. Полученное сообщение не удаляется, а становится невидимым для других консьюмеров. Если воркер успел обработать и вызвать `DeleteMessage` — сообщение удалено. Если нет (упал, завис, не успел) — по истечении timeout сообщение снова появляется в очереди.

Это то же самое, что `AckWait` в NATS JetStream или consumer timeout в RabbitMQ, только это единственный механизм подтверждения.

## 3.2 Standard и FIFO

| | **Standard** | **FIFO** |
|---|---|---|
| Доставка | At-least-once: возможны **дубли** | Exactly-once processing в окне дедупликации 5 минут |
| Порядок | Лучшее усилие, **не гарантирован** | Строгий внутри `MessageGroupId` |
| Пропускная способность | Практически не ограничена | 300 операций/с на API без батчей, 3000 с батчами; в режиме high throughput — на порядки больше (зависит от региона) |
| Имя | Любое | Обязательно оканчивается на `.fifo` |
| Задержка отдельного сообщения | Да (`DelaySeconds` до 15 минут) | Только на уровне очереди |

**Standard** — выбор по умолчанию для очередей задач: быстро, дёшево, масштабируется без усилий. Консьюмер обязан быть идемпотентным: одно сообщение может прийти дважды, а порядок может нарушиться.

**FIFO** — когда нужен порядок по сущности или дедупликация на стороне сервиса:

- `MessageGroupId` — аналог ключа партиции: сообщения одной группы доставляются строго по порядку, и пока одно сообщение группы в обработке, следующее из этой группы никому не выдаётся. Разные группы обрабатываются параллельно;
- `MessageDeduplicationId` — повторная отправка с тем же id в течение 5 минут не создаёт дубль. Можно включить `ContentBasedDeduplication`, тогда id считается как хеш тела.

## 3.3 Параметры очереди

| Атрибут | По умолчанию | Диапазон | Смысл |
|---|---|---|---|
| `VisibilityTimeout` | 30 с | 0 с – 12 ч | Сколько сообщение невидимо после получения |
| `MessageRetentionPeriod` | 4 дня | 1 мин – 14 дней | Сколько хранится неудалённое сообщение |
| `ReceiveMessageWaitTimeSeconds` | 0 | 0 – 20 с | Long polling по умолчанию для очереди |
| `DelaySeconds` | 0 | 0 – 15 мин | Задержка перед тем, как сообщение станет видимым |
| `MaximumMessageSize` | 1 МиБ | 1 Б – 1 МиБ | Максимальный размер |
| `RedrivePolicy` | нет | — | DLQ и `maxReceiveCount` |
| `SqsManagedSseEnabled` | включено | — | Шифрование на стороне сервера |

## 3.4 Long polling

`ReceiveMessage` без ожидания (short polling) опрашивает **только часть серверов** SQS и может вернуть пустой ответ, даже когда сообщения есть. Кроме того, каждый пустой запрос стоит денег.

**Всегда используй long polling**: `WaitTimeSeconds=20` в запросе или `ReceiveMessageWaitTimeSeconds=20` на очереди. Запрос ждёт до 20 секунд, пока появится хотя бы одно сообщение, опрашивая все серверы.

## 3.5 Цикл консьюмера на Python (boto3)

```python
import json
import boto3

# для эмулятора: endpoint_url="http://localhost:4566"
sqs = boto3.client("sqs", region_name="eu-central-1")
queue_url = sqs.get_queue_url(QueueName="tasks")["QueueUrl"]

while True:
    resp = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,          # до 10 сообщений за запрос
        WaitTimeSeconds=20,              # long polling
        VisibilityTimeout=60,            # больше, чем обработка пачки
        MessageSystemAttributeNames=["ApproximateReceiveCount"],
    )
    for msg in resp.get("Messages", []):
        attempts = int(msg["Attributes"]["ApproximateReceiveCount"])
        try:
            process(json.loads(msg["Body"]))                 # должно быть идемпотентным
        except Exception as e:
            print(f"ошибка (попытка {attempts}): {e}")       # не удаляем: вернётся после timeout
            continue
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
```

Правила:

- удаляй сообщение **только после** успешной обработки;
- `ReceiptHandle` действителен только для **этого** получения: при повторной доставке он другой;
- для пачек используй `delete_message_batch` (до 10 сообщений за вызов);
- `ApproximateReceiveCount` — сколько раз сообщение уже получали: полезно для логов и решения «хватит ретраить».

## 3.6 Отправка

```python
# одно сообщение
sqs.send_message(
    QueueUrl=queue_url,
    MessageBody=json.dumps({"task": "resize", "image": "1.png"}),
    MessageAttributes={"event-type": {"DataType": "String", "StringValue": "ResizeRequested"}},
)

# пачка до 10 сообщений: дешевле и быстрее
entries = [{"Id": str(i), "MessageBody": json.dumps({"n": i})} for i in range(10)]
resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
if resp.get("Failed"):
    print("не отправлены:", resp["Failed"])    # батч может частично не пройти: проверяй
```

**Частичные ошибки батча** — частая ловушка: `send_message_batch` возвращает успех HTTP-запроса, даже если часть сообщений не принята. Всегда проверяй поле `Failed` и повторяй неудавшиеся.

FIFO-очередь:

```python
sqs.send_message(
    QueueUrl=fifo_url,                          # имя оканчивается на .fifo
    MessageBody=json.dumps({"order_id": "order-1", "event": "OrderPaid"}),
    MessageGroupId="order-1",                   # порядок внутри заказа
    MessageDeduplicationId="order-1-paid",      # повтор в течение 5 минут не создаст дубль
)
```

## 3.7 Consumer на Go (aws-sdk-go-v2)

```go
package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		// o.BaseEndpoint = aws.String("http://localhost:4566") // для эмулятора
	})
	q, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("tasks")})
	if err != nil {
		log.Fatal(err)
	}

	for {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            q.QueueUrl,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
			VisibilityTimeout:   60,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			log.Println("receive:", err)
			continue
		}
		for _, m := range out.Messages {
			if err := process(aws.ToString(m.Body)); err != nil {
				log.Printf("ошибка, попытка %s: %v", m.Attributes["ApproximateReceiveCount"], err)
				continue // вернётся после visibility timeout
			}
			if _, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl: q.QueueUrl, ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				log.Println("delete:", err)
			}
		}
	}
}

func process(body string) error { log.Println(body); return nil }
```

## 3.8 Долгая обработка: продление видимости

Если обработка может занять больше visibility timeout, продлевай его, пока работаешь (аналог `InProgress()` в NATS или `RENEW` в Kafka share groups):

```python
sqs.change_message_visibility(
    QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"], VisibilityTimeout=120)
```

Типичный приём — фоновый «heartbeat», который продлевает видимость каждые N секунд, пока идёт обработка. Иначе долгое сообщение будет выдано второму воркеру, и его обработают дважды параллельно.

Правило для timeout: **больше p99 времени обработки пачки** с запасом, но не намного больше, иначе упавшее сообщение будет долго ждать повторной попытки.

## 3.9 Dead letter queue и redrive

DLQ — обычная очередь того же типа (standard для standard, FIFO для FIFO), куда SQS переносит сообщение после `maxReceiveCount` неудачных получений:

```bash
awsl sqs create-queue --queue-name tasks-dlq --attributes MessageRetentionPeriod=1209600
DLQ_ARN=$(awsl sqs get-queue-attributes \
  --queue-url http://localhost:4566/123456789012/tasks-dlq \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

awsl sqs set-queue-attributes \
  --queue-url http://localhost:4566/123456789012/tasks \
  --attributes '{"RedrivePolicy":"{\"deadLetterTargetArn\":\"'"$DLQ_ARN"'\",\"maxReceiveCount\":\"5\"}"}'
```

Что важно знать:

- `maxReceiveCount` считает **получения**, а не явные ошибки: сообщение, которое воркер получил и не удалил (упал, истёк timeout), тоже считается попыткой;
- у standard-очередей срок хранения считается от **исходного** времени постановки в очередь: сообщение, которое провело 3 дня в основной очереди, в DLQ с retention 4 дня проживёт ещё только день. Делай retention DLQ **максимальным** (14 дней);
- **redrive** — возврат сообщений из DLQ в исходную очередь после исправления бага: в консоли или через API `StartMessageMoveTask`;
- **алерт** на `ApproximateNumberOfMessagesVisible > 0` у DLQ обязателен (модуль 14).

## 3.10 Отложенные сообщения

- `DelaySeconds` на очереди или на отдельном сообщении (только standard) — до 15 минут;
- для задержек дольше 15 минут и расписаний — **EventBridge Scheduler** (модуль 5);
- ретраи с растущей задержкой в SQS делаются через `change_message_visibility`: при ошибке выставляй видимость на `min(base × 2^attempt, максимум)` вместо ожидания стандартного timeout.

```python
except TemporaryError:
    attempts = int(msg["Attributes"]["ApproximateReceiveCount"])
    delay = min(2 ** attempts * 5, 900)       # 10 с, 20 с, 40 с ... до 15 минут
    sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"],
                                  VisibilityTimeout=delay)
```

## 3.11 Типичные ошибки с SQS

| Ошибка | Последствие | Как правильно |
|---|---|---|
| Short polling | Пустые ответы при наличии сообщений, лишние расходы | `WaitTimeSeconds=20` |
| Удаление до обработки | Потеря при падении воркера | Удалять после успешной обработки |
| Visibility timeout меньше времени обработки | Параллельная повторная обработка | Timeout > p99 обработки или продление видимости |
| Нет DLQ | «Ядовитое» сообщение крутится до истечения retention и тратит деньги | DLQ + `maxReceiveCount` + алерт |
| Retention DLQ как у основной очереди | Сообщения исчезают из DLQ раньше, чем их разберут | 14 дней на DLQ |
| Не проверять `Failed` в батчах | Молчаливая потеря части сообщений | Повторять неудавшиеся |
| Standard-очередь и расчёт на порядок | Нарушение порядка | FIFO с `MessageGroupId` или порядок в данных |
| Неидемпотентный консьюмер | Дубли в standard-очередях | Идемпотентность (модуль 11) |

### Вопросы для самопроверки

1. Чем visibility timeout отличается от ack в RabbitMQ?
2. Какие гарантии дают standard- и FIFO-очереди?
3. Почему short polling может вернуть пустой ответ, когда сообщения есть?
4. Почему у DLQ standard-очереди должен быть максимальный retention?
5. Как сделать ретраи с растущей задержкой в SQS?

---

# Модуль 4. AWS SNS: fan-out, фильтрация и SNS + SQS

## 4.1 Что такое SNS

**Amazon SNS** — pub/sub: издатель публикует сообщение в **топик**, SNS доставляет копию каждому **подписчику**. SNS не хранит сообщения: он доставляет их сразу (с ретраями), поэтому подписчик должен быть доступен или быть очередью.

Типы подписок:

| Протокол | Для чего |
|---|---|
| `sqs` | Надёжная доставка в очередь: основной вариант для сервисов |
| `lambda` | Вызов функции |
| `http` / `https` | Вебхук |
| `firehose` | Поток в хранилище и аналитику |
| `email`, `sms`, мобильные push | Уведомления людям |

## 4.2 Главный паттерн: SNS + SQS

```
                          +--> [SQS billing]   --> сервис биллинга
order-service --> SNS  ---+--> [SQS stock]     --> склад
     publish   "orders"   +--> [SQS analytics] --> аналитика
```

Каждый сервис получает **свою очередь**, подписанную на общий топик. Это прямой аналог fanout exchange с очередями в RabbitMQ:

- сервис может лежать — сообщения ждут в его очереди;
- каждый сервис читает в своём темпе, со своей DLQ;
- новый сервис подключается новой очередью и подпиской, издатель не меняется.

```bash
awsl sns create-topic --name orders
TOPIC_ARN=arn:aws:sns:eu-central-1:123456789012:orders

awsl sqs create-queue --queue-name billing
BILLING_ARN=arn:aws:sqs:eu-central-1:123456789012:billing

awsl sns subscribe --topic-arn $TOPIC_ARN --protocol sqs --notification-endpoint $BILLING_ARN \
  --attributes RawMessageDelivery=true
```

В облаке очереди нужна **политика доступа**, разрешающая SNS писать в неё:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "sns.amazonaws.com"},
    "Action": "sqs:SendMessage",
    "Resource": "arn:aws:sqs:eu-central-1:123456789012:billing",
    "Condition": {"ArnEquals": {"aws:SourceArn": "arn:aws:sns:eu-central-1:123456789012:orders"}}
  }]
}
```

Без неё подписка создастся, но сообщения молча не будут доставляться. Это одна из самых частых проблем с SNS + SQS.

## 4.3 Raw message delivery

По умолчанию SNS оборачивает сообщение в JSON-конверт, и в очередь приходит не твоё тело, а:

```json
{"Type":"Notification","MessageId":"...","TopicArn":"...","Message":"{\"order_id\":\"order-1\"}","Timestamp":"...", ...}
```

С атрибутом подписки `RawMessageDelivery=true` в очередь попадает исходное тело, а атрибуты сообщения становятся атрибутами SQS. Для межсервисной интеграции почти всегда включай raw delivery.

## 4.4 Фильтрация подписок

Подписка может принимать не все сообщения топика, а только подходящие под **filter policy**:

```json
{
  "event_type": ["OrderCreated", "OrderCancelled"],
  "region": [{"prefix": "eu-"}],
  "amount": [{"numeric": [">=", 1000]}]
}
```

```bash
awsl sns set-subscription-attributes --subscription-arn $SUB_ARN \
  --attribute-name FilterPolicy --attribute-value file://filter.json
# фильтровать по телу сообщения, а не по атрибутам:
awsl sns set-subscription-attributes --subscription-arn $SUB_ARN \
  --attribute-name FilterPolicyScope --attribute-value MessageBody
```

Операторы: точные значения, `prefix`, `suffix`, `anything-but`, `numeric`, `exists`, `equals-ignore-case`, IP-адреса. Фильтрация на стороне SNS дешевле, чем принимать всё и выбрасывать в консьюмере.

## 4.5 Надёжность доставки

- В SQS и Lambda SNS доставляет надёжно, с многочисленными ретраями.
- Для HTTP-эндпоинтов настраивается **delivery policy** (число и интервалы ретраев).
- У каждой подписки можно задать **DLQ** (`RedrivePolicy` на подписке): сообщения, которые не удалось доставить подписчику, попадут в SQS-очередь для разбора.
- SNS standard — at-least-once, без порядка. **SNS FIFO** — порядок и дедупликация, в паре с SQS FIFO.

## 4.6 Большие сообщения

С сентября 2026 года топик может принимать сообщения до 1 МиБ, если задать атрибут `MaximumMessageSize`; такие топики поддерживают подписки SQS, Lambda и Firehose и ограничены сотней подписок. По умолчанию лимит остаётся 256 КиБ. Для ещё больших данных — паттерн claim check: объект в S3, ссылка в сообщении (есть готовые Extended Client Libraries).

### Вопросы для самопроверки

1. Почему для надёжного fan-out к сервисам используют SNS + SQS, а не прямые подписки HTTP?
2. Что произойдёт, если у очереди нет политики, разрешающей SNS писать в неё?
3. Зачем нужен `RawMessageDelivery`?
4. Как отфильтровать сообщения на стороне SNS по полю тела?

---

# Модуль 5. AWS EventBridge и Kinesis

## 5.1 EventBridge: шина событий

**Amazon EventBridge** — шина событий с маршрутизацией по **правилам на содержимое**. Издатель отправляет событие в шину, правила решают, какие цели его получат.

```
                         правило: source=shop.orders, detail-type=OrderCreated  --> Lambda
PutEvents --> [шина] ---+ правило: detail.amount > 10000                        --> Step Functions (антифрод)
                         правило: всё из source=shop.*                           --> SQS (аудит)
```

Шины:

| Шина | Что в неё приходит |
|---|---|
| **default** | События сервисов AWS: изменения EC2, S3, CodePipeline, CloudTrail и сотен других |
| **custom** | События твоих приложений |
| **partner** | События SaaS-партнёров (Zendesk, Datadog, Shopify и другие) |

Главная сила EventBridge — **интеграции**: реакция на события инфраструктуры AWS и SaaS без написания опросчиков.

## 5.2 Структура события и правила

```json
{
  "version": "0",
  "id": "6a7e8feb-b491-4cf7-a9f1-bf3703467718",
  "detail-type": "OrderCreated",
  "source": "shop.orders",
  "account": "123456789012",
  "time": "2026-09-15T10:00:00Z",
  "region": "eu-central-1",
  "resources": [],
  "detail": {"order_id": "order-1", "amount": 14990, "country": "DE"}
}
```

Шаблон правила — JSON, который должен «совпасть» с событием:

```json
{
  "source": ["shop.orders"],
  "detail-type": ["OrderCreated"],
  "detail": {
    "amount": [{"numeric": [">", 10000]}],
    "country": [{"anything-but": ["US"]}]
  }
}
```

```bash
awsl events create-event-bus --name shop
awsl events put-rule --name big-orders --event-bus-name shop --event-pattern file://pattern.json
awsl events put-targets --rule big-orders --event-bus-name shop \
  --targets "Id"="fraud","Arn"="arn:aws:sqs:eu-central-1:123456789012:fraud-checks"

awsl events put-events --entries '[{"Source":"shop.orders","DetailType":"OrderCreated",
  "EventBusName":"shop","Detail":"{\"order_id\":\"order-1\",\"amount\":14990,\"country\":\"DE\"}"}]'
```

`PutEvents` принимает до 10 событий за вызов и, как батчи SQS, может **частично не пройти**: проверяй `FailedEntryCount`.

## 5.3 Цели, ретраи и DLQ

- У правила до 5 целей: Lambda, SQS, SNS, Step Functions, Kinesis, API destinations (HTTP-эндпоинты вне AWS с аутентификацией и лимитом скорости) и многие другие.
- **Input transformer** меняет форму события перед доставкой в цель.
- При ошибке доставки EventBridge повторяет попытки (по умолчанию до 24 часов и 185 попыток); задай **DLQ на цель** — SQS-очередь для недоставленных событий.
- Доставка at-least-once, **порядок не гарантирован**, задержка — обычно сотни миллисекунд. Для строгого порядка EventBridge не подходит.

## 5.4 Archive и replay

Шина может **архивировать** события (все или по шаблону) и **переиграть** их за период в ту же шину. Это даёт ограниченную возможность «перечитать историю», например после исправления бага в обработчике. Полноценным логом с offset'ами это не является — для этого есть Kinesis или Kafka.

## 5.5 EventBridge Scheduler и Pipes

**Scheduler** — отложенные и повторяющиеся задачи: разовый запуск в заданное время, cron и rate-выражения, часовые пояса, миллионы расписаний, цели как у шины. Это правильный ответ на «отправить напоминание через 3 дня», который не решается задержкой SQS (15 минут максимум).

```bash
awsl scheduler create-schedule --name remind-order-1 \
  --schedule-expression "at(2026-09-18T10:00:00)" --flexible-time-window Mode=OFF \
  --target '{"Arn":"arn:aws:sqs:eu-central-1:123456789012:reminders","RoleArn":"arn:aws:iam::123456789012:role/scheduler","Input":"{\"order_id\":\"order-1\"}"}'
```

**Pipes** — связка «источник → фильтр → обогащение → цель» без кода: например, читать из SQS, отфильтровать, обогатить через Lambda и отправить в Step Functions.

## 5.6 Kinesis Data Streams: лог в AWS

Когда нужен **лог** — порядок по ключу, повторное чтение, много независимых читателей, большие потоки, — в AWS есть два пути: **Kinesis Data Streams** и **Amazon MSK** (управляемая Kafka, см. курс по Kafka).

| Понятие Kinesis | Аналог в Kafka |
|---|---|
| Stream | Топик |
| Shard | Партиция |
| Partition key | Ключ сообщения |
| Sequence number | Offset |
| KCL (Kinesis Client Library) + DynamoDB | Consumer group + `__consumer_offsets` |
| Enhanced fan-out | Выделенная пропускная способность на читателя |

- Режимы: **on-demand** (масштабируется сам) или **provisioned** (шарды задаёшь ты). Шард — порядка 1 МБ/с или 1000 записей/с на запись.
- Хранение: 24 часа по умолчанию, можно увеличить до 365 дней.
- Порядок гарантирован внутри шарда, то есть по partition key.

Kinesis или MSK: Kinesis проще в эксплуатации и тесно интегрирован с AWS; MSK даёт экосистему Kafka (Connect, Streams, Schema Registry) и переносимость.

## 5.7 Что выбрать в AWS

| Задача | Сервис |
|---|---|
| Очередь задач | SQS standard |
| Очередь с порядком по сущности или дедупликацией | SQS FIFO |
| Одно событие нескольким сервисам | SNS + SQS |
| Маршрутизация по содержимому, события AWS и SaaS | EventBridge |
| Отложенные задачи и расписания | EventBridge Scheduler |
| Лог, стриминг, повторное чтение | Kinesis Data Streams или MSK |
| Переезд с RabbitMQ без переписывания | Amazon MQ |

### Вопросы для самопроверки

1. Чем EventBridge отличается от SNS?
2. Почему `PutEvents` нужно проверять на частичные ошибки?
3. Как в AWS запланировать событие через три дня?
4. Когда вместо SQS нужен Kinesis или MSK?

---

# Модуль 6. Azure Service Bus: очереди, топики, сессии

## 6.1 Что такое Service Bus

**Azure Service Bus** — корпоративный брокер сообщений: очереди, топики с подписками, сессии для порядка, дедупликация, отложенные сообщения, DLQ и транзакции. Из трёх облаков это сервис, по возможностям **ближе всего к RabbitMQ**, и он говорит по **AMQP 1.0**.

```
namespace (shop-bus.servicebus.windows.net)
  ├── queue "tasks"                     -> воркеры (competing consumers)
  │     └── $DeadLetterQueue            (подочередь для мёртвых сообщений)
  └── topic "orders"
        ├── subscription "billing"      правило: EventType = 'OrderCreated'
        │     └── $DeadLetterQueue
        ├── subscription "stock"        правило: EventType IN ('OrderCreated','OrderCancelled')
        └── subscription "audit"        правило: 1=1 (всё)
```

- **Namespace** — контейнер и граница доступа, аналог vhost в RabbitMQ.
- **Queue** — очередь для одного типа получателей.
- **Topic + subscriptions** — pub/sub: каждая подписка ведёт себя как отдельная очередь со своими правилами фильтрации. Прямой аналог topic exchange + очереди.

## 6.2 Тарифы

| | Basic | Standard | Premium |
|---|---|---|---|
| Очереди | Да | Да | Да |
| Топики и подписки | **Нет** | Да | Да |
| Сессии, дедупликация, транзакции | Нет | Да | Да |
| Макс. размер сообщения | 256 КиБ | 256 КиБ | До 100 МБ |
| Ресурсы | Общие | Общие | Выделенные (messaging units), предсказуемая задержка |
| Приватная сеть (private endpoints), geo-репликация | Нет | Нет | Да |

Для продакшена с требованиями к задержке, изоляции и сети — Premium. Standard хорош для большинства интеграций со средней нагрузкой.

## 6.3 Peek-lock и способы завершить сообщение

Service Bus выдаёт сообщения в режиме **peek-lock** (по умолчанию): сообщение блокируется за получателем на `LockDuration` (по умолчанию 1 минута, максимум 5 минут), и получатель должен явно его **завершить**:

| Действие | Эффект | Аналог |
|---|---|---|
| `complete` | Обработано, удалить | ack |
| `abandon` | Вернуть в очередь, счётчик доставок +1 | nack с requeue |
| `dead-letter` | Отправить в DLQ с причиной и описанием | reject в DLX |
| `defer` | Отложить: сообщение остаётся в очереди, но выдаётся только по `sequence number` | — (своя особенность Service Bus) |
| lock истёк | Сообщение снова доступно, счётчик +1 | Истёк visibility timeout |

Режим **receive-and-delete** удаляет сообщение сразу при выдаче: at-most-once, для данных, которые не жалко.

После `MaxDeliveryCount` доставок (по умолчанию **10**) сообщение автоматически уходит в подочередь `$DeadLetterQueue`. Туда же могут попадать сообщения с истёкшим TTL, если включено `DeadLetteringOnMessageExpiration`.

## 6.4 Отправка и получение на Python

```bash
pip install azure-servicebus azure-identity
```

```python
import json
from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage, AutoLockRenewer

# в облаке — Entra ID вместо строки подключения
client = ServiceBusClient("shop-bus.servicebus.windows.net", credential=DefaultAzureCredential())
# для эмулятора: ServiceBusClient.from_connection_string(CONN)  (см. модуль 2.5)

with client:
    with client.get_queue_sender("tasks") as sender:
        sender.send_messages(ServiceBusMessage(
            json.dumps({"task": "resize", "image": "1.png"}),
            message_id="resize-1.png",                  # для дедупликации
            application_properties={"event-type": "ResizeRequested"},
        ))

    renewer = AutoLockRenewer(max_lock_renewal_duration=600)    # продлевать lock до 10 минут
    with client.get_queue_receiver("tasks", max_wait_time=20, auto_lock_renewer=renewer) as receiver:
        for msg in receiver:
            try:
                process(json.loads(str(msg)))                    # должно быть идемпотентным
                receiver.complete_message(msg)
            except ValueError as e:
                receiver.dead_letter_message(msg, reason="InvalidPayload", error_description=str(e))
            except TemporaryError:
                receiver.abandon_message(msg)                   # вернётся, delivery count +1
```

`AutoLockRenewer` продлевает блокировку, пока идёт долгая обработка: без него сообщение, обрабатываемое дольше `LockDuration`, будет выдано второму получателю.

То же на Go (`azservicebus`):

```go
import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

cred, _ := azidentity.NewDefaultAzureCredential(nil)
client, err := azservicebus.NewClient("shop-bus.servicebus.windows.net", cred, nil)
// для эмулятора: azservicebus.NewClientFromConnectionString(conn, nil)

sender, _ := client.NewSender("tasks", nil)
err = sender.SendMessage(ctx, &azservicebus.Message{
	Body:                  []byte(`{"task":"resize","image":"1.png"}`),
	MessageID:             to.Ptr("resize-1.png"), // для дедупликации
	ApplicationProperties: map[string]any{"event-type": "ResizeRequested"},
}, nil)

receiver, _ := client.NewReceiverForQueue("tasks", nil) // peek-lock по умолчанию
msgs, err := receiver.ReceiveMessages(ctx, 10, nil)
for _, m := range msgs {
	switch err := process(m.Body); {
	case err == nil:
		receiver.CompleteMessage(ctx, m, nil)
	case errors.Is(err, errTemporary):
		receiver.AbandonMessage(ctx, m, nil) // delivery count +1
	default:
		receiver.DeadLetterMessage(ctx, m, &azservicebus.DeadLetterOptions{
			Reason: to.Ptr("InvalidPayload"), ErrorDescription: to.Ptr(err.Error()),
		})
	}
}
```

В Go-SDK нет готового аналога `AutoLockRenewer`: для долгой обработки вызывай `receiver.RenewMessageLock(ctx, m, nil)` в фоне. Полный консьюмер — в `examples/go/cmd/servicebus-worker`.

Чтение DLQ для разбора:

```python
from azure.servicebus import ServiceBusSubQueue

with client.get_queue_receiver("tasks", sub_queue=ServiceBusSubQueue.DEAD_LETTER, max_wait_time=5) as dlq:
    for msg in dlq:
        print(msg.dead_letter_reason, msg.dead_letter_error_description, str(msg))
        dlq.complete_message(msg)
```

## 6.5 Сессии: порядок по сущности

**Сессия** — группа сообщений с одинаковым `session_id`. Очередь или подписка с включёнными сессиями (`requires_session=True`) гарантирует, что:

- сообщения сессии выдаются **строго по порядку**;
- всю сессию одновременно обрабатывает **один** получатель (он «захватывает» сессию);
- разные сессии обрабатываются параллельно.

Это аналог `MessageGroupId` в SQS FIFO и ordering keys в Pub/Sub.

```python
from azure.servicebus import NEXT_AVAILABLE_SESSION

with client.get_queue_sender("orders-ordered") as sender:
    for event in ["OrderCreated", "OrderPaid", "OrderShipped"]:
        sender.send_messages(ServiceBusMessage(json.dumps({"event": event}), session_id="order-1"))

with client.get_queue_receiver("orders-ordered", session_id=NEXT_AVAILABLE_SESSION,
                               max_wait_time=5) as receiver:
    print("обрабатываю сессию", receiver.session.session_id)
    for msg in receiver:
        handle(msg)
        receiver.complete_message(msg)
```

У сессии есть **состояние** (`session.set_state` / `get_state`): небольшой блок данных, который хранится в Service Bus и переживает смену получателя. Удобно для конечных автоматов и саг.

## 6.6 Дедупликация и отложенные сообщения

**Duplicate detection** включается при создании очереди или топика (`requires_duplicate_detection=True`) и настраивается окном (по умолчанию 10 минут). Сообщение с `message_id`, уже виденным в окне, принимается, но **молча отбрасывается**. Это защищает от дублей при повторной отправке после сбоя — при условии, что `message_id` стабилен для бизнес-события.

**Scheduled messages** — сообщение становится доступным в заданное время:

```python
from datetime import datetime, timedelta, timezone

with client.get_queue_sender("reminders") as sender:
    seq = sender.schedule_messages(ServiceBusMessage('{"order_id":"order-1"}'),
                                   datetime.now(timezone.utc) + timedelta(days=3))
    # sender.cancel_scheduled_messages(seq)   # передумали
```

## 6.7 Топики, подписки и правила

```bash
az servicebus topic create -g shop-rg --namespace-name shop-bus -n orders
az servicebus topic subscription create -g shop-rg --namespace-name shop-bus --topic-name orders \
  -n billing --max-delivery-count 5 --enable-dead-lettering-on-message-expiration true

# заменить правило по умолчанию (всё) на фильтр
az servicebus topic subscription rule delete -g shop-rg --namespace-name shop-bus \
  --topic-name orders --subscription-name billing -n '$Default'
az servicebus topic subscription rule create -g shop-rg --namespace-name shop-bus \
  --topic-name orders --subscription-name billing -n created-only \
  --filter-sql-expression "EventType = 'OrderCreated' AND Amount > 0"
```

| Фильтр | Как | Когда |
|---|---|---|
| **SQL filter** | Выражение по свойствам сообщения (`application_properties`) | Гибкие условия |
| **Correlation filter** | Точное совпадение `correlation_id`, `subject`, свойств | Быстрее SQL, покрывает большинство случаев |
| **True filter** (`1=1`) | Всё | Правило `$Default` при создании подписки |

Правила фильтруют по **свойствам**, а не по телу: кладите поля для маршрутизации в `application_properties`.

**Auto-forwarding** пересылает сообщения из очереди или подписки в другую очередь или топик того же namespace: так строят цепочки и разветвления без кода.

## 6.8 Транзакции

В пределах namespace Service Bus поддерживает транзакции: получить сообщение, отправить новые и завершить исходное **атомарно** (через `send-via`). Это даёт consume-transform-produce без дублей внутри Service Bus — аналог транзакций Kafka. На внешнюю базу данных транзакция не распространяется: там нужна идемпотентность (модуль 11).

### Вопросы для самопроверки

1. Чем `abandon` отличается от `dead-letter` и `defer`?
2. Зачем нужен `AutoLockRenewer`?
3. Как сессии обеспечивают порядок, и чем это похоже на SQS FIFO?
4. Почему фильтры подписок работают по свойствам, а не по телу сообщения?
5. Какой тариф нужен для топиков и для private endpoints?

---

# Модуль 7. Azure Event Grid и Event Hubs

## 7.1 Event Grid: маршрутизация событий

**Azure Event Grid** — шина событий, аналог EventBridge. Главные сценарии:

- реагировать на **события ресурсов Azure**: файл появился в Blob Storage, создана виртуальная машина, изменился секрет в Key Vault;
- раздавать **события приложений** подписчикам с фильтрацией.

| Понятие | Что это |
|---|---|
| **System topic** | События сервиса Azure (Storage, Resource Groups, Key Vault...) |
| **Custom topic** | События твоего приложения |
| **Domain** | Много топиков под одним эндпоинтом (для мультиарендных систем) |
| **Event subscription** | Подписка с фильтром и обработчиком |

```bash
# реагировать на новые файлы в контейнере uploads и отправлять события в очередь Service Bus
az eventgrid event-subscription create -n new-uploads \
  --source-resource-id $STORAGE_ACCOUNT_ID \
  --included-event-types Microsoft.Storage.BlobCreated \
  --subject-begins-with /blobServices/default/containers/uploads/ \
  --endpoint-type servicebusqueue --endpoint $SERVICEBUS_QUEUE_ID \
  --max-delivery-attempts 30 --event-ttl 1440
```

Особенности:

- доставка **push** на обработчики: Azure Functions, webhooks, Service Bus, Storage Queues, Event Hubs;
- ретраи с backoff (по умолчанию до 24 часов и 30 попыток), **dead-letter в Blob Storage** для недоставленных событий;
- поддерживает формат **CloudEvents 1.0** (модуль 17);
- **Event Grid namespaces** дополнительно дают pull-доставку по HTTP и встроенный **MQTT-брокер** для IoT.

Частый паттерн: Event Grid → очередь Service Bus → воркеры. Event Grid хорош в маршрутизации, а очередь даёт буфер, контроль темпа и peek-lock.

## 7.2 Event Hubs: лог в Azure

**Azure Event Hubs** — сервис для больших потоков событий, по модели — **Kafka**:

| Понятие Event Hubs | Аналог в Kafka |
|---|---|
| Namespace | Кластер |
| Event hub | Топик |
| Partition | Партиция |
| Consumer group | Consumer group |
| Offset / sequence number | Offset |
| Checkpoint (в Blob Storage) | Committed offset |

- Партиции задаются при создании; порядок гарантирован внутри партиции (по partition key).
- Хранение — от часа до 7 дней на Standard, до 90 дней на Premium и Dedicated.
- **Kafka-эндпоинт** (Standard и выше): обычные Kafka-клиенты подключаются к Event Hubs, меняя только адрес и аутентификацию. Всё из курса по Kafka о producer'ах и consumer groups применимо, но это не настоящий Kafka: доступа к брокерам нет, часть Kafka API и настроек топиков поддерживается с ограничениями, лимиты свои. Перед миграцией сверь список поддерживаемых возможностей.
- **Capture** — автоматическая выгрузка потока в Blob Storage или Data Lake в формате Avro/Parquet.
- Есть свой **Schema Registry**.

```python
from azure.eventhub import EventHubProducerClient, EventData

producer = EventHubProducerClient(fully_qualified_namespace="shop-hub.servicebus.windows.net",
                                  eventhub_name="clicks", credential=DefaultAzureCredential())
with producer:
    batch = producer.create_batch(partition_key="user-42")    # порядок по пользователю
    batch.add(EventData('{"page":"/cart"}'))
    producer.send_batch(batch)
```

Читают поток через `EventProcessorClient`/`EventHubConsumerClient` с хранилищем checkpoint'ов в Blob Storage: библиотека распределяет партиции между экземплярами, как consumer group в Kafka.

## 7.3 Что выбрать в Azure

| Задача | Сервис |
|---|---|
| Очередь задач с надёжной доставкой | Service Bus queue |
| Порядок по сущности | Service Bus с сессиями |
| Pub/sub между сервисами с фильтрами | Service Bus topics |
| Реакция на события ресурсов Azure, маршрутизация | Event Grid |
| Большие потоки, телеметрия, лог, Kafka-клиенты | Event Hubs |
| Очень простая и дешёвая очередь | Storage Queues |
| MQTT для устройств | Event Grid namespaces (MQTT-брокер) или IoT Hub |

### Вопросы для самопроверки

1. Чем Event Grid отличается от Service Bus topics?
2. Куда Event Grid отправляет события, которые не удалось доставить?
3. Что общего у Event Hubs и Kafka, и чем Event Hubs с Kafka-эндпоинтом отличается от настоящего Kafka?

---

# Модуль 8. Google Cloud Pub/Sub

## 8.1 Модель Pub/Sub

**Google Cloud Pub/Sub** — глобальный сервис обмена сообщениями, который в одном продукте закрывает и **очередь**, и **pub/sub**:

```
publisher --> [topic orders] --+--> subscription "billing"   (pull) --> 3 воркера делят сообщения
                               +--> subscription "stock"     (pull) --> 2 воркера
                               +--> subscription "webhook"   (push) --> HTTPS-эндпоинт
                               +--> subscription "bq"        (BigQuery) --> таблица
```

- Сообщение, опубликованное в **топик**, копируется в **каждую подписку**.
- Внутри подписки сообщения **делят между собой** все её получатели (competing consumers).
- Итого: топик + одна подписка = очередь; топик + несколько подписок = fan-out.

Это та же модель, что exchange + очереди в RabbitMQ, но без отдельного понятия «очередь»: её роль играет подписка.

## 8.2 Типы подписок

| Тип | Как | Когда |
|---|---|---|
| **Pull** (streaming pull) | Клиентская библиотека держит поток и получает сообщения | Воркеры, основной вариант |
| **Push** | Pub/Sub вызывает HTTPS-эндпоинт, 2xx = ack | Cloud Run, serverless, вебхуки |
| **BigQuery** | Пишет сообщения прямо в таблицу | Аналитика без кода |
| **Cloud Storage** | Пишет файлы в бакет | Архив, data lake |

## 8.3 Ack deadline и повторная доставка

Полученное сообщение нужно подтвердить (`ack`) до истечения **ack deadline** (10 секунд по умолчанию, от 10 секунд до 10 минут). Не подтвердил — сообщение будет доставлено снова. Клиентские библиотеки **продлевают deadline автоматически**, пока сообщение в обработке (lease management), до настраиваемого предела.

- `ack` — обработано;
- `nack` — вернуть немедленно (или с задержкой по retry policy);
- ничего — повтор после истечения deadline.

**Retry policy** подписки: немедленно или с экспоненциальной задержкой между `min-retry-delay` (от 10 с) и `max-retry-delay` (до 600 с).

## 8.4 Публикация и получение на Python

```bash
pip install google-cloud-pubsub
```

```python
import json
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

project = "my-project"
publisher = pubsub_v1.PublisherClient()
topic = publisher.topic_path(project, "orders")

# publish возвращает future: результат — message id после подтверждения сервисом
future = publisher.publish(
    topic,
    json.dumps({"order_id": "order-1", "amount": 4990}).encode(),
    event_type="OrderCreated",          # атрибуты: строка -> строка
)
print("опубликовано:", future.result(timeout=30))

subscriber = pubsub_v1.SubscriberClient()
sub = subscriber.subscription_path(project, "billing")

def callback(message: pubsub_v1.subscriber.message.Message):
    try:
        process(json.loads(message.data))        # должно быть идемпотентным
        message.ack()
    except ValueError:
        message.ack()                            # мусор: не ретраить (или отправить в свою DLQ)
    except Exception:
        message.nack()                           # вернуть по retry policy

flow = pubsub_v1.types.FlowControl(max_messages=100)   # аналог prefetch
streaming = subscriber.subscribe(sub, callback=callback, flow_control=flow)
with subscriber:
    try:
        streaming.result()                       # блокирует, пока не остановим
    except (KeyboardInterrupt, TimeoutError):
        streaming.cancel()
        streaming.result()
```

То же на Go (`cloud.google.com/go/pubsub/v2`):

```go
import "cloud.google.com/go/pubsub/v2"

client, err := pubsub.NewClient(ctx, "my-project") // с PUBSUB_EMULATOR_HOST — эмулятор

publisher := client.Publisher("orders")
defer publisher.Stop()
id, err := publisher.Publish(ctx, &pubsub.Message{
	Data:       []byte(`{"order_id":"order-1","amount":4990}`),
	Attributes: map[string]string{"event_type": "OrderCreated"},
}).Get(ctx) // дождаться подтверждения сервисом

sub := client.Subscriber("billing")
sub.ReceiveSettings.MaxOutstandingMessages = 100 // аналог prefetch
err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
	if err := process(m.Data); err != nil {
		m.Nack() // вернуть по retry policy
		return
	}
	m.Ack() // клиент сам продлевает ack deadline, пока колбэк работает
})
```

Топики и подписки в Go создаются через `client.TopicAdminClient` и `client.SubscriptionAdminClient` (типы из `pubsubpb`). Полный консьюмер — в `examples/go-gcp/cmd/pubsub-worker`.

- `FlowControl` ограничивает число сообщений в обработке: это prefetch Pub/Sub. Без него быстрый поток может переполнить память воркера.
- Колбэки вызываются в пуле потоков: обработка должна быть потокобезопасной.
- Publisher сам батчит сообщения; настройки батчинга — `BatchSettings`.

## 8.5 Dead letter topic

```bash
gcloud pubsub topics create orders-dlq
gcloud pubsub subscriptions create billing --topic orders \
  --ack-deadline 60 \
  --dead-letter-topic orders-dlq --max-delivery-attempts 5 \
  --min-retry-delay 10s --max-retry-delay 600s
gcloud pubsub subscriptions create orders-dlq-sub --topic orders-dlq
```

- После `max-delivery-attempts` (от 5 до 100) сообщение публикуется в dead letter **топик**. Чтобы сообщения там не пропали, у DLQ-топика должна быть **подписка**.
- Сервисному аккаунту Pub/Sub нужны права **публиковать** в DLQ-топик и **подписчика** на исходной подписке, иначе dead lettering молча не работает. В консоли это настраивается кнопкой, в Terraform — явными IAM-привязками (модуль 16).
- Число попыток доступно консьюмеру в `message.delivery_attempt`.

## 8.6 Порядок: ordering keys

```bash
gcloud pubsub subscriptions create stock --topic orders --enable-message-ordering
```

```python
publisher = pubsub_v1.PublisherClient(
    publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True))
for event in ["OrderCreated", "OrderPaid", "OrderShipped"]:
    publisher.publish(topic, json.dumps({"event": event}).encode(), ordering_key="order-1")
```

- Сообщения с одним `ordering_key`, опубликованные в **одном регионе**, доставляются подписчикам с включённым ordering по порядку. Разные ключи — параллельно.
- Если публикация с ключом упала, publisher приостанавливает этот ключ, чтобы не нарушить порядок; после обработки ошибки нужно вызвать `publisher.resume_publish(topic, ordering_key)`.
- Ordering снижает пропускную способность на ключ и повышает задержку: включай только там, где порядок действительно нужен.

## 8.7 Exactly-once delivery

Pull-подписка с `--enable-exactly-once-delivery` гарантирует, что успешно подтверждённое сообщение **не будет доставлено повторно**, пока не истёк его ack deadline:

```python
from google.cloud.pubsub_v1.subscriber import exceptions as sub_exceptions

def callback(message):
    process(message.data)
    ack_future = message.ack_with_response()      # узнать, принят ли ack сервисом
    try:
        ack_future.result()                       # успех: повторов не будет
    except sub_exceptions.AcknowledgeError as e:
        print("ack не принят, сообщение будет доставлено снова:", e.error_code)
```

Честные ограничения:

- работает для **pull**-подписок и в пределах региона;
- защищает от повторной **доставки**, а не от повторной публикации: если publisher отправил сообщение дважды, это два разных сообщения;
- если обработка — запись во внешнюю базу, идемпотентность всё равно нужна (модуль 11);
- снижает пропускную способность и повышает задержку.

## 8.8 Фильтры, seek и retention

**Фильтр подписки** — по атрибутам, задаётся при создании и **не меняется**:

```bash
gcloud pubsub subscriptions create billing-eu --topic orders \
  --message-filter='attributes.event_type = "OrderCreated" AND hasPrefix(attributes.region, "eu-")'
```

Отфильтрованные сообщения автоматически подтверждаются и не тарифицируются как доставка.

**Retention и seek** дают повторное чтение:

- подписка хранит неподтверждённые сообщения до 7 дней; с `--retain-acked-messages` — и подтверждённые;
- топик может хранить сообщения до 31 дня (`--message-retention-duration`), тогда новая подписка может прочитать прошлое;
- **seek** перематывает подписку на момент времени или на **snapshot**:

```bash
gcloud pubsub snapshots create before-deploy --subscription billing   # перед рискованным релизом
gcloud pubsub subscriptions seek billing --snapshot before-deploy      # откат после бага
gcloud pubsub subscriptions seek billing --time 2026-09-15T10:00:00Z
```

Это не полноценный лог с offset'ами, как Kafka, но на практике закрывает главные сценарии повторного чтения.

## 8.9 Push-подписки

Pub/Sub отправляет POST на HTTPS-эндпоинт; ответ 2xx — ack, иначе повтор с backoff. Для Cloud Run и других сервисов Google эндпоинт защищают **OIDC-токеном** сервисного аккаунта, который Pub/Sub прикладывает к запросу. Push удобен для serverless, но темп доставки контролирует Pub/Sub: обработчик должен выдерживать всплески.

### Вопросы для самопроверки

1. Как в Pub/Sub получить очередь, а как — fan-out?
2. Зачем нужен `FlowControl`?
3. Какие права нужны сервисному аккаунту Pub/Sub для работы dead letter topic?
4. От чего защищает exactly-once delivery, и от чего — нет?
5. Как откатить подписку к состоянию до неудачного релиза?

---

# Модуль 9. Google Eventarc, Cloud Tasks и Managed Kafka

## 9.1 Eventarc: маршрутизация событий

**Eventarc** — аналог EventBridge и Event Grid в Google Cloud: доставляет события в Cloud Run, GKE и Workflows.

Источники событий:

- **прямые события** сервисов Google (например, объект создан в Cloud Storage);
- **Cloud Audit Logs** — почти любое действие с ресурсами Google Cloud;
- **Pub/Sub**-топики, в том числе с событиями твоих приложений;
- сторонние провайдеры.

```bash
gcloud eventarc triggers create uploads-trigger \
  --location=europe-west1 \
  --destination-run-service=thumbnailer --destination-run-region=europe-west1 \
  --event-filters="type=google.cloud.storage.object.v1.finalized" \
  --event-filters="bucket=shop-uploads" \
  --service-account=eventarc-sa@my-project.iam.gserviceaccount.com
```

События приходят в формате **CloudEvents**. Под капотом Eventarc использует Pub/Sub, поэтому гарантии похожие: at-least-once, без порядка. **Eventarc Advanced** добавляет шину сообщений и конвейеры с фильтрацией и преобразованием — ближе к полноценному EventBridge.

## 9.2 Cloud Tasks: очередь HTTP-задач

**Cloud Tasks** решает другую задачу, чем Pub/Sub: не «раздать событие», а «выполнить конкретный HTTP-вызов, с контролем темпа, в нужное время».

| | Pub/Sub | Cloud Tasks |
|---|---|---|
| Кто решает, куда доставить | Подписчики | Издатель задаёт цель в каждой задаче |
| Контроль темпа | Flow control у получателя | На очереди: `max-dispatches-per-second`, `max-concurrent-dispatches` |
| Отложенное выполнение | Нет | `schedule_time` — в будущем (до 30 дней) |
| Дедупликация | Exactly-once на подписке | По имени задачи |
| Fan-out | Да | Нет |

```bash
gcloud tasks queues create emails --location=europe-west1 \
  --max-dispatches-per-second=10 --max-concurrent-dispatches=5 \
  --max-attempts=10 --min-backoff=5s --max-backoff=300s
```

Типичные применения: вызовы внешнего API с ограничением частоты, отложенные действия («напомнить через 2 дня»), разгрузка тяжёлой работы из HTTP-обработчика.

## 9.3 Managed Service for Apache Kafka

Для лога в Google Cloud — **Managed Service for Apache Kafka**: настоящий Kafka под управлением Google, с поддержкой Kafka Connect. Всё из курса по Kafka применимо напрямую. Именно на него Google предлагал мигрировать с отключённого Pub/Sub Lite.

Pub/Sub или Kafka в Google Cloud:

| Нужно | Выбор |
|---|---|
| Интеграция сервисов, очереди, push в Cloud Run, минимум эксплуатации | Pub/Sub |
| Экосистема Kafka, порядок по партициям, долгое хранение, переносимость | Managed Kafka |
| Аналитика в BigQuery без кода | Pub/Sub с BigQuery-подпиской |

## 9.4 Что выбрать в Google Cloud

| Задача | Сервис |
|---|---|
| Очередь задач | Pub/Sub, pull-подписка |
| Fan-out | Pub/Sub, несколько подписок |
| Порядок по сущности | Pub/Sub ordering keys |
| Без повторных доставок | Pub/Sub exactly-once |
| Реакция на события ресурсов Google Cloud | Eventarc |
| HTTP-задачи с ограничением темпа и отложенным запуском | Cloud Tasks |
| Расписание | Cloud Scheduler |
| Лог, экосистема Kafka | Managed Service for Apache Kafka |

### Вопросы для самопроверки

1. Чем Cloud Tasks отличается от Pub/Sub?
2. В каком формате Eventarc доставляет события?
3. На что Google предлагал мигрировать с Pub/Sub Lite?

---

# Модуль 10. Паттерны в трёх облаках: таблицы соответствия

Этот модуль — шпаргалка для тех, кто знает одно облако и переходит в другое, или проектирует систему, которая должна жить в нескольких.

## 10.1 Очередь задач

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Сервис | SQS standard | Service Bus queue | Pub/Sub, pull-подписка |
| Получить | `ReceiveMessage` (long polling 20 с) | receiver (peek-lock) | streaming pull |
| Подтвердить | `DeleteMessage` | `complete` | `ack` |
| Вернуть | Не удалять или `ChangeMessageVisibility(0)` | `abandon` | `nack` |
| Время на обработку | Visibility timeout (до 12 ч) | Lock duration (до 5 мин) + продление | Ack deadline (до 10 мин) + автопродление |
| Продлить | `ChangeMessageVisibility` | `renew_message_lock` / `AutoLockRenewer` | Автоматически в клиенте |
| Ограничить параллелизм | `MaxNumberOfMessages` и число потоков | `prefetch_count`, число получателей | `FlowControl` |
| Счётчик попыток | `ApproximateReceiveCount` | `delivery_count` | `delivery_attempt` (при DLQ) |

## 10.2 Fan-out

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Как | SNS-топик + SQS-очередь на каждый сервис | Service Bus topic + subscription на каждый сервис | Pub/Sub топик + подписка на каждый сервис |
| Фильтр | Filter policy подписки (атрибуты или тело) | SQL/correlation filter (свойства) | Message filter (атрибуты, неизменяемый) |
| Что не забыть | Политика доступа SQS для SNS, `RawMessageDelivery` | Удалить правило `$Default`, если нужен фильтр | Подписка на DLQ-топик, права сервисного аккаунта |

## 10.3 Порядок по сущности

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Механизм | SQS FIFO, `MessageGroupId` | Сессии, `session_id` | Ordering keys |
| Параллелизм | Между группами | Между сессиями | Между ключами |
| Цена | Ниже пропускная способность, чем у standard | Тариф Standard+ | Ниже пропускная способность на ключ, один регион |

Во всех трёх облаках модель одинакова и совпадает с Kafka: **порядок внутри ключа, параллелизм между ключами**.

## 10.4 Дедупликация и exactly-once

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Дедупликация публикаций | SQS FIFO: `MessageDeduplicationId`, окно 5 мин | Duplicate detection по `message_id`, окно настраивается | Нет |
| Защита от повторной доставки | FIFO: сообщение группы не выдаётся параллельно | Peek-lock, сессии | Exactly-once delivery на pull-подписке |
| Атомарность внутри брокера | — | Транзакции в пределах namespace | — |

Ни один сервис не даёт exactly-once **с внешней базой данных**. Везде нужна идемпотентность консьюмера (модуль 11).

## 10.5 DLQ и redrive

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Как включить | `RedrivePolicy` с `maxReceiveCount` | Встроена: подочередь `$DeadLetterQueue`, `MaxDeliveryCount` (10) | Dead letter topic + `max-delivery-attempts` (5–100) |
| Причина в сообщении | Нет (только атрибуты) | `dead_letter_reason`, `dead_letter_error_description` | Атрибуты с исходной подпиской и числом попыток |
| Явная отправка в DLQ консьюмером | Нет (только через лимит попыток или свою публикацию) | `dead_letter_message` | Нет (ack + публикация в свой топик) |
| Вернуть из DLQ | Redrive (`StartMessageMoveTask`) | Прочитать DLQ и переотправить | Подписка на DLQ-топик и переотправка |

## 10.6 Задержки, ретраи и расписания

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Задержка сообщения | `DelaySeconds` до 15 мин | Scheduled messages (любое время) | Нет в Pub/Sub; Cloud Tasks `schedule_time` |
| Ретрай с backoff | `ChangeMessageVisibility` с растущей задержкой | `abandon` + свой backoff или scheduled resend | Retry policy подписки (10–600 с) |
| Расписание | EventBridge Scheduler | Scheduled messages, Logic Apps | Cloud Scheduler, Cloud Tasks |

## 10.7 Повторное чтение

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Очередь | Нет | Нет | Seek по времени или snapshot, retention топика до 31 дня |
| Шина событий | EventBridge archive & replay | — | — |
| Лог | Kinesis, MSK | Event Hubs | Managed Kafka |

## 10.8 Большие сообщения

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Лимит | SQS 1 МиБ, SNS 256 КиБ (до 1 МиБ с атрибутом), EventBridge 1 МБ | 256 КиБ Standard, до 100 МБ Premium | 10 МБ |
| Больше лимита | Claim check через S3 (Extended Client Library) | Claim check через Blob Storage | Claim check через Cloud Storage |

**Claim check** — универсальный паттерн: большой объект кладётся в объектное хранилище, а в сообщение — ссылка и метаданные. Даже когда лимит позволяет, мегабайты в каждом сообщении дорого стоят (тарификация по объёму) и медленнее обрабатываются.

## 10.9 Request-reply

Облачные брокеры плохо подходят для синхронного RPC: задержка HTTPS-вызовов, нет аналога direct reply-to (кроме сессий Service Bus, которые можно использовать для ответов). Для запрос-ответа между сервисами используй HTTP или gRPC, а брокер — для асинхронных событий и задач.

---

# Модуль 11. Гарантии доставки, идемпотентность и outbox

## 11.1 At-least-once — это норма

Почти все облачные брокеры по умолчанию дают **at-least-once**:

| Сервис | Почему возможны дубли |
|---|---|
| SQS standard | Распределённое хранение: редкие повторные доставки заложены в дизайн |
| SQS FIFO | Повтор после истечения visibility timeout, если консьюмер не успел удалить |
| SNS, EventBridge, Event Grid | Повторные попытки доставки на цель |
| Service Bus | Lock истёк раньше `complete`; `complete` не дошёл до сервиса |
| Pub/Sub | Ack не дошёл или пришёл после deadline (без exactly-once) |
| Lambda, Functions, Cloud Run | Повторный вызов при ошибке или таймауте |

Вывод прежний: **консьюмер должен быть идемпотентным**. Облако этого не отменяет.

## 11.2 Идемпотентный консьюмер в облаке

Хранилище обработанных id выбирают в том же облаке, с условной записью:

| Облако | Хранилище | Условная запись |
|---|---|---|
| AWS | DynamoDB | `PutItem` с `ConditionExpression="attribute_not_exists(pk)"` |
| Azure | Cosmos DB | `create_item` (конфликт по id) |
| Google Cloud | Firestore / Spanner | `create` документа (ошибка, если существует) / вставка в транзакции |
| Любое | PostgreSQL | `INSERT ... ON CONFLICT DO NOTHING` в транзакции с бизнес-изменением |

```python
import boto3
from botocore.exceptions import ClientError

table = boto3.resource("dynamodb").Table("processed-messages")

def handle_once(message_id: str, work) -> bool:
    try:
        table.put_item(
            Item={"pk": message_id, "ttl": expires_in_days(7)},   # TTL DynamoDB чистит старые записи
            ConditionExpression="attribute_not_exists(pk)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False                  # уже обработано: просто подтверждаем сообщение
        raise
    work()
    return True
```

Внимание к порядку: если `work()` упадёт после записи id, повтор будет пропущен. Надёжные варианты:

- **бизнес-изменение и отметка — в одной транзакции** (одна база, транзакция или `TransactWriteItems` в DynamoDB);
- **естественная идемпотентность**: `UPSERT` по ключу, условное обновление по версии;
- отметка с состоянием `in_progress`/`done` и повтором для зависших.

Какой id использовать:

| Источник | Идентификатор |
|---|---|
| Свой producer | Свой стабильный `event_id` в теле или атрибутах — лучший вариант |
| SQS | `MessageId` (меняется при повторной отправке producer'ом) |
| Service Bus | `message_id` (задаёт отправитель) |
| Pub/Sub | `message_id` (назначает сервис при публикации) |
| EventBridge, Event Grid, Eventarc | `id` события (CloudEvents `id` + `source`) |

Идентификатор, назначенный **брокером**, не защищает от повторной **публикации**: если producer отправил событие дважды, у него будут два разных id. Поэтому бизнес-id события должен задавать producer.

## 11.3 Transactional outbox в облаке

Проблема двойной записи та же, что и везде (курсы по Kafka и RabbitMQ): записать в базу и опубликовать событие атомарно нельзя. Решение — outbox, и у облаков есть удобные способы доставлять его без своего опросчика:

| Облако | База → поток изменений → брокер |
|---|---|
| AWS | DynamoDB: запись заказа и события в одной транзакции → **DynamoDB Streams** → **EventBridge Pipes** → EventBridge или SQS |
| AWS | Aurora/RDS PostgreSQL: таблица outbox → Debezium или свой ретранслятор → SQS/SNS/EventBridge |
| Azure | Cosmos DB: документ заказа и события в одном транзакционном батче → **Change Feed** → Azure Function → Service Bus |
| Google Cloud | Spanner: запись в одной транзакции → **change streams** → Dataflow → Pub/Sub |
| Любое | PostgreSQL outbox → **Debezium Server** (поддерживает Pub/Sub, Kinesis, Event Hubs и другие приёмники) |

```
+----------- одна транзакция базы ------------+
| заказ          +  событие OrderCreated        |
+---------------------------------------------+
            |
            v  поток изменений базы (Streams / Change Feed / change streams / WAL)
   ретранслятор (Pipes / Function / Dataflow / Debezium)
            |
            v
   EventBridge / Service Bus / Pub/Sub  ->  консьюмеры (идемпотентные)
```

Ретранслятор может доставить событие дважды, поэтому в событии всегда есть стабильный `event_id`, а консьюмеры дедуплицируют по нему.

## 11.4 Частичные ошибки батчей — главная облачная ловушка

Облачные API почти всегда батчевые, и почти везде батч может **частично** не пройти:

| API | Как узнать о частичной ошибке |
|---|---|
| SQS `SendMessageBatch`, `DeleteMessageBatch` | Поле `Failed` в ответе |
| SNS `PublishBatch` | Поле `Failed` |
| EventBridge `PutEvents` | `FailedEntryCount` и `ErrorCode` в `Entries` |
| Kinesis `PutRecords` | `FailedRecordCount` |
| Pub/Sub `publish` | Отдельный future на каждое сообщение |
| Lambda + SQS | Ответ `batchItemFailures` (модуль 12) |

HTTP 200 **не означает**, что все сообщения приняты. Это частая причина «таинственных» потерь.

## 11.5 Выбор гарантий

| Задача | Рекомендация |
|---|---|
| Метрики, логи | Standard-сервисы, потеря редких сообщений допустима |
| Уведомления | At-least-once + дедупликация по id события |
| Бизнес-события | Outbox + at-least-once + идемпотентный консьюмер + DLQ с алертом |
| Строгий порядок по сущности | SQS FIFO / Service Bus sessions / Pub/Sub ordering keys |
| Деньги | Всё вышеперечисленное + транзакционная отметка обработки + сверки |

### Вопросы для самопроверки

1. Почему идентификатор, назначенный брокером, не защищает от повторной публикации?
2. Чем опасна схема «записать id обработанного сообщения, потом выполнить работу»?
3. Как реализовать outbox в AWS без собственного опросчика?
4. Почему HTTP 200 от батчевого API не гарантирует, что все сообщения приняты?

---

# Модуль 12. Serverless-консьюмеры: Lambda, Azure Functions, Cloud Run

## 12.1 Как функции читают очереди

В serverless-архитектуре консьюмер — не долгоживущий процесс, а функция, которую вызывает платформа:

```
очередь / подписка --> платформа (event source mapping, trigger, push) --> функция
                          |  забирает пачку сообщений
                          |  вызывает функцию
                          |  по результату: удаляет / возвращает сообщения
```

Платформа берёт на себя опрос очереди и масштабирование, но **семантика подтверждения** теперь зависит от того, чем закончилась функция. Главный вопрос каждой интеграции: **что происходит с пачкой, если упало одно сообщение из десяти?**

## 12.2 AWS Lambda + SQS

**Event source mapping** опрашивает SQS и вызывает функцию с пачкой сообщений:

| Параметр | Смысл |
|---|---|
| `BatchSize` | Сообщений в пачке (для standard — до 10 000 с окном батчинга, для FIFO — до 10) |
| `MaximumBatchingWindowInSeconds` | Сколько ждать, чтобы набрать пачку (до 300 с) |
| `MaximumConcurrency` | Потолок одновременных вызовов для этой очереди: защищает базу от всплеска |
| `FunctionResponseTypes: ReportBatchItemFailures` | Разрешить функции сообщать, какие сообщения пачки не обработались |

**Без** `ReportBatchItemFailures` ошибка функции возвращает в очередь **всю пачку**: девять успешно обработанных сообщений будут обработаны снова. С ним функция возвращает список неудавшихся:

```python
import json

def handler(event, context):
    failures = []
    for record in event["Records"]:
        try:
            process(json.loads(record["body"]))          # идемпотентно
        except Exception:
            failures.append({"itemIdentifier": record["messageId"]})
    # успешные сообщения Lambda удалит, неудавшиеся вернутся в очередь
    return {"batchItemFailures": failures}
```

Правила:

- visibility timeout очереди — **не меньше шести таймаутов функции**: иначе сообщение станет видимым, пока функция ещё работает, и его обработают дважды;
- DLQ настраивается **на очереди** (`RedrivePolicy`), а не на функции: для SQS-триггера destinations и DLQ функции не используются;
- `MaximumConcurrency` — главный инструмент, чтобы тысяча параллельных функций не положила базу данных;
- для FIFO-очереди при частичной ошибке нужно вернуть неудавшееся сообщение **и все последующие** в той же группе, иначе нарушится порядок.

AWS обнаруживает **рекурсивные циклы** (функция пишет в очередь, которая вызывает эту же функцию) и останавливает их после примерно 16 повторений — это защита от бесконечного счёта, а не повод строить такие цепочки.

## 12.3 AWS Lambda + SNS и EventBridge

SNS и EventBridge вызывают Lambda **асинхронно**: при ошибке Lambda сама повторяет вызов (по умолчанию дважды), а потом отправляет событие в **destination** или DLQ функции. Надёжнее ставить между ними очередь: SNS/EventBridge → SQS → Lambda. Тогда у тебя есть буфер, контроль параллелизма и `ReportBatchItemFailures`.

## 12.4 Azure Functions + Service Bus

Триггер Service Bus получает сообщения в режиме peek-lock и по умолчанию **сам завершает** их: успешное выполнение — `complete`, исключение — `abandon` (после `MaxDeliveryCount` сообщение уйдёт в DLQ).

```python
import json
import azure.functions as func

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="msg", queue_name="tasks", connection="ServiceBusConnection")
def process_task(msg: func.ServiceBusMessage):
    payload = json.loads(msg.get_body().decode())
    process(payload)                 # исключение -> abandon -> повтор -> DLQ после MaxDeliveryCount
```

Важные настройки в `host.json`:

| Настройка | Смысл |
|---|---|
| `maxConcurrentCalls` | Параллельных сообщений на экземпляр |
| `maxAutoLockRenewalDuration` | Сколько продлевать блокировку при долгой обработке |
| `autoCompleteMessages` | Завершать автоматически (выключи, если завершаешь вручную) |
| `isSessionsEnabled` | Работа с сессиями, порядок по сущности |

Подключение — через **managed identity** (`ServiceBusConnection__fullyQualifiedNamespace`), без строки подключения с ключом.

## 12.5 Cloud Run + Pub/Sub

Два способа:

**Push-подписка** (или триггер Eventarc) вызывает сервис Cloud Run по HTTPS:

```python
import base64, json
from flask import Flask, request

app = Flask(__name__)

@app.post("/")
def pubsub_push():
    envelope = request.get_json()
    msg = envelope["message"]
    data = json.loads(base64.b64decode(msg["data"]))
    try:
        process(data)                         # идемпотентно по msg["messageId"] или своему id
    except TemporaryError:
        return ("retry later", 503)           # не 2xx -> Pub/Sub повторит с backoff
    return ("", 204)                          # 2xx -> ack
```

- ack deadline push-подписки (до 600 с) должен покрывать время обработки, а таймаут запроса Cloud Run — быть не меньше;
- на невалидное сообщение возвращай 2xx (и логируй/отправляй в свою DLQ), иначе Pub/Sub будет повторять до dead letter topic;
- эндпоинт защищай OIDC-токеном: разрешай вызов только сервисному аккаунту Pub/Sub.

**Pull в долгоживущем сервисе** (например, в worker pool Cloud Run или в GKE) — обычный streaming pull из модуля 8: полный контроль темпа через `FlowControl`.

## 12.6 Общие правила serverless-консьюмеров

1. **Частичные ошибки пачки** — `batchItemFailures` в Lambda, отдельное завершение сообщений в Functions, по одному сообщению в push.
2. **Ограничение параллелизма** — иначе автомасштабирование функций положит базу или внешнее API.
3. **Таймауты согласованы**: visibility timeout / lock duration / ack deadline ≥ время работы функции с запасом.
4. **DLQ на стороне брокера** и алерт на неё.
5. **Идемпотентность** — платформы повторяют вызовы.
6. **Холодный старт** добавляет задержку: для строгих SLA по задержке нужны заранее прогретые экземпляры.

### Вопросы для самопроверки

1. Что произойдёт с успешно обработанными сообщениями пачки, если Lambda упадёт без `ReportBatchItemFailures`?
2. Почему visibility timeout должен быть в несколько раз больше таймаута функции?
3. Что вернуть из push-обработчика Cloud Run для невалидного сообщения, и почему?
4. Зачем ограничивать параллелизм serverless-консьюмера?

---

# Модуль 13. Безопасность: IAM, шифрование, приватный доступ

## 13.1 Принципы

- **Никаких долгоживущих ключей в коде**: роли и managed identity вместо access keys и строк подключения.
- **Минимальные права на конкретный ресурс**: «может отправлять в очередь `orders`», а не «полный доступ к SQS».
- **Отдельные учётные записи** для producer'а и консьюмера.
- **Шифрование ключами, которыми управляешь ты**, если этого требуют комплаенс или изоляция.
- **Приватный доступ**: трафик к брокеру не выходит в интернет.

## 13.2 AWS

IAM-политика консьюмера — только на свою очередь:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"],
    "Resource": "arn:aws:sqs:eu-central-1:123456789012:billing"
  }]
}
```

- **Resource policies** на очереди и топике разрешают доступ другим сервисам (SNS → SQS, EventBridge → SQS) и другим аккаунтам.
- **Шифрование**: SSE-SQS (ключ AWS) включено по умолчанию; **SSE-KMS** с собственным ключом — для контроля и аудита. Ловушка: если очередь зашифрована собственным ключом KMS, то **SNS и EventBridge тоже нужны права на этот ключ** (`kms:GenerateDataKey`, `kms:Decrypt`) в политике ключа — иначе доставка молча не работает.
- **VPC endpoints (PrivateLink)** для SQS, SNS и EventBridge: трафик из VPC не идёт через интернет; политика эндпоинта дополнительно ограничивает, к каким ресурсам можно обращаться.

## 13.3 Azure

- **Entra ID и RBAC** вместо SAS-ключей: встроенные роли **Azure Service Bus Data Sender**, **Data Receiver**, **Data Owner**, назначаемые на namespace, очередь или топик.
- **Managed identity** для приложений и функций: ни одного секрета в конфигурации.
- **Отключи локальную аутентификацию** (`disableLocalAuth`), чтобы SAS-ключи нельзя было использовать вообще.
- **Private endpoints** и отключение публичного доступа (Premium для Service Bus).
- **Customer-managed keys** из Key Vault (Premium).

```bash
az role assignment create --assignee $APP_IDENTITY_ID \
  --role "Azure Service Bus Data Receiver" \
  --scope $(az servicebus queue show -g shop-rg --namespace-name shop-bus -n tasks --query id -o tsv)
```

## 13.4 Google Cloud

- **Роли на уровне топика и подписки**: `roles/pubsub.publisher` на топик для producer'а, `roles/pubsub.subscriber` на подписку для консьюмера.
- **Сервисный агент Pub/Sub** (`service-<номер-проекта>@gcp-sa-pubsub.iam.gserviceaccount.com`) нуждается в правах для dead lettering, push с OIDC и CMEK — это частый источник «ничего не работает».
- **CMEK** — шифрование ключами Cloud KMS.
- **VPC Service Controls** — периметр, не позволяющий выносить данные за пределы разрешённых проектов.
- **Message storage policy** — в каких регионах разрешено хранить сообщения топика (требования к размещению данных).
- **Push-эндпоинты** проверяют OIDC-токен сервисного аккаунта.

```bash
gcloud pubsub subscriptions add-iam-policy-binding billing \
  --member=serviceAccount:billing-sa@my-project.iam.gserviceaccount.com \
  --role=roles/pubsub.subscriber
```

## 13.5 Чеклист безопасности

- [ ] Нет access keys, SAS-ключей и JSON-ключей сервисных аккаунтов в коде и конфигурации
- [ ] Роли и managed identity для всех приложений и функций
- [ ] Права на конкретные очереди, топики и подписки, отдельно на отправку и получение
- [ ] Resource policies разрешают только нужные сервисы и аккаунты
- [ ] Шифрование собственными ключами там, где требует комплаенс, с правами на ключ для сервисов-издателей
- [ ] Приватный доступ (VPC endpoints, private endpoints, VPC Service Controls)
- [ ] Локальная аутентификация по ключам отключена (Azure)
- [ ] Push-эндпоинты проверяют подлинность отправителя
- [ ] Аудит доступа: CloudTrail, Azure Activity Log, Cloud Audit Logs
- [ ] Чувствительные данные в сообщениях зашифрованы или вынесены из них

---

# Модуль 14. Мониторинг и алерты

## 14.1 Самая важная метрика — возраст самого старого сообщения

Длина очереди сама по себе мало что говорит: 10 000 сообщений могут быть нормальным буфером. А вот **возраст самого старого необработанного сообщения** напрямую показывает, насколько система отстаёт, и хорошо ложится на SLA («события обрабатываются не позже чем за 5 минут»).

| | AWS SQS | Azure Service Bus | Google Pub/Sub |
|---|---|---|---|
| Возраст самого старого | `ApproximateAgeOfOldestMessage` | Нет прямой метрики (считай по `ActiveMessages` и скорости или читай peek) | `subscription/oldest_unacked_message_age` |
| Длина | `ApproximateNumberOfMessagesVisible` | `ActiveMessages` | `subscription/num_undelivered_messages` |
| В обработке | `ApproximateNumberOfMessagesNotVisible` | — | — |
| DLQ | `ApproximateNumberOfMessagesVisible` у DLQ | `DeadletteredMessages` | `num_undelivered_messages` у подписки DLQ-топика, `dead_letter_message_count` |
| Входящий поток | `NumberOfMessagesSent` | `IncomingMessages` | `topic/send_request_count` |
| Ошибки и троттлинг | `NumberOfEmptyReceives` (стоимость) | `ServerErrors`, `ThrottledRequests` | `push_request_count` по кодам ответа |

Для шин событий: EventBridge — `FailedInvocations`, `DeadLetterInvocations`, `ThrottledRules`; Event Grid — `DeliveryFailedCount`, `DeadLetteredCount`; SNS — `NumberOfNotificationsFailed`.

## 14.2 Что алертить

| Алерт | Условие | Почему важно |
|---|---|---|
| **Отставание** | Возраст самого старого сообщения > SLA | Обработка не успевает или стоит |
| **Сообщения в DLQ** | Любые | Бизнес-логика падает |
| **Растущая очередь без обработки** | Длина растёт, а удалений (`NumberOfMessagesDeleted`, `CompleteMessage`, acks) нет | Консьюмеры не работают |
| **Ошибки доставки шины** | `FailedInvocations`, `DeliveryFailedCount` > 0 | Цели недоступны или нет прав |
| **Push-ошибки** | Доля не-2xx ответов растёт | Обработчик падает |
| **Троттлинг** | `ThrottledRequests`, отказы по квотам | Упёрлись в лимиты тарифа или квоты |
| **Расходы** | Бюджет превышен или аномалия | Бесконечные ретраи, циклы, пустые опросы |

## 14.3 Трассировка

Сквозная трассировка через брокер требует **передавать контекст в атрибутах сообщения** (заголовок W3C `traceparent`):

- AWS: X-Ray и OpenTelemetry, контекст — в атрибуте `AWSTraceHeader` SQS или в своих атрибутах;
- Azure: Application Insights и OpenTelemetry; SDK Service Bus передаёт `Diagnostic-Id`/`traceparent`;
- Google Cloud: Cloud Trace и OpenTelemetry; клиентские библиотеки Pub/Sub умеют передавать контекст в атрибутах.

Без этого трассировка обрывается на брокере, и найти, какой запрос породил сломанное сообщение, очень трудно.

## 14.4 Логи и аудит

- Логи консьюмеров должны содержать id сообщения, id события и число попыток.
- Аудит управляющих операций (кто удалил очередь, кто поменял политику) — CloudTrail, Azure Activity Log, Cloud Audit Logs.
- Логи доставки: SNS delivery status logging, диагностические логи Service Bus и Event Grid, логи push-подписок Pub/Sub.

---

# Модуль 15. Стоимость, квоты и лимиты

## 15.1 Как считают деньги

Цены меняются, поэтому здесь — **модели оплаты**; конкретные числа бери из калькуляторов облаков.

| Сервис | За что платишь | Что важно знать |
|---|---|---|
| SQS | Запросы API | Запрос до 64 КБ = 1 единица; **батч из 10 сообщений = 1 запрос**; пустые `ReceiveMessage` тоже платные; есть бесплатный уровень |
| SNS | Публикации + доставки по типу подписки | Доставки в SQS и Lambda дешевле, чем SMS и email |
| EventBridge | Опубликованные события, вызовы API destinations, архив, Scheduler | События сервисов AWS в default-шину обычно бесплатны |
| Kinesis | Шард-часы или объём (on-demand), хранение сверх 24 ч, enhanced fan-out | |
| Service Bus | Basic/Standard — операции (у Standard есть базовая плата); Premium — часы messaging units | Premium — фиксированная стоимость за выделенные ресурсы |
| Event Hubs | Throughput/processing units, события, Capture | |
| Pub/Sub | **Объём данных** публикации и доставки, хранение retention, межрегиональный трафик | Минимальный объём одного запроса тарифицируется как 1 КБ: маленькие сообщения выгоднее батчить |

## 15.2 Что раздувает счёт

| Причина | Как исправить |
|---|---|
| Short polling SQS: тысячи пустых запросов в минуту | Long polling 20 с |
| Отправка по одному сообщению | Батчи |
| «Ядовитое» сообщение без DLQ крутится днями | DLQ с разумным лимитом попыток |
| Рекурсия: функция публикует событие, которое её же вызывает | Разные топики и очереди, защита от циклов |
| Fan-out на десятки подписок с большими сообщениями | Фильтры подписок, claim check |
| Retention и retain acked messages в Pub/Sub | Хранить столько, сколько реально нужно |
| Межрегиональный трафик | Консьюмеры в регионе брокера |
| Premium/выделенные ресурсы, простаивающие ночью | Правильный тариф под профиль нагрузки |

## 15.3 Облако или свой кластер: грубая оценка

```
облачный сервис:  стоимость ≈ число операций (или объём) × цена за единицу
свой кластер:     стоимость ≈ серверы × часы + диски + трафик + время инженеров на эксплуатацию
```

- При **малом и неравномерном** потоке облачный сервис почти всегда дешевле: не нужно держать кластер под пик и платить инженерам.
- При **большом постоянном** потоке (сотни миллионов и миллиарды сообщений в месяц) оплата за операции может превысить стоимость своего кластера Kafka, RabbitMQ или NATS в разы. Но учитывай стоимость эксплуатации: зарплата инженеров обычно больше счёта за серверы.
- Промежуточный вариант — управляемые Kafka и RabbitMQ (MSK, Amazon MQ, Managed Kafka): платишь за инстансы, а не за операции.

## 15.4 Квоты, которые стоит знать

| Квота | Где встречается |
|---|---|
| Сообщений в обработке (in-flight) на очередь | SQS: порядка 120 000; при превышении `ReceiveMessage` перестаёт выдавать сообщения |
| Пропускная способность FIFO | SQS FIFO: включай режим high throughput для больших потоков |
| Запросы `PutEvents` в секунду | EventBridge: мягкая квота на аккаунт и регион, повышается через заявку |
| Троттлинг на общих ресурсах | Service Bus Standard: при превышении — ошибки троттлинга, нужен Premium или шардирование по namespace |
| Пропускная способность и размер запросов | Pub/Sub: региональные квоты на проект |
| Число подписок, правил, топиков | Все сервисы: лимиты на ресурс и аккаунт |

Мягкие квоты повышаются заявкой — **заранее**, до запуска, а не в день распродажи.

## 15.5 Защита от неожиданного счёта

- **Бюджеты и алерты** на расходы в каждом облаке (AWS Budgets, Azure Cost Management, Google Cloud Budgets).
- Алерт на **аномалии** расходов (у всех трёх облаков есть детекторы аномалий).
- Теги или метки на ресурсах по командам и сервисам: понятно, кто сколько тратит.
- Учебные ресурсы — в отдельном аккаунте или проекте, удаляются после занятия.

---

# Модуль 16. Инфраструктура как код: Terraform

## 16.1 Зачем

Очереди, топики, подписки, DLQ, политики и права — это инфраструктура. Созданные руками в консоли, они расходятся между окружениями, а критичные настройки (DLQ, права сервисного агента, политика очереди) забываются. Опиши их в Terraform и держи в Git рядом с кодом.

## 16.2 AWS: SQS с DLQ и подпиской на SNS

```hcl
resource "aws_sqs_queue" "billing_dlq" {
  name                      = "billing-dlq"
  message_retention_seconds = 1209600 # 14 дней: больше, чем у основной очереди
}

resource "aws_sqs_queue" "billing" {
  name                       = "billing"
  visibility_timeout_seconds = 60
  receive_wait_time_seconds  = 20 # long polling по умолчанию
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.billing_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sns_topic" "orders" {
  name = "orders"
}

resource "aws_sns_topic_subscription" "billing" {
  topic_arn            = aws_sns_topic.orders.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.billing.arn
  raw_message_delivery = true
  filter_policy        = jsonencode({ event_type = ["OrderCreated"] })
}

# без этой политики SNS не сможет писать в очередь
resource "aws_sqs_queue_policy" "billing" {
  queue_url = aws_sqs_queue.billing.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.billing.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_sns_topic.orders.arn } }
    }]
  })
}
```

## 16.3 Azure: Service Bus

```hcl
resource "azurerm_servicebus_namespace" "shop" {
  name                = "shop-bus"
  location            = azurerm_resource_group.shop.location
  resource_group_name = azurerm_resource_group.shop.name
  sku                 = "Standard"
  local_auth_enabled  = false # только Entra ID
}

resource "azurerm_servicebus_queue" "tasks" {
  name                                 = "tasks"
  namespace_id                         = azurerm_servicebus_namespace.shop.id
  max_delivery_count                   = 5
  lock_duration                        = "PT1M"
  requires_duplicate_detection         = true
  dead_lettering_on_message_expiration = true
}

resource "azurerm_servicebus_topic" "orders" {
  name         = "orders"
  namespace_id = azurerm_servicebus_namespace.shop.id
}

resource "azurerm_servicebus_subscription" "billing" {
  name               = "billing"
  topic_id           = azurerm_servicebus_topic.orders.id
  max_delivery_count = 5
}

resource "azurerm_servicebus_subscription_rule" "billing_created" {
  name            = "created-only"
  subscription_id = azurerm_servicebus_subscription.billing.id
  filter_type     = "SqlFilter"
  sql_filter      = "EventType = 'OrderCreated'"
}
```

## 16.4 Google Cloud: Pub/Sub с dead letter topic

```hcl
data "google_project" "current" {}

locals {
  pubsub_agent = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_topic" "orders" {
  name = "orders"
}

resource "google_pubsub_topic" "orders_dlq" {
  name = "orders-dlq"
}

resource "google_pubsub_subscription" "orders_dlq" {
  name  = "orders-dlq-sub" # без подписки сообщения в DLQ-топике никто не сохранит
  topic = google_pubsub_topic.orders_dlq.id
}

resource "google_pubsub_subscription" "billing" {
  name                 = "billing"
  topic                = google_pubsub_topic.orders.id
  ack_deadline_seconds = 60
  filter               = "attributes.event_type = \"OrderCreated\""

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.orders_dlq.id
    max_delivery_attempts = 5
  }
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# права сервисного агента Pub/Sub: без них dead lettering не работает
resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  topic  = google_pubsub_topic.orders_dlq.id
  role   = "roles/pubsub.publisher"
  member = local.pubsub_agent
}

resource "google_pubsub_subscription_iam_member" "billing_subscriber" {
  subscription = google_pubsub_subscription.billing.id
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_agent
}
```

## 16.5 Практика IaC для брокеров

- Модуль Terraform «очередь с DLQ и алертами» — один раз, переиспользуется всеми сервисами: никто не забудет DLQ.
- Алерты (CloudWatch alarms, Azure Monitor alerts, Cloud Monitoring policies) — в том же модуле.
- Права приложений — рядом с ресурсом, которому они нужны.
- План (`terraform plan`) — в CI при каждом изменении; удаление очереди с сообщениями — только осознанно.

---

# Модуль 17. Переносимость, CloudEvents и миграции

## 17.1 Цена привязки к облаку

Код, который вызывает `sqs.receive_message` или `ServiceBusReceiver`, привязан к облаку. Это не всегда плохо: переносимость тоже стоит денег. Но решение нужно принимать осознанно.

| Уровень переносимости | Как | Цена |
|---|---|---|
| Нет | Прямые вызовы SDK облака везде | Переезд = переписывание |
| Интерфейс в коде | Свой интерфейс `Publisher`/`Consumer`, адаптеры под облака (ports and adapters) | Немного кода, теряются уникальные возможности |
| Библиотека-абстракция | Dapr pub/sub, Spring Cloud Stream, MassTransit (.NET), Watermill и Go CDK (`gocloud.dev/pubsub`) | Зависимость от библиотеки, «наименьший общий знаменатель» |
| Открытый протокол | Kafka (MSK, Event Hubs, Managed Kafka), AMQP 1.0 (Service Bus, RabbitMQ), MQTT | Меньше выбор сервисов |

**Наименьший общий знаменатель** — главный минус абстракций: сессии Service Bus, фильтры SNS и exactly-once Pub/Sub выглядят и работают по-разному, и универсальный интерфейс обычно их не покрывает.

## 17.2 CloudEvents

**CloudEvents** — открытая спецификация CNCF для **описания** событий, одинакового в разных системах:

```json
{
  "specversion": "1.0",
  "id": "5f1c2a7e-9b1d-4c1e-8a4e-0c7b2d9f1a11",
  "source": "/shop/orders",
  "type": "com.shop.order.created",
  "subject": "order-123",
  "time": "2026-09-15T10:00:00Z",
  "datacontenttype": "application/json",
  "data": {"order_id": "order-123", "amount": 4990}
}
```

- Обязательные поля: `specversion`, `id`, `source`, `type`. Пара `source` + `id` уникально идентифицирует событие — готовый ключ для идемпотентности.
- Есть **привязки к протоколам**: HTTP, Kafka, AMQP, MQTT (атрибуты — в заголовках, данные — в теле).
- **Event Grid** и **Eventarc** работают с CloudEvents нативно; EventBridge использует свой формат, но преобразуется через input transformer.

Даже если ты не планируешь переезжать, CloudEvents — хороший стандарт конверта событий: одинаковые поля во всех сервисах и облаках, готовые SDK.

## 17.3 Миграции

**С RabbitMQ в облако:**

| RabbitMQ | AWS | Azure | Google Cloud |
|---|---|---|---|
| Очередь (quorum) | SQS | Service Bus queue | Pub/Sub подписка |
| Fanout/topic exchange + очереди | SNS + SQS (или EventBridge) | Service Bus topic + subscriptions | Pub/Sub топик + подписки |
| Ack / nack / reject | Delete / visibility / DLQ по лимиту | complete / abandon / dead-letter | ack / nack / DLQ по лимиту |
| DLX + delivery-limit | RedrivePolicy | MaxDeliveryCount + $DeadLetterQueue | Dead letter topic |
| Single active consumer | SQS FIFO с одной группой | Сессии | Ordering key |
| Приоритеты | Отдельные очереди | Отдельные очереди | Отдельные подписки |
| RPC через direct reply-to | Не подходит, HTTP/gRPC | Сессии (ограниченно) | Не подходит, HTTP/gRPC |

Или без переписывания: **Amazon MQ for RabbitMQ**.

**С Kafka в облако:** MSK, Event Hubs (Kafka-эндпоинт) или Managed Service for Apache Kafka — код не меняется, меняются адрес и аутентификация.

**Между облаками:** период **двойной публикации** (событие пишется в оба брокера), консьюмеры переключаются по одному, затем старый брокер выключается. Мост на время миграции — функция или Shovel/Debezium, перекладывающая события из одного сервиса в другой. События с CloudEvents-конвертом и стабильным `id` переносить намного проще.

---

# Модуль 18. Облачный сервис или свой Kafka, RabbitMQ, NATS

## 18.1 Таблица решения

| Критерий | Облачный сервис | Управляемый Kafka/RabbitMQ | Свой кластер |
|---|---|---|---|
| Эксплуатация | Почти нет | Частично (размеры, версии) | Полностью твоя |
| Переносимость | Низкая | Высокая (протокол) | Высокая |
| Возможности | То, что даёт сервис | Полные возможности продукта | Полные |
| Задержка | Десятки миллисекунд (HTTPS) | Единицы миллисекунд | Единицы миллисекунд и ниже |
| Стоимость при малом потоке | Низкая | Средняя (инстансы 24/7) | Средняя + инженеры |
| Стоимость при огромном постоянном потоке | Может быть высокой | Средняя | Низкая по инфраструктуре, высокая по людям |
| Интеграции с облаком | Отличные | Хорошие | Своими руками |
| Мультиоблако и on-premises | Нет | Частично | Да |

## 18.2 Практические рекомендации

- **Стартап или небольшая команда в одном облаке** — облачные сервисы: SQS/SNS/EventBridge, Service Bus/Event Grid, Pub/Sub. Эксплуатация кластеров не должна съедать время продукта.
- **Нужен лог и экосистема Kafka** — управляемый Kafka (MSK, Event Hubs, Managed Kafka), свой кластер — только при серьёзных причинах.
- **Сложная маршрутизация, приоритеты, RPC, переносимость** — RabbitMQ (Amazon MQ или свой) или NATS.
- **Низкая задержка, edge, мультиоблако** — NATS.
- **Смешанная система** — нормальна: Pub/Sub для событий в Google Cloud, Kafka для аналитического потока, внутренний NATS для RPC. Главное — понимать гарантии каждого звена.

Парные курсы серии разбирают Kafka, RabbitMQ и NATS в той же глубине, что этот курс — облачные сервисы.

---

# Модуль 19. Итоговый проект: интернет-магазин в трёх облаках

## 19.1 Что строим

Одна и та же система, которую можно собрать в любом из трёх облаков:

```
  HTTP  --> Order API --> база + outbox --> поток изменений --> [шина / топик событий заказов]
                                                                       |
                  +----------------------------+-----------------------+-------------------+
                  v                            v                       v                   v
          [очередь billing]            [очередь stock]         [очередь notify]     архив / аналитика
          идемпотентно,                порядок по заказу       serverless-функция
          DLQ + алерт                  (FIFO / сессии /        с частичными
                                        ordering keys)          ошибками пачки
  Плюс:
  - напоминание об оплате через 3 дня (Scheduler / scheduled message / Cloud Tasks)
  - реакция на загрузку чека в хранилище (EventBridge / Event Grid / Eventarc)
  - Terraform для всей топологии, алерты, бюджет
```

## 19.2 Соответствие компонентов

| Компонент | AWS | Azure | Google Cloud |
|---|---|---|---|
| Outbox | DynamoDB Streams → EventBridge Pipes | Cosmos DB Change Feed → Function | Spanner change streams или Debezium Server → Pub/Sub |
| Топик событий | SNS (или EventBridge) | Service Bus topic | Pub/Sub топик |
| Очередь сервиса | SQS | Service Bus subscription | Pub/Sub подписка |
| Порядок по заказу | SQS FIFO + SNS FIFO | Сессии | Ordering keys |
| Serverless-консьюмер | Lambda + `batchItemFailures` | Functions + Service Bus trigger | Cloud Run + push |
| Отложенное напоминание | EventBridge Scheduler | Scheduled message | Cloud Tasks |
| Событие загрузки файла | EventBridge (S3) | Event Grid (Blob) | Eventarc (Cloud Storage) |
| Идемпотентность | DynamoDB conditional put | Cosmos DB | Firestore или Spanner |
| Мониторинг | CloudWatch | Azure Monitor | Cloud Monitoring |

## 19.3 Требования

1. Событие заказа публикуется через outbox: **никакой двойной записи**.
2. Каждый сервис читает свою очередь или подписку со своей DLQ и алертом.
3. Billing идемпотентен по id события.
4. Stock обрабатывает события одного заказа строго по порядку.
5. Notifications — serverless, с корректной обработкой частичных ошибок пачки и ограничением параллелизма.
6. Напоминание об оплате приходит через 3 дня и отменяется, если заказ оплачен.
7. Загрузка чека в хранилище запускает обработку через шину событий.
8. Все ресурсы описаны в Terraform; права — минимальные, без ключей в коде.
9. Алерты на возраст самого старого сообщения, DLQ и расходы.
10. Локальные интеграционные тесты проходят на эмуляторах.

## 19.4 Этапы

| Этап | Что сделать |
|---|---|
| 1 | Локальная среда на эмуляторах, топология из кода |
| 2 | Order API + outbox + публикация событий |
| 3 | Billing с идемпотентностью и DLQ |
| 4 | Stock с порядком по заказу |
| 5 | Notifications на serverless с частичными ошибками |
| 6 | Отложенное напоминание |
| 7 | Событие загрузки файла через шину |
| 8 | Terraform, IAM, шифрование |
| 9 | Мониторинг, алерты, бюджет |
| 10 | Учения: отправить битое сообщение, уронить консьюмер посреди обработки, превысить параллелизм, проверить redrive из DLQ |

Если после учений очереди пусты, в DLQ только намеренно битые сообщения, в базе нет дублей, а бюджетные алерты молчат — курс пройден. Бонус: собери тот же проект во втором облаке и сравни.

---

# Шпаргалка CLI: aws, az, gcloud

```bash
# ---------- AWS: SQS ----------
aws sqs create-queue --queue-name tasks --attributes ReceiveMessageWaitTimeSeconds=20,VisibilityTimeout=60
aws sqs create-queue --queue-name orders.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true
aws sqs get-queue-url --queue-name tasks
aws sqs send-message --queue-url $Q --message-body '{"task":"resize"}'
aws sqs receive-message --queue-url $Q --max-number-of-messages 10 --wait-time-seconds 20
aws sqs delete-message --queue-url $Q --receipt-handle $RH
aws sqs change-message-visibility --queue-url $Q --receipt-handle $RH --visibility-timeout 120
aws sqs get-queue-attributes --queue-url $Q --attribute-names All
aws sqs set-queue-attributes --queue-url $Q --attributes file://redrive.json
aws sqs start-message-move-task --source-arn $DLQ_ARN            # redrive из DLQ
aws sqs purge-queue --queue-url $Q

# ---------- AWS: SNS и EventBridge ----------
aws sns create-topic --name orders
aws sns subscribe --topic-arn $T --protocol sqs --notification-endpoint $QUEUE_ARN --attributes RawMessageDelivery=true
aws sns set-subscription-attributes --subscription-arn $S --attribute-name FilterPolicy --attribute-value file://filter.json
aws sns publish --topic-arn $T --message '{"order_id":"order-1"}' --message-attributes file://attrs.json
aws events create-event-bus --name shop
aws events put-rule --name big-orders --event-bus-name shop --event-pattern file://pattern.json
aws events put-targets --rule big-orders --event-bus-name shop --targets file://targets.json
aws events put-events --entries file://events.json

# ---------- Azure: Service Bus ----------
az servicebus namespace create -g shop-rg -n shop-bus --sku Standard
az servicebus queue create -g shop-rg --namespace-name shop-bus -n tasks \
  --max-delivery-count 5 --lock-duration PT1M --enable-session false
az servicebus topic create -g shop-rg --namespace-name shop-bus -n orders
az servicebus topic subscription create -g shop-rg --namespace-name shop-bus --topic-name orders -n billing
az servicebus topic subscription rule create -g shop-rg --namespace-name shop-bus --topic-name orders \
  --subscription-name billing -n created-only --filter-sql-expression "EventType = 'OrderCreated'"
az servicebus queue show -g shop-rg --namespace-name shop-bus -n tasks --query countDetails
az role assignment create --assignee $ID --role "Azure Service Bus Data Receiver" --scope $QUEUE_ID

# ---------- Azure: Event Grid ----------
az eventgrid topic create -g shop-rg -n shop-events -l westeurope
az eventgrid event-subscription create -n to-queue --source-resource-id $SOURCE_ID \
  --endpoint-type servicebusqueue --endpoint $QUEUE_ID

# ---------- Google Cloud: Pub/Sub ----------
gcloud pubsub topics create orders
gcloud pubsub subscriptions create billing --topic orders --ack-deadline 60 \
  --dead-letter-topic orders-dlq --max-delivery-attempts 5 \
  --min-retry-delay 10s --max-retry-delay 600s
gcloud pubsub subscriptions create stock --topic orders --enable-message-ordering
gcloud pubsub subscriptions create exact --topic orders --enable-exactly-once-delivery
gcloud pubsub subscriptions create eu --topic orders --message-filter='attributes.region = "eu"'
gcloud pubsub topics publish orders --message '{"order_id":"order-1"}' --attribute event_type=OrderCreated
gcloud pubsub subscriptions pull billing --limit 10 --auto-ack
gcloud pubsub snapshots create before-deploy --subscription billing
gcloud pubsub subscriptions seek billing --snapshot before-deploy
gcloud pubsub subscriptions add-iam-policy-binding billing --member serviceAccount:$SA --role roles/pubsub.subscriber

# ---------- Google Cloud: Eventarc и Cloud Tasks ----------
gcloud eventarc triggers create t --location europe-west1 --destination-run-service svc \
  --event-filters type=google.cloud.storage.object.v1.finalized --event-filters bucket=b
gcloud tasks queues create emails --location europe-west1 --max-dispatches-per-second 10
```

---

# Шпаргалка важных настроек

**Очередь задач (любое облако):**

```
long polling / streaming pull
время на обработку (visibility / lock / ack deadline) > p99 обработки, с продлением для долгих задач
DLQ + лимит попыток 3–10 + алерт на DLQ
retention DLQ — максимальный
идемпотентный консьюмер по id события
ограничение параллелизма консьюмеров
```

**AWS:**

```
SQS: WaitTimeSeconds=20, RedrivePolicy (maxReceiveCount 5), DLQ retention 14 дней
SQS FIFO: MessageGroupId по сущности, MessageDeduplicationId = id события
SNS -> SQS: политика очереди для SNS, RawMessageDelivery=true, FilterPolicy
Lambda + SQS: ReportBatchItemFailures, MaximumConcurrency, visibility timeout >= 6 × таймаут функции
EventBridge: DLQ на цель, проверять FailedEntryCount
```

**Azure:**

```
Service Bus: peek-lock, MaxDeliveryCount 5–10, AutoLockRenewer, duplicate detection
порядок: сессии; фильтры: correlation или SQL по application_properties
Entra ID + managed identity, local_auth_enabled = false
Premium для private endpoints и больших сообщений
```

**Google Cloud:**

```
Pub/Sub: ack deadline под обработку, FlowControl, retry policy с backoff
dead letter topic + подписка на него + права сервисного агента
порядок: ordering keys; без повторных доставок: exactly-once (pull, регион)
фильтры задаются при создании подписки
snapshot перед рискованным релизом
```

**Всегда:**

```
проверять частичные ошибки батчевых API
id события задаёт producer (CloudEvents: source + id)
outbox для событий из транзакции
алерты: возраст самого старого сообщения, DLQ, ошибки доставки, бюджет
вся топология — в Terraform
```

---

# Вопросы на собеседовании с ответами

**Junior**

1. **Чем облачный брокер отличается от своего Kafka или RabbitMQ?** Облако управляет серверами, репликацией и обновлениями; ты работаешь через API и платишь за операции или объём, но привязан к облаку и ограничен возможностями сервиса.
2. **Что такое visibility timeout в SQS?** Время, на которое полученное сообщение скрывается от других консьюмеров; если его не удалили за это время, оно снова становится видимым.
3. **Чем SQS standard отличается от FIFO?** Standard — at-least-once, без гарантии порядка, почти без лимита пропускной способности; FIFO — порядок внутри группы и дедупликация в окне 5 минут, с ограниченной пропускной способностью.
4. **Зачем long polling?** Short polling опрашивает часть серверов и может вернуть пустой ответ при наличии сообщений; long polling ждёт до 20 секунд и снижает число платных пустых запросов.
5. **Что такое SNS и зачем связка SNS + SQS?** SNS — pub/sub без хранения; в паре с SQS каждый сервис получает свою надёжную очередь с буфером и DLQ.
6. **Что такое peek-lock в Service Bus?** Сообщение блокируется за получателем, который должен явно завершить его (`complete`), вернуть (`abandon`), отправить в DLQ или отложить.
7. **Как в Pub/Sub получить очередь и fan-out?** Топик + одна подписка — очередь; топик + несколько подписок — fan-out.
8. **Что такое DLQ?** Очередь для сообщений, которые не удалось обработать за заданное число попыток.
9. **Что такое ack deadline в Pub/Sub?** Время на подтверждение сообщения; без ack оно будет доставлено снова. Клиентские библиотеки продлевают его автоматически.
10. **Зачем эмуляторы?** Локальная разработка и интеграционные тесты без облачного аккаунта и счёта: moto или LocalStack, эмуляторы Service Bus и Pub/Sub.

**Middle**

11. **Как обеспечить порядок по сущности в каждом облаке?** SQS FIFO с `MessageGroupId`, сессии Service Bus, ordering keys в Pub/Sub; порядок внутри ключа, параллелизм между ключами.
12. **Почему у DLQ standard-очереди SQS должен быть максимальный retention?** Срок хранения считается от исходного времени постановки сообщения в очередь, и в DLQ оно может истечь раньше, чем его разберут.
13. **Почему SNS может не доставлять сообщения в подписанную очередь SQS?** Нет политики очереди, разрешающей SNS отправку, или нет прав SNS на ключ KMS, которым зашифрована очередь.
14. **Что такое `ReportBatchItemFailures`?** Ответ Lambda со списком неудавшихся сообщений пачки SQS: успешные удаляются, неудавшиеся возвращаются; без него при ошибке возвращается вся пачка.
15. **Какое соотношение visibility timeout и таймаута функции Lambda рекомендуется?** Visibility timeout — не меньше шести таймаутов функции, чтобы сообщение не стало видимым во время обработки.
16. **Чем `abandon` отличается от `defer` в Service Bus?** `abandon` возвращает сообщение для повторной доставки, `defer` оставляет его в очереди, но выдаёт только по sequence number.
17. **Почему dead lettering в Pub/Sub может молча не работать?** Сервисному агенту Pub/Sub не выданы права публиковать в DLQ-топик и подписчика на исходной подписке, или у DLQ-топика нет подписки.
18. **От чего защищает exactly-once delivery в Pub/Sub?** От повторной доставки подтверждённого сообщения на pull-подписке в регионе; не от повторной публикации и не от ошибок записи во внешнюю базу.
19. **Чем EventBridge отличается от SNS?** EventBridge маршрутизирует по правилам на содержимое события, интегрирован с событиями AWS и SaaS, имеет archive/replay; SNS — простой fan-out с фильтрами подписок.
20. **Как сделать отложенное на 3 дня событие в каждом облаке?** EventBridge Scheduler, scheduled messages в Service Bus, Cloud Tasks с `schedule_time`; SQS задерживает только до 15 минут.
21. **Что такое частичная ошибка батча, и где она встречается?** Батчевый запрос успешен, но часть элементов отклонена: SQS, SNS `PublishBatch`, EventBridge `PutEvents`, Kinesis `PutRecords`; нужно проверять ответ и повторять неудавшиеся.
22. **Какая метрика лучше всего показывает отставание консьюмеров?** Возраст самого старого необработанного сообщения: `ApproximateAgeOfOldestMessage` в SQS, `oldest_unacked_message_age` в Pub/Sub.
23. **Как фильтровать сообщения в Service Bus, SNS и Pub/Sub?** Правила подписки (SQL/correlation по свойствам), filter policy подписки (атрибуты или тело), фильтр подписки Pub/Sub (атрибуты, неизменяемый).
24. **Когда нужен Kinesis, Event Hubs или Managed Kafka вместо очереди?** Для лога: повторное чтение, много независимых читателей, большие потоки, порядок по партициям.
25. **Что такое claim check?** Большой объект кладётся в объектное хранилище, а в сообщение — ссылка; решает лимиты размера и снижает стоимость.

**Senior**

26. **Как реализовать outbox в AWS, Azure и Google Cloud без своего опросчика?** DynamoDB Streams → EventBridge Pipes; Cosmos DB Change Feed → Function → Service Bus; Spanner change streams → Dataflow → Pub/Sub; или PostgreSQL + Debezium Server.
27. **Почему id сообщения, назначенный брокером, не годится для идемпотентности?** Повторная публикация тем же producer'ом создаёт новое сообщение с новым id; ключ идемпотентности должен задавать producer.
28. **Как ограничить нагрузку serverless-консьюмеров на базу данных?** `MaximumConcurrency` у event source mapping, `maxConcurrentCalls` у Functions, лимиты экземпляров Cloud Run, `FlowControl` в pull.
29. **Как устроена безопасность доступа к брокерам в трёх облаках?** IAM-роли и политики на конкретные ресурсы (SQS/SNS policies, роли Service Bus Data Sender/Receiver, роли Pub/Sub на топик и подписку), managed identity вместо ключей, приватные эндпоинты, собственные ключи шифрования.
30. **Когда облачный брокер дороже своего кластера?** При огромном постоянном потоке с оплатой за операции; но стоимость эксплуатации своего кластера (инженеры) часто перевешивает.
31. **Что такое CloudEvents и зачем?** Спецификация конверта событий с полями `id`, `source`, `type`, `specversion`; единый формат между сервисами и облаками, нативно в Event Grid и Eventarc, `source + id` — ключ идемпотентности.
32. **Как мигрировать с RabbitMQ в облако?** Сопоставить exchanges и очереди с топиками и подписками (SNS+SQS, Service Bus topics, Pub/Sub), DLX с DLQ, порядок с FIFO/сессиями/ordering keys; либо Amazon MQ без переписывания; двойная публикация на период миграции.
33. **Какие ограничения у абстракций над облачными брокерами?** Наименьший общий знаменатель: уникальные возможности (сессии, фильтры, exactly-once) недоступны или работают по-разному.
34. **Как не получить неожиданный счёт?** Long polling, батчи, DLQ с разумным лимитом, защита от рекурсии, фильтры подписок, бюджеты и алерты на аномалии, учебные ресурсы в отдельных аккаунтах.
35. **Как выбрать между облачным сервисом, управляемым Kafka/RabbitMQ и своим кластером?** По требованиям к переносимости, возможностям, задержке, стоимости при твоём профиле нагрузки и готовности команды к эксплуатации.

---

# FAQ

**Нужен ли облачный аккаунт, чтобы пройти курс?**
Для большинства модулей — нет: хватит эмуляторов (moto, Service Bus emulator, Pub/Sub emulator); готовые тесты лежат в `examples/`. Аккаунт нужен для IAM, serverless-интеграций, квот и стоимости.

**SQS или SNS?**
SQS — очередь для воркеров. SNS — раздача одного сообщения многим. Для межсервисных событий обычно оба: SNS-топик и SQS-очередь на каждый сервис.

**SNS или EventBridge?**
SNS — простой и дешёвый fan-out. EventBridge — маршрутизация по содержимому, события AWS и SaaS, archive/replay, Scheduler и Pipes.

**Service Bus или Event Grid?**
Service Bus — надёжные очереди и топики с peek-lock, сессиями и DLQ. Event Grid — маршрутизация событий, особенно событий ресурсов Azure, с push-доставкой. Часто используются вместе: Event Grid → Service Bus.

**Pub/Sub или Cloud Tasks?**
Pub/Sub — раздача событий подписчикам. Cloud Tasks — конкретные HTTP-вызовы с контролем темпа и отложенным запуском.

**Есть ли exactly-once в облаке?**
SQS FIFO и Service Bus дают дедупликацию публикаций в окне, Pub/Sub — exactly-once доставку на pull-подписке, Service Bus — транзакции внутри namespace. С внешней базой данных exactly-once не даёт никто: нужна идемпотентность.

**Сохраняют ли облачные брокеры порядок?**
Только в специальных режимах: SQS FIFO, Service Bus sessions, Pub/Sub ordering keys — и только внутри ключа. SQS standard, SNS standard, EventBridge и Event Grid порядок не гарантируют.

**Можно ли перечитать сообщения?**
Из очередей — нет (кроме Pub/Sub seek). EventBridge — archive и replay. Для полноценного повторного чтения — Kinesis, Event Hubs или Kafka.

**Как отправить сообщение больше лимита?**
Claim check: объект в S3, Blob Storage или Cloud Storage, в сообщении — ссылка. Для SQS есть Extended Client Library.

**Что с Pub/Sub Lite?**
Отключён 18 марта 2026 года. Альтернативы — обычный Pub/Sub или Managed Service for Apache Kafka.

**Как писать переносимый код?**
Свой интерфейс с адаптерами или библиотека-абстракция (Dapr, Spring Cloud Stream, MassTransit, Go CDK), CloudEvents как формат событий, либо открытый протокол (Kafka, AMQP 1.0).

---

# Глоссарий

| Термин | Что значит |
|---|---|
| **Managed service** | Сервис, эксплуатацию которого берёт на себя облако |
| **SQS** | Очередь сообщений AWS (standard и FIFO) |
| **Visibility timeout** | Время, на которое полученное сообщение SQS скрыто от других консьюмеров |
| **Receipt handle** | Идентификатор конкретного получения сообщения SQS, нужен для удаления |
| **Long polling** | Ожидание сообщений в запросе до 20 секунд |
| **Message group ID** | Ключ порядка в SQS FIFO |
| **Redrive policy** | Настройка DLQ и лимита получений в SQS |
| **SNS** | Pub/sub-сервис AWS |
| **Raw message delivery** | Доставка из SNS без JSON-конверта |
| **Filter policy** | Фильтр подписки SNS |
| **EventBridge** | Шина событий AWS |
| **Event pattern** | Шаблон правила EventBridge |
| **Kinesis Data Streams** | Лог AWS с шардами |
| **Service Bus** | Корпоративный брокер Azure |
| **Namespace** | Контейнер сущностей Service Bus или Event Hubs |
| **Peek-lock** | Режим получения с блокировкой и явным завершением |
| **Lock duration** | Время блокировки сообщения в Service Bus |
| **Session** | Группа сообщений Service Bus с порядком и одним получателем |
| **Duplicate detection** | Дедупликация публикаций в Service Bus по message id |
| **Deferral** | Отложенное сообщение Service Bus, доступное по sequence number |
| **Event Grid** | Шина событий Azure |
| **Event Hubs** | Лог Azure с партициями, совместимый с Kafka-клиентами |
| **Pub/Sub** | Сервис обмена сообщениями Google Cloud |
| **Subscription** | Подписка Pub/Sub: очередь внутри топика |
| **Ack deadline** | Время на подтверждение сообщения Pub/Sub |
| **Ordering key** | Ключ порядка в Pub/Sub |
| **Exactly-once delivery** | Режим подписки Pub/Sub без повторной доставки подтверждённых сообщений |
| **Seek / snapshot** | Перемотка подписки Pub/Sub по времени или к снимку |
| **Eventarc** | Маршрутизация событий Google Cloud |
| **Cloud Tasks** | Очередь HTTP-задач Google Cloud |
| **Event source mapping** | Механизм Lambda, читающий очередь или поток |
| **Batch item failures** | Частичные ошибки пачки в Lambda |
| **Managed identity** | Учётная запись приложения в Azure без секретов |
| **Service agent** | Служебная учётная запись сервиса Google Cloud |
| **CMEK / SSE-KMS** | Шифрование собственными ключами |
| **Claim check** | Паттерн: объект в хранилище, ссылка в сообщении |
| **CloudEvents** | Спецификация CNCF для описания событий |
| **Outbox** | Таблица событий в транзакции с бизнес-данными |
| **Emulator** | Локальная замена облачного сервиса для разработки и тестов |

---

# Официальные источники и что читать дальше

- **Amazon SQS** — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/
- **Amazon SNS** — https://docs.aws.amazon.com/sns/latest/dg/
- **Amazon EventBridge** — https://docs.aws.amazon.com/eventbridge/latest/userguide/
- **Что нового в AWS** — https://aws.amazon.com/new/
- **Azure Service Bus** — https://learn.microsoft.com/azure/service-bus-messaging/
- **Azure Event Grid** — https://learn.microsoft.com/azure/event-grid/
- **Azure Event Hubs** — https://learn.microsoft.com/azure/event-hubs/
- **Google Cloud Pub/Sub** — https://cloud.google.com/pubsub/docs
- **Eventarc** — https://cloud.google.com/eventarc/docs
- **Cloud Tasks** — https://cloud.google.com/tasks/docs
- **CloudEvents** — https://cloudevents.io
- **moto** — https://github.com/getmoto/moto
- **LocalStack** — https://docs.localstack.cloud
- **Service Bus emulator** — https://learn.microsoft.com/azure/service-bus-messaging/overview-emulator
- **Pub/Sub emulator** — https://cloud.google.com/pubsub/docs/emulator
- **Парные курсы серии:** NATS, Apache Kafka, RabbitMQ

---

## Как помочь курсу

Нашёл ошибку, устаревший лимит или изменившееся поведение сервиса? Открой issue или пришли pull request. Особенно полезны:

- примеры на других языках (Go, Java, C#, Node.js);
- реальные production-истории и разборы инцидентов;
- обновления лимитов и новых возможностей сервисов.

⭐ Если курс помог, поставь звезду: так его найдут другие разработчики.

**Лицензия:** текст курса распространяется по лицензии [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.ru), примеры кода — по [MIT License](LICENSE). Можно свободно использовать, адаптировать и распространять материалы, в том числе для внутренних воркшопов, с указанием источника.
