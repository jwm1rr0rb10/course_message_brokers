# Cloud Message Brokers 2026: a free course on AWS, Azure and Google Cloud messaging from zero to pro

![AWS](https://img.shields.io/badge/AWS-SQS%20%7C%20SNS%20%7C%20EventBridge-FF9900?logo=amazonwebservices&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Service%20Bus%20%7C%20Event%20Grid%20%7C%20Event%20Hubs-0078D4?logo=microsoftazure&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-Pub%2FSub%20%7C%20Eventarc-4285F4?logo=googlecloud&logoColor=white)
![Language English](https://img.shields.io/badge/language-english-red)
![Free course](https://img.shields.io/badge/price-free-brightgreen)

> **A complete free course on cloud message brokers.** AWS SQS, SNS, EventBridge and Kinesis; Azure Service Bus, Event Grid and Event Hubs; Google Cloud Pub/Sub, Eventarc and Cloud Tasks. The same problems — task queues, fan-out, ordering, deduplication, DLQs, delayed messages, exactly-once — are worked through side by side in all three clouds, with mapping tables. Local development on emulators, serverless integrations, security, monitoring, cost, Terraform, and choosing between a cloud service and your own Kafka, RabbitMQ or NATS. Current for **2026**.

**Clouds without the fluff:** every module gives you clear theory, diagrams, commands you can actually run (including without a cloud account, on emulators), the mistakes people make, and self-check questions.

This course is part of a series. For a deeper look at the messaging models themselves, see the companion courses: **[NATS](../nats/README.md)**, **[Apache Kafka](../kafka/README.md)** and **[RabbitMQ](../rabbit/README.md)**. This course links to them wherever a cloud service repeats their ideas.

⭐ If the course helps, star the repository so other developers can find it.

🇷🇺 Русская версия: [READMEru.md](READMEru.md)

---

## Who this course is for

| Who you are | What you get |
|---|---|
| **Backend developer** in the cloud | How to send and receive messages with SQS, Service Bus and Pub/Sub correctly, without losing them or processing them twice |
| **Serverless developer** | Lambda, Azure Functions and Cloud Run as consumers: batches, partial failures, retries |
| **Architect** | A map of three clouds' services, choosing between a queue, pub/sub, an event bus and a log, multi-cloud |
| **DevOps / SRE / Platform** | IAM, encryption, monitoring, alerts, quotas, Terraform, cost |
| **Coming from RabbitMQ or Kafka** | What maps to what in the cloud, and where cloud services behave differently |
| **Interview prep** | 35 questions with answers at junior, middle and senior level |

## What you'll be able to do after the course

- find your way around the messaging services of AWS, Azure and Google Cloud and pick the right one for the job;
- work with SQS queues: standard and FIFO, visibility timeout, long polling, DLQ and redrive;
- build fan-out with SNS + SQS and filter messages;
- route events with EventBridge, Event Grid and Eventarc;
- use Azure Service Bus: peek-lock, sessions, deduplication, scheduled messages, topics and subscriptions;
- use Google Pub/Sub: pull and push, ack deadline, ordering keys, exactly-once, filters and seek;
- know when you need a log (Kinesis, Event Hubs, Managed Kafka) instead of a queue;
- design idempotent consumers and an outbox in the cloud;
- connect serverless functions to queues without losses on partial failures;
- configure IAM, encryption with KMS keys and private access;
- monitor the age of the oldest message, DLQs and subscriber backlog;
- estimate cost and avoid unexpected bills;
- describe the whole topology in Terraform;
- develop and test locally on emulators, without a cloud account.

---

## Table of contents

- [Who this course is for](#who-this-course-is-for)
- [What you'll be able to do after the course](#what-youll-be-able-to-do-after-the-course)
- [How to take this course](#how-to-take-this-course)
- [Module 0. Why cloud brokers, and how they differ from your own](#module-0-why-cloud-brokers-and-how-they-differ-from-your-own)
- [Module 1. The service map: queues, pub/sub, event buses and logs](#module-1-the-service-map-queues-pubsub-event-buses-and-logs)
- [Module 2. Local environment: emulators and first commands](#module-2-local-environment-emulators-and-first-commands)
- [Module 3. AWS SQS: standard and FIFO queues](#module-3-aws-sqs-standard-and-fifo-queues)
- [Module 4. AWS SNS: fan-out, filtering and SNS + SQS](#module-4-aws-sns-fan-out-filtering-and-sns--sqs)
- [Module 5. AWS EventBridge and Kinesis](#module-5-aws-eventbridge-and-kinesis)
- [Module 6. Azure Service Bus: queues, topics, sessions](#module-6-azure-service-bus-queues-topics-sessions)
- [Module 7. Azure Event Grid and Event Hubs](#module-7-azure-event-grid-and-event-hubs)
- [Module 8. Google Cloud Pub/Sub](#module-8-google-cloud-pubsub)
- [Module 9. Google Eventarc, Cloud Tasks and Managed Kafka](#module-9-google-eventarc-cloud-tasks-and-managed-kafka)
- [Module 10. Patterns across three clouds: mapping tables](#module-10-patterns-across-three-clouds-mapping-tables)
- [Module 11. Delivery guarantees, idempotency and outbox](#module-11-delivery-guarantees-idempotency-and-outbox)
- [Module 12. Serverless consumers: Lambda, Azure Functions, Cloud Run](#module-12-serverless-consumers-lambda-azure-functions-cloud-run)
- [Module 13. Security: IAM, encryption, private access](#module-13-security-iam-encryption-private-access)
- [Module 14. Monitoring and alerts](#module-14-monitoring-and-alerts)
- [Module 15. Cost, quotas and limits](#module-15-cost-quotas-and-limits)
- [Module 16. Infrastructure as code: Terraform](#module-16-infrastructure-as-code-terraform)
- [Module 17. Portability, CloudEvents and migrations](#module-17-portability-cloudevents-and-migrations)
- [Module 18. A cloud service or your own Kafka, RabbitMQ, NATS](#module-18-a-cloud-service-or-your-own-kafka-rabbitmq-nats)
- [Module 19. Capstone project: an online shop in three clouds](#module-19-capstone-project-an-online-shop-in-three-clouds)
- [CLI cheat sheet: aws, az, gcloud](#cli-cheat-sheet-aws-az-gcloud)
- [Configuration cheat sheet](#configuration-cheat-sheet)
- [Interview questions with answers](#interview-questions-with-answers)
- [FAQ](#faq)
- [Glossary](#glossary)
- [Official sources and what to read next](#official-sources-and-what-to-read-next)

---

## How to take this course

1. **Start with modules 0–2**: the overall map and the local environment. Then you can follow the cloud you need (AWS — modules 3–5, Azure — 6–7, Google Cloud — 8–9) and come back to the shared modules 10–18.
2. **Run the commands on emulators.** Most exercises don't need a cloud account.
3. **Compare.** Module 10 puts the same patterns side by side in three clouds: the best way to see where the services are alike and where they aren't.
4. **Answer the questions at the end of each module** out loud, as if you were in an interview.
5. **Do the capstone project** in at least one cloud.

**What to install:** Docker, Python 3.10+, Git. Cloud CLIs: `aws` (AWS CLI v2), `az` (Azure CLI), `gcloud` (Google Cloud CLI) — install the ones you need. Code examples are in Python (boto3, azure-servicebus, google-cloud-pubsub) and Go (aws-sdk-go-v2, azservicebus, cloud.google.com/go/pubsub/v2).

**Runnable examples:** a docker-compose file with emulators for all three clouds, consumers and tests for the course's claims in Python and Go live in [`examples/`](examples/). CI ([`examples.yml`](../.github/workflows/examples.yml)) runs them on every push and weekly: `go vet` and the Go and Python tests against the emulators (moto, the Service Bus emulator, the Pub/Sub emulator), with both the pinned and the latest image versions, so changes in emulators and SDKs show up immediately.

**Being current.** Cloud services change all the time and without version numbers: limits grow, new features appear. The course gives the values as of 2026; before designing, check the limits against the service's *Quotas* page. For example, SQS in August 2025 and EventBridge in January 2026 raised the maximum message size from 256 KiB to 1 MiB, SNS in September 2026 allowed up to 1 MiB via a separate topic attribute, and Google Pub/Sub Lite was shut down on 18 March 2026 — older articles don't know any of this.

---

# Module 0. Why cloud brokers, and how they differ from your own

## 0.1 A cloud broker in plain words

A **cloud message broker** is a service that does what RabbitMQ or Kafka do: accepts messages, stores them and hands them to recipients. The difference is that **you don't see the servers**: you create a queue or topic via an API, the console or Terraform, and the cloud takes care of scaling, replication, upgrades and failure recovery.

```
YOUR OWN BROKER (Kafka, RabbitMQ, NATS)    CLOUD BROKER (SQS, Service Bus, Pub/Sub)
---------------------------------------    ----------------------------------------
you choose servers and disks               no servers, just an API and quotas
you configure the cluster and replication  cross-zone replication is built in
you upgrade versions                       upgrades are transparent
you monitor nodes                          you monitor queues, nodes are invisible
you pay for servers 24/7                   you pay for requests and volume
portable across clouds                     tied to the cloud
every open-source feature                  only what the service offers
```

## 0.2 The shared responsibility model

| Task | Your own broker | Cloud broker |
|---|---|---|
| Servers, disks, network | You | The cloud |
| Replication and fault tolerance | You | The cloud (within a region) |
| Upgrades and security patches | You | The cloud |
| Scaling | You | The cloud (within quotas) |
| Topology: queues, topics, subscriptions | You | You |
| Access control | You | You (via IAM) |
| Idempotency, retries, DLQ | You | You |
| Queue monitoring and alerts | You | You (the cloud provides metrics) |
| Cost | Servers | Requests, volume, traffic |

The key point: **the cloud takes operations off your hands, but not design**. It's as easy to lose a message or process it twice in SQS as in RabbitMQ if you don't understand the delivery model.

## 0.3 When a cloud broker is a good choice

- The system already lives in one cloud, and portability isn't a goal.
- There's no team that wants to operate broker clusters.
- The load is uneven: paying per request is cheaper than keeping a cluster sized for the peak.
- You need tight integration with other cloud services: functions, storage, analytics, infrastructure events.
- You need certified security and audit tooling out of the box.

## 0.4 When a cloud broker is a poor choice

- You need portability across clouds or on-premises.
- You need features the service doesn't have: complex routing, priorities, a years-long log, low-latency request-reply.
- A very large constant message flow: with per-request billing your own cluster can be several times cheaper (module 15).
- You need minimal latency (microseconds or single milliseconds): cloud APIs run over HTTPS and add latency.

## 0.5 Two groups of cloud services

| Group | Examples | Feature |
|---|---|---|
| **The cloud's own services** | SQS, SNS, EventBridge, Kinesis; Service Bus, Event Grid, Event Hubs; Pub/Sub, Eventarc, Cloud Tasks | Their own API, exist only in that cloud |
| **Managed open-source brokers** | Amazon MSK (Kafka), Amazon MQ (RabbitMQ, ActiveMQ); Google Managed Service for Apache Kafka; Event Hubs with the Kafka protocol | Standard Kafka and RabbitMQ clients, portability is preserved |

Managed Kafka and RabbitMQ are covered in depth in the companion courses ([Kafka](../kafka/README.md), [RabbitMQ](../rabbit/README.md)): everything said there about clients, guarantees and design applies to them. This course is about the clouds' **own** services.

## 0.6 The common model: HTTP API, pull and push

Almost all of the clouds' own brokers work through an **HTTPS API** rather than a persistent TCP connection with a binary protocol (the exceptions are Azure Service Bus and Event Hubs, which speak AMQP 1.0).

```
PULL (SQS, Pub/Sub pull, Service Bus)       PUSH (SNS, EventBridge, Event Grid, Pub/Sub push)
-------------------------------------       ------------------------------------------------
the consumer asks: "anything for me?"       the service calls your handler itself
long polling: wait up to N seconds           an HTTP endpoint, a function, another queue
the consumer controls the pace               the service controls the pace and retries
good for workers                             good for serverless and webhooks
```

## 0.7 What a cloud broker will NOT do for you

- idempotent processing: almost every service is at-least-once;
- the right visibility timeout / ack deadline for your processing time;
- a DLQ and an alert on it: you must create and wire them up;
- message schemas and versioning;
- protection from a surprise bill: endless retries cost money too;
- portability: code written against a specific API is tied to the cloud.

### Self-check questions

1. What does the cloud take on, and what stays with you, when you use a cloud broker?
2. How do the clouds' own services differ from managed Kafka and RabbitMQ?
3. How does the pull model differ from the push model?
4. When does a cloud broker end up more expensive than your own cluster?

---

# Module 1. The service map: queues, pub/sub, event buses and logs

## 1.1 Four classes of services

| Class | What it does | Delivery model | Open-source analogue |
|---|---|---|---|
| **Queue** | Hands tasks to workers, each message is processed once | Pull, per-message acknowledgement | A RabbitMQ queue |
| **Pub/sub** | Copies a message to every subscriber | Push or pull | RabbitMQ fanout/topic exchange, NATS subjects |
| **Event bus** | Routes events by content-based rules, integrates services and SaaS | Push to targets | A topic exchange with filters |
| **Log (streaming)** | Stores a stream, supports replay, partitions | Pull with an offset | Kafka, RabbitMQ streams, NATS JetStream |

## 1.2 A mapping table for three clouds

| Task | AWS | Azure | Google Cloud |
|---|---|---|---|
| Task queue | **SQS** (standard, FIFO) | **Service Bus** queues; Storage Queues (basic) | **Pub/Sub** pull subscription; **Cloud Tasks** |
| Pub/sub, fan-out | **SNS** (+ SQS) | **Service Bus** topics and subscriptions | **Pub/Sub** topic + several subscriptions |
| Event bus, content-based routing | **EventBridge** | **Event Grid** | **Eventarc** |
| Log, streaming | **Kinesis Data Streams**, **MSK** | **Event Hubs** (incl. the Kafka protocol) | **Managed Service for Apache Kafka**; Pub/Sub with seek |
| Managed RabbitMQ | **Amazon MQ** | — (marketplace) | — (marketplace) |
| Delayed tasks and schedules | SQS delay (up to 15 min), **EventBridge Scheduler** | Service Bus scheduled messages | **Cloud Tasks**, Cloud Scheduler |
| Message ordering | SQS FIFO (message group), SNS FIFO | Service Bus sessions | Pub/Sub ordering keys |
| Service-side deduplication | SQS FIFO (5 minutes) | Service Bus duplicate detection | Pub/Sub exactly-once (on the subscription) |

**The main takeaway from the table:** in Google Cloud a single service, Pub/Sub, covers both the queue and pub/sub; AWS needs two services for that (SQS and SNS); in Azure, Service Bus does both queues and topics.

## 1.3 Key limits (ballparks for 2026)

| | SQS | SNS | EventBridge | Service Bus | Pub/Sub |
|---|---|---|---|---|---|
| Max message size | 1 MiB (since Aug 2025) | 256 KiB; up to 1 MiB via the `MaximumMessageSize` attribute (since Sep 2026, for SQS, Lambda and Firehose subscriptions) | 1 MB (since Jan 2026) | 256 KiB (Standard), up to 100 MB (Premium) | 10 MB |
| Retention | 1 min – 14 days (4 days by default) | Doesn't store, delivers | Doesn't store (except archive and replay) | By message TTL; at most 14 days on the Basic tier | Up to 7 days on a subscription (topic retention up to 31 days) |
| Processing time | Visibility timeout up to 12 hours | — | — | Lock duration up to 5 minutes (renewable) | Ack deadline 10 s – 10 min (extendable) |
| Ordering | FIFO queues | FIFO topics | No | Sessions | Ordering keys |

These values change: before designing, open the *Quotas* page of the service.

## 1.4 How to choose

```
need to hand tasks to workers?
   ├── yes -> QUEUE: SQS / Service Bus queue / Pub/Sub pull
   └── no
        need to deliver one event to many independent recipients?
           ├── yes -> content-based routing and SaaS integrations more important?
           │         ├── yes -> EVENT BUS: EventBridge / Event Grid / Eventarc
           │         └── no  -> PUB/SUB: SNS+SQS / Service Bus topic / Pub/Sub
           └── no
                need history, replay, large flows, partitions?
                   └── yes -> LOG: Kinesis or MSK / Event Hubs / Managed Kafka
```

### Self-check questions

1. What four classes of cloud messaging services exist, and how does an event bus differ from pub/sub?
2. Which services implement fan-out in each of the three clouds?
3. Why does a queue with fan-out need two services in AWS but one in Google Cloud?
4. Which service would you choose for hours of event history with replay?

---

# Module 2. Local environment: emulators and first commands

## 2.1 Why emulators

A cloud account for learning means the risk of an accidental bill and the hassle of setting up access. For most of the course's exercises **emulators** that run in Docker and speak the same APIs are enough:

| Cloud | Emulator | What it covers |
|---|---|---|
| AWS | **moto** in server mode (open source) or **LocalStack** (needs an auth token since March 2026, a free plan exists) | SQS, SNS, EventBridge, Kinesis and much more |
| Azure | **Azure Service Bus emulator** (official) | Queues, topics, subscriptions, sessions, DLQ |
| Google Cloud | **Pub/Sub emulator** (official, from the Cloud SDK) | Topics, subscriptions, pull and push |

Emulator limitations: no IAM and quotas, some features aren't implemented (for example, exactly-once in the Pub/Sub emulator), and behaviour under load doesn't match the cloud. That's enough for learning and integration tests; verify behaviour that production depends on in the cloud.

## 2.2 Docker Compose with all emulators

`docker-compose.yml`:

```yaml
services:
  # AWS: SQS, SNS, EventBridge via moto (open source, no token)
  aws:
    image: motoserver/moto:latest
    ports: ["4566:5000"]

  # Google Cloud Pub/Sub
  pubsub:
    image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
    command: gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=course-project
    ports: ["8085:8085"]

  # Azure Service Bus emulator; entities are created from servicebus-config.json
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

  # metadata database for the Service Bus emulator (Azure SQL Edge has been retired)
  sql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "Course_Passw0rd!"
```

The Service Bus emulator creates queues and topics from a configuration file on start; a ready `servicebus-config.json` lives in `examples/emulators/`. The emulator stores metadata in SQL Server: examples used to use Azure SQL Edge, but it has been retired, so the `mssql/server` image is used here. Emulator images and environment variables change: check their documentation if something doesn't start.

LocalStack was the standard AWS emulator for a long time, but since March 2026 its image requires an auth token (a free plan remains). So the course uses the open-source **moto** in server mode: the same SQS, SNS and EventBridge APIs on port 4566, no registration. The commands below work with LocalStack too.

```bash
docker compose up -d
```

## 2.3 AWS CLI with the emulator

```bash
# dummy credentials: the emulator doesn't check them
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=eu-central-1
alias awsl='aws --endpoint-url=http://localhost:4566'

awsl sqs create-queue --queue-name tasks
awsl sqs list-queues
awsl sqs send-message --queue-url http://localhost:4566/123456789012/tasks --message-body '{"task":"resize"}'
awsl sqs receive-message --queue-url http://localhost:4566/123456789012/tasks --wait-time-seconds 5
```

`123456789012` is the account id moto uses (LocalStack uses `000000000000`). In the cloud a queue URL looks like `https://sqs.eu-central-1.amazonaws.com/<account-id>/tasks`.

## 2.4 The Pub/Sub emulator

Google's client libraries switch to the emulator automatically when an environment variable is set:

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

`gcloud pubsub` doesn't work with the emulator: manage topics and subscriptions through the client libraries or the emulator's REST API.

## 2.5 The Service Bus emulator

The Service Bus emulator accepts a connection string with a special flag:

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

In the cloud, prefer Entra ID credentials (`DefaultAzureCredential`, module 13) over a connection string.

## 2.6 When you need a real cloud account

- IAM policies and private endpoints;
- serverless integrations (Lambda, Azure Functions, Cloud Run) in a real environment;
- behaviour under load, quotas and cost;
- features emulators don't have.

For such exercises create a separate learning account or project with **a budget and a spending alert** (module 15), and delete resources after each session (`terraform destroy`, module 16).

### Practice

1. Bring up the emulators with Docker Compose.
2. Create an SQS queue in the emulator, send and receive a message with the AWS CLI.
3. Create a topic and subscription in the Pub/Sub emulator, publish and receive a message from Python.
4. Send and receive a message through the Service Bus emulator.

---

# Module 3. AWS SQS: standard and FIFO queues

## 3.1 How SQS works

**Amazon SQS** is a fully managed queue: you create a queue, send messages, and workers take them, process them and delete them.

```
producer --SendMessage--> [ SQS queue ] <--ReceiveMessage-- worker
                                |                              |
                                |   the message becomes        | processed
                                |   INVISIBLE for the          v
                                |   visibility timeout       DeleteMessage(receipt handle)
                                |   (30 s by default)
                                |
                                +-- not deleted in time -> visible again -> redelivery
```

The key idea of SQS is the **visibility timeout** instead of an ack. A received message isn't deleted, it becomes invisible to other consumers. If the worker managed to process it and call `DeleteMessage`, the message is gone. If not (it crashed, hung, ran out of time), the message reappears in the queue when the timeout expires.

It's the same as `AckWait` in NATS JetStream or the consumer timeout in RabbitMQ, except here it's the only acknowledgement mechanism.

## 3.2 Standard and FIFO

| | **Standard** | **FIFO** |
|---|---|---|
| Delivery | At-least-once: **duplicates** are possible | Exactly-once processing within a 5-minute deduplication window |
| Ordering | Best effort, **not guaranteed** | Strict within a `MessageGroupId` |
| Throughput | Practically unlimited | 300 operations/s per API without batching, 3000 with batching; orders of magnitude more in high throughput mode (region-dependent) |
| Name | Anything | Must end with `.fifo` |
| Per-message delay | Yes (`DelaySeconds` up to 15 minutes) | Only at the queue level |

**Standard** is the default for task queues: fast, cheap, scales effortlessly. The consumer must be idempotent: a message can arrive twice and ordering can break.

**FIFO** is for when you need per-entity ordering or service-side deduplication:

- `MessageGroupId` is like a partition key: messages in one group are delivered strictly in order, and while one message of a group is in flight, the next one in that group isn't handed to anyone. Different groups are processed in parallel;
- `MessageDeduplicationId`: resending with the same id within 5 minutes doesn't create a duplicate. You can enable `ContentBasedDeduplication`, and the id becomes a hash of the body.

## 3.3 Queue parameters

| Attribute | Default | Range | Meaning |
|---|---|---|---|
| `VisibilityTimeout` | 30 s | 0 s – 12 h | How long a message is invisible after being received |
| `MessageRetentionPeriod` | 4 days | 1 min – 14 days | How long an undeleted message is kept |
| `ReceiveMessageWaitTimeSeconds` | 0 | 0 – 20 s | Default long polling for the queue |
| `DelaySeconds` | 0 | 0 – 15 min | Delay before a message becomes visible |
| `MaximumMessageSize` | 1 MiB | 1 KiB – 1 MiB | Maximum size |
| `RedrivePolicy` | none | — | DLQ and `maxReceiveCount` |
| `SqsManagedSseEnabled` | enabled | — | Server-side encryption |

## 3.4 Long polling

`ReceiveMessage` without waiting (short polling) queries **only a subset of SQS servers** and can return an empty response even when there are messages. Besides, every empty request costs money.

**Always use long polling**: `WaitTimeSeconds=20` in the request or `ReceiveMessageWaitTimeSeconds=20` on the queue. The request waits up to 20 seconds until at least one message appears, querying all servers.

## 3.5 A consumer loop in Python (boto3)

```python
import json
import boto3

# for the emulator: endpoint_url="http://localhost:4566"
sqs = boto3.client("sqs", region_name="eu-central-1")
queue_url = sqs.get_queue_url(QueueName="tasks")["QueueUrl"]

while True:
    resp = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,          # up to 10 messages per request
        WaitTimeSeconds=20,              # long polling
        VisibilityTimeout=60,            # longer than processing the batch
        MessageSystemAttributeNames=["ApproximateReceiveCount"],
    )
    for msg in resp.get("Messages", []):
        attempts = int(msg["Attributes"]["ApproximateReceiveCount"])
        try:
            process(json.loads(msg["Body"]))                 # must be idempotent
        except Exception as e:
            print(f"error (attempt {attempts}): {e}")         # not deleted: returns after the timeout
            continue
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
```

Rules:

- delete a message **only after** it was processed successfully;
- a `ReceiptHandle` is valid only for **this** receive: on redelivery it's different;
- for batches use `delete_message_batch` (up to 10 messages per call);
- `ApproximateReceiveCount` is how many times the message has been received: useful for logs and deciding "enough retries".

## 3.6 Sending

```python
# one message
sqs.send_message(
    QueueUrl=queue_url,
    MessageBody=json.dumps({"task": "resize", "image": "1.png"}),
    MessageAttributes={"event-type": {"DataType": "String", "StringValue": "ResizeRequested"}},
)

# a batch of up to 10 messages: cheaper and faster
entries = [{"Id": str(i), "MessageBody": json.dumps({"n": i})} for i in range(10)]
resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
if resp.get("Failed"):
    print("not sent:", resp["Failed"])        # a batch can partially fail: check it
```

**Partial batch failures** are a common trap: `send_message_batch` returns a successful HTTP request even if some messages weren't accepted. Always check the `Failed` field and retry the failed ones.

A FIFO queue:

```python
sqs.send_message(
    QueueUrl=fifo_url,                          # the name ends with .fifo
    MessageBody=json.dumps({"order_id": "order-1", "event": "OrderPaid"}),
    MessageGroupId="order-1",                   # order within the order
    MessageDeduplicationId="order-1-paid",      # a retry within 5 minutes won't create a duplicate
)
```

## 3.7 A consumer in Go (aws-sdk-go-v2)

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
		// o.BaseEndpoint = aws.String("http://localhost:4566") // for the emulator
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
				log.Printf("error, attempt %s: %v", m.Attributes["ApproximateReceiveCount"], err)
				continue // returns after the visibility timeout
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

## 3.8 Long processing: extending visibility

If processing can take longer than the visibility timeout, extend it while you work (like `InProgress()` in NATS or `RENEW` in Kafka share groups):

```python
sqs.change_message_visibility(
    QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"], VisibilityTimeout=120)
```

A typical technique is a background "heartbeat" that extends visibility every N seconds while processing runs. Otherwise a long message is handed to a second worker and processed twice in parallel.

The rule for the timeout: **longer than the p99 time to process a batch**, with headroom, but not much longer, or a failed message waits a long time before its next attempt.

## 3.9 Dead letter queue and redrive

A DLQ is an ordinary queue of the same type (standard for standard, FIFO for FIFO) to which SQS moves a message after `maxReceiveCount` unsuccessful receives:

```bash
awsl sqs create-queue --queue-name tasks-dlq --attributes MessageRetentionPeriod=1209600
DLQ_ARN=$(awsl sqs get-queue-attributes \
  --queue-url http://localhost:4566/123456789012/tasks-dlq \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

awsl sqs set-queue-attributes \
  --queue-url http://localhost:4566/123456789012/tasks \
  --attributes '{"RedrivePolicy":"{\"deadLetterTargetArn\":\"'"$DLQ_ARN"'\",\"maxReceiveCount\":\"5\"}"}'
```

What to know:

- `maxReceiveCount` counts **receives**, not explicit errors: a message a worker received and didn't delete (it crashed, the timeout expired) also counts as an attempt;
- for standard queues retention is counted from the **original** enqueue time: a message that spent 3 days in the main queue lives only one more day in a DLQ with 4-day retention. Make DLQ retention **the maximum** (14 days);
- **redrive** moves messages from the DLQ back to the source queue after a bug fix: in the console or via the `StartMessageMoveTask` API;
- an **alert** on `ApproximateNumberOfMessagesVisible > 0` for the DLQ is mandatory (module 14).

## 3.10 Delayed messages

- `DelaySeconds` on the queue or on an individual message (standard only), up to 15 minutes;
- for delays longer than 15 minutes and schedules, use **EventBridge Scheduler** (module 5);
- retries with a growing delay in SQS are done with `change_message_visibility`: on an error set visibility to `min(base × 2^attempt, maximum)` instead of waiting for the standard timeout.

```python
except TemporaryError:
    attempts = int(msg["Attributes"]["ApproximateReceiveCount"])
    delay = min(2 ** attempts * 5, 900)       # 10 s, 20 s, 40 s ... up to 15 minutes
    sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"],
                                  VisibilityTimeout=delay)
```

## 3.11 Common SQS mistakes

| Mistake | Consequence | Do this instead |
|---|---|---|
| Short polling | Empty responses while messages exist, extra cost | `WaitTimeSeconds=20` |
| Deleting before processing | Loss when a worker crashes | Delete after successful processing |
| Visibility timeout shorter than processing | Parallel duplicate processing | Timeout > p99 processing, or extend visibility |
| No DLQ | A "poison" message loops until retention expires and costs money | DLQ + `maxReceiveCount` + an alert |
| DLQ retention same as the main queue | Messages vanish from the DLQ before anyone looks at them | 14 days on the DLQ |
| Not checking `Failed` in batches | Silent loss of some messages | Retry the failed ones |
| A standard queue and relying on order | Broken ordering | FIFO with `MessageGroupId`, or ordering in the data |
| A non-idempotent consumer | Duplicates in standard queues | Idempotency (module 11) |

### Self-check questions

1. How does the visibility timeout differ from an ack in RabbitMQ?
2. What guarantees do standard and FIFO queues give?
3. Why can short polling return an empty response when there are messages?
4. Why must a standard queue's DLQ have maximum retention?
5. How do you implement retries with a growing delay in SQS?

---

# Module 4. AWS SNS: fan-out, filtering and SNS + SQS

## 4.1 What SNS is

**Amazon SNS** is pub/sub: a publisher publishes a message to a **topic**, and SNS delivers a copy to every **subscriber**. SNS doesn't store messages: it delivers them immediately (with retries), so the subscriber must be available or be a queue.

Subscription types:

| Protocol | What for |
|---|---|
| `sqs` | Reliable delivery to a queue: the main option for services |
| `lambda` | Invoking a function |
| `http` / `https` | A webhook |
| `firehose` | A stream into storage and analytics |
| `email`, `sms`, mobile push | Notifications for people |

## 4.2 The main pattern: SNS + SQS

```
                          +--> [SQS billing]   --> billing service
order-service --> SNS  ---+--> [SQS stock]     --> warehouse
     publish   "orders"   +--> [SQS analytics] --> analytics
```

Every service gets **its own queue** subscribed to the shared topic. It's a direct analogue of a fanout exchange with queues in RabbitMQ:

- a service can be down: messages wait in its queue;
- each service reads at its own pace, with its own DLQ;
- a new service attaches with a new queue and subscription, the publisher doesn't change.

```bash
awsl sns create-topic --name orders
TOPIC_ARN=arn:aws:sns:eu-central-1:123456789012:orders

awsl sqs create-queue --queue-name billing
BILLING_ARN=arn:aws:sqs:eu-central-1:123456789012:billing

awsl sns subscribe --topic-arn $TOPIC_ARN --protocol sqs --notification-endpoint $BILLING_ARN \
  --attributes RawMessageDelivery=true
```

In the cloud the queue needs an **access policy** that allows SNS to write to it:

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

Without it the subscription is created, but messages are silently not delivered. This is one of the most common problems with SNS + SQS.

## 4.3 Raw message delivery

By default SNS wraps the message in a JSON envelope, and the queue receives not your body but:

```json
{"Type":"Notification","MessageId":"...","TopicArn":"...","Message":"{\"order_id\":\"order-1\"}","Timestamp":"...", ...}
```

With the subscription attribute `RawMessageDelivery=true` the queue receives the original body, and message attributes become SQS attributes. For service-to-service integration almost always enable raw delivery.

## 4.4 Subscription filtering

A subscription can accept only the messages of the topic that match a **filter policy**:

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
# filter on the message body instead of attributes:
awsl sns set-subscription-attributes --subscription-arn $SUB_ARN \
  --attribute-name FilterPolicyScope --attribute-value MessageBody
```

Operators: exact values, `prefix`, `suffix`, `anything-but`, `numeric`, `exists`, `equals-ignore-case`, IP addresses. Filtering on the SNS side is cheaper than accepting everything and discarding in the consumer.

## 4.5 Delivery reliability

- SNS delivers to SQS and Lambda reliably, with many retries.
- For HTTP endpoints you configure a **delivery policy** (the number and spacing of retries).
- Each subscription can have a **DLQ** (a `RedrivePolicy` on the subscription): messages that couldn't be delivered to the subscriber land in an SQS queue for investigation.
- SNS standard is at-least-once, unordered. **SNS FIFO** gives ordering and deduplication, paired with SQS FIFO.

## 4.6 Large messages

Since September 2026 a topic can accept messages up to 1 MiB if you set the `MaximumMessageSize` attribute; such topics support SQS, Lambda and Firehose subscriptions and are limited to a hundred subscriptions. The default limit stays at 256 KiB. For even larger data use the claim check pattern: the object in S3, a reference in the message (there are ready-made Extended Client Libraries).

### Self-check questions

1. Why do you use SNS + SQS, rather than direct HTTP subscriptions, for reliable fan-out to services?
2. What happens if the queue has no policy allowing SNS to write to it?
3. What is `RawMessageDelivery` for?
4. How do you filter messages on the SNS side by a body field?

---

# Module 5. AWS EventBridge and Kinesis

## 5.1 EventBridge: an event bus

**Amazon EventBridge** is an event bus with routing by **content-based rules**. A publisher sends an event to the bus, and rules decide which targets receive it.

```
                         rule: source=shop.orders, detail-type=OrderCreated  --> Lambda
PutEvents --> [bus] ----+ rule: detail.amount > 10000                        --> Step Functions (anti-fraud)
                         rule: everything from source=shop.*                  --> SQS (audit)
```

Buses:

| Bus | What arrives there |
|---|---|
| **default** | AWS service events: changes in EC2, S3, CodePipeline, CloudTrail and hundreds of others |
| **custom** | Events from your applications |
| **partner** | Events from SaaS partners (Zendesk, Datadog, Shopify and others) |

EventBridge's main strength is **integrations**: reacting to AWS infrastructure and SaaS events without writing pollers.

## 5.2 Event structure and rules

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

A rule's pattern is JSON that must "match" the event:

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

`PutEvents` accepts up to 10 events per call and, like SQS batches, can **partially fail**: check `FailedEntryCount`.

## 5.3 Targets, retries and DLQs

- A rule has up to 5 targets: Lambda, SQS, SNS, Step Functions, Kinesis, API destinations (HTTP endpoints outside AWS with authentication and rate limiting) and many more.
- An **input transformer** reshapes the event before delivery to a target.
- On a delivery error EventBridge retries (by default up to 24 hours and 185 attempts); set a **DLQ on the target**, an SQS queue for undelivered events.
- Delivery is at-least-once, **ordering isn't guaranteed**, latency is usually hundreds of milliseconds. EventBridge doesn't fit strict ordering.

## 5.4 Archive and replay

A bus can **archive** events (all or by pattern) and **replay** them for a period into the same bus. That gives a limited ability to "re-read history", for example after a handler bug fix. It isn't a full log with offsets: that's what Kinesis or Kafka are for.

## 5.5 EventBridge Scheduler and Pipes

**Scheduler** handles delayed and recurring tasks: a one-off run at a given time, cron and rate expressions, time zones, millions of schedules, the same targets as the bus. It's the right answer to "send a reminder in 3 days", which an SQS delay (15 minutes max) can't handle.

```bash
awsl scheduler create-schedule --name remind-order-1 \
  --schedule-expression "at(2026-09-18T10:00:00)" --flexible-time-window Mode=OFF \
  --target '{"Arn":"arn:aws:sqs:eu-central-1:123456789012:reminders","RoleArn":"arn:aws:iam::123456789012:role/scheduler","Input":"{\"order_id\":\"order-1\"}"}'
```

**Pipes** link "source → filter → enrichment → target" without code: for example, read from SQS, filter, enrich via Lambda and send to Step Functions.

## 5.6 Kinesis Data Streams: a log in AWS

When you need a **log** — per-key ordering, replay, many independent readers, large flows — AWS offers two paths: **Kinesis Data Streams** and **Amazon MSK** (managed Kafka, see the [Kafka course](../kafka/README.md)).

| Kinesis concept | Kafka analogue |
|---|---|
| Stream | Topic |
| Shard | Partition |
| Partition key | Message key |
| Sequence number | Offset |
| KCL (Kinesis Client Library) + DynamoDB | Consumer group + `__consumer_offsets` |
| Enhanced fan-out | Dedicated throughput per reader |

- Modes: **on-demand** (scales itself) or **provisioned** (you set the shards). A shard handles roughly 1 MB/s or 1000 records/s of writes.
- Retention: 24 hours by default, extendable to 365 days.
- Ordering is guaranteed within a shard, that is, per partition key.

Kinesis or MSK: Kinesis is simpler to operate and tightly integrated with AWS; MSK gives the Kafka ecosystem (Connect, Streams, Schema Registry) and portability.

## 5.7 What to choose in AWS

| Task | Service |
|---|---|
| Task queue | SQS standard |
| A queue with per-entity ordering or deduplication | SQS FIFO |
| One event for several services | SNS + SQS |
| Content-based routing, AWS and SaaS events | EventBridge |
| Delayed tasks and schedules | EventBridge Scheduler |
| Log, streaming, replay | Kinesis Data Streams or MSK |
| Moving off RabbitMQ without a rewrite | Amazon MQ |

### Self-check questions

1. How does EventBridge differ from SNS?
2. Why must you check `PutEvents` for partial failures?
3. How do you schedule an event three days out in AWS?
4. When do you need Kinesis or MSK instead of SQS?

---

# Module 6. Azure Service Bus: queues, topics, sessions

## 6.1 What Service Bus is

**Azure Service Bus** is an enterprise message broker: queues, topics with subscriptions, sessions for ordering, deduplication, scheduled messages, DLQs and transactions. Of the three clouds, it's the service **closest to RabbitMQ** in features, and it speaks **AMQP 1.0**.

```
namespace (shop-bus.servicebus.windows.net)
  ├── queue "tasks"                     -> workers (competing consumers)
  │     └── $DeadLetterQueue            (a sub-queue for dead messages)
  └── topic "orders"
        ├── subscription "billing"      rule: EventType = 'OrderCreated'
        │     └── $DeadLetterQueue
        ├── subscription "stock"        rule: EventType IN ('OrderCreated','OrderCancelled')
        └── subscription "audit"        rule: 1=1 (everything)
```

- A **namespace** is a container and access boundary, like a vhost in RabbitMQ.
- A **queue** serves one kind of recipient.
- **Topic + subscriptions** is pub/sub: each subscription behaves like a separate queue with its own filter rules. A direct analogue of a topic exchange + queues.

## 6.2 Tiers

| | Basic | Standard | Premium |
|---|---|---|---|
| Queues | Yes | Yes | Yes |
| Topics and subscriptions | **No** | Yes | Yes |
| Sessions, deduplication, transactions | No | Yes | Yes |
| Max message size | 256 KiB | 256 KiB | Up to 100 MB |
| Resources | Shared | Shared | Dedicated (messaging units), predictable latency |
| Private network (private endpoints), geo-replication | No | No | Yes |

For production with requirements on latency, isolation and networking, use Premium. Standard is fine for most integrations with moderate load.

## 6.3 Peek-lock and ways to settle a message

Service Bus hands out messages in **peek-lock** mode (the default): the message is locked to the receiver for `LockDuration` (1 minute by default, 5 minutes max), and the receiver must explicitly **settle** it:

| Action | Effect | Analogue |
|---|---|---|
| `complete` | Processed, delete it | ack |
| `abandon` | Return to the queue, delivery count +1 | nack with requeue |
| `dead-letter` | Send to the DLQ with a reason and description | reject to a DLX |
| `defer` | Set aside: the message stays in the queue but is handed out only by `sequence number` | — (a Service Bus feature of its own) |
| lock expired | The message is available again, count +1 | An expired visibility timeout |

**Receive-and-delete** mode deletes the message as soon as it's handed out: at-most-once, for data you can afford to lose.

After `MaxDeliveryCount` deliveries (**10** by default) the message automatically moves to the `$DeadLetterQueue` sub-queue. Messages with an expired TTL can go there too if `DeadLetteringOnMessageExpiration` is enabled.

## 6.4 Sending and receiving in Python

```bash
pip install azure-servicebus azure-identity
```

```python
import json
from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage, AutoLockRenewer

# in the cloud: Entra ID instead of a connection string
client = ServiceBusClient("shop-bus.servicebus.windows.net", credential=DefaultAzureCredential())
# for the emulator: ServiceBusClient.from_connection_string(CONN)  (see module 2.5)

with client:
    with client.get_queue_sender("tasks") as sender:
        sender.send_messages(ServiceBusMessage(
            json.dumps({"task": "resize", "image": "1.png"}),
            message_id="resize-1.png",                  # for deduplication
            application_properties={"event-type": "ResizeRequested"},
        ))

    renewer = AutoLockRenewer(max_lock_renewal_duration=600)    # renew the lock for up to 10 minutes
    with client.get_queue_receiver("tasks", max_wait_time=20, auto_lock_renewer=renewer) as receiver:
        for msg in receiver:
            try:
                process(json.loads(str(msg)))                    # must be idempotent
                receiver.complete_message(msg)
            except ValueError as e:
                receiver.dead_letter_message(msg, reason="InvalidPayload", error_description=str(e))
            except TemporaryError:
                receiver.abandon_message(msg)                   # comes back, delivery count +1
```

`AutoLockRenewer` renews the lock while long processing runs: without it a message processed for longer than `LockDuration` is handed to a second receiver.

The same in Go (`azservicebus`):

```go
import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

cred, _ := azidentity.NewDefaultAzureCredential(nil)
client, err := azservicebus.NewClient("shop-bus.servicebus.windows.net", cred, nil)
// for the emulator: azservicebus.NewClientFromConnectionString(conn, nil)

sender, _ := client.NewSender("tasks", nil)
err = sender.SendMessage(ctx, &azservicebus.Message{
	Body:                  []byte(`{"task":"resize","image":"1.png"}`),
	MessageID:             to.Ptr("resize-1.png"), // for deduplication
	ApplicationProperties: map[string]any{"event-type": "ResizeRequested"},
}, nil)

receiver, _ := client.NewReceiverForQueue("tasks", nil) // peek-lock by default
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

The Go SDK has no ready-made equivalent of `AutoLockRenewer`: for long processing call `receiver.RenewMessageLock(ctx, m, nil)` in the background. The full consumer is in `examples/go/cmd/servicebus-worker`.

Reading the DLQ for investigation:

```python
from azure.servicebus import ServiceBusSubQueue

with client.get_queue_receiver("tasks", sub_queue=ServiceBusSubQueue.DEAD_LETTER, max_wait_time=5) as dlq:
    for msg in dlq:
        print(msg.dead_letter_reason, msg.dead_letter_error_description, str(msg))
        dlq.complete_message(msg)
```

## 6.5 Sessions: per-entity ordering

A **session** is a group of messages with the same `session_id`. A queue or subscription with sessions enabled (`requires_session=True`) guarantees that:

- a session's messages are handed out **strictly in order**;
- the whole session is processed by **one** receiver at a time (it "locks" the session);
- different sessions are processed in parallel.

It's the analogue of `MessageGroupId` in SQS FIFO and ordering keys in Pub/Sub.

```python
from azure.servicebus import NEXT_AVAILABLE_SESSION

with client.get_queue_sender("orders-ordered") as sender:
    for event in ["OrderCreated", "OrderPaid", "OrderShipped"]:
        sender.send_messages(ServiceBusMessage(json.dumps({"event": event}), session_id="order-1"))

with client.get_queue_receiver("orders-ordered", session_id=NEXT_AVAILABLE_SESSION,
                               max_wait_time=5) as receiver:
    print("processing session", receiver.session.session_id)
    for msg in receiver:
        handle(msg)
        receiver.complete_message(msg)
```

A session has **state** (`session.set_state` / `get_state`): a small blob of data stored in Service Bus that survives a change of receiver. Handy for state machines and sagas.

## 6.6 Deduplication and scheduled messages

**Duplicate detection** is enabled when a queue or topic is created (`requires_duplicate_detection=True`) and configured with a window (10 minutes by default). A message with a `message_id` already seen in the window is accepted but **silently dropped**. It protects against duplicates on resend after a failure, provided the `message_id` is stable for the business event.

**Scheduled messages** become available at a given time:

```python
from datetime import datetime, timedelta, timezone

with client.get_queue_sender("reminders") as sender:
    seq = sender.schedule_messages(ServiceBusMessage('{"order_id":"order-1"}'),
                                   datetime.now(timezone.utc) + timedelta(days=3))
    # sender.cancel_scheduled_messages(seq)   # changed our mind
```

## 6.7 Topics, subscriptions and rules

```bash
az servicebus topic create -g shop-rg --namespace-name shop-bus -n orders
az servicebus topic subscription create -g shop-rg --namespace-name shop-bus --topic-name orders \
  -n billing --max-delivery-count 5 --enable-dead-lettering-on-message-expiration true

# replace the default rule (everything) with a filter
az servicebus topic subscription rule delete -g shop-rg --namespace-name shop-bus \
  --topic-name orders --subscription-name billing -n '$Default'
az servicebus topic subscription rule create -g shop-rg --namespace-name shop-bus \
  --topic-name orders --subscription-name billing -n created-only \
  --filter-sql-expression "EventType = 'OrderCreated' AND Amount > 0"
```

| Filter | How | When |
|---|---|---|
| **SQL filter** | An expression over message properties (`application_properties`) | Flexible conditions |
| **Correlation filter** | Exact match on `correlation_id`, `subject`, properties | Faster than SQL, covers most cases |
| **True filter** (`1=1`) | Everything | The `$Default` rule when a subscription is created |

Rules filter on **properties**, not the body: put the fields used for routing into `application_properties`.

**Auto-forwarding** forwards messages from a queue or subscription to another queue or topic in the same namespace: that's how you build chains and branches without code.

## 6.8 Transactions

Within a namespace, Service Bus supports transactions: receive a message, send new ones and complete the original **atomically** (via `send-via`). That gives consume-transform-produce without duplicates inside Service Bus, like Kafka transactions. The transaction doesn't extend to an external database: there you need idempotency (module 11).

### Self-check questions

1. How does `abandon` differ from `dead-letter` and `defer`?
2. What is `AutoLockRenewer` for?
3. How do sessions provide ordering, and how is that like SQS FIFO?
4. Why do subscription filters work on properties rather than the message body?
5. Which tier do you need for topics, and which for private endpoints?

---

# Module 7. Azure Event Grid and Event Hubs

## 7.1 Event Grid: event routing

**Azure Event Grid** is an event bus, the analogue of EventBridge. Its main scenarios:

- react to **Azure resource events**: a file appeared in Blob Storage, a virtual machine was created, a secret changed in Key Vault;
- distribute **application events** to subscribers with filtering.

| Concept | What it is |
|---|---|
| **System topic** | Events from an Azure service (Storage, Resource Groups, Key Vault...) |
| **Custom topic** | Events from your application |
| **Domain** | Many topics behind one endpoint (for multi-tenant systems) |
| **Event subscription** | A subscription with a filter and a handler |

```bash
# react to new files in the uploads container and send events to a Service Bus queue
az eventgrid event-subscription create -n new-uploads \
  --source-resource-id $STORAGE_ACCOUNT_ID \
  --included-event-types Microsoft.Storage.BlobCreated \
  --subject-begins-with /blobServices/default/containers/uploads/ \
  --endpoint-type servicebusqueue --endpoint $SERVICEBUS_QUEUE_ID \
  --max-delivery-attempts 30 --event-ttl 1440
```

Features:

- **push** delivery to handlers: Azure Functions, webhooks, Service Bus, Storage Queues, Event Hubs;
- retries with backoff (by default up to 24 hours and 30 attempts), **dead-lettering to Blob Storage** for undelivered events;
- supports the **CloudEvents 1.0** format (module 17);
- **Event Grid namespaces** add HTTP pull delivery and a built-in **MQTT broker** for IoT.

A common pattern: Event Grid → Service Bus queue → workers. Event Grid is good at routing, while the queue gives a buffer, pace control and peek-lock.

## 7.2 Event Hubs: a log in Azure

**Azure Event Hubs** is a service for large event streams, modelled on **Kafka**:

| Event Hubs concept | Kafka analogue |
|---|---|
| Namespace | Cluster |
| Event hub | Topic |
| Partition | Partition |
| Consumer group | Consumer group |
| Offset / sequence number | Offset |
| Checkpoint (in Blob Storage) | Committed offset |

- Partitions are set at creation; ordering is guaranteed within a partition (by partition key).
- Retention ranges from an hour to 7 days on Standard, up to 90 days on Premium and Dedicated.
- **Kafka endpoint** (Standard and above): ordinary Kafka clients connect to Event Hubs by changing only the address and authentication. Everything in the [Kafka course](../kafka/README.md) about producers and consumer groups applies, but it isn't real Kafka: there's no access to brokers, part of the Kafka API and topic settings is supported with limitations, and the limits are its own. Check the list of supported features before migrating.
- **Capture** automatically exports the stream to Blob Storage or Data Lake in Avro/Parquet.
- It has its own **Schema Registry**.

```python
from azure.eventhub import EventHubProducerClient, EventData

producer = EventHubProducerClient(fully_qualified_namespace="shop-hub.servicebus.windows.net",
                                  eventhub_name="clicks", credential=DefaultAzureCredential())
with producer:
    batch = producer.create_batch(partition_key="user-42")    # order per user
    batch.add(EventData('{"page":"/cart"}'))
    producer.send_batch(batch)
```

The stream is read with `EventProcessorClient`/`EventHubConsumerClient` with a checkpoint store in Blob Storage: the library spreads partitions across instances, like a consumer group in Kafka.

## 7.3 What to choose in Azure

| Task | Service |
|---|---|
| A task queue with reliable delivery | Service Bus queue |
| Per-entity ordering | Service Bus with sessions |
| Pub/sub between services with filters | Service Bus topics |
| Reacting to Azure resource events, routing | Event Grid |
| Large streams, telemetry, a log, Kafka clients | Event Hubs |
| A very simple and cheap queue | Storage Queues |
| MQTT for devices | Event Grid namespaces (MQTT broker) or IoT Hub |

### Self-check questions

1. How does Event Grid differ from Service Bus topics?
2. Where does Event Grid send events it couldn't deliver?
3. What do Event Hubs and Kafka have in common, and how does Event Hubs with the Kafka endpoint differ from real Kafka?

---

# Module 8. Google Cloud Pub/Sub

## 8.1 The Pub/Sub model

**Google Cloud Pub/Sub** is a global messaging service that covers both the **queue** and **pub/sub** in one product:

```
publisher --> [topic orders] --+--> subscription "billing"   (pull) --> 3 workers share messages
                               +--> subscription "stock"     (pull) --> 2 workers
                               +--> subscription "webhook"   (push) --> an HTTPS endpoint
                               +--> subscription "bq"        (BigQuery) --> a table
```

- A message published to a **topic** is copied into **every subscription**.
- Within a subscription, all of its receivers **share** the messages (competing consumers).
- So: topic + one subscription = a queue; topic + several subscriptions = fan-out.

It's the same model as an exchange + queues in RabbitMQ, but without a separate "queue" concept: the subscription plays that role.

## 8.2 Subscription types

| Type | How | When |
|---|---|---|
| **Pull** (streaming pull) | The client library holds a stream and receives messages | Workers, the main option |
| **Push** | Pub/Sub calls an HTTPS endpoint, 2xx = ack | Cloud Run, serverless, webhooks |
| **BigQuery** | Writes messages straight into a table | Analytics without code |
| **Cloud Storage** | Writes files to a bucket | Archive, data lake |

## 8.3 Ack deadline and redelivery

A received message must be acknowledged (`ack`) before the **ack deadline** expires (10 seconds by default, from 10 seconds to 10 minutes). No ack means redelivery. The client libraries **extend the deadline automatically** while a message is being processed (lease management), up to a configurable limit.

- `ack`: processed;
- `nack`: return immediately (or with a delay according to the retry policy);
- nothing: redelivery after the deadline expires.

The subscription's **retry policy**: immediate, or exponential delay between `min-retry-delay` and `max-retry-delay` (both 0–600 s; defaults 10 s and 600 s).

## 8.4 Publishing and receiving in Python

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

# publish returns a future: the result is the message id once the service confirms
future = publisher.publish(
    topic,
    json.dumps({"order_id": "order-1", "amount": 4990}).encode(),
    event_type="OrderCreated",          # attributes: string -> string
)
print("published:", future.result(timeout=30))

subscriber = pubsub_v1.SubscriberClient()
sub = subscriber.subscription_path(project, "billing")

def callback(message: pubsub_v1.subscriber.message.Message):
    try:
        process(json.loads(message.data))        # must be idempotent
        message.ack()
    except ValueError:
        message.ack()                            # garbage: don't retry (or send to your own DLQ)
    except Exception:
        message.nack()                           # return according to the retry policy

flow = pubsub_v1.types.FlowControl(max_messages=100)   # the equivalent of prefetch
streaming = subscriber.subscribe(sub, callback=callback, flow_control=flow)
with subscriber:
    try:
        streaming.result()                       # blocks until stopped
    except (KeyboardInterrupt, TimeoutError):
        streaming.cancel()
        streaming.result()
```

The same in Go (`cloud.google.com/go/pubsub/v2`):

```go
import "cloud.google.com/go/pubsub/v2"

client, err := pubsub.NewClient(ctx, "my-project") // with PUBSUB_EMULATOR_HOST set: the emulator

publisher := client.Publisher("orders")
defer publisher.Stop()
id, err := publisher.Publish(ctx, &pubsub.Message{
	Data:       []byte(`{"order_id":"order-1","amount":4990}`),
	Attributes: map[string]string{"event_type": "OrderCreated"},
}).Get(ctx) // wait for the service to confirm

sub := client.Subscriber("billing")
sub.ReceiveSettings.MaxOutstandingMessages = 100 // the equivalent of prefetch
err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
	if err := process(m.Data); err != nil {
		m.Nack() // return according to the retry policy
		return
	}
	m.Ack() // the client extends the ack deadline while the callback runs
})
```

In Go, topics and subscriptions are created via `client.TopicAdminClient` and `client.SubscriptionAdminClient` (types from `pubsubpb`). The full consumer is in `examples/go-gcp/cmd/pubsub-worker`.

- `FlowControl` limits the number of messages in flight: it's Pub/Sub's prefetch. Without it a fast stream can overflow a worker's memory.
- Callbacks run in a thread pool: processing must be thread-safe.
- The publisher batches messages itself; batching settings are `BatchSettings`.

## 8.5 Dead letter topic

```bash
gcloud pubsub topics create orders-dlq
gcloud pubsub subscriptions create billing --topic orders \
  --ack-deadline 60 \
  --dead-letter-topic orders-dlq --max-delivery-attempts 5 \
  --min-retry-delay 10s --max-retry-delay 600s
gcloud pubsub subscriptions create orders-dlq-sub --topic orders-dlq
```

- After `max-delivery-attempts` (from 5 to 100) the message is published to the dead letter **topic**. So messages there aren't lost, the DLQ topic must have a **subscription**.
- The Pub/Sub service account needs permission to **publish** to the DLQ topic and **subscriber** permission on the source subscription, otherwise dead lettering silently doesn't work. In the console a button sets this up; in Terraform, explicit IAM bindings (module 16).
- The consumer sees the attempt count in `message.delivery_attempt`.

## 8.6 Ordering: ordering keys

```bash
gcloud pubsub subscriptions create stock --topic orders --enable-message-ordering
```

```python
publisher = pubsub_v1.PublisherClient(
    publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True))
for event in ["OrderCreated", "OrderPaid", "OrderShipped"]:
    publisher.publish(topic, json.dumps({"event": event}).encode(), ordering_key="order-1")
```

- Messages with the same `ordering_key` published in **one region** are delivered in order to subscribers with ordering enabled. Different keys go in parallel.
- If a publish with a key fails, the publisher pauses that key so as not to break the order; after handling the error, call `publisher.resume_publish(topic, ordering_key)`.
- Ordering lowers per-key throughput and raises latency: enable it only where order really matters.

## 8.7 Exactly-once delivery

A pull subscription with `--enable-exactly-once-delivery` guarantees that a message isn't redelivered while it's outstanding (before its ack deadline expires), and a successfully acknowledged message **won't be redelivered** at all:

```python
from google.cloud.pubsub_v1.subscriber import exceptions as sub_exceptions

def callback(message):
    process(message.data)
    ack_future = message.ack_with_response()      # find out whether the service accepted the ack
    try:
        ack_future.result()                       # success: no redelivery
    except sub_exceptions.AcknowledgeError as e:
        print("ack not accepted, the message will be redelivered:", e.error_code)
```

Honest limitations:

- it works for **pull** subscriptions and within a region;
- it protects against repeated **delivery**, not repeated publishing: if the publisher sent a message twice, those are two different messages;
- if processing writes to an external database, you still need idempotency (module 11);
- it lowers throughput and raises latency.

## 8.8 Filters, seek and retention

A **subscription filter** works on attributes, is set at creation and **can't change**:

```bash
gcloud pubsub subscriptions create billing-eu --topic orders \
  --message-filter='attributes.event_type = "OrderCreated" AND hasPrefix(attributes.region, "eu-")'
```

Pub/Sub acknowledges filtered-out messages automatically: you don't pay outbound (egress) fees for them, but message delivery fees still apply.

**Retention and seek** give replay:

- a subscription keeps unacknowledged messages up to 7 days; with `--retain-acked-messages`, acknowledged ones too;
- a topic can keep messages up to 31 days (`--message-retention-duration`), so a new subscription can read the past;
- **seek** rewinds a subscription to a point in time or a **snapshot**:

```bash
gcloud pubsub snapshots create before-deploy --subscription billing   # before a risky release
gcloud pubsub subscriptions seek billing --snapshot before-deploy      # roll back after a bug
gcloud pubsub subscriptions seek billing --time 2026-09-15T10:00:00Z
```

It isn't a full log with offsets like Kafka, but in practice it covers the main replay scenarios.

## 8.9 Push subscriptions

Pub/Sub sends a POST to an HTTPS endpoint; a 2xx response is an ack, anything else means a retry with backoff. For Cloud Run and other Google services the endpoint is protected by an **OIDC token** of a service account that Pub/Sub attaches to the request. Push is convenient for serverless, but Pub/Sub controls the delivery pace: the handler must withstand bursts.

### Self-check questions

1. How do you get a queue in Pub/Sub, and how do you get fan-out?
2. What is `FlowControl` for?
3. Which permissions does the Pub/Sub service account need for a dead letter topic to work?
4. What does exactly-once delivery protect against, and what doesn't it?
5. How do you roll a subscription back to its state before a bad release?

---

# Module 9. Google Eventarc, Cloud Tasks and Managed Kafka

## 9.1 Eventarc: event routing

**Eventarc** is Google Cloud's analogue of EventBridge and Event Grid: it delivers events to Cloud Run, GKE and Workflows.

Event sources:

- **direct events** from Google services (for example, an object created in Cloud Storage);
- **Cloud Audit Logs**: almost any action on Google Cloud resources;
- **Pub/Sub** topics, including your applications' events;
- third-party providers.

```bash
gcloud eventarc triggers create uploads-trigger \
  --location=europe-west1 \
  --destination-run-service=thumbnailer --destination-run-region=europe-west1 \
  --event-filters="type=google.cloud.storage.object.v1.finalized" \
  --event-filters="bucket=shop-uploads" \
  --service-account=eventarc-sa@my-project.iam.gserviceaccount.com
```

Events arrive in the **CloudEvents** format. Under the hood Eventarc uses Pub/Sub, so the guarantees are similar: at-least-once, unordered. **Eventarc Advanced** adds a message bus and pipelines with filtering and transformation, closer to a full EventBridge.

## 9.2 Cloud Tasks: a queue of HTTP tasks

**Cloud Tasks** solves a different problem than Pub/Sub: not "distribute an event" but "perform a specific HTTP call, with pace control, at the right time".

| | Pub/Sub | Cloud Tasks |
|---|---|---|
| Who decides where to deliver | Subscribers | The publisher sets the target in each task |
| Pace control | Flow control at the receiver | On the queue: `max-dispatches-per-second`, `max-concurrent-dispatches` |
| Delayed execution | No | `schedule_time` in the future (up to 30 days) |
| Deduplication | Exactly-once on the subscription | By task name |
| Fan-out | Yes | No |

```bash
gcloud tasks queues create emails --location=europe-west1 \
  --max-dispatches-per-second=10 --max-concurrent-dispatches=5 \
  --max-attempts=10 --min-backoff=5s --max-backoff=300s
```

Typical uses: calls to an external API with a rate limit, delayed actions ("remind in 2 days"), offloading heavy work from an HTTP handler.

## 9.3 Managed Service for Apache Kafka

For a log in Google Cloud there's **Managed Service for Apache Kafka**: real Kafka managed by Google, with Kafka Connect support. Everything in the [Kafka course](../kafka/README.md) applies directly. It's what Google suggested migrating to from the shut-down Pub/Sub Lite.

Pub/Sub or Kafka in Google Cloud:

| You need | Choice |
|---|---|
| Service integration, queues, push to Cloud Run, minimal operations | Pub/Sub |
| The Kafka ecosystem, per-partition ordering, long retention, portability | Managed Kafka |
| Analytics in BigQuery without code | Pub/Sub with a BigQuery subscription |

## 9.4 What to choose in Google Cloud

| Task | Service |
|---|---|
| Task queue | Pub/Sub, pull subscription |
| Fan-out | Pub/Sub, several subscriptions |
| Per-entity ordering | Pub/Sub ordering keys |
| No redeliveries | Pub/Sub exactly-once |
| Reacting to Google Cloud resource events | Eventarc |
| HTTP tasks with rate control and delayed start | Cloud Tasks |
| Schedules | Cloud Scheduler |
| Log, the Kafka ecosystem | Managed Service for Apache Kafka |

### Self-check questions

1. How does Cloud Tasks differ from Pub/Sub?
2. In what format does Eventarc deliver events?
3. What did Google suggest migrating to from Pub/Sub Lite?

---

# Module 10. Patterns across three clouds: mapping tables

This module is a cheat sheet for anyone who knows one cloud and is moving to another, or designing a system that must live in several.

## 10.1 Task queue

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Service | SQS standard | Service Bus queue | Pub/Sub, pull subscription |
| Receive | `ReceiveMessage` (long polling 20 s) | receiver (peek-lock) | streaming pull |
| Acknowledge | `DeleteMessage` | `complete` | `ack` |
| Return | Don't delete, or `ChangeMessageVisibility(0)` | `abandon` | `nack` |
| Processing time | Visibility timeout (up to 12 h) | Lock duration (up to 5 min) + renewal | Ack deadline (up to 10 min) + auto-extension |
| Extend | `ChangeMessageVisibility` | `renew_message_lock` / `AutoLockRenewer` | Automatically in the client |
| Limit concurrency | `MaxNumberOfMessages` and the number of threads | `prefetch_count`, number of receivers | `FlowControl` |
| Attempt counter | `ApproximateReceiveCount` | `delivery_count` | `delivery_attempt` (with a DLQ) |

## 10.2 Fan-out

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| How | An SNS topic + an SQS queue per service | A Service Bus topic + a subscription per service | A Pub/Sub topic + a subscription per service |
| Filter | Subscription filter policy (attributes or body) | SQL/correlation filter (properties) | Message filter (attributes, immutable) |
| Don't forget | The SQS access policy for SNS, `RawMessageDelivery` | Delete the `$Default` rule if you need a filter | A subscription on the DLQ topic, service account permissions |

## 10.3 Per-entity ordering

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Mechanism | SQS FIFO, `MessageGroupId` | Sessions, `session_id` | Ordering keys |
| Parallelism | Across groups | Across sessions | Across keys |
| Cost | Lower throughput than standard | Standard tier or above | Lower per-key throughput, one region |

In all three clouds the model is the same and matches Kafka: **ordering within a key, parallelism across keys**.

## 10.4 Deduplication and exactly-once

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Publish deduplication | SQS FIFO: `MessageDeduplicationId`, 5-min window | Duplicate detection by `message_id`, configurable window | No |
| Protection from redelivery | FIFO: a group's message isn't handed out in parallel | Peek-lock, sessions | Exactly-once delivery on a pull subscription |
| Atomicity inside the broker | — | Transactions within a namespace | — |

None of the services gives exactly-once **with an external database**. Everywhere you need an idempotent consumer (module 11).

## 10.5 DLQ and redrive

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| How to enable | `RedrivePolicy` with `maxReceiveCount` | Built in: the `$DeadLetterQueue` sub-queue, `MaxDeliveryCount` (10) | A dead letter topic + `max-delivery-attempts` (5–100) |
| Reason in the message | No (attributes only) | `dead_letter_reason`, `dead_letter_error_description` | Attributes with the source subscription and attempt count |
| Explicit dead-lettering by the consumer | No (only via the attempt limit or your own publish) | `dead_letter_message` | No (ack + publish to your own topic) |
| Bring back from the DLQ | Redrive (`StartMessageMoveTask`) | Read the DLQ and resend | A subscription on the DLQ topic and resend |

## 10.6 Delays, retries and schedules

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Message delay | `DelaySeconds` up to 15 min | Scheduled messages (any time) | Not in Pub/Sub; Cloud Tasks `schedule_time` |
| Retry with backoff | `ChangeMessageVisibility` with a growing delay | `abandon` + your own backoff or a scheduled resend | The subscription's retry policy (0–600 s) |
| Schedules | EventBridge Scheduler | Scheduled messages, Logic Apps | Cloud Scheduler, Cloud Tasks |

## 10.7 Replay

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Queue | No | No | Seek by time or snapshot, topic retention up to 31 days |
| Event bus | EventBridge archive & replay | — | — |
| Log | Kinesis, MSK | Event Hubs | Managed Kafka |

## 10.8 Large messages

| | AWS | Azure | Google Cloud |
|---|---|---|---|
| Limit | SQS 1 MiB, SNS 256 KiB (up to 1 MiB with the attribute), EventBridge 1 MB | 256 KiB Standard, up to 100 MB Premium | 10 MB |
| Beyond the limit | Claim check via S3 (Extended Client Library) | Claim check via Blob Storage | Claim check via Cloud Storage |

**Claim check** is a universal pattern: the large object goes to object storage, and the message carries a reference and metadata. Even when the limit allows it, megabytes in every message cost a lot (billing by volume) and are slower to process.

## 10.9 Request-reply

Cloud brokers are poorly suited to synchronous RPC: HTTPS call latency, no analogue of direct reply-to (except Service Bus sessions, which can be used for replies). For request-response between services use HTTP or gRPC, and the broker for asynchronous events and tasks.

---

# Module 11. Delivery guarantees, idempotency and outbox

## 11.1 At-least-once is the norm

Almost all cloud brokers default to **at-least-once**:

| Service | Why duplicates are possible |
|---|---|
| SQS standard | Distributed storage: rare redeliveries are part of the design |
| SQS FIFO | Redelivery after the visibility timeout if the consumer didn't delete in time |
| SNS, EventBridge, Event Grid | Retried deliveries to the target |
| Service Bus | The lock expired before `complete`; `complete` didn't reach the service |
| Pub/Sub | The ack got lost or arrived after the deadline (without exactly-once) |
| Lambda, Functions, Cloud Run | A repeated invocation on an error or timeout |

The conclusion is the same as always: **the consumer must be idempotent**. The cloud doesn't change that.

## 11.2 An idempotent consumer in the cloud

The store of processed ids is chosen in the same cloud, with a conditional write:

| Cloud | Store | Conditional write |
|---|---|---|
| AWS | DynamoDB | `PutItem` with `ConditionExpression="attribute_not_exists(pk)"` |
| Azure | Cosmos DB | `create_item` (a conflict on the id) |
| Google Cloud | Firestore / Spanner | `create` a document (fails if it exists) / an insert in a transaction |
| Any | PostgreSQL | `INSERT ... ON CONFLICT DO NOTHING` in a transaction with the business change |

```python
import boto3
from botocore.exceptions import ClientError

table = boto3.resource("dynamodb").Table("processed-messages")

def handle_once(message_id: str, work) -> bool:
    try:
        table.put_item(
            Item={"pk": message_id, "ttl": expires_in_days(7)},   # DynamoDB TTL cleans up old items
            ConditionExpression="attribute_not_exists(pk)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False                  # already processed: just acknowledge the message
        raise
    work()
    return True
```

Mind the order: if `work()` fails after the id is written, the retry will be skipped. Reliable options:

- **the business change and the mark in one transaction** (one database, a transaction or `TransactWriteItems` in DynamoDB);
- **natural idempotency**: an `UPSERT` by key, a conditional update on a version;
- a mark with an `in_progress`/`done` state and a retry for stuck ones.

Which id to use:

| Source | Identifier |
|---|---|
| Your own producer | Your own stable `event_id` in the body or attributes: the best option |
| SQS | `MessageId` (changes if the producer resends) |
| Service Bus | `message_id` (set by the sender) |
| Pub/Sub | `message_id` (assigned by the service on publish) |
| EventBridge, Event Grid, Eventarc | The event `id` (CloudEvents `id` + `source`) |

An id assigned **by the broker** doesn't protect against a repeated **publish**: if the producer sent the event twice, it has two different ids. So the business id of an event must be set by the producer.

## 11.3 Transactional outbox in the cloud

The dual-write problem is the same as everywhere (see the [Kafka](../kafka/README.md) and [RabbitMQ](../rabbit/README.md) courses): you can't atomically write to a database and publish an event. The solution is an outbox, and the clouds offer convenient ways to deliver it without your own poller:

| Cloud | Database → change stream → broker |
|---|---|
| AWS | DynamoDB: the order and the event written in one transaction → **DynamoDB Streams** → **EventBridge Pipes** → EventBridge or SQS |
| AWS | Aurora/RDS PostgreSQL: an outbox table → Debezium or your own relay → SQS/SNS/EventBridge |
| Azure | Cosmos DB: the order document and the event in one transactional batch → **Change Feed** → Azure Function → Service Bus |
| Google Cloud | Spanner: a write in one transaction → **change streams** → Dataflow → Pub/Sub |
| Any | PostgreSQL outbox → **Debezium Server** (supports Pub/Sub, Kinesis, Event Hubs and other sinks) |

```
+----------- one database transaction ------------+
| the order      +  the OrderCreated event         |
+-------------------------------------------------+
            |
            v  the database's change stream (Streams / Change Feed / change streams / WAL)
   relay (Pipes / Function / Dataflow / Debezium)
            |
            v
   EventBridge / Service Bus / Pub/Sub  ->  consumers (idempotent)
```

The relay can deliver an event twice, so the event always carries a stable `event_id`, and consumers deduplicate by it.

## 11.4 Partial batch failures: the main cloud trap

Cloud APIs are almost always batched, and almost everywhere a batch can **partially** fail:

| API | How to detect a partial failure |
|---|---|
| SQS `SendMessageBatch`, `DeleteMessageBatch` | The `Failed` field in the response |
| SNS `PublishBatch` | The `Failed` field |
| EventBridge `PutEvents` | `FailedEntryCount` and `ErrorCode` in `Entries` |
| Kinesis `PutRecords` | `FailedRecordCount` |
| Pub/Sub `publish` | A separate future per message |
| Lambda + SQS | The `batchItemFailures` response (module 12) |

HTTP 200 **doesn't mean** every message was accepted. It's a common cause of "mysterious" losses.

## 11.5 Choosing guarantees

| Task | Recommendation |
|---|---|
| Metrics, logs | Standard services, losing a rare message is acceptable |
| Notifications | At-least-once + deduplication by event id |
| Business events | Outbox + at-least-once + an idempotent consumer + a DLQ with an alert |
| Strict per-entity ordering | SQS FIFO / Service Bus sessions / Pub/Sub ordering keys |
| Money | All of the above + a transactional processing mark + reconciliation |

### Self-check questions

1. Why doesn't an id assigned by the broker protect against a repeated publish?
2. What's dangerous about "write the processed message id, then do the work"?
3. How do you implement an outbox in AWS without your own poller?
4. Why doesn't HTTP 200 from a batch API guarantee that every message was accepted?

---

# Module 12. Serverless consumers: Lambda, Azure Functions, Cloud Run

## 12.1 How functions read queues

In a serverless architecture the consumer isn't a long-lived process but a function the platform invokes:

```
queue / subscription --> platform (event source mapping, trigger, push) --> function
                           |  takes a batch of messages
                           |  invokes the function
                           |  depending on the result: deletes / returns messages
```

The platform takes care of polling the queue and scaling, but **the acknowledgement semantics** now depend on how the function finished. The main question for every integration: **what happens to the batch if one message out of ten failed?**

## 12.2 AWS Lambda + SQS

An **event source mapping** polls SQS and invokes the function with a batch of messages:

| Parameter | Meaning |
|---|---|
| `BatchSize` | Messages per batch (standard: up to 10,000 with a batching window; FIFO: up to 10) |
| `MaximumBatchingWindowInSeconds` | How long to wait to fill a batch (up to 300 s) |
| `MaximumConcurrency` | A ceiling on concurrent invocations for this queue: protects the database from a burst |
| `FunctionResponseTypes: ReportBatchItemFailures` | Lets the function report which messages of the batch failed |

**Without** `ReportBatchItemFailures` a function error returns **the whole batch** to the queue: nine successfully processed messages get processed again. With it, the function returns the list of failures:

```python
import json

def handler(event, context):
    failures = []
    for record in event["Records"]:
        try:
            process(json.loads(record["body"]))          # idempotent
        except Exception:
            failures.append({"itemIdentifier": record["messageId"]})
    # Lambda deletes the successful messages, failed ones return to the queue
    return {"batchItemFailures": failures}
```

Rules:

- the queue's visibility timeout must be **at least six times the function timeout**: otherwise a message becomes visible while the function is still running and gets processed twice;
- the DLQ is configured **on the queue** (`RedrivePolicy`), not on the function: function destinations and DLQs aren't used for the SQS trigger;
- `MaximumConcurrency` is the main tool for stopping a thousand parallel functions from taking down the database;
- for a FIFO queue on a partial failure return the failed message **and all later ones** in the same group, or ordering breaks.

AWS detects **recursive loops** (a function writes to a queue that invokes the same function) and stops them after about 16 iterations. It's protection against an endless bill, not an invitation to build such chains.

## 12.3 AWS Lambda + SNS and EventBridge

SNS and EventBridge invoke Lambda **asynchronously**: on an error Lambda retries itself (twice by default), then sends the event to a **destination** or the function's DLQ. It's more reliable to put a queue in between: SNS/EventBridge → SQS → Lambda. Then you have a buffer, concurrency control and `ReportBatchItemFailures`.

## 12.4 Azure Functions + Service Bus

The Service Bus trigger receives messages in peek-lock mode and by default **settles them itself**: a successful run means `complete`, an exception means `abandon` (after `MaxDeliveryCount` the message goes to the DLQ).

```python
import json
import azure.functions as func

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="msg", queue_name="tasks", connection="ServiceBusConnection")
def process_task(msg: func.ServiceBusMessage):
    payload = json.loads(msg.get_body().decode())
    process(payload)                 # exception -> abandon -> retry -> DLQ after MaxDeliveryCount
```

Important settings in `host.json`:

| Setting | Meaning |
|---|---|
| `maxConcurrentCalls` | Concurrent messages per instance |
| `maxAutoLockRenewalDuration` | How long to renew the lock during long processing |
| `autoCompleteMessages` | Settle automatically (turn off if you settle manually) |
| `isSessionsEnabled` | Working with sessions, per-entity ordering |

Connect via **managed identity** (`ServiceBusConnection__fullyQualifiedNamespace`), without a connection string with a key.

## 12.5 Cloud Run + Pub/Sub

Two ways:

A **push subscription** (or an Eventarc trigger) calls the Cloud Run service over HTTPS:

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
        process(data)                         # idempotent by msg["messageId"] or your own id
    except TemporaryError:
        return ("retry later", 503)           # non-2xx -> Pub/Sub retries with backoff
    return ("", 204)                          # 2xx -> ack
```

- the push subscription's ack deadline (up to 600 s) must cover the processing time, and the Cloud Run request timeout must be at least as long;
- for an invalid message return 2xx (and log it or send it to your own DLQ), otherwise Pub/Sub keeps retrying until the dead letter topic;
- protect the endpoint with an OIDC token: allow calls only from the Pub/Sub service account.

**Pull in a long-lived service** (for example, a Cloud Run worker pool or GKE) is the ordinary streaming pull from module 8: full pace control via `FlowControl`.

## 12.6 General rules for serverless consumers

1. **Partial batch failures**: `batchItemFailures` in Lambda, settling messages individually in Functions, one message per push.
2. **Concurrency limits**, or function autoscaling takes down the database or an external API.
3. **Consistent timeouts**: visibility timeout / lock duration / ack deadline ≥ function run time, with headroom.
4. **A DLQ on the broker side** and an alert on it.
5. **Idempotency**: platforms retry invocations.
6. **Cold starts** add latency: strict latency SLAs need pre-warmed instances.

### Self-check questions

1. What happens to the successfully processed messages of a batch if Lambda fails without `ReportBatchItemFailures`?
2. Why must the visibility timeout be several times the function timeout?
3. What should a Cloud Run push handler return for an invalid message, and why?
4. Why limit the concurrency of a serverless consumer?

---

# Module 13. Security: IAM, encryption, private access

## 13.1 Principles

- **No long-lived keys in code**: roles and managed identity instead of access keys and connection strings.
- **Minimal permissions on a specific resource**: "may send to the `orders` queue", not "full access to SQS".
- **Separate identities** for the producer and the consumer.
- **Encryption with keys you control**, if compliance or isolation requires it.
- **Private access**: traffic to the broker doesn't leave for the internet.

## 13.2 AWS

A consumer's IAM policy covers only its own queue:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"],
    "Resource": "arn:aws:sqs:eu-central-1:123456789012:billing"
  }]
}
```

- **Resource policies** on the queue and topic grant access to other services (SNS → SQS, EventBridge → SQS) and other accounts.
- **Encryption**: SSE-SQS (an AWS key) is on by default; **SSE-KMS** with your own key gives control and auditing. The trap: if the queue is encrypted with your own KMS key, **SNS and EventBridge also need permissions on that key** (`kms:GenerateDataKey`, `kms:Decrypt`) in the key policy, otherwise delivery silently fails.
- **VPC endpoints (PrivateLink)** for SQS, SNS and EventBridge: traffic from the VPC doesn't go through the internet; the endpoint policy further restricts which resources can be accessed.

## 13.3 Azure

- **Entra ID and RBAC** instead of SAS keys: the built-in roles **Azure Service Bus Data Sender**, **Data Receiver** and **Data Owner**, assigned on a namespace, queue or topic.
- **Managed identity** for applications and functions: not a single secret in configuration.
- **Disable local authentication** (`disableLocalAuth`) so SAS keys can't be used at all.
- **Private endpoints** and disabling public access (Premium for Service Bus).
- **Customer-managed keys** from Key Vault (Premium).

```bash
az role assignment create --assignee $APP_IDENTITY_ID \
  --role "Azure Service Bus Data Receiver" \
  --scope $(az servicebus queue show -g shop-rg --namespace-name shop-bus -n tasks --query id -o tsv)
```

## 13.4 Google Cloud

- **Roles at the topic and subscription level**: `roles/pubsub.publisher` on the topic for the producer, `roles/pubsub.subscriber` on the subscription for the consumer.
- **The Pub/Sub service agent** (`service-<project-number>@gcp-sa-pubsub.iam.gserviceaccount.com`) needs permissions for dead lettering, push with OIDC and CMEK: a frequent source of "nothing works".
- **CMEK**: encryption with Cloud KMS keys.
- **VPC Service Controls**: a perimeter that prevents data from leaving the allowed projects.
- **Message storage policy**: the regions where a topic's messages may be stored (data residency requirements).
- **Push endpoints** verify the service account's OIDC token.

```bash
gcloud pubsub subscriptions add-iam-policy-binding billing \
  --member=serviceAccount:billing-sa@my-project.iam.gserviceaccount.com \
  --role=roles/pubsub.subscriber
```

## 13.5 Security checklist

- [ ] No access keys, SAS keys or service account JSON keys in code and configuration
- [ ] Roles and managed identity for every application and function
- [ ] Permissions on specific queues, topics and subscriptions, separately for sending and receiving
- [ ] Resource policies allow only the necessary services and accounts
- [ ] Encryption with your own keys where compliance requires it, with key permissions for publishing services
- [ ] Private access (VPC endpoints, private endpoints, VPC Service Controls)
- [ ] Local key-based authentication disabled (Azure)
- [ ] Push endpoints verify the sender's authenticity
- [ ] Access auditing: CloudTrail, Azure Activity Log, Cloud Audit Logs
- [ ] Sensitive data in messages encrypted or moved out of them

---

# Module 14. Monitoring and alerts

## 14.1 The most important metric: the age of the oldest message

Queue length on its own says little: 10,000 messages can be a normal buffer. But **the age of the oldest unprocessed message** directly shows how far behind the system is, and maps well onto an SLA ("events are processed within 5 minutes").

| | AWS SQS | Azure Service Bus | Google Pub/Sub |
|---|---|---|---|
| Age of the oldest | `ApproximateAgeOfOldestMessage` | No direct metric (estimate from `ActiveMessages` and the rate, or peek) | `subscription/oldest_unacked_message_age` |
| Length | `ApproximateNumberOfMessagesVisible` | `ActiveMessages` | `subscription/num_undelivered_messages` |
| In flight | `ApproximateNumberOfMessagesNotVisible` | — | — |
| DLQ | `ApproximateNumberOfMessagesVisible` on the DLQ | `DeadletteredMessages` | `num_undelivered_messages` on the DLQ topic's subscription, `dead_letter_message_count` |
| Inbound flow | `NumberOfMessagesSent` | `IncomingMessages` | `topic/send_request_count` |
| Errors and throttling | `NumberOfEmptyReceives` (cost) | `ServerErrors`, `ThrottledRequests` | `push_request_count` by response code |

For event buses: EventBridge — `FailedInvocations`, `DeadLetterInvocations`, `ThrottledRules`; Event Grid — `DeliveryFailedCount`, `DeadLetteredCount`; SNS — `NumberOfNotificationsFailed`.

## 14.2 What to alert on

| Alert | Condition | Why it matters |
|---|---|---|
| **Backlog** | Age of the oldest message > the SLA | Processing can't keep up or has stopped |
| **Messages in a DLQ** | Any | Business logic is failing |
| **A growing queue with no processing** | Length grows while deletions (`NumberOfMessagesDeleted`, `CompleteMessage`, acks) don't | Consumers aren't working |
| **Event bus delivery errors** | `FailedInvocations`, `DeliveryFailedCount` > 0 | Targets are unavailable or lack permissions |
| **Push errors** | The share of non-2xx responses grows | The handler is failing |
| **Throttling** | `ThrottledRequests`, quota rejections | Tier limits or quotas reached |
| **Spend** | Budget exceeded or an anomaly | Endless retries, loops, empty polls |

## 14.3 Tracing

End-to-end tracing through a broker requires **passing the context in message attributes** (the W3C `traceparent` header):

- AWS: X-Ray and OpenTelemetry, the context in the SQS `AWSTraceHeader` attribute or your own attributes;
- Azure: Application Insights and OpenTelemetry; the Service Bus SDK propagates `Diagnostic-Id`/`traceparent`;
- Google Cloud: Cloud Trace and OpenTelemetry; the Pub/Sub client libraries can propagate the context in attributes.

Without it the trace breaks at the broker, and finding which request produced a broken message is very hard.

## 14.4 Logs and auditing

- Consumer logs should contain the message id, the event id and the attempt count.
- Audit management operations (who deleted a queue, who changed a policy): CloudTrail, Azure Activity Log, Cloud Audit Logs.
- Delivery logs: SNS delivery status logging, Service Bus and Event Grid diagnostic logs, Pub/Sub push subscription logs.

---

# Module 15. Cost, quotas and limits

## 15.1 How you're billed

Prices change, so here are the **billing models**; take concrete numbers from the clouds' calculators.

| Service | What you pay for | What to know |
|---|---|---|
| SQS | API requests | A request up to 64 KB = 1 unit; **a batch of 10 messages = 1 request**; empty `ReceiveMessage` calls are billed too; there's a free tier |
| SNS | Publishes + deliveries by subscription type | Deliveries to SQS and Lambda are cheaper than SMS and email |
| EventBridge | Published events, API destination calls, archive, Scheduler | AWS service events on the default bus are usually free |
| Kinesis | Shard-hours or volume (on-demand), retention beyond 24 h, enhanced fan-out | |
| Service Bus | Basic/Standard: operations (Standard has a base charge); Premium: messaging unit hours | Premium is a fixed cost for dedicated resources |
| Event Hubs | Throughput/processing units, events, Capture | |
| Pub/Sub | **Data volume** published and delivered, retention storage, cross-region traffic | A single request is billed as at least 1 KB: batch small messages |

## 15.2 What inflates the bill

| Cause | Fix |
|---|---|
| SQS short polling: thousands of empty requests a minute | Long polling, 20 s |
| Sending one message at a time | Batches |
| A "poison" message with no DLQ loops for days | A DLQ with a sensible attempt limit |
| Recursion: a function publishes an event that invokes it again | Separate topics and queues, loop protection |
| Fan-out to dozens of subscriptions with large messages | Subscription filters, claim check |
| Retention and retain acked messages in Pub/Sub | Keep only as much as you really need |
| Cross-region traffic | Consumers in the broker's region |
| Premium/dedicated resources idling at night | The right tier for your load profile |

## 15.3 Cloud or your own cluster: a rough estimate

```
cloud service:  cost ≈ number of operations (or volume) × unit price
own cluster:    cost ≈ servers × hours + disks + traffic + engineers' time on operations
```

- With a **small and uneven** flow a cloud service is almost always cheaper: you don't keep a cluster sized for the peak or pay engineers to run it.
- With a **large constant** flow (hundreds of millions to billions of messages a month), per-operation billing can exceed the cost of your own Kafka, RabbitMQ or NATS cluster several times over. But count the cost of operations: engineers' salaries are usually larger than the server bill.
- A middle ground is managed Kafka and RabbitMQ (MSK, Amazon MQ, Managed Kafka): you pay for instances, not operations.

## 15.4 Quotas worth knowing

| Quota | Where it shows up |
|---|---|
| Messages in flight per queue | SQS: around 120,000; beyond that `ReceiveMessage` stops handing out messages |
| FIFO throughput | SQS FIFO: enable high throughput mode for large flows |
| `PutEvents` requests per second | EventBridge: a soft quota per account and region, raised on request |
| Throttling on shared resources | Service Bus Standard: throttling errors when exceeded; you need Premium or sharding across namespaces |
| Throughput and request size | Pub/Sub: regional quotas per project |
| Number of subscriptions, rules, topics | All services: limits per resource and account |

Soft quotas are raised on request, **in advance**, before launch, not on the day of a sale.

## 15.5 Protection from a surprise bill

- **Budgets and alerts** on spend in every cloud (AWS Budgets, Azure Cost Management, Google Cloud Budgets).
- An alert on spend **anomalies** (all three clouds have anomaly detectors).
- Tags or labels on resources by team and service: it's clear who spends what.
- Learning resources in a separate account or project, deleted after each session.

---

# Module 16. Infrastructure as code: Terraform

## 16.1 Why

Queues, topics, subscriptions, DLQs, policies and permissions are infrastructure. Created by hand in the console, they drift between environments, and critical settings (a DLQ, the service agent's permissions, a queue policy) get forgotten. Describe them in Terraform and keep them in Git next to the code.

## 16.2 AWS: SQS with a DLQ and an SNS subscription

```hcl
resource "aws_sqs_queue" "billing_dlq" {
  name                      = "billing-dlq"
  message_retention_seconds = 1209600 # 14 days: longer than the main queue
}

resource "aws_sqs_queue" "billing" {
  name                       = "billing"
  visibility_timeout_seconds = 60
  receive_wait_time_seconds  = 20 # long polling by default
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

# without this policy SNS can't write to the queue
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
  local_auth_enabled  = false # Entra ID only
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

# A subscription is created with the $Default rule (TrueFilter), and rules are OR'ed:
# while $Default is there, billing gets every message. With this flag (azurerm 4.81+)
# the provider deletes $Default right after creating the subscription.
provider "azurerm" {
  features {
    servicebus {
      auto_delete_subscription_default_rule = true
    }
  }
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

## 16.4 Google Cloud: Pub/Sub with a dead letter topic

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
  name  = "orders-dlq-sub" # without a subscription nobody keeps the messages in the DLQ topic
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

# permissions for the Pub/Sub service agent: dead lettering doesn't work without them
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

## 16.5 IaC practice for brokers

- A Terraform module "queue with DLQ and alerts", written once and reused by every service: nobody forgets the DLQ.
- Alerts (CloudWatch alarms, Azure Monitor alerts, Cloud Monitoring policies) in the same module.
- Application permissions next to the resource that needs them.
- `terraform plan` in CI on every change; deleting a queue that holds messages only deliberately.

---

# Module 17. Portability, CloudEvents and migrations

## 17.1 The price of cloud lock-in

Code that calls `sqs.receive_message` or `ServiceBusReceiver` is tied to the cloud. That isn't always bad: portability costs money too. But the decision should be deliberate.

| Portability level | How | Cost |
|---|---|---|
| None | Direct cloud SDK calls everywhere | Moving = rewriting |
| An interface in code | Your own `Publisher`/`Consumer` interface, adapters per cloud (ports and adapters) | A little code, unique features are lost |
| An abstraction library | Dapr pub/sub, Spring Cloud Stream, MassTransit (.NET), Watermill and Go CDK (`gocloud.dev/pubsub`) | A library dependency, the "lowest common denominator" |
| An open protocol | Kafka (MSK, Event Hubs, Managed Kafka), AMQP 1.0 (Service Bus, RabbitMQ), MQTT | A narrower choice of services |

The **lowest common denominator** is the main downside of abstractions: Service Bus sessions, SNS filters and Pub/Sub exactly-once look and work differently, and a universal interface usually doesn't cover them.

## 17.2 CloudEvents

**CloudEvents** is an open CNCF specification for **describing** events, the same across systems:

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

- Required fields: `specversion`, `id`, `source`, `type`. The pair `source` + `id` uniquely identifies an event: a ready-made idempotency key.
- There are **protocol bindings**: HTTP, Kafka, AMQP, MQTT (attributes in headers, data in the body).
- **Event Grid** and **Eventarc** work with CloudEvents natively; EventBridge uses its own format, but it can be converted with an input transformer.

Even if you don't plan to move, CloudEvents is a good standard event envelope: the same fields in every service and cloud, ready SDKs.

## 17.3 Migrations

**From RabbitMQ to the cloud:**

| RabbitMQ | AWS | Azure | Google Cloud |
|---|---|---|---|
| Queue (quorum) | SQS | Service Bus queue | Pub/Sub subscription |
| Fanout/topic exchange + queues | SNS + SQS (or EventBridge) | Service Bus topic + subscriptions | Pub/Sub topic + subscriptions |
| Ack / nack / reject | Delete / visibility / DLQ by limit | complete / abandon / dead-letter | ack / nack / DLQ by limit |
| DLX + delivery-limit | RedrivePolicy | MaxDeliveryCount + $DeadLetterQueue | Dead letter topic |
| Single active consumer | SQS FIFO with one group | Sessions | Ordering key |
| Priorities | Separate queues | Separate queues | Separate subscriptions |
| RPC via direct reply-to | Not a fit, use HTTP/gRPC | Sessions (limited) | Not a fit, use HTTP/gRPC |

Or without a rewrite: **Amazon MQ for RabbitMQ**.

**From Kafka to the cloud:** MSK, Event Hubs (Kafka endpoint) or Managed Service for Apache Kafka: the code doesn't change, only the address and authentication do.

**Between clouds:** a period of **dual publishing** (the event is written to both brokers), consumers switch over one at a time, then the old broker is turned off. A bridge for the migration period is a function, Shovel or Debezium that moves events from one service to the other. Events with a CloudEvents envelope and a stable `id` are much easier to move.

---

# Module 18. A cloud service or your own Kafka, RabbitMQ, NATS

## 18.1 A decision table

| Criterion | Cloud service | Managed Kafka/RabbitMQ | Your own cluster |
|---|---|---|---|
| Operations | Almost none | Partial (sizing, versions) | Entirely yours |
| Portability | Low | High (protocol) | High |
| Features | What the service offers | The product's full features | Full |
| Latency | Tens of milliseconds (HTTPS) | Single-digit milliseconds | Single-digit milliseconds and below |
| Cost with a small flow | Low | Medium (instances 24/7) | Medium + engineers |
| Cost with a huge constant flow | Can be high | Medium | Low for infrastructure, high for people |
| Cloud integrations | Excellent | Good | Do it yourself |
| Multi-cloud and on-premises | No | Partly | Yes |

## 18.2 Practical recommendations

- **A startup or small team in one cloud**: cloud services — SQS/SNS/EventBridge, Service Bus/Event Grid, Pub/Sub. Running clusters shouldn't eat the product's time.
- **You need a log and the Kafka ecosystem**: managed Kafka (MSK, Event Hubs, Managed Kafka); your own cluster only for serious reasons.
- **Complex routing, priorities, RPC, portability**: RabbitMQ (Amazon MQ or your own) or NATS.
- **Low latency, edge, multi-cloud**: NATS.
- **A mixed system** is normal: Pub/Sub for events in Google Cloud, Kafka for the analytics stream, internal NATS for RPC. The key is understanding the guarantees of every link.

The companion courses cover [Kafka](../kafka/README.md), [RabbitMQ](../rabbit/README.md) and [NATS](../nats/README.md) in the same depth as this course covers cloud services.

---

# Module 19. Capstone project: an online shop in three clouds

## 19.1 What you're building

The same system, which you can build in any of the three clouds:

```
  HTTP  --> Order API --> database + outbox --> change stream --> [order events bus / topic]
                                                                         |
                  +-----------------------------+------------------------+-------------------+
                  v                             v                        v                   v
          [billing queue]              [stock queue]             [notify queue]      archive / analytics
          idempotent,                  ordered per order         serverless function
          DLQ + alert                  (FIFO / sessions /        with partial
                                        ordering keys)            batch failures
  Plus:
  - a payment reminder in 3 days (Scheduler / scheduled message / Cloud Tasks)
  - reacting to a receipt upload to storage (EventBridge / Event Grid / Eventarc)
  - Terraform for the whole topology, alerts, a budget
```

## 19.2 Component mapping

| Component | AWS | Azure | Google Cloud |
|---|---|---|---|
| Outbox | DynamoDB Streams → EventBridge Pipes | Cosmos DB Change Feed → Function | Spanner change streams or Debezium Server → Pub/Sub |
| Event topic | SNS (or EventBridge) | Service Bus topic | Pub/Sub topic |
| A service's queue | SQS | Service Bus subscription | Pub/Sub subscription |
| Per-order ordering | SQS FIFO + SNS FIFO | Sessions | Ordering keys |
| Serverless consumer | Lambda + `batchItemFailures` | Functions + Service Bus trigger | Cloud Run + push |
| Delayed reminder | EventBridge Scheduler | Scheduled message | Cloud Tasks |
| File upload event | EventBridge (S3) | Event Grid (Blob) | Eventarc (Cloud Storage) |
| Idempotency | DynamoDB conditional put | Cosmos DB | Firestore or Spanner |
| Monitoring | CloudWatch | Azure Monitor | Cloud Monitoring |

## 19.3 Requirements

1. The order event is published through an outbox: **no dual writes**.
2. Each service reads its own queue or subscription with its own DLQ and alert.
3. Billing is idempotent by event id.
4. Stock processes one order's events strictly in order.
5. Notifications are serverless, with correct handling of partial batch failures and a concurrency limit.
6. The payment reminder arrives after 3 days and is cancelled if the order is paid.
7. Uploading a receipt to storage triggers processing through the event bus.
8. All resources are described in Terraform; permissions are minimal, with no keys in code.
9. Alerts on the age of the oldest message, DLQs and spend.
10. Local integration tests pass on emulators.

## 19.4 Stages

| Stage | What to do |
|---|---|
| 1 | A local environment on emulators, topology from code |
| 2 | Order API + outbox + event publishing |
| 3 | Billing with idempotency and a DLQ |
| 4 | Stock with per-order ordering |
| 5 | Serverless notifications with partial failures |
| 6 | The delayed reminder |
| 7 | The file upload event through the bus |
| 8 | Terraform, IAM, encryption |
| 9 | Monitoring, alerts, a budget |
| 10 | Drills: send a broken message, kill a consumer mid-processing, exceed the concurrency, test a redrive from the DLQ |

If after the drills the queues are empty, the DLQs hold only deliberately broken messages, the database has no duplicates and the budget alerts are silent, you've finished the course. Bonus: build the same project in a second cloud and compare.

---

# CLI cheat sheet: aws, az, gcloud

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
aws sqs start-message-move-task --source-arn $DLQ_ARN            # redrive from the DLQ
aws sqs purge-queue --queue-url $Q

# ---------- AWS: SNS and EventBridge ----------
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
az servicebus topic subscription rule delete -g shop-rg --namespace-name shop-bus --topic-name orders \
  --subscription-name billing -n '$Default'
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

# ---------- Google Cloud: Eventarc and Cloud Tasks ----------
gcloud eventarc triggers create t --location europe-west1 --destination-run-service svc \
  --event-filters type=google.cloud.storage.object.v1.finalized --event-filters bucket=b
gcloud tasks queues create emails --location europe-west1 --max-dispatches-per-second 10
```

---

# Configuration cheat sheet

**A task queue (any cloud):**

```
long polling / streaming pull
processing time (visibility / lock / ack deadline) > p99 processing, extended for long tasks
DLQ + an attempt limit of 3–10 + an alert on the DLQ
maximum DLQ retention
an idempotent consumer keyed by event id
a concurrency limit for consumers
```

**AWS:**

```
SQS: WaitTimeSeconds=20, RedrivePolicy (maxReceiveCount 5), DLQ retention 14 days
SQS FIFO: MessageGroupId per entity, MessageDeduplicationId = event id
SNS -> SQS: a queue policy for SNS, RawMessageDelivery=true, FilterPolicy
Lambda + SQS: ReportBatchItemFailures, MaximumConcurrency, visibility timeout >= 6 × function timeout
EventBridge: a DLQ per target, check FailedEntryCount
```

**Azure:**

```
Service Bu/home/user/workspace/course_message_brokers/clouds: peek-lock, MaxDeliveryCount 5–10, AutoLockRenewer, duplicate detection
ordering: sessions; filters: correlation or SQL on application_properties
Entra ID + managed identity, local_auth_enabled = false
Premium for private endpoints and large messages
```

**Google Cloud:**

```
Pub/Sub: ack deadline sized for processing, FlowControl, retry policy with backoff
dead letter topic + a subscription on it + service agent permissions
ordering: ordering keys; no redeliveries: exactly-once (pull, one region)
filters are set when the subscription is created
a snapshot before a risky release
```

**Always:**

```
check partial failures of batch APIs
the event id is set by the producer (CloudEvents: source + id)
an outbox for events from a transaction
alerts: age of the oldest message, DLQ, delivery errors, budget
the whole topology in Terraform
```

---

# Interview questions with answers

**Junior**

1. **How does a cloud broker differ from your own Kafka or RabbitMQ?** The cloud manages servers, replication and upgrades; you work through an API and pay for operations or volume, but you're tied to the cloud and limited to the service's features.
2. **What is the visibility timeout in SQS?** The time during which a received message is hidden from other consumers; if it isn't deleted within that time, it becomes visible again.
3. **How does SQS standard differ from FIFO?** Standard is at-least-once, unordered, with almost unlimited throughput; FIFO gives ordering within a group and deduplication in a 5-minute window, with limited throughput.
4. **Why long polling?** Short polling queries a subset of servers and can return an empty response while messages exist; long polling waits up to 20 seconds and reduces billable empty requests.
5. **What is SNS, and why pair it with SQS?** SNS is pub/sub without storage; paired with SQS each service gets its own reliable queue with a buffer and a DLQ.
6. **What is peek-lock in Service Bus?** The message is locked to the receiver, which must explicitly settle it (`complete`), return it (`abandon`), dead-letter it or defer it.
7. **How do you get a queue and fan-out in Pub/Sub?** A topic + one subscription is a queue; a topic + several subscriptions is fan-out.
8. **What is a DLQ?** A queue for messages that couldn't be processed within a given number of attempts.
9. **What is the ack deadline in Pub/Sub?** The time to acknowledge a message; without an ack it's redelivered. Client libraries extend it automatically.
10. **Why emulators?** Local development and integration tests without a cloud account or a bill: moto or LocalStack, the Service Bus and Pub/Sub emulators.

**Middle**

11. **How do you guarantee per-entity ordering in each cloud?** SQS FIFO with `MessageGroupId`, Service Bus sessions, Pub/Sub ordering keys: ordering within a key, parallelism across keys.
12. **Why must a standard SQS queue's DLQ have maximum retention?** Retention is counted from the message's original enqueue time, so it can expire in the DLQ before anyone looks at it.
13. **Why might SNS not deliver messages to a subscribed SQS queue?** There's no queue policy allowing SNS to send, or SNS has no permissions on the KMS key the queue is encrypted with.
14. **What is `ReportBatchItemFailures`?** A Lambda response listing the failed messages of an SQS batch: successful ones are deleted, failed ones return; without it the whole batch returns on an error.
15. **What ratio of visibility timeout to Lambda function timeout is recommended?** A visibility timeout of at least six function timeouts, so a message doesn't become visible during processing.
16. **How /home/user/workspace/course_message_brokers/clouddoes `abandon` differ from `defer` in Service Bus?** `abandon` returns the message for redelivery; `defer` keeps it in the queue but hands it out only by sequence number.
17. **Why might dead lettering in Pub/Sub silently not work?** The Pub/Sub service agent wasn't granted permission to publish to the DLQ topic and subscriber permission on the source subscription, or the DLQ topic has no subscription.
18. **What does exactly-once delivery in Pub/Sub protect against?** Redelivery of an acknowledged message on a pull subscription within a region; not a repeated publish, and not failures writing to an external database.
19. **How does EventBridge differ from SNS?** EventBridge routes by content-based rules, integrates with AWS and SaaS events and has archive/replay; SNS is simple fan-out with subscription filters.
20. **How do you schedule an event 3 days out in each cloud?** EventBridge Scheduler, Service Bus scheduled messages, Cloud Tasks with `schedule_time`; SQS delays only up to 15 minutes.
21. **What is a partial batch failure, and where does it occur?** The batch request succeeds but some entries are rejected: SQS, SNS `PublishBatch`, EventBridge `PutEvents`, Kinesis `PutRecords`; check the response and retry the failed entries.
22. **Which metric best shows consumer backlog?** The age of the oldest unprocessed message: `ApproximateAgeOfOldestMessage` in SQS, `oldest_unacked_message_age` in Pub/Sub.
23. **How do you filter messages in Service Bus, SNS and Pub/Sub?** Subscription rules (SQL/correlation on properties), a subscription filter policy (attributes or body), a Pub/Sub subscription filter (attributes, immutable).
24. **When do you need Kinesis, Event Hubs or Managed Kafka instead of a queue?** For a log: replay, many independent readers, large flows, per-partition ordering.
25. **What is a claim check?** The large object goes to object storage and the message carries a reference; it solves size limits and reduces cost.

**Senior**

26. **How do you implement an outbox in AWS, Azure and Google Cloud without your own poller?** DynamoDB Streams → EventBridge Pipes; Cosmos DB Change Feed → Function → Service Bus; Spanner change streams → Dataflow → Pub/Sub; or PostgreSQL + Debezium Server.
27. **Why is a broker-assigned message id unsuitable for idempotency?** A repeated publish by the same producer creates a new message with a new id; the idempotency key must be set by the producer.
28. **How do you limit the load serverless consumers put on a database?** `MaximumConcurrency` on the event source mapping, `maxConcurrentCalls` in Functions, Cloud Run instance limits, `FlowControl` in pull.
29. **How is access to brokers secured in the three clouds?** IAM roles and policies on specific resources (SQS/SNS policies, Service Bus Data Sender/Receiver roles, Pub/Sub roles on topics and subscriptions), managed identity instead of keys, private endpoints, customer-managed encryption keys.
30. **When is a cloud broker more expensive than your own cluster?** With a huge constant flow and per-operation billing; but the cost of operating your own cluster (engineers) often outweighs it.
31. **What is CloudEvents and why use it?** A specification for an event envelope with `id`, `source`, `type`, `specversion`; a single format across services and clouds, native in Event Grid and Eventarc, `source + id` as an idempotency key.
32. **How do you migrate from RabbitMQ to the cloud?** Map exchanges and queues to topics and subscriptions (SNS+SQS, Service Bus topics, Pub/Sub), DLX to DLQs, ordering to FIFO/sessions/ordering keys; or Amazon MQ without a rewrite; dual publishing during the migration.
33. **What are the limitations of abstractions over cloud brokers?** The lowest common denominator: unique features (sessions, filters, exactly-once) are unavailable or behave differently.
34. **How do you avoid a surprise bill?** Long polling, batches, DLQs with a sensible limit, recursion protection, subscription filters, budgets and anomaly alerts, learning resources in separate accounts.
35. **How do you choose between a cloud service, managed Kafka/RabbitMQ and your own cluster?** By requirements on portability, features, latency, cost at your load profile, and the team's readiness to operate it.

---

# FAQ

**Do I need a cloud account to take the course?**
For most modules, no: emulators are enough (moto, the Service Bus emulator, the Pub/Sub emulator); ready-made tests live in `examples/`. An account is needed for IAM, serverless integrations, quotas and cost.

**SQS or SNS?**
SQS is a queue for workers. SNS distributes one message to many. For service-to-service events you usually use both: an SNS topic and an SQS queue per service.

**SNS or EventBridge?**
SNS is simple and cheap fan-out. EventBridge is content-based routing, AWS and SaaS events, archive/replay, Scheduler and Pipes.

**Service Bus or Event Grid?**
Service Bus is reliable queues and topics with peek-lock, sessions and DLQs. Event Grid is event routing, especially Azure resource events, with push delivery. They're often used together: Event Grid → Service Bus.

**Pub/Sub or Cloud Tasks?**
Pub/Sub distributes events to subscribers. Cloud Tasks performs specific HTTP calls with pace control and delayed start.

**Is there exactly-once in the cloud?**
SQS FIFO and Service Bus deduplicate publishes within a window, Pub/Sub has exactly-once delivery on pull subscriptions, Service Bus has transactions within a namespace. Nobody gives exactly-once with an external database: you need idempotency.

**Do cloud brokers preserve ordering?**
Only in special modes: SQS FIFO, Service Bus sessions, Pub/Sub ordering keys, and only within a key. SQS standard, SNS standard, EventBridge and Event Grid don't guarantee ordering.

**Can I re-read messages?**
From queues, no (except Pub/Sub seek). EventBridge has archive and replay. For full replay use Kinesis, Event Hubs or Kafka.

**How do I send a message larger than the limit?**
Claim check: the object in S3, Blob Storage or Cloud Storage, a reference in the message. SQS has an Extended Client Library.

**What happened to Pub/Sub Lite?**
It was shut down on 18 March 2026. The alternatives are standard Pub/Sub or Managed Service for Apache Kafka.

**How do I write portable code?**
Your own interface with adapters or an abstraction library (Dapr, Spring Cloud Stream, MassTransit, Go CDK), CloudEvents as the event format, or an open protocol (Kafka, AMQP 1.0).

---

# Glossary

| Term | Meaning |
|---|---|
| **Managed service** | A service whose operation is handled by the cloud |
| **SQS** | AWS's message queue (standard and FIFO) |
| **Visibility timeout** | The time a received SQS message is hidden from other consumers |
| **Receipt handle** | The id of a specific receive of an SQS message, needed to delete it |
| **Long polling** | Waiting for messages in a request for up to 20 seconds |
| **Message group ID** | The ordering key in SQS FIFO |
| **Redrive policy** | The DLQ and receive limit setting in SQS |
| **SNS** | AWS's pub/sub service |
| **Raw message delivery** | Delivery from SNS without the JSON envelope |
| **Filter policy** | An SNS subscription filter |
| **EventBridge** | AWS's event bus |
| **Event pattern** | An EventBridge rule pattern |
| **Kinesis Data Streams** | AWS's log with shards |
| **Service Bus** | Azure's enterprise broker |
| **Namespace** | A container of Service Bus or Event Hubs entities |
| **Peek-lock** | A receive mode with a lock and explicit settlement |
| **Lock duration** | How long a Service Bus message is locked |
| **Session** | A group of Service Bus messages with ordering and a single receiver |
| **Duplicate detection** | Deduplication of publishes in Service Bus by message id |
| **Deferral** | A deferred Service Bus message, retrievable by sequence number |
| **Event Grid** | Azure's event bus |
| **Event Hubs** | Azure's log with partitions, compatible with Kafka clients |
| **Pub/Sub** | Google Cloud's messaging service |
| **Subscription** | A Pub/Sub subscription: a queue within a topic |
| **Ack deadline** | The time to acknowledge a Pub/Sub message |
| **Ordering key** | The ordering key in Pub/Sub |
| **Exactly-once delivery** | A Pub/Sub subscription mode without redelivery of acknowledged messages |
| **Seek / snapshot** | Rewinding a Pub/Sub subscription by time or to a snapshot |
| **Eventarc** | Google Cloud's event routing |
| **Cloud Tasks** | Google Cloud's queue of HTTP tasks |
| **Event source mapping** | The Lambda mechanism that reads a queue or stream |
| **Batch item failures** | Partial batch failures in Lambda |
| **Managed identity** | An application identity in Azure without secrets |
| **Service agent** | A service's internal identity in Google Cloud |
| **CMEK / SSE-KMS** | Encryption with your own keys |
| **Claim check** | A pattern: the object in storage, a reference in the message |
| **CloudEvents** | A CNCF specification for describing events |
| **Outbox** | A table of events written in a transaction with business data |
| **Emulator** | A local substitute for a cloud service for development and tests |

---

# Official sources and what to read next

- **Amazon SQS** — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/
- **Amazon SNS** — https://docs.aws.amazon.com/sns/latest/dg/
- **Amazon EventBridge** — https://docs.aws.amazon.com/eventbridge/latest/userguide/
- **What's new at AWS** — https://aws.amazon.com/new/
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
- **Compan/home/user/workspace/course_message_brokers/cloudion courses in the series:** NATS, Apache Kafka, RabbitMQ

---

## Contributing

Found an error, an outdated limit or a change in a service's behaviour? Open an issue or send a pull request. Especially welcome:

- examples in other languages (Go, Java, C#, Node.js);
- real production stories and incident write-ups;
- updates to limits and new service features.

⭐ If this course helped, star the repo so other developers can find it.

**Licence:** the course text is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), and the code samples under the [MIT License](../LICENSE). You're free to use, adapt and share the material, including for internal workshops, as long as you credit the source.
