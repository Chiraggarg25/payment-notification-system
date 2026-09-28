# Payment Notification System

Sends the payment status of a transaction to third‑party **platforms** (Udemy, Amazon, Coursera, …) reliably, following the whiteboard HLD:

```
                                        ┌─▶ topic payment-notifications.udemy    ─▶ group notification-service.udemy    ─┐
 React UI ──POST──▶ platform-service ───┼─▶ topic payment-notifications.amazon   ─▶ group notification-service.amazon   ─┤
                   (key = platformId)   └─▶ topic payment-notifications.coursera ─▶ group notification-service.coursera ─┤
                                                                                                                         ▼
                                                         notification-service  (one consumer thread per platform)
                                            Retry(1s→2s→4s) ∘ CircuitBreaker (one per platform)
                                                    │ sync HTTP                     │ exhausted / circuit OPEN
                                                    ▼                               ▼
                                          Platform API (mock-vendor-api)     Redis  notification:{platform}:{txn}
                                                    ▲                               │  notifications:retry-queue (ZSET)
                                                    │                               │  notifications:dead (SET)
                                                    └──────── sweeper-service ◀─────┘
                                                   (every 5s, exponential backoff 10s→20s→40s→80s, max 5 → DEAD)
```

| Module | Port | What it does |
|---|---|---|
| `ui` (React + Vite, served by nginx) | **3000** | Send events, break platforms, watch circuit breakers and Redis live |
| `platform-service` | 8080 | `POST /api/v1/payments/notifications` → validates → publishes to the platform's own topic **`payment-notifications.{platformId}`** with **message key = platformId** (idempotent producer, `acks=all`). Creates the topics on startup |
| `notification-service` | 8081 | **One Kafka listener container + consumer group per platform topic**. Sync call to the platform wrapped in **Retry (exponential backoff) + Circuit Breaker** per platform. Commits the offset **only after** success or after retries/circuit breaker are exhausted and the failure is stored in Redis |
| `sweeper-service` | 8082 | Scheduled job: picks due failures from Redis, re-sends them, exponential backoff between sweeps, marks **DEAD** after max attempts. Also the REST API to read Redis state |
| `mock-vendor-api` | 8090 | Fake Udemy / Amazon / Coursera webhooks. Each platform can be switched to `UP / DOWN / FLAKY / TIMEOUT / REJECT` at runtime |
| `common` | – | Shared event model, `PlatformClient` (HTTP), `NotificationStore` (Redis), `platforms.yml` |
| Kafka (KRaft) | 29092 | Broker (no ZooKeeper) |
| Kafka UI | **8088** | See the per‑platform topics, messages, consumer‑group offsets / lag |
| Redis | 6379 | Failure store |
| Redis Commander | **8085** | Browse Redis keys in the browser |

Tech: Java 21, Spring Boot 3.5, Spring Kafka, Resilience4j 2.3, Spring Data Redis, React 18.

---

## 1. Run it

Prerequisites: **Docker Desktop** (or Docker Engine + Compose v2), ~4 GB RAM free for Docker. Nothing else — Maven and Node run inside the Docker build.

```bash
cd payment-notification-system
docker compose up --build -d          # first build takes a few minutes (Maven downloads)
docker compose ps                     # wait until everything is "running" / kafka "healthy"
```

Open:

* **Console UI** → http://localhost:3000
* Kafka UI → http://localhost:8088
* Redis Commander → http://localhost:8085

Follow the interesting logs in a second terminal (keep this open while testing):

```bash
docker compose logs -f notification-service sweeper-service
```

Stop: `docker compose down` (add `-v` to also wipe Redis data).

---

## 2. Happy path

1. In the UI (card **1 · Send payment status**) pick `udemy`, keep the generated transaction id, status `SUCCESS`, click **Send**.
2. The table under the form shows the Kafka **topic‑partition @ offset**: `payment-notifications.udemy-0 @ 0`. Send one for `amazon` → it goes to `payment-notifications.amazon`. Card **3** shows each platform's topic, consumer group and assigned partition.
3. Card **4 · Redis** shows the transaction as `DELIVERED by CONSUMER`, consumer tries = 1.

Same thing with curl:

```bash
curl -s -X POST localhost:8080/api/v1/payments/notifications \
  -H 'Content-Type: application/json' \
  -d '{"platformId":"udemy","transactionId":"TXN-1001","paymentStatus":"SUCCESS"}'
# {"eventId":"…","platformId":"udemy",…,"topic":"payment-notifications.udemy","messageKey":"udemy","partition":0,"offset":0}
```

Logs:

```
[KAFKA] received topic=payment-notifications.udemy key=udemy partition=0 offset=0
[DELIVERY] udemy txn=TXN-1001 status=SUCCESS attempt #1 (circuit=CLOSED)
[REDIS] udemy:TXN-1001 -> DELIVERED by CONSUMER (consumerAttempts=1, sweepAttempts=0)
[KAFKA] offset payment-notifications.udemy-0@0 committed (outcome=DELIVERED)
```

---

## 3. Test failed API calls → retries → circuit breaker → Redis → sweeper → DEAD

> Shortcut: `./scripts/demo-failures.sh` does 3.1–3.5 with curl (udemy DOWN, 6 events) and prints the circuit breakers and every Redis hash every 10 s until the records turn `DEAD`. Windows: run it from Git Bash / WSL, or just use the UI.

### 3.1 Take a platform down

In card **2 · Platform APIs** click **DOWN** for `udemy` (or `curl -X PUT localhost:8090/admin/vendors/udemy/mode/DOWN`). Udemy's webhook now returns **503**.

### 3.2 Retries with exponential backoff (consumer)

Send **one** `udemy` event. In the logs you'll see 4 attempts with growing waits:

```
[DELIVERY] udemy txn=TXN-… attempt #1 (circuit=CLOSED)
[RETRY] 'udemy' attempt #1 failed (HTTP 503 from udemy); waiting 1000 ms before next attempt
[DELIVERY] udemy txn=TXN-… attempt #2 (circuit=CLOSED)
[RETRY] 'udemy' attempt #2 failed (HTTP 503 from udemy); waiting 2000 ms before next attempt
[DELIVERY] udemy txn=TXN-… attempt #3 (circuit=CLOSED)
[RETRY] 'udemy' attempt #3 failed (HTTP 503 from udemy); waiting 4000 ms before next attempt
[DELIVERY] udemy txn=TXN-… attempt #4 (circuit=CLOSED)
[RETRY] 'udemy' exhausted after 4 attempts: HTTP 503 from udemy
[REDIS] udemy:TXN-… -> RETRY_PENDING (consumerAttempts=4, nextRetryAt=…)
[KAFKA] offset payment-notifications.udemy-0@… committed (outcome=PARKED_FOR_RETRY)
```

Note the offset is committed **after** the retries are exhausted and the event is safely in Redis.

### 3.3 Circuit breaker opens

Click **Send 5 (new txns)** for `udemy`. The breaker has recorded 4 failures; the 5th failed call pushes the failure rate over 50% (min. 5 calls) and it **OPENs**:

```
[CIRCUIT-BREAKER] 'udemy' State transition from CLOSED to OPEN
[CIRCUIT-BREAKER] 'udemy' is OPEN -> call short-circuited
[REDIS] udemy:TXN-… -> RETRY_PENDING (consumerAttempts=0, … error=Circuit breaker OPEN for 'udemy' - call not permitted)
```

Card **3 · Consumers & circuit breakers** shows `udemy = OPEN` while `amazon` / `coursera` stay `CLOSED` (one breaker per platform). The rest of the burst is parked in Redis instantly — no more waiting on a dead platform. After 30 s the breaker moves to `HALF_OPEN` automatically and lets 3 trial calls through.

(Also at http://localhost:8081/actuator/circuitbreakers and http://localhost:8081/actuator/retryevents.)

### 3.4 Look at Redis

UI card 4 (filter `RETRY_PENDING`), Redis Commander (http://localhost:8085), or the CLI:

```bash
docker exec -it redis redis-cli
> KEYS notification:*
> HGETALL notification:udemy:<txnId>
> ZRANGE notifications:retry-queue 0 -1 WITHSCORES      # score = nextRetryAt (epoch ms)
> SMEMBERS notifications:dead
```

### 3.5 Sweeper retries until they max out

Leave `udemy` **DOWN**. `sweeper-service` polls every 5 s, claims due items and re-sends them. Each failure increments `sweepAttempts` and reschedules with exponential backoff **10 s → 20 s → 40 s → 80 s**; on the 5th failed sweep the record becomes **DEAD** (removed from the retry queue, added to `notifications:dead`):

```
[SWEEPER] claimed 3 due notification(s): [udemy:TXN-…, …]
[SWEEPER] udemy:TXN-… sweep attempt #1/5 (circuit=CLOSED)
[REDIS] udemy:TXN-… sweep attempt #1 failed -> next retry at … (HTTP 503 from udemy)
…
[REDIS] udemy:TXN-… postponed to … (Sweeper circuit breaker OPEN for 'udemy')
…
[SWEEPER] udemy:TXN-… sweep attempt #5/5 (circuit=HALF_OPEN)
[REDIS] udemy:TXN-… -> DEAD (Max sweep attempts (5) exhausted. Last error: HTTP 503 from udemy)
```

In the UI watch the **Sweeps** column go 1 → 5 and the status flip to `DEAD`. Expect **a few minutes** end‑to‑end: the sweeper also has its own circuit breaker, and while it is OPEN items are *postponed without using up an attempt* (the platform was not actually called).

To make it faster, lower the knobs in `docker-compose.yml` → `sweeper-service` and run `docker compose up -d sweeper-service`:

```yaml
SWEEPER_MAX_ATTEMPTS: 3
SWEEPER_BASE_DELAY: 5s
```

### 3.6 Recovery

* Put `udemy` back to **UP** while some records are still `RETRY_PENDING` → the next sweep delivers them: `DELIVERED by SWEEPER`.
* For `DEAD` records click **Requeue** (or `curl -X POST localhost:8082/api/notifications/udemy/<txnId>/requeue`) → they get a fresh set of sweeper attempts and are delivered.
* **Sweep now** (`POST localhost:8082/api/notifications/sweep`) runs a sweep immediately instead of waiting for the scheduler.

### 3.7 Other failure modes worth trying

| Mode | What happens |
|---|---|
| Isolation | Keep `udemy` **DOWN**, send one `udemy` event and **immediately** one `amazon` event. The amazon one is `DELIVERED` within milliseconds while udemy is still in its 1s/2s/4s backoff — they are on different topics, consumed by different threads. (Log thread names: `consumer-udemy-0-C-1` vs `consumer-amazon-0-C-1`.) |
| `FLAKY` | 50% of calls return 500. Most events end `DELIVERED` with consumer tries 2–3 (retry saves them). |
| `TIMEOUT` | Platform sleeps 5 s, client read timeout is 2 s → retryable timeout, same path as `DOWN` but slower. |
| `REJECT` | 400 Bad Request → **not retried**, ignored by the circuit breaker, goes straight to `DEAD`. |
| Redis down | `docker stop redis`, then send an event. The consumer can't take the lock or record the outcome, so it does **not** commit the offset; the error handler re‑processes the record with backoff (1 s → 30 s). In Kafka UI → Consumers you'll see lag on `notification-service.udemy`. `docker start redis` within 5 min → the record is processed and the offset committed. Leave it down longer → the record lands in `payment-notifications.udemy.DLT` (visible in Kafka UI) instead of looping forever. |
| Invalid record | `docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payment-notifications.udemy` and type `not-json`. The record goes straight to `payment-notifications.udemy.DLT` without retries, and the consumer moves on. |
| Client retry | Send the same POST twice with the same `Idempotency-Key` header (curl). Both are accepted by Kafka, but the second is logged `[IDEMPOTENCY] … already handled` and the platform receives it once. |
| Idempotency | `docker compose stop notification-service`, then `docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group notification-service.udemy --topic payment-notifications.udemy --reset-offsets --to-earliest --execute`, then `docker compose start notification-service`. All messages are re‑consumed but logged as `[IDEMPOTENCY] … already handled -> skipping duplicate`. |

Reset everything between experiments: **Clear Redis** + **Reset** buttons in the UI (or `curl -X DELETE localhost:8082/api/notifications` and `curl -X POST localhost:8090/admin/vendors/reset`).

---

### Troubleshooting the Docker setup

| Symptom | Check | Fix |
|---|---|---|
| http://localhost:3000 spins or won't load | `docker compose ps`: is `ui` *Up (healthy)*? Is a Java service *Restarting*? | `docker compose logs --tail=100 <service>` for the one that isn't healthy. The UI no longer depends on the backends being up: it loads anyway, and each card says which service is unreachable. |
| Port 3000 / 8080-8082 / 8090 already in use | Windows: `netstat -ano \| findstr :3000`, then `Get-Process -Id <PID>` | Stop the local `npm run dev` / IDE apps (or `Stop-Process -Id <PID>`), then `docker compose up -d`. |
| Everything is slow, containers restart | `docker stats --no-stream` | Each Java service is capped at 768 MB and Kafka at 1 GB (about 5 GB total). Give Docker at least 6 GB, or stop `kafka-ui` and `redis-commander`. |
| UI shows an old version after a rebuild | | `docker compose up -d --build ui`, then a hard reload (Ctrl+F5). |

## 4. Design notes

### Topic per platform, key = platformId
| Platform | Topic | Consumer group | Consumer thread |
|---|---|---|---|
| udemy | `payment-notifications.udemy` | `notification-service.udemy` | `consumer-udemy-0-C-1` |
| amazon | `payment-notifications.amazon` | `notification-service.amazon` | `consumer-amazon-0-C-1` |
| coursera | `payment-notifications.coursera` | `notification-service.coursera` | `consumer-coursera-0-C-1` |

* **Isolation:** each platform has its own topic, consumer group, consumer thread, offsets and circuit breaker. A platform that is down (and blocks its consumer thread with 1s/2s/4s backoff) can't delay another platform's events, and a rebalance in one group doesn't pause the others.
* **Ordering:** the message key is still the platformId, so every event of a platform lands on the same partition and is processed **in order**. For that reason each topic has **1 partition** (`platforms.partitions-per-topic`); with key = platformId, extra partitions would never be used. If one platform ever needs more throughput, switch that platform's key to `transactionId` and raise its partitions. You keep ordering per transaction and get one consumer thread per partition.
* **Producer:** `enable.idempotence=true` + `acks=all`, so broker‑side retries never duplicate a message.
* **Where it's wired:** topic naming lives in `common/.../platforms.yml` (`topic-prefix`). `KafkaTopicConfig` in platform-service (and `KafkaConsumerConfig` in notification-service) creates a topic for every configured platform on startup. `PlatformConsumerContainers` in notification-service starts one listener container per platform from Spring Boot's listener factory, so each inherits manual ack mode and the error handler.

### Retry + circuit breaker (notification-service)
`Retry( CircuitBreaker( httpCall ) )`, one instance of each **per platform** (created on demand from the `default` config, so adding a platform needs no code change).

* Retry: `max-attempts: 4` (1 + 3 retries), `wait 1s × 2^n` → 1 s, 2 s, 4 s (cap 16 s). Only `RetryableDeliveryException` (5xx, 408, 429, timeouts, connection refused) is retried.
* Circuit breaker: count window 10, min 5 calls, 50% failure rate → OPEN for 30 s → HALF_OPEN (3 trial calls). 4xx is ignored (it's the request that's bad, not the platform).
* `CallNotPermittedException` (breaker OPEN) is **not retried** → parked in Redis immediately.

### Offset commit semantics and bounded error handling
`enable-auto-commit=false`, `ack-mode=manual_immediate`. The listener calls `ack.acknowledge()` only after `deliver()` returns: after success, **or** after retries/circuit breaker are exhausted **and** the failure is written to Redis.

If the listener throws (Redis down, transaction lock busy, invalid record), the offset is **not** committed and `DefaultErrorHandler` re‑processes the record with backoff 1 s → 2 s → … capped at 30 s. That's bounded by `app.kafka.error-handler.max-elapsed-time` (5 min). After that the record goes to the platform's **dead‑letter topic** `payment-notifications.{platform}.DLT` (with exception headers) and its offset is committed. Invalid records (empty, not JSON, missing fields, wrong topic) skip the retries and go straight to the DLT. There are no infinite loops, and no message is dropped silently. `max-poll-records=10` keeps worst‑case processing time per poll well under `max.poll.interval.ms`.

### Idempotency
* **API:** `POST /payments/notifications` accepts an `Idempotency-Key` header, which becomes the `eventId`. Without the header, the eventId is a deterministic UUID of `(platformId, transactionId, paymentStatus)`. A client that retries the same POST therefore produces the same eventId. (The UI sends a fresh key per click.)
* **Consumer:** handled eventIds are stored in `notification-events:processed:{eventId}` (TTL 7 d, ≥ Kafka retention) in the same MULTI/EXEC as the outcome. A re‑delivered or retried event is skipped (`DUPLICATE_SKIPPED`). An event older than the one already recorded for that transaction (e.g. after an offset replay) is skipped as `SUPERSEDED`, so a stale status can never overwrite a newer one.
* **Platform:** every call carries `Idempotency-Key: {eventId}`. So in the one unavoidable at‑least‑once case (the platform acknowledged, then Redis failed before the outcome was recorded), the platform can still de‑duplicate the re‑send. The mock vendor flags these as `DUPLICATE`.

### Concurrency (no races between consumer, sweeper and requeue)
* **Per‑transaction lock** `notification-locks:{platform}:{txn}` (SET NX PX + token, released by compare‑and‑delete Lua). The consumer holds it for the whole delivery (TTL 60 s, bounded wait 10 s). The sweeper takes it per item without waiting; if it's busy, it reschedules the item 5 s later. `requeue` takes it too. So two workers can never send or overwrite the same transaction at once.
* The sweeper acquires the lock **and** reads the hash in **one** Lua call, so it always acts on the latest state.
* Items are **claimed** atomically (Lua, `now + 60 s` lease), so parallel sweeps (scheduled + "Sweep now", or several sweeper instances) never pick the same item. An item claimed by a crashed sweeper becomes due again after the lease.
* `requeue` only accepts `DEAD` records (a `DELIVERED` one is never re‑sent; you get `409 NOT_DEAD`).
* `PlatformConsumerContainers` start/stop/view are synchronized. The mock vendor's state is thread‑safe with bounded memory.

### Redis model
| Key | Type | Content |
|---|---|---|
| `notification:{platformId}:{txnId}` | HASH | eventId, platformId, transactionId, paymentStatus, deliveryStatus (`DELIVERED`/`RETRY_PENDING`/`DEAD`), consumerAttempts, sweepAttempts, postpones, lastError, nextRetryAt, timestamps. `DELIVERED` records expire after 7 days, failures are kept. |
| `notifications:retry-queue` | ZSET | member `{platformId}:{txnId}`, score = nextRetryAt (epoch ms) |
| `notifications:dead` | SET | members whose retries maxed out |
| `notification-events:processed:{eventId}` | STRING (TTL) | idempotency marker (`CONSUMER`/`SWEEPER`/`PARKED`/`DEAD`/`SUPERSEDED`) |
| `notification-locks:{platformId}:{txnId}` | STRING (TTL) | per‑transaction lock token |

All state changes are done in `MULTI/EXEC`. Reads of many records (list, stats) use `SCAN` plus **pipelined** `HGETALL`/`HGET` in chunks of 500: one round trip per chunk rather than one per key (no N+1). The list endpoint is capped with `?limit=` (max 1000).

### Sweeper
Every 5 s: claim ≤ 50 due items, group them by platform, and sweep **platforms in parallel** on a bounded pool (4 threads, whole sweep capped at 45 s < claim lease). One slow platform doesn't hold up the others. Per item: lock + load → send (guarded by its own per‑platform circuit breaker) →
* success = `DELIVERED`
* transient failure = `sweepAttempts++`, next retry after `10s × 2^(attempt-1)` (cap 10 min); attempt 5 = `DEAD`
* 4xx or a corrupt record = `DEAD`
* breaker OPEN = postponed without consuming an attempt, **bounded** by `max-postpones` (20), then `DEAD`
* lock held by the consumer = rescheduled +5 s

An unexpected error on one item doesn't abort the batch. A Redis error after a successful send is never mistaken for a failed delivery.

### Configuration (nothing hard‑coded)
Every timeout, retry count, backoff, TTL, lock lease, batch size, parallelism and limit is a typed `@ConfigurationProperties` record with defaults, **validated at startup** (fail fast on e.g. a zero timeout or `sweep-timeout >= claim-lease`). Platform names come only from `platforms.yml`. Circuit breakers and retries are created from it at startup, not listed in YAML. Main knobs:

| Service | Prefix | Keys |
|---|---|---|
| all | `platforms.*` (platforms.yml) | endpoints, topic-prefix, partitions-per-topic, dlt-suffix, connect-timeout, read-timeout |
| platform-service | `app.*` | publish-timeout, max-idempotency-key-length (+ `max.block.ms`, `spring.mvc.async.request-timeout`) |
| notification-service | `app.kafka.*`, `app.delivery.*`, `app.redis.*`, `resilience4j.*` | group/client-id prefixes, error-handler backoff + max-elapsed-time, first-retry-delay, lock-ttl/wait, TTLs |
| sweeper-service | `app.sweeper.*`, `app.redis.*` | batch-size, max-attempts, base-delay, multiplier, max-delay, max-postpones, claim-lease, sweep-timeout, lock-ttl, platform-parallelism, max-list-limit |
| mock-vendor-api | `mock.*` | vendors, timeout-delay, flaky-failure-rate, max-recent, max-remembered-keys |

### Blocking vs async
* platform-service returns a `CompletableFuture`, so the servlet thread is released while Kafka acknowledges. The continuation on the producer's I/O thread only builds the response. `send()` blocking on missing metadata is bounded by `max.block.ms=5s`, and the ack wait by `publish-timeout`.
* The retry backoff sleeps and the synchronous HTTP call in notification-service run on the platform's **own Kafka consumer thread**, which is a dedicated blocking thread, not an event loop. That's intentional: the offset must not move until the outcome is known.
* The sweeper's HTTP calls run on its bounded worker pool with an overall timeout. The mock vendor runs on virtual threads, so `TIMEOUT` mode's sleep is cheap.

---

## 5. REST API reference

```
# platform-service :8080
POST /api/v1/payments/notifications   {"platformId","transactionId","paymentStatus": SUCCESS|FAILED|PENDING|REFUNDED}
     [header Idempotency-Key: <key>]  # optional; same key => same eventId => delivered once
GET  /api/v1/platforms

# notification-service :8081
GET  /api/consumers                             # per-platform topic, consumer group, assigned partitions
GET  /api/circuit-breakers
POST /api/circuit-breakers/{platform}/reset
GET  /actuator/circuitbreakers | /actuator/circuitbreakerevents | /actuator/retryevents | /actuator/health

# sweeper-service :8082  (Redis view)
GET    /api/notifications[?status=DELIVERED|RETRY_PENDING|DEAD][&limit=200]   # newest first, limit <= 1000
GET    /api/notifications/stats
GET    /api/notifications/{platform}/{txn}
POST   /api/notifications/sweep
POST   /api/notifications/{platform}/{txn}/requeue   # 202 REQUEUED | 404 | 409 NOT_DEAD / BUSY
DELETE /api/notifications                      # wipe (local testing)

# mock-vendor-api :8090
POST /{platform}/payments/notify               # the "platform" webhook
GET  /admin/vendors
PUT  /admin/vendors/{platform}/mode/{UP|DOWN|FLAKY|TIMEOUT|REJECT}
GET  /admin/vendors/{platform}/received
POST /admin/vendors/reset
```

## 6. Adding a platform
Add one line to `common/src/main/resources/platforms.yml`:

```yaml
platforms:
  endpoints:
    pluralsight: https://api.pluralsight.example/payments/webhook
```

Rebuild and restart with `docker compose up -d --build` (platforms.yml is packaged into every service). platform-service then creates the topic `payment-notifications.pluralsight` and accepts the platform. notification-service starts a consumer (group `notification-service.pluralsight`) for it, and it and the sweeper create its retry and circuit breaker automatically. (The mock vendor accepts any platform name, so `${MOCK_VENDOR_BASE_URL}/pluralsight/payments/notify` works for local testing.)

## 7. Running the Java services from your IDE (optional)
Start only the infrastructure and the mock vendor in Docker, then run the apps from IntelliJ / Maven — all defaults point to `localhost`:

```bash
docker compose up -d kafka redis kafka-ui redis-commander
mvn -q -DskipTests install
mvn -pl mock-vendor-api spring-boot:run
mvn -pl platform-service spring-boot:run
mvn -pl notification-service spring-boot:run
mvn -pl sweeper-service spring-boot:run
cd ui && npm install && npm run dev          # http://localhost:3000 (Vite proxies /api/* to the services)
```

## 8. Project layout

```
payment-notification-system/
├── docker-compose.yml
├── docker/java.Dockerfile            # one multi-stage Dockerfile for all Spring Boot modules (--build-arg MODULE)
├── pom.xml                           # Maven parent (Spring Boot 3.5, Java 21)
├── common/                           # event model, PlatformClient, NotificationStore (Redis), platforms.yml
├── platform-service/                 # REST -> Kafka producer
├── notification-service/             # per-platform Kafka consumers + Resilience4j retry/circuit breaker
├── sweeper-service/                  # scheduled re-delivery from Redis + Redis admin API
├── mock-vendor-api/                  # fake Udemy/Amazon/Coursera
└── ui/                               # React (Vite) console, served by nginx
```

## 9. Where to take it next
* Persist every notification / audit trail in a DB (the "DB" box on the HLD) and keep Redis only as the retry queue.
* Replace the mock with real platform endpoints + auth (HMAC signature / OAuth), and move endpoints to a config service.
* Metrics: expose Micrometer/Prometheus (Resilience4j publishes circuit breaker and retry metrics out of the box) and alert on `notifications:dead` size.
* A replay tool for the `.DLT` topics (today you inspect them in Kafka UI and re‑publish manually).
* Redis Cluster: add hash tags to the keys so the multi‑key Lua scripts (lock + hash) map to one slot.
