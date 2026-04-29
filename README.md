# url-checker

A study project exploring RabbitMQ and Bun. The idea is simple: a scheduler pushes URLs into a queue, one or more checkers pull from it and make HTTP requests, and a notifier receives the results and decides whether to alert. Three independent processes, loosely coupled through a message broker.

## What this is actually about

The interesting part is not the URL checking itself — it is how the services communicate without knowing about each other. The scheduler does not call the checker directly. The checker does not know the notifier exists. RabbitMQ sits in between, and each service only cares about its own queue or exchange.

This makes it easy to scale the checker horizontally (just run two instances, they compete for the same queue), add new consumers without touching existing code, and benchmark each layer independently.

## Architecture

```
                         ┌─────────────┐
                         │  sites.txt  │
                         └──────┬──────┘
                                │ reads URLs
                                ▼
                        ┌───────────────┐
                        │   Scheduler   │  every 10s
                        └───────┬───────┘
                                │ publishes to queue
                                ▼
                     ┌─────────────────────┐
                     │  RabbitMQ           │
                     │  queue: url_checks  │
                     └──────────┬──────────┘
                                │ consumed by (competing)
               ┌────────────────┴────────────────┐
               ▼                                 ▼
        ┌─────────────┐                  ┌─────────────┐
        │  Checker 1  │                  │  Checker 2  │
        └──────┬──────┘                  └──────┬──────┘
               │                                │
               └──────────────┬─────────────────┘
                              │ publishes to fanout exchange
                              ▼
              ┌───────────────────────────────┐
              │  RabbitMQ                     │
              │  exchange: check_results      │
              │  (fanout)                     │
              └───────────────┬───────────────┘
                              │ broadcast to all consumers
                              ▼
                      ┌───────────────┐
                      │   Notifier    │
                      └───────┬───────┘
                              │ writes to
                              ▼
                       ┌─────────────┐
                       │ uptime.sqlite│
                       └─────────────┘
```

The queue between the scheduler and checkers uses a **work queue** pattern — messages are distributed across however many checker instances are running. The exchange between checkers and the notifier uses a **fanout** pattern — every consumer bound to it receives every message. This matters if you ever want to add a second consumer (a dashboard, a webhook, a logger) without touching the checkers.

## Services

**Scheduler** (`services/scheduler/`) — reads URLs from `sites.txt` (comma-separated) and pushes each one into the `url_checks` queue every 10 seconds.

**Checker** (`services/checker/`) — pulls messages from `url_checks`, makes an HTTP request with a 5-second timeout, and publishes the result (status, HTTP code, response time) to the `check_results_exchange` fanout exchange. Runs with `prefetch(10)` so up to 10 checks are in-flight concurrently per instance.

**Notifier** (`services/notifier/`) — subscribes to the fanout exchange and handles results. Writes every result to `uptime.sqlite`. Tracks consecutive failures per URL and only fires an alert after 3 consecutive failures, then stays silent until the URL recovers.

Alert behavior:
- 1-2 consecutive failures: logs a warning
- 3rd consecutive failure: logs an alert
- Back to up after alert: logs recovery, resets counter

## Prerequisites

- [Bun](https://bun.sh) v1.0 or later
- Docker (for RabbitMQ)

## Setup

Install dependencies:

```bash
bun install
```

Start RabbitMQ:

```bash
docker compose up -d
```

RabbitMQ management UI is available at `http://localhost:15672` (guest / guest). Useful for inspecting queues, message rates, and consumer counts while the system is running.

Add the URLs you want to monitor to `sites.txt`:

```
https://google.com,https://github.com,https://example.com
```

## Running

Start all services with one command:

```bash
bun run dev
```

This starts the scheduler, two checker instances, and the notifier in parallel. Output from each service is labeled and color-coded in the terminal.

To stop everything, press `Ctrl+C`.

## Benchmarking

The benchmark bypasses the scheduler and floods the queue directly, then measures how many results come back and how long each one takes.

```bash
bun run benchmark
```

Default run is 10 checks across a built-in set of URLs. You can pass a count and a custom URL list:

```bash
bun run benchmark 50
bun run benchmark 20 "https://google.com,https://github.com,https://example.com"
```

Output includes total throughput, end-to-end latency (enqueue to result received), and HTTP response time (fetch only inside the checker). The two latency numbers together tell you how much overhead the queue and exchange are adding on top of the raw HTTP time.

Benchmark results are isolated from the scheduler using a correlation ID, so running a benchmark while the system is live does not produce false results.

## Scaling the checker

To run more checker instances, add entries to the `services` array in `dev.ts`. Since checkers are stateless and consume from a shared queue, RabbitMQ distributes messages across all running instances automatically. Each instance adds another 10 concurrent HTTP checks (the prefetch limit).

## Project structure

```
.
├── services/
│   ├── checker/        work queue consumer, HTTP fetch
│   ├── notifier/       fanout consumer, alerting, SQLite writes
│   └── scheduler/      queue producer, reads sites.txt
├── shared/
│   └── types.ts
├── benchmark.ts        standalone load tester
├── dev.ts              process orchestrator
├── docker-compose.yml  RabbitMQ
└── sites.txt           URLs to monitor
```
