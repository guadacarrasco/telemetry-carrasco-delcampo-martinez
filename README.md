# F1 Live Race Telemetry

A serverless backend that ingests real Formula 1 session data, simulates a race replay in compressed time, and streams live telemetry to a Grafana dashboard via Prometheus.

Built with AWS CDK (Python), LocalStack, Docker, and the public [OpenF1 API](https://openf1.org).

---

## Architecture

```
OpenF1 API
    │
    ▼  POST /sessions/{key}/ingest
┌───────────────────┐
│  ingest_session   │  Lambda — fetches sessions, drivers, laps from OpenF1
│      Lambda       │  and stores them in DynamoDB
└─────────┬─────────┘
          │ writes to
          ▼
  ┌───────────────────────────────────────┐
  │              DynamoDB                 │
  │  f1_sessions · f1_driver_stats        │
  │  f1_laps · f1_live_state              │
  │  f1_simulator_state                   │
  └───────────────────────────────────────┘
          ▲                   │
          │ reads             │ reads
          │                   ▼
┌─────────────────┐   POST /start-simulation
│  start_simulation│  Lambda — computes compression ratio,
│      Lambda      │  publishes N lap events to SQS with
└────────┬─────────┘  calculated DelaySeconds
         │ SQS events (timed)
         ▼
┌─────────────────────┐
│    f1-consumer      │  Docker container — reads SQS, updates
│    (Python)         │  f1_live_state in real time
└────────┬────────────┘
         │ writes live state
         ▼
┌─────────────────────┐
│  metrics-exporter   │  Docker container — polls DynamoDB,
│    (Python)         │  exposes /metrics (prometheus_client)
└────────┬────────────┘
         ▼
    Prometheus ──► Grafana :3000
```

### REST API (6 Lambda functions)

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/sessions` | List ingested sessions |
| `POST` | `/sessions/{key}/ingest` | Trigger data ingestion from OpenF1 |
| `GET`  | `/sessions/{key}/drivers` | List drivers for a session |
| `GET`  | `/sessions/{key}/drivers/{num}/summary` | Pre-computed driver stats |
| `GET`  | `/sessions/{key}/drivers/{num}/laps` | Lap-by-lap breakdown |
| `POST` | `/start-simulation` | Start a race replay at compressed speed |

---

## How the simulation works

`POST /start-simulation` accepts a `session_key` and `playback_seconds`. The Lambda reads every lap timestamp from DynamoDB, computes how many real seconds that lap occurred after race start, divides by a **compression ratio** (`real_duration / playback_seconds`), and publishes each lap as an SQS message with the corresponding `DelaySeconds`. The consumer processes events as they arrive, updating live state — so a 2-hour race replays in however many seconds you choose.

---

## Stack

| Layer | Technology |
|-------|-----------|
| Infrastructure | AWS CDK v2 (Python), LocalStack |
| Compute | AWS Lambda (Python 3.12), API Gateway REST |
| Storage | DynamoDB (5 tables, PAY_PER_REQUEST) |
| Messaging | Amazon SQS |
| Observability | Docker Compose, Prometheus, Grafana |
| CI | GitHub Actions (lint → unit tests → CDK synth) |

---

## Project layout

```
project/
├── lambdas/          # Lambda function handlers
│   ├── layer/        # Shared repositories + utils (Lambda layer)
│   ├── ingest_session/
│   ├── list_sessions/
│   ├── list_drivers/
│   ├── get_driver_summary/
│   ├── get_driver_laps/
│   └── start_simulation/
├── stacks/           # CDK stack definitions
├── observability/
│   ├── f1-consumer/        # SQS consumer container
│   ├── metrics-exporter/   # DynamoDB → Prometheus exporter
│   ├── grafana/            # Dashboard provisioning
│   └── prometheus/
├── tests/            # Unit tests (pytest)
└── docker-compose.yml
localstack/           # LocalStack docker-compose
```

---

## Running locally

**Prerequisites:** Docker, Python 3.12, Node.js 22, [LocalStack CLI](https://docs.localstack.cloud/getting-started/installation/)

```bash
# 1 — Start LocalStack
cd localstack && docker-compose up -d

# 2 — Deploy AWS infrastructure
cd project
make bootstrap   # run once per LocalStack instance
make deploy      # CDK deploy → writes API URL to .api_url

# 3 — Start observability stack
make observability-up
# Grafana:    http://localhost:3000  (admin / admin)
# Prometheus: http://localhost:9090

# 4 — Ingest a session (Abu Dhabi 2024 = 9662)
make ingest SESSION_KEY=9662

# 5 — Start race simulation (replay in 5 minutes)
make simulate SESSION_KEY=9662 MINUTES=5
```

Open Grafana at `http://localhost:3000` and select the **Live Race** dashboard. The `session_key` variable auto-populates once Prometheus receives metrics.

---

## Grafana dashboards

- **Live Race** — positions, lap times, gap to leader, tyre strategy, pit stops (all 20 drivers, live)
- **Lap Times** — per-driver lap time trends across the full race
- **Strategy** — tyre compound history and stint lengths

---

## Testing

```bash
cd project
pip install -r requirements-dev.txt
pytest tests/ -v
```

---

## Data source

Session data is fetched from [OpenF1](https://openf1.org) — a free, public API providing real F1 timing data.
