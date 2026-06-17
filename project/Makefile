.PHONY: install bootstrap deploy ingest test-sessions test-drivers test-summary test-laps test all \
        observability-up observability-down consumer-restart simulate

SESSION_KEY ?= 9662
DRIVER     ?= 1
API_URL    ?= $(shell cat .api_url 2>/dev/null)

# Install CDK dependencies
install:
	python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# Bootstrap CDK on LocalStack (run once)
bootstrap:
	cdklocal bootstrap

# Deploy all stacks to LocalStack and save the API URL
deploy: install
	cdklocal deploy --all --require-approval never 2>&1 | tee /tmp/cdk_deploy.log
	@grep -o 'https://[^ ]*\.localstack\.cloud[^ ]*prod/' /tmp/cdk_deploy.log | tail -1 > .api_url
	@echo "API URL: $$(cat .api_url)"

# Trigger session ingestion (async - returns 202 immediately)
ingest:
	@echo "Ingesting session $(SESSION_KEY)..."
	curl -sk -X POST $(API_URL)sessions/$(SESSION_KEY)/ingest | python3 -m json.tool

# List available sessions
test-sessions:
	@echo "GET /sessions"
	curl -sk $(API_URL)sessions | python3 -m json.tool

# List drivers of a session
test-drivers:
	@echo "GET /sessions/$(SESSION_KEY)/drivers"
	curl -sk $(API_URL)sessions/$(SESSION_KEY)/drivers | python3 -m json.tool

# Driver summary (stats pre-computed during ingestion)
test-summary:
	@echo "GET /sessions/$(SESSION_KEY)/drivers/$(DRIVER)/summary"
	curl -sk $(API_URL)sessions/$(SESSION_KEY)/drivers/$(DRIVER)/summary | python3 -m json.tool

# Driver laps with position per lap
test-laps:
	@echo "GET /sessions/$(SESSION_KEY)/drivers/$(DRIVER)/laps"
	curl -sk $(API_URL)sessions/$(SESSION_KEY)/drivers/$(DRIVER)/laps | python3 -m json.tool

# Run all read endpoints (requires prior ingestion)
test: test-sessions test-drivers test-summary test-laps

all: deploy ingest test

# ── Observability (Part 2) ──────────────────────────────────────────────────

# Start full stack: LocalStack + f1-consumer + metrics-exporter + Prometheus + Grafana
# Run 'make deploy' first so CDK tables and queue exist in LocalStack.
observability-up:
	docker compose up -d --build
	@echo ""
	@echo "Services running:"
	@echo "  Grafana:    http://localhost:3000"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Metrics:    http://localhost:8000/metrics"
	@echo "  LocalStack: http://localhost:4566"

# Stop all containers (keeps volumes so data survives)
observability-down:
	docker compose down

# Restart only the f1-consumer (call this before each new simulation)
consumer-restart:
	docker compose restart f1-consumer

# Start a simulation: SESSION_KEY and MINUTES are configurable
# Example: make simulate SESSION_KEY=9662 MINUTES=5
MINUTES ?= 5
simulate:
	python3 run_race.py --session $(SESSION_KEY) --minutes $(MINUTES)
