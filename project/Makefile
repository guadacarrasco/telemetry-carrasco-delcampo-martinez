.PHONY: install bootstrap deploy ingest test-sessions test-drivers test-summary test-laps test all

SESSION_KEY ?= 9662
DRIVER     ?= 1
API_URL    ?= $(shell cat .api_url 2>/dev/null)

# Install CDK dependencies
install:
	pip install -r requirements.txt

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
	curl -s -X POST $(API_URL)sessions/$(SESSION_KEY)/ingest | python3 -m json.tool

# List available sessions
test-sessions:
	@echo "GET /sessions"
	curl -s $(API_URL)sessions | python3 -m json.tool

# List drivers of a session
test-drivers:
	@echo "GET /sessions/$(SESSION_KEY)/drivers"
	curl -s $(API_URL)sessions/$(SESSION_KEY)/drivers | python3 -m json.tool

# Driver summary (stats pre-computed during ingestion)
test-summary:
	@echo "GET /sessions/$(SESSION_KEY)/drivers/$(DRIVER)/summary"
	curl -s $(API_URL)sessions/$(SESSION_KEY)/drivers/$(DRIVER)/summary | python3 -m json.tool

# Driver laps with position per lap
test-laps:
	@echo "GET /sessions/$(SESSION_KEY)/drivers/$(DRIVER)/laps"
	curl -s $(API_URL)sessions/$(SESSION_KEY)/drivers/$(DRIVER)/laps | python3 -m json.tool

# Run all read endpoints (requires prior ingestion)
test: test-sessions test-drivers test-summary test-laps

all: deploy ingest test
