.PHONY: build test-list test-get test-ingest test start-api deploy-dev deploy-prod all

SESSION_KEY ?= 9662

# Builds the Lambda packages including dependencies
build:
	samlocal build

# Invokes ListSessionsFunction locally (requires Docker running)
test-list: build
	sam local invoke ListSessionsFunction

# Invokes GetSessionFunction locally with a session_key
test-get: build
	echo '{"pathParameters": {"session_key": "$(SESSION_KEY)"}}' | sam local invoke GetSessionFunction --event -

# Invokes IngestSessionFunction locally with a session_key
test-ingest: build
	echo '{"pathParameters": {"session_key": "$(SESSION_KEY)"}}' | sam local invoke IngestSessionFunction --event -

# Runs all local tests
test: test-list test-get test-ingest

# Starts local API Gateway (use curl http://localhost:3000/sessions to test)
start-api: build
	sam local start-api

# Deploys to LocalStack dev environment (requires LocalStack running: localstack start)
deploy-dev: build
	samlocal deploy --config-env dev --region us-east-1

# Deploys to LocalStack prod environment (requires LocalStack running: localstack start)
deploy-prod: build
	samlocal deploy --config-env prod --region us-east-1

all: test
