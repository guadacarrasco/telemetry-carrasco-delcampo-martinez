.PHONY: build deploy build-and-deploy test get-url

REGION      := us-east-2
STACK_NAME  := week-02-api-gateway

# Resolve the API base URL from AWS
API_URL = $(shell aws apigateway get-rest-apis \
	--region $(REGION) \
	--query "items[?name=='$(STACK_NAME)'].id | [0]" \
	--output text | xargs -I{} echo "https://{}.execute-api.$(REGION).amazonaws.com/dev")

SESSION_KEY := 9158

## Build the SAM application
build:
	sam build

## Deploy using saved samconfig.toml (no prompts)
deploy:
	sam deploy

## Build and deploy in one step
build-and-deploy: build deploy

## Print the live API base URL
get-url:
	@echo "API base URL: $(API_URL)"

## Test all three endpoints
test:
	@echo "Base URL: $(API_URL)\n"

	@echo "--- [1] GET /sessions ---"
	curl -s -X GET "$(API_URL)/sessions" | python3 -m json.tool

	@echo "\n--- [2] GET /sessions/$(SESSION_KEY) ---"
	curl -s -X GET "$(API_URL)/sessions/$(SESSION_KEY)" | python3 -m json.tool

	@echo "\n--- [3] POST /sessions/$(SESSION_KEY)/ingest ---"
	curl -s -X POST "$(API_URL)/sessions/$(SESSION_KEY)/ingest" | python3 -m json.tool
