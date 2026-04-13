.PHONY: test

# Pass the API base URL as a parameter:
#   make test URL=https://<api-id>.execute-api.us-east-2.amazonaws.com/dev

ifndef URL
$(error URL is required. Usage: make test URL=https://<api-id>.execute-api.us-east-2.amazonaws.com/dev)
endif

## Test all three endpoints against the live API
test:
	bash test_api.sh $(URL)
