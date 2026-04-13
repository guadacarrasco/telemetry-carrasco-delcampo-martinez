.PHONY: build invoke deploy all

# Builds the Lambda package including dependencies
build:
	sam build

# Invokes the function locally using Docker (requires Docker running)
invoke: build
	sam local invoke StartSimulationFunction --env-vars env.json

# Deploys to LocalStack (requires LocalStack running: localstack start)
deploy: build
	samlocal deploy --guided --region us-east-1

all: invoke
