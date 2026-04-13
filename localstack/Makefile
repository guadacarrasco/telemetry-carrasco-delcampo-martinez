.PHONY: start stop init logs

start:
	docker compose up -d

stop:
	docker compose down

logs:
	docker compose logs -f localstack

# Deploy stacks to LocalStack (requires: pip install aws-cdk-local awscli-local)
init:
	cd ../activities/part-1/week-04-data-storage/solution && \
		export JSII_PACKAGE_CACHE="$$PWD/.jsii-package-cache" && \
		CDK_DEFAULT_REGION=us-east-1 CDK_DEFAULT_ACCOUNT=000000000000 \
		cdklocal bootstrap aws://000000000000/us-east-1 && \
		cdklocal deploy --all --require-approval never
