# Week 4: S3, DynamoDB, and EventBridge with CDK

## Objectives
- Migrate from SAM to CDK for infrastructure as code
- Create DynamoDB tables for session and driver data
- Store raw data in S3
- Configure EventBridge rule to trigger Lambda on schedule
- Implement repository pattern for data access

## Tools
- AWS CDK (Python), DynamoDB, S3, EventBridge

## Activity
1. Create CDK stacks for data storage (DynamoDB tables + S3 bucket)
2. Implement repository classes for DynamoDB and S3 operations
3. Wire EventBridge rule to trigger ingestion Lambda
4. Store ingested data in both S3 (raw) and DynamoDB (parsed)

## Steps

### 1. Set up CDK project
```bash
cd activities/part-1/week-04-data-storage/solution
pip install -r requirements.txt
```

### 2. Create DataStack
Define DynamoDB tables:
- `f1_sessions` — PK: session_key (Number)
- `f1_driver_stats` — PK: session_key (Number), SK: driver_number (Number)

Define S3 bucket:
- `f1-raw-data` for storing raw API responses

### 3. Implement repositories
Create repository classes that use boto3 to interact with DynamoDB and S3.
Use `AWS_ENDPOINT_URL` environment variable for LocalStack compatibility.

### 4. Create MessagingStack
Define EventBridge rule on a schedule (the solution uses `rate(1 minute)` — the minimum EventBridge allows; disabled by default).

### 5. Test with LocalStack
```bash
cd localstack && make start && make init
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566
cd ../activities/part-1/week-04-data-storage/solution && PYTHONPATH=. python -c "from repositories.session_repo import SessionRepository; print('OK')"
```
For `make init`, install [aws-cdk-local](https://github.com/localstack/aws-cdk-local) (`pip install aws-cdk-local`) so `cdklocal` is on your PATH. If CDK/jsii cannot write to the default cache dir, set e.g. `export JSII_PACKAGE_CACHE=$PWD/.jsii-package-cache` before deploying.

## Key Concepts
- **CDK vs SAM**: CDK uses real programming languages, SAM uses YAML templates
- **CDK Stacks**: Logical grouping of resources
- **Repository Pattern**: Abstraction layer over data access
- **EventBridge Rules**: Schedule or event-pattern based triggers
- **DynamoDB Partition/Sort Keys**: Efficient data access patterns
