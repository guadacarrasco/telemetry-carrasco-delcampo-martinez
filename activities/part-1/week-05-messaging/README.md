# Week 5: Messaging — SQS and EventBridge

## Objectives
- Understand message queue patterns (point-to-point, pub/sub)
- Implement SQS-based event pipeline for telemetry
- Configure EventBridge to trigger simulator engine on schedule
- Publish and consume messages from SQS

## Tools
- CDK, SQS, EventBridge, Lambda

## Activity
Build the telemetry event pipeline:
1. Simulator engine publishes telemetry events to SQS queue
2. EventBridge rule triggers engine_handler every 5 seconds
3. Event consumer Lambda reads from SQS and updates live state

## Architecture
```
EventBridge (5s) → engine_handler → SQS (f1-telemetry-events) → event_consumer → DynamoDB
                                      ↓
                                    DLQ (3 retries)
```

## Steps

### 1. Create the messaging stack
Define SQS queue with DLQ, the consumer Lambda (with `SqsEventSource`), the engine Lambda, and an EventBridge rule targeting the engine handler. **Note:** EventBridge `Schedule.rate()` requires whole minutes — use `Duration.minutes(1)`, not seconds.

### 2. Implement simulator service
Create a service that generates random telemetry events and publishes to SQS.

### 3. Implement engine + event consumer handlers
- `engine_handler.handler` — invoked by EventBridge, runs one `SimulatorService.tick(...)`.
- `event_consumer.handler` — invoked by SQS, parses each record and upserts `f1_live_state`. Returns `batchItemFailures` for any failures.

### 4. Deploy the stack to LocalStack

The activity is self-contained — it deploys as its own CDK app from the `solution/` folder. Pre-existing queues created by `init-aws.sh` will collide with the stack, so delete them first.

```bash
# (one-time) start LocalStack
cd localstack && docker compose up -d && cd ..

# Remove the pre-seeded queues so the stack can create them
awslocal sqs delete-queue --queue-url http://localhost:4566/000000000000/f1-telemetry-events
awslocal sqs delete-queue --queue-url http://localhost:4566/000000000000/f1-telemetry-events-dlq

# Synth + deploy from a scratch dir (keeps cdk.out out of the asset bundle)
mkdir -p /tmp/week5-cdk && cd /tmp/week5-cdk
cat > app.py <<'EOF'
import sys
sys.path.insert(0, "<repo>/activities/part-1/week-05-messaging/solution")
from aws_cdk import App
from messaging_stack import MessagingStack
app = App()
MessagingStack(app, "Week5Messaging")
app.synth()
EOF
echo '{"app": "python3 app.py"}' > cdk.json

cdklocal bootstrap
cdklocal deploy --require-approval never
```

### 5. Test the pipeline
```bash
# Send a message directly to the queue — the consumer Lambda should drain it
awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/f1-telemetry-events \
  --message-body '{"event_type":"telemetry_update","session_key":9468,"driver_number":1,"timestamp":"2024-01-01T00:00:00Z","data":{"speed":315,"rpm":12000,"throttle":80,"brake":0,"drs":1,"n_gear":7},"idempotency_key":"test-001"}'
sleep 5
awslocal dynamodb scan --table-name f1_live_state

# Or trigger the engine handler manually (same path EventBridge would take)
awslocal lambda invoke --function-name f1-engine-handler --payload '{}' /tmp/out.json
cat /tmp/out.json    # {"published": 4, "session_key": 9468}
```

## Key Concepts
- **SQS**: Managed message queue — decouples producers and consumers
- **Dead Letter Queue (DLQ)**: Captures messages that fail processing after N retries
- **EventBridge Rule**: Trigger actions on schedule or in response to events
- **Visibility Timeout**: Time a message is hidden from other consumers after being read
- **Batch Processing**: Process multiple SQS messages in a single Lambda invocation

## AWS Academy Note
Kinesis is often unavailable on academy accounts. SQS provides similar decoupling capabilities for this use case.
