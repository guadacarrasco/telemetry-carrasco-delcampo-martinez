"""
Week 5: Messaging — CDK Stack
Pipeline: EventBridge -> engine_handler -> SQS (f1-telemetry-events) -> event_consumer -> DynamoDB
"""
import os

from aws_cdk import (
    Stack,
    Duration,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_events,
    aws_dynamodb as dynamodb,
)
from constructs import Construct

CODE_DIR = os.path.dirname(os.path.abspath(__file__))


class MessagingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Dead Letter Queue
        dlq = sqs.Queue(
            self, "TelemetryDLQ",
            queue_name="f1-telemetry-events-dlq",
            retention_period=Duration.days(14),
        )

        # Main Telemetry Queue
        self.telemetry_queue = sqs.Queue(
            self, "TelemetryQueue",
            queue_name="f1-telemetry-events",
            visibility_timeout=Duration.seconds(60),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq,
            ),
        )

        # Import existing DynamoDB table
        live_state_table = dynamodb.Table.from_table_name(
            self, "LiveStateTable", "f1_live_state"
        )

        # Event Consumer Lambda
        consumer_fn = _lambda.Function(
            self, "EventConsumer",
            function_name="f1-event-consumer",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="event_consumer.handler",
            code=_lambda.Code.from_asset(CODE_DIR),
            environment={
                "LIVE_STATE_TABLE": "f1_live_state",
            },
        )
        live_state_table.grant_read_write_data(consumer_fn)
        consumer_fn.add_event_source(
            lambda_events.SqsEventSource(self.telemetry_queue, batch_size=10)
        )

        # Engine Lambda
        engine_fn = _lambda.Function(
            self, "EngineHandler",
            function_name="f1-engine-handler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="engine_handler.handler",
            code=_lambda.Code.from_asset(CODE_DIR),
            environment={
                "TELEMETRY_QUEUE_URL": self.telemetry_queue.queue_url,
                "SIMULATOR_SESSION_KEY": "9468",
                "SIMULATOR_DRIVERS": "1,4,11,16",
            },
        )
        self.telemetry_queue.grant_send_messages(engine_fn)

        # EventBridge Rule — triggers engine every minute
        # NOTE: Schedule.rate() requires whole minutes, seconds will fail
        events.Rule(
            self, "SimulatorTick",
            rule_name="f1-simulator-tick",
            schedule=events.Schedule.rate(Duration.minutes(1)),
            targets=[targets.LambdaFunction(engine_fn)],
            enabled=False,
        )
