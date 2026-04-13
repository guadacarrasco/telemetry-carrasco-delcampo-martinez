import os

from aws_cdk import Duration, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from constructs import Construct


class MessagingStack(Stack):
    """Scheduled ingestion via EventBridge (disabled by default)."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        sessions_table: dynamodb.ITable,
        driver_stats_table: dynamodb.ITable,
        raw_bucket: s3.IBucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ingest_dir = os.path.join(os.path.dirname(__file__), "..", "lambdas", "ingest")

        ingest_fn = lambda_.Function(
            self,
            "IngestFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset(ingest_dir),
            timeout=Duration.seconds(120),
            memory_size=512,
            environment={
                "SESSIONS_TABLE": sessions_table.table_name,
                "DRIVER_STATS_TABLE": driver_stats_table.table_name,
                "RAW_BUCKET": raw_bucket.bucket_name,
                "SESSION_KEY": "9158",
                "OPENF1_BASE_URL": "https://api.openf1.org",
            },
        )

        sessions_table.grant_read_write_data(ingest_fn)
        driver_stats_table.grant_read_write_data(ingest_fn)
        raw_bucket.grant_put(ingest_fn)

        # EventBridge accepts at least rate(1 minute); rate(5 seconds) is invalid on AWS/LocalStack.
        rule = events.Rule(
            self,
            "IngestScheduleRule",
            schedule=events.Schedule.rate(Duration.minutes(1)),
            enabled=False,
        )
        rule.add_target(targets.LambdaFunction(ingest_fn))
