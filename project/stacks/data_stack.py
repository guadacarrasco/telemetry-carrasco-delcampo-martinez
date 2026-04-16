from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_s3 as s3
from constructs import Construct


class DataStack(Stack):
    """DynamoDB tables and S3 bucket for F1 telemetry."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.sessions_table = dynamodb.Table(
            self,
            "SessionsTable",
            table_name="f1_sessions",
            partition_key=dynamodb.Attribute(
                name="session_key", type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.driver_stats_table = dynamodb.Table(
            self,
            "DriverStatsTable",
            table_name="f1_driver_stats",
            partition_key=dynamodb.Attribute(
                name="session_key", type=dynamodb.AttributeType.NUMBER
            ),
            sort_key=dynamodb.Attribute(
                name="driver_number", type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # PK: "session_key#driver_number" (e.g. "9662#1"), SK: lap_number
        self.laps_table = dynamodb.Table(
            self,
            "LapsTable",
            table_name="f1_laps",
            partition_key=dynamodb.Attribute(
                name="session_driver", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="lap_number", type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.raw_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        CfnOutput(self, "SessionsTableName", value=self.sessions_table.table_name)
        CfnOutput(self, "DriverStatsTableName", value=self.driver_stats_table.table_name)
        CfnOutput(self, "LapsTableName", value=self.laps_table.table_name)
        CfnOutput(self, "RawBucketName", value=self.raw_bucket.bucket_name)
