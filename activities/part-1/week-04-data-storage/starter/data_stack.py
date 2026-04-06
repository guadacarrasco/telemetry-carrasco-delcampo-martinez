"""
Week 4: Data Storage — CDK Stack (Starter)

TODO: Create DynamoDB tables and S3 bucket for F1 Telemetry data.
"""
from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
)
from constructs import Construct


class DataStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # TODO: Create f1_sessions DynamoDB table
        # Partition Key: session_key (Number)
        # Billing Mode: PAY_PER_REQUEST

        # TODO: Create f1_driver_stats DynamoDB table
        # Partition Key: session_key (Number)
        # Sort Key: driver_number (Number)

        # TODO: Create f1-raw-data S3 bucket
        pass
