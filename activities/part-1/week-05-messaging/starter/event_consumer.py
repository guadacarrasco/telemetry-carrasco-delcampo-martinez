"""
Week 5: Messaging — Event Consumer Lambda (Starter)

TODO: Implement SQS consumer that processes telemetry events.
"""
import json
import os
import boto3


def handler(event, context):
    """Process SQS messages containing telemetry events."""
    # TODO: Iterate over event["Records"]
    # TODO: Parse each record's body as JSON
    # TODO: Process the telemetry event (update DynamoDB live state)
    # TODO: Return batchItemFailures for any failed records

    return {"batchItemFailures": []}
