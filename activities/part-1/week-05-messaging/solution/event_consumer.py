"""
Week 5: Messaging — Event Consumer Lambda
Triggered by SQS. Parses telemetry events and upserts into f1_live_state DynamoDB table.
"""
import json
import os
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.getenv("LIVE_STATE_TABLE", "f1_live_state")

_kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
_endpoint = os.getenv("AWS_ENDPOINT_URL")
if _endpoint:
    _kwargs["endpoint_url"] = _endpoint

dynamodb = boto3.resource("dynamodb", **_kwargs)
table = dynamodb.Table(TABLE_NAME)


def process_record(body: dict) -> None:
    """Upsert a telemetry event into f1_live_state."""
    table.put_item(Item={
        "session_key": str(body["session_key"]),
        "driver_number": str(body["driver_number"]),
        "timestamp": body["timestamp"],
        "event_type": body.get("event_type", "telemetry_update"),
        "data": body.get("data", {}),
        "idempotency_key": body.get("idempotency_key", ""),
    })


def handler(event, context):
    """Process SQS messages containing telemetry events."""
    batch_item_failures = []

    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            body = json.loads(record["body"])
            process_record(body)
            logger.info("Processed message %s for driver %s", message_id, body.get("driver_number"))
        except Exception as e:
            logger.error("Failed to process message %s: %s", message_id, e)
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
