"""
Week 5: Messaging — Simulator Service (Starter)

TODO: Implement a service that generates telemetry events and publishes to SQS.
"""
import json
import random
import os
from datetime import datetime, timezone

import boto3


class SimulatorService:
    def __init__(self):
        kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        self.sqs = boto3.client("sqs", **kwargs)
        self.queue_url = os.getenv("TELEMETRY_QUEUE_URL", "")

    def generate_telemetry(self, session_key: int, driver_number: int) -> dict:
        """Generate a random telemetry event."""
        # TODO: Create a telemetry event dict with:
        # - event_type: "telemetry_update"
        # - session_key, driver_number
        # - timestamp (ISO format)
        # - data: speed, rpm, throttle, brake, drs, n_gear (random values)
        # - idempotency_key (unique ID)
        pass

    def publish_event(self, event: dict) -> None:
        """Publish a telemetry event to SQS."""
        # TODO: Use sqs.send_message() to publish the event
        # Message body should be JSON-encoded
        pass

    def tick(self, session_key: int, driver_numbers: list) -> None:
        """Execute one simulation tick for all drivers."""
        # TODO: Generate and publish telemetry for each driver
        pass
