"""
Week 5: Messaging — Simulator Service
"""
import json
import random
import uuid
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
        return {
            "event_type": "telemetry_update",
            "session_key": session_key,
            "driver_number": driver_number,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "speed": random.randint(80, 350),
                "rpm": random.randint(6000, 13000),
                "throttle": random.randint(0, 100),
                "brake": random.randint(0, 100),
                "drs": random.randint(0, 1),
                "n_gear": random.randint(1, 8),
            },
            "idempotency_key": str(uuid.uuid4()),
        }

    def publish_event(self, event: dict) -> None:
        """Publish a telemetry event to SQS."""
        self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(event),
        )

    def tick(self, session_key: int, driver_numbers: list) -> int:
        """Generate and publish telemetry for each driver. Returns count published."""
        for driver_number in driver_numbers:
            event = self.generate_telemetry(session_key, driver_number)
            self.publish_event(event)
        return len(driver_numbers)
