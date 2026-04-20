"""
Week 5: Messaging — Engine Handler Lambda (Starter)

TODO: Triggered by EventBridge on a schedule. Run one simulator tick that
publishes telemetry events to SQS for each configured driver.
"""
import os

from simulator_service import SimulatorService


def handler(event, context):
    # TODO: Read SIMULATOR_SESSION_KEY and SIMULATOR_DRIVERS (comma-separated) from env
    # TODO: Instantiate SimulatorService and call .tick(session_key, drivers)
    # TODO: Return a small dict like {"published": <n>, "session_key": <key>}
    pass
