"""
Week 5: Messaging — Engine Handler Lambda
Triggered by EventBridge on a schedule. Publishes telemetry events to SQS.
"""
import os
import logging

from simulator_service import SimulatorService

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    session_key = int(os.getenv("SIMULATOR_SESSION_KEY", "9468"))
    drivers_env = os.getenv("SIMULATOR_DRIVERS", "1,4,11,16")
    driver_numbers = [int(d.strip()) for d in drivers_env.split(",")]

    service = SimulatorService()
    published = service.tick(session_key, driver_numbers)

    logger.info("Published %d events for session %s", published, session_key)
    return {"published": published, "session_key": session_key}
