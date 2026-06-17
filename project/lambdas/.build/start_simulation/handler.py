import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layer", "python"))

import boto3
from repositories import DriverStatsRepository, LapsRepository, SessionRepository
from utils import error, ok

SQS_MAX_DELAY_SECONDS = 900


def _lambda_client():
    kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("lambda", **kwargs)


def _sqs_client():
    kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("sqs", **kwargs)


def handler(event, context):
    if event.get("_async"):
        return _process(event["session_key"], event["playback_seconds"])
    return _trigger(event, context)


def _trigger(event, context):
    body = {}
    if event.get("body"):
        try:
            body = json.loads(event["body"])
        except (json.JSONDecodeError, TypeError):
            return error(400, "Invalid JSON body")

    session_key = body.get("session_key")
    playback_seconds = body.get("playback_seconds")

    if session_key is None or playback_seconds is None:
        return error(400, "session_key and playback_seconds are required")

    try:
        session_key = int(session_key)
        playback_seconds = int(playback_seconds)
    except (ValueError, TypeError):
        return error(400, "session_key and playback_seconds must be integers")

    if playback_seconds <= 0:
        return error(400, "playback_seconds must be greater than 0")

    _lambda_client().invoke(
        FunctionName=context.function_name,
        InvocationType="Event",
        Payload=json.dumps({
            "_async": True,
            "session_key": session_key,
            "playback_seconds": playback_seconds,
        }).encode(),
    )

    return {
        "statusCode": 202,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "message": "Simulation started",
            "session_key": session_key,
            "playback_seconds": playback_seconds,
            "hint": f"Events will be published to the queue over the next {playback_seconds} seconds",
        }),
    }


def _parse_date(date_str: str):
    """Parse ISO 8601 string to datetime (UTC). Returns None if empty or invalid."""
    if not date_str:
        return None
    try:
        # Handle offsets like +00:00 or Z
        normalized = date_str.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _process(session_key: int, playback_seconds: int):
    session_repo = SessionRepository()
    driver_stats_repo = DriverStatsRepository()
    laps_repo = LapsRepository()
    sqs = _sqs_client()
    queue_url = os.getenv("SIMULATION_QUEUE_URL")

    # 1 — Validate session exists
    try:
        session_repo.get(session_key)
    except LookupError:
        return {"ok": False, "error": f"Session {session_key} not found. Ingest it first."}

    # 2 — Get all drivers for the session
    drivers = driver_stats_repo.list_for_session(session_key)
    if not drivers:
        return {"ok": False, "error": f"No drivers found for session {session_key}. Ingest it first."}

    # 3 — Collect all laps across all drivers
    all_laps = []
    driver_info = {int(d["driver_number"]): d for d in drivers}

    for driver in drivers:
        driver_number = int(driver["driver_number"])
        laps = laps_repo.list_for_driver(session_key, driver_number)
        for lap in laps:
            all_laps.append({**lap, "_driver_number": driver_number})

    if not all_laps:
        return {"ok": False, "error": f"No laps found for session {session_key}. Ingest it first."}

    # 4 — Determine session start from the earliest lap date_start
    parsed_dates = [
        (lap, _parse_date(str(lap.get("date_start", ""))))
        for lap in all_laps
    ]
    dated_laps = [(lap, dt) for lap, dt in parsed_dates if dt is not None]

    if not dated_laps:
        return {"ok": False, "error": "No laps with valid date_start found. Cannot determine session timeline."}

    session_start = min(dt for _, dt in dated_laps)

    # 5 — Determine real session duration (seconds from first to last lap start)
    session_end = max(dt for _, dt in dated_laps)
    real_duration_seconds = max((session_end - session_start).total_seconds(), 1)

    # compression ratio: how many real seconds map to 1 playback second
    compression_ratio = real_duration_seconds / playback_seconds

    # 6 — Publish each lap as an SQS event with calculated delay
    events_queued = 0

    for lap, lap_dt in dated_laps:
        driver_number = int(lap["_driver_number"])
        driver = driver_info.get(driver_number, {})

        offset_seconds = (lap_dt - session_start).total_seconds()
        delay_seconds = int(offset_seconds / compression_ratio)
        # SQS max delay is 900s; cap it and embed the real target in the body
        capped_delay = min(delay_seconds, SQS_MAX_DELAY_SECONDS)

        message = {
            "event_type": "lap_completed",
            "session_key": session_key,
            "driver_number": driver_number,
            "driver_name": driver.get("full_name", ""),
            "driver_acronym": driver.get("acronym", ""),
            "team_name": driver.get("team_name", ""),
            "lap_number": int(lap["lap_number"]) if lap.get("lap_number") is not None else None,
            "lap_duration": float(lap["lap_duration"]) if lap.get("lap_duration") is not None else None,
            "position": int(lap["position"]) if lap.get("position") is not None else None,
            "sector_1": float(lap["sector_1"]) if lap.get("sector_1") is not None else None,
            "sector_2": float(lap["sector_2"]) if lap.get("sector_2") is not None else None,
            "sector_3": float(lap["sector_3"]) if lap.get("sector_3") is not None else None,
            "is_pit_out": bool(lap.get("is_pit_out", False)),
            "original_timestamp": str(lap.get("date_start", "")),
            "simulated_delay_seconds": delay_seconds,
        }

        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message),
            DelaySeconds=capped_delay,
        )
        events_queued += 1

    return {
        "ok": True,
        "session_key": session_key,
        "playback_seconds": playback_seconds,
        "real_duration_seconds": int(real_duration_seconds),
        "compression_ratio": round(compression_ratio, 2),
        "events_queued": events_queued,
    }
