import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layer", "python"))

import boto3
import openf1_client
from repositories import (
    DriverStatsRepository,
    LapsRepository,
    RawDataRepository,
    SessionRepository,
)
from utils import error, ok


# ── boto3 helpers ─────────────────────────────────────────────────────────────

def _lambda_client():
    kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("lambda", **kwargs)


# ── Entry point ───────────────────────────────────────────────────────────────

def handler(event, context):
    """Route between trigger mode (API Gateway) and process mode (async)."""
    if event.get("_async"):
        return _process(event["session_key"])
    return _trigger(event, context)


# ── Trigger: validate + kick off async invocation ─────────────────────────────

def _trigger(event, context):
    path_params = event.get("pathParameters") or {}
    session_key_str = path_params.get("session_key")

    if not session_key_str:
        return error(400, "session_key is required")

    try:
        session_key = int(session_key_str)
    except ValueError:
        return error(400, "session_key must be an integer")

    _lambda_client().invoke(
        FunctionName=context.function_name,
        InvocationType="Event",  # async — does not wait for result
        Payload=json.dumps({"_async": True, "session_key": session_key}).encode(),
    )

    return {
        "statusCode": 202,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(
            {
                "message": "Ingestion started",
                "session_key": session_key,
                "hint": f"Poll GET /sessions/{session_key}/drivers to check progress",
            }
        ),
    }


# ── Process: actual ingestion (runs async, no API Gateway timeout) ─────────────

def _find_position_at_lap_start(position_records: list, lap_date_start: str):
    if not position_records or not lap_date_start:
        return None
    last_position = None
    for record in position_records:
        if record.get("date", "") <= lap_date_start:
            last_position = record.get("position")
        else:
            break
    return last_position


def _process(session_key: int):
    session_repo = SessionRepository()
    driver_stats_repo = DriverStatsRepository()
    laps_repo = LapsRepository()
    raw_repo = RawDataRepository()

    # 1 — Session
    try:
        session = openf1_client.get_session(session_key)
    except Exception as e:
        return {"ok": False, "error": f"OpenF1 session: {e}"}

    if not session:
        return {"ok": False, "error": f"Session {session_key} not found"}

    raw_repo.put_json(f"sessions/{session_key}/session.json", session)
    session_repo.save(
        {
            "session_key": session_key,
            "session_name": session.get("session_name", ""),
            "session_type": session.get("session_type", ""),
            "circuit": session.get("circuit_short_name", ""),
            "country": session.get("country_name", ""),
            "year": session.get("year"),
            "date_start": session.get("date_start", ""),
            "date_end": session.get("date_end", ""),
        }
    )

    # 2 — Drivers
    try:
        drivers = openf1_client.get_drivers(session_key)
    except Exception as e:
        return {"ok": False, "error": f"OpenF1 drivers: {e}"}

    raw_repo.put_json(f"sessions/{session_key}/drivers.json", drivers)

    drivers_ingested = 0

    for driver in drivers:
        driver_number = driver.get("driver_number")
        if driver_number is None:
            continue

        try:
            laps = openf1_client.get_laps(session_key, driver_number)
            car_data = openf1_client.get_car_data(session_key, driver_number)
            position_records = openf1_client.get_position(session_key, driver_number)
        except Exception:
            time.sleep(2)
            continue

        prefix = f"sessions/{session_key}/drivers/{driver_number}"
        raw_repo.put_json(f"{prefix}/laps.json", laps)
        raw_repo.put_json(f"{prefix}/car_data.json", car_data)
        raw_repo.put_json(f"{prefix}/position.json", position_records)

        speeds = [r["speed"] for r in car_data if r.get("speed") is not None]
        avg_speed = round(sum(speeds) / len(speeds), 2) if speeds else None
        max_speed = max(speeds) if speeds else None

        valid_laps = [
            lap
            for lap in laps
            if lap.get("lap_duration") is not None and not lap.get("is_pit_out_lap")
        ]
        total_laps = len(laps)
        best_lap_duration = None
        best_lap_number = None

        if valid_laps:
            best = min(valid_laps, key=lambda lap: lap["lap_duration"])
            best_lap_duration = best["lap_duration"]
            best_lap_number = best["lap_number"]

        driver_stats_repo.save(
            {
                "session_key": session_key,
                "driver_number": driver_number,
                "full_name": driver.get("full_name", ""),
                "acronym": driver.get("name_acronym", ""),
                "team_name": driver.get("team_name", ""),
                "total_laps": total_laps,
                "best_lap_duration": best_lap_duration,
                "best_lap_number": best_lap_number,
                "avg_speed": avg_speed,
                "max_speed": max_speed,
            }
        )

        position_sorted = sorted(position_records, key=lambda r: r.get("date", ""))

        for lap in laps:
            lap_number = lap.get("lap_number")
            if lap_number is None:
                continue
            position = _find_position_at_lap_start(
                position_sorted, lap.get("date_start", "")
            )
            laps_repo.save(
                {
                    "session_driver": f"{session_key}#{driver_number}",
                    "lap_number": lap_number,
                    "lap_duration": lap.get("lap_duration"),
                    "position": position,
                    "sector_1": lap.get("duration_sector_1"),
                    "sector_2": lap.get("duration_sector_2"),
                    "sector_3": lap.get("duration_sector_3"),
                    "is_pit_out": lap.get("is_pit_out_lap", False),
                    "date_start": lap.get("date_start", ""),
                }
            )

        drivers_ingested += 1
        time.sleep(0.5)

    return {"ok": True, "session_key": session_key, "drivers_ingested": drivers_ingested}
