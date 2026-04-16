import json
import os
import sys
import time

# Allow running locally with the layer code on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layer", "python"))

import openf1_client
from repositories import (
    SessionRepository,
    DriverStatsRepository,
    LapsRepository,
    RawDataRepository,
)


def _find_position_at_lap_start(position_records: list, lap_date_start: str):
    """Return the driver's position at the moment a lap begins.

    position_records must be sorted ascending by date.
    Returns None if no matching record is found.
    """
    if not position_records or not lap_date_start:
        return None
    last_position = None
    for record in position_records:
        if record.get("date", "") <= lap_date_start:
            last_position = record.get("position")
        else:
            break
    return last_position


def handler(event, context):
    path_params = event.get("pathParameters") or {}
    session_key_str = path_params.get("session_key")

    if not session_key_str:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "session_key is required"}),
        }

    try:
        session_key = int(session_key_str)
    except ValueError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "session_key must be an integer"}),
        }

    session_repo = SessionRepository()
    driver_stats_repo = DriverStatsRepository()
    laps_repo = LapsRepository()
    raw_repo = RawDataRepository()

    # 1 — Session
    try:
        session = openf1_client.get_session(session_key)
    except Exception as e:
        return {"statusCode": 502, "body": json.dumps({"error": f"OpenF1 session: {e}"})}

    if not session:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": f"Session {session_key} not found in OpenF1"}),
        }

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
        return {"statusCode": 502, "body": json.dumps({"error": f"OpenF1 drivers: {e}"})}

    raw_repo.put_json(f"sessions/{session_key}/drivers.json", drivers)

    drivers_ingested = 0

    for driver in drivers:
        driver_number = driver.get("driver_number")
        if driver_number is None:
            continue

        # 3 — Per-driver data from OpenF1
        try:
            laps = openf1_client.get_laps(session_key, driver_number)
            car_data = openf1_client.get_car_data(session_key, driver_number)
            position_records = openf1_client.get_position(session_key, driver_number)
        except Exception:
            time.sleep(2)
            continue  # skip driver on network error, don't abort full ingestion

        prefix = f"sessions/{session_key}/drivers/{driver_number}"
        raw_repo.put_json(f"{prefix}/laps.json", laps)
        raw_repo.put_json(f"{prefix}/car_data.json", car_data)
        raw_repo.put_json(f"{prefix}/position.json", position_records)

        # 4 — Speed stats from car_data
        speeds = [r["speed"] for r in car_data if r.get("speed") is not None]
        avg_speed = round(sum(speeds) / len(speeds), 2) if speeds else None
        max_speed = max(speeds) if speeds else None

        # 5 — Lap stats (exclude pit-out laps from best-lap calculation)
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

        # 6 — Save driver stats (with pre-computed fields)
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

        # 7 — Save each lap with position at lap start
        position_sorted = sorted(position_records, key=lambda r: r.get("date", ""))

        for lap in laps:
            lap_number = lap.get("lap_number")
            if lap_number is None:
                continue
            position = _find_position_at_lap_start(position_sorted, lap.get("date_start", ""))
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
        time.sleep(0.5)  # avoid OpenF1 rate limiting

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "ok": True,
                "session_key": session_key,
                "session_name": session.get("session_name"),
                "drivers_ingested": drivers_ingested,
            }
        ),
    }
