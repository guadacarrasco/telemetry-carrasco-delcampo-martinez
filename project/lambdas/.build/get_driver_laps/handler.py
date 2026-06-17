import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layer", "python"))

from repositories import LapsRepository
from utils import error, ok


def handler(event, context):
    path_params = event.get("pathParameters") or {}
    session_key_str = path_params.get("session_key")
    driver_number_str = path_params.get("driver_number")

    if not session_key_str or not driver_number_str:
        return error(400, "session_key and driver_number are required")

    try:
        session_key = int(session_key_str)
        driver_number = int(driver_number_str)
    except ValueError:
        return error(400, "session_key and driver_number must be integers")

    try:
        laps = LapsRepository().list_for_driver(session_key, driver_number)
    except Exception as e:
        return error(500, str(e))

    if not laps:
        return error(404, f"No laps found for driver {driver_number} in session {session_key}")

    # DynamoDB returns laps sorted by lap_number (SK) ascending
    result = [
        {
            "lap_number": lap.get("lap_number"),
            "lap_duration": lap.get("lap_duration"),
            "position": lap.get("position"),
            "sector_1": lap.get("sector_1"),
            "sector_2": lap.get("sector_2"),
            "sector_3": lap.get("sector_3"),
            "is_pit_out": lap.get("is_pit_out"),
        }
        for lap in laps
    ]
    return ok(result)
