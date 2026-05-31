import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layer", "python"))

from repositories import DriverStatsRepository
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
        driver = DriverStatsRepository().get(session_key, driver_number)
    except LookupError:
        return error(404, f"Driver {driver_number} not found in session {session_key}")
    except Exception as e:
        return error(500, str(e))

    return ok(
        {
            "driver_number": driver.get("driver_number"),
            "full_name": driver.get("full_name"),
            "acronym": driver.get("acronym"),
            "team_name": driver.get("team_name"),
            "total_laps": driver.get("total_laps"),
            "best_lap_duration": driver.get("best_lap_duration"),
            "best_lap_number": driver.get("best_lap_number"),
            "avg_speed": driver.get("avg_speed"),
            "max_speed": driver.get("max_speed"),
        }
    )
